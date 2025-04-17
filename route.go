package main

import (
	"context"
	"errors"
	"net"
	"sync"
	"syscall"
	"time"
)

type ActiveRoute struct {
	routeName    string
	listenAddr   *net.UDPAddr
	listener     *net.UDPConn
	targetSource *net.UDPAddr
	flowTimeout  time.Duration

	clientFlows sync.Map

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup

	configLock sync.RWMutex

	packetChan chan *packetBuffer
	workerWg   sync.WaitGroup
	numWorkers int
}

func (ar *ActiveRoute) startProcessing() {
	ar.wg.Add(1)
	go ar.clientListenerLoop()
	ar.configLock.RLock()
	rName := ar.routeName
	addr := ar.listenAddr.String()
	workers := ar.numWorkers
	ar.configLock.RUnlock()
	log.Debugf("Route '%s': Client listener loop goroutine started for %s", rName, addr)

	log.Debugf("Route '%s': Starting %d packet worker goroutines for %s", rName, workers, addr)
	ar.workerWg.Add(workers)
	for i := 0; i < workers; i++ {
		go ar.packetWorker(i)
	}
	log.Debugf("Route '%s': All %d packet workers started", rName, workers)
}

func (ar *ActiveRoute) clientListenerLoop() {
	defer ar.wg.Done()

	ar.configLock.RLock()
	initialRouteName := ar.routeName
	localAddrStr := "unknown"
	listener := ar.listener
	if listener != nil && listener.LocalAddr() != nil {
		localAddrStr = listener.LocalAddr().String()
	}
	ar.configLock.RUnlock()

	log.Infof("Route '%s': Starting client listener loop on %s", initialRouteName, localAddrStr)
	defer log.Infof("Route '%s': Client listener loop finished for %s", initialRouteName, localAddrStr)

	for {

		pb := getPacketBuffer()

		currentListener := ar.listener
		if currentListener == nil {
			putPacketBuffer(pb)
			log.Warnf("Route '%s': Listener became nil unexpectedly during loop for %s.", initialRouteName, localAddrStr)
			return
		}

		n, remoteAddr, readErr := currentListener.ReadFromUDP(pb.Data)

		if readErr != nil {
			putPacketBuffer(pb)

			if errors.Is(readErr, net.ErrClosed) {

				log.Infof("Route '%s': Listener %s closed, exiting client listener loop gracefully.", initialRouteName, localAddrStr)
				return
			}

			ar.configLock.RLock()
			currentName := ar.routeName
			ar.configLock.RUnlock()
			log.Errorf("Route '%s': Error reading UDP from client on %s: %v", currentName, localAddrStr, readErr)

			log.Warnf("Route '%s': Exiting listener loop on %s due to unhandled read error.", currentName, localAddrStr)
			return
		}

		if n == 0 {
			log.Debugf("Route '%s': Received empty UDP packet from %s on %s. Ignoring.", initialRouteName, remoteAddr.String(), localAddrStr)
			putPacketBuffer(pb)
			continue
		}

		pb.N = n
		pb.RemoteAddr = remoteAddr

		select {
		case ar.packetChan <- pb:
			log.Tracef("Route '%s': Packet from %s (%d bytes) queued for processing.", initialRouteName, remoteAddr.String(), n)

		case <-ar.ctx.Done():

			log.Warnf("Route '%s': Context cancelled while queueing packet from %s. Dropping packet and exiting listener.", initialRouteName, remoteAddr.String())
			putPacketBuffer(pb)
			return
		default:

			log.Warnf("Route '%s': Packet processing channel for %s is full (dropping packet from %s). Consider increasing workers or chan_size.",
				initialRouteName, localAddrStr, remoteAddr.String())
			putPacketBuffer(pb)

		}
	}
}

func (ar *ActiveRoute) packetWorker(workerID int) {
	defer ar.workerWg.Done()

	ar.configLock.RLock()
	routeName := ar.routeName
	ar.configLock.RUnlock()

	log.Debugf("Route '%s': Worker %d started.", routeName, workerID)
	defer log.Debugf("Route '%s': Worker %d stopped.", routeName, workerID)

	for {
		select {
		case pb, ok := <-ar.packetChan:
			if !ok {

				log.Debugf("Route '%s': Worker %d detected packet channel close. Exiting.", routeName, workerID)
				return
			}

			if pb == nil {

				log.Warnf("Route '%s': Worker %d received nil packet buffer from channel. Skipping.", routeName, workerID)
				continue
			}
			if pb.RemoteAddr == nil {

				log.Errorf("Route '%s': Worker %d received packetBuffer with nil RemoteAddr. Dropping.", routeName, workerID)
				putPacketBuffer(pb)
				continue
			}

			ar.configLock.RLock()
			currentRouteName := ar.routeName
			ar.configLock.RUnlock()
			log.Tracef("Route '%s': Worker %d picked up packet from %s (%d bytes).", currentRouteName, workerID, pb.RemoteAddr.String(), pb.N)

			ar.handleClientPacket(pb)

			putPacketBuffer(pb)
			log.Tracef("Route '%s': Worker %d finished processing packet from %s, buffer returned.", currentRouteName, workerID, pb.RemoteAddr.String())

		case <-ar.ctx.Done():
			log.Debugf("Route '%s': Worker %d detected context cancellation. Exiting.", routeName, workerID)
			return
		}
	}
}

func (ar *ActiveRoute) handleClientPacket(pb *packetBuffer) {
	clientAddr := pb.RemoteAddr
	if clientAddr == nil {
		log.Errorf("handleClientPacket received packetBuffer with nil RemoteAddr. Dropping.")
		return
	}
	clientAddrStr := clientAddr.String()

	ar.configLock.RLock()
	routeName := ar.routeName
	targetSource := ar.targetSource
	flowTimeout := ar.flowTimeout
	parentListener := ar.listener
	listenerAddrStr := "unknown"
	if parentListener != nil && parentListener.LocalAddr() != nil {
		listenerAddrStr = parentListener.LocalAddr().String()
	}
	ar.configLock.RUnlock()

	logSRTPacket(clientAddr, pb.Data[:pb.N], false, routeName, listenerAddrStr)

	flowVal, found := ar.clientFlows.Load(clientAddrStr)

	if found {

		flow := flowVal.(*ClientFlow)
		ar.processPacketForExistingFlow(flow, pb, routeName)
	} else {

		ar.createNewFlowAndProcessPacket(pb, routeName, targetSource, flowTimeout, parentListener)
	}
}

func (ar *ActiveRoute) processPacketForExistingFlow(flow *ClientFlow, pb *packetBuffer, routeName string) {
	clientAddrStr := flow.clientAddr.String()

	select {
	case <-flow.flowCtx.Done():
		log.Warnf("Route '%s': Received packet from client %s for an already stopped flow (context done). Ensuring map cleanup.", routeName, clientAddrStr)

		ar.clientFlows.Delete(clientAddrStr)
		return
	default:

	}

	outConn := flow.outConn
	if outConn == nil {

		log.Warnf("Route '%s': Flow for client %s exists but outConn is nil. Dropping packet and attempting cleanup.", routeName, clientAddrStr)
		ar.clientFlows.Delete(clientAddrStr)
		go flow.Stop()
		return
	}
	ephemeralAddrStr := outConn.LocalAddr().String()
	sourceAddrStr := flow.sourceAddr.String()

	_, writeErr := outConn.Write(pb.Data[:pb.N])

	if writeErr != nil {

		if errors.Is(writeErr, net.ErrClosed) {

			log.Debugf("Route '%s': Write to source %s for client %s failed: connection closed (flow likely stopping).",
				routeName, sourceAddrStr, clientAddrStr)

			select {
			case <-flow.flowCtx.Done():

			default:

				log.Warnf("Route '%s': Connection closed unexpectedly while writing to source for flow %s. Initiating stop.", routeName, clientAddrStr)

				ar.clientFlows.Delete(clientAddrStr)
				go flow.Stop()
			}
		} else if errors.Is(writeErr, syscall.EPIPE) || errors.Is(writeErr, syscall.ECONNREFUSED) {

			log.Warnf("Route '%s': Write to source %s for client %s failed: %v. Stopping flow.",
				routeName, sourceAddrStr, clientAddrStr, writeErr)

			ar.clientFlows.Delete(clientAddrStr)
			go flow.Stop()
		} else {

			log.Errorf("Route '%s': Error writing to source %s for client %s via ephemeral port %s: %v",
				routeName, sourceAddrStr, clientAddrStr, ephemeralAddrStr, writeErr)

		}
	} else {

		logSRTPacket(flow.sourceAddr, pb.Data[:pb.N], true, routeName, ephemeralAddrStr)
	}
}

func (ar *ActiveRoute) createNewFlowAndProcessPacket(pb *packetBuffer, routeName string, targetSource *net.UDPAddr, flowTimeout time.Duration, parentListener *net.UDPConn) {
	clientAddr := pb.RemoteAddr
	clientAddrStr := clientAddr.String()
	targetSourceStr := targetSource.String()

	log.Infof("Route '%s': New flow detected from client %s -> target source %s. Creating...", routeName, clientAddrStr, targetSourceStr)

	if parentListener == nil {
		log.Errorf("Route '%s': Cannot create new flow for client %s, parent listener is nil (route stopping?). Dropping packet.", routeName, clientAddrStr)
		return
	}

	outConn, err := net.DialUDP("udp", nil, targetSource)
	if err != nil {
		log.Errorf("Route '%s': Failed to dial source %s for new client %s: %v. Dropping packet.",
			routeName, targetSourceStr, clientAddrStr, err)
		return
	}
	ephemeralAddr := outConn.LocalAddr()
	ephemeralAddrStr := ephemeralAddr.String()
	log.Debugf("Route '%s': Created outgoing UDP connection %s -> %s for client %s",
		routeName, ephemeralAddrStr, targetSourceStr, clientAddrStr)

	flowCtx, flowCancel := context.WithCancel(ar.ctx)

	clientAddrCopy := &net.UDPAddr{IP: make(net.IP, len(clientAddr.IP)), Port: clientAddr.Port, Zone: clientAddr.Zone}
	copy(clientAddrCopy.IP, clientAddr.IP)
	sourceAddrCopy := &net.UDPAddr{IP: make(net.IP, len(targetSource.IP)), Port: targetSource.Port, Zone: targetSource.Zone}
	copy(sourceAddrCopy.IP, targetSource.IP)

	newFlow := &ClientFlow{
		clientAddr:     clientAddrCopy,
		sourceAddr:     sourceAddrCopy,
		outConn:        outConn,
		flowCtx:        flowCtx,
		flowCancel:     flowCancel,
		parentListener: parentListener,
		routeName:      routeName,
		routeTimeout:   flowTimeout,
		parentRoute:    ar,
	}

	actualFlowVal, loaded := ar.clientFlows.LoadOrStore(clientAddrStr, newFlow)

	if loaded {

		existingFlow := actualFlowVal.(*ClientFlow)
		log.Warnf("Route '%s': Race condition detected creating flow for client %s. Using existing flow, closing redundant outConn %s.",
			routeName, clientAddrStr, ephemeralAddrStr)

		flowCancel()
		outConn.Close()

		ar.processPacketForExistingFlow(existingFlow, pb, routeName)
		return
	}

	log.Debugf("Route '%s': Successfully stored new flow for client %s.", routeName, clientAddrStr)

	newFlow.flowWg.Add(1)
	go newFlow.reverseListenerLoop()
	log.Debugf("Route '%s': Reverse listener loop goroutine started for flow %s (%s <- %s)",
		routeName, clientAddrStr, ephemeralAddrStr, targetSourceStr)

	_, writeErr := newFlow.outConn.Write(pb.Data[:pb.N])
	if writeErr != nil {

		log.Errorf("Route '%s': Failed initial write to source %s for client %s via %s: %v. Stopping newly created flow.",
			routeName, targetSourceStr, clientAddrStr, ephemeralAddrStr, writeErr)

		ar.clientFlows.Delete(clientAddrStr)
		go newFlow.Stop()
		return
	}

	logSRTPacket(newFlow.sourceAddr, pb.Data[:pb.N], true, routeName, ephemeralAddrStr)
	log.Debugf("Route '%s': Initial packet sent for new flow %s.", routeName, clientAddrStr)
}

func (ar *ActiveRoute) Stop() {
	ar.configLock.RLock()
	routeName := ar.routeName
	localAddrStr := "unknown"
	listener := ar.listener
	if ar.listenAddr != nil {
		localAddrStr = ar.listenAddr.String()
	}
	numWorkers := ar.numWorkers
	ar.configLock.RUnlock()

	log.Infof("Route '%s': Stopping route on %s...", routeName, localAddrStr)

	log.Debugf("Route '%s': Cancelling context.", routeName)
	ar.cancel()

	if listener != nil {
		log.Debugf("Route '%s': Closing main listener %s.", routeName, localAddrStr)
		err := listener.Close()

		if err != nil && !errors.Is(err, net.ErrClosed) {
			log.Warnf("Route '%s': Error closing main listener on %s: %v", routeName, localAddrStr, err)
		} else {
			log.Debugf("Route '%s': Main listener on %s closed.", routeName, localAddrStr)
		}
	} else {
		log.Debugf("Route '%s': Main listener was already nil.", routeName)
	}

	log.Debugf("Route '%s': Waiting for client listener goroutine (%s) to stop...", routeName, localAddrStr)
	ar.wg.Wait()
	log.Debugf("Route '%s': Client listener goroutine (%s) stopped.", routeName, localAddrStr)

	log.Debugf("Route '%s': Closing packet channel.", routeName)
	if ar.packetChan != nil {

		func() {
			defer func() {
				if r := recover(); r != nil {
					log.Warnf("Route '%s': Recovered from panic while closing packet channel: %v", routeName, r)
				}
			}()
			close(ar.packetChan)
		}()
	}

	log.Debugf("Route '%s': Waiting for %d worker goroutines to stop...", routeName, numWorkers)
	ar.workerWg.Wait()
	log.Debugf("Route '%s': All %d worker goroutines stopped.", routeName, numWorkers)

	var flowsToStop []*ClientFlow
	var flowKeysToRemove []string
	flowCount := 0
	ar.clientFlows.Range(func(key, value any) bool {
		flowCount++
		clientKey := key.(string)
		flow := value.(*ClientFlow)
		flowsToStop = append(flowsToStop, flow)
		flowKeysToRemove = append(flowKeysToRemove, clientKey)
		return true
	})

	for _, key := range flowKeysToRemove {
		ar.clientFlows.Delete(key)
	}

	if flowCount > 0 {
		log.Infof("Route '%s': Initiating stop for %d remaining client flows...", routeName, flowCount)
		var flowStopWg sync.WaitGroup
		flowStopWg.Add(len(flowsToStop))
		for _, flow := range flowsToStop {
			go func(cf *ClientFlow) {
				defer flowStopWg.Done()

				log.Debugf("Route '%s': Triggering stop for flow %s.", routeName, cf.clientAddr.String())
				cf.Stop()
			}(flow)
		}
		flowStopWg.Wait()
		log.Infof("Route '%s': Finished stopping remaining %d client flows.", routeName, len(flowsToStop))
	} else {
		log.Debugf("Route '%s': No active client flows found in map during route stop.", routeName)
	}

	log.Infof("Route '%s': Route on %s fully stopped.", routeName, localAddrStr)
}
