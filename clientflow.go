package main

import (
	"context"
	"errors"
	"net"
	"sync"
	"syscall"
	"time"
)

type ClientFlow struct {
	clientAddr     *net.UDPAddr
	sourceAddr     *net.UDPAddr
	outConn        *net.UDPConn
	flowCtx        context.Context
	flowCancel     context.CancelFunc
	flowWg         sync.WaitGroup
	parentListener *net.UDPConn
	routeName      string
	//routeTimeout   time.Duration
	parentRoute *ActiveRoute
	stopOnce    sync.Once
}

func (cf *ClientFlow) reverseListenerLoop() {
	defer cf.flowWg.Done()

	routeName := cf.routeName
	clientAddrStr := cf.clientAddr.String()
	sourceAddrStr := cf.sourceAddr.String()
	localEphemAddrStr := "unknown"
	outConn := cf.outConn
	parentListener := cf.parentListener

	if outConn != nil {
		if la := outConn.LocalAddr(); la != nil {
			localEphemAddrStr = la.String()
		}
	}

	log.Debugf("Route '%s': Starting reverse listener loop for flow %s (%s <- %s),",
		routeName, clientAddrStr, localEphemAddrStr, sourceAddrStr)
	defer log.Debugf("Route '%s': Reverse listener loop finished for flow %s (%s <- %s)",
		routeName, clientAddrStr, localEphemAddrStr, sourceAddrStr)

	for {

		select {
		case <-cf.flowCtx.Done():
			log.Debugf("Route '%s': Context cancelled for flow %s before reading from source. Exiting reverse loop.", routeName, clientAddrStr)
			return
		default:
		}

		pb := getPacketBuffer()

		currentOutConn := cf.outConn
		if currentOutConn == nil {
			putPacketBuffer(pb)
			log.Warnf("Route '%s': outConn became nil unexpectedly for reverse flow %s. Exiting loop.", routeName, clientAddrStr)
			go cf.Stop()
			return
		}

		var currentTimeout time.Duration
		if cf.parentRoute != nil {
			cf.parentRoute.configLock.RLock()
			currentTimeout = cf.parentRoute.flowTimeout
			cf.parentRoute.configLock.RUnlock()
		} else {
			log.Errorf("Route '%s': BUG: parentRoute is nil for active flow %s. Cannot get timeout. Stopping flow.", routeName, clientAddrStr)
			putPacketBuffer(pb)
			go cf.Stop()
			return
		}

		var deadline time.Time
		if currentTimeout > 0 {
			deadline = time.Now().Add(currentTimeout)
			err := currentOutConn.SetReadDeadline(deadline)
			if err != nil {
				putPacketBuffer(pb)
				if errors.Is(err, net.ErrClosed) {
					log.Debugf("Route '%s': Failed to set read deadline on outConn for flow %s: connection already closed. Exiting loop.", routeName, clientAddrStr)
					return
				}
				log.Errorf("Route '%s': Failed to set read deadline (%v) on outConn for flow %s: %v. Stopping flow.",
					routeName, currentTimeout, clientAddrStr, err)
				go cf.Stop()
				return
			}
		} else {
			err := currentOutConn.SetReadDeadline(time.Time{})
			if err != nil && !errors.Is(err, net.ErrClosed) {
				log.Warnf("Route '%s': Failed to remove read deadline on outConn for flow %s: %v.", routeName, clientAddrStr, err)
			}
		}

		n, readErr := currentOutConn.Read(pb.Data)

		if readErr != nil {
			putPacketBuffer(pb)

			if errors.Is(readErr, net.ErrClosed) {
				log.Infof("Route '%s': Outgoing connection %s closed, exiting reverse listener loop for flow %s gracefully.",
					routeName, localEphemAddrStr, clientAddrStr)

				return
			}

			if ne, ok := readErr.(net.Error); ok && ne.Timeout() {
				log.Infof("Route '%s': Flow for client %s timed out (no data from source %s for %v). Stopping flow.",
					routeName, clientAddrStr, sourceAddrStr, currentTimeout)
				go cf.Stop()
				return
			}

			log.Errorf("Route '%s': Error reading UDP from source %s on %s for flow %s: %v",
				routeName, sourceAddrStr, localEphemAddrStr, clientAddrStr, readErr)
			log.Warnf("Route '%s': Exiting reverse listener loop for flow %s (%s <- %s) due to unhandled read error.",
				routeName, clientAddrStr, localEphemAddrStr, sourceAddrStr)

			go cf.Stop()
			return
		}

		if n == 0 {
			log.Debugf("Route '%s': Received empty UDP packet from source %s via %s (flow %s). Ignoring.",
				routeName, sourceAddrStr, localEphemAddrStr, clientAddrStr)
			putPacketBuffer(pb)
			continue
		}
		pb.N = n
		pb.RemoteAddr = cf.sourceAddr

		logSRTPacket(cf.sourceAddr, pb.Data[:n], false, routeName, localEphemAddrStr)

		if parentListener == nil {
			log.Warnf("Route '%s': Parent listener is nil for flow %s. Cannot forward packet from source. Stopping flow.", routeName, clientAddrStr)
			putPacketBuffer(pb)

			go cf.Stop()
			return
		}

		select {
		case <-cf.flowCtx.Done():
			log.Warnf("Route '%s': Context cancelled for flow %s before writing back to client %s. Dropping packet.", routeName, clientAddrStr, cf.clientAddr.String())
			putPacketBuffer(pb)
			return
		default:

		}

		parentListenerLocalAddr := parentListener.LocalAddr().String()
		_, writeErr := parentListener.WriteToUDP(pb.Data[:n], cf.clientAddr)

		if writeErr != nil {
			putPacketBuffer(pb)

			if errors.Is(writeErr, net.ErrClosed) {
				log.Infof("Route '%s': Write to client %s failed: parent listener %s closed (route likely stopping). Exiting reverse flow.",
					routeName, clientAddrStr, parentListenerLocalAddr)
				go cf.Stop()
				return
			} else if errors.Is(writeErr, syscall.EPIPE) || errors.Is(writeErr, syscall.ECONNREFUSED) || errors.Is(writeErr, syscall.ENETUNREACH) || errors.Is(writeErr, syscall.EHOSTUNREACH) {
				log.Warnf("Route '%s': Write to client %s failed: %v. Stopping flow and exiting reverse loop.",
					routeName, clientAddrStr, writeErr)
				go cf.Stop()
				return
			} else {
				log.Errorf("Route '%s': Error writing to client %s via %s (from source %s): %v",
					routeName, clientAddrStr, parentListenerLocalAddr, sourceAddrStr, writeErr)
				go cf.Stop()
				return
			}
		} else {

			logSRTPacket(cf.clientAddr, pb.Data[:n], true, routeName, parentListenerLocalAddr)
			putPacketBuffer(pb)
		}
	}
}

func (cf *ClientFlow) Stop() {

	cf.stopOnce.Do(func() {

		clientAddrStr := cf.clientAddr.String()
		routeName := cf.routeName
		log.Debugf("Route '%s': Initiating stop sequence for flow %s.", routeName, clientAddrStr)

		cf.flowCancel()
		log.Debugf("Route '%s': Context cancelled for flow %s.", routeName, clientAddrStr)

		outConn := cf.outConn
		if outConn != nil {
			localAddr := "unknown"
			if la := outConn.LocalAddr(); la != nil {
				localAddr = la.String()
			}
			log.Debugf("Route '%s': Closing outConn %s for flow %s.", routeName, localAddr, clientAddrStr)

			err := outConn.Close()
			if err != nil && !errors.Is(err, net.ErrClosed) {
				log.Warnf("Route '%s': Error closing outConn %s for flow %s: %v", routeName, localAddr, clientAddrStr, err)
			} else {
				log.Debugf("Route '%s': Closed outConn %s for flow %s.", routeName, localAddr, clientAddrStr)
			}
		} else {
			log.Debugf("Route '%s': outConn was already nil during stop sequence for flow %s.", routeName, clientAddrStr)
		}

		if cf.parentRoute != nil {
			log.Debugf("Route '%s': Removing flow %s from parent route map.", routeName, clientAddrStr)
			cf.parentRoute.clientFlows.Delete(clientAddrStr)
		} else {
			log.Warnf("Route '%s': Cannot remove flow %s from parent map, parentRoute is nil during stop!", routeName, clientAddrStr)
		}

		log.Debugf("Route '%s': Waiting for reverse listener goroutine to stop for flow %s.", routeName, clientAddrStr)
		cf.flowWg.Wait()
		log.Debugf("Route '%s': Reverse listener stopped. Flow %s fully stopped.", routeName, clientAddrStr)
	})
}
