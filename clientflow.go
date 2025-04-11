package main

import (
	"context"
	"errors"
	"net"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

type ClientFlow struct {
	clientAddr     *net.UDPAddr
	sourceAddr     *net.UDPAddr
	outConn        *net.UDPConn
	lastActivity   atomic.Int64
	flowCtx        context.Context
	flowCancel     context.CancelFunc
	flowWg         sync.WaitGroup
	parentListener *net.UDPConn
	routeName      string
	routeTimeout   time.Duration
}

func (cf *ClientFlow) reverseListenerLoop() {
	defer cf.flowWg.Done()

	routeName := cf.routeName
	clientAddrStr := cf.clientAddr.String()
	sourceAddrStr := cf.sourceAddr.String()
	localEphemAddrStr := "unknown"
	outConn := cf.outConn
	if outConn != nil && outConn.LocalAddr() != nil {
		localEphemAddrStr = outConn.LocalAddr().String()
	}
	parentListener := cf.parentListener

	log.Debugf("Route '%s': Starting reverse listener loop for flow %s (%s <- %s)",
		routeName, clientAddrStr, localEphemAddrStr, sourceAddrStr)
	defer log.Debugf("Route '%s': Reverse listener loop finished for flow %s (%s <- %s)",
		routeName, clientAddrStr, localEphemAddrStr, sourceAddrStr)

	for {
		pb := getPacketBuffer()

		currentOutConn := cf.outConn
		if currentOutConn == nil {
			putPacketBuffer(pb)
			log.Warnf("Route '%s': outConn became nil unexpectedly for reverse flow %s. Exiting loop.", routeName, clientAddrStr)
			return
		}

		n, readErr := currentOutConn.Read(pb.Data)

		if readErr != nil {
			putPacketBuffer(pb)

			if errors.Is(readErr, net.ErrClosed) {
				log.Infof("Route '%s': Outgoing connection %s closed, exiting reverse listener loop for flow %s gracefully.",
					routeName, localEphemAddrStr, clientAddrStr)
				return
			}

			log.Errorf("Route '%s': Error reading UDP from source %s on %s for flow %s: %v",
				routeName, sourceAddrStr, localEphemAddrStr, clientAddrStr, readErr)

			log.Warnf("Route '%s': Exiting reverse listener loop for flow %s (%s <- %s) due to unhandled read error.",
				routeName, clientAddrStr, localEphemAddrStr, sourceAddrStr)
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

		nowNanos := time.Now().UnixNano()
		cf.lastActivity.Store(nowNanos)

		logSRTPacket(cf.sourceAddr, pb.Data[:n], false, routeName, localEphemAddrStr)

		if parentListener == nil {
			log.Warnf("Route '%s': Parent listener is nil for flow %s. Cannot forward packet from source.", routeName, clientAddrStr)
			putPacketBuffer(pb)
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
			if errors.Is(writeErr, net.ErrClosed) {
				log.Infof("Route '%s': Write to client %s failed: parent listener %s closed (route likely stopping). Exiting reverse flow.",
					routeName, clientAddrStr, parentListenerLocalAddr)
				putPacketBuffer(pb)
				return
			} else if errors.Is(writeErr, syscall.EPIPE) || errors.Is(writeErr, syscall.ECONNREFUSED) || errors.Is(writeErr, syscall.ENETUNREACH) || errors.Is(writeErr, syscall.EHOSTUNREACH) {
				log.Warnf("Route '%s': Write to client %s failed: %v. Stopping flow and exiting reverse loop.",
					routeName, clientAddrStr, writeErr)
				putPacketBuffer(pb)
				go cf.Stop()
				return
			} else {
				log.Errorf("Route '%s': Error writing to client %s via %s (from source %s): %v",
					routeName, clientAddrStr, parentListenerLocalAddr, sourceAddrStr, writeErr)
			}
		} else {
			logSRTPacket(cf.clientAddr, pb.Data[:n], true, routeName, parentListenerLocalAddr)
		}

		putPacketBuffer(pb)
	}
}

func (cf *ClientFlow) Stop() {
	clientAddrStr := cf.clientAddr.String()
	routeName := cf.routeName

	log.Debugf("Route '%s': Cancelling context for flow %s.", routeName, clientAddrStr)
	cf.flowCancel()

	outConn := cf.outConn
	if outConn != nil {
		localAddr := "unknown"
		if outConn.LocalAddr() != nil {
			localAddr = outConn.LocalAddr().String()
		}
		log.Debugf("Route '%s': Closing outConn %s for flow %s.", routeName, localAddr, clientAddrStr)
		err := outConn.Close()
		if err != nil && !errors.Is(err, net.ErrClosed) {
			log.Warnf("Route '%s': Error closing outConn %s for flow %s: %v", routeName, localAddr, clientAddrStr, err)
		} else {
			log.Debugf("Route '%s': Closed outConn %s for flow %s.", routeName, localAddr, clientAddrStr)
		}
	} else {
		log.Debugf("Route '%s': outConn was already nil for flow %s.", routeName, clientAddrStr)
	}

	log.Debugf("Route '%s': Waiting for reverse listener to stop for flow %s.", routeName, clientAddrStr)
	cf.flowWg.Wait()
	log.Debugf("Route '%s': Reverse listener stopped for flow %s.", routeName, clientAddrStr)
}
