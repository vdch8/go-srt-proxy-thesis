package main

import (
	"net"
	"sync"
	"sync/atomic"
)

const (
	DefaultUDPReadBufferSize = 2048
	MinUDPReadBufferSize     = 64
	MaxUDPPayloadSize        = 65507
)

var currentUDPReadBufferSize atomic.Int32

func init() {
	currentUDPReadBufferSize.Store(DefaultUDPReadBufferSize)
}

func SetEffectiveUDPReadBufferSize(size int) {
	if size < MinUDPReadBufferSize || size > MaxUDPPayloadSize {
		log.Errorf("SetEffectiveUDPReadBufferSize: Attempted to set invalid size %d (allowed range: %d-%d). Keeping previous value %d.",
			size, MinUDPReadBufferSize, MaxUDPPayloadSize, currentUDPReadBufferSize.Load())
		return
	}

	newSize := int32(size)
	currentVal := currentUDPReadBufferSize.Load()

	if currentVal != newSize {
		swapped := currentUDPReadBufferSize.CompareAndSwap(currentVal, newSize)
		if swapped {
			log.Infof("Effective UDP read buffer size changed from %d to %d bytes.", currentVal, newSize)
		} else {
			log.Warnf("Concurrent update detected while attempting to change UDP buffer size from %d to %d. Value might have already been updated.", currentVal, newSize)
		}
	} else {
		log.Debugf("Effective UDP read buffer size remains %d bytes.", newSize)
	}
}

var udpBufferPool = sync.Pool{
	New: func() any {
		size := int(currentUDPReadBufferSize.Load())
		b := make([]byte, size)
		return &b
	},
}

var packetBufferPool = sync.Pool{
	New: func() any {
		return new(packetBuffer)
	},
}

type packetBuffer struct {
	Data       []byte
	N          int
	RemoteAddr *net.UDPAddr
	buffer     []byte
}

func getPacketBuffer() *packetBuffer {
	pb := packetBufferPool.Get().(*packetBuffer)
	bufPtr := udpBufferPool.Get().(*[]byte)

	currentSize := int(currentUDPReadBufferSize.Load())

	if bufPtr != nil && cap(*bufPtr) == currentSize {
		pb.buffer = *bufPtr
		log.Tracef("getPacketBuffer: Reusing buffer from pool with correct capacity %d.", currentSize)
	} else {
		if bufPtr == nil {
			log.Warnf("getPacketBuffer: retrieved nil buffer pointer from pool! Allocating new one with current size %d.", currentSize)
		} else {
			log.Debugf("getPacketBuffer: Discarding buffer from pool with old capacity %d, allocating new with current size %d.", cap(*bufPtr), currentSize)
		}

		pb.buffer = make([]byte, currentSize)
	}

	pb.Data = pb.buffer[:cap(pb.buffer)]
	pb.N = 0
	pb.RemoteAddr = nil

	return pb
}

func putPacketBuffer(pb *packetBuffer) {
	if pb.buffer != nil {
		udpBufferPool.Put(&pb.buffer)
	} else {
		// log.Warnf("putPacketBuffer: pb.buffer is already nil for pb: %+v", pb)
	}

	pb.Data = nil
	pb.RemoteAddr = nil
	pb.N = 0

	packetBufferPool.Put(pb)
}
