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

func init() { currentUDPReadBufferSize.Store(DefaultUDPReadBufferSize) }

func SetEffectiveUDPReadBufferSize(size int) {
	if size < MinUDPReadBufferSize || size > MaxUDPPayloadSize {
		log.Errorf("SetEffectiveUDPReadBufferSize: invalid %d (allowed %d–%d)",
			size, MinUDPReadBufferSize, MaxUDPPayloadSize)
		return
	}
	currentUDPReadBufferSize.Store(int32(size))
}

type packetBuffer struct {
	Data       []byte
	N          int
	RemoteAddr *net.UDPAddr
}

var packetPool = sync.Pool{
	New: func() any {
		sz := int(currentUDPReadBufferSize.Load())
		return &packetBuffer{Data: make([]byte, sz)}
	},
}

func getPacketBuffer() *packetBuffer {
	pb := packetPool.Get().(*packetBuffer)
	sz := int(currentUDPReadBufferSize.Load())
	if cap(pb.Data) != sz {
		pb.Data = make([]byte, sz)
	}
	pb.N, pb.RemoteAddr = 0, nil
	return pb
}

func putPacketBuffer(pb *packetBuffer) { packetPool.Put(pb) }
