package main

import (
	"context"
	"fmt"
	"net"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/datarhei/gosrt/packet"
	"github.com/sirupsen/logrus"
)

var log = logrus.New()

const (
	logTimestampFormat = "2006-01-02 15:04:05.000"

	srtLogChannelCapacity = 8192
	srtHeaderSize         = 16
)

type SRTLogMsg struct {
	Timestamp    time.Time
	RouteName    string
	RemoteAddr   *net.UDPAddr
	LocalAddrStr string
	Data         []byte
	Sending      bool
}

var srtLogChan chan SRTLogMsg

var srtLogWorkerRunning bool
var srtLogWorkerMutex sync.Mutex

func srtLogWorker(ctx context.Context) {
	log.Debug("Starting asynchronous SRT log worker...")
	srtLogWorkerMutex.Lock()
	srtLogWorkerRunning = true
	srtLogWorkerMutex.Unlock()

	defer func() {
		srtLogWorkerMutex.Lock()
		srtLogWorkerRunning = false
		srtLogWorkerMutex.Unlock()
		log.Debug("Asynchronous SRT log worker stopped.")
	}()

	for {
		select {
		case msg, ok := <-srtLogChan:
			if !ok {
				log.Warn("SRT log channel closed.")
				return
			}

			processSRTLogMessage(msg)

		case <-ctx.Done():
			log.Info("SRT log worker stopping due to context cancellation.")

			log.Debug("SRT log worker draining remaining messages...")
		DrainLoop:
			for {
				select {
				case msg, ok := <-srtLogChan:
					if !ok {
						log.Warn("SRT log channel closed while draining.")
						break DrainLoop
					}
					processSRTLogMessage(msg)
				default:
					break DrainLoop
				}
			}
			log.Debug("SRT log worker finished draining messages.")
			return
		}
	}
}

func processSRTLogMessage(msg SRTLogMsg) {
	currentLogLevel := log.GetLevel()
	if currentLogLevel < logrus.DebugLevel {
		return
	}

	direction := "Received"
	targetRelation := "from"
	if msg.Sending {
		direction = "Sending "
		targetRelation = "to  "
	}
	logPrefix := fmt.Sprintf("Route '%s':", msg.RouteName)
	logBase := fmt.Sprintf("%s %s UDP packet (size %d) %s %s via %s",
		logPrefix, direction, len(msg.Data), targetRelation, msg.RemoteAddr.String(), msg.LocalAddrStr)

	if len(msg.Data) < srtHeaderSize {
		log.Debugf("%s (Too small for SRT header)", logBase)
		return
	}

	srtPkt := packet.NewPacket(nil)
	err := srtPkt.Unmarshal(msg.Data)
	if err != nil {
		log.Debugf("%s (Failed to unmarshal SRT header: %v)", logBase, err)
		srtPkt.Decommission()
		return
	}

	defer srtPkt.Decommission()

	hdr := srtPkt.Header()
	var logMsg string

	if hdr.IsControlPacket {
		techDetails := fmt.Sprintf("(Ts: %d, DstSockID: %#08x, Size: %d)", hdr.Timestamp, hdr.DestinationSocketId, len(msg.Data))

		switch hdr.ControlType {
		case packet.CTRLTYPE_HANDSHAKE:
			var handshakeCIF packet.CIFHandshake
			cifErr := srtPkt.UnmarshalCIF(&handshakeCIF)
			handshakeTypeStr := "HANDSHAKE (Unknown Type)"
			if cifErr == nil {
				handshakeTypeStr = fmt.Sprintf("HANDSHAKE(%s)", handshakeCIF.HandshakeType.String())
				if currentLogLevel >= logrus.TraceLevel {

					techDetails += fmt.Sprintf(" (HS Ver: %d, SynCookie: %#08x, SockID: %#08x, InitSeq: %d)", handshakeCIF.Version, handshakeCIF.SynCookie, handshakeCIF.SRTSocketId, handshakeCIF.InitialPacketSequenceNumber.Val())
				}
			} else {
				log.Tracef("%s Failed to unmarshal HANDSHAKE CIF: %v", logPrefix, cifErr)
			}
			logMsg = fmt.Sprintf("%s %s SRT Control %s %s %s via %s",
				logPrefix, direction, handshakeTypeStr, targetRelation, msg.RemoteAddr.String(), msg.LocalAddrStr)
			log.Debugf(logMsg)
			if currentLogLevel >= logrus.TraceLevel {
				log.Tracef("%s %s", logMsg, techDetails)
			}

		case packet.CTRLTYPE_SHUTDOWN:
			logMsg = fmt.Sprintf("%s %s SRT Control SHUTDOWN %s %s via %s",
				logPrefix, direction, targetRelation, msg.RemoteAddr.String(), msg.LocalAddrStr)
			log.Debug(logMsg)
			if currentLogLevel >= logrus.TraceLevel {
				log.Tracef("%s %s", logMsg, techDetails)
			}

		case packet.CTRLTYPE_KEEPALIVE:
			logMsg = fmt.Sprintf("%s %s SRT Control KEEPALIVE %s %s via %s",
				logPrefix, direction, targetRelation, msg.RemoteAddr.String(), msg.LocalAddrStr)
			log.Trace(logMsg)

		case packet.CTRLTYPE_ACK:
			var ackCIF packet.CIFACK
			cifErr := srtPkt.UnmarshalCIF(&ackCIF)
			ackTypeStr := "ACK"

			if cifErr == nil {

				if currentLogLevel >= logrus.TraceLevel {

					techDetails += fmt.Sprintf(" (LastACKPkt: %d, RTT: %dms, BufAvail: %d)", ackCIF.LastACKPacketSequenceNumber.Val(), ackCIF.RTT/1000, ackCIF.AvailableBufferSize)
				}
			} else {
				log.Tracef("%s Failed to unmarshal ACK CIF: %v", logPrefix, cifErr)
			}

			logMsg = fmt.Sprintf("%s %s SRT Control %s %s %s via %s",
				logPrefix, direction, ackTypeStr, targetRelation, msg.RemoteAddr.String(), msg.LocalAddrStr)
			log.Debug(logMsg)
			if currentLogLevel >= logrus.TraceLevel {
				log.Tracef("%s %s", logMsg, techDetails)
			}

		case packet.CTRLTYPE_NAK:
			var nakCIF packet.CIFNAK
			cifErr := srtPkt.UnmarshalCIF(&nakCIF)

			logMsg = fmt.Sprintf("%s %s SRT Control NAK %s %s via %s",
				logPrefix, direction, targetRelation, msg.RemoteAddr.String(), msg.LocalAddrStr)
			log.Debug(logMsg)

			if cifErr == nil {

				if currentLogLevel >= logrus.TraceLevel {
					lostListStr := "Lost Ranges: "

					if len(nakCIF.LostPacketSequenceNumber)%2 == 0 {
						for i := 0; i < len(nakCIF.LostPacketSequenceNumber); i += 2 {
							start := nakCIF.LostPacketSequenceNumber[i]
							end := nakCIF.LostPacketSequenceNumber[i+1]
							if i > 0 {
								lostListStr += ", "
							}
							if start.Equals(end) {
								lostListStr += fmt.Sprintf("%d", start.Val())
							} else {
								lostListStr += fmt.Sprintf("%d-%d", start.Val(), end.Val())
							}
						}
					} else {

						lostListStr += "invalid_data (odd number of elements)"
						log.Warnf("%s NAK CIF contains odd number of sequence numbers after successful unmarshal: %d", logPrefix, len(nakCIF.LostPacketSequenceNumber))
					}
					techDetails += " (" + lostListStr + ")"
					log.Tracef("%s %s", logMsg, techDetails)
				}
			} else {

				log.Tracef("%s Failed to unmarshal NAK CIF: %v", logPrefix, cifErr)

				if currentLogLevel >= logrus.TraceLevel {
					log.Tracef("%s %s", logMsg, techDetails)
				}
			}

		default:
			logMsg = fmt.Sprintf("%s %s SRT Control(%s", logPrefix, direction, hdr.ControlType)
			if hdr.SubType != packet.CTRLSUBTYPE_NONE {
				logMsg += fmt.Sprintf("/%s", hdr.SubType)
			}
			logMsg += fmt.Sprintf(") %s %s via %s", targetRelation, msg.RemoteAddr.String(), msg.LocalAddrStr)
			if currentLogLevel >= logrus.TraceLevel {
				log.Tracef("%s %s", logMsg, techDetails)
			} else {
				log.Debug(logMsg)
			}
		}

	} else {
		if currentLogLevel >= logrus.TraceLevel {
			techDetails := fmt.Sprintf("(Seq: %d, Ts: %d, DstSockID: %#08x, Size: %d)",
				hdr.PacketSequenceNumber.Val(), hdr.Timestamp, hdr.DestinationSocketId, len(msg.Data))
			logMsg = fmt.Sprintf("%s %s SRT Data        %s %s via %s",
				logPrefix, direction, targetRelation, msg.RemoteAddr.String(), msg.LocalAddrStr)
			log.Tracef("%s %s", logMsg, techDetails)
		}
	}

}

func SetupLogger(levelStr string, ctx context.Context) {
	log.SetOutput(os.Stderr)
	log.SetFormatter(&logrus.TextFormatter{
		FullTimestamp:   true,
		TimestampFormat: logTimestampFormat,
		PadLevelText:    true,
		ForceColors:     true,
		DisableColors:   false,
	})

	logLevel := logrus.InfoLevel
	effectiveLevelStr := logLevel.String()

	if levelStr != "" {
		parsedLevel, err := logrus.ParseLevel(strings.ToLower(levelStr))
		if err != nil {
			log.Warnf("Invalid log_level '%s' provided: %v. Using default level '%s'.",
				levelStr, err, effectiveLevelStr)
		} else {
			logLevel = parsedLevel
			effectiveLevelStr = logLevel.String()
			log.Infof("Log level set to '%s' based on configuration.", effectiveLevelStr)
		}
	} else {
		log.Infof("No log_level specified. Using default level '%s'.", effectiveLevelStr)
	}

	log.SetLevel(logLevel)
	log.Debugf("Logger initialization complete. Effective log level: %s", log.GetLevel().String())

	srtLogWorkerMutex.Lock()
	defer srtLogWorkerMutex.Unlock()

	if logLevel >= logrus.DebugLevel {
		if !srtLogWorkerRunning {
			if srtLogChan == nil {
				srtLogChan = make(chan SRTLogMsg, srtLogChannelCapacity)
				log.Debugf("Initialized SRT log channel with capacity %d.", srtLogChannelCapacity)
			}
			go srtLogWorker(ctx)
		} else {
			log.Debug("SRT log worker already running.")
		}
	} else {
		if srtLogWorkerRunning {
			log.Info("Disabling SRT packet logging as level is below DEBUG.")

			if srtLogChan != nil {

				select {
				case _, ok := <-srtLogChan:
					if ok {

						log.Warn("Closing SRT log channel while possibly not empty due to log level change.")
					}
				default:

				}

				currentChan := srtLogChan
				srtLogChan = nil
				if currentChan != nil {
					close(currentChan)
				}
			}

		} else {
			log.Debug("SRT packet logging remains disabled.")
		}
	}
}

func logSRTPacket(remoteAddr *net.UDPAddr, data []byte, sending bool, routeName string, localAddrStr string) {

	currentChan := srtLogChan
	if log.GetLevel() < logrus.DebugLevel || currentChan == nil {
		return
	}

	dataCopy := make([]byte, len(data))
	copy(dataCopy, data)

	logMsg := SRTLogMsg{
		Timestamp:    time.Now(),
		RouteName:    routeName,
		RemoteAddr:   remoteAddr,
		LocalAddrStr: localAddrStr,
		Data:         dataCopy,
		Sending:      sending,
	}

	defer func() {
		if r := recover(); r != nil {

			log.Warnf("Recovered from panic writing to SRT log channel (likely closed): %v", r)
		}
	}()

	select {
	case currentChan <- logMsg:

	default:

	}
}
