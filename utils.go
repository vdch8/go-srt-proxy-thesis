package main

import (
	"fmt"
	"net"
	"time"

	"github.com/datarhei/gosrt/packet"
	"github.com/sirupsen/logrus"
)

const DefaultFlowTimeout = 60 * time.Second

const srtHeaderSize = 16

func parseTimeout(routeTimeoutStr, globalDefaultTimeoutStr string) time.Duration {
	var effectiveDefault time.Duration

	if globalDefaultTimeoutStr != "" {
		if d, err := time.ParseDuration(globalDefaultTimeoutStr); err == nil {
			if d > 0 {
				effectiveDefault = d
			} else {
				log.Warnf("Global default flow_timeout '%s' is not positive, using absolute default %v.", globalDefaultTimeoutStr, DefaultFlowTimeout)
				effectiveDefault = DefaultFlowTimeout
			}
		} else {
			log.Warnf("Invalid global default flow_timeout '%s', using absolute default %v. Error: %v", globalDefaultTimeoutStr, DefaultFlowTimeout, err)
			effectiveDefault = DefaultFlowTimeout
		}
	} else {
		effectiveDefault = DefaultFlowTimeout
	}

	var routeTimeout time.Duration
	if routeTimeoutStr != "" {
		if d, err := time.ParseDuration(routeTimeoutStr); err == nil {
			if d > 0 {
				routeTimeout = d
			} else {
				if log.GetLevel() >= logrus.WarnLevel {
					log.Warnf("Route-specific flow_timeout '%s' is not positive, using effective default %v for this route.", routeTimeoutStr, effectiveDefault)
				}
				routeTimeout = effectiveDefault
			}
		} else {
			if log.GetLevel() >= logrus.WarnLevel {
				log.Warnf("Invalid route-specific flow_timeout '%s', using effective default %v for this route. Error: %v", routeTimeoutStr, effectiveDefault, err)
			}
			routeTimeout = effectiveDefault
		}
	} else {
		routeTimeout = effectiveDefault
	}

	return routeTimeout
}

func logSRTPacket(addr *net.UDPAddr, data []byte, sending bool, routeName string, sockInfo string) {
	currentLogLevel := log.GetLevel()

	if currentLogLevel < logrus.DebugLevel {
		return
	}

	direction := "Received"
	targetRelation := "from"
	if sending {
		direction = "Sending"
		targetRelation = "to"
	}

	logPrefix := fmt.Sprintf("Route '%s':", routeName)
	logBase := fmt.Sprintf("%s %s UDP packet (size %d bytes) %s %s via %s",
		logPrefix, direction, len(data), targetRelation, addr.String(), sockInfo)

	if len(data) < srtHeaderSize {
		log.Debugf("%s (Too small for SRT header)", logBase)
		return
	}

	srtPkt := packet.NewPacket(nil)
	defer srtPkt.Decommission()

	err := srtPkt.Unmarshal(data)
	if err != nil {
		log.Debugf("%s (Failed to unmarshal SRT header: %v)", logBase, err)
		return
	}

	hdr := srtPkt.Header()
	var logMsg string

	if hdr.IsControlPacket {
		techDetails := fmt.Sprintf("(Ts: %d, DstSockID: %#08x, Size: %d)", hdr.Timestamp, hdr.DestinationSocketId, len(data))

		switch hdr.ControlType {
		case packet.CTRLTYPE_HANDSHAKE:
			var handshakeCIF packet.CIFHandshake
			cifErr := srtPkt.UnmarshalCIF(&handshakeCIF)

			handshakeTypeStr := "HANDSHAKE (Unknown Type)"
			if cifErr == nil {
				handshakeTypeStr = fmt.Sprintf("HANDSHAKE(%s)", handshakeCIF.HandshakeType.String())
				if currentLogLevel >= logrus.TraceLevel {
					techDetails += fmt.Sprintf(" (HS Ver: %d, SynCookie: %#08x)", handshakeCIF.Version, handshakeCIF.SynCookie)
				}
			} else {
				log.Debugf("%s Failed to unmarshal HANDSHAKE CIF: %v", logPrefix, cifErr)
			}

			logMsg = fmt.Sprintf("%s %s SRT Control %s %s %s via %s",
				logPrefix, direction, handshakeTypeStr, targetRelation, addr.String(), sockInfo)

			log.Debugf(logMsg)
			if currentLogLevel >= logrus.TraceLevel && logMsg != "" {
				log.Tracef("%s %s", logMsg, techDetails)
			}

		case packet.CTRLTYPE_SHUTDOWN:
			var actionDesc string
			if sending {
				actionDesc = fmt.Sprintf("Forwarding connection SHUTDOWN request to %s", addr.String())
			} else {
				actionDesc = fmt.Sprintf("Received connection SHUTDOWN initiated by %s", addr.String())
			}

			logMsg = fmt.Sprintf("%s %s via %s", logPrefix, actionDesc, sockInfo)

			log.Debug(logMsg)
			if currentLogLevel >= logrus.TraceLevel && logMsg != "" {
				log.Tracef("%s %s", logMsg, techDetails)
			}

		default:
			logMsg = fmt.Sprintf("%s %s SRT Control(%s", logPrefix, direction, hdr.ControlType)
			if hdr.SubType != packet.CTRLSUBTYPE_NONE {
				logMsg += fmt.Sprintf("/%s", hdr.SubType)
			}
			logMsg += fmt.Sprintf(") %s %s via %s", targetRelation, addr.String(), sockInfo)

			if currentLogLevel >= logrus.TraceLevel {
				log.Tracef("%s %s", logMsg, techDetails)
			}
		}

	} else {
		if currentLogLevel >= logrus.TraceLevel {
			techDetails := fmt.Sprintf("(Seq: %d, Ts: %d, DstSockID: %#08x, Size: %d)",
				hdr.PacketSequenceNumber.Val(), hdr.Timestamp, hdr.DestinationSocketId, len(data))
			logMsg = fmt.Sprintf("%s %s SRT Data %s %s via %s",
				logPrefix, direction, targetRelation, addr.String(), sockInfo)
			log.Tracef("%s %s", logMsg, techDetails)
		}
	}
}
