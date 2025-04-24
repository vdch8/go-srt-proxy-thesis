package main

import (
	"context"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/datarhei/gosrt/packet"
	"github.com/sirupsen/logrus"
	"gopkg.in/natefinch/lumberjack.v2"
)

var log = logrus.New()

const (
	defaultLogTimestampFormat = "2006-01-02 15:04:05.000"
	defaultSRTLogChannelCap   = 8192
	srtHeaderSize             = 16
	srtLogActiveLevel         = logrus.DebugLevel
)

type SRTLogMsg struct {
	Timestamp    time.Time
	RouteName    string
	RemoteAddr   *net.UDPAddr
	LocalAddrStr string
	Data         []byte
	Sending      bool
}

var (
	srtLogChanValue    atomic.Value
	srtLogWorkerMutex  sync.Mutex
	srtLogWorkerCancel context.CancelFunc = func() {}
	srtLogChannelSize  int                = defaultSRTLogChannelCap
	logTimestampFormat string             = defaultLogTimestampFormat
)

type ConsoleHook struct {
	Writer    io.Writer
	LogLevels []logrus.Level
	Formatter logrus.Formatter
}

func (hook *ConsoleHook) Fire(entry *logrus.Entry) error {
	line, err := hook.Formatter.Format(entry)
	if err != nil {
		fmt.Fprintf(os.Stderr, "ConsoleHook: Unable to format log entry: %v\n", err)
		return err
	}
	_, err = hook.Writer.Write(line)
	if err != nil {
		fmt.Fprintf(os.Stderr, "ConsoleHook: Failed to write log entry: %v\n", err)
		return err
	}
	return nil
}

func (hook *ConsoleHook) Levels() []logrus.Level {
	return hook.LogLevels
}

type FileHook struct {
	Writer    io.Writer
	LogLevels []logrus.Level
	Formatter logrus.Formatter
}

func (hook *FileHook) Fire(entry *logrus.Entry) error {
	line, err := hook.Formatter.Format(entry)
	if err != nil {
		fmt.Fprintf(os.Stderr, "FileHook: Unable to format log entry for file: %v\n", err)
		return err
	}
	_, err = hook.Writer.Write(line)
	if err != nil {
		fmt.Fprintf(os.Stderr, "FileHook: Failed to write log entry to file: %v\n", err)
		return err
	}
	return nil
}

func (hook *FileHook) Levels() []logrus.Level {
	return hook.LogLevels
}

func SetupLogger(cfg *Config, appCtx context.Context) {
	consoleLevel := logrus.InfoLevel
	fileLevel := logrus.InfoLevel
	var highestLevel logrus.Level = logrus.PanicLevel

	logCfg := cfg.LogSettings
	if logCfg == nil {
		fmt.Fprintln(os.Stderr, "FATAL: LogSettings is nil during SetupLogger call. Check LoadConfig.")
		os.Exit(1)
	}

	logTimestampFormat = defaultLogTimestampFormat
	if logCfg.TimestampFormat != "" {
		logTimestampFormat = logCfg.TimestampFormat
	}

	consoleFormatter := &logrus.TextFormatter{
		FullTimestamp:   true,
		TimestampFormat: logTimestampFormat,
		PadLevelText:    true,
		ForceColors:     false,
		DisableColors:   false,
	}
	fileFormatter := &logrus.TextFormatter{
		FullTimestamp:   true,
		TimestampFormat: logTimestampFormat,
		PadLevelText:    true,
		DisableColors:   true,
	}

	if lvl, err := logrus.ParseLevel(strings.ToLower(logCfg.ConsoleLevel)); err == nil {
		consoleLevel = lvl
	} else {
		fmt.Fprintf(os.Stderr, "WARN: Invalid log_settings.console_level '%s': %v. Using level '%s'.\n",
			logCfg.ConsoleLevel, err, consoleLevel.String())
	}
	if consoleLevel > highestLevel {
		highestLevel = consoleLevel
	}

	fileEnabled := logCfg.File != nil && logCfg.File.Enabled
	var fileLogger *lumberjack.Logger
	var fileLevels []logrus.Level

	if fileEnabled {
		fileSettings := logCfg.File
		if fileSettings.Level != "" {
			if lvl, err := logrus.ParseLevel(strings.ToLower(fileSettings.Level)); err == nil {
				fileLevel = lvl
			} else {
				fmt.Fprintf(os.Stderr, "WARN: Invalid log_settings.file.level '%s': %v. Using level '%s' for file.\n",
					fileSettings.Level, err, fileLevel.String())
			}
		} else {
			fileLevel = consoleLevel
			fmt.Fprintf(os.Stderr, "INFO: log_settings.file.level not set. Inheriting level '%s' from console.\n", fileLevel.String())
		}

		if fileLevel > highestLevel {
			highestLevel = fileLevel
		}

		if fileSettings.Path == "" {
			fmt.Fprintln(os.Stderr, "ERROR: File logging enabled but log_settings.file.path is empty. Disabling file logging.")
			fileEnabled = false
		} else {
			logDir := filepath.Dir(fileSettings.Path)
			if err := os.MkdirAll(logDir, 0755); err != nil {
				fmt.Fprintf(os.Stderr, "ERROR: Failed to create log directory '%s': %v. Disabling file logging.\n", logDir, err)
				fileEnabled = false
			} else {
				fileLogger = &lumberjack.Logger{
					Filename:   fileSettings.Path,
					MaxSize:    fileSettings.MaxSize,
					MaxAge:     fileSettings.MaxAge,
					MaxBackups: fileSettings.MaxBackups,
					LocalTime:  true,
					Compress:   fileSettings.Compress,
				}
				fileLevels = make([]logrus.Level, 0, fileLevel+1)
				for _, level := range logrus.AllLevels {
					if level <= fileLevel {
						fileLevels = append(fileLevels, level)
					}
				}
			}
		}
	}

	if logCfg.SRTLogBufSize != nil && *logCfg.SRTLogBufSize > 0 {
		srtLogChannelSize = *logCfg.SRTLogBufSize
	} else {
		srtLogChannelSize = defaultSRTLogChannelCap
	}

	if srtLogActiveLevel > highestLevel {
		highestLevel = srtLogActiveLevel
	}

	log.SetLevel(highestLevel)
	log.SetOutput(io.Discard)
	log.ReplaceHooks(make(logrus.LevelHooks))

	consoleLevels := make([]logrus.Level, 0, consoleLevel+1)
	for _, level := range logrus.AllLevels {
		if level <= consoleLevel {
			consoleLevels = append(consoleLevels, level)
		}
	}
	log.AddHook(&ConsoleHook{
		Writer:    os.Stderr,
		LogLevels: consoleLevels,
		Formatter: consoleFormatter,
	})
	log.Infof("Console logging level set to: %s", consoleLevel.String())
	if logTimestampFormat != defaultLogTimestampFormat {
		log.Debugf("Using custom timestamp format: %s", logTimestampFormat)
	}

	if fileEnabled && fileLogger != nil {
		log.AddHook(&FileHook{
			Writer:    fileLogger,
			LogLevels: fileLevels,
			Formatter: fileFormatter,
		})
		log.Infof("File logging enabled. Path: %s, Level: %s, MaxSize: %dMB, MaxAge: %d days, MaxBackups: %d, Compress: %t",
			fileLogger.Filename, fileLevel.String(), fileLogger.MaxSize, fileLogger.MaxAge, fileLogger.MaxBackups, fileLogger.Compress)
	} else {
		log.Info("File logging is disabled.")
	}

	if srtLogChannelSize != defaultSRTLogChannelCap {
		log.Debugf("SRT log buffer size configured to: %d", srtLogChannelSize)
	} else {
		log.Debugf("Using default SRT log buffer size: %d", defaultSRTLogChannelCap)
	}

	srtLogWorkerMutex.Lock()
	defer srtLogWorkerMutex.Unlock()

	shouldRunSRTWorker := highestLevel >= srtLogActiveLevel
	currentChanRaw := srtLogChanValue.Load()
	var currentChan chan SRTLogMsg
	if currentChanRaw != nil {
		ch, ok := currentChanRaw.(chan SRTLogMsg)
		if ok {
			currentChan = ch
		} else {
			log.Errorf("BUG: srtLogChanValue contained unexpected type: %T", currentChanRaw)
			srtLogChanValue.Store(nil)
		}
	}

	if shouldRunSRTWorker {
		if currentChan == nil || cap(currentChan) != srtLogChannelSize {
			if currentChan != nil {
				log.Debugf("Closing old SRT log channel (capacity %d).", cap(currentChan))
				close(currentChan)
			}
			log.Debugf("Creating new SRT log channel with capacity %d.", srtLogChannelSize)
			newChan := make(chan SRTLogMsg, srtLogChannelSize)
			srtLogChanValue.Store(newChan)
			currentChan = newChan

			log.Debugf("Starting SRT log worker...")
			var srtCtx context.Context
			srtCtx, srtLogWorkerCancel = context.WithCancel(appCtx)
			go srtLogWorker(srtCtx, currentChan)

		} else {
			log.Debugf("SRT log worker should be running with existing channel (capacity %d).", cap(currentChan))
		}
	} else {
		if currentChan != nil {
			log.Infof("Log level (%s < %s) is below threshold for SRT logging. Stopping SRT log worker...",
				highestLevel.String(), srtLogActiveLevel.String())
			srtLogWorkerCancel()
			srtLogChanValue.Store(nil)
		} else {
			log.Debugf("SRT log worker already stopped (no active channel).")
		}
	}

	log.Debug("Logger setup complete.")
}

func srtLogWorker(ctx context.Context, logChannel chan SRTLogMsg) {
	log.Debug("Asynchronous SRT log worker starting...")

	defer func() {
		log.Debug("Asynchronous SRT log worker stopped.")
	}()

	for {
		select {
		case msg, ok := <-logChannel:
			if !ok {
				log.Warn("SRT log channel was closed. Exiting worker.")
				return
			}
			processSRTLogMessage(msg)

		case <-ctx.Done():
			log.Info("SRT log worker received cancellation signal. Draining channel...")
		DrainLoop:
			for {
				select {
				case msg, ok := <-logChannel:
					if !ok {
						log.Warn("SRT log channel closed while draining.")
						break DrainLoop
					}
					processSRTLogMessage(msg)
				default:
					log.Debug("SRT log channel drained.")
					break DrainLoop
				}
			}
			return
		}
	}
}

func processSRTLogMessage(msg SRTLogMsg) {
	currentLogLevel := log.GetLevel()
	if currentLogLevel < srtLogActiveLevel {
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
	isTraceLevel := currentLogLevel >= logrus.TraceLevel

	if hdr.IsControlPacket {
		techDetails := ""
		if isTraceLevel {
			techDetails = fmt.Sprintf("(Ts: %d, DstSockID: %#08x, Size: %d)", hdr.Timestamp, hdr.DestinationSocketId, len(msg.Data))
		}

		switch hdr.ControlType {
		case packet.CTRLTYPE_HANDSHAKE:
			var handshakeCIF packet.CIFHandshake
			cifErr := srtPkt.UnmarshalCIF(&handshakeCIF)
			handshakeTypeStr := "HANDSHAKE (Unknown Type)"
			if cifErr == nil {
				handshakeTypeStr = fmt.Sprintf("HANDSHAKE(%s)", handshakeCIF.HandshakeType.String())
				if isTraceLevel {
					techDetails += fmt.Sprintf(" (HS Ver: %d, SynCookie: %#08x, SockID: %#08x, InitSeq: %d)",
						handshakeCIF.Version, handshakeCIF.SynCookie, handshakeCIF.SRTSocketId, handshakeCIF.InitialPacketSequenceNumber.Val())
				}
			} else if isTraceLevel {
				log.Tracef("%s Failed to unmarshal HANDSHAKE CIF: %v", logPrefix, cifErr)
			}
			logMsg = fmt.Sprintf("%s %s SRT Control %s %s %s via %s",
				logPrefix, direction, handshakeTypeStr, targetRelation, msg.RemoteAddr.String(), msg.LocalAddrStr)
			log.Debug(logMsg)
			if isTraceLevel {
				log.Tracef("%s %s", logMsg, techDetails)
			}

		case packet.CTRLTYPE_SHUTDOWN:
			logMsg = fmt.Sprintf("%s %s SRT Control SHUTDOWN %s %s via %s",
				logPrefix, direction, targetRelation, msg.RemoteAddr.String(), msg.LocalAddrStr)
			log.Debug(logMsg)
			if isTraceLevel {
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

			if cifErr == nil && isTraceLevel {
				techDetails += fmt.Sprintf(" (LastACKPkt: %d, RTT: %dms, BufAvail: %d)",
					ackCIF.LastACKPacketSequenceNumber.Val(), ackCIF.RTT/1000, ackCIF.AvailableBufferSize)
			} else if cifErr != nil && isTraceLevel {
				log.Tracef("%s Failed to unmarshal ACK CIF: %v", logPrefix, cifErr)
			}

			logMsg = fmt.Sprintf("%s %s SRT Control %s %s %s via %s",
				logPrefix, direction, ackTypeStr, targetRelation, msg.RemoteAddr.String(), msg.LocalAddrStr)
			log.Debug(logMsg)
			if isTraceLevel {
				log.Tracef("%s %s", logMsg, techDetails)
			}

		case packet.CTRLTYPE_NAK:
			var nakCIF packet.CIFNAK
			cifErr := srtPkt.UnmarshalCIF(&nakCIF)

			logMsg = fmt.Sprintf("%s %s SRT Control NAK %s %s via %s",
				logPrefix, direction, targetRelation, msg.RemoteAddr.String(), msg.LocalAddrStr)
			log.Debug(logMsg)

			if cifErr == nil {

				if isTraceLevel {
					lostListStr := "Lost Ranges: "

					seqNums := nakCIF.LostPacketSequenceNumber
					if len(seqNums)%2 == 0 {
						for i := 0; i < len(seqNums); i += 2 {
							start, end := seqNums[i], seqNums[i+1]
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

						lostListStr += "[invalid_data]"
						log.Warnf("%s NAK CIF contains odd number of sequence numbers after successful unmarshal: %d", logPrefix, len(seqNums))
					}
					techDetails += " (" + lostListStr + ")"
					log.Tracef("%s %s", logMsg, techDetails)
				}
			} else {
				if isTraceLevel {
					log.Tracef("%s Failed to unmarshal NAK CIF: %v", logPrefix, cifErr)
					log.Tracef("%s %s", logMsg, techDetails)
				}
			}

		default:
			logMsg = fmt.Sprintf("%s %s SRT Control(Type:%s", logPrefix, direction, hdr.ControlType)
			if hdr.SubType != packet.CTRLSUBTYPE_NONE {
				logMsg += fmt.Sprintf("/SubType:%s", hdr.SubType)
			}
			logMsg += fmt.Sprintf(") %s %s via %s", targetRelation, msg.RemoteAddr.String(), msg.LocalAddrStr)
			log.Debug(logMsg)
			if isTraceLevel {
				log.Tracef("%s %s", logMsg, techDetails)
			}
		}
	} else {
		if isTraceLevel {
			techDetails := fmt.Sprintf("(Seq: %d, Ts: %d, DstSockID: %#08x, Size: %d)",
				hdr.PacketSequenceNumber.Val(), hdr.Timestamp, hdr.DestinationSocketId, len(msg.Data))
			logMsg = fmt.Sprintf("%s %s SRT Data        %s %s via %s",
				logPrefix, direction, targetRelation, msg.RemoteAddr.String(), msg.LocalAddrStr)
			log.Tracef("%s %s", logMsg, techDetails)
		}
	}
}

func logSRTPacket(remoteAddr *net.UDPAddr, data []byte, sending bool, routeName string, localAddrStr string) {
	if log.GetLevel() < srtLogActiveLevel {
		return
	}

	currentChanRaw := srtLogChanValue.Load()
	if currentChanRaw == nil {
		return
	}

	currentChannel, ok := currentChanRaw.(chan SRTLogMsg)
	if !ok {
		log.Errorf("BUG: logSRTPacket found unexpected type in srtLogChanValue: %T", currentChanRaw)
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

	select {
	case currentChannel <- logMsg:

	default:

	}
}
