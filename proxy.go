package main

import (
	"context"
	"errors"
	"fmt"
	"net"
	"sync"
	"time"

	"github.com/sirupsen/logrus"
)

const (
	udpNetwork = "udp"
)

const (
	DefaultRouteWorkers                   = 1
	MinRouteWorkers                       = 1
	MaxRouteWorkers                       = 128
	AbsoluteDefaultPacketChanSize         = 1024
	AbsoluteDefaultMinPacketChanSize      = 64
	AbsoluteDefaultMaxPacketChanSize      = 8192
	AbsoluteDefaultListenSocketBufferSize = 16777216
	AbsoluteDefaultClientSocketBufferSize = 212992
)

const (
	AbsoluteDefaultCleanerInterval = 60 * time.Second
)

type ProxyServer struct {
	activeRoutes map[string]*ActiveRoute
	routesLock   sync.RWMutex
	globalCtx    context.Context

	cleanerWg         sync.WaitGroup
	cleanerInterval   time.Duration
	cleanerConfigLock sync.RWMutex

	effectiveMinChanSize int
	effectiveMaxChanSize int
	chanSizeLimitsLock   sync.RWMutex
}

type resolvedRoute struct {
	routeCfg                StreamRoute
	listenAddr              *net.UDPAddr
	sourceAddr              *net.UDPAddr
	timeout                 time.Duration
	workers                 int
	chanSize                int
	listenReadBuffer        int
	listenWriteBuffer       int
	clientReadBuffer        int
	clientWriteBuffer       int
	listenReadBufferSource  string
	listenWriteBufferSource string
	clientReadBufferSource  string
	clientWriteBufferSource string
}

func resolveWorkerCount(routeCfg StreamRoute, defaults StreamRoute, routeName string) int {
	workers := DefaultRouteWorkers
	configSource := "absolute default"

	if defaults.Workers != nil {
		workers = *defaults.Workers
		configSource = "'defaults' section"
		log.Tracef("Route '%s': Using default worker count %d from %s", routeName, workers, configSource)
	}

	if routeCfg.Workers != nil {
		workers = *routeCfg.Workers
		configSource = "route-specific setting"
		log.Tracef("Route '%s': Using specific worker count %d from %s", routeName, workers, configSource)
	}

	appliedLimits := false
	if workers < MinRouteWorkers {
		log.Warnf("Route '%s': Requested worker count %d (from %s) is below minimum %d, using minimum.",
			routeName, workers, configSource, MinRouteWorkers)
		workers = MinRouteWorkers
		appliedLimits = true
	}
	if workers > MaxRouteWorkers {
		log.Warnf("Route '%s': Requested worker count %d (from %s) exceeds maximum %d, using maximum.",
			routeName, workers, configSource, MaxRouteWorkers)
		workers = MaxRouteWorkers
		appliedLimits = true
	}

	if !appliedLimits && configSource != "absolute default" {
		log.Debugf("Route '%s': Final worker count set to %d (from %s, within limits [%d, %d])",
			routeName, workers, configSource, MinRouteWorkers, MaxRouteWorkers)
	} else if configSource == "absolute default" {
		log.Debugf("Route '%s': Using absolute default worker count: %d", routeName, workers)
	} else if appliedLimits {
		log.Debugf("Route '%s': Final worker count clamped to %d (within limits [%d, %d])",
			routeName, workers, MinRouteWorkers, MaxRouteWorkers)
	}

	return workers
}

func (p *ProxyServer) resolveChannelSize(routeCfg StreamRoute, defaults StreamRoute, routeName string) int {
	chanSize := AbsoluteDefaultPacketChanSize
	configSource := "absolute default"

	if defaults.ChanSize != nil {
		chanSize = *defaults.ChanSize
		configSource = "'defaults' section"
		log.Tracef("Route '%s': Using default channel size %d from %s", routeName, chanSize, configSource)
	}
	if routeCfg.ChanSize != nil {
		chanSize = *routeCfg.ChanSize
		configSource = "route-specific setting"
		log.Tracef("Route '%s': Using specific channel size %d from %s", routeName, chanSize, configSource)
	}

	p.chanSizeLimitsLock.RLock()
	effectiveMin := p.effectiveMinChanSize
	effectiveMax := p.effectiveMaxChanSize
	p.chanSizeLimitsLock.RUnlock()

	appliedMinMax := false
	if chanSize < effectiveMin {
		log.Warnf("Route '%s': Configured channel size %d (from %s) is below effective minimum %d, using minimum.",
			routeName, chanSize, configSource, effectiveMin)
		chanSize = effectiveMin
		appliedMinMax = true
	}
	if chanSize > effectiveMax {
		log.Warnf("Route '%s': Configured channel size %d (from %s) exceeds effective maximum %d, using maximum.",
			routeName, chanSize, configSource, effectiveMax)
		chanSize = effectiveMax
		appliedMinMax = true
	}

	if !appliedMinMax && configSource != "absolute default" {
		log.Debugf("Route '%s': Final channel size set to %d (from %s, within effective limits [%d, %d])",
			routeName, chanSize, configSource, effectiveMin, effectiveMax)
	} else if configSource == "absolute default" {
		log.Debugf("Route '%s': Using absolute default channel size: %d (within effective limits [%d, %d])",
			routeName, chanSize, effectiveMin, effectiveMax)
	} else if appliedMinMax {
		log.Debugf("Route '%s': Final channel size clamped to %d (within effective limits [%d, %d])",
			routeName, chanSize, effectiveMin, effectiveMax)
	}

	return chanSize
}

func resolveSocketBuffer(routeName, bufferName string, routeValue *int, defaultValue *int, absoluteDefault int) (size int, source string) {
	value := absoluteDefault
	configSource := "absolute default"

	if defaultValue != nil {
		if *defaultValue > 0 {
			value = *defaultValue
			configSource = "'defaults' section"
			log.Debugf("Route '%s': Using default %s %d bytes from %s", routeName, bufferName, value, configSource)
		} else {
			log.Debugf("Route '%s': Ignoring non-positive default %s %d, keeping %s: %d bytes",
				routeName, bufferName, *defaultValue, configSource, value)
		}
	}

	if routeValue != nil {
		if *routeValue > 0 {
			value = *routeValue
			configSource = "route-specific setting"
			log.Debugf("Route '%s': Using specific %s %d bytes from %s", routeName, bufferName, value, configSource)
		} else {
			log.Debugf("Route '%s': Ignoring non-positive route-specific %s %d, keeping value from %s: %d bytes",
				routeName, bufferName, *routeValue, configSource, value)
		}
	}

	if value <= 0 {
		log.Debugf("Route '%s': Resolved %s is %d (non-positive), letting OS use its default. Source was '%s'.", routeName, bufferName, value, configSource)
		return 0, configSource
	}

	return value, configSource
}

func NewProxyServer(parentCtx context.Context) *ProxyServer {
	log.Debug("Creating new Proxy Server")
	p := &ProxyServer{
		activeRoutes:         make(map[string]*ActiveRoute),
		globalCtx:            parentCtx,
		cleanerInterval:      AbsoluteDefaultCleanerInterval,
		effectiveMinChanSize: AbsoluteDefaultMinPacketChanSize,
		effectiveMaxChanSize: AbsoluteDefaultMaxPacketChanSize,
	}
	return p
}

func (p *ProxyServer) Start(cfg *Config) error {
	log.Info("Initializing proxy server...")

	p.updateCleanerSettings(cfg.Cleaner)
	p.updateChannelSizeLimits(cfg)
	p.resolveAndUpdateUDPReadBufferSize(cfg)

	p.routesLock.Lock()
	defer p.routesLock.Unlock()

	log.Debugf("Starting proxy server with %d stream definitions", len(cfg.Streams))

	log.Debug("Validating and resolving routes from configuration...")
	resolvedRoutes, hasInvalidRoute := p.validateAndResolveNewConfig(cfg)
	if hasInvalidRoute {

		log.Warn("Start: Found invalid route configurations. Proceeding without them.")
	}

	log.Debugf("Attempting to start %d resolved routes...", len(resolvedRoutes))
	hasFailures := false
	for listenAddrStr, resRoute := range resolvedRoutes {
		err := p.startRouteLocked(
			resRoute.routeCfg.Name,
			resRoute.listenAddr,
			resRoute.sourceAddr,
			resRoute.timeout,
			resRoute.workers,
			resRoute.chanSize,
			resRoute.listenReadBuffer, resRoute.listenReadBufferSource,
			resRoute.listenWriteBuffer, resRoute.listenWriteBufferSource,
			resRoute.clientReadBuffer,
			resRoute.clientWriteBuffer,
		)
		if err != nil {
			log.Errorf("Start: Failed to start route '%s' (%s -> %s): %v",
				resRoute.routeCfg.Name, listenAddrStr, resRoute.sourceAddr.String(), err)
			hasFailures = true
		}
	}

	if hasFailures {
		log.Warn("One or more routes failed to start during initial setup. Server continues with active routes.")
	} else if len(resolvedRoutes) > 0 {
		log.Infof("Successfully started %d initial routes.", len(resolvedRoutes))
	} else {
		log.Warn("No valid routes were configured or started.")
	}

	log.Debug("Starting flow cleaner goroutine...")
	p.cleanerWg.Add(1)
	go p.flowCleanerLoop()

	log.Info("Proxy server initialization complete.")
	return nil
}

func (p *ProxyServer) startRouteLocked(
	routeName string,
	listenAddr *net.UDPAddr,
	sourceAddr *net.UDPAddr,
	timeout time.Duration,
	workers int,
	chanSize int,
	listenReadBuf int, listenReadSrc string,
	listenWriteBuf int, listenWriteSrc string,
	clientReadBuf int,
	clientWriteBuf int,
) error {
	listenAddrStr := listenAddr.String()
	sourceAddrStr := sourceAddr.String()

	if _, exists := p.activeRoutes[listenAddrStr]; exists {
		log.Warnf("startRouteLocked: Route '%s' for %s already exists, skipping duplicate start.", routeName, listenAddrStr)
		return fmt.Errorf("route for %s already exists", listenAddrStr)
	}

	listener, err := net.ListenUDP(udpNetwork, listenAddr)
	if err != nil {
		return fmt.Errorf("listen on %s failed for route '%s': %w", listenAddrStr, routeName, err)
	}

	listenBufLog := ""

	finalListenReadBuf := 0
	finalListenWriteBuf := 0

	if listenReadBuf > 0 {
		err := listener.SetReadBuffer(listenReadBuf)
		if err != nil {
			log.Debugf("Route '%s' (%s): Failed to set listener read buffer to %d bytes (requested from %s): %v. OS default will be used.",
				routeName, listenAddrStr, listenReadBuf, listenReadSrc, err)
			listenBufLog += fmt.Sprintf(" ReadBufErr(req:%d(%s)):%v", listenReadBuf, listenReadSrc, err)
		} else {
			log.Debugf("Route '%s' (%s): Successfully applied listener read buffer size %d bytes (from %s).",
				routeName, listenAddrStr, listenReadBuf, listenReadSrc)
			listenBufLog += fmt.Sprintf(" ReadBuf:%d(%s)", listenReadBuf, listenReadSrc)
			finalListenReadBuf = listenReadBuf
		}
	} else {
		log.Debugf("Route '%s' (%s): Using OS default listener read buffer (requested from %s).",
			routeName, listenAddrStr, listenReadSrc)
		listenBufLog += fmt.Sprintf(" ReadBuf:OSDef(%s)", listenReadSrc)
	}

	if listenWriteBuf > 0 {
		err := listener.SetWriteBuffer(listenWriteBuf)
		if err != nil {
			log.Debugf("Route '%s' (%s): Failed to set listener write buffer to %d bytes (requested from %s): %v. OS default will be used.",
				routeName, listenAddrStr, listenWriteBuf, listenWriteSrc, err)
			listenBufLog += fmt.Sprintf(" WriteBufErr(req:%d(%s)):%v", listenWriteBuf, listenWriteSrc, err)
		} else {
			log.Debugf("Route '%s' (%s): Successfully applied listener write buffer size %d bytes (from %s).",
				routeName, listenAddrStr, listenWriteBuf, listenWriteSrc)
			listenBufLog += fmt.Sprintf(" WriteBuf:%d(%s)", listenWriteBuf, listenWriteSrc)
			finalListenWriteBuf = listenWriteBuf
		}
	} else {
		log.Debugf("Route '%s' (%s): Using OS default listener write buffer (requested from %s).",
			routeName, listenAddrStr, listenWriteSrc)
		listenBufLog += fmt.Sprintf(" WriteBuf:OSDef(%s)", listenWriteSrc)
	}

	log.Infof("Route '%s': Listening on %s, forwarding new flows to %s (Timeout: %v, Workers: %d, ChanSize: %d, Buffer:%s)",
		routeName, listenAddrStr, sourceAddrStr, timeout, workers, chanSize, listenBufLog)

	ctx, cancel := context.WithCancel(p.globalCtx)

	log.Debugf("Route '%s': Initializing with %d workers and channel size %d", routeName, workers, chanSize)

	activeRoute := &ActiveRoute{
		routeName:         routeName,
		listenAddr:        listenAddr,
		listener:          listener,
		targetSource:      sourceAddr,
		flowTimeout:       timeout,
		ctx:               ctx,
		cancel:            cancel,
		numWorkers:        workers,
		packetChan:        make(chan *packetBuffer, chanSize),
		clientFlows:       sync.Map{},
		listenReadBuffer:  finalListenReadBuf,
		listenWriteBuffer: finalListenWriteBuf,
		clientReadBuffer:  clientReadBuf,
		clientWriteBuffer: clientWriteBuf,
	}

	p.activeRoutes[listenAddrStr] = activeRoute

	activeRoute.startProcessing()

	return nil
}

func (p *ProxyServer) Reload(newCfg *Config) error {
	log.Info("Starting configuration reload process...")

	p.updateCleanerSettings(newCfg.Cleaner)
	p.updateChannelSizeLimits(newCfg)
	p.resolveAndUpdateUDPReadBufferSize(newCfg)

	p.routesLock.Lock()
	defer p.routesLock.Unlock()

	log.Debug("Validating and resolving new configuration...")
	resolvedRoutes, hasInvalidNewRoute := p.validateAndResolveNewConfig(newCfg)
	if hasInvalidNewRoute {
		log.Warn("Reload: Found invalid route configurations in the new config. Proceeding with valid ones.")
	}
	log.Debugf("Finished resolving new configuration. Found %d valid routes.", len(resolvedRoutes))

	stopFailures := p.stopRemovedRoutesLocked(resolvedRoutes)

	processFailures := p.processNewAndUpdatedRoutesLocked(resolvedRoutes)

	log.Info("Configuration reload process finished.")
	if hasInvalidNewRoute || stopFailures || processFailures {
		errMsg := "configuration reload completed with errors:"
		if hasInvalidNewRoute {
			errMsg += " invalid route definitions found;"
		}
		if stopFailures {
			errMsg += " failed to stop one or more removed routes;"
		}
		if processFailures {
			errMsg += " failed to start/update/restart one or more routes;"
		}

		log.Error(errMsg)

	}

	return nil
}

func (p *ProxyServer) resolveAndUpdateUDPReadBufferSize(cfg *Config) {
	requestedSize := DefaultUDPReadBufferSize
	configSource := "default"

	if cfg != nil && cfg.UDPReadBufferSize != nil {
		configSource = "configuration"
		requestedSize = *cfg.UDPReadBufferSize
		log.Tracef("Found udp_read_buffer_size: %d in %s.", requestedSize, configSource)
	} else {
		log.Tracef("Using default udp_read_buffer_size: %d.", requestedSize)
	}

	effectiveSize := requestedSize
	appliedLimits := false

	if effectiveSize < MinUDPReadBufferSize {
		log.Warnf("Configured udp_read_buffer_size %d (from %s) is below minimum %d. Using minimum.",
			requestedSize, configSource, MinUDPReadBufferSize)
		effectiveSize = MinUDPReadBufferSize
		appliedLimits = true
	}

	if effectiveSize > MaxUDPPayloadSize {
		log.Warnf("Configured udp_read_buffer_size %d (from %s) exceeds maximum %d. Using maximum.",
			requestedSize, configSource, MaxUDPPayloadSize)
		effectiveSize = MaxUDPPayloadSize
		appliedLimits = true
	}

	if !appliedLimits && configSource == "configuration" {
		log.Debugf("Setting effective UDP read buffer size to %d bytes (from %s).", effectiveSize, configSource)
	} else if appliedLimits {
		log.Debugf("Setting effective UDP read buffer size to %d bytes (clamped from %d, requested in %s).", effectiveSize, requestedSize, configSource)
	} else {
		log.Debugf("Setting effective UDP read buffer size to %d bytes (default).", effectiveSize)
	}

	SetEffectiveUDPReadBufferSize(effectiveSize)
}

func (p *ProxyServer) validateAndResolveNewConfig(cfg *Config) (map[string]resolvedRoute, bool) {
	newRouteConfigs := make(map[string]resolvedRoute)
	hasInvalidRoute := false
	uniqueListenAddrs := make(map[string]string)

	defaultsConfig := cfg.Defaults

	for i, routeCfg := range cfg.Streams {

		if routeCfg.Name == "" || routeCfg.ListenAddress == "" || routeCfg.SourceAddress == "" {
			log.Errorf("Config Validation: Skipping route #%d due to empty required fields: Name='%s', Listen='%s', Source='%s'",
				i+1, routeCfg.Name, routeCfg.ListenAddress, routeCfg.SourceAddress)
			hasInvalidRoute = true
			continue
		}

		if existingName, found := uniqueListenAddrs[routeCfg.ListenAddress]; found {
			log.Errorf("Config Validation: Duplicate ListenAddress '%s' for routes '%s' and '%s' in new config. Skipping route '%s'.",
				routeCfg.ListenAddress, existingName, routeCfg.Name, routeCfg.Name)
			hasInvalidRoute = true
			continue
		}

		listenAddr, err := net.ResolveUDPAddr(udpNetwork, routeCfg.ListenAddress)
		if err != nil {
			log.Errorf("Config Validation: Invalid ListenAddress '%s' for route '%s': %v. Skipping.",
				routeCfg.ListenAddress, routeCfg.Name, err)
			hasInvalidRoute = true
			continue
		}
		sourceAddr, err := net.ResolveUDPAddr(udpNetwork, routeCfg.SourceAddress)
		if err != nil {
			log.Errorf("Config Validation: Invalid SourceAddress '%s' for route '%s': %v. Skipping.",
				routeCfg.SourceAddress, routeCfg.Name, err)
			hasInvalidRoute = true
			continue
		}

		timeout := parseTimeout(routeCfg.FlowTimeout, defaultsConfig.FlowTimeout)
		workers := resolveWorkerCount(routeCfg, defaultsConfig, routeCfg.Name)
		chanSize := p.resolveChannelSize(routeCfg, defaultsConfig, routeCfg.Name)

		listenReadBuf, listenReadSrc := resolveSocketBuffer(routeCfg.Name, "listen_read_buffer",
			routeCfg.ListenReadBuffer, defaultsConfig.ListenReadBuffer, AbsoluteDefaultListenSocketBufferSize)
		listenWriteBuf, listenWriteSrc := resolveSocketBuffer(routeCfg.Name, "listen_write_buffer",
			routeCfg.ListenWriteBuffer, defaultsConfig.ListenWriteBuffer, AbsoluteDefaultListenSocketBufferSize)
		clientReadBuf, clientReadSrc := resolveSocketBuffer(routeCfg.Name, "client_read_buffer",
			routeCfg.ClientReadBuffer, defaultsConfig.ClientReadBuffer, AbsoluteDefaultClientSocketBufferSize)
		clientWriteBuf, clientWriteSrc := resolveSocketBuffer(routeCfg.Name, "client_write_buffer",
			routeCfg.ClientWriteBuffer, defaultsConfig.ClientWriteBuffer, AbsoluteDefaultClientSocketBufferSize)

		resRoute := resolvedRoute{
			routeCfg:                routeCfg,
			listenAddr:              listenAddr,
			sourceAddr:              sourceAddr,
			timeout:                 timeout,
			workers:                 workers,
			chanSize:                chanSize,
			listenReadBuffer:        listenReadBuf,
			listenWriteBuffer:       listenWriteBuf,
			clientReadBuffer:        clientReadBuf,
			clientWriteBuffer:       clientWriteBuf,
			listenReadBufferSource:  listenReadSrc,
			listenWriteBufferSource: listenWriteSrc,
			clientReadBufferSource:  clientReadSrc,
			clientWriteBufferSource: clientWriteSrc,
		}

		newRouteConfigs[listenAddr.String()] = resRoute
		uniqueListenAddrs[routeCfg.ListenAddress] = routeCfg.Name

		log.Debugf("Config Validation: Resolved route '%s': Listen=%s Source=%s Timeout=%v Workers=%d ChanSize=%d ListenBuf(R:%d(%s)/W:%d(%s)) ClientBuf(R:%d(%s)/W:%d(%s))",
			routeCfg.Name, listenAddr.String(), sourceAddr.String(), timeout, workers, chanSize,
			listenReadBuf, listenReadSrc, listenWriteBuf, listenWriteSrc,
			clientReadBuf, clientReadSrc, clientWriteBuf, clientWriteSrc)
	}
	return newRouteConfigs, hasInvalidRoute
}

func (p *ProxyServer) stopRemovedRoutesLocked(newRouteConfigs map[string]resolvedRoute) bool {
	routesToStop := []*ActiveRoute{}
	currentRouteAddrs := make([]string, 0, len(p.activeRoutes))
	for listenAddrStr := range p.activeRoutes {
		currentRouteAddrs = append(currentRouteAddrs, listenAddrStr)
	}

	for _, listenAddrStr := range currentRouteAddrs {
		if _, existsInNew := newRouteConfigs[listenAddrStr]; !existsInNew {
			if activeRoute, ok := p.activeRoutes[listenAddrStr]; ok {
				activeRoute.configLock.RLock()
				routeName := activeRoute.routeName
				activeRoute.configLock.RUnlock()
				log.Infof("Reload: Marking route '%s' (%s) for stopping (removed from config).",
					routeName, listenAddrStr)
				routesToStop = append(routesToStop, activeRoute)

				delete(p.activeRoutes, listenAddrStr)
			} else {
				log.Warnf("Reload: Route %s was expected in activeRoutes but not found during removal check.", listenAddrStr)
			}
		}
	}

	hasStopFailures := false
	if len(routesToStop) > 0 {
		var stopWg sync.WaitGroup
		stopWg.Add(len(routesToStop))
		log.Infof("Reload: Stopping %d removed routes...", len(routesToStop))
		for _, route := range routesToStop {
			go func(r *ActiveRoute) {
				defer stopWg.Done()
				r.Stop()
			}(route)
		}
		stopWg.Wait()
		log.Infof("Reload: Finished stopping %d removed routes.", len(routesToStop))
	} else {
		log.Debug("Reload: No routes need to be stopped.")
	}
	return hasStopFailures
}

func (p *ProxyServer) processNewAndUpdatedRoutesLocked(newRouteConfigs map[string]resolvedRoute) bool {
	log.Debug("Processing new and updated routes...")
	hasFailures := false

	for listenAddrStr, newRouteData := range newRouteConfigs {
		activeRoute, exists := p.activeRoutes[listenAddrStr]

		if exists {
			activeRoute.configLock.RLock()
			oldWorkers := activeRoute.numWorkers
			oldName := activeRoute.routeName

			activeRoute.configLock.RUnlock()

			log.Debugf("Reload: Route '%s' (%s) exists, checking for updates (New: Name='%s' Source=%s, Workers=%d, Timeout=%v, ChanSize=%d, LBuf=%d/%d, CBuf=%d/%d)",
				oldName, listenAddrStr, newRouteData.routeCfg.Name, newRouteData.sourceAddr.String(), newRouteData.workers, newRouteData.timeout, newRouteData.chanSize,
				newRouteData.listenReadBuffer, newRouteData.listenWriteBuffer, newRouteData.clientReadBuffer, newRouteData.clientWriteBuffer)

			resolvedNewWorkers := newRouteData.workers
			workersCountChanged := oldWorkers != resolvedNewWorkers

			requiresRestart := false
			restartReason := ""

			if workersCountChanged && newRouteData.routeCfg.Workers != nil {
				requiresRestart = true
				restartReason = fmt.Sprintf("Explicit workers setting changed (%d -> %d). ", oldWorkers, resolvedNewWorkers)

				log.Infof("Reload: Route '%s' (%s) requires RESTART due to explicit worker count change.", oldName, listenAddrStr)
			} else if workersCountChanged {

				log.Warnf("Reload: Route '%s' (%s): Effective worker count changed (%d -> %d) due to 'defaults.workers'. "+
					"Change will NOT be applied immediately to prevent disruption. Route continues with %d workers.",
					oldName, listenAddrStr, oldWorkers, resolvedNewWorkers, oldWorkers)
			}

			if requiresRestart {
				log.Infof("Reload: %sRoute '%s' (%s) is being RESTARTED.", restartReason, oldName, listenAddrStr)

				activeRoute.Stop()
				delete(p.activeRoutes, listenAddrStr)

				log.Infof("Reload: Restarting route '%s' with new config (Listen: %s, Source: %s, Workers: %d, ChanSize: %d, Timeout: %v, LBuf=%d/%d, CBuf=%d/%d)",
					newRouteData.routeCfg.Name, listenAddrStr, newRouteData.sourceAddr.String(), resolvedNewWorkers, newRouteData.chanSize, newRouteData.timeout,
					newRouteData.listenReadBuffer, newRouteData.listenWriteBuffer, newRouteData.clientReadBuffer, newRouteData.clientWriteBuffer)

				err := p.startRouteLocked(
					newRouteData.routeCfg.Name,
					newRouteData.listenAddr,
					newRouteData.sourceAddr,
					newRouteData.timeout,
					resolvedNewWorkers,
					newRouteData.chanSize,
					newRouteData.listenReadBuffer, newRouteData.listenReadBufferSource,
					newRouteData.listenWriteBuffer, newRouteData.listenWriteBufferSource,
					newRouteData.clientReadBuffer,
					newRouteData.clientWriteBuffer,
				)
				if err != nil {
					log.Errorf("Reload: Failed to RESTART route '%s': %v", newRouteData.routeCfg.Name, err)
					hasFailures = true
				} else {
					log.Infof("Reload: Route '%s' restarted successfully with %d workers.", newRouteData.routeCfg.Name, resolvedNewWorkers)
				}
			} else {
				log.Debugf("Reload: Applying updates in-place for route '%s' (%s).", oldName, listenAddrStr)
				p.updateRouteInPlaceLocked(activeRoute, newRouteData)
			}
		} else {
			log.Infof("Reload: Starting new route '%s' (Listen: %s, Source: %s, Workers: %d, ChanSize: %d, Timeout: %v, LBuf=%d/%d, CBuf=%d/%d)",
				newRouteData.routeCfg.Name, listenAddrStr, newRouteData.sourceAddr.String(), newRouteData.workers, newRouteData.chanSize, newRouteData.timeout,
				newRouteData.listenReadBuffer, newRouteData.listenWriteBuffer, newRouteData.clientReadBuffer, newRouteData.clientWriteBuffer)
			err := p.startRouteLocked(
				newRouteData.routeCfg.Name,
				newRouteData.listenAddr,
				newRouteData.sourceAddr,
				newRouteData.timeout,
				newRouteData.workers,
				newRouteData.chanSize,
				newRouteData.listenReadBuffer, newRouteData.listenReadBufferSource,
				newRouteData.listenWriteBuffer, newRouteData.listenWriteBufferSource,
				newRouteData.clientReadBuffer,
				newRouteData.clientWriteBuffer,
			)
			if err != nil {
				log.Errorf("Reload: Failed to start new route '%s': %v", newRouteData.routeCfg.Name, err)
				hasFailures = true
			} else {
				log.Infof("Reload: New route '%s' started successfully.", newRouteData.routeCfg.Name)
			}
		}
	}

	if log.GetLevel() >= logrus.DebugLevel {
		finalActive := make([]string, 0, len(p.activeRoutes))
		for k, v := range p.activeRoutes {
			v.configLock.RLock()
			rName := v.routeName
			rWorkers := v.numWorkers
			v.configLock.RUnlock()
			finalActive = append(finalActive, fmt.Sprintf("'%s'(%s, %dw)", rName, k, rWorkers))
		}
		log.Debugf("Reload: Final active routes after processing: %v", finalActive)
	}

	return hasFailures
}

func (p *ProxyServer) updateRouteInPlaceLocked(activeRoute *ActiveRoute, newRouteData resolvedRoute) {
	activeRoute.configLock.Lock()
	defer activeRoute.configLock.Unlock()

	oldName := activeRoute.routeName
	oldTimeout := activeRoute.flowTimeout
	oldTargetSource := activeRoute.targetSource
	listenAddrStr := activeRoute.listenAddr.String()

	nameChanged := oldName != newRouteData.routeCfg.Name
	timeoutChanged := oldTimeout != newRouteData.timeout
	targetSourceChanged := oldTargetSource.String() != newRouteData.sourceAddr.String()

	updatedFieldsLog := ""
	if nameChanged {
		activeRoute.routeName = newRouteData.routeCfg.Name
		updatedFieldsLog += fmt.Sprintf(" Name: '%s'->'%s'", oldName, activeRoute.routeName)
	}
	if timeoutChanged {
		activeRoute.flowTimeout = newRouteData.timeout
		updatedFieldsLog += fmt.Sprintf(" Timeout: %v->%v (applies dynamically)", oldTimeout, activeRoute.flowTimeout)
	}
	if targetSourceChanged {
		activeRoute.targetSource = newRouteData.sourceAddr
		updatedFieldsLog += fmt.Sprintf(" TargetSource: '%s'->'%s' (for new flows)", oldTargetSource.String(), activeRoute.targetSource.String())
	}

	listener := activeRoute.listener
	oldListenReadBuf := activeRoute.listenReadBuffer
	oldListenWriteBuf := activeRoute.listenWriteBuffer
	newListenReadBuf := newRouteData.listenReadBuffer
	newListenReadSrc := newRouteData.listenReadBufferSource
	newListenWriteBuf := newRouteData.listenWriteBuffer
	newListenWriteSrc := newRouteData.listenWriteBufferSource

	listenerBufChanged := false

	if listener != nil && oldListenReadBuf != newListenReadBuf {
		listenerBufChanged = true
		log.Debugf("Reload: Route '%s' (%s): Attempting to update listener read buffer from %d to %d (requested from %s)",
			activeRoute.routeName, listenAddrStr, oldListenReadBuf, newListenReadBuf, newListenReadSrc)
		if newListenReadBuf > 0 {
			err := listener.SetReadBuffer(newListenReadBuf)
			if err != nil {
				log.Debugf("Reload: Route '%s' (%s): Failed to update listener read buffer to %d (requested from %s): %v. Previous value %d remains.",
					activeRoute.routeName, listenAddrStr, newListenReadBuf, newListenReadSrc, err, oldListenReadBuf)
				updatedFieldsLog += fmt.Sprintf(" ListenReadBufErr(req:%d(%s)):%v", newListenReadBuf, newListenReadSrc, err)
			} else {
				log.Debugf("Reload: Route '%s' (%s): Successfully applied listener read buffer update to %d (from %s).",
					activeRoute.routeName, listenAddrStr, newListenReadBuf, newListenReadSrc)
				activeRoute.listenReadBuffer = newListenReadBuf
				updatedFieldsLog += fmt.Sprintf(" ListenReadBuf:%d(%s)", newListenReadBuf, newListenReadSrc)
			}
		} else {
			log.Debugf("Reload: Route '%s' (%s): Requested listener read buffer set to OS default (requested from %s). Previous was %d.",
				activeRoute.routeName, listenAddrStr, newListenReadSrc, oldListenReadBuf)
			activeRoute.listenReadBuffer = 0
			updatedFieldsLog += fmt.Sprintf(" ListenReadBuf:OSDef(%s)", newListenReadSrc)
		}
	}

	if listener != nil && oldListenWriteBuf != newListenWriteBuf {
		listenerBufChanged = true
		log.Debugf("Reload: Route '%s' (%s): Attempting to update listener write buffer from %d to %d (requested from %s)",
			activeRoute.routeName, listenAddrStr, oldListenWriteBuf, newListenWriteBuf, newListenWriteSrc)
		if newListenWriteBuf > 0 {
			err := listener.SetWriteBuffer(newListenWriteBuf)
			if err != nil {
				log.Debugf("Reload: Route '%s' (%s): Failed to update listener write buffer to %d (requested from %s): %v. Previous value %d remains.",
					activeRoute.routeName, listenAddrStr, newListenWriteBuf, newListenWriteSrc, err, oldListenWriteBuf)
				updatedFieldsLog += fmt.Sprintf(" ListenWriteBufErr(req:%d(%s)):%v", newListenWriteBuf, newListenWriteSrc, err)
			} else {
				log.Debugf("Reload: Route '%s' (%s): Successfully applied listener write buffer update to %d (from %s).",
					activeRoute.routeName, listenAddrStr, newListenWriteBuf, newListenWriteSrc)
				activeRoute.listenWriteBuffer = newListenWriteBuf
				updatedFieldsLog += fmt.Sprintf(" ListenWriteBuf:%d(%s)", newListenWriteBuf, newListenWriteSrc)
			}
		} else {
			log.Debugf("Reload: Route '%s' (%s): Requested listener write buffer set to OS default (requested from %s). Previous was %d.",
				activeRoute.routeName, listenAddrStr, newListenWriteSrc, oldListenWriteBuf)
			activeRoute.listenWriteBuffer = 0
			updatedFieldsLog += fmt.Sprintf(" ListenWriteBuf:OSDef(%s)", newListenWriteSrc)
		}
	}

	if listenerBufChanged {
		log.Debugf("Reload: Route '%s' (%s): Listener buffer update check complete.", activeRoute.routeName, listenAddrStr)
	}

	oldClientReadBuf := activeRoute.clientReadBuffer
	oldClientWriteBuf := activeRoute.clientWriteBuffer
	newClientReadBuf := newRouteData.clientReadBuffer
	newClientWriteBuf := newRouteData.clientWriteBuffer

	clientReadBufChanged := oldClientReadBuf != newClientReadBuf
	clientWriteBufChanged := oldClientWriteBuf != newClientWriteBuf

	if clientReadBufChanged {
		activeRoute.clientReadBuffer = newClientReadBuf
		updatedFieldsLog += fmt.Sprintf(" ClientReadBufTemplate:%d", activeRoute.clientReadBuffer)
	}
	if clientWriteBufChanged {
		activeRoute.clientWriteBuffer = newClientWriteBuf
		updatedFieldsLog += fmt.Sprintf(" ClientWriteBufTemplate:%d", activeRoute.clientWriteBuffer)
	}

	if clientReadBufChanged || clientWriteBufChanged {
		log.Debugf("Reload: Route '%s' (%s): Client buffer templates updated.", activeRoute.routeName, listenAddrStr)
	}

	if updatedFieldsLog != "" {
		log.Infof("Reload: Updated existing route '%s' (%s) in-place:%s", activeRoute.routeName, listenAddrStr, updatedFieldsLog)
	} else {
		log.Debugf("Reload: Route '%s' (%s) configuration requires no in-place update (Name, Timeout, TargetSource, ListenBuf, ClientBufTemplate) and no restart (Workers).",
			activeRoute.routeName, listenAddrStr)
	}

	if clientReadBufChanged || clientWriteBufChanged {
		applyClientBufferUpdatesToFlows(activeRoute, newClientReadBuf, newClientWriteBuf, clientReadBufChanged, clientWriteBufChanged)
	} else {
		log.Debugf("Reload: Route '%s' (%s): Skipping application of client buffers to flows as templates did not change.",
			activeRoute.routeName, listenAddrStr)
	}
}

func (p *ProxyServer) updateCleanerSettings(settings *CleanerSettings) {
	newInterval := AbsoluteDefaultCleanerInterval
	configSource := "absolute default"

	if settings != nil && settings.Interval != "" {
		parsedInterval, err := time.ParseDuration(settings.Interval)
		if err == nil {
			if parsedInterval > 0 {
				newInterval = parsedInterval
				configSource = "configuration file"
			} else {
				log.Warnf("Invalid cleaner interval '%s' (%v) in config is not positive. Using previous/default value %v.",
					settings.Interval, parsedInterval, p.cleanerInterval)
				newInterval = p.cleanerInterval
				configSource = "previous value due to invalid config"
			}
		} else {
			log.Warnf("Invalid cleaner interval duration format '%s': %v. Using previous/default value %v.",
				settings.Interval, err, p.cleanerInterval)
			newInterval = p.cleanerInterval
			configSource = "previous value due to parse error"
		}
	}

	p.cleanerConfigLock.Lock()
	updated := p.cleanerInterval != newInterval
	oldInterval := p.cleanerInterval
	p.cleanerInterval = newInterval
	p.cleanerConfigLock.Unlock()

	if updated {
		log.Infof("Cleaner interval updated (source: %s): %v -> %v",
			configSource, oldInterval, newInterval)
	} else {
		log.Debugf("Cleaner interval remains unchanged: %v (source: %s)",
			newInterval, configSource)
	}
}

func (p *ProxyServer) updateChannelSizeLimits(cfg *Config) {
	minSize := AbsoluteDefaultMinPacketChanSize
	maxSize := AbsoluteDefaultMaxPacketChanSize
	minSource := "absolute default"
	maxSource := "absolute default"

	if cfg.MinChannelSize != nil {
		if *cfg.MinChannelSize > 0 {
			minSize = *cfg.MinChannelSize
			minSource = "configuration"
			log.Tracef("Using min_channel_size %d from configuration.", minSize)
		} else {
			log.Warnf("Configured min_channel_size (%d) is not positive. Using absolute default %d.", *cfg.MinChannelSize, AbsoluteDefaultMinPacketChanSize)
		}
	} else {
		log.Tracef("Using absolute default min_channel_size %d.", minSize)
	}

	if cfg.MaxChannelSize != nil {
		if *cfg.MaxChannelSize > 0 {
			maxSize = *cfg.MaxChannelSize
			maxSource = "configuration"
			log.Tracef("Using max_channel_size %d from configuration.", maxSize)
		} else {
			log.Warnf("Configured max_channel_size (%d) is not positive. Using absolute default %d.", *cfg.MaxChannelSize, AbsoluteDefaultMaxPacketChanSize)
		}
	} else {
		log.Tracef("Using absolute default max_channel_size %d.", maxSize)
	}

	if minSize > maxSize {
		log.Warnf("Effective min_channel_size (%d from %s) is greater than effective max_channel_size (%d from %s). Clamping max_channel_size = min_channel_size.", minSize, minSource, maxSize, maxSource)
		maxSize = minSize
		maxSource = fmt.Sprintf("%s (clamped to min)", maxSource)
	}

	p.chanSizeLimitsLock.Lock()
	updated := p.effectiveMinChanSize != minSize || p.effectiveMaxChanSize != maxSize
	p.effectiveMinChanSize = minSize
	p.effectiveMaxChanSize = maxSize
	p.chanSizeLimitsLock.Unlock()

	if updated {
		log.Infof("Effective channel size limits updated: Min=%d (from %s), Max=%d (from %s)", minSize, minSource, maxSize, maxSource)
	} else {
		log.Debugf("Effective channel size limits remain unchanged: Min=%d (from %s), Max=%d (from %s)", minSize, minSource, maxSize, maxSource)
	}
}

func (p *ProxyServer) Stop() {
	log.Info("Initiating proxy server shutdown...")

	p.routesLock.Lock()
	routesToStop := make([]*ActiveRoute, 0, len(p.activeRoutes))
	routeDescriptions := make([]string, 0, len(p.activeRoutes))
	for k, v := range p.activeRoutes {
		routesToStop = append(routesToStop, v)
		v.configLock.RLock()
		routeDescriptions = append(routeDescriptions, fmt.Sprintf("'%s'(%s)", v.routeName, k))
		v.configLock.RUnlock()
	}
	p.activeRoutes = make(map[string]*ActiveRoute)
	p.routesLock.Unlock()

	count := len(routesToStop)
	if count > 0 {
		log.Infof("Stopping %d active routes: %v...", count, routeDescriptions)
		var wg sync.WaitGroup
		wg.Add(count)
		for _, route := range routesToStop {
			route.configLock.RLock()
			rName := route.routeName
			rAddr := "unknown"
			if route.listenAddr != nil {
				rAddr = route.listenAddr.String()
			}
			route.configLock.RUnlock()
			log.Debugf("Triggering stop for route '%s' (%s)", rName, rAddr)

			go func(r *ActiveRoute, name string, addr string) {
				defer wg.Done()
				r.Stop()
				log.Debugf("Stop completed for route '%s' (%s)", name, addr)
			}(route, rName, rAddr)
		}
		wg.Wait()
		log.Infof("Finished stopping all %d routes.", count)
	} else {
		log.Info("No active routes to stop.")
	}

	log.Info("Waiting for flow cleaner routine to finish...")
	p.cleanerWg.Wait()
	log.Info("Flow cleaner finished.")

	log.Info("Proxy server shutdown complete.")
}

func applyClientBufferUpdatesToFlows(ar *ActiveRoute, newReadBuf, newWriteBuf int, readChanged, writeChanged bool) {

	routeName := ar.routeName
	listenAddrStr := "unknown"
	if ar.listenAddr != nil {
		listenAddrStr = ar.listenAddr.String()
	}

	applyCount := 0
	failCount := 0
	flowCount := 0

	log.Debugf("Reload: Route '%s' (%s): Applying client buffer updates (R:%d, W:%d, readChanged:%t, writeChanged:%t) to existing flows...",
		routeName, listenAddrStr, newReadBuf, newWriteBuf, readChanged, writeChanged)

	ar.clientFlows.Range(func(key, value any) bool {
		flowCount++
		clientKey := key.(string)
		flow, ok := value.(*ClientFlow)
		if !ok {
			log.Warnf("Reload/BufferApply: Found unexpected type (%T) in clientFlows map for key %s on route '%s'. Skipping.", value, clientKey, routeName)
			return true
		}

		select {
		case <-flow.flowCtx.Done():
			log.Tracef("Reload/BufferApply: Skipping flow %s on route '%s' - context already done.", clientKey, routeName)
			return true
		default:
		}

		outConn := flow.outConn
		if outConn == nil {
			log.Warnf("Reload/BufferApply: Flow %s on route '%s' has nil outConn while potentially active. Skipping.", clientKey, routeName)
			return true
		}

		appliedToThisFlow := false

		if readChanged {
			if newReadBuf > 0 {
				err := outConn.SetReadBuffer(newReadBuf)
				if err != nil {
					if !errors.Is(err, net.ErrClosed) {
						log.Tracef("Reload/BufferApply: Failed to set client read buffer to %d for flow %s (%s): %v",
							newReadBuf, clientKey, routeName, err)
						failCount++
					} else {
						log.Tracef("Reload/BufferApply: Skipped setting read buffer for closed flow %s (%s).", clientKey, routeName)
					}
				} else {
					log.Tracef("Reload/BufferApply: Successfully set client read buffer to %d for flow %s (%s)",
						newReadBuf, clientKey, routeName)
					appliedToThisFlow = true
				}
			} else {
				log.Tracef("Reload/BufferApply: Requested client read buffer is OS default for flow %s (%s), not changing existing socket.",
					clientKey, routeName)
			}
		} else if newReadBuf > 0 {
			log.Tracef("Reload/BufferApply: Client read buffer (%d) did not change for flow %s (%s), skipping SetReadBuffer call.",
				newReadBuf, clientKey, routeName)
		}

		if writeChanged {
			if newWriteBuf > 0 {
				err := outConn.SetWriteBuffer(newWriteBuf)
				if err != nil {
					if !errors.Is(err, net.ErrClosed) {
						log.Tracef("Reload/BufferApply: Failed to set client write buffer to %d for flow %s (%s): %v",
							newWriteBuf, clientKey, routeName, err)
						failCount++
					} else {
						log.Tracef("Reload/BufferApply: Skipped setting write buffer for closed flow %s (%s).", clientKey, routeName)
					}
				} else {
					log.Tracef("Reload/BufferApply: Successfully set client write buffer to %d for flow %s (%s)",
						newWriteBuf, clientKey, routeName)
					appliedToThisFlow = true
				}
			} else {
				log.Tracef("Reload/BufferApply: Requested client write buffer is OS default for flow %s (%s), not changing existing socket.",
					clientKey, routeName)
			}
		} else if newWriteBuf > 0 {
			log.Tracef("Reload/BufferApply: Client write buffer (%d) did not change for flow %s (%s), skipping SetWriteBuffer call.",
				newWriteBuf, clientKey, routeName)
		}

		if appliedToThisFlow {
			applyCount++
		}

		return true
	})

	if flowCount > 0 {
		log.Debugf("Reload: Route '%s' (%s): Finished applying client buffer updates to %d iterated flows. Succeeded for %d flows (at least one buffer set), failed for %d operations (excluding closed conns).",
			routeName, listenAddrStr, flowCount, applyCount, failCount)
	} else {
		log.Debugf("Reload: Route '%s' (%s): No active flows found to apply client buffer updates.", routeName, listenAddrStr)
	}
}
