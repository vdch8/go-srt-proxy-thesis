package main

import (
	"context"
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
	DefaultRouteWorkers              = 1
	MinRouteWorkers                  = 1
	MaxRouteWorkers                  = 128
	AbsoluteDefaultPacketChanSize    = 1024
	AbsoluteDefaultMinPacketChanSize = 64
	AbsoluteDefaultMaxPacketChanSize = 8192
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
	routeCfg   StreamRoute
	listenAddr *net.UDPAddr
	sourceAddr *net.UDPAddr
	timeout    time.Duration
	workers    int
	chanSize   int
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

func (p *ProxyServer) startRouteLocked(routeName string, listenAddr *net.UDPAddr, sourceAddr *net.UDPAddr, timeout time.Duration, workers int, chanSize int) error {
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

	log.Infof("Route '%s': Listening on %s, forwarding new flows to %s (Timeout: %v, Workers: %d, ChanSize: %d)",
		routeName, listenAddrStr, sourceAddrStr, timeout, workers, chanSize)

	ctx, cancel := context.WithCancel(p.globalCtx)

	log.Debugf("Route '%s': Initializing with %d workers and channel size %d", routeName, workers, chanSize)

	activeRoute := &ActiveRoute{
		routeName:    routeName,
		listenAddr:   listenAddr,
		listener:     listener,
		targetSource: sourceAddr,
		flowTimeout:  timeout,
		ctx:          ctx,
		cancel:       cancel,
		numWorkers:   workers,
		packetChan:   make(chan *packetBuffer, chanSize),
		clientFlows:  sync.Map{},
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

		resRoute := resolvedRoute{
			routeCfg:   routeCfg,
			listenAddr: listenAddr,
			sourceAddr: sourceAddr,
			timeout:    timeout,
			workers:    workers,
			chanSize:   chanSize,
		}

		newRouteConfigs[listenAddr.String()] = resRoute
		uniqueListenAddrs[routeCfg.ListenAddress] = routeCfg.Name

		log.Debugf("Config Validation: Resolved new/updated route '%s': Listen=%s Source=%s Timeout=%v Workers=%d ChanSize=%d",
			routeCfg.Name, listenAddr.String(), sourceAddr.String(), timeout, workers, chanSize)
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
			oldSourceStr := activeRoute.targetSource.String()
			oldWorkers := activeRoute.numWorkers
			oldName := activeRoute.routeName

			activeRoute.configLock.RUnlock()

			log.Debugf("Reload: Route '%s' (%s) exists, checking for updates (New: Name='%s' Source=%s, Workers=%d, Timeout=%v, ChanSize=%d)",
				oldName, listenAddrStr, newRouteData.routeCfg.Name, newRouteData.sourceAddr.String(), newRouteData.workers, newRouteData.timeout, newRouteData.chanSize)

			sourceChanged := oldSourceStr != newRouteData.sourceAddr.String()

			workersChanged := oldWorkers != newRouteData.workers

			if sourceChanged || workersChanged {

				reason := ""
				if sourceChanged {
					reason += fmt.Sprintf("Source address changed ('%s' -> '%s'). ", oldSourceStr, newRouteData.sourceAddr.String())
				}
				if workersChanged {
					reason += fmt.Sprintf("Worker count changed (%d -> %d). ", oldWorkers, newRouteData.workers)
				}
				log.Infof("Reload: %sRoute '%s' (%s) requires RESTART. Applying new settings (Name: '%s', ChanSize: %d).",
					reason, oldName, listenAddrStr, newRouteData.routeCfg.Name, newRouteData.chanSize)

				activeRoute.Stop()
				delete(p.activeRoutes, listenAddrStr)

				log.Infof("Reload: Restarting route '%s' with new config (Listen: %s, Source: %s, Workers: %d, ChanSize: %d, Timeout: %v)",
					newRouteData.routeCfg.Name, listenAddrStr, newRouteData.sourceAddr.String(), newRouteData.workers, newRouteData.chanSize, newRouteData.timeout)
				err := p.startRouteLocked(
					newRouteData.routeCfg.Name,
					newRouteData.listenAddr,
					newRouteData.sourceAddr,
					newRouteData.timeout,
					newRouteData.workers,
					newRouteData.chanSize,
				)
				if err != nil {
					log.Errorf("Reload: Failed to RESTART route '%s': %v", newRouteData.routeCfg.Name, err)
					hasFailures = true
				} else {
					log.Infof("Reload: Route '%s' restarted successfully.", newRouteData.routeCfg.Name)
				}
			} else {

				p.updateRouteInPlaceLocked(activeRoute, newRouteData)
			}
		} else {
			log.Infof("Reload: Starting new route '%s' (Listen: %s, Source: %s, Workers: %d, ChanSize: %d, Timeout: %v)",
				newRouteData.routeCfg.Name, listenAddrStr, newRouteData.sourceAddr.String(), newRouteData.workers, newRouteData.chanSize, newRouteData.timeout)
			err := p.startRouteLocked(
				newRouteData.routeCfg.Name,
				newRouteData.listenAddr,
				newRouteData.sourceAddr,
				newRouteData.timeout,
				newRouteData.workers,
				newRouteData.chanSize,
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
			v.configLock.RUnlock()
			finalActive = append(finalActive, fmt.Sprintf("'%s'(%s)", rName, k))
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
	listenAddrStr := activeRoute.listenAddr.String()

	nameChanged := oldName != newRouteData.routeCfg.Name
	timeoutChanged := oldTimeout != newRouteData.timeout

	updatedFieldsLog := ""
	if nameChanged {
		activeRoute.routeName = newRouteData.routeCfg.Name
		updatedFieldsLog += fmt.Sprintf(" Name: '%s'->'%s'", oldName, activeRoute.routeName)
	}
	if timeoutChanged {
		activeRoute.flowTimeout = newRouteData.timeout
		updatedFieldsLog += fmt.Sprintf(" Timeout: %v->%v", oldTimeout, activeRoute.flowTimeout)

	}

	if updatedFieldsLog != "" {
		log.Infof("Reload: Updated existing route '%s' (%s) in-place:%s", activeRoute.routeName, listenAddrStr, updatedFieldsLog)
	} else {
		log.Debugf("Reload: Route '%s' (%s) configuration requires no in-place update (Name, Timeout) and no restart (Source, Workers).", activeRoute.routeName, listenAddrStr)
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
