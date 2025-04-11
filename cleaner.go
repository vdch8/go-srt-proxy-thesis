package main

import (
	"sync"
	"sync/atomic"
	"time"

	"github.com/sirupsen/logrus"
)

func (p *ProxyServer) flowCleanerLoop() {
	defer p.cleanerWg.Done()

	p.cleanerConfigLock.RLock()
	currentInterval := p.cleanerBaseInterval
	initialMin := p.cleanerMinInterval
	initialMax := p.cleanerMaxInterval
	p.cleanerConfigLock.RUnlock()

	log.Infof("Starting global flow cleaner loop with initial interval %v (min: %v, max: %v, based on current settings)", currentInterval, initialMin, initialMax)
	ticker := time.NewTicker(currentInterval)
	defer ticker.Stop()

	for {
		select {
		case <-p.globalCtx.Done():
			log.Info("Stopping global flow cleaner loop due to context cancellation.")
			return

		case <-ticker.C:
			log.Debug("Flow cleaner ticker fired. Running cleanup cycle.")
			removedCount := p.cleanupClientFlows()

			if removedCount > 0 {
				log.Infof("Flow cleanup cycle removed %d expired flows.", removedCount)
			} else {
				log.Debug("Flow cleanup cycle finished. No expired flows found.")
			}

			p.cleanerConfigLock.RLock()
			baseInterval := p.cleanerBaseInterval
			minInterval := p.cleanerMinInterval
			maxInterval := p.cleanerMaxInterval
			intervalDivisor := p.cleanerIntervalDivisor
			p.cleanerConfigLock.RUnlock()

			minTimeout := DefaultFlowTimeout
			foundActiveRouteWithTimeout := false

			p.routesLock.RLock()
			activeRouteList := make([]*ActiveRoute, 0, len(p.activeRoutes))
			for _, r := range p.activeRoutes {
				select {
				case <-r.ctx.Done():
				default:
					activeRouteList = append(activeRouteList, r)
				}
			}
			p.routesLock.RUnlock()

			for _, route := range activeRouteList {
				route.configLock.RLock()
				currentRouteTimeout := route.flowTimeout
				route.configLock.RUnlock()

				if currentRouteTimeout > 0 {
					if !foundActiveRouteWithTimeout || currentRouteTimeout < minTimeout {
						minTimeout = currentRouteTimeout
					}
					foundActiveRouteWithTimeout = true
				}
			}

			var newInterval time.Duration
			if !foundActiveRouteWithTimeout {
				newInterval = baseInterval
				log.Debugf("No active connections with positive timeouts found, using configured base cleaner interval: %v", newInterval)
			} else {
				newInterval = minTimeout / time.Duration(intervalDivisor)
				if newInterval < minInterval {
					newInterval = minInterval
				}
				if newInterval > maxInterval {
					newInterval = maxInterval
				}
				log.Debugf("Minimum active connections timeout is %v. Calculated cleaner interval: %v (using divisor %d, clamped to [%v, %v])",
					minTimeout, newInterval, intervalDivisor, minInterval, maxInterval)
			}

			if newInterval != currentInterval {
				log.Infof("Adjusting flow cleaner interval from %v to %v", currentInterval, newInterval)
				ticker.Reset(newInterval)
				currentInterval = newInterval
			}
		}
	}
}

func (p *ProxyServer) cleanupClientFlows() int64 {
	now := time.Now()
	nowNanos := now.UnixNano()
	totalRemovedCount := atomic.Int64{}

	p.routesLock.RLock()
	routesToCheck := make([]*ActiveRoute, 0, len(p.activeRoutes))
	for _, route := range p.activeRoutes {
		routesToCheck = append(routesToCheck, route)
	}
	p.routesLock.RUnlock()

	if len(routesToCheck) == 0 {
		log.Debug("Cleanup cycle: No active routes to check.")
		return 0
	}

	var wg sync.WaitGroup

	for _, route := range routesToCheck {
		wg.Add(1)
		go func(ar *ActiveRoute) {
			defer wg.Done()
			routeRemovedCount := 0

			select {
			case <-ar.ctx.Done():
				if log.GetLevel() >= logrus.DebugLevel {
					ar.configLock.RLock()
					rName := ar.routeName
					ar.configLock.RUnlock()
					log.Debugf("Cleanup cycle: Skipping already stopped route '%s'", rName)
				}
				return
			default:
			}

			ar.configLock.RLock()
			routeName := ar.routeName
			routeFlowTimeout := ar.flowTimeout
			ar.configLock.RUnlock()

			if routeFlowTimeout <= 0 {
				if log.GetLevel() >= logrus.DebugLevel {
					log.Debugf("Cleanup cycle: Skipping route '%s' due to non-positive timeout (%v).", routeName, routeFlowTimeout)
				}
				return
			}
			timeoutNanos := routeFlowTimeout.Nanoseconds()

			ar.clientFlows.Range(func(key, value any) bool {
				clientKey := key.(string)
				flow := value.(*ClientFlow)

				select {
				case <-flow.flowCtx.Done():
					if log.GetLevel() >= logrus.DebugLevel {
						log.Debugf("Cleanup cycle: Found already stopped flow for client %s on route '%s'. Ensuring removal.", clientKey, routeName)
					}
					ar.clientFlows.Delete(clientKey)
					return true
				default:
				}

				lastActivityNanos := flow.lastActivity.Load()

				if (nowNanos - lastActivityNanos) > timeoutNanos {
					log.Infof("Route '%s': Flow for client %s timed out (last activity: %v ago, timeout: %v). Attempting removal.",
						routeName, clientKey, now.Sub(time.Unix(0, lastActivityNanos)).Round(time.Millisecond), routeFlowTimeout)

					if actualFlowVal, loaded := ar.clientFlows.LoadAndDelete(clientKey); loaded {
						log.Debugf("Route '%s': Successfully removed flow for client %s from map.", routeName, clientKey)
						actualFlow := actualFlowVal.(*ClientFlow)

						go actualFlow.Stop()

						routeRemovedCount++
						totalRemovedCount.Add(1)
					} else {
						if log.GetLevel() >= logrus.DebugLevel {
							log.Debugf("Route '%s': Flow for client %s was already removed before cleanup could claim it.", routeName, clientKey)
						}
					}
				}
				return true
			})

			if routeRemovedCount > 0 && log.GetLevel() >= logrus.DebugLevel {
				log.Debugf("Cleanup cycle: Finished route '%s'. Removed %d expired flows.", routeName, routeRemovedCount)
			}

		}(route)
	}

	wg.Wait()

	return totalRemovedCount.Load()
}
