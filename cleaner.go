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
	currentInterval := p.cleanerInterval
	p.cleanerConfigLock.RUnlock()

	log.Infof("Starting global flow cleaner loop with interval %v (based on current settings)", currentInterval)
	ticker := time.NewTicker(currentInterval)
	defer ticker.Stop()

	for {
		select {
		case <-p.globalCtx.Done():
			log.Info("Stopping global flow cleaner loop due to context cancellation.")
			return

		case <-ticker.C:

			p.cleanerConfigLock.RLock()
			newInterval := p.cleanerInterval
			p.cleanerConfigLock.RUnlock()

			if newInterval != currentInterval {
				log.Infof("Adjusting flow cleaner interval from %v to %v", currentInterval, newInterval)
				ticker.Reset(newInterval)
				currentInterval = newInterval
			}

			log.Debug("Flow cleaner ticker fired. Running cleanup cycle.")

			removedCount := p.cleanupClientFlows()

			if removedCount > 0 {
				log.Infof("Flow cleanup cycle removed %d potentially leaked/stuck flows.", removedCount)
			} else {
				log.Debug("Flow cleanup cycle finished. No stopped flows found in map needing removal.")
			}

		}
	}
}

func (p *ProxyServer) cleanupClientFlows() int64 {
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
			ar.configLock.RUnlock()

			ar.clientFlows.Range(func(key, value any) bool {
				clientKey := key.(string)
				flow, ok := value.(*ClientFlow)
				if !ok {
					log.Warnf("Cleanup cycle: Found unexpected type in clientFlows map for key %s on route '%s'. Attempting removal.", clientKey, routeName)

					if _, loaded := ar.clientFlows.LoadAndDelete(clientKey); loaded {
						log.Debugf("Cleanup cycle: Removed unexpected entry for key %s in route '%s'.", clientKey, routeName)
						totalRemovedCount.Add(1)
					}
					return true
				}

				select {
				case <-flow.flowCtx.Done():
					log.Warnf("Cleanup cycle: Found stopped flow (context done) for client %s on route '%s' still in map. Removing.", clientKey, routeName)

					if _, loaded := ar.clientFlows.LoadAndDelete(clientKey); loaded {
						log.Debugf("Cleanup cycle: Successfully removed potentially leaked flow for client %s in route '%s' from map.", clientKey, routeName)
						routeRemovedCount++
						totalRemovedCount.Add(1)

					} else {
						log.Debugf("Cleanup cycle: Flow for client %s in route '%s' was concurrently removed before cleanup could claim it.", clientKey, routeName)
					}
					return true
				default:

					return true
				}
			})

			if routeRemovedCount > 0 && log.GetLevel() >= logrus.InfoLevel {
				log.Infof("Cleanup cycle: Finished route '%s'. Removed %d potentially leaked flows.", routeName, routeRemovedCount)
			}

		}(route)
	}

	wg.Wait()

	return totalRemovedCount.Load()
}
