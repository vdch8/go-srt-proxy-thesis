package main

import (
	"time"
)

const DefaultFlowTimeout = 60 * time.Second

func parseTimeout(routeTimeoutStr, globalDefaultTimeoutStr string) time.Duration {
	var effectiveDefault time.Duration

	if globalDefaultTimeoutStr != "" {
		if d, err := time.ParseDuration(globalDefaultTimeoutStr); err == nil {
			if d > 0 {
				effectiveDefault = d
				log.Debugf("Using global default flow_timeout from config: %v", effectiveDefault)
			} else {
				log.Warnf("Global default flow_timeout '%s' is not positive, using absolute default %v.", globalDefaultTimeoutStr, DefaultFlowTimeout)
				effectiveDefault = DefaultFlowTimeout
			}
		} else {
			log.Warnf("Invalid global default flow_timeout '%s', using absolute default %v. Error: %v", globalDefaultTimeoutStr, DefaultFlowTimeout, err)
			effectiveDefault = DefaultFlowTimeout
		}
	} else {
		log.Debugf("No global default flow_timeout configured, using absolute default: %v", DefaultFlowTimeout)
		effectiveDefault = DefaultFlowTimeout
	}

	var routeTimeout time.Duration
	if routeTimeoutStr != "" {
		if d, err := time.ParseDuration(routeTimeoutStr); err == nil {

			if d > 0 {
				routeTimeout = d
				log.Debugf("Using route-specific flow_timeout from config: %v", routeTimeout)
			} else {

				log.Warnf("Route-specific flow_timeout '%s' is not positive, using effective default %v for this route.", routeTimeoutStr, effectiveDefault)
				routeTimeout = effectiveDefault
			}
		} else {

			log.Warnf("Invalid route-specific flow_timeout '%s', using effective default %v for this route. Error: %v", routeTimeoutStr, effectiveDefault, err)
			routeTimeout = effectiveDefault
		}
	} else {

		log.Debugf("No route-specific flow_timeout configured, using effective default: %v", effectiveDefault)
		routeTimeout = effectiveDefault
	}

	return routeTimeout
}
