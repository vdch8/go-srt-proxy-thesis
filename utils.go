package main

import (
	"time"
)

const DefaultFlowTimeout = 10 * time.Second

func parseTimeout(routeTimeoutStr, defaultsTimeoutStr string) time.Duration {
	var effectiveTimeout time.Duration
	baseTimeoutSource := "absolute default"

	if defaultsTimeoutStr != "" {
		if d, err := time.ParseDuration(defaultsTimeoutStr); err == nil {
			if d > 0 {
				effectiveTimeout = d
				baseTimeoutSource = "'defaults' section"
				log.Debugf("Using base flow_timeout from %s: %v", baseTimeoutSource, effectiveTimeout)
			} else {
				log.Warnf("Flow_timeout '%s' in 'defaults' section is not positive, using absolute default %v.", defaultsTimeoutStr, DefaultFlowTimeout)
				effectiveTimeout = DefaultFlowTimeout
			}
		} else {
			log.Warnf("Invalid flow_timeout '%s' in 'defaults' section, using absolute default %v. Error: %v", defaultsTimeoutStr, DefaultFlowTimeout, err)
			effectiveTimeout = DefaultFlowTimeout
		}
	} else {
		log.Debugf("No flow_timeout in 'defaults' section, base is absolute default: %v", DefaultFlowTimeout)
		effectiveTimeout = DefaultFlowTimeout
	}

	if routeTimeoutStr != "" {
		if d, err := time.ParseDuration(routeTimeoutStr); err == nil {
			if d > 0 {
				effectiveTimeout = d
				log.Debugf("Using route-specific flow_timeout from config: %v (overrides base from %s)", effectiveTimeout, baseTimeoutSource)
			} else {
				log.Warnf("Route-specific flow_timeout '%s' is not positive, using effective base timeout %v (from %s) for this route.", routeTimeoutStr, effectiveTimeout, baseTimeoutSource)
			}
		} else {
			log.Warnf("Invalid route-specific flow_timeout '%s', using effective base timeout %v (from %s) for this route. Error: %v", routeTimeoutStr, effectiveTimeout, baseTimeoutSource, err)
		}
	} else {
		log.Debugf("No route-specific flow_timeout configured, using effective base timeout: %v (from %s)", effectiveTimeout, baseTimeoutSource)
	}
	return effectiveTimeout
}
