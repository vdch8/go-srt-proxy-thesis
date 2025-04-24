package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"reflect"
	"sync"
	"syscall"
	"time"

	"github.com/fsnotify/fsnotify"
)

const (
	configReloadDebounce = 2 * time.Second
	configFlagName       = "config"
	defaultConfigPath    = "config.yaml"
	modTimeFormat        = time.RFC3339
)

func main() {
	var configPath string
	flag.StringVar(&configPath, configFlagName, defaultConfigPath, "Path to YAML configuration file")
	flag.Parse()

	absConfigPath, err := filepath.Abs(configPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "FATAL: Failed to get absolute config path for '%s': %v\n", configPath, err)
		os.Exit(1)
	}

	globalCtx, globalCancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer globalCancel()

	cfg, err := LoadConfig(absConfigPath)
	if err != nil {

		fmt.Fprintf(os.Stderr, "FATAL: Failed to load initial config '%s': %v\n", absConfigPath, err)
		os.Exit(1)
	}

	SetupLogger(cfg, globalCtx)

	log.Infof("Starting Proxy Server...")
	log.Infof("Using configuration file: %s", absConfigPath)

	var wg sync.WaitGroup

	proxyServer := NewProxyServer(globalCtx)
	if err := proxyServer.Start(cfg); err != nil {
		log.Fatalf("Failed to start proxy server: %v", err)
	}
	log.Info("Proxy server started successfully")

	reloadRequest := make(chan string, 1)
	reloadDebounceTimer := time.NewTimer(configReloadDebounce)
	if !reloadDebounceTimer.Stop() {
		select {
		case <-reloadDebounceTimer.C:
		default:
		}
	}

	var lastModTime time.Time
	configDir := filepath.Dir(absConfigPath)

	info, statErr := os.Stat(absConfigPath)
	if statErr == nil {
		lastModTime = info.ModTime()
		log.Debugf("Initial config modTime: %s", lastModTime.Format(modTimeFormat))
	} else {
		log.Warnf("Could not stat initial config file '%s': %v", absConfigPath, statErr)
	}

	wg.Add(1)
	go func() {
		defer wg.Done()
		watcher, err := fsnotify.NewWatcher()
		if err != nil {
			log.Errorf("Failed to create file watcher: %v. Automatic config reload disabled.", err)
			return
		}
		defer watcher.Close()

		watchCtx, watchCancel := context.WithCancel(globalCtx)
		defer watchCancel()

		err = watcher.Add(configDir)
		if err != nil {

			if _, statErr := os.Stat(configDir); os.IsNotExist(statErr) {
				log.Errorf("Config directory '%s' does not exist. Cannot watch for changes.", configDir)
			} else {
				log.Errorf("Failed to watch config directory '%s': %v. Automatic config reload disabled.", configDir, err)
			}
			return
		}
		log.Infof("Watching config directory '%s' for changes to '%s'...", configDir, filepath.Base(absConfigPath))

		for {
			select {
			case event, ok := <-watcher.Events:
				if !ok {
					log.Warn("File watcher events channel closed.")
					return
				}

				cleanEventPath := filepath.Clean(event.Name)
				if cleanEventPath == absConfigPath {

					isRelevantOp := event.Has(fsnotify.Write) || event.Has(fsnotify.Create) || event.Has(fsnotify.Rename) || event.Has(fsnotify.Chmod)
					if isRelevantOp {

						currentInfo, err := os.Stat(absConfigPath)
						if err != nil {
							log.Warnf("Config file '%s' change detected (event: %s), but stat failed: %v. Debouncing reload.", absConfigPath, event.Op, err)
							if !reloadDebounceTimer.Stop() {
								select {
								case <-reloadDebounceTimer.C:
								default:
								}
							}
							reloadDebounceTimer.Reset(configReloadDebounce)
							continue
						}
						currentModTime := currentInfo.ModTime()

						if currentModTime.After(lastModTime) || event.Has(fsnotify.Create) || event.Has(fsnotify.Rename) {
							log.Infof("Config file '%s' change detected (event: %s, modtime: %s). Debouncing reload...", absConfigPath, event.Op, currentModTime.Format(modTimeFormat))
							if !reloadDebounceTimer.Stop() {
								select {
								case <-reloadDebounceTimer.C:
								default:
								}
							}
							reloadDebounceTimer.Reset(configReloadDebounce)

						} else {
							log.Debugf("Config file event %s ignored for '%s', modification time %s did not change.", event.Op, absConfigPath, currentModTime.Format(modTimeFormat))
						}
					} else {
						log.Debugf("Ignoring fsnotify event %s for config file %s", event.Op, absConfigPath)
					}
				} else if cleanEventPath != filepath.Clean(configDir) {

					log.Debugf("Ignoring fsnotify event for unrelated file: %s (%s)", event.Name, event.Op)
				}

			case err, ok := <-watcher.Errors:
				if !ok {
					log.Warn("File watcher errors channel closed.")
					return
				}
				log.Errorf("File watcher error: %v", err)

			case <-watchCtx.Done():
				log.Info("Stopping file watcher due to application shutdown.")
				return
			}
		}
	}()

	log.Info("Application started. Waiting for signals or config changes...")
	keepRunning := true
	for keepRunning {
		select {
		case <-globalCtx.Done():
			log.Info("Shutdown initiated by signal or context cancellation.")
			keepRunning = false

		case <-reloadDebounceTimer.C:
			log.Info("Debounce timer fired. Queueing configuration reload.")

			select {
			case reloadRequest <- absConfigPath:
				log.Debug("Reload request queued.")
			default:
				log.Warn("Reload request channel busy, skipping this trigger.")
			}

		case configToReload := <-reloadRequest:
			log.Infof("Processing configuration reload request for '%s'...", configToReload)
			newCfg, err := LoadConfig(configToReload)
			if err != nil {
				log.Errorf("Failed to reload config from '%s': %v. Keeping old configuration.", configToReload, err)

				info, statErr := os.Stat(configToReload)
				if statErr == nil {
					lastModTime = info.ModTime()
				}
				continue
			}

			if !reflect.DeepEqual(cfg.LogSettings, newCfg.LogSettings) {
				log.Info("Log settings have changed. Reconfiguring logger...")
				SetupLogger(newCfg, globalCtx)
				log.Info("Logger reconfigured successfully.")
			} else {
				log.Debug("Log settings remain unchanged.")
			}

			log.Info("Applying reloaded configuration to the proxy server...")
			if err := proxyServer.Reload(newCfg); err != nil {
				log.Errorf("Failed to apply reloaded configuration: %v. Server might be in an inconsistent state.", err)

			} else {
				log.Info("Configuration reloaded and applied successfully.")
				cfg = newCfg

				currentInfo, statErr := os.Stat(configToReload)
				if statErr == nil {
					lastModTime = currentInfo.ModTime()
					log.Debugf("Updated last successful reload modTime to %s", lastModTime.Format(modTimeFormat))
				} else {
					log.Warnf("Could not stat config file '%s' after successful reload: %v", configToReload, statErr)
				}
			}
		}
	}

	log.Info("Shutting down proxy server...")
	proxyServer.Stop()

	log.Info("Waiting for background tasks to finish...")

	wg.Wait()

	log.Info("Proxy Server stopped.")
	os.Exit(0)
}
