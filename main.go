package main

import (
	"context"
	"flag"
	"os"
	"os/signal"
	"path/filepath"
	"sync"
	"syscall"
	"time"

	"github.com/fsnotify/fsnotify"
	"github.com/sirupsen/logrus"
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
		println("FATAL: Failed to get absolute config path:", err.Error())
		os.Exit(1)
	}

	cfg, err := LoadConfig(absConfigPath)
	if err != nil {
		println("FATAL: Failed to load initial config '"+absConfigPath+"':", err.Error())
		os.Exit(1)
	}

	SetupLogger(cfg.LogLevel)

	log.Infof("Starting Proxy Server...")
	log.Infof("Using configuration file: %s", absConfigPath)

	globalCtx, globalCancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup

	proxyServer := NewProxyServer(globalCtx)
	if err := proxyServer.Start(cfg); err != nil {
		log.Fatalf("Failed to start proxy server: %v", err)
	}
	log.Info("Proxy server started successfully")

	reloadRequest := make(chan string, 1)
	reloadTimer := time.NewTimer(0)
	<-reloadTimer.C
	reloadTimer.Stop()
	var lastModTime time.Time
	configDir := filepath.Dir(absConfigPath)

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

		info, statErr := os.Stat(absConfigPath)
		if statErr == nil {
			lastModTime = info.ModTime()
		} else {
			log.Warnf("Could not stat initial config file '%s': %v", absConfigPath, statErr)
		}

		for {
			select {
			case event, ok := <-watcher.Events:
				if !ok {
					log.Warn("File watcher events channel closed.")
					return
				}
				cleanEventPath := filepath.Clean(event.Name)
				if cleanEventPath == absConfigPath {
					if event.Has(fsnotify.Write) || event.Has(fsnotify.Create) || event.Has(fsnotify.Rename) || event.Has(fsnotify.Chmod) {
						info, err := os.Stat(absConfigPath)
						if err != nil {
							log.Warnf("Config file '%s' change detected (event: %s), but stat failed: %v. Debouncing reload.", absConfigPath, event.Op, err)
							reloadTimer.Reset(configReloadDebounce)
							continue
						}
						currentModTime := info.ModTime()
						if currentModTime.After(lastModTime) || event.Has(fsnotify.Rename) || event.Has(fsnotify.Create) {
							log.Infof("Config file '%s' change detected (event: %s, modtime: %s). Debouncing reload...", absConfigPath, event.Op, currentModTime.Format(modTimeFormat))
							reloadTimer.Reset(configReloadDebounce)
						} else if log.GetLevel() >= logrus.DebugLevel {
							log.Debugf("Config file event %s ignored, modification time %s did not change.", event.Op, currentModTime.Format(modTimeFormat))
						}
					}
				} else if cleanEventPath != filepath.Clean(configDir) && log.GetLevel() >= logrus.DebugLevel {
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

	sigterm := make(chan os.Signal, 1)
	signal.Notify(sigterm, syscall.SIGINT, syscall.SIGTERM)

	log.Info("Application started. Waiting for signals or config changes...")
	keepRunning := true
	for keepRunning {
		select {
		case <-sigterm:
			log.Info("Shutdown signal received, initiating graceful shutdown...")
			globalCancel()
			keepRunning = false

		case <-reloadTimer.C:
			log.Info("Debounce timer fired. Queueing configuration reload.")
			select {
			case reloadRequest <- absConfigPath:
				log.Debug("Reload request queued.")
			default:
				log.Warn("Reload request channel busy, skipping this trigger.")
			}

		case configToReload := <-reloadRequest:
			log.Info("Processing configuration reload request...")
			newCfg, err := LoadConfig(configToReload)
			if err != nil {
				log.Errorf("Failed to reload config from '%s': %v. Keeping old configuration.", configToReload, err)
				info, statErr := os.Stat(configToReload)
				if statErr == nil {
					lastModTime = info.ModTime()
				}
				continue
			}

			if err := proxyServer.Reload(newCfg); err != nil {
				log.Errorf("Failed to apply reloaded configuration: %v. Server might be in an inconsistent state.", err)
			} else {
				log.Info("Configuration reloaded and applied successfully.")
				info, statErr := os.Stat(configToReload)
				if statErr == nil {
					lastModTime = info.ModTime()
					log.Debugf("Updated lastModTime to %s", lastModTime.Format(modTimeFormat))
				}

				currentLevel := log.GetLevel()
				newLvl, Lerr := logrus.ParseLevel(newCfg.LogLevel)
				if Lerr == nil {
					if newLvl != currentLevel {
						log.SetLevel(newLvl)
						log.Infof("Log level updated to %s", newLvl.String())
					} else {
						log.Debugf("Log level '%s' in reloaded config is the same as current level.", newLvl.String())
					}
				} else {
					log.Warnf("Invalid log_level '%s' in reloaded config, keeping level %s. Error: %v",
						newCfg.LogLevel, currentLevel.String(), Lerr)
				}
			}

		case <-globalCtx.Done():
			if keepRunning {
				log.Warn("Global context cancelled unexpectedly, initiating shutdown.")
				keepRunning = false
			}
		}
	}

	log.Info("Shutting down proxy server...")
	proxyServer.Stop()

	log.Info("Waiting for background tasks (like file watcher) to finish...")
	wg.Wait()

	log.Info("Proxy Server stopped.")
	os.Exit(0)
}
