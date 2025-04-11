package main

import (
	"fmt"
	"os"

	"gopkg.in/yaml.v3"
)

type CleanerSettings struct {
	BaseInterval    string `yaml:"base_interval,omitempty"`
	MinInterval     string `yaml:"min_interval,omitempty"`
	MaxInterval     string `yaml:"max_interval,omitempty"`
	IntervalDivisor *int   `yaml:"interval_divisor,omitempty"`
}

type Config struct {
	LogLevel          string           `yaml:"log_level,omitempty"`
	FlowTimeout       string           `yaml:"flow_timeout,omitempty"`
	Defaults          StreamRoute      `yaml:"defaults,omitempty"`
	Streams           []StreamRoute    `yaml:"streams"`
	Cleaner           *CleanerSettings `yaml:"cleaner_settings,omitempty"`
	MinChannelSize    *int             `yaml:"min_channel_size,omitempty"`
	MaxChannelSize    *int             `yaml:"max_channel_size,omitempty"`
	UDPReadBufferSize *int             `yaml:"udp_read_buffer_size,omitempty"`
}

type StreamRoute struct {
	Name          string `yaml:"name"`
	ListenAddress string `yaml:"listen_address"`
	SourceAddress string `yaml:"source_address"`
	FlowTimeout   string `yaml:"flow_timeout,omitempty"`
	Workers       *int   `yaml:"workers,omitempty"`
	ChanSize      *int   `yaml:"chan_size,omitempty"`
}

func LoadConfig(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read config file '%s': %w", path, err)
	}

	var cfg Config
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return nil, fmt.Errorf("unmarshal yaml config '%s': %w", path, err)
	}

	if len(cfg.Streams) == 0 {
		log.Warnf("Configuration file '%s' loaded, but contains no stream definitions.", path)
	}

	listenAddrs := make(map[string]string)
	for _, stream := range cfg.Streams {
		if stream.ListenAddress == "" {
			continue
		}
		if existingName, found := listenAddrs[stream.ListenAddress]; found {
			return nil, fmt.Errorf("duplicate listen_address '%s' found in config for routes '%s' and '%s'", stream.ListenAddress, existingName, stream.Name)
		}
		listenAddrs[stream.ListenAddress] = stream.Name
	}

	return &cfg, nil
}
