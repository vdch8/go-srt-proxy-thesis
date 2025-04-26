package main

import (
	"fmt"
	"os"

	"gopkg.in/yaml.v3"
)

type LogFileSettings struct {
	Enabled    bool   `yaml:"enabled"`
	Path       string `yaml:"path"`
	Level      string `yaml:"level,omitempty"`
	MaxSize    int    `yaml:"max_size,omitempty"`
	MaxAge     int    `yaml:"max_age,omitempty"`
	MaxBackups int    `yaml:"max_backups,omitempty"`
	Compress   bool   `yaml:"compress,omitempty"`
}

type LogSettings struct {
	ConsoleLevel    string           `yaml:"console_level,omitempty"`
	File            *LogFileSettings `yaml:"file,omitempty"`
	SRTLogBufSize   *int             `yaml:"srt_log_buffer_size,omitempty"`
	TimestampFormat string           `yaml:"timestamp_format,omitempty"`
}

type CleanerSettings struct {
	Interval string `yaml:"interval,omitempty"`
}

type Config struct {
	LogSettings       *LogSettings     `yaml:"log_settings,omitempty"`
	Defaults          StreamRoute      `yaml:"defaults,omitempty"`
	Streams           []StreamRoute    `yaml:"streams"`
	Cleaner           *CleanerSettings `yaml:"cleaner_settings,omitempty"`
	MinChannelSize    *int             `yaml:"min_channel_size,omitempty"`
	MaxChannelSize    *int             `yaml:"max_channel_size,omitempty"`
	UDPReadBufferSize *int             `yaml:"udp_read_buffer_size,omitempty"`
}

type StreamRoute struct {
	Name              string `yaml:"name"`
	ListenAddress     string `yaml:"listen_address"`
	SourceAddress     string `yaml:"source_address"`
	FlowTimeout       string `yaml:"flow_timeout,omitempty"`
	Workers           *int   `yaml:"workers,omitempty"`
	ChanSize          *int   `yaml:"chan_size,omitempty"`
	ListenReadBuffer  *int   `yaml:"listen_read_buffer,omitempty"`
	ListenWriteBuffer *int   `yaml:"listen_write_buffer,omitempty"`
	ClientReadBuffer  *int   `yaml:"client_read_buffer,omitempty"`
	ClientWriteBuffer *int   `yaml:"client_write_buffer,omitempty"`
}

func LoadConfig(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read config file '%s': %w", path, err)
	}

	cfg := Config{
		LogSettings: &LogSettings{
			ConsoleLevel: "info",
			File: &LogFileSettings{
				Enabled:    false,
				Path:       "logs/proxy.log",
				Level:      "info",
				MaxSize:    100,
				MaxAge:     7,
				MaxBackups: 3,
				Compress:   false,
			},
			SRTLogBufSize:   intPtr(defaultSRTLogChannelCap),
			TimestampFormat: defaultLogTimestampFormat,
		},
		Defaults: StreamRoute{},
		Cleaner:  &CleanerSettings{},
	}

	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return nil, fmt.Errorf("unmarshal yaml config '%s': %w", path, err)
	}

	if cfg.LogSettings == nil {
		cfg.LogSettings = &LogSettings{
			ConsoleLevel: "info",
			File: &LogFileSettings{
				Enabled: false, Path: "logs/proxy.log", Level: "info",
				MaxSize: 100, MaxAge: 7, MaxBackups: 3, Compress: false,
			},
			SRTLogBufSize: intPtr(defaultSRTLogChannelCap), TimestampFormat: defaultLogTimestampFormat,
		}
	}
	if cfg.LogSettings.ConsoleLevel == "" {
		cfg.LogSettings.ConsoleLevel = "info"
	}
	if cfg.LogSettings.File == nil {
		cfg.LogSettings.File = &LogFileSettings{
			Enabled: false, Path: "logs/proxy.log", Level: "info",
			MaxSize: 100, MaxAge: 7, MaxBackups: 3, Compress: false,
		}
	} else {
		if cfg.LogSettings.File.Path == "" {
			cfg.LogSettings.File.Path = "logs/proxy.log"
		}
		if cfg.LogSettings.File.Level == "" {
			cfg.LogSettings.File.Level = "info"
		}
		if cfg.LogSettings.File.MaxSize <= 0 {
			cfg.LogSettings.File.MaxSize = 100
		}
		if cfg.LogSettings.File.MaxAge <= 0 {
			cfg.LogSettings.File.MaxAge = 7
		}
		if cfg.LogSettings.File.MaxBackups < 0 {
			cfg.LogSettings.File.MaxBackups = 3
		}
	}
	if cfg.LogSettings.SRTLogBufSize == nil {
		cfg.LogSettings.SRTLogBufSize = intPtr(defaultSRTLogChannelCap)
	} else if *cfg.LogSettings.SRTLogBufSize <= 0 {
		fmt.Fprintf(os.Stderr, "WARN: Invalid log_settings.srt_log_buffer_size %d, using default %d\n", *cfg.LogSettings.SRTLogBufSize, defaultSRTLogChannelCap)
		cfg.LogSettings.SRTLogBufSize = intPtr(defaultSRTLogChannelCap)
	}
	if cfg.LogSettings.TimestampFormat == "" {
		cfg.LogSettings.TimestampFormat = defaultLogTimestampFormat
	}

	if cfg.Cleaner == nil {
		cfg.Cleaner = &CleanerSettings{}
	}

	if len(cfg.Streams) == 0 {
		fmt.Fprintf(os.Stderr, "WARN: Configuration file '%s' loaded, but contains no stream definitions.\n", path)
	}

	listenAddrs := make(map[string]string)
	for i, stream := range cfg.Streams {
		if stream.Name == "" {
			fmt.Fprintf(os.Stderr, "WARN: Stream #%d in config is missing required field 'name'. Skipping.\n", i+1)
			continue
		}
		if stream.ListenAddress == "" {
			fmt.Fprintf(os.Stderr, "WARN: Stream '%s' (index %d) in config is missing required field 'listen_address'. Skipping.\n", stream.Name, i+1)
			continue
		}
		if existingName, found := listenAddrs[stream.ListenAddress]; found {
			return nil, fmt.Errorf("duplicate listen_address '%s' found in config for routes '%s' and '%s'", stream.ListenAddress, existingName, stream.Name)
		}
		listenAddrs[stream.ListenAddress] = stream.Name
	}

	checkBufferConfig("defaults", "listen_read_buffer", cfg.Defaults.ListenReadBuffer)
	checkBufferConfig("defaults", "listen_write_buffer", cfg.Defaults.ListenWriteBuffer)
	checkBufferConfig("defaults", "client_read_buffer", cfg.Defaults.ClientReadBuffer)
	checkBufferConfig("defaults", "client_write_buffer", cfg.Defaults.ClientWriteBuffer)
	for i, stream := range cfg.Streams {
		checkBufferConfig(fmt.Sprintf("stream %s (index %d)", stream.Name, i+1), "listen_read_buffer", stream.ListenReadBuffer)
		checkBufferConfig(fmt.Sprintf("stream %s (index %d)", stream.Name, i+1), "listen_write_buffer", stream.ListenWriteBuffer)
		checkBufferConfig(fmt.Sprintf("stream %s (index %d)", stream.Name, i+1), "client_read_buffer", stream.ClientReadBuffer)
		checkBufferConfig(fmt.Sprintf("stream %s (index %d)", stream.Name, i+1), "client_write_buffer", stream.ClientWriteBuffer)
	}

	return &cfg, nil
}

func checkBufferConfig(source, fieldName string, value *int) {
	if value != nil && *value <= 0 {
		fmt.Fprintf(os.Stderr, "WARN: Config value %s for %s is %d (not positive). OS default or absolute default will be used during resolution.\n", fieldName, source, *value)
	}
}

func intPtr(i int) *int {
	p := i
	return &p
}
