package streaming

import (
	"log/slog"
	"os"
	"time"

	"github.com/orbitflare/orbitflare-sdk-go/internal/endpoint"
	"github.com/orbitflare/orbitflare-sdk-go/orbitflare"
)

type ConfigBuilder struct {
	url             string
	hasURL          bool
	fallbacks       []string
	retry           orbitflare.RetryPolicy
	timeout         time.Duration
	keepalive       time.Duration
	pingInterval    time.Duration
	maxMissedPongs  uint32
	channelCapacity int
	logger          *slog.Logger
}

func NewConfigBuilder() *ConfigBuilder {
	return &ConfigBuilder{
		retry:           orbitflare.DefaultRetryPolicy(),
		timeout:         DefaultTimeout,
		keepalive:       DefaultKeepalive,
		pingInterval:    DefaultPingInterval,
		maxMissedPongs:  DefaultMaxMissedPongs,
		channelCapacity: DefaultChannelCapacity,
	}
}

func (b *ConfigBuilder) URL(url string) { b.url, b.hasURL = url, true }

func (b *ConfigBuilder) URLs(urls ...string) {
	if len(urls) > 0 {
		b.url, b.hasURL = urls[0], true
		b.fallbacks = append([]string(nil), urls[1:]...)
	}
}

func (b *ConfigBuilder) FallbackURL(url string)      { b.fallbacks = append(b.fallbacks, url) }
func (b *ConfigBuilder) FallbackURLs(urls ...string) { b.fallbacks = append(b.fallbacks, urls...) }

func (b *ConfigBuilder) Retry(p orbitflare.RetryPolicy) { b.retry = p }
func (b *ConfigBuilder) Timeout(d time.Duration)        { b.timeout = d }
func (b *ConfigBuilder) Keepalive(d time.Duration)      { b.keepalive = d }
func (b *ConfigBuilder) PingInterval(d time.Duration)   { b.pingInterval = d }
func (b *ConfigBuilder) MaxMissedPongs(n uint32)        { b.maxMissedPongs = n }
func (b *ConfigBuilder) ChannelCapacity(n int)          { b.channelCapacity = n }
func (b *ConfigBuilder) Logger(l *slog.Logger)          { b.logger = l }

func (b *ConfigBuilder) Build(envVar string) (*Config, error) {
	url := b.url
	if !b.hasURL {
		url = os.Getenv(envVar)
	}
	if url == "" {
		return nil, &orbitflare.ConfigError{Message: "no URL provided. Call URL() on the builder or set " + envVar + " in your environment"}
	}

	return &Config{
		Endpoints:       endpoint.New(url, b.fallbacks),
		Retry:           b.retry,
		Timeout:         b.timeout,
		Keepalive:       b.keepalive,
		PingInterval:    b.pingInterval,
		MaxMissedPongs:  b.maxMissedPongs,
		ChannelCapacity: b.channelCapacity,
		Logger:          orbitflare.ResolveLogger(b.logger),
	}, nil
}
