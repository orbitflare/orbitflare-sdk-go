package ws

import (
	"context"
	"log/slog"
	"os"
	"time"

	"github.com/orbitflare/orbitflare-sdk-go/internal/endpoint"
	"github.com/orbitflare/orbitflare-sdk-go/orbitflare"
)

const (
	defaultPingIntervalSecs = 10
	defaultMaxMissedPongs   = 3
)

type Builder struct {
	url            string
	hasURL         bool
	fallbacks      []string
	apiKey         string
	retry          orbitflare.RetryPolicy
	pingInterval   time.Duration
	maxMissedPongs uint32
	logger         *slog.Logger
}

func NewBuilder() *Builder {
	return &Builder{
		retry:          orbitflare.DefaultRetryPolicy(),
		pingInterval:   defaultPingIntervalSecs * time.Second,
		maxMissedPongs: defaultMaxMissedPongs,
	}
}

func (b *Builder) URL(url string) *Builder {
	b.url, b.hasURL = url, true
	return b
}

func (b *Builder) URLs(urls ...string) *Builder {
	if len(urls) > 0 {
		b.url, b.hasURL = urls[0], true
		b.fallbacks = append([]string(nil), urls[1:]...)
	}
	return b
}

func (b *Builder) FallbackURL(url string) *Builder {
	b.fallbacks = append(b.fallbacks, url)
	return b
}

func (b *Builder) FallbackURLs(urls ...string) *Builder {
	b.fallbacks = append(b.fallbacks, urls...)
	return b
}

func (b *Builder) APIKey(key string) *Builder {
	b.apiKey = key
	return b
}

func (b *Builder) Retry(policy orbitflare.RetryPolicy) *Builder {
	b.retry = policy
	return b
}

func (b *Builder) PingInterval(d time.Duration) *Builder {
	b.pingInterval = d
	return b
}

func (b *Builder) MaxMissedPongs(n uint32) *Builder {
	b.maxMissedPongs = n
	return b
}

func (b *Builder) Logger(l *slog.Logger) *Builder {
	b.logger = l
	return b
}

func (b *Builder) Build(ctx context.Context) (*Client, error) {
	url := b.url
	if !b.hasURL {
		url = os.Getenv("ORBITFLARE_WS_URL")
	}
	if url == "" {
		return nil, &orbitflare.ConfigError{Message: "no WebSocket URL provided. Call URL() on the builder or set ORBITFLARE_WS_URL in your environment"}
	}

	apiKey := b.apiKey
	if apiKey == "" {
		apiKey = os.Getenv("ORBITFLARE_LICENSE_KEY")
	}
	apply := func(u string) string {
		if apiKey == "" {
			return u
		}
		return orbitflare.ApplyAPIKey(u, apiKey)
	}
	primary := apply(url)
	fallbacks := make([]string, len(b.fallbacks))
	for i, fb := range b.fallbacks {
		fallbacks[i] = apply(fb)
	}

	endpoints := endpoint.New(primary, fallbacks)
	logger := orbitflare.ResolveLogger(b.logger)

	conn, _, err := dial(ctx, endpoints, logger)
	if err != nil {
		return nil, err
	}

	bgCtx, cancel := context.WithCancel(context.Background())
	bg := &background{
		endpoints:      endpoints,
		cmd:            make(chan command, 64),
		retry:          b.retry,
		pingInterval:   b.pingInterval,
		maxMissedPongs: b.maxMissedPongs,
		logger:         logger,
		done:           make(chan struct{}),
		ctx:            bgCtx,
	}

	go bg.run(conn)

	return &Client{bg: bg, cancel: cancel}, nil
}
