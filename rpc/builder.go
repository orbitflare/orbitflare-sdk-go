package rpc

import (
	"log/slog"
	"net/http"
	"os"
	"time"

	"github.com/orbitflare/orbitflare-sdk-go/internal/endpoint"
	"github.com/orbitflare/orbitflare-sdk-go/orbitflare"
)

type Builder struct {
	url        string
	hasURL     bool
	fallbacks  []string
	apiKey     string
	commitment string
	retry      orbitflare.RetryPolicy
	timeout    time.Duration
	logger     *slog.Logger
}

func NewBuilder() *Builder {
	return &Builder{
		commitment: "confirmed",
		retry:      orbitflare.DefaultRetryPolicy(),
		timeout:    30 * time.Second,
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

func (b *Builder) Commitment(commitment string) *Builder {
	b.commitment = commitment
	return b
}

func (b *Builder) Retry(policy orbitflare.RetryPolicy) *Builder {
	b.retry = policy
	return b
}

func (b *Builder) Timeout(timeout time.Duration) *Builder {
	b.timeout = timeout
	return b
}

func (b *Builder) Logger(l *slog.Logger) *Builder {
	b.logger = l
	return b
}

func (b *Builder) Build() (*Client, error) {
	url := b.url
	if !b.hasURL {
		url = os.Getenv("ORBITFLARE_RPC_URL")
	}
	if url == "" {
		return nil, &orbitflare.ConfigError{Message: "no RPC URL provided. Call URL() on the builder or set ORBITFLARE_RPC_URL in your environment"}
	}

	return &Client{
		http:       &http.Client{Timeout: b.timeout},
		endpoints:  endpoint.New(url, b.fallbacks),
		apiKey:     b.apiKey,
		commitment: b.commitment,
		retry:      b.retry,
		logger:     orbitflare.ResolveLogger(b.logger),
	}, nil
}
