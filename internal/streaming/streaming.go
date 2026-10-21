package streaming

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/url"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/status"

	"github.com/orbitflare/orbitflare-sdk-go/internal/endpoint"
	"github.com/orbitflare/orbitflare-sdk-go/orbitflare"
)

const (
	DefaultPingInterval    = 10 * time.Second
	DefaultMaxMissedPongs  = 3
	DefaultTimeout         = 30 * time.Second
	DefaultKeepalive       = 60 * time.Second
	DefaultChannelCapacity = 4096
)

type Result[T any] struct {
	Update T
	Err    error
}

type Config struct {
	Endpoints       *endpoint.Set
	APIKey          string
	Retry           orbitflare.RetryPolicy
	Timeout         time.Duration
	Keepalive       time.Duration
	PingInterval    time.Duration
	MaxMissedPongs  uint32
	ChannelCapacity int
	Logger          *slog.Logger
}

type apiKeyCredentials string

func (k apiKeyCredentials) GetRequestMetadata(context.Context, ...string) (map[string]string, error) {
	return map[string]string{"x-token": string(k)}, nil
}

func (apiKeyCredentials) RequireTransportSecurity() bool { return false }

func (c *Config) Connect(ctx context.Context, rawURL string) (*grpc.ClientConn, error) {
	target, creds, err := parseTarget(rawURL)
	if err != nil {
		return nil, &orbitflare.ConfigError{Message: fmt.Sprintf("invalid endpoint: %v", err)}
	}

	opts := []grpc.DialOption{
		grpc.WithTransportCredentials(creds),
		grpc.WithKeepaliveParams(keepalive.ClientParameters{
			Time:                c.Keepalive,
			Timeout:             c.Timeout,
			PermitWithoutStream: true,
		}),
	}
	if c.APIKey != "" {
		opts = append(opts, grpc.WithPerRPCCredentials(apiKeyCredentials(c.APIKey)))
	}
	conn, err := grpc.NewClient(target, opts...)
	if err != nil {
		return nil, orbitflare.NewTransportError(err)
	}

	dialCtx, cancel := context.WithTimeout(ctx, c.Timeout)
	defer cancel()
	conn.Connect()
	for conn.GetState() != connectivity.Ready {
		if !conn.WaitForStateChange(dialCtx, conn.GetState()) {
			conn.Close()
			return nil, orbitflare.TransportErrorf("connect timed out")
		}
	}
	return conn, nil
}

func parseTarget(raw string) (string, credentials.TransportCredentials, error) {
	u, err := url.Parse(raw)
	if err != nil {
		return "", nil, err
	}
	host := u.Host
	scheme := u.Scheme
	if host == "" {
		host, scheme = raw, "http"
	}
	switch scheme {
	case "https":
		return withPort(host, "443"), credentials.NewTLS(&tls.Config{}), nil
	case "http", "":
		return withPort(host, "80"), insecure.NewCredentials(), nil
	default:
		return "", nil, fmt.Errorf("unsupported scheme %q", scheme)
	}
}

func withPort(host, defaultPort string) string {
	if _, _, err := net.SplitHostPort(host); err == nil {
		return host
	}
	return host + ":" + defaultPort
}

func RecvError(err error) error {
	if errors.Is(err, io.EOF) {
		return &orbitflare.StreamError{Message: "stream ended"}
	}
	if st, ok := status.FromError(err); ok {
		return &orbitflare.GRPCError{Code: int32(st.Code()), Message: st.Message()}
	}
	return orbitflare.NewTransportError(err)
}

type RunConn func(ctx context.Context, conn *grpc.ClientConn, onConnected func()) error

func RunStream[T any](ctx context.Context, cfg *Config, label string, out chan<- Result[T], runConn RunConn) {
	defer close(out)

	var attempt uint32
	for {
		if ctx.Err() != nil {
			return
		}

		idx := cfg.Endpoints.Pick()
		url := cfg.Endpoints.Get(idx)
		attempt++
		cfg.Logger.Info("connecting", "endpoint", orbitflare.SanitizeURL(url), "attempt", attempt, "stream", label)

		runErr := connectAndRun(ctx, cfg, idx, url, label, &attempt, runConn)
		if runErr == nil {
			return
		}

		cfg.Endpoints.MarkFailure(idx)
		if !cfg.Retry.HasAttemptsLeft(attempt) {
			cfg.Logger.Error("reconnect attempts exhausted", "endpoint", orbitflare.SanitizeURL(url), "attempt", attempt, "stream", label, "error", runErr)
			select {
			case out <- Result[T]{Err: orbitflare.WithEndpoint(runErr, url)}:
			case <-ctx.Done():
			}
			return
		}

		delay := cfg.Retry.DelayForAttempt(attempt)
		cfg.Logger.Warn("reconnecting", "error", runErr, "endpoint", orbitflare.SanitizeURL(url),
			"attempt", attempt, "delay_ms", delay.Milliseconds(), "stream", label)
		select {
		case <-ctx.Done():
			return
		case <-time.After(delay):
		}
	}
}

func connectAndRun(ctx context.Context, cfg *Config, idx int, url, label string, attempt *uint32, runConn RunConn) error {
	conn, err := cfg.Connect(ctx, url)
	if err != nil {
		return err
	}
	defer conn.Close()

	return runConn(ctx, conn, func() {
		cfg.Endpoints.MarkSuccess(idx)
		*attempt = 0
		cfg.Logger.Info("connected, streaming", "stream", label)
	})
}

func UnaryWithFailover[T any](ctx context.Context, cfg *Config, call func(ctx context.Context, conn *grpc.ClientConn) (T, error)) (T, error) {
	var zero T
	var attempt uint32
	for {
		attempt++
		idx := cfg.Endpoints.Pick()
		url := cfg.Endpoints.Get(idx)

		v, err := func() (T, error) {
			conn, err := cfg.Connect(ctx, url)
			if err != nil {
				return zero, err
			}
			defer conn.Close()
			return call(ctx, conn)
		}()
		if err == nil {
			cfg.Endpoints.MarkSuccess(idx)
			return v, nil
		}

		cfg.Endpoints.MarkFailure(idx)
		if !orbitflare.IsRetryable(err) || !cfg.Retry.HasAttemptsLeft(attempt) {
			return zero, orbitflare.WithEndpoint(err, url)
		}
		select {
		case <-ctx.Done():
			return zero, ctx.Err()
		case <-time.After(cfg.Retry.DelayForAttempt(attempt)):
		}
	}
}
