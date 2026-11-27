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
	"strings"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"

	"github.com/orbitflare/orbitflare-sdk-go/internal/endpoint"
	"github.com/orbitflare/orbitflare-sdk-go/internal/events"
	"github.com/orbitflare/orbitflare-sdk-go/orbitflare"
)

const (
	DefaultPingInterval    = 10 * time.Second
	DefaultMaxMissedPongs  = 3
	DefaultTimeout         = 30 * time.Second
	DefaultKeepalive       = 20 * time.Second
	DefaultChannelCapacity = 4096

	DefaultMaxDecodingMessageSize = 64 * 1024 * 1024

	keepaliveTimeout = 10 * time.Second
)

type Result[T any] struct {
	Update T
	Err    error
}

type Config struct {
	Endpoints              *endpoint.Set
	APIKey                 string
	Retry                  orbitflare.RetryPolicy
	Timeout                time.Duration
	Keepalive              time.Duration
	PingInterval           time.Duration
	MaxMissedPongs         uint32
	ChannelCapacity        int
	MaxDecodingMessageSize int
	Logger                 *slog.Logger
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
			Timeout:             keepaliveTimeout,
			PermitWithoutStream: true,
		}),
		grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(c.MaxDecodingMessageSize)),
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
			err := connectError(conn)
			conn.Close()
			return nil, err
		}
	}
	return conn, nil
}

func connectError(conn *grpc.ClientConn) error {
	if conn.GetState() == connectivity.TransientFailure {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		err := conn.Invoke(ctx, "/orbitflare.Connect/Probe", &emptypb.Empty{}, &emptypb.Empty{})
		if st, ok := status.FromError(err); ok && st.Code() == codes.Unavailable {
			return orbitflare.TransportErrorf("connect timed out: %s", st.Message())
		}
	}
	return orbitflare.TransportErrorf("connect timed out")
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

type Conn struct {
	*grpc.ClientConn
	URL      string
	Endpoint string
	Attempt  uint32
	attempt  *uint32
	onReady  func()
	hub      *events.Hub
}

func (c *Conn) Connected() { c.onReady() }

func (c *Conn) Message() { *c.attempt = 0 }

func (c *Conn) Emit(ev events.Event) { c.hub.Emit(ev) }

type RunConn func(ctx context.Context, conn *Conn) error

func IsPermanent(err error) bool {
	var ge *orbitflare.GRPCError
	if !errors.As(err, &ge) {
		return false
	}
	switch codes.Code(ge.Code) {
	case codes.InvalidArgument, codes.PermissionDenied, codes.Unimplemented, codes.Unauthenticated:
		return true
	}
	return false
}

func EndpointLabel(raw string) string {
	s := orbitflare.SanitizeURL(raw)
	scheme, rest, ok := strings.Cut(s, "://")
	if !ok {
		return s
	}
	authority, path := rest, ""
	if i := strings.Index(rest, "/"); i >= 0 {
		authority, path = rest[:i], rest[i:]
	}
	if i := strings.LastIndex(authority, "@"); i >= 0 {
		authority = authority[i+1:]
	}
	return scheme + "://" + authority + path
}

func RunStream[T any](ctx context.Context, cfg *Config, label string, out chan<- Result[T], hub *events.Hub, runConn RunConn) {
	defer close(out)
	defer hub.End()

	var attempt uint32
	for {
		if ctx.Err() != nil {
			return
		}

		idx := cfg.Endpoints.Pick()
		url := cfg.Endpoints.Get(idx)
		attempt++
		endpoint := EndpointLabel(url)
		cfg.Logger.Info("connecting", "endpoint", orbitflare.SanitizeURL(url), "attempt", attempt, "stream", label)
		hub.Emit(events.Connecting{Endpoint: endpoint, Attempt: attempt})

		runErr := connectAndRun(ctx, cfg, idx, url, endpoint, label, &attempt, hub, runConn)
		if runErr == nil || ctx.Err() != nil {
			return
		}

		cfg.Endpoints.MarkFailure(idx)
		if IsPermanent(runErr) || !cfg.Retry.HasAttemptsLeft(attempt) {
			cfg.Logger.Error("stream failed", "endpoint", orbitflare.SanitizeURL(url), "attempt", attempt, "stream", label, "error", runErr)
			hub.Emit(events.Disconnected{Endpoint: endpoint, Error: runErr.Error()})
			select {
			case out <- Result[T]{Err: orbitflare.WithEndpoint(runErr, url)}:
			case <-ctx.Done():
			}
			return
		}

		delay := cfg.Retry.DelayForAttempt(attempt)
		hub.Emit(events.Disconnected{Endpoint: endpoint, Error: runErr.Error(), WillRetry: true, RetryIn: delay})
		cfg.Logger.Warn("reconnecting", "error", runErr, "endpoint", orbitflare.SanitizeURL(url),
			"attempt", attempt, "delay_ms", delay.Milliseconds(), "stream", label)
		select {
		case <-ctx.Done():
			return
		case <-time.After(delay):
		}
	}
}

func connectAndRun(ctx context.Context, cfg *Config, idx int, url, endpoint, label string, attempt *uint32, hub *events.Hub, runConn RunConn) error {
	conn, err := cfg.Connect(ctx, url)
	if err != nil {
		return err
	}
	defer conn.Close()

	return runConn(ctx, &Conn{
		ClientConn: conn,
		URL:        url,
		Endpoint:   endpoint,
		Attempt:    *attempt,
		attempt:    attempt,
		hub:        hub,
		onReady: func() {
			cfg.Endpoints.MarkSuccess(idx)
			cfg.Logger.Info("connected, streaming", "stream", label)
			hub.Emit(events.Connected{Endpoint: endpoint})
		},
	})
}

func Header(stream grpc.ClientStream, recv func() error) error {
	md, err := stream.Header()
	if err != nil {
		return RecvError(err)
	}
	if md == nil {
		return RecvError(recv())
	}
	return nil
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
