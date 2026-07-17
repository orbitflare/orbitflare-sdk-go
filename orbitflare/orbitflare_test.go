package orbitflare

import (
	"errors"
	"strings"
	"testing"
	"time"
)

func TestApplyAPIKey(t *testing.T) {
	cases := []struct {
		name, url, key, want string
	}{
		{"appends_key", "http://fra.rpc.orbitflare.com", "ORBIT-123", "http://fra.rpc.orbitflare.com/?api_key=ORBIT-123"},
		{"keeps_existing_path", "http://fra.rpc.orbitflare.com/v1", "ORBIT-123", "http://fra.rpc.orbitflare.com/v1?api_key=ORBIT-123"},
		{"appends_with_existing_query", "http://fra.rpc.orbitflare.com?foo=bar", "ORBIT-123", "http://fra.rpc.orbitflare.com?foo=bar&api_key=ORBIT-123"},
		{"skips_if_already_present", "http://fra.rpc.orbitflare.com?api_key=ORBIT-ABC", "ORBIT-123", "http://fra.rpc.orbitflare.com?api_key=ORBIT-ABC"},
		{"skips_if_empty_key", "http://fra.rpc.orbitflare.com", "", "http://fra.rpc.orbitflare.com"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := ApplyAPIKey(c.url, c.key); got != c.want {
				t.Fatalf("ApplyAPIKey(%q,%q) = %q, want %q", c.url, c.key, got, c.want)
			}
		})
	}
}

func TestSanitizeURL(t *testing.T) {
	cases := []struct{ in, want string }{
		{"http://fra.rpc.orbitflare.com/?api_key=SECRET", "http://fra.rpc.orbitflare.com/"},
		{"http://fra.rpc.orbitflare.com/?api_key=SECRET&foo=bar", "http://fra.rpc.orbitflare.com/&foo=bar"},
		{"http://fra.rpc.orbitflare.com/?foo=bar&api_key=SECRET", "http://fra.rpc.orbitflare.com/?foo=bar"},
		{"http://fra.rpc.orbitflare.com/", "http://fra.rpc.orbitflare.com/"},
		{"connect to (http://x?api_key=SECRET) failed", "connect to (http://x) failed"},
	}
	for _, c := range cases {
		if got := SanitizeURL(c.in); got != c.want {
			t.Fatalf("SanitizeURL(%q) = %q, want %q", c.in, got, c.want)
		}
	}
}

func TestTransportErrorSanitizesAPIKey(t *testing.T) {
	raw := errors.New(`Post "http://127.0.0.1:1/?api_key=ORBIT-SECRET-123": dial tcp 127.0.0.1:1: connect: connection refused`)
	te := NewTransportError(raw)

	for _, msg := range []string{te.Error(), WithEndpoint(te, "http://ny.rpc.orbitflare.com/?api_key=ORBIT-SECRET-123").Error()} {
		if strings.Contains(msg, "ORBIT-SECRET-123") {
			t.Fatalf("api_key leaked in error message: %q", msg)
		}
	}
}

func TestIsRetryable(t *testing.T) {
	retryable := []error{
		&TransportError{Err: errors.New("boom")},
		ErrTimeout,
		&RateLimitedError{},
		&StreamError{Message: "closed"},
		&RPCError{Code: -32005, Message: "node behind"},
		&RPCError{Code: -32016, Message: "min context slot"},
	}
	for _, e := range retryable {
		if !IsRetryable(e) {
			t.Fatalf("expected retryable: %v", e)
		}
	}
	notRetryable := []error{
		nil,
		&RPCError{Code: -32602, Message: "invalid params"},
		&AuthError{Message: "bad key"},
		&ConfigError{Message: "no url"},
		&SerializationError{Message: "bad json"},
		&GRPCError{Code: 3, Message: "invalid argument"},
	}
	for _, e := range notRetryable {
		if IsRetryable(e) {
			t.Fatalf("expected NOT retryable: %v", e)
		}
	}
}

func TestIsRetryableThroughWrapping(t *testing.T) {
	wrapped := errors.Join(errors.New("context"), &TransportError{Err: errors.New("boom")})
	if !IsRetryable(wrapped) {
		t.Fatalf("expected wrapped transport error to be retryable")
	}
}

func TestRetryAfter(t *testing.T) {
	d := 3 * time.Second
	if got := RetryAfter(&RateLimitedError{RetryAfter: &d}); got == nil || *got != d {
		t.Fatalf("RetryAfter = %v, want %v", got, d)
	}
	if got := RetryAfter(&RateLimitedError{}); got != nil {
		t.Fatalf("expected nil retry-after")
	}
	if got := RetryAfter(errors.New("other")); got != nil {
		t.Fatalf("expected nil for non-rate-limit error")
	}
}

func TestWithEndpointAnnotatesAndSanitizes(t *testing.T) {
	err := WithEndpoint(&RPCError{Code: 42, Message: "boom"}, "http://ny.rpc.orbitflare.com/?api_key=SECRET")
	var re *RPCError
	if !errors.As(err, &re) {
		t.Fatalf("expected RPCError, got %T", err)
	}
	if re.Endpoint != "http://ny.rpc.orbitflare.com/" {
		t.Fatalf("endpoint not sanitized: %q", re.Endpoint)
	}
	if want := "rpc error 42: [http://ny.rpc.orbitflare.com/] boom"; re.Error() != want {
		t.Fatalf("Error() = %q, want %q", re.Error(), want)
	}

	auth := &AuthError{Message: "x"}
	if WithEndpoint(auth, "http://y") != error(auth) {
		t.Fatalf("expected auth error unchanged")
	}
}

func TestRetryPolicyDefaults(t *testing.T) {
	p := DefaultRetryPolicy()
	if p.InitialDelay != 100*time.Millisecond || p.MaxDelay != 30*time.Second || p.Multiplier != 2.0 || p.MaxAttempts != 0 {
		t.Fatalf("unexpected defaults: %+v", p)
	}
}

func TestHasAttemptsLeft(t *testing.T) {
	inf := RetryPolicy{MaxAttempts: 0}
	if !inf.HasAttemptsLeft(1_000_000) {
		t.Fatalf("infinite policy should always have attempts")
	}
	fin := RetryPolicy{MaxAttempts: 3}
	if !fin.HasAttemptsLeft(2) || fin.HasAttemptsLeft(3) || fin.HasAttemptsLeft(4) {
		t.Fatalf("finite policy boundary wrong")
	}
}

func TestDelayForAttemptGrowsAndClamps(t *testing.T) {
	p := RetryPolicy{InitialDelay: 100 * time.Millisecond, MaxDelay: 30 * time.Second, Multiplier: 2.0}

	d1 := p.DelayForAttempt(1)
	if d1 < 100*time.Millisecond || d1 > 110*time.Millisecond {
		t.Fatalf("attempt 1 delay out of range: %v", d1)
	}

	d3 := p.DelayForAttempt(3)
	if d3 < 400*time.Millisecond || d3 > 440*time.Millisecond {
		t.Fatalf("attempt 3 delay out of range: %v", d3)
	}

	dBig := p.DelayForAttempt(50)
	if dBig < 30*time.Second || dBig > 33*time.Second {
		t.Fatalf("clamped delay out of range: %v", dBig)
	}

	if d0 := p.DelayForAttempt(0); d0 < 100*time.Millisecond || d0 > 110*time.Millisecond {
		t.Fatalf("attempt 0 delay out of range: %v", d0)
	}
}
