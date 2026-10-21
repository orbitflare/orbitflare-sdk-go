package streaming

import (
	"context"
	"os"
	"testing"
)

func TestParseTargetSchemes(t *testing.T) {
	cases := []struct {
		raw            string
		wantTarget     string
		wantSecurityIs string
	}{
		{"http://fra.grpc.orbitflare.com", "fra.grpc.orbitflare.com:80", "insecure"},
		{"http://fra.grpc.orbitflare.com:10000", "fra.grpc.orbitflare.com:10000", "insecure"},
		{"https://fra.grpc.orbitflare.com", "fra.grpc.orbitflare.com:443", "tls"},
		{"https://fra.grpc.orbitflare.com:8443", "fra.grpc.orbitflare.com:8443", "tls"},
		{"fra.grpc.orbitflare.com:9000", "fra.grpc.orbitflare.com:9000", "insecure"},
	}
	for _, tc := range cases {
		target, creds, err := parseTarget(tc.raw)
		if err != nil {
			t.Fatalf("%s: unexpected error: %v", tc.raw, err)
		}
		if target != tc.wantTarget {
			t.Errorf("%s: target = %q, want %q", tc.raw, target, tc.wantTarget)
		}
		if got := creds.Info().SecurityProtocol; got != tc.wantSecurityIs {
			t.Errorf("%s: security = %q, want %q", tc.raw, got, tc.wantSecurityIs)
		}
	}
}

func TestParseTargetRejectsUnknownScheme(t *testing.T) {
	if _, _, err := parseTarget("ftp://example.com"); err == nil {
		t.Fatal("expected error for unsupported scheme")
	}
}

func TestConfigBuilderRequiresURL(t *testing.T) {
	os.Unsetenv("ORBITFLARE_TEST_STREAM_URL")
	if _, err := NewConfigBuilder().Build("ORBITFLARE_TEST_STREAM_URL"); err == nil {
		t.Fatal("expected error when no URL is provided")
	}
}

func TestConfigBuilderReadsEnv(t *testing.T) {
	t.Setenv("ORBITFLARE_TEST_STREAM_URL", "http://example.com:9000")
	cfg, err := NewConfigBuilder().Build("ORBITFLARE_TEST_STREAM_URL")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg.Endpoints.Len() != 1 {
		t.Fatalf("expected 1 endpoint, got %d", cfg.Endpoints.Len())
	}
	if cfg.Logger == nil {
		t.Fatal("logger should be resolved to a non-nil value")
	}
}

func TestConfigBuilderDefaults(t *testing.T) {
	b := NewConfigBuilder()
	if b.timeout != DefaultTimeout || b.pingInterval != DefaultPingInterval ||
		b.maxMissedPongs != DefaultMaxMissedPongs || b.channelCapacity != DefaultChannelCapacity {
		t.Fatal("unexpected streaming defaults")
	}
}

func TestAPIKeyCredentialsSendXToken(t *testing.T) {
	md, err := apiKeyCredentials("ORBIT-123").GetRequestMetadata(context.Background())
	if err != nil || md["x-token"] != "ORBIT-123" {
		t.Fatalf("got %v, %v", md, err)
	}
	if apiKeyCredentials("x").RequireTransportSecurity() {
		t.Fatal("must work over plaintext connections")
	}
}

func TestAPIKeyFallsBackToEnv(t *testing.T) {
	t.Setenv("ORBITFLARE_LICENSE_KEY", "ORBIT-ENV")
	b := NewConfigBuilder()
	b.URL("http://localhost:1")
	cfg, err := b.Build("UNUSED")
	if err != nil || cfg.APIKey != "ORBIT-ENV" {
		t.Fatalf("got %q, %v", cfg.APIKey, err)
	}
	b.APIKey("ORBIT-EXPLICIT")
	cfg, _ = b.Build("UNUSED")
	if cfg.APIKey != "ORBIT-EXPLICIT" {
		t.Fatalf("explicit key should win, got %q", cfg.APIKey)
	}
}
