package streaming

import (
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
