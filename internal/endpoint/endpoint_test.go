package endpoint

import (
	"testing"
	"time"
)

func TestNewAndLen(t *testing.T) {
	s := New("http://primary", []string{"http://fb1", "http://fb2"})
	if s.Len() != 3 {
		t.Fatalf("Len = %d, want 3", s.Len())
	}
	if s.Get(0) != "http://primary" || s.Get(1) != "http://fb1" || s.Get(2) != "http://fb2" {
		t.Fatalf("unexpected URLs")
	}
}

func TestPickPrefersFirstHealthy(t *testing.T) {
	s := New("http://a", []string{"http://b"})
	if got := s.Pick(); got != 0 {
		t.Fatalf("fresh set Pick = %d, want 0", got)
	}
}

func TestMarkFailureQuarantinesAndPickFallsBack(t *testing.T) {
	s := New("http://a", []string{"http://b"})
	s.MarkFailure(0)
	if got := s.Pick(); got != 1 {
		t.Fatalf("Pick after primary failure = %d, want 1 (fallback)", got)
	}
}

func TestPickReturnsSoonestWhenAllQuarantined(t *testing.T) {
	s := New("http://a", []string{"http://b"})

	s.MarkFailure(1)
	s.MarkFailure(1)
	s.MarkFailure(0)
	if got := s.Pick(); got != 0 {
		t.Fatalf("Pick with all quarantined = %d, want 0 (soonest available)", got)
	}
}

func TestMarkSuccessClearsQuarantine(t *testing.T) {
	s := New("http://a", []string{"http://b"})
	s.MarkFailure(0)
	s.MarkSuccess(0)
	if got := s.Pick(); got != 0 {
		t.Fatalf("Pick after MarkSuccess = %d, want 0", got)
	}
}

func TestMarkOutOfRangeIsSafe(t *testing.T) {
	s := New("http://a", nil)
	s.MarkFailure(5)
	s.MarkSuccess(-1)
	if s.Pick() != 0 {
		t.Fatalf("expected primary still selectable")
	}
}

func TestCooldownSchedule(t *testing.T) {
	cases := []struct {
		failures uint32
		want     time.Duration
	}{
		{1, 10 * time.Second},
		{2, 20 * time.Second},
		{3, 40 * time.Second},
		{4, 60 * time.Second},
		{5, 60 * time.Second},
		{10, 60 * time.Second},
	}
	for _, c := range cases {
		if got := cooldown(c.failures); got != c.want {
			t.Fatalf("cooldown(%d) = %v, want %v", c.failures, got, c.want)
		}
	}
}
