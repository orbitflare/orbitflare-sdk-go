package orbitflare

import (
	"math"
	"time"
)

type RetryPolicy struct {
	InitialDelay time.Duration
	MaxDelay     time.Duration
	Multiplier   float64
	MaxAttempts  uint32
}

func DefaultRetryPolicy() RetryPolicy {
	return RetryPolicy{
		InitialDelay: 100 * time.Millisecond,
		MaxDelay:     30 * time.Second,
		Multiplier:   2.0,
		MaxAttempts:  0,
	}
}

func (p RetryPolicy) DelayForAttempt(attempt uint32) time.Duration {
	exp := int32(0)
	if attempt > 0 {
		exp = int32(attempt - 1)
	}
	base := float64(p.InitialDelay.Milliseconds()) * math.Pow(p.Multiplier, float64(exp))
	clamped := math.Min(base, float64(p.MaxDelay.Milliseconds()))

	nanos := time.Now().Nanosecond()
	jitter := (float64(nanos) / float64(math.MaxUint32)) * clamped * 0.25
	return time.Duration(uint64(clamped+jitter)) * time.Millisecond
}

func (p RetryPolicy) HasAttemptsLeft(attempt uint32) bool {
	return p.MaxAttempts == 0 || attempt < p.MaxAttempts
}
