package endpoint

import (
	"sync"
	"time"
)

type endpoint struct {
	url                 string
	consecutiveFailures uint32
	availableAfter      time.Time
}

type Set struct {
	mu        sync.Mutex
	endpoints []endpoint
}

func New(primary string, fallbacks []string) *Set {
	now := time.Now()
	eps := make([]endpoint, 0, 1+len(fallbacks))
	eps = append(eps, endpoint{url: primary, availableAfter: now})
	for _, fb := range fallbacks {
		eps = append(eps, endpoint{url: fb, availableAfter: now})
	}
	return &Set{endpoints: eps}
}

func (s *Set) Get(idx int) string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.endpoints[idx].url
}

func (s *Set) Len() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.endpoints)
}

func (s *Set) Pick() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := time.Now()

	for i := range s.endpoints {
		if !now.Before(s.endpoints[i].availableAfter) {
			return i
		}
	}

	best := 0
	for i := range s.endpoints {
		if s.endpoints[i].availableAfter.Before(s.endpoints[best].availableAfter) {
			best = i
		}
	}
	return best
}

func (s *Set) MarkSuccess(idx int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if idx < 0 || idx >= len(s.endpoints) {
		return
	}
	s.endpoints[idx].consecutiveFailures = 0
	s.endpoints[idx].availableAfter = time.Now()
}

func (s *Set) MarkFailure(idx int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if idx < 0 || idx >= len(s.endpoints) {
		return
	}
	s.endpoints[idx].consecutiveFailures++
	s.endpoints[idx].availableAfter = time.Now().Add(cooldown(s.endpoints[idx].consecutiveFailures))
}

func cooldown(failures uint32) time.Duration {
	shift := failures
	if shift > 4 {
		shift = 4
	}
	secs := uint64(5) << shift
	if secs > 60 {
		secs = 60
	}
	return time.Duration(secs) * time.Second
}
