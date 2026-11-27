package events

import (
	"context"
	"sync"
	"time"
)

const Capacity = 256

type Event interface{ connectionEvent() }

type Connecting struct {
	Endpoint string
	Attempt  uint32
}

type Connected struct {
	Endpoint string
}

type Disconnected struct {
	Endpoint  string
	Error     string
	WillRetry bool
	RetryIn   time.Duration
}

type ReplayClamped struct {
	Requested uint64
	Available uint64
}

type ReplayUnavailable struct {
	Requested uint64
}

type Lagged struct {
	Missed uint64
}

func (Connecting) connectionEvent()        {}
func (Connected) connectionEvent()         {}
func (Disconnected) connectionEvent()      {}
func (ReplayClamped) connectionEvent()     {}
func (ReplayUnavailable) connectionEvent() {}
func (Lagged) connectionEvent()            {}

type Events struct {
	mu     sync.Mutex
	queue  []Event
	missed uint64
	closed bool
	notify chan struct{}
}

func newEvents() *Events { return &Events{notify: make(chan struct{}, 1)} }

func (e *Events) push(ev Event) {
	e.mu.Lock()
	if e.closed {
		e.mu.Unlock()
		return
	}
	if len(e.queue) >= Capacity {
		e.queue = e.queue[1:]
		e.missed++
	}
	e.queue = append(e.queue, ev)
	e.mu.Unlock()
	e.wake()
}

func (e *Events) end() {
	e.mu.Lock()
	e.closed = true
	e.mu.Unlock()
	e.wake()
}

func (e *Events) wake() {
	select {
	case e.notify <- struct{}{}:
	default:
	}
}

func (e *Events) TryRecv() (Event, bool) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.missed > 0 {
		missed := e.missed
		e.missed = 0
		return Lagged{Missed: missed}, true
	}
	if len(e.queue) > 0 {
		ev := e.queue[0]
		e.queue = e.queue[1:]
		return ev, true
	}
	return nil, false
}

func (e *Events) Recv(ctx context.Context) (Event, bool) {
	for {
		if ev, ok := e.TryRecv(); ok {
			return ev, true
		}
		e.mu.Lock()
		closed := e.closed
		e.mu.Unlock()
		if closed {
			if ev, ok := e.TryRecv(); ok {
				return ev, true
			}
			return nil, false
		}
		select {
		case <-e.notify:
		case <-ctx.Done():
			return nil, false
		}
	}
}

type Hub struct {
	mu      sync.Mutex
	readers []*Events
	first   *Events
	ended   bool
}

func NewHub() *Hub {
	first := newEvents()
	return &Hub{readers: []*Events{first}, first: first}
}

func (h *Hub) Subscribe() *Events {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.first != nil {
		r := h.first
		h.first = nil
		return r
	}
	r := newEvents()
	if h.ended {
		r.closed = true
		return r
	}
	h.readers = append(h.readers, r)
	return r
}

func (h *Hub) Emit(ev Event) {
	h.mu.Lock()
	readers := h.readers
	h.mu.Unlock()
	for _, r := range readers {
		r.push(ev)
	}
}

func (h *Hub) End() {
	h.mu.Lock()
	h.ended = true
	readers := h.readers
	h.mu.Unlock()
	for _, r := range readers {
		r.end()
	}
}
