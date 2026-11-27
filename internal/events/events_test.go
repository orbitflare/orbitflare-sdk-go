package events

import (
	"context"
	"testing"
	"time"
)

func recv(t *testing.T, e *Events) Event {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	ev, ok := e.Recv(ctx)
	if !ok {
		t.Fatal("no event")
	}
	return ev
}

func TestFirstReaderSeesEventsSentBeforeItSubscribed(t *testing.T) {
	hub := NewHub()
	hub.Emit(Connected{Endpoint: "a"})
	first := hub.Subscribe()
	second := hub.Subscribe()
	hub.Emit(Connected{Endpoint: "b"})

	if got := recv(t, first); got != (Connected{Endpoint: "a"}) {
		t.Fatalf("first got %#v", got)
	}
	if got := recv(t, first); got != (Connected{Endpoint: "b"}) {
		t.Fatalf("first got %#v", got)
	}
	if got := recv(t, second); got != (Connected{Endpoint: "b"}) {
		t.Fatalf("second got %#v", got)
	}
}

func TestSlowReadersAreToldWhatTheyMissed(t *testing.T) {
	hub := NewHub()
	events := hub.Subscribe()
	for i := 0; i < Capacity+10; i++ {
		hub.Emit(Connecting{Endpoint: "a", Attempt: uint32(i)})
	}
	if got := recv(t, events); got != (Lagged{Missed: 10}) {
		t.Fatalf("got %#v", got)
	}
	if got := recv(t, events); got != (Connecting{Endpoint: "a", Attempt: 10}) {
		t.Fatalf("got %#v", got)
	}
}

func TestEndsWhenTheStreamIsGone(t *testing.T) {
	hub := NewHub()
	events := hub.Subscribe()
	hub.Emit(Connected{Endpoint: "a"})
	hub.End()
	if got := recv(t, events); got != (Connected{Endpoint: "a"}) {
		t.Fatalf("got %#v", got)
	}
	if _, ok := events.Recv(context.Background()); ok {
		t.Fatal("expected the events to end")
	}
	if _, ok := hub.Subscribe().Recv(context.Background()); ok {
		t.Fatal("a late reader must not block")
	}
}

func TestRecvWakesOnEmit(t *testing.T) {
	hub := NewHub()
	events := hub.Subscribe()
	go func() {
		time.Sleep(20 * time.Millisecond)
		hub.Emit(Connected{Endpoint: "a"})
	}()
	if got := recv(t, events); got != (Connected{Endpoint: "a"}) {
		t.Fatalf("got %#v", got)
	}
	if _, ok := events.TryRecv(); ok {
		t.Fatal("expected nothing queued")
	}
}
