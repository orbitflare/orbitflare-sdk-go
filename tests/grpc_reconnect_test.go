package tests

import (
	"context"
	"errors"
	"io"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"github.com/orbitflare/orbitflare-sdk-go/geyser"
	"github.com/orbitflare/orbitflare-sdk-go/orbitflare"
	pb "github.com/orbitflare/orbitflare-sdk-go/proto/geyser"
)

type recorded struct {
	conn int
	req  *pb.SubscribeRequest
}

type mockGeyser struct {
	pb.UnimplementedGeyserServer
	mu          sync.Mutex
	requests    []recorded
	connections int
	nextSlot    uint64
	dropAfter   int
	replay      *pb.SubscribeReplayInfoResponse
	reject      codes.Code
	payload     []byte
}

func (m *mockGeyser) Subscribe(stream grpc.BidiStreamingServer[pb.SubscribeRequest, pb.SubscribeUpdate]) error {
	m.mu.Lock()
	m.connections++
	conn := m.connections
	m.mu.Unlock()
	if err := stream.SendHeader(metadata.MD{}); err != nil {
		return err
	}

	reqs := make(chan *pb.SubscribeRequest, 16)
	go func() {
		for {
			r, err := stream.Recv()
			if err != nil {
				close(reqs)
				return
			}
			reqs <- r
		}
	}()

	var tick <-chan time.Time
	sent := 0
	for {
		select {
		case r, ok := <-reqs:
			if !ok {
				return nil
			}
			m.mu.Lock()
			m.requests = append(m.requests, recorded{conn, r})
			reject := m.reject
			m.mu.Unlock()
			if r.Ping != nil {
				if err := stream.Send(&pb.SubscribeUpdate{UpdateOneof: &pb.SubscribeUpdate_Pong{Pong: &pb.SubscribeUpdatePong{Id: r.Ping.Id}}}); err != nil {
					return err
				}
			}
			if reject != codes.OK {
				return status.Error(reject, "mock reject")
			}
			if r.Ping != nil || tick != nil {
				continue
			}
			ticker := time.NewTicker(10 * time.Millisecond)
			defer ticker.Stop()
			tick = ticker.C
		case <-tick:
			m.mu.Lock()
			if m.dropAfter > 0 && sent >= m.dropAfter {
				m.mu.Unlock()
				return status.Error(codes.Unavailable, "mock drop")
			}
			slot := m.nextSlot
			m.nextSlot++
			payload := m.payload
			m.mu.Unlock()
			update := &pb.SubscribeUpdate{UpdateOneof: &pb.SubscribeUpdate_Slot{Slot: &pb.SubscribeUpdateSlot{Slot: slot}}}
			if payload != nil {
				update = &pb.SubscribeUpdate{UpdateOneof: &pb.SubscribeUpdate_Account{Account: &pb.SubscribeUpdateAccount{
					Slot: slot, Account: &pb.SubscribeUpdateAccountInfo{Data: payload},
				}}}
			}
			if err := stream.Send(update); err != nil {
				return err
			}
			sent++
		case <-stream.Context().Done():
			return nil
		}
	}
}

func (m *mockGeyser) SubscribeReplayInfo(context.Context, *pb.SubscribeReplayInfoRequest) (*pb.SubscribeReplayInfoResponse, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.replay == nil {
		return nil, status.Error(codes.Unimplemented, "no replay info")
	}
	return m.replay, nil
}

func (m *mockGeyser) subscriptions() []recorded {
	m.mu.Lock()
	defer m.mu.Unlock()
	var out []recorded
	for _, r := range m.requests {
		if r.req.Ping == nil {
			out = append(out, r)
		}
	}
	return out
}

func (m *mockGeyser) connectionCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.connections
}

func serveGeyser(t *testing.T, m *mockGeyser) (string, func()) {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	server := grpc.NewServer()
	pb.RegisterGeyserServer(server, m)
	go server.Serve(lis)
	t.Cleanup(server.Stop)
	return "http://" + lis.Addr().String(), server.Stop
}

func retryPolicy(initial, max time.Duration, attempts uint32) orbitflare.RetryPolicy {
	return orbitflare.RetryPolicy{InitialDelay: initial, MaxDelay: max, Multiplier: 2, MaxAttempts: attempts}
}

func mockClient(t *testing.T, url string, policy orbitflare.RetryPolicy) *geyser.Client {
	t.Helper()
	c, err := geyser.NewBuilder().URL(url).Retry(policy).Timeout(2 * time.Second).APIKey("").Build()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(c.Close)
	return c
}

func named(filter string, fromSlot *uint64) *pb.SubscribeRequest {
	return &pb.SubscribeRequest{
		Transactions: map[string]*pb.SubscribeRequestFilterTransactions{filter: {}},
		FromSlot:     fromSlot,
	}
}

func filterOf(r *pb.SubscribeRequest) string {
	for k := range r.Transactions {
		return k
	}
	return ""
}

func ptr(v uint64) *uint64 { return &v }

func nextUpdate(t *testing.T, s *geyser.Stream) (*pb.SubscribeUpdate, error) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	return s.Next(ctx)
}

func nextSlot(t *testing.T, s *geyser.Stream) uint64 {
	t.Helper()
	u, err := nextUpdate(t, s)
	if err != nil {
		t.Fatalf("next: %v", err)
	}
	if u.GetSlot() == nil {
		t.Fatalf("expected a slot update, got %v", u)
	}
	return u.GetSlot().Slot
}

func drain(events *orbitflare.ConnectionEvents) []orbitflare.ConnectionEvent {
	time.Sleep(200 * time.Millisecond)
	var out []orbitflare.ConnectionEvent
	for ev, ok := events.TryRecv(); ok; ev, ok = events.TryRecv() {
		out = append(out, ev)
	}
	return out
}

func eventually(t *testing.T, what string, check func() bool) {
	t.Helper()
	for i := 0; i < 300; i++ {
		if check() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

func kinds(evs []orbitflare.ConnectionEvent) []string {
	var out []string
	for _, ev := range evs {
		switch ev.(type) {
		case orbitflare.Connecting:
			out = append(out, "connecting")
		case orbitflare.Connected:
			out = append(out, "connected")
		case orbitflare.Disconnected:
			out = append(out, "disconnected")
		case orbitflare.ReplayClamped:
			out = append(out, "replayClamped")
		case orbitflare.ReplayUnavailable:
			out = append(out, "replayUnavailable")
		case orbitflare.Lagged:
			out = append(out, "lagged")
		}
	}
	return out
}

func TestReconnectRebuildsTheRequestAndResumesAfterTheLastSlot(t *testing.T) {
	m := &mockGeyser{nextSlot: 1000, dropAfter: 5, replay: &pb.SubscribeReplayInfoResponse{FirstAvailable: ptr(0)}}
	url, _ := serveGeyser(t, m)
	var mu sync.Mutex
	var infos []geyser.ConnectInfo
	stream := mockClient(t, url, retryPolicy(20*time.Millisecond, 50*time.Millisecond, 0)).SubscribeWith(func(info geyser.ConnectInfo) *pb.SubscribeRequest {
		mu.Lock()
		infos = append(infos, info)
		mu.Unlock()
		if info.LastSlot == nil {
			return named("resume", nil)
		}
		return named("resume", ptr(*info.LastSlot+1))
	})
	defer stream.Close()
	events := stream.Events()

	for i := uint64(0); i < 15; i++ {
		if got := nextSlot(t, stream); got != 1000+i {
			t.Fatalf("slot %d = %d, want %d", i, got, 1000+i)
		}
	}

	mu.Lock()
	if len(infos) < 3 {
		t.Fatalf("expected 3 connects, got %d", len(infos))
	}
	if infos[0].Reconnect || infos[0].LastSlot != nil {
		t.Fatalf("first connect info %+v", infos[0])
	}
	if !infos[1].Reconnect || infos[1].LastSlot == nil || *infos[1].LastSlot != 1004 {
		t.Fatalf("second connect info %+v", infos[1])
	}
	if *infos[2].LastSlot != 1009 {
		t.Fatalf("third connect lastSlot %d", *infos[2].LastSlot)
	}
	mu.Unlock()

	subs := m.subscriptions()
	if subs[0].req.FromSlot != nil {
		t.Fatalf("first request fromSlot %d", *subs[0].req.FromSlot)
	}
	if subs[1].conn != 2 || *subs[1].req.FromSlot != 1005 {
		t.Fatalf("second request conn %d fromSlot %v", subs[1].conn, subs[1].req.FromSlot)
	}
	if subs[2].conn != 3 || *subs[2].req.FromSlot != 1010 {
		t.Fatalf("third request conn %d fromSlot %v", subs[2].conn, subs[2].req.FromSlot)
	}

	seen := drain(events)
	want := []string{"connecting", "connected", "disconnected", "connecting", "connected", "disconnected", "connecting"}
	if got := kinds(seen); len(got) < len(want) || strings.Join(got[:len(want)], ",") != strings.Join(want, ",") {
		t.Fatalf("events %v", got)
	}
	dropped := seen[2].(orbitflare.Disconnected)
	if dropped.Endpoint != url || !strings.Contains(dropped.Error, "mock drop") || !dropped.WillRetry || dropped.RetryIn <= 0 {
		t.Fatalf("disconnected event %+v", dropped)
	}
}

func TestUpdatesApplyInPlaceAndSurviveAReconnect(t *testing.T) {
	m := &mockGeyser{}
	url, _ := serveGeyser(t, m)
	stream := mockClient(t, url, retryPolicy(20*time.Millisecond, 50*time.Millisecond, 0)).Subscribe(named("first", nil))
	defer stream.Close()
	nextSlot(t, stream)

	if err := stream.Handle().Update(named("second", ptr(7))); err != nil {
		t.Fatal(err)
	}
	eventually(t, "the update", func() bool { return len(m.subscriptions()) == 2 })
	subs := m.subscriptions()
	if m.connectionCount() != 1 || subs[1].conn != 1 || filterOf(subs[1].req) != "second" || subs[1].req.FromSlot != nil {
		t.Fatalf("update sent as conn %d filter %q fromSlot %v (connections %d)", subs[1].conn, filterOf(subs[1].req), subs[1].req.FromSlot, m.connectionCount())
	}

	m.mu.Lock()
	m.dropAfter = 1
	m.mu.Unlock()
	eventually(t, "the resubscription", func() bool {
		for _, r := range m.subscriptions() {
			if r.conn == 2 {
				return true
			}
		}
		return false
	})
	for _, r := range m.subscriptions() {
		if r.conn == 2 && (filterOf(r.req) != "second" || r.req.FromSlot != nil) {
			t.Fatalf("resubscribed with filter %q fromSlot %v", filterOf(r.req), r.req.FromSlot)
		}
	}
}

func TestUpdatesSentWhileDisconnectedApplyOnTheNextConnection(t *testing.T) {
	m := &mockGeyser{dropAfter: 1}
	url, _ := serveGeyser(t, m)
	stream := mockClient(t, url, retryPolicy(300*time.Millisecond, 300*time.Millisecond, 0)).Subscribe(named("first", nil))
	defer stream.Close()
	events := stream.Events()
	for {
		ev, ok := events.Recv(context.Background())
		if !ok {
			t.Fatal("events ended")
		}
		if _, ok := ev.(orbitflare.Disconnected); ok {
			break
		}
	}
	if err := stream.Update(named("second", nil)); err != nil {
		t.Fatal(err)
	}
	eventually(t, "the reconnect", func() bool {
		for _, r := range m.subscriptions() {
			if r.conn == 2 {
				return true
			}
		}
		return false
	})
	var second []recorded
	for _, r := range m.subscriptions() {
		if r.conn == 2 {
			second = append(second, r)
		}
	}
	if len(second) != 1 || filterOf(second[0].req) != "second" {
		t.Fatalf("connection 2 requests: %d, first filter %q", len(second), filterOf(second[0].req))
	}
}

func TestFromSlotOlderThanTheReplayWindowIsClamped(t *testing.T) {
	m := &mockGeyser{replay: &pb.SubscribeReplayInfoResponse{FirstAvailable: ptr(500)}}
	url, _ := serveGeyser(t, m)
	stream := mockClient(t, url, retryPolicy(20*time.Millisecond, 50*time.Millisecond, 0)).SubscribeWith(func(geyser.ConnectInfo) *pb.SubscribeRequest {
		return named("x", ptr(100))
	})
	defer stream.Close()
	events := stream.Events()
	nextSlot(t, stream)
	if got := m.subscriptions()[0].req.FromSlot; got == nil || *got != 500 {
		t.Fatalf("fromSlot %v", got)
	}
	found := false
	for _, ev := range drain(events) {
		if ev == (orbitflare.ReplayClamped{Requested: 100, Available: 500}) {
			found = true
		}
	}
	if !found {
		t.Fatal("no ReplayClamped event")
	}
}

func TestFromSlotInsideTheWindowIsKept(t *testing.T) {
	m := &mockGeyser{replay: &pb.SubscribeReplayInfoResponse{FirstAvailable: ptr(50)}}
	url, _ := serveGeyser(t, m)
	stream := mockClient(t, url, retryPolicy(20*time.Millisecond, 50*time.Millisecond, 0)).SubscribeWith(func(geyser.ConnectInfo) *pb.SubscribeRequest {
		return named("x", ptr(100))
	})
	defer stream.Close()
	events := stream.Events()
	nextSlot(t, stream)
	if got := m.subscriptions()[0].req.FromSlot; got == nil || *got != 100 {
		t.Fatalf("fromSlot %v", got)
	}
	for _, k := range kinds(drain(events)) {
		if k == "replayClamped" || k == "replayUnavailable" {
			t.Fatalf("unexpected %s", k)
		}
	}
}

func TestServersWithoutReplayStartLive(t *testing.T) {
	m := &mockGeyser{replay: &pb.SubscribeReplayInfoResponse{}}
	url, _ := serveGeyser(t, m)
	stream := mockClient(t, url, retryPolicy(20*time.Millisecond, 50*time.Millisecond, 0)).SubscribeWith(func(geyser.ConnectInfo) *pb.SubscribeRequest {
		return named("x", ptr(100))
	})
	defer stream.Close()
	events := stream.Events()
	nextSlot(t, stream)
	if got := m.subscriptions()[0].req.FromSlot; got != nil {
		t.Fatalf("fromSlot %d, want none", *got)
	}
	found := false
	for _, ev := range drain(events) {
		if ev == (orbitflare.ReplayUnavailable{Requested: 100}) {
			found = true
		}
	}
	if !found {
		t.Fatal("no ReplayUnavailable event")
	}
}

func TestFailedReplayLookupLeavesFromSlotToTheServer(t *testing.T) {
	m := &mockGeyser{}
	url, _ := serveGeyser(t, m)
	stream := mockClient(t, url, retryPolicy(20*time.Millisecond, 50*time.Millisecond, 0)).SubscribeWith(func(geyser.ConnectInfo) *pb.SubscribeRequest {
		return named("x", ptr(100))
	})
	defer stream.Close()
	nextSlot(t, stream)
	if got := m.subscriptions()[0].req.FromSlot; got == nil || *got != 100 {
		t.Fatalf("fromSlot %v", got)
	}
}

func TestSubscribeDoesNotMutateTheCallersRequest(t *testing.T) {
	m := &mockGeyser{replay: &pb.SubscribeReplayInfoResponse{FirstAvailable: ptr(500)}}
	url, _ := serveGeyser(t, m)
	req := named("x", ptr(100))
	stream := mockClient(t, url, retryPolicy(20*time.Millisecond, 50*time.Millisecond, 0)).Subscribe(req)
	defer stream.Close()
	nextSlot(t, stream)
	if *req.FromSlot != 100 {
		t.Fatalf("caller's request changed to fromSlot %d", *req.FromSlot)
	}
}

func TestExhaustedRetryPolicyEndsTheStreamAndSaysSo(t *testing.T) {
	url, stop := serveGeyser(t, &mockGeyser{})
	stop()
	stream := mockClient(t, url, retryPolicy(10*time.Millisecond, 10*time.Millisecond, 2)).Subscribe(named("x", nil))
	defer stream.Close()
	events := stream.Events()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if _, err := stream.Next(ctx); err == nil || !strings.Contains(err.Error(), "connection refused") {
		t.Fatalf("expected the connect error with its cause, got %v", err)
	}
	if _, err := stream.Next(ctx); !errors.Is(err, io.EOF) {
		t.Fatalf("expected EOF after the error, got %v", err)
	}
	var retries []bool
	for _, ev := range drain(events) {
		if d, ok := ev.(orbitflare.Disconnected); ok {
			retries = append(retries, d.WillRetry)
		}
	}
	if len(retries) != 2 || !retries[0] || retries[1] {
		t.Fatalf("disconnect retries %v, want [true false]", retries)
	}
	if _, ok := events.Recv(context.Background()); ok {
		t.Fatal("events should end with the stream")
	}
}

func rejected(t *testing.T, code codes.Code, attempts uint32) ([]orbitflare.ConnectionEvent, int, error) {
	t.Helper()
	m := &mockGeyser{reject: code}
	url, _ := serveGeyser(t, m)
	stream := mockClient(t, url, retryPolicy(10*time.Millisecond, 10*time.Millisecond, attempts)).Subscribe(named("x", nil))
	defer stream.Close()
	events := stream.Events()
	_, err := nextUpdate(t, stream)
	if err == nil || errors.Is(err, io.EOF) || errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected the rejection, got %v", err)
	}
	if _, eof := nextUpdate(t, stream); !errors.Is(eof, io.EOF) {
		t.Fatalf("expected EOF after the error, got %v", eof)
	}
	return drain(events), m.connectionCount(), err
}

func TestInvalidRequestsAreNotRetried(t *testing.T) {
	evs, connections, err := rejected(t, codes.InvalidArgument, 0)
	if connections != 1 {
		t.Fatalf("connections %d, want 1", connections)
	}
	var ge *orbitflare.GRPCError
	if !errors.As(err, &ge) || ge.Code != int32(codes.InvalidArgument) {
		t.Fatalf("error %v", err)
	}
	last, ok := evs[len(evs)-1].(orbitflare.Disconnected)
	if !ok || last.WillRetry {
		t.Fatalf("last event %#v", evs[len(evs)-1])
	}
}

func TestRejectionsCountAgainstTheRetryPolicyWhenNoMessageArrived(t *testing.T) {
	evs, connections, _ := rejected(t, codes.Internal, 3)
	if connections != 3 {
		t.Fatalf("connections %d, want 3", connections)
	}
	var attempts []uint32
	for _, ev := range evs {
		if c, ok := ev.(orbitflare.Connecting); ok {
			attempts = append(attempts, c.Attempt)
		}
	}
	if len(attempts) != 3 || attempts[0] != 1 || attempts[1] != 2 || attempts[2] != 3 {
		t.Fatalf("attempts %v", attempts)
	}
}

func TestMessagesResetTheAttemptCounter(t *testing.T) {
	m := &mockGeyser{dropAfter: 2}
	url, _ := serveGeyser(t, m)
	stream := mockClient(t, url, retryPolicy(10*time.Millisecond, 10*time.Millisecond, 2)).Subscribe(named("x", nil))
	defer stream.Close()
	for i := 0; i < 10; i++ {
		if _, err := nextUpdate(t, stream); err != nil {
			t.Fatalf("update %d: %v", i, err)
		}
	}
	if m.connectionCount() < 4 {
		t.Fatalf("connections %d", m.connectionCount())
	}
}

func TestLargeMessagesFitTheDefaultLimit(t *testing.T) {
	m := &mockGeyser{payload: make([]byte, 10<<20)}
	url, _ := serveGeyser(t, m)
	stream := mockClient(t, url, retryPolicy(10*time.Millisecond, 10*time.Millisecond, 1)).Subscribe(named("x", nil))
	defer stream.Close()
	u, err := nextUpdate(t, stream)
	if err != nil {
		t.Fatal(err)
	}
	if n := len(u.GetAccount().GetAccount().GetData()); n != 10<<20 {
		t.Fatalf("payload %d bytes", n)
	}
}

func TestMessagesOverTheLimitAreRejected(t *testing.T) {
	m := &mockGeyser{payload: make([]byte, 10<<20)}
	url, _ := serveGeyser(t, m)
	c, err := geyser.NewBuilder().URL(url).Retry(retryPolicy(10*time.Millisecond, 10*time.Millisecond, 1)).
		MaxDecodingMessageSize(4 << 20).APIKey("").Build()
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	stream := c.Subscribe(named("x", nil))
	defer stream.Close()
	_, err = nextUpdate(t, stream)
	var ge *orbitflare.GRPCError
	if !errors.As(err, &ge) || ge.Code != int32(codes.ResourceExhausted) {
		t.Fatalf("error %v, want RESOURCE_EXHAUSTED", err)
	}
}
