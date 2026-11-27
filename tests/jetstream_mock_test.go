package tests

import (
	"context"
	"errors"
	"io"
	"net"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/orbitflare/orbitflare-sdk-go/geyser"
	"github.com/orbitflare/orbitflare-sdk-go/jetstream"
	jetstreamv2 "github.com/orbitflare/orbitflare-sdk-go/jetstream/v2"
	"github.com/orbitflare/orbitflare-sdk-go/orbitflare"
	pbjs "github.com/orbitflare/orbitflare-sdk-go/proto/jetstream"
	pbv2 "github.com/orbitflare/orbitflare-sdk-go/proto/jetstreamv2"
)

type mockV2 struct {
	pbv2.UnimplementedJetstreamV2Server
	mu          sync.Mutex
	slotReqs    []*pbv2.SubscribeSlotsRequest
	connections int
	reject      codes.Code
}

func (m *mockV2) SubscribeSlots(req *pbv2.SubscribeSlotsRequest, stream grpc.ServerStreamingServer[pbv2.SlotEvent]) error {
	m.mu.Lock()
	m.connections++
	m.slotReqs = append(m.slotReqs, req)
	reject := m.reject
	m.mu.Unlock()
	if reject != codes.OK {
		return status.Error(reject, "mock reject")
	}
	for seq := uint64(1); ; seq++ {
		ev := &pbv2.SlotEvent{Slot: 100 + seq, Sequence: seq, Status: pbv2.SlotStatus_SLOT_STATUS_COMPLETE}
		if req.IncludeBlockFooter && seq%2 == 0 {
			ev.Status = pbv2.SlotStatus_SLOT_STATUS_BLOCK_FOOTER
			ev.BlockFooter = &pbv2.BlockFooter{Slot: ev.Slot}
		}
		if err := stream.Send(ev); err != nil {
			return nil
		}
		select {
		case <-stream.Context().Done():
			return nil
		case <-time.After(10 * time.Millisecond):
		}
	}
}

func (m *mockV2) SubscribeTransactions(stream grpc.BidiStreamingServer[pbv2.SubscribeTransactionsRequest, pbv2.SubscribeTransactionsResponse]) error {
	m.mu.Lock()
	m.connections++
	reject := m.reject
	m.mu.Unlock()
	if _, err := stream.Recv(); err != nil {
		return nil
	}
	if reject != codes.OK {
		return status.Error(reject, "mock reject")
	}
	<-stream.Context().Done()
	return nil
}

type mockV1 struct {
	pbjs.UnimplementedJetstreamServer
	mu          sync.Mutex
	connections int
	reject      codes.Code
}

func (m *mockV1) Subscribe(stream grpc.BidiStreamingServer[pbjs.SubscribeRequest, pbjs.SubscribeUpdate]) error {
	m.mu.Lock()
	m.connections++
	reject := m.reject
	m.mu.Unlock()
	if _, err := stream.Recv(); err != nil {
		return nil
	}
	if reject != codes.OK {
		return status.Error(reject, "mock reject")
	}
	for {
		if err := stream.Send(&pbjs.SubscribeUpdate{}); err != nil {
			return nil
		}
		select {
		case <-stream.Context().Done():
			return nil
		case <-time.After(10 * time.Millisecond):
		}
	}
}

func listen(t *testing.T, register func(*grpc.Server)) string {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	server := grpc.NewServer()
	register(server)
	go server.Serve(lis)
	t.Cleanup(server.Stop)
	return "http://" + lis.Addr().String()
}

func v2Client(t *testing.T, url string) *jetstreamv2.Client {
	t.Helper()
	c, err := jetstreamv2.NewBuilder().URL(url).Retry(retryPolicy(10*time.Millisecond, 10*time.Millisecond, 3)).Build()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(c.Close)
	return c
}

func within(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	t.Cleanup(cancel)
	return ctx
}

func TestV2SlotFootersAreOptIn(t *testing.T) {
	m := &mockV2{}
	url := listen(t, func(s *grpc.Server) { pbv2.RegisterJetstreamV2Server(s, m) })
	c := v2Client(t, url)

	plain := c.SubscribeSlots()
	footers := c.SubscribeSlotsWithBlockFooters()
	defer plain.Close()
	defer footers.Close()

	seen := 0
	for i := 0; i < 6; i++ {
		ev, err := footers.Next(within(t))
		if err != nil {
			t.Fatal(err)
		}
		if ev.Status == pbv2.SlotStatus_SLOT_STATUS_BLOCK_FOOTER {
			seen++
		}
	}
	for i := 0; i < 6; i++ {
		ev, err := plain.Next(within(t))
		if err != nil {
			t.Fatal(err)
		}
		if ev.Status == pbv2.SlotStatus_SLOT_STATUS_BLOCK_FOOTER || ev.BlockFooter != nil {
			t.Fatal("footer without opt-in")
		}
	}
	if seen == 0 {
		t.Fatal("no footers with opt-in")
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	opted := map[bool]int{}
	for _, r := range m.slotReqs {
		opted[r.IncludeBlockFooter]++
	}
	if opted[true] != 1 || opted[false] != 1 {
		t.Fatalf("requests %v", opted)
	}
}

func TestV2StreamsReportEventsAndDoNotRetryRejections(t *testing.T) {
	m := &mockV2{reject: codes.PermissionDenied}
	url := listen(t, func(s *grpc.Server) { pbv2.RegisterJetstreamV2Server(s, m) })
	c := v2Client(t, url)

	slots := c.SubscribeSlots()
	txs := c.SubscribeTransactions(jetstreamv2.NewTransactionFilter().AccountInclude("x").WithID("f"))
	for _, next := range []func(context.Context) error{
		func(ctx context.Context) error { _, err := slots.Next(ctx); return err },
		func(ctx context.Context) error { _, err := txs.Next(ctx); return err },
	} {
		err := next(within(t))
		var ge *orbitflare.GRPCError
		if !errors.As(err, &ge) || ge.Code != int32(codes.PermissionDenied) {
			t.Fatalf("error %v", err)
		}
		if err := next(within(t)); !errors.Is(err, io.EOF) {
			t.Fatalf("expected EOF, got %v", err)
		}
	}
	for _, events := range []*orbitflare.ConnectionEvents{slots.Events(), txs.Events()} {
		got := kinds(drain(events))
		if len(got) != 2 || got[0] != "connecting" || got[1] != "disconnected" {
			t.Fatalf("events %v", got)
		}
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.connections != 2 {
		t.Fatalf("connections %d, want 2", m.connections)
	}
}

func TestV2EventsOnASuccessfulConnect(t *testing.T) {
	m := &mockV2{}
	url := listen(t, func(s *grpc.Server) { pbv2.RegisterJetstreamV2Server(s, m) })
	slots := v2Client(t, url).SubscribeSlots()
	defer slots.Close()
	events := slots.Events()
	if _, err := slots.Next(within(t)); err != nil {
		t.Fatal(err)
	}
	first, _ := events.Recv(within(t))
	second, _ := events.Recv(within(t))
	if c, ok := first.(orbitflare.Connecting); !ok || c.Attempt != 1 || c.Endpoint != url {
		t.Fatalf("first event %#v", first)
	}
	if c, ok := second.(orbitflare.Connected); !ok || c.Endpoint != url {
		t.Fatalf("second event %#v", second)
	}
}

func TestV1StreamsReportEventsAndDoNotRetryRejections(t *testing.T) {
	ok := &mockV1{}
	url := listen(t, func(s *grpc.Server) { pbjs.RegisterJetstreamServer(s, ok) })
	c, err := jetstream.NewBuilder().URL(url).Retry(retryPolicy(10*time.Millisecond, 10*time.Millisecond, 3)).Build()
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	req := jetstream.NewSubscribeRequestBuilder().Transactions("t", jetstream.NewTransactionFilter().AccountInclude("x")).Build()
	stream := c.Subscribe(req)
	events := stream.Events()
	if _, err := stream.Next(within(t)); err != nil {
		t.Fatal(err)
	}
	stream.Close()
	if got := kinds(drain(events)); len(got) < 2 || got[0] != "connecting" || got[1] != "connected" {
		t.Fatalf("events %v", got)
	}

	denied := &mockV1{reject: codes.PermissionDenied}
	url = listen(t, func(s *grpc.Server) { pbjs.RegisterJetstreamServer(s, denied) })
	c2, err := jetstream.NewBuilder().URL(url).Retry(retryPolicy(10*time.Millisecond, 10*time.Millisecond, 3)).Build()
	if err != nil {
		t.Fatal(err)
	}
	defer c2.Close()
	stream = c2.Subscribe(req)
	_, err = stream.Next(within(t))
	var ge *orbitflare.GRPCError
	if !errors.As(err, &ge) || ge.Code != int32(codes.PermissionDenied) {
		t.Fatalf("error %v", err)
	}
	if denied.connections != 1 {
		t.Fatalf("connections %d, want 1", denied.connections)
	}
}

func TestBlockFooterFilterInRequestBuilder(t *testing.T) {
	req := geyser.NewSubscribeRequestBuilder().
		BlockFooter("certs", geyser.NewBlockFooterFilter().IncludeCertificates(true)).
		BlockFooter("plain", geyser.NewBlockFooterFilter()).
		Build()
	if len(req.BlockFooter) != 2 {
		t.Fatalf("block footer filters %d", len(req.BlockFooter))
	}
	if v := req.BlockFooter["certs"].IncludeCertificates; v == nil || !*v {
		t.Fatal("certs filter should include certificates")
	}
	if req.BlockFooter["plain"].IncludeCertificates != nil {
		t.Fatal("plain filter should leave include_certificates unset")
	}
	if len(geyser.NewSubscribeRequestBuilder().Build().BlockFooter) != 0 {
		t.Fatal("no footer filters by default")
	}
}
