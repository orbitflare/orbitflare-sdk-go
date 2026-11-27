package tests

import (
	"context"
	"errors"
	"math/big"
	"os"
	"slices"
	"sync/atomic"
	"testing"
	"time"

	"github.com/orbitflare/orbitflare-sdk-go/geyser"
	jetstreamv2 "github.com/orbitflare/orbitflare-sdk-go/jetstream/v2"
	"github.com/orbitflare/orbitflare-sdk-go/orbitflare"
	pb "github.com/orbitflare/orbitflare-sdk-go/proto/geyser"
	pbv2 "github.com/orbitflare/orbitflare-sdk-go/proto/jetstreamv2"
)

const (
	jupiter       = "JUP6LkbZbjS1jKKwapdHNy74zcZ3tLUZoi5QNyVTaV4"
	computeBudget = "ComputeBudget111111111111111111111111111111"
)

func liveURL(t *testing.T, env string) {
	t.Helper()
	if os.Getenv(env) == "" {
		t.Skip("set " + env + " to run live stream tests")
	}
}

func liveGeyser(t *testing.T) *geyser.Client {
	t.Helper()
	c, err := geyser.NewBuilder().Build()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(c.Close)
	return c
}

func liveV2(t *testing.T) *jetstreamv2.Client {
	t.Helper()
	c, err := jetstreamv2.NewBuilder().Build()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(c.Close)
	return c
}

func base58(b []byte) string {
	const alphabet = "123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz"
	n := new(big.Int).SetBytes(b)
	mod, radix := new(big.Int), big.NewInt(58)
	var out []byte
	for n.Sign() > 0 {
		n.DivMod(n, radix, mod)
		out = append(out, alphabet[mod.Int64()])
	}
	for _, c := range b {
		if c != 0 {
			break
		}
		out = append(out, '1')
	}
	slices.Reverse(out)
	return string(out)
}

func collect(t *testing.T, stream *geyser.Stream, d time.Duration) []*pb.SubscribeUpdate {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), d)
	defer cancel()
	var out []*pb.SubscribeUpdate
	for {
		u, err := stream.Next(ctx)
		if err != nil {
			if errors.Is(err, context.DeadlineExceeded) {
				stream.Close()
				return out
			}
			t.Fatalf("stream failed: %v", err)
		}
		out = append(out, u)
	}
}

func watching(account string) *pb.SubscribeRequest {
	return geyser.NewSubscribeRequestBuilder().
		Transactions("t", geyser.NewTransactionFilter().Vote(false).AccountInclude(account)).
		Slots("s", geyser.NewSlotFilter()).
		Commitment(geyser.Processed).
		Build()
}

func accountsOf(u *pb.SubscribeUpdate) []string {
	info := u.GetTransaction().GetTransaction()
	msg := info.GetTransaction().GetMessage()
	if info == nil || msg == nil {
		return nil
	}
	var keys []string
	for _, group := range [][][]byte{msg.AccountKeys, info.GetMeta().GetLoadedWritableAddresses(), info.GetMeta().GetLoadedReadonlyAddresses()} {
		for _, k := range group {
			keys = append(keys, base58(k))
		}
	}
	return keys
}

func nextTx(ctx context.Context, t *testing.T, stream *geyser.Stream) []string {
	t.Helper()
	for {
		u, err := stream.Next(ctx)
		if err != nil {
			t.Fatalf("waiting for a transaction: %v", err)
		}
		if keys := accountsOf(u); keys != nil {
			return keys
		}
	}
}

func TestLiveBlockFootersWithCertificates(t *testing.T) {
	liveURL(t, "ORBITFLARE_GRPC_URL")
	req := geyser.NewSubscribeRequestBuilder().
		BlockFooter("f", geyser.NewBlockFooterFilter().IncludeCertificates(true)).
		Commitment(geyser.Processed).
		Build()
	updates := collect(t, liveGeyser(t).Subscribe(req), 20*time.Second)

	seen := map[[2]uint64]bool{}
	withFinal := 0
	for _, u := range updates {
		f := u.GetBlockFooter()
		if f == nil {
			continue
		}
		if !slices.Equal(u.Filters, []string{"f"}) {
			t.Fatalf("filters %v", u.Filters)
		}
		key := [2]uint64{f.Slot, f.BankId}
		if seen[key] {
			t.Fatalf("duplicate footer %d", f.Slot)
		}
		seen[key] = true
		if f.BankId == 0 || len(f.BankHash) != 32 || len(f.BlockUserAgent) > 255 || f.BlockProducerTimeNanos == 0 {
			t.Fatalf("bad footer %v", f)
		}
		if cert := f.BlockFinalCert; cert != nil {
			withFinal++
			if cert.Slot > f.Slot {
				t.Fatalf("cert slot %d after footer slot %d", cert.Slot, f.Slot)
			}
			for _, agg := range []*pb.BlockFooterVotesAggregate{cert.FinalAggregate, cert.NotarAggregate} {
				if agg == nil {
					continue
				}
				if len(agg.Signature) != 96 || (len(agg.BlockId) != 0 && len(agg.BlockId) != 32) || len(agg.SignerBitmap) == 0 {
					t.Fatalf("bad aggregate %v", agg)
				}
			}
		}
	}
	if len(seen) == 0 {
		t.Fatalf("no footers in %d updates", len(updates))
	}
	if withFinal == 0 {
		t.Fatal("no footer carried a final cert")
	}
	t.Logf("%d footers, %d with final cert", len(seen), withFinal)
}

func TestLiveBlockFootersWithoutCertificates(t *testing.T) {
	liveURL(t, "ORBITFLARE_GRPC_URL")
	req := geyser.NewSubscribeRequestBuilder().
		BlockFooter("f", geyser.NewBlockFooterFilter()).
		Commitment(geyser.Processed).
		Build()
	footers := 0
	for _, u := range collect(t, liveGeyser(t).Subscribe(req), 10*time.Second) {
		f := u.GetBlockFooter()
		if f == nil {
			continue
		}
		footers++
		if len(f.BankHash) != 32 || f.BlockFinalCert != nil || f.SkipRewardCert != nil || f.NotarRewardCert != nil {
			t.Fatalf("bad footer %v", f)
		}
	}
	if footers == 0 {
		t.Fatal("no footers")
	}
}

func TestLiveBankIDsAndV1Transactions(t *testing.T) {
	liveURL(t, "ORBITFLARE_GRPC_URL")
	req := geyser.NewSubscribeRequestBuilder().
		Slots("s", geyser.NewSlotFilter().FilterByCommitment(false)).
		Transactions("t", geyser.NewTransactionFilter().Vote(false).AccountInclude(computeBudget)).
		Commitment(geyser.Processed).
		Build()
	txs, v1, slots := 0, 0, 0
	for _, u := range collect(t, liveGeyser(t).Subscribe(req), 20*time.Second) {
		if tx := u.GetTransaction(); tx != nil {
			txs++
			if tx.BankId == 0 {
				t.Fatal("transaction without bank_id")
			}
			msg := tx.GetTransaction().GetTransaction().GetMessage()
			if msg == nil {
				t.Fatal("transaction without message")
			}
			if msg.Config != nil {
				v1++
				if !msg.Versioned {
					t.Fatal("v1 tx with versioned=false")
				}
			}
		} else if s := u.GetSlot(); s != nil {
			slots++
		}
	}
	if txs == 0 || slots == 0 {
		t.Fatalf("%d txs, %d slots", txs, slots)
	}
	t.Logf("%d txs (%d v1), %d slot updates", txs, v1, slots)
}

func TestLiveUpdateReplacesFiltersWithoutReconnecting(t *testing.T) {
	liveURL(t, "ORBITFLARE_GRPC_URL")
	stream := liveGeyser(t).Subscribe(watching(jupiter))
	defer stream.Close()
	events := stream.Events()
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	if keys := nextTx(ctx, t, stream); !slices.Contains(keys, jupiter) {
		t.Fatal("first transaction does not touch Jupiter")
	}
	if err := stream.Update(watching(pumpfun)); err != nil {
		t.Fatal(err)
	}
	settle, cancelSettle := context.WithTimeout(ctx, 3*time.Second)
	for {
		if _, err := stream.Next(settle); err != nil {
			break
		}
	}
	cancelSettle()
	for i := 0; i < 20; i++ {
		if keys := nextTx(ctx, t, stream); !slices.Contains(keys, pumpfun) {
			t.Fatalf("transaction %d: old filter still applied", i)
		}
	}
	connects := 0
	for ev, ok := events.TryRecv(); ok; ev, ok = events.TryRecv() {
		switch e := ev.(type) {
		case orbitflare.Connected:
			connects++
		case orbitflare.Disconnected:
			t.Fatalf("disconnected: %s", e.Error)
		}
	}
	if connects != 1 {
		t.Fatalf("the update must not reconnect, saw %d connects", connects)
	}
}

func TestLiveReplayCatchesUpWithTheLiveStream(t *testing.T) {
	liveURL(t, "ORBITFLARE_GRPC_URL")
	client := liveGeyser(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var tip, max atomic.Uint64
	track := func(stream *geyser.Stream, into *atomic.Uint64) {
		for {
			u, err := stream.Next(ctx)
			if err != nil {
				return
			}
			if tx := u.GetTransaction(); tx != nil && tx.Slot > into.Load() {
				into.Store(tx.Slot)
			}
		}
	}
	live := client.Subscribe(watching(jupiter))
	defer live.Close()
	go track(live, &tip)
	waitFor(t, 30*time.Second, "a live transaction", func() bool { return tip.Load() > 0 })

	from := tip.Load() - 32
	replay := client.SubscribeWith(func(geyser.ConnectInfo) *pb.SubscribeRequest {
		req := watching(jupiter)
		req.FromSlot = &from
		return req
	})
	defer replay.Close()
	go track(replay, &max)
	start := time.Now()
	waitFor(t, 20*time.Second, "the replay to reach the live tip", func() bool {
		return max.Load() > 0 && max.Load()+2 >= tip.Load()
	})
	t.Logf("replay from %d caught up with the tip in %v", from, time.Since(start).Round(100*time.Millisecond))
}

func TestLiveSubscribeWithReplaysFromTheRequestedSlot(t *testing.T) {
	liveURL(t, "ORBITFLARE_GRPC_URL")
	client := liveGeyser(t)
	ctx, cancel := context.WithTimeout(context.Background(), 110*time.Second)
	defer cancel()

	probe := client.Subscribe(watching(jupiter))
	var now uint64
	for now == 0 {
		u, err := probe.Next(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if s := u.GetSlot(); s != nil {
			now = s.Slot
		}
	}
	probe.Close()

	from := now - 150
	stream := client.SubscribeWith(func(info geyser.ConnectInfo) *pb.SubscribeRequest {
		req := watching(jupiter)
		req.FromSlot = &from
		return req
	})
	var first uint64
	for first == 0 {
		u, err := stream.Next(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if tx := u.GetTransaction(); tx != nil {
			first = tx.Slot
		}
	}
	stream.Close()
	if first < from || first >= now {
		t.Fatalf("first tx at %d, from %d, now %d", first, from, now)
	}

	one := uint64(1)
	old := client.SubscribeWith(func(geyser.ConnectInfo) *pb.SubscribeRequest {
		req := watching(jupiter)
		req.FromSlot = &one
		return req
	})
	defer old.Close()
	events := old.Events()
	if _, err := old.Next(ctx); err != nil {
		t.Fatal(err)
	}
	var clamped *orbitflare.ReplayClamped
	for ev, ok := events.TryRecv(); ok; ev, ok = events.TryRecv() {
		if c, ok := ev.(orbitflare.ReplayClamped); ok {
			clamped = &c
		}
	}
	if clamped == nil {
		t.Fatal("expected the slot to be clamped")
	}
	if clamped.Requested != 1 || clamped.Available <= 1 || clamped.Available > now {
		t.Fatalf("clamped %+v, now %d", clamped, now)
	}
}

func TestLiveInvalidFilterIsNotRetried(t *testing.T) {
	liveURL(t, "ORBITFLARE_GRPC_URL")
	stream := liveGeyser(t).Subscribe(geyser.NewSubscribeRequestBuilder().
		Transactions("t", geyser.NewTransactionFilter().AccountInclude("not-a-pubkey")).
		Build())
	defer stream.Close()
	events := stream.Events()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	_, err := stream.Next(ctx)
	var ge *orbitflare.GRPCError
	if !errors.As(err, &ge) || ge.Code != 3 {
		t.Fatalf("error %v, want INVALID_ARGUMENT", err)
	}
	connecting := 0
	for _, ev := range drain(events) {
		switch e := ev.(type) {
		case orbitflare.Connecting:
			connecting++
		case orbitflare.Disconnected:
			if e.WillRetry {
				t.Fatal("a rejected request must not be retried")
			}
		}
	}
	if connecting != 1 {
		t.Fatalf("%d connects, want 1", connecting)
	}
	t.Logf("rejected: %v", err)
}

func waitFor(t *testing.T, d time.Duration, what string, check func() bool) {
	t.Helper()
	deadline := time.Now().Add(d)
	for !check() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func collectSlots(t *testing.T, stream *jetstreamv2.SlotStream, d time.Duration) []*pbv2.SlotEvent {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), d)
	defer cancel()
	var out []*pbv2.SlotEvent
	for {
		ev, err := stream.Next(ctx)
		if err != nil {
			if errors.Is(err, context.DeadlineExceeded) {
				stream.Close()
				return out
			}
			t.Fatalf("slots failed: %v", err)
		}
		out = append(out, ev)
	}
}

func increasing(events []*pbv2.SlotEvent) bool {
	for i := 1; i < len(events); i++ {
		if events[i].Sequence <= events[i-1].Sequence {
			return false
		}
	}
	return true
}

func TestLiveV2SlotsWithoutFooters(t *testing.T) {
	liveURL(t, "ORBITFLARE_JETSTREAM_URL")
	events := collectSlots(t, liveV2(t).SubscribeSlots(), 15*time.Second)
	if len(events) <= 10 || !increasing(events) {
		t.Fatalf("%d events, increasing=%v", len(events), increasing(events))
	}
	for _, ev := range events {
		if ev.Status == pbv2.SlotStatus_SLOT_STATUS_BLOCK_FOOTER || ev.BlockFooter != nil {
			t.Fatal("footer without opt-in")
		}
	}
}

func TestLiveV2SlotsWithFooters(t *testing.T) {
	liveURL(t, "ORBITFLARE_JETSTREAM_URL")
	events := collectSlots(t, liveV2(t).SubscribeSlotsWithBlockFooters(), 30*time.Second)
	if !increasing(events) {
		t.Fatal("sequence not increasing")
	}
	seen := map[uint64]bool{}
	for _, ev := range events {
		if ev.Status != pbv2.SlotStatus_SLOT_STATUS_BLOCK_FOOTER {
			if ev.BlockFooter != nil {
				t.Fatalf("footer on %s", ev.Status)
			}
			continue
		}
		if seen[ev.Slot] {
			t.Fatalf("two footers for slot %d", ev.Slot)
		}
		seen[ev.Slot] = true
		f := ev.BlockFooter
		if f == nil || f.Slot != ev.Slot || len(f.BankHash) != 32 || (len(f.BlockId) != 0 && len(f.BlockId) != 32) ||
			len(f.BlockUserAgent) > 255 || f.BlockProducerTimeNanos == 0 {
			t.Fatalf("bad footer %v", f)
		}
		if cert := f.BlockFinalCert; cert != nil {
			for _, agg := range []*pbv2.BlockFooterVotesAggregate{cert.FinalAggregate, cert.NotarAggregate} {
				if agg != nil && (len(agg.Signature) != 96 || (len(agg.BlockId) != 0 && len(agg.BlockId) != 32)) {
					t.Fatalf("bad aggregate %v", agg)
				}
			}
		}
	}
	if len(seen) == 0 {
		t.Fatalf("no footers in %d events", len(events))
	}
	t.Logf("%d footers in %d events", len(seen), len(events))
}

func TestLiveV2EnrichedTransactions(t *testing.T) {
	liveURL(t, "ORBITFLARE_JETSTREAM_URL")
	stream := liveV2(t).SubscribeTransactions(
		jetstreamv2.NewTransactionFilter().AccountInclude(computeBudget).IncludeEnrichment(true).WithID("cb"))
	defer stream.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	accepted := false
	var txs []*pbv2.TransactionMessage
	for len(txs) < 5000 {
		resp, err := stream.Next(ctx)
		if err != nil {
			if errors.Is(err, context.DeadlineExceeded) {
				break
			}
			t.Fatal(err)
		}
		if v := resp.GetFilterValidation(); v != nil {
			if !v.Accepted {
				t.Fatalf("filter rejected: %s", v.RejectionReason)
			}
			accepted = true
		} else if tx := resp.GetTransaction(); tx != nil {
			if !slices.Equal(tx.FilterIds, []string{"cb"}) || tx.Transaction == nil {
				t.Fatalf("bad transaction %v", tx)
			}
			txs = append(txs, tx.Transaction)
		}
	}
	if !accepted || len(txs) == 0 {
		t.Fatalf("accepted=%v txs=%d", accepted, len(txs))
	}
	fee, v1 := false, 0
	for _, tx := range txs {
		if len(tx.FeePayer) != 32 {
			t.Fatal("enrichment missing")
		}
		fee = fee || tx.PriorityFeeLamports > 0
		if cfg := tx.Config; cfg != nil {
			v1++
			if !tx.Versioned || tx.PriorityFeeLamports != cfg.PriorityFee || tx.ComputeUnitPrice != 0 {
				t.Fatalf("bad v1 tx %v", tx)
			}
		}
	}
	if !fee {
		t.Fatal("no priority fees")
	}
	t.Logf("%d txs, %d v1", len(txs), v1)
}

func TestLiveV2StreamsReportConnectionEvents(t *testing.T) {
	liveURL(t, "ORBITFLARE_JETSTREAM_URL")
	c := liveV2(t)
	txs := c.SubscribeTransactions(jetstreamv2.NewTransactionFilter().AccountInclude(computeBudget).WithID("events"))
	slots := c.SubscribeSlots()
	defer txs.Close()
	defer slots.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	if _, err := txs.Next(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := slots.Next(ctx); err != nil {
		t.Fatal(err)
	}
	for _, events := range []*orbitflare.ConnectionEvents{txs.Events(), slots.Events()} {
		first, _ := events.Recv(ctx)
		second, _ := events.Recv(ctx)
		if c, ok := first.(orbitflare.Connecting); !ok || c.Attempt != 1 {
			t.Fatalf("first event %#v", first)
		}
		if _, ok := second.(orbitflare.Connected); !ok {
			t.Fatalf("second event %#v", second)
		}
	}
}

func TestLiveV2UnaryCalls(t *testing.T) {
	liveURL(t, "ORBITFLARE_JETSTREAM_URL")
	c := liveV2(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	if v, err := c.GetVersion(ctx); err != nil || v == "" {
		t.Fatalf("version %q, %v", v, err)
	}
	if n, err := c.Ping(ctx, 7); err != nil || n != 7 {
		t.Fatalf("ping %d, %v", n, err)
	}
}
