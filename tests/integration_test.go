package tests

import (
	"context"
	"log/slog"
	"os"
	"testing"
	"time"

	pbgeyser "github.com/orbitflare/orbitflare-sdk-go/proto/geyser"

	"github.com/orbitflare/orbitflare-sdk-go/geyser"
	"github.com/orbitflare/orbitflare-sdk-go/jetstream"
	jetstreamv2 "github.com/orbitflare/orbitflare-sdk-go/jetstream/v2"
	"github.com/orbitflare/orbitflare-sdk-go/rpc"
	"github.com/orbitflare/orbitflare-sdk-go/ws"
)

const (
	rpcAMS       = "http://ams.rpc.orbitflare.com"
	rpcSLC       = "http://slc.rpc.orbitflare.com"
	wsAMS        = "ws://ams.rpc.orbitflare.com"
	wsSLC        = "ws://slc.rpc.orbitflare.com"
	grpcAMS      = "http://ams.rpc.orbitflare.com:10000"
	grpcSLC      = "http://slc.rpc.orbitflare.com:10000"
	grpcJP       = "http://jp.rpc.orbitflare.com:10000"
	jetstreamAMS = "http://ams.jetstream.orbitflare.com"
	jetstreamSLC = "http://slc.jetstream.orbitflare.com"

	pumpfun       = "6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P"
	tokenProgram  = "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA"
	wsol          = "So11111111111111111111111111111111111111112"
	systemProgram = "11111111111111111111111111111111"
)

func liveKey(t *testing.T) {
	t.Helper()
	if os.Getenv("ORBITFLARE_LICENSE_KEY") == "" {
		t.Skip("set ORBITFLARE_LICENSE_KEY to run live tests")
	}
}

func TestGeyserRequestBuilder(t *testing.T) {
	req := geyser.NewSubscribeRequestBuilder().
		Transactions("tx", geyser.NewTransactionFilter().
			Vote(false).
			Failed(false).
			Signature("sig").
			AccountInclude("a", "b").
			AccountExclude("c")).
		Accounts("acc", geyser.NewAccountFilter().
			Owner(tokenProgram).
			Memcmp(geyser.MemcmpBase58(0, "data")).
			Datasize(165).
			Lamports(geyser.LamportsGt(100)).
			TokenAccountState(true).
			NonemptyTxnSignature(true)).
		Slots("sl", geyser.NewSlotFilter().FilterByCommitment(true)).
		Blocks("bl", geyser.NewBlockFilter().IncludeTransactions(true)).
		BlocksMeta("bm").
		Entry("en").
		AccountsDataSlice(8, 32).
		Commitment(geyser.Confirmed).
		FromSlot(42).
		Build()

	if req.GetCommitment() != pbgeyser.CommitmentLevel_CONFIRMED {
		t.Fatalf("commitment = %v", req.GetCommitment())
	}
	if req.GetFromSlot() != 42 {
		t.Fatalf("from_slot = %d", req.GetFromSlot())
	}
	if req.GetPing() != nil {
		t.Fatal("initial request carries a ping; vanilla Yellowstone would ignore its filters")
	}
	tx := req.GetTransactions()["tx"]
	if tx.GetVote() || tx.GetSignature() != "sig" || len(tx.GetAccountInclude()) != 2 || len(tx.GetAccountExclude()) != 1 {
		t.Fatal("transaction filter mismatch")
	}
	acc := req.GetAccounts()["acc"]
	if len(acc.GetFilters()) != 4 || !acc.GetNonemptyTxnSignature() {
		t.Fatalf("account filter mismatch: %d conditions", len(acc.GetFilters()))
	}
	if !req.GetSlots()["sl"].GetFilterByCommitment() {
		t.Fatal("slot filter mismatch")
	}
	if !req.GetBlocks()["bl"].GetIncludeTransactions() {
		t.Fatal("block filter mismatch")
	}
	if _, ok := req.GetBlocksMeta()["bm"]; !ok {
		t.Fatal("missing blocks_meta")
	}
	if _, ok := req.GetEntry()["en"]; !ok {
		t.Fatal("missing entry")
	}
	if len(req.GetAccountsDataSlice()) != 1 || req.GetAccountsDataSlice()[0].GetOffset() != 8 || req.GetAccountsDataSlice()[0].GetLength() != 32 {
		t.Fatal("accounts_data_slice mismatch")
	}
}

func TestGeyserCommitmentOmittedByDefault(t *testing.T) {
	if geyser.NewSubscribeRequestBuilder().Build().Commitment != nil {
		t.Fatal("commitment should be nil when unset")
	}
}

func TestJetstreamV1RequestBuilder(t *testing.T) {
	req := jetstream.NewSubscribeRequestBuilder().
		Transactions("tx", jetstream.NewTransactionFilter().
			AccountInclude("a").
			AccountRequired("r")).
		Build()

	tx := req.GetTransactions()["tx"]
	if len(tx.GetAccountInclude()) != 1 || len(tx.GetAccountRequired()) != 1 {
		t.Fatal("v1 filter mismatch")
	}
	if req.GetPing().GetId() != 1 {
		t.Fatal("missing initial ping")
	}
}

func TestJetstreamV2FilterWithID(t *testing.T) {
	f := jetstreamv2.NewTransactionFilter().
		AccountInclude("a", "b").
		AccountExclude("c").
		IncludeEnrichment(true).
		WithID("f1")

	if f.GetFilterId() != "f1" {
		t.Fatalf("filter id = %q", f.GetFilterId())
	}
	if !f.GetFilter().GetIncludeEnrichment() || len(f.GetFilter().GetAccountInclude()) != 2 || len(f.GetFilter().GetAccountExclude()) != 1 {
		t.Fatal("v2 filter mismatch")
	}
}

func TestBuildersRequireURL(t *testing.T) {
	t.Setenv("ORBITFLARE_GRPC_URL", "")
	t.Setenv("ORBITFLARE_JETSTREAM_URL", "")
	if _, err := geyser.NewBuilder().Build(); err == nil {
		t.Fatal("geyser: expected error without URL")
	}
	if _, err := jetstream.NewBuilder().Build(); err == nil {
		t.Fatal("jetstream v1: expected error without URL")
	}
	if _, err := jetstreamv2.NewBuilder().Build(); err == nil {
		t.Fatal("jetstream v2: expected error without URL")
	}
}

func TestJetstreamV2HandleAfterCloseErrors(t *testing.T) {
	client, err := jetstreamv2.NewBuilder().URL("http://localhost:1").Timeout(time.Second).Build()
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	defer client.Close()

	stream := client.SubscribeTransactions(jetstreamv2.NewTransactionFilter().AccountInclude("abc").WithID("f1"))
	handle := stream.Handle()
	stream.Close()

	if err := handle.AddFilters([]*jetstreamv2.TxFilter{jetstreamv2.NewTransactionFilter().AccountInclude("xyz").WithID("f2")}); err == nil {
		t.Fatal("expected error adding filters after close")
	}
}

func TestSubscribeAgainstDeadEndpointDoesNotBlockForever(t *testing.T) {
	client, err := geyser.NewBuilder().URL("http://localhost:1").Timeout(time.Second).Build()
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	defer client.Close()

	stream := client.Subscribe(geyser.NewSubscribeRequestBuilder().Slots("s", geyser.NewSlotFilter()).Build())
	defer stream.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if _, err := stream.Next(ctx); err == nil {
		t.Fatal("expected a deadline error from a dead endpoint")
	}
}

func TestLiveRPC(t *testing.T) {
	liveKey(t)
	client, err := rpc.NewBuilder().URLs(rpcAMS, rpcSLC).APIKey(os.Getenv("ORBITFLARE_LICENSE_KEY")).Build()
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	slot, err := client.GetSlot(ctx)
	if err != nil || slot == 0 {
		t.Fatalf("GetSlot: slot=%d err=%v", slot, err)
	}
	t.Logf("slot %d", slot)

	hash, lastValid, err := client.GetLatestBlockhash(ctx)
	if err != nil || hash == "" || lastValid == 0 {
		t.Fatalf("GetLatestBlockhash: %q %d err=%v", hash, lastValid, err)
	}
	if _, err := client.GetBalance(ctx, systemProgram); err != nil {
		t.Fatalf("GetBalance: %v", err)
	}
	if _, err := client.Request(ctx, "getHealth", []any{}); err != nil {
		t.Fatalf("getHealth: %v", err)
	}
}

func TestLiveWebSocket(t *testing.T) {
	liveKey(t)
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Second)
	defer cancel()

	client, err := ws.NewBuilder().URLs(wsAMS, wsSLC).APIKey(os.Getenv("ORBITFLARE_LICENSE_KEY")).Build(ctx)
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	defer client.Close()

	sub, err := client.SlotSubscribe(ctx)
	if err != nil {
		t.Fatalf("SlotSubscribe: %v", err)
	}
	defer sub.Unsubscribe()

	for i := 0; i < 2; i++ {
		evt, ok := sub.Next(ctx)
		if !ok {
			t.Fatalf("slot stream ended after %d events", i)
		}
		t.Logf("slot event %s", string(evt))
	}
}

func TestLiveGeyser(t *testing.T) {
	liveKey(t)
	client, err := geyser.NewBuilder().URLs(grpcAMS, grpcSLC).Build()
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	defer client.Close()

	req := geyser.NewSubscribeRequestBuilder().
		Slots("slots", geyser.NewSlotFilter()).
		Commitment(geyser.Confirmed).
		Build()

	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Second)
	defer cancel()

	stream := client.Subscribe(req)
	defer stream.Close()

	for i := 0; i < 2; i++ {
		update, err := stream.Next(ctx)
		if err != nil {
			t.Fatalf("Next: %v", err)
		}
		if s := update.GetSlot(); s != nil {
			t.Logf("slot %d (%s)", s.GetSlot(), s.GetStatus())
		}
	}
}

func TestLiveJetstreamV1(t *testing.T) {
	liveKey(t)
	client, err := jetstream.NewBuilder().URLs(jetstreamAMS, jetstreamSLC).Build()
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	defer client.Close()

	req := jetstream.NewSubscribeRequestBuilder().
		Transactions("pumpfun", jetstream.NewTransactionFilter().AccountInclude(pumpfun)).
		Build()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	stream := client.Subscribe(req)
	defer stream.Close()

	for i := 0; i < 2; i++ {
		update, err := stream.Next(ctx)
		if err != nil {
			t.Fatalf("Next: %v", err)
		}
		if tx := update.GetTransaction(); tx != nil {
			t.Logf("tx slot %d", tx.GetSlot())
		}
	}
}

func TestLiveGeyserFailover(t *testing.T) {
	liveKey(t)
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelDebug}))
	client, err := geyser.NewBuilder().
		URLs(grpcJP, grpcAMS).
		Timeout(4 * time.Second).
		Logger(logger).
		Build()
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	defer client.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	stream := client.Subscribe(geyser.NewSubscribeRequestBuilder().
		Slots("slots", geyser.NewSlotFilter()).
		Commitment(geyser.Confirmed).
		Build())
	defer stream.Close()

	update, err := stream.Next(ctx)
	if err != nil {
		t.Fatalf("failover to ams should have produced data, got: %v", err)
	}
	if s := update.GetSlot(); s != nil {
		t.Logf("streaming, slot %d (%s)", s.GetSlot(), s.GetStatus())
	} else {
		t.Logf("streaming, update %T", update.GetUpdateOneof())
	}
}

func TestLiveJetstreamV2(t *testing.T) {
	liveKey(t)
	client, err := jetstreamv2.NewBuilder().URLs(jetstreamAMS, jetstreamSLC).Build()
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	defer client.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 35*time.Second)
	defer cancel()

	version, err := client.GetVersion(ctx)
	if err != nil {
		t.Fatalf("GetVersion: %v", err)
	}
	t.Logf("version %s", version)

	count, err := client.Ping(ctx, 7)
	if err != nil || count != 7 {
		t.Fatalf("Ping: count=%d err=%v", count, err)
	}

	slots := client.SubscribeSlots()
	defer slots.Close()
	for i := 0; i < 2; i++ {
		event, err := slots.Next(ctx)
		if err != nil {
			t.Fatalf("slots Next: %v", err)
		}
		t.Logf("slot %d (%s)", event.GetSlot(), event.GetStatus())
	}

	txs := client.SubscribeTransactions(jetstreamv2.NewTransactionFilter().AccountInclude(pumpfun).WithID("pumpfun"))
	defer txs.Close()
	for i := 0; i < 2; i++ {
		resp, err := txs.Next(ctx)
		if err != nil {
			t.Fatalf("tx Next: %v", err)
		}
		t.Logf("tx seq %d", resp.GetSequence())
	}
	if err := txs.RemoveFilters([]string{"pumpfun"}); err != nil {
		t.Fatalf("RemoveFilters: %v", err)
	}
}
