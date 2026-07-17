package main

import (
	"context"
	"fmt"
	"log"
	"os"

	"github.com/orbitflare/orbitflare-sdk-go/geyser"
)

const (
	pumpFun      = "6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P"
	tokenProgram = "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA"
	wsol         = "So11111111111111111111111111111111111111112"
)

func main() {
	client, err := geyser.NewBuilder().
		URL(envOr("ORBITFLARE_GRPC_URL", "http://ams.rpc.orbitflare.com:10000")).
		Build()
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	request := geyser.NewSubscribeRequestBuilder().
		Transactions("pumpfun", geyser.NewTransactionFilter().
			Vote(false).
			Failed(false).
			AccountInclude(pumpFun).
			AccountRequired(wsol)).
		Accounts("wsol-token-accounts", geyser.NewAccountFilter().
			Owner(tokenProgram).
			Datasize(165).
			Memcmp(geyser.MemcmpBase58(0, wsol)).
			Lamports(geyser.LamportsGt(0))).
		Slots("slots", geyser.NewSlotFilter().FilterByCommitment(true)).
		Commitment(geyser.Confirmed).
		Build()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	stream := client.Subscribe(request)
	defer stream.Close()

	for i := 0; i < 20; i++ {
		update, err := stream.Next(ctx)
		if err != nil {
			log.Fatal(err)
		}
		switch {
		case update.GetTransaction() != nil:
			fmt.Printf("tx slot=%d filters=%v\n", update.GetTransaction().GetSlot(), update.GetFilters())
		case update.GetAccount() != nil:
			fmt.Printf("account slot=%d filters=%v\n", update.GetAccount().GetSlot(), update.GetFilters())
		case update.GetSlot() != nil:
			fmt.Printf("slot %d\n", update.GetSlot().GetSlot())
		}
	}
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
