package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/orbitflare/orbitflare-sdk-go/rpc"
)

func main() {
	client, err := rpc.NewBuilder().
		URL(envOr("ORBITFLARE_RPC_URL", "http://ams.rpc.orbitflare.com")).
		APIKey(os.Getenv("ORBITFLARE_LICENSE_KEY")).
		Commitment("confirmed").
		Build()
	if err != nil {
		log.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	slot, err := client.GetSlot(ctx)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("slot: %d\n", slot)

	hash, lastValid, err := client.GetLatestBlockhash(ctx)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("blockhash: %s (valid until block %d)\n", hash, lastValid)

	bal, err := client.GetBalance(ctx, "11111111111111111111111111111111")
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("system program balance: %d lamports\n", bal)

	health, err := client.Request(ctx, "getHealth", []any{})
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("health: %s\n", string(health))
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
