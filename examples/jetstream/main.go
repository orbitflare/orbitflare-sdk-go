package main

import (
	"context"
	"fmt"
	"log"
	"os"

	"github.com/orbitflare/orbitflare-sdk-go/jetstream"
)

const pumpFun = "6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P"

func main() {
	client, err := jetstream.NewBuilder().
		URL(envOr("ORBITFLARE_JETSTREAM_URL", "http://ams.jetstream.orbitflare.com")).
		Build()
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	request := jetstream.NewSubscribeRequestBuilder().
		Transactions("pumpfun", jetstream.NewTransactionFilter().AccountInclude(pumpFun)).
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
		if tx := update.GetTransaction(); tx != nil {
			fmt.Printf("jet tx slot=%d\n", tx.GetSlot())
		}
	}
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
