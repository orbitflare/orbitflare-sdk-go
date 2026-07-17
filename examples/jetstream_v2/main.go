package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"time"

	jetstreamv2 "github.com/orbitflare/orbitflare-sdk-go/jetstream/v2"
)

const wsol = "So11111111111111111111111111111111111111112"

func main() {
	client, err := jetstreamv2.NewBuilder().
		URL(envOr("ORBITFLARE_JETSTREAM_URL", "http://ams.jetstream.orbitflare.com")).
		Build()
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if v, err := client.GetVersion(ctx); err != nil {
		fmt.Printf("get_version error: %v\n", err)
	} else {
		fmt.Printf("version=%s\n", v)
	}
	if n, err := client.Ping(ctx, 7); err != nil {
		fmt.Printf("ping error: %v\n", err)
	} else {
		fmt.Printf("ping=%d\n", n)
	}

	filter := jetstreamv2.NewTransactionFilter().
		AccountInclude(wsol).
		IncludeEnrichment(true).
		WithID("wsol")

	stream := client.SubscribeTransactions(filter)
	defer stream.Close()

	for i := 0; i < 20; i++ {
		resp, err := stream.Next(ctx)
		if err != nil {
			log.Fatal(err)
		}
		fmt.Printf("seq=%d\n", resp.GetSequence())
	}
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
