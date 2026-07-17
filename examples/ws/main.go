package main

import (
	"context"
	"fmt"
	"log"
	"os"

	"github.com/orbitflare/orbitflare-sdk-go/ws"
)

func main() {
	ctx := context.Background()

	client, err := ws.NewBuilder().
		URL(envOr("ORBITFLARE_WS_URL", "ws://ams.rpc.orbitflare.com")).
		APIKey(os.Getenv("ORBITFLARE_LICENSE_KEY")).
		Build(ctx)
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	sub, err := client.SlotSubscribe(ctx)
	if err != nil {
		log.Fatal(err)
	}
	defer sub.Unsubscribe()

	for i := 0; i < 10; i++ {
		evt, ok := sub.Next(ctx)
		if !ok {
			break
		}
		fmt.Printf("slot: %s\n", string(evt))
	}
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
