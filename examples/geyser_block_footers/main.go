package main

import (
	"context"
	"fmt"
	"log"
	"os"

	"github.com/orbitflare/orbitflare-sdk-go/geyser"
)

func main() {
	client, err := geyser.NewBuilder().
		URL(envOr("ORBITFLARE_GRPC_URL", "http://ny.rpc.orbitflare.com:10000")).
		Build()
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	request := geyser.NewSubscribeRequestBuilder().
		BlockFooter("footers", geyser.NewBlockFooterFilter().IncludeCertificates(true)).
		Commitment(geyser.Processed).
		Build()
	stream := client.Subscribe(request)
	defer stream.Close()

	ctx := context.Background()
	for {
		update, err := stream.Next(ctx)
		if err != nil {
			log.Fatal(err)
		}
		f := update.GetBlockFooter()
		if f == nil {
			continue
		}
		fmt.Printf("slot=%d bank_id=%d bank_hash=%x user_agent=%q final=%v skip=%v notar=%v\n",
			f.Slot, f.BankId, f.BankHash, f.BlockUserAgent,
			f.BlockFinalCert != nil, f.SkipRewardCert != nil, f.NotarRewardCert != nil)
	}
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
