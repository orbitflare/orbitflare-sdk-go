package main

import (
	"context"
	"fmt"
	"log"
	"os"

	jetstreamv2 "github.com/orbitflare/orbitflare-sdk-go/jetstream/v2"
	pb "github.com/orbitflare/orbitflare-sdk-go/proto/jetstreamv2"
)

func main() {
	client, err := jetstreamv2.NewBuilder().
		URL(envOr("ORBITFLARE_JETSTREAM_URL", "http://fra.jetstream.orbitflare.com")).
		Build()
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	slots := client.SubscribeSlotsWithBlockFooters()
	defer slots.Close()

	ctx := context.Background()
	for {
		event, err := slots.Next(ctx)
		if err != nil {
			log.Fatal(err)
		}
		if event.GetStatus() != pb.SlotStatus_SLOT_STATUS_BLOCK_FOOTER {
			fmt.Printf("seq=%d slot=%d parent=%d %s\n", event.Sequence, event.Slot, event.ParentSlot, event.Status)
			continue
		}
		footer := event.GetBlockFooter()
		if footer == nil {
			continue
		}
		fmt.Printf("seq=%d slot=%d FOOTER bank_hash=%x block_id=%x user_agent=%q final_cert=%v\n",
			event.Sequence, footer.Slot, footer.BankHash, footer.BlockId, footer.BlockUserAgent, footer.BlockFinalCert != nil)
	}
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
