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
		URL(envOr("ORBITFLARE_RPC_URL", "https://fra.rpc.orbitflare.com")).
		APIKey(os.Getenv("ORBITFLARE_LICENSE_KEY")).
		Build()
	if err != nil {
		log.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	asset, err := client.GetAsset(ctx, "GJfYbfW7gufzFYfm8gFatrUE71wYsjay6FwzDkTa3vaD", nil)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("%s (%s), owner %s\n", asset.Name(), asset.Interface, asset.Owner())

	portfolio, err := client.GetAssetsByOwner(ctx, &rpc.GetAssetsByOwnerRequest{
		OwnerAddress: "86xCnPeV69n6t3DnyGvkKobf9FdN2H9oiVDdaMpo2MMY",
		Options:      &rpc.DisplayOptions{ShowFungible: true, ShowNativeBalance: true},
		Paging:       rpc.Paging{Page: 1, Limit: 100},
	})
	if err != nil {
		log.Fatal(err)
	}
	if sol := portfolio.NativeBalance; sol != nil && sol.TotalPrice != nil {
		fmt.Printf("SOL: %d lamports ($%.2f)\n", sol.Lamports, *sol.TotalPrice)
	}
	fmt.Printf("%d assets on page 1\n", len(portfolio.Items))

	tokens, err := client.SearchAssets(ctx, &rpc.SearchAssetsRequest{
		OwnerAddress: "86xCnPeV69n6t3DnyGvkKobf9FdN2H9oiVDdaMpo2MMY",
		TokenType:    rpc.TokenTypeFungible,
	})
	if err != nil {
		log.Fatal(err)
	}
	for _, t := range tokens.Items {
		if t.TokenInfo != nil && t.TokenInfo.Balance != nil {
			fmt.Printf("%10s %d\n", t.TokenInfo.Symbol, *t.TokenInfo.Balance)
		}
	}

	withProof, err := client.GetAssetWithProof(ctx, "AssVbUqhyK9JWTRKTmd7verNPPCgFqupUMbhDDsc4TTU", nil)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("cNFT leaf %d in tree %s, %d proof nodes\n",
		withProof.Asset.Compression.LeafID, withProof.Proof.TreeID, len(withProof.Proof.Proof))

	// Walk a whole collection, page by page.
	holders := map[string]int{}
	pager := client.AssetPages(rpc.AssetsInCollection("CDDyz6MHYY8972DPCXiEeReCxGqJSCbxHnimrvoyWrFF"))
	for {
		page, ok, err := pager.Next(ctx)
		if err != nil {
			log.Fatal(err)
		}
		if !ok {
			break
		}
		for _, a := range page.Items {
			if owner := a.Owner(); owner != "" {
				holders[owner]++
			}
		}
	}
	fmt.Printf("%d holders\n", len(holders))
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
