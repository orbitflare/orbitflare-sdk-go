package tests

// End-to-end DAS tests against a live endpoint. Skipped unless ORBITFLARE_LICENSE_KEY is set:
//
//	ORBITFLARE_LICENSE_KEY=... go test ./tests -run DAS -count=1 -v
//
// ORBITFLARE_RPC_URL overrides the endpoint (default Frankfurt).

import (
	"context"
	"errors"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/orbitflare/orbitflare-sdk-go/orbitflare"
	"github.com/orbitflare/orbitflare-sdk-go/rpc"
)

const (
	orbitflarePass          = "GJfYbfW7gufzFYfm8gFatrUE71wYsjay6FwzDkTa3vaD"
	orbitflarePasses        = "CDDyz6MHYY8972DPCXiEeReCxGqJSCbxHnimrvoyWrFF"
	orbitflarePassAuthority = "9Ppfjr8tFAwK9xxvXsVN2xAaShRLZu5ZzDAwnCCLxewo"
	// The OrbitFlare Pass creator is unverified, so the verified-creator check uses Mad Lads.
	madLadsCreator = "5XvhfmRjwXkGp3jHGmaKpqeerNYjkuZZBYLVQYdeVcRv"
	// Unicat #91.
	cnft           = "AssVbUqhyK9JWTRKTmd7verNPPCgFqupUMbhDDsc4TTU"
	cnftCollection = "EuRteU98mnoRAer1eSc8do2BLeyxkY3aWAbTW27BSMUx"
	usdc           = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v"
	wallet         = "86xCnPeV69n6t3DnyGvkKobf9FdN2H9oiVDdaMpo2MMY"
	notAnAsset     = "11111111111111111111111111111111"
)

func dasClient(t *testing.T) (*rpc.Client, context.Context) {
	t.Helper()
	liveKey(t)
	url := os.Getenv("ORBITFLARE_RPC_URL")
	if url == "" {
		url = "https://fra.rpc.orbitflare.com"
	}
	c, err := rpc.NewBuilder().URL(url).APIKey(os.Getenv("ORBITFLARE_LICENSE_KEY")).Build()
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	t.Cleanup(cancel)
	return c, ctx
}

func rpcCode(err error) int64 {
	var e *orbitflare.RPCError
	if errors.As(err, &e) {
		return e.Code
	}
	return 0
}

func TestLiveDASGetAsset(t *testing.T) {
	c, ctx := dasClient(t)
	a, err := c.GetAsset(ctx, orbitflarePass, nil)
	if err != nil {
		t.Fatal(err)
	}
	if a.ID != orbitflarePass || a.Interface != rpc.InterfaceProgrammableNFT || a.Collection() != orbitflarePasses {
		t.Fatalf("asset = %+v", a)
	}
	if !strings.HasPrefix(a.Name(), "OrbitFlare Pass") {
		t.Fatalf("name = %q", a.Name())
	}
	if a.Owner() == "" || a.LastIndexedSlot == nil || *a.LastIndexedSlot == 0 {
		t.Fatal("owner or last_indexed_slot missing")
	}
	t.Logf("%s owned by %s", a.Name(), a.Owner())

	withMeta, err := c.GetAsset(ctx, orbitflarePass, &rpc.DisplayOptions{ShowCollectionMetadata: true})
	if err != nil {
		t.Fatal(err)
	}
	if len(withMeta.Grouping[0].CollectionMetadata) == 0 {
		t.Fatal("collection_metadata missing")
	}
}

func TestLiveDASGetAssetFungibleAndCompressed(t *testing.T) {
	c, ctx := dasClient(t)
	u, err := c.GetAsset(ctx, usdc, nil)
	if err != nil {
		t.Fatal(err)
	}
	if u.Interface != rpc.InterfaceFungibleToken || u.TokenInfo == nil || *u.TokenInfo.Decimals != 6 {
		t.Fatalf("usdc = %+v", u)
	}
	n, err := c.GetAsset(ctx, cnft, nil)
	if err != nil {
		t.Fatal(err)
	}
	if !n.IsCompressed() || n.Collection() != cnftCollection {
		t.Fatalf("cnft = %+v", n)
	}
}

func TestLiveDASGetAssetNotFound(t *testing.T) {
	c, ctx := dasClient(t)
	_, err := c.GetAsset(ctx, notAnAsset, nil)
	if rpcCode(err) != -32000 {
		t.Fatalf("err = %v", err)
	}
}

func TestLiveDASGetAssetBatch(t *testing.T) {
	c, ctx := dasClient(t)
	assets, err := c.GetAssetBatch(ctx, []string{orbitflarePass, notAnAsset, cnft}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(assets) != 3 || assets[0].ID != orbitflarePass || assets[1] != nil || assets[2].ID != cnft {
		t.Fatalf("assets = %v", assets)
	}
}

func TestLiveDASProofs(t *testing.T) {
	c, ctx := dasClient(t)
	p, err := c.GetAssetProof(ctx, cnft)
	if err != nil {
		t.Fatal(err)
	}
	if len(p.Proof) == 0 || p.Root == "" {
		t.Fatalf("proof = %+v", p)
	}
	both, err := c.GetAssetWithProof(ctx, cnft, nil)
	if err != nil {
		t.Fatal(err)
	}
	if both.Proof.TreeID != both.Asset.Compression.Tree || both.Proof.Leaf != both.Asset.Compression.AssetHash {
		t.Fatalf("asset and proof disagree: %+v", both)
	}
	batch, err := c.GetAssetProofBatch(ctx, []string{cnft, orbitflarePass})
	if err != nil {
		t.Fatal(err)
	}
	if batch[cnft] == nil || batch[orbitflarePass] != nil {
		t.Fatalf("batch = %v", batch)
	}
	if _, err := c.GetAssetProof(ctx, orbitflarePass); rpcCode(err) != -32000 {
		t.Fatalf("err = %v", err)
	}
}

func TestLiveDASGetAssetsByOwnerWithTokens(t *testing.T) {
	c, ctx := dasClient(t)
	list, err := c.GetAssetsByOwner(ctx, &rpc.GetAssetsByOwnerRequest{
		OwnerAddress: wallet,
		Options:      &rpc.DisplayOptions{ShowFungible: true, ShowNativeBalance: true},
		Paging:       rpc.Paging{Page: 1, Limit: 50},
	})
	if err != nil {
		t.Fatal(err)
	}
	if list.Page == nil || *list.Page != 1 || len(list.Items) > 50 || list.NativeBalance == nil {
		t.Fatalf("list = %+v", list)
	}
	for _, a := range list.Items {
		if o := a.Owner(); o != "" && o != wallet {
			t.Fatalf("%s owned by %s", a.ID, o)
		}
	}
}

func TestLiveDASListMethods(t *testing.T) {
	c, ctx := dasClient(t)
	group, err := c.GetAssetsByGroup(ctx, &rpc.GetAssetsByGroupRequest{GroupKey: "collection", GroupValue: orbitflarePasses, Paging: rpc.Paging{Limit: 3}})
	if err != nil {
		t.Fatal(err)
	}
	if len(group.Items) != 3 || group.Cursor == "" {
		t.Fatalf("group = %+v", group)
	}
	for _, a := range group.Items {
		if a.Collection() != orbitflarePasses {
			t.Fatalf("%s is in %s", a.ID, a.Collection())
		}
	}

	creator, err := c.GetAssetsByCreator(ctx, &rpc.GetAssetsByCreatorRequest{CreatorAddress: madLadsCreator, OnlyVerified: true, Paging: rpc.Paging{Limit: 2}})
	if err != nil {
		t.Fatal(err)
	}
	if len(creator.Items) == 0 {
		t.Fatal("no assets by creator")
	}
	verified := false
	for _, cr := range creator.Items[0].Creators {
		verified = verified || (cr.Address == madLadsCreator && cr.Verified)
	}
	if !verified {
		t.Fatal("creator not verified on the first asset")
	}

	authority, err := c.GetAssetsByAuthority(ctx, &rpc.GetAssetsByAuthorityRequest{AuthorityAddress: orbitflarePassAuthority, Paging: rpc.Paging{Page: 1, Limit: 2}})
	if err != nil {
		t.Fatal(err)
	}
	if len(authority.Items) == 0 {
		t.Fatal("no assets by authority")
	}

	sorted, err := c.GetAssetsByGroup(ctx, &rpc.GetAssetsByGroupRequest{
		GroupKey: "collection", GroupValue: orbitflarePasses,
		SortBy: &rpc.SortBy{SortBy: rpc.SortByCreated, SortDirection: rpc.SortDesc},
		Paging: rpc.Paging{Page: 1, Limit: 2},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(sorted.Items) != 2 {
		t.Fatalf("sorted = %d items", len(sorted.Items))
	}
}

func TestLiveDASSearchAssets(t *testing.T) {
	c, ctx := dasClient(t)
	fungible, err := c.SearchAssets(ctx, &rpc.SearchAssetsRequest{OwnerAddress: wallet, TokenType: rpc.TokenTypeFungible, Paging: rpc.Paging{Limit: 5}})
	if err != nil {
		t.Fatal(err)
	}
	for _, a := range fungible.Items {
		if a.TokenInfo == nil {
			t.Fatalf("%s has no token_info", a.ID)
		}
	}
	inCollection, err := c.SearchAssets(ctx, &rpc.SearchAssetsRequest{
		Grouping: &[2]string{"collection", orbitflarePasses},
		Burnt:    rpc.Ptr(false),
		Paging:   rpc.Paging{Page: 1, Limit: 2},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(inCollection.Items) != 2 {
		t.Fatalf("got %d items", len(inCollection.Items))
	}
	for _, a := range inCollection.Items {
		if a.Burnt {
			t.Fatalf("%s is burnt", a.ID)
		}
	}
}

func TestLiveDASGetTokenAccounts(t *testing.T) {
	c, ctx := dasClient(t)
	list, err := c.GetTokenAccounts(ctx, &rpc.GetTokenAccountsRequest{Mint: usdc, Paging: rpc.Paging{Limit: 5}})
	if err != nil {
		t.Fatal(err)
	}
	if len(list.TokenAccounts) != 5 {
		t.Fatalf("got %d accounts", len(list.TokenAccounts))
	}
	for _, ta := range list.TokenAccounts {
		if ta.Mint != usdc || ta.Amount == 0 {
			t.Fatalf("account = %+v", ta)
		}
	}
}

func TestLiveDASGetNftEditions(t *testing.T) {
	c, ctx := dasClient(t)
	// OrbitFlare Passes aren't master editions, so this only checks the method is wired up.
	e, err := c.GetNftEditions(ctx, &rpc.GetNftEditionsRequest{Mint: orbitflarePass, Paging: rpc.Paging{Limit: 5}})
	switch {
	case err == nil:
		t.Logf("%d editions", e.Supply)
	case rpcCode(err) == -32000:
		t.Logf("not a master edition: %v", err)
	default:
		t.Fatal(err)
	}
}

func TestLiveDASGetSignaturesForAsset(t *testing.T) {
	c, ctx := dasClient(t)
	sigs, err := c.GetSignaturesForAsset(ctx, &rpc.GetSignaturesForAssetRequest{ID: cnft, Paging: rpc.Paging{Limit: 5}})
	if err != nil {
		t.Fatal(err)
	}
	if len(sigs.Items) == 0 {
		t.Fatal("no signatures")
	}
	for _, s := range sigs.Items {
		if s.Signature == "" || s.Instruction == "" {
			t.Fatalf("signature = %+v", s)
		}
	}
	t.Logf("first: %s %s", sigs.Items[0].Instruction, sigs.Items[0].Signature)
}

func TestLiveDASPagesByCursor(t *testing.T) {
	c, ctx := dasClient(t)
	pager := c.AssetPages(&rpc.GetAssetsByGroupRequest{GroupKey: "collection", GroupValue: orbitflarePasses, Paging: rpc.Paging{Limit: 5}})
	seen := map[string]bool{}
	for i := 0; i < 3; i++ {
		page, ok, err := pager.Next(ctx)
		if err != nil || !ok {
			t.Fatalf("page %d: ok=%v err=%v", i, ok, err)
		}
		if len(page.Items) != 5 {
			t.Fatalf("page %d has %d items", i, len(page.Items))
		}
		for _, a := range page.Items {
			seen[a.ID] = true
		}
	}
	if len(seen) != 15 {
		t.Fatalf("pages overlap: %d unique of 15", len(seen))
	}
}
