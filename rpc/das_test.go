package rpc

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/orbitflare/orbitflare-sdk-go/orbitflare"
)

// recorder serves respond(body) for every request and keeps the request bodies.
type recorder struct {
	mu     sync.Mutex
	bodies []map[string]any
}

func (r *recorder) all() []map[string]any {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]map[string]any(nil), r.bodies...)
}

func mockDAS(t *testing.T, respond func(body map[string]any) any) (*Client, *recorder) {
	t.Helper()
	rec := &recorder{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		raw, _ := io.ReadAll(req.Body)
		var body map[string]any
		if err := json.Unmarshal(raw, &body); err != nil {
			t.Errorf("bad request body: %v", err)
		}
		rec.mu.Lock()
		rec.bodies = append(rec.bodies, body)
		rec.mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(respond(body))
	}))
	t.Cleanup(srv.Close)
	c, err := NewBuilder().URL(srv.URL).Retry(singleAttempt()).Build()
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	return c, rec
}

func mockResult(t *testing.T, result string) (*Client, *recorder) {
	t.Helper()
	var r any
	if err := json.Unmarshal([]byte(result), &r); err != nil {
		t.Fatalf("bad fixture: %v", err)
	}
	return mockDAS(t, func(map[string]any) any {
		return map[string]any{"jsonrpc": "2.0", "id": 1, "result": r}
	})
}

// equalJSON compares v with the JSON in want, after a round trip so number types match.
func equalJSON(t *testing.T, v any, want string) {
	t.Helper()
	b, _ := json.Marshal(v)
	var got, exp any
	json.Unmarshal(b, &got)
	if err := json.Unmarshal([]byte(want), &exp); err != nil {
		t.Fatalf("bad expectation: %v", err)
	}
	if !reflect.DeepEqual(got, exp) {
		t.Fatalf("got  %s\nwant %s", b, want)
	}
}

// The compressed DRiP NFT from the getAsset docs.
const dripCNFT = `{
  "last_indexed_slot": 457714576,
  "interface": "V1_NFT",
  "id": "JEKNNLZK6uQaQUEUz9GRoYZ7buHQ4rTN6k3qhX9NG8qU",
  "content": {
    "$schema": "https://schema.metaplex.com/nft1.0.json",
    "json_uri": "https://arweave.net/ymSDHwAqb3hnNKBid03WBUqY1Svj5Hzr0fdAYGvZBKA",
    "files": [{"uri": "https://arweave.net/2dfCrqZmpUTPEaPHuI9QUdZocNa-JYkG-3nBKycF-AM?ext=jpg", "mime": "image/jpeg"}],
    "metadata": {
      "attributes": [{"value": "3", "trait_type": "Season"}, {"value": "Common", "trait_type": "Rarity"}],
      "description": "Combination between Angkor Wat photography with microscopic creation as the sky.",
      "name": "Aurora borealis",
      "symbol": "",
      "token_standard": "NonFungible"
    },
    "links": {
      "external_url": "https://drip.haus/showcase",
      "image": "https://arweave.net/2dfCrqZmpUTPEaPHuI9QUdZocNa-JYkG-3nBKycF-AM?ext=jpg"
    },
    "category": "image"
  },
  "authorities": [{"address": "5warkgXZEtubURnNHN1BcpgSBja31zeSvWudeXQEPpTr", "scopes": ["full"]}],
  "compression": {
    "eligible": false, "compressed": true,
    "data_hash": "6TuHoKagPg1Vqz92K5fiUNJtSC7wXHXSQfadLPnXsP8s",
    "creator_hash": "EuiqSYWYf6KnUcEmorrTrGQovPKF9a1wSVJAk7bfLgnR",
    "asset_hash": "CcMhrzXFkjyLwZdijjRibgSYw4tUzXYALoCxwms4aMFH",
    "tree": "E7fYCRWkNykc8HNZvEMRj5GuNYSwJ3WjA6HuLsh3YHvL",
    "seq": 627605, "leaf_id": 622994
  },
  "grouping": [{"group_key": "collection", "group_value": "DRiP3vrQ7LuvQtmwL74RcejviWmQ1HuuKFcJdUpYG6Di"}],
  "royalty": {"royalty_model": "creators", "target": null, "percent": 0.015, "basis_points": 150, "primary_sale_happened": false, "locked": false},
  "creators": [
    {"address": "DRiPPP2LytGjNZ5fVpdZS7Xi1oANSY3Df1gSxvUKpzny", "share": 0, "verified": true},
    {"address": "WoMbXFtdfH8crq2Zi7bQhfGx2Gv8EN4saP13gcdUGog", "share": 100, "verified": false}
  ],
  "ownership": {"frozen": false, "delegated": false, "delegate": null, "ownership_model": "single", "owner": "5MRNQW3NxNes1EDnxUhqyXwffseQBLpdu6QJF8G4MdN9"},
  "supply": {"print_max_supply": 0, "print_current_supply": 0, "edition_nonce": null},
  "mutable": true,
  "burnt": false
}`

const proofFixture = `{
  "last_indexed_slot": 457714576,
  "root": "3wxyaNRvzuZqDd9jtqthCu87spdKuhcpaKVzP1msGyHW",
  "proof": [
    "8mjbWMPrBwYJeEmQbeYLR7mscS4umW4U3kxibuH8hcY1",
    "DV31TLc5BXCnCQMaQ9H6empbG9i69w4AWkjR5RPiyXr8",
    "G6f1pEKetujNm6JXQxD47eBudxLV1irR1U5atpAXBb4q"
  ],
  "node_index": 1671570,
  "leaf": "CcMhrzXFkjyLwZdijjRibgSYw4tUzXYALoCxwms4aMFH",
  "tree_id": "E7fYCRWkNykc8HNZvEMRj5GuNYSwJ3WjA6HuLsh3YHvL"
}`

func TestDASGetAssetParsesCompressedNFT(t *testing.T) {
	c, rec := mockResult(t, dripCNFT)
	a, err := c.GetAsset(ctx(), "JEKNNLZK6uQaQUEUz9GRoYZ7buHQ4rTN6k3qhX9NG8qU", nil)
	if err != nil {
		t.Fatal(err)
	}
	if a.Interface != InterfaceV1NFT || *a.LastIndexedSlot != 457714576 {
		t.Fatalf("interface/slot = %s/%v", a.Interface, a.LastIndexedSlot)
	}
	if a.Name() != "Aurora borealis" || a.Symbol() != "" {
		t.Fatalf("name/symbol = %q/%q", a.Name(), a.Symbol())
	}
	if a.Owner() != "5MRNQW3NxNes1EDnxUhqyXwffseQBLpdu6QJF8G4MdN9" || a.Collection() != "DRiP3vrQ7LuvQtmwL74RcejviWmQ1HuuKFcJdUpYG6Di" {
		t.Fatalf("owner/collection = %q/%q", a.Owner(), a.Collection())
	}
	if !strings.HasPrefix(a.Image(), "https://arweave.net/") || !a.IsCompressed() {
		t.Fatalf("image/compressed = %q/%v", a.Image(), a.IsCompressed())
	}
	if a.Compression.Seq != 627605 || a.Compression.LeafID != 622994 {
		t.Fatalf("compression = %+v", a.Compression)
	}
	if a.Content.Schema != "https://schema.metaplex.com/nft1.0.json" || a.Content.Files[0].Mime != "image/jpeg" {
		t.Fatalf("content = %+v", a.Content)
	}
	if attr := a.Content.Metadata.Attributes[1]; attr.TraitType != "Rarity" || attr.Value != "Common" {
		t.Fatalf("attribute = %+v", attr)
	}
	if a.Royalty.RoyaltyModel != RoyaltyCreators || a.Royalty.BasisPoints != 150 || a.Royalty.Target != nil {
		t.Fatalf("royalty = %+v", a.Royalty)
	}
	if a.Creators[1].Share != 100 || a.Creators[1].Verified {
		t.Fatalf("creator = %+v", a.Creators[1])
	}
	if !reflect.DeepEqual(a.Authorities[0].Scopes, []Scope{ScopeFull}) || a.Supply.EditionNonce != nil {
		t.Fatalf("scopes/nonce = %v/%v", a.Authorities[0].Scopes, a.Supply.EditionNonce)
	}
	if !a.Mutable || a.Burnt {
		t.Fatal("mutable/burnt")
	}
	b := rec.all()[0]
	if b["method"] != "getAsset" {
		t.Fatalf("method = %v", b["method"])
	}
	equalJSON(t, b["params"], `{"id":"JEKNNLZK6uQaQUEUz9GRoYZ7buHQ4rTN6k3qhX9NG8qU"}`)
}

func TestDASGetAssetParsesFullAssetObject(t *testing.T) {
	c, _ := mockResult(t, `{
	  "interface": "ProgrammableNFT",
	  "id": "JEGruwYE13mhX2wi2MGrPmeLiVyZtbBptmVy9vG3pXRC",
	  "content": {
	    "metadata": {"name": "Mad Lads #6867", "symbol": "MAD", "attributes": [{"value": 7, "trait_type": 1}]},
	    "links": {"image": "https://madlads.s3.us-west-2.amazonaws.com/images/6867.png", "external_url": "https://madlads.com", "extra": "x"}
	  },
	  "compression": {"compressed": false},
	  "ownership": {"frozen": true, "delegated": true, "delegate": "FARqKAafAbgT25QcgiX5d1g6xpadgG7xymu5N6gSmp4x", "ownership_model": "single", "owner": "CxBhuJwhVgNMc7yjnRx3XFAsTnnbYDc2bhAGSaLBjNqZ"},
	  "supply": {"print_max_supply": 0, "print_current_supply": 0, "edition_nonce": 254},
	  "token_info": {"supply": 1, "decimals": 0, "token_program": "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA", "associated_token_address": "J4z9rkXuQFVFYEcE4FnbZrT677Eh6WU3f125wyaDyQXg"},
	  "mpl_core_info": {"num_minted": 3, "current_size": 3, "plugins_json_version": 1},
	  "some_future_field": {"x": 1}
	}`)
	a, err := c.GetAsset(ctx(), "JEGruwYE13mhX2wi2MGrPmeLiVyZtbBptmVy9vG3pXRC", nil)
	if err != nil {
		t.Fatal(err)
	}
	if a.Interface != InterfaceProgrammableNFT || a.Symbol() != "MAD" || a.IsCompressed() {
		t.Fatalf("asset = %+v", a)
	}
	o := a.Ownership
	if !o.Frozen || !o.Delegated || o.OwnershipModel != OwnershipSingle || *o.Delegate != "FARqKAafAbgT25QcgiX5d1g6xpadgG7xymu5N6gSmp4x" {
		t.Fatalf("ownership = %+v", o)
	}
	if *a.Supply.EditionNonce != 254 || *a.TokenInfo.Supply != 1 || *a.TokenInfo.Decimals != 0 || *a.MplCoreInfo.NumMinted != 3 {
		t.Fatal("supply/token_info/mpl_core_info")
	}
	attr := a.Content.Metadata.Attributes[0]
	if attr.TraitType != "1" || attr.Value != float64(7) {
		t.Fatalf("hand-written attribute = %+v", attr)
	}
	if a.Content.Links.ExternalURL != "https://madlads.com" || a.Content.Links.Other["extra"] != "x" {
		t.Fatalf("links = %+v", a.Content.Links)
	}
}

func TestDASGetAssetToleratesNullsAndUnknownValues(t *testing.T) {
	c, _ := mockResult(t, `{
	  "interface": "SomethingNew", "id": "x",
	  "content": {"$schema": null, "json_uri": null, "files": null, "metadata": {"name": null, "symbol": null}, "links": {}},
	  "authorities": null, "grouping": null, "creators": null,
	  "ownership": {"owner": null, "ownership_model": "weird", "frozen": null, "delegated": false, "delegate": null},
	  "royalty": {"royalty_model": "single", "target": "abc", "percent": null, "basis_points": null},
	  "supply": null, "mutable": null, "burnt": false
	}`)
	a, err := c.GetAsset(ctx(), "x", nil)
	if err != nil {
		t.Fatal(err)
	}
	if a.Interface != "SomethingNew" || a.Name() != "" || a.Owner() != "" || a.Collection() != "" {
		t.Fatalf("asset = %+v", a)
	}
	if len(a.Creators) != 0 || a.Ownership.OwnershipModel != "weird" || a.Royalty.RoyaltyModel != RoyaltySingle || a.Supply != nil {
		t.Fatalf("asset = %+v", a)
	}
}

func TestDASGetAssetSendsOptionsOnlyWhenSet(t *testing.T) {
	c, rec := mockResult(t, dripCNFT)
	if _, err := c.GetAsset(ctx(), "id", &DisplayOptions{ShowCollectionMetadata: true, ShowUnverifiedCollections: true}); err != nil {
		t.Fatal(err)
	}
	if _, err := c.GetAsset(ctx(), "id", &DisplayOptions{}); err != nil {
		t.Fatal(err)
	}
	b := rec.all()
	equalJSON(t, b[0]["params"], `{"id":"id","options":{"showCollectionMetadata":true,"showUnverifiedCollections":true}}`)
	equalJSON(t, b[1]["params"], `{"id":"id"}`)
}

// echoBatch answers a batch with an asset per ID, or null for "missing".
func echoBatch(body map[string]any) any {
	ids := body["params"].(map[string]any)["ids"].([]any)
	out := make([]any, len(ids))
	for i, id := range ids {
		if id != "missing" {
			out[i] = map[string]any{"interface": "V1_NFT", "id": id}
		}
	}
	return map[string]any{"jsonrpc": "2.0", "id": 1, "result": out}
}

func TestDASGetAssetBatchKeepsOrderAndNulls(t *testing.T) {
	c, rec := mockDAS(t, echoBatch)
	assets, err := c.GetAssetBatch(ctx(), []string{"a", "missing", "b"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(assets) != 3 || assets[0].ID != "a" || assets[1] != nil || assets[2].ID != "b" {
		t.Fatalf("assets = %v", assets)
	}
	if rec.all()[0]["method"] != "getAssetBatch" {
		t.Fatal("method")
	}
}

func TestDASGetAssetBatchSplitsIntoChunksOf1000(t *testing.T) {
	c, rec := mockDAS(t, echoBatch)
	ids := make([]string, 2500)
	for i := range ids {
		ids[i] = fmt.Sprintf("id%d", i)
	}
	assets, err := c.GetAssetBatch(ctx(), ids, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(assets) != 2500 || assets[2499].ID != "id2499" {
		t.Fatalf("got %d assets", len(assets))
	}
	var sizes []int
	for _, b := range rec.all() {
		sizes = append(sizes, len(b["params"].(map[string]any)["ids"].([]any)))
	}
	if !reflect.DeepEqual(sizes, []int{1000, 1000, 500}) {
		t.Fatalf("chunk sizes = %v", sizes)
	}
}

func TestDASGetAssetBatchWithNoIDsMakesNoRequest(t *testing.T) {
	c, rec := mockDAS(t, echoBatch)
	assets, err := c.GetAssetBatch(ctx(), nil, nil)
	if err != nil || len(assets) != 0 || len(rec.all()) != 0 {
		t.Fatalf("assets=%v err=%v requests=%d", assets, err, len(rec.all()))
	}
}

func TestDASGetAssetProofAndProofPath(t *testing.T) {
	c, rec := mockResult(t, proofFixture)
	p, err := c.GetAssetProof(ctx(), "id")
	if err != nil {
		t.Fatal(err)
	}
	if p.Root != "3wxyaNRvzuZqDd9jtqthCu87spdKuhcpaKVzP1msGyHW" || p.NodeIndex != 1671570 || p.TreeID != "E7fYCRWkNykc8HNZvEMRj5GuNYSwJ3WjA6HuLsh3YHvL" {
		t.Fatalf("proof = %+v", p)
	}
	if len(p.ProofPath(0)) != 3 || !reflect.DeepEqual(p.ProofPath(1), p.Proof[:2]) || len(p.ProofPath(10)) != 0 {
		t.Fatal("ProofPath")
	}
	if rec.all()[0]["method"] != "getAssetProof" {
		t.Fatal("method")
	}
}

func TestDASGetAssetProofBatchMapsIDs(t *testing.T) {
	c, rec := mockResult(t, `{"JEKNNLZK6uQaQUEUz9GRoYZ7buHQ4rTN6k3qhX9NG8qU": `+proofFixture+`, "JEGruwYE13mhX2wi2MGrPmeLiVyZtbBptmVy9vG3pXRC": null}`)
	proofs, err := c.GetAssetProofBatch(ctx(), []string{"JEKNNLZK6uQaQUEUz9GRoYZ7buHQ4rTN6k3qhX9NG8qU", "JEGruwYE13mhX2wi2MGrPmeLiVyZtbBptmVy9vG3pXRC"})
	if err != nil {
		t.Fatal(err)
	}
	if proofs["JEKNNLZK6uQaQUEUz9GRoYZ7buHQ4rTN6k3qhX9NG8qU"].NodeIndex != 1671570 || proofs["JEGruwYE13mhX2wi2MGrPmeLiVyZtbBptmVy9vG3pXRC"] != nil {
		t.Fatalf("proofs = %v", proofs)
	}
	if rec.all()[0]["method"] != "getAssetProofBatch" {
		t.Fatal("method")
	}
}

func TestDASGetAssetWithProofReturnsBoth(t *testing.T) {
	c, rec := mockResult(t, `{"asset": `+dripCNFT+`, "proof": `+proofFixture+`}`)
	both, err := c.GetAssetWithProof(ctx(), "id", nil)
	if err != nil {
		t.Fatal(err)
	}
	if both.Asset.Compression.LeafID != 622994 || both.Proof.Leaf != both.Asset.Compression.AssetHash {
		t.Fatalf("both = %+v", both)
	}
	if rec.all()[0]["method"] != "getAssetWithProof" {
		t.Fatal("method")
	}
}

func TestDASGetAssetsByOwnerParsesTokensAndNativeBalance(t *testing.T) {
	c, rec := mockResult(t, `{
	  "last_indexed_slot": 457714577, "total": 1, "limit": 100, "page": 1,
	  "nativeBalance": {"lamports": 1328527472954, "price_per_sol": 101.5, "total_price": 134845.54},
	  "items": [{
	    "interface": "FungibleToken",
	    "id": "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v",
	    "ownership": {"owner": "86xCnPeV69n6t3DnyGvkKobf9FdN2H9oiVDdaMpo2MMY", "ownership_model": "token"},
	    "token_info": {"symbol": "USDC", "balance": 2500000, "decimals": 6, "token_program": "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA",
	      "price_info": {"price_per_token": 1.0, "total_price": 2.5, "currency": "USDC"}}
	  }]
	}`)
	list, err := c.GetAssetsByOwner(ctx(), &GetAssetsByOwnerRequest{
		OwnerAddress: "86xCnPeV69n6t3DnyGvkKobf9FdN2H9oiVDdaMpo2MMY",
		Options:      &DisplayOptions{ShowFungible: true, ShowNativeBalance: true},
		Paging:       Paging{Page: 1, Limit: 100},
	})
	if err != nil {
		t.Fatal(err)
	}
	if list.Total != 1 || list.Limit != 100 || *list.Page != 1 {
		t.Fatalf("list = %+v", list)
	}
	if list.NativeBalance.Lamports != 1328527472954 || *list.NativeBalance.PricePerSol != 101.5 {
		t.Fatalf("native = %+v", list.NativeBalance)
	}
	usdc := list.Items[0]
	if usdc.Interface != InterfaceFungibleToken || usdc.Ownership.OwnershipModel != OwnershipToken {
		t.Fatalf("usdc = %+v", usdc)
	}
	if *usdc.TokenInfo.Balance != 2500000 || *usdc.TokenInfo.Decimals != 6 || *usdc.TokenInfo.PriceInfo.TotalPrice != 2.5 {
		t.Fatalf("token_info = %+v", usdc.TokenInfo)
	}
	b := rec.all()[0]
	if b["method"] != "getAssetsByOwner" {
		t.Fatal("method")
	}
	equalJSON(t, b["params"], `{"ownerAddress":"86xCnPeV69n6t3DnyGvkKobf9FdN2H9oiVDdaMpo2MMY","page":1,"limit":100,"options":{"showFungible":true,"showNativeBalance":true}}`)
}

func TestDASListRequestsSendDocumentedParams(t *testing.T) {
	c, rec := mockResult(t, `{"total": 0, "limit": 1000, "items": []}`)
	group := AssetsInCollection("J1S9H3QjnRtBbbuD4HjPV6RpRhwuk4zKbxsnCHuTgh9w")
	group.SortBy = &SortBy{SortBy: SortByRecentAction, SortDirection: SortDesc}
	group.Page = 2
	group.Options = &DisplayOptions{}
	if _, err := c.GetAssetsByGroup(ctx(), group); err != nil {
		t.Fatal(err)
	}
	if _, err := c.GetAssetsByCreator(ctx(), &GetAssetsByCreatorRequest{CreatorAddress: "creator", OnlyVerified: true, Paging: Paging{Cursor: "c1"}}); err != nil {
		t.Fatal(err)
	}
	if _, err := c.GetAssetsByAuthority(ctx(), &GetAssetsByAuthorityRequest{AuthorityAddress: "auth", Paging: Paging{After: "a", Limit: 5}}); err != nil {
		t.Fatal(err)
	}
	b := rec.all()
	if b[0]["method"] != "getAssetsByGroup" || b[1]["method"] != "getAssetsByCreator" || b[2]["method"] != "getAssetsByAuthority" {
		t.Fatal("methods")
	}
	equalJSON(t, b[0]["params"], `{"groupKey":"collection","groupValue":"J1S9H3QjnRtBbbuD4HjPV6RpRhwuk4zKbxsnCHuTgh9w","sortBy":{"sortBy":"recent_action","sortDirection":"desc"},"page":2}`)
	equalJSON(t, b[1]["params"], `{"creatorAddress":"creator","onlyVerified":true,"cursor":"c1"}`)
	equalJSON(t, b[2]["params"], `{"authorityAddress":"auth","after":"a","limit":5}`)
}

func TestDASSearchAssetsSerializesFilters(t *testing.T) {
	c, rec := mockResult(t, `{"total": 0, "limit": 1000, "items": []}`)
	_, err := c.SearchAssets(ctx(), &SearchAssetsRequest{
		OwnerAddress:      "86xCnPeV69n6t3DnyGvkKobf9FdN2H9oiVDdaMpo2MMY",
		Grouping:          &[2]string{"collection", "DRiP3vrQ7LuvQtmwL74RcejviWmQ1HuuKFcJdUpYG6Di"},
		CreatorAddress:    "creator",
		CreatorVerified:   Ptr(true),
		Interface:         InterfaceProgrammableNFT,
		TokenType:         TokenTypeCompressedNFT,
		OwnerType:         OwnershipSingle,
		RoyaltyTargetType: RoyaltyFanout,
		RoyaltyAmount:     Ptr[uint32](500),
		Compressed:        Ptr(true),
		Burnt:             Ptr(false),
		ConditionType:     ConditionAny,
		Negate:            Ptr(true),
		Paging:            Paging{Limit: 10},
	})
	if err != nil {
		t.Fatal(err)
	}
	equalJSON(t, rec.all()[0]["params"], `{
	  "ownerAddress": "86xCnPeV69n6t3DnyGvkKobf9FdN2H9oiVDdaMpo2MMY",
	  "grouping": ["collection", "DRiP3vrQ7LuvQtmwL74RcejviWmQ1HuuKFcJdUpYG6Di"],
	  "creatorAddress": "creator", "creatorVerified": true,
	  "interface": "ProgrammableNFT", "tokenType": "compressedNFT", "ownerType": "single",
	  "royaltyTargetType": "fanout", "royaltyAmount": 500,
	  "compressed": true, "burnt": false, "conditionType": "any", "negate": true, "limit": 10
	}`)
}

func TestDASGetTokenAccountsParsesAccounts(t *testing.T) {
	c, rec := mockResult(t, `{
	  "total": 2, "limit": 2, "page": 1,
	  "token_accounts": [
	    {"address": "1113odRAP5wXa8n7NFVBG53XrqWeDrViZu5rSiSsVA3", "mint": "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v", "owner": "HJNr4feTNiDn59RCL1pYoARXMFoBEQSmAY7R7Cm21z9T",
	     "amount": 170812, "delegate": null, "delegated_amount": 0, "close_authority": null, "frozen": false},
	    {"address": "11NCSQ2Bm6UZdtDqBeHFcos5eFXT1kJ3jG29qKJCWU", "mint": "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v", "owner": "FRPPBdG4sYCH7EnTMeKFSVLsfzfEUo4PQ1wWCUkzzmuv",
	     "amount": 1804811, "delegate": null, "delegated_amount": 0, "close_authority": null, "frozen": true, "token_extensions": {"immutable_owner": true}}
	  ]
	}`)
	list, err := c.GetTokenAccounts(ctx(), &GetTokenAccountsRequest{
		Mint:    "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v",
		Options: &DisplayOptions{ShowZeroBalance: true},
		Paging:  Paging{Page: 1, Limit: 2},
	})
	if err != nil {
		t.Fatal(err)
	}
	ta := list.TokenAccounts[1]
	if len(list.TokenAccounts) != 2 || ta.Amount != 1804811 || !ta.Frozen || ta.TokenExtensions == nil {
		t.Fatalf("accounts = %+v", list.TokenAccounts)
	}
	equalJSON(t, rec.all()[0]["params"], `{"mint":"EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v","page":1,"limit":2,"options":{"showZeroBalance":true}}`)
}

func TestDASGetTokenAccountsNeedsOwnerOrMint(t *testing.T) {
	c, rec := mockResult(t, `{}`)
	_, err := c.GetTokenAccounts(ctx(), &GetTokenAccountsRequest{})
	var cfg *orbitflare.ConfigError
	if !errors.As(err, &cfg) || len(rec.all()) != 0 {
		t.Fatalf("err = %v, requests = %d", err, len(rec.all()))
	}
}

func TestDASGetNftEditionsParsesUnlimitedSupply(t *testing.T) {
	c, rec := mockResult(t, `{
	  "total": 2, "limit": 100, "page": 1,
	  "master_edition_address": "MASTER", "supply": 2, "max_supply": null,
	  "editions": [
	    {"mint": "PRINT_MINT_1", "edition_address": "PRINT_EDITION_1", "edition": 1},
	    {"mint": "PRINT_MINT_2", "edition_address": "PRINT_EDITION_2", "edition": 2}
	  ]
	}`)
	e, err := c.GetNftEditions(ctx(), &GetNftEditionsRequest{Mint: "mint", Paging: Paging{Page: 1, Limit: 100}})
	if err != nil {
		t.Fatal(err)
	}
	if e.Supply != 2 || e.MaxSupply != nil || e.Editions[1].Edition != 2 {
		t.Fatalf("editions = %+v", e)
	}
	equalJSON(t, rec.all()[0]["params"], `{"mint":"mint","page":1,"limit":100}`)
}

func TestDASGetSignaturesForAssetParsesPairs(t *testing.T) {
	c, rec := mockResult(t, `{
	  "last_indexed_slot": 457714577, "total": 1, "limit": 100, "page": 1,
	  "items": [["3eUm7YYq71vcZdgYT3X51xrJeNKaUg5fFfCFaBS2fwP3BPvip2hYxaAaqfNDs3Pj53JPDriQauBtWbz4u7cFN2dw", "MintToCollectionV1"]]
	}`)
	sigs, err := c.GetSignaturesForAsset(ctx(), &GetSignaturesForAssetRequest{Tree: "tree", LeafIndex: Ptr[uint64](622994), SortDirection: SortDesc})
	if err != nil {
		t.Fatal(err)
	}
	if sigs.Items[0].Instruction != "MintToCollectionV1" || !strings.HasPrefix(sigs.Items[0].Signature, "3eUm7") {
		t.Fatalf("items = %+v", sigs.Items)
	}
	equalJSON(t, rec.all()[0]["params"], `{"tree":"tree","leafIndex":622994,"sortDirection":"desc"}`)
}

func TestDASGetSignaturesForAssetNeedsAnAsset(t *testing.T) {
	c, _ := mockResult(t, `{}`)
	_, err := c.GetSignaturesForAsset(ctx(), &GetSignaturesForAssetRequest{Tree: "tree"})
	var cfg *orbitflare.ConfigError
	if !errors.As(err, &cfg) {
		t.Fatalf("err = %v", err)
	}
}

// paged serves pages of assets with the given sizes, by cursor or by page number.
func paged(sizes []int) func(map[string]any) any {
	return func(body map[string]any) any {
		p := body["params"].(map[string]any)
		index := 0
		if page, ok := p["page"].(float64); ok {
			index = int(page) - 1
		} else if cur, ok := p["cursor"].(string); ok {
			index, _ = strconv.Atoi(strings.TrimPrefix(cur, "c"))
		}
		n := 0
		if index < len(sizes) {
			n = sizes[index]
		}
		items := make([]any, n)
		for i := range items {
			items[i] = map[string]any{"interface": "V1_NFT", "id": fmt.Sprintf("%d-%d", index, i)}
		}
		limit, ok := p["limit"]
		if !ok {
			limit = 1000
		}
		r := map[string]any{"total": n, "limit": limit, "items": items}
		if _, ok := p["page"]; ok {
			r["page"] = p["page"]
		} else {
			r["cursor"] = fmt.Sprintf("c%d", index+1)
		}
		return map[string]any{"jsonrpc": "2.0", "id": 1, "result": r}
	}
}

func TestDASPagesWalkByCursorUntilAShortPage(t *testing.T) {
	c, rec := mockDAS(t, paged([]int{1000, 1000, 3}))
	items, err := c.AssetPages(AssetsInCollection("col")).All(ctx())
	if err != nil {
		t.Fatal(err)
	}
	if len(items) != 2003 || items[2002].ID != "2-2" {
		t.Fatalf("got %d items", len(items))
	}
	var cursors []any
	for _, b := range rec.all() {
		p := b["params"].(map[string]any)
		cursors = append(cursors, p["cursor"])
		if _, ok := p["page"]; ok {
			t.Fatal("cursor walk sent a page")
		}
	}
	if !reflect.DeepEqual(cursors, []any{nil, "c1", "c2"}) {
		t.Fatalf("cursors = %v", cursors)
	}
}

func TestDASPagesWalkByNumberWhenNotSortedByID(t *testing.T) {
	c, rec := mockDAS(t, paged([]int{2, 2, 1}))
	req := &GetAssetsByOwnerRequest{OwnerAddress: "owner", SortBy: &SortBy{SortBy: SortByCreated, SortDirection: SortDesc}, Paging: Paging{Limit: 2}}
	pager := c.AssetPages(req)
	var seen [][2]int
	for {
		page, ok, err := pager.Next(ctx())
		if err != nil {
			t.Fatal(err)
		}
		if !ok {
			break
		}
		seen = append(seen, [2]int{int(*page.Page), len(page.Items)})
	}
	if !reflect.DeepEqual(seen, [][2]int{{1, 2}, {2, 2}, {3, 1}}) {
		t.Fatalf("pages = %v", seen)
	}
	for _, b := range rec.all() {
		if _, ok := b["params"].(map[string]any)["cursor"]; ok {
			t.Fatal("page walk sent a cursor")
		}
	}
	if req.Page != 0 {
		t.Fatal("the caller's request was changed")
	}
}

func TestDASPagesStopOnAnEmptyPageAfterAFullOne(t *testing.T) {
	c, rec := mockDAS(t, paged([]int{2, 0}))
	items, err := c.AssetPages(&SearchAssetsRequest{OwnerAddress: "o", Paging: Paging{Limit: 2}}).All(ctx())
	if err != nil || len(items) != 2 || len(rec.all()) != 2 {
		t.Fatalf("items=%d err=%v requests=%d", len(items), err, len(rec.all()))
	}
}

func TestDASSignaturePagesAlwaysUsePageNumbers(t *testing.T) {
	c, rec := mockResult(t, `{"total": 1, "limit": 1000, "page": 1, "items": [["sig", "Transfer"]]}`)
	items, err := c.SignaturePages(&GetSignaturesForAssetRequest{ID: "asset"}).All(ctx())
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(items, []AssetSignature{{Signature: "sig", Instruction: "Transfer"}}) {
		t.Fatalf("items = %v", items)
	}
	equalJSON(t, rec.all()[0]["params"], `{"id":"asset","page":1}`)
}

func TestDASAssetNotFoundIsAnRPCErrorAndNotRetried(t *testing.T) {
	rec := &recorder{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		rec.mu.Lock()
		rec.bodies = append(rec.bodies, nil)
		rec.mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		io.WriteString(w, `{"jsonrpc":"2.0","error":{"code":-32000,"message":"Database Error: RecordNotFound Error: Asset Not Found"},"id":1}`)
	}))
	t.Cleanup(srv.Close)
	retry := orbitflare.DefaultRetryPolicy()
	retry.MaxAttempts = 5
	retry.InitialDelay = time.Millisecond
	c, err := NewBuilder().URL(srv.URL).Retry(retry).Build()
	if err != nil {
		t.Fatal(err)
	}

	_, err = c.GetAsset(ctx(), "11111111111111111111111111111111", nil)
	var rpcErr *orbitflare.RPCError
	if !errors.As(err, &rpcErr) || rpcErr.Code != -32000 || !strings.Contains(rpcErr.Message, "Asset Not Found") {
		t.Fatalf("err = %v", err)
	}
	if len(rec.all()) != 1 {
		t.Fatalf("requests = %d, want 1", len(rec.all()))
	}
}

func TestDASInvalidParamsSurfaceTheServerMessage(t *testing.T) {
	c, _ := mockDAS(t, func(map[string]any) any {
		return map[string]any{"jsonrpc": "2.0", "id": 1, "error": map[string]any{"code": -32602, "message": "Pagination Error. Limit should not be greater than 1000."}}
	})
	_, err := c.GetAssetsByOwner(ctx(), &GetAssetsByOwnerRequest{OwnerAddress: "o", Paging: Paging{Limit: 5000}})
	if err == nil || !strings.Contains(err.Error(), "Limit should not be greater than 1000") {
		t.Fatalf("err = %v", err)
	}
}
