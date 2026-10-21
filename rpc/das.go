package rpc

// Digital Asset Standard (DAS) API: NFTs, compressed NFTs, MPL Core assets and SPL tokens
// through one set of JSON-RPC methods, served on the same endpoint as standard RPC.

import (
	"context"
	"encoding/json"
	"strings"

	"github.com/orbitflare/orbitflare-sdk-go/orbitflare"
)

const (
	// DASMaxBatch is the most IDs a batch method takes per request. Larger batches are split
	// automatically.
	DASMaxBatch = 1000
	// DASMaxLimit is the largest page size, and the server's default.
	DASMaxLimit = 1000
)

// Ptr returns a pointer to v, for the optional request fields.
func Ptr[T any](v T) *T { return &v }

func (c *Client) GetAsset(ctx context.Context, id string, options *DisplayOptions) (*Asset, error) {
	return dasCall[*Asset](ctx, c, "getAsset", withOptions(map[string]any{"id": id}, options))
}

// GetAssetBatch returns one entry per ID, in order, with nil for IDs that are not indexed assets.
func (c *Client) GetAssetBatch(ctx context.Context, ids []string, options *DisplayOptions) ([]*Asset, error) {
	out := make([]*Asset, 0, len(ids))
	for i := 0; i < len(ids); i += DASMaxBatch {
		chunk := ids[i:min(i+DASMaxBatch, len(ids))]
		assets, err := dasCall[[]*Asset](ctx, c, "getAssetBatch", withOptions(map[string]any{"ids": chunk}, options))
		if err != nil {
			return nil, err
		}
		out = append(out, assets...)
	}
	return out, nil
}

func (c *Client) GetAssetProof(ctx context.Context, id string) (*AssetProof, error) {
	return dasCall[*AssetProof](ctx, c, "getAssetProof", map[string]any{"id": id})
}

// GetAssetProofBatch returns proofs keyed by asset ID, with nil where no proof was found.
func (c *Client) GetAssetProofBatch(ctx context.Context, ids []string) (map[string]*AssetProof, error) {
	out := make(map[string]*AssetProof, len(ids))
	for i := 0; i < len(ids); i += DASMaxBatch {
		chunk := ids[i:min(i+DASMaxBatch, len(ids))]
		proofs, err := dasCall[map[string]*AssetProof](ctx, c, "getAssetProofBatch", map[string]any{"ids": chunk})
		if err != nil {
			return nil, err
		}
		for k, v := range proofs {
			out[k] = v
		}
	}
	return out, nil
}

// GetAssetWithProof returns a compressed asset and its Merkle proof in one call: everything a
// Bubblegum transfer, burn or delegate instruction needs.
func (c *Client) GetAssetWithProof(ctx context.Context, id string, options *DisplayOptions) (*AssetWithProof, error) {
	return dasCall[*AssetWithProof](ctx, c, "getAssetWithProof", withOptions(map[string]any{"id": id}, options))
}

func (c *Client) GetAssetsByOwner(ctx context.Context, req *GetAssetsByOwnerRequest) (*AssetList, error) {
	return dasCall[*AssetList](ctx, c, req.dasMethod(), req.payload())
}

func (c *Client) GetAssetsByGroup(ctx context.Context, req *GetAssetsByGroupRequest) (*AssetList, error) {
	return dasCall[*AssetList](ctx, c, req.dasMethod(), req.payload())
}

func (c *Client) GetAssetsByCreator(ctx context.Context, req *GetAssetsByCreatorRequest) (*AssetList, error) {
	return dasCall[*AssetList](ctx, c, req.dasMethod(), req.payload())
}

func (c *Client) GetAssetsByAuthority(ctx context.Context, req *GetAssetsByAuthorityRequest) (*AssetList, error) {
	return dasCall[*AssetList](ctx, c, req.dasMethod(), req.payload())
}

func (c *Client) SearchAssets(ctx context.Context, req *SearchAssetsRequest) (*AssetList, error) {
	return dasCall[*AssetList](ctx, c, req.dasMethod(), req.payload())
}

func (c *Client) GetTokenAccounts(ctx context.Context, req *GetTokenAccountsRequest) (*TokenAccountList, error) {
	if err := req.validate(); err != nil {
		return nil, err
	}
	return dasCall[*TokenAccountList](ctx, c, req.dasMethod(), req.payload())
}

func (c *Client) GetNftEditions(ctx context.Context, req *GetNftEditionsRequest) (*NftEditions, error) {
	return dasCall[*NftEditions](ctx, c, req.dasMethod(), req.payload())
}

func (c *Client) GetSignaturesForAsset(ctx context.Context, req *GetSignaturesForAssetRequest) (*AssetSignatures, error) {
	if err := req.validate(); err != nil {
		return nil, err
	}
	return dasCall[*AssetSignatures](ctx, c, req.dasMethod(), req.payload())
}

// AssetPages walks every page of an asset list request. Requests sorted by id page by cursor;
// other sorts, and requests that set Page, page by number. The request is copied, not changed.
func (c *Client) AssetPages(req AssetListRequest) *Pager[AssetList, Asset] {
	return newPager[AssetList, Asset](c, req)
}

func (c *Client) TokenAccountPages(req *GetTokenAccountsRequest) *Pager[TokenAccountList, TokenAccount] {
	return newPager[TokenAccountList, TokenAccount](c, req)
}

func (c *Client) NftEditionPages(req *GetNftEditionsRequest) *Pager[NftEditions, NftEdition] {
	return newPager[NftEditions, NftEdition](c, req)
}

// SignaturePages pages by number; getSignaturesForAsset takes no cursor.
func (c *Client) SignaturePages(req *GetSignaturesForAssetRequest) *Pager[AssetSignatures, AssetSignature] {
	return newPager[AssetSignatures, AssetSignature](c, req)
}

func dasCall[T any](ctx context.Context, c *Client, method string, params any) (T, error) {
	var out T
	raw, err := c.Request(ctx, method, params)
	if err != nil {
		return out, err
	}
	if err := json.Unmarshal(raw, &out); err != nil {
		return out, &orbitflare.SerializationError{Message: method + ": " + err.Error()}
	}
	return out, nil
}

func withOptions(params map[string]any, options *DisplayOptions) map[string]any {
	if !options.empty() {
		params["options"] = options
	}
	return params
}

/* ---------- Requests ---------- */

// DisplayOptions controls what a response includes. Every option defaults to off.
type DisplayOptions struct {
	ShowFungible              bool `json:"showFungible,omitempty"`
	ShowNativeBalance         bool `json:"showNativeBalance,omitempty"`
	ShowZeroBalance           bool `json:"showZeroBalance,omitempty"`
	ShowUnverifiedCollections bool `json:"showUnverifiedCollections,omitempty"`
	ShowCollectionMetadata    bool `json:"showCollectionMetadata,omitempty"`
	ShowInscription           bool `json:"showInscription,omitempty"`
}

func (o *DisplayOptions) empty() bool { return o == nil || *o == DisplayOptions{} }

// Paging holds the paging parameters shared by the list methods. Use one of Page, Cursor, or
// Before/After. Zero values are left out.
type Paging struct {
	Page   uint32 `json:"page,omitempty"`
	Limit  uint32 `json:"limit,omitempty"`
	Cursor string `json:"cursor,omitempty"`
	Before string `json:"before,omitempty"`
	After  string `json:"after,omitempty"`
}

type AssetSortBy string

const (
	SortByID           AssetSortBy = "id"
	SortByCreated      AssetSortBy = "created"
	SortByUpdated      AssetSortBy = "updated"
	SortByRecentAction AssetSortBy = "recent_action"
	SortByNone         AssetSortBy = "none"
)

type SortDirection string

const (
	SortAsc  SortDirection = "asc"
	SortDesc SortDirection = "desc"
)

type SortBy struct {
	SortBy        AssetSortBy   `json:"sortBy"`
	SortDirection SortDirection `json:"sortDirection,omitempty"`
}

type TokenType string

const (
	TokenTypeFungible      TokenType = "fungible"
	TokenTypeNonFungible   TokenType = "nonFungible"
	TokenTypeRegularNFT    TokenType = "regularNFT"
	TokenTypeCompressedNFT TokenType = "compressedNFT"
	TokenTypeAll           TokenType = "all"
)

type ConditionType string

const (
	ConditionAll ConditionType = "all"
	ConditionAny ConditionType = "any"
)

// AssetListRequest is a request that returns an AssetList: GetAssetsByOwnerRequest,
// GetAssetsByGroupRequest, GetAssetsByCreatorRequest, GetAssetsByAuthorityRequest or
// SearchAssetsRequest.
type AssetListRequest interface {
	pagedRequest
	assetList()
}

type pagedRequest interface {
	dasMethod() string
	paging() *Paging
	pagesOnly() bool
	sortedByID() bool
	clone() pagedRequest
	payload() any
}

func sortedByID(s *SortBy) bool { return s == nil || s.SortBy == "" || s.SortBy == SortByID }

type GetAssetsByOwnerRequest struct {
	OwnerAddress string          `json:"ownerAddress"`
	SortBy       *SortBy         `json:"sortBy,omitempty"`
	Options      *DisplayOptions `json:"options,omitempty"`
	Paging
}

func (r *GetAssetsByOwnerRequest) dasMethod() string   { return "getAssetsByOwner" }
func (r *GetAssetsByOwnerRequest) paging() *Paging     { return &r.Paging }
func (r *GetAssetsByOwnerRequest) pagesOnly() bool     { return false }
func (r *GetAssetsByOwnerRequest) sortedByID() bool    { return sortedByID(r.SortBy) }
func (r *GetAssetsByOwnerRequest) clone() pagedRequest { cp := *r; return &cp }
func (r *GetAssetsByOwnerRequest) assetList()          {}
func (r *GetAssetsByOwnerRequest) payload() any {
	cp := *r
	if cp.Options.empty() {
		cp.Options = nil
	}
	return cp
}

type GetAssetsByGroupRequest struct {
	// GroupKey is the group type; use "collection".
	GroupKey   string          `json:"groupKey"`
	GroupValue string          `json:"groupValue"`
	SortBy     *SortBy         `json:"sortBy,omitempty"`
	Options    *DisplayOptions `json:"options,omitempty"`
	Paging
}

// AssetsInCollection is a GetAssetsByGroupRequest for the assets in a collection.
func AssetsInCollection(collection string) *GetAssetsByGroupRequest {
	return &GetAssetsByGroupRequest{GroupKey: "collection", GroupValue: collection}
}

func (r *GetAssetsByGroupRequest) dasMethod() string   { return "getAssetsByGroup" }
func (r *GetAssetsByGroupRequest) paging() *Paging     { return &r.Paging }
func (r *GetAssetsByGroupRequest) pagesOnly() bool     { return false }
func (r *GetAssetsByGroupRequest) sortedByID() bool    { return sortedByID(r.SortBy) }
func (r *GetAssetsByGroupRequest) clone() pagedRequest { cp := *r; return &cp }
func (r *GetAssetsByGroupRequest) assetList()          {}
func (r *GetAssetsByGroupRequest) payload() any {
	cp := *r
	if cp.Options.empty() {
		cp.Options = nil
	}
	return cp
}

type GetAssetsByCreatorRequest struct {
	CreatorAddress string          `json:"creatorAddress"`
	OnlyVerified   bool            `json:"onlyVerified,omitempty"`
	SortBy         *SortBy         `json:"sortBy,omitempty"`
	Options        *DisplayOptions `json:"options,omitempty"`
	Paging
}

func (r *GetAssetsByCreatorRequest) dasMethod() string   { return "getAssetsByCreator" }
func (r *GetAssetsByCreatorRequest) paging() *Paging     { return &r.Paging }
func (r *GetAssetsByCreatorRequest) pagesOnly() bool     { return false }
func (r *GetAssetsByCreatorRequest) sortedByID() bool    { return sortedByID(r.SortBy) }
func (r *GetAssetsByCreatorRequest) clone() pagedRequest { cp := *r; return &cp }
func (r *GetAssetsByCreatorRequest) assetList()          {}
func (r *GetAssetsByCreatorRequest) payload() any {
	cp := *r
	if cp.Options.empty() {
		cp.Options = nil
	}
	return cp
}

type GetAssetsByAuthorityRequest struct {
	AuthorityAddress string          `json:"authorityAddress"`
	SortBy           *SortBy         `json:"sortBy,omitempty"`
	Options          *DisplayOptions `json:"options,omitempty"`
	Paging
}

func (r *GetAssetsByAuthorityRequest) dasMethod() string   { return "getAssetsByAuthority" }
func (r *GetAssetsByAuthorityRequest) paging() *Paging     { return &r.Paging }
func (r *GetAssetsByAuthorityRequest) pagesOnly() bool     { return false }
func (r *GetAssetsByAuthorityRequest) sortedByID() bool    { return sortedByID(r.SortBy) }
func (r *GetAssetsByAuthorityRequest) clone() pagedRequest { cp := *r; return &cp }
func (r *GetAssetsByAuthorityRequest) assetList()          {}
func (r *GetAssetsByAuthorityRequest) payload() any {
	cp := *r
	if cp.Options.empty() {
		cp.Options = nil
	}
	return cp
}

// SearchAssetsRequest combines filters in one query. Every filter must match unless
// ConditionType is ConditionAny. Unset fields are left out; use Ptr for the optional ones.
type SearchAssetsRequest struct {
	OwnerAddress     string `json:"ownerAddress,omitempty"`
	CreatorAddress   string `json:"creatorAddress,omitempty"`
	CreatorVerified  *bool  `json:"creatorVerified,omitempty"`
	AuthorityAddress string `json:"authorityAddress,omitempty"`
	// Grouping is a group key and value, such as {"collection", <address>}.
	Grouping          *[2]string      `json:"grouping,omitempty"`
	Delegate          string          `json:"delegate,omitempty"`
	Interface         Interface       `json:"interface,omitempty"`
	TokenType         TokenType       `json:"tokenType,omitempty"`
	OwnerType         OwnershipModel  `json:"ownerType,omitempty"`
	Compressed        *bool           `json:"compressed,omitempty"`
	Compressible      *bool           `json:"compressible,omitempty"`
	Frozen            *bool           `json:"frozen,omitempty"`
	Burnt             *bool           `json:"burnt,omitempty"`
	Supply            *uint64         `json:"supply,omitempty"`
	SupplyMint        string          `json:"supplyMint,omitempty"`
	RoyaltyTargetType RoyaltyModel    `json:"royaltyTargetType,omitempty"`
	RoyaltyTarget     string          `json:"royaltyTarget,omitempty"`
	RoyaltyAmount     *uint32         `json:"royaltyAmount,omitempty"`
	JSONURI           string          `json:"jsonUri,omitempty"`
	Name              string          `json:"name,omitempty"` // needs OwnerAddress
	ConditionType     ConditionType   `json:"conditionType,omitempty"`
	Negate            *bool           `json:"negate,omitempty"`
	SortBy            *SortBy         `json:"sortBy,omitempty"`
	Options           *DisplayOptions `json:"options,omitempty"`
	Paging
}

func (r *SearchAssetsRequest) dasMethod() string   { return "searchAssets" }
func (r *SearchAssetsRequest) paging() *Paging     { return &r.Paging }
func (r *SearchAssetsRequest) pagesOnly() bool     { return false }
func (r *SearchAssetsRequest) sortedByID() bool    { return sortedByID(r.SortBy) }
func (r *SearchAssetsRequest) clone() pagedRequest { cp := *r; return &cp }
func (r *SearchAssetsRequest) assetList()          {}
func (r *SearchAssetsRequest) payload() any {
	cp := *r
	if cp.Options.empty() {
		cp.Options = nil
	}
	return cp
}

// GetTokenAccountsRequest takes an Owner, a Mint or both.
type GetTokenAccountsRequest struct {
	Owner   string          `json:"owner,omitempty"`
	Mint    string          `json:"mint,omitempty"`
	Options *DisplayOptions `json:"options,omitempty"` // only ShowZeroBalance applies
	Paging
}

func (r *GetTokenAccountsRequest) dasMethod() string   { return "getTokenAccounts" }
func (r *GetTokenAccountsRequest) paging() *Paging     { return &r.Paging }
func (r *GetTokenAccountsRequest) pagesOnly() bool     { return false }
func (r *GetTokenAccountsRequest) sortedByID() bool    { return true }
func (r *GetTokenAccountsRequest) clone() pagedRequest { cp := *r; return &cp }
func (r *GetTokenAccountsRequest) payload() any {
	cp := *r
	if cp.Options.empty() {
		cp.Options = nil
	}
	return cp
}

func (r *GetTokenAccountsRequest) validate() error {
	if r.Owner == "" && r.Mint == "" {
		return &orbitflare.ConfigError{Message: "getTokenAccounts needs an owner, a mint or both"}
	}
	return nil
}

type GetNftEditionsRequest struct {
	// Mint is the master edition's mint.
	Mint string `json:"mint"`
	Paging
}

func (r *GetNftEditionsRequest) dasMethod() string   { return "getNftEditions" }
func (r *GetNftEditionsRequest) paging() *Paging     { return &r.Paging }
func (r *GetNftEditionsRequest) pagesOnly() bool     { return false }
func (r *GetNftEditionsRequest) sortedByID() bool    { return true }
func (r *GetNftEditionsRequest) clone() pagedRequest { cp := *r; return &cp }
func (r *GetNftEditionsRequest) payload() any        { return *r }

// GetSignaturesForAssetRequest identifies the asset by ID, or by Tree and LeafIndex together.
// It pages by number only; Cursor, Before and After don't apply.
type GetSignaturesForAssetRequest struct {
	ID            string        `json:"id,omitempty"`
	Tree          string        `json:"tree,omitempty"`
	LeafIndex     *uint64       `json:"leafIndex,omitempty"`
	SortDirection SortDirection `json:"sortDirection,omitempty"`
	Paging
}

func (r *GetSignaturesForAssetRequest) dasMethod() string   { return "getSignaturesForAsset" }
func (r *GetSignaturesForAssetRequest) paging() *Paging     { return &r.Paging }
func (r *GetSignaturesForAssetRequest) pagesOnly() bool     { return true }
func (r *GetSignaturesForAssetRequest) sortedByID() bool    { return true }
func (r *GetSignaturesForAssetRequest) clone() pagedRequest { cp := *r; return &cp }
func (r *GetSignaturesForAssetRequest) payload() any        { return *r }

func (r *GetSignaturesForAssetRequest) validate() error {
	if r.ID == "" && (r.Tree == "" || r.LeafIndex == nil) {
		return &orbitflare.ConfigError{Message: "getSignaturesForAsset needs an id, or a tree and leaf index"}
	}
	return nil
}

/* ---------- Paging ---------- */

type pageResult[I any] interface {
	itemCount() int
	nextCursor() string
	pageItems() []I
}

// Pager walks the pages of a list request. Call Next until ok is false, or All for every item.
type Pager[P pageResult[I], I any] struct {
	c    *Client
	req  pagedRequest
	done bool
}

func newPager[P pageResult[I], I any](c *Client, req pagedRequest) *Pager[P, I] {
	r := req.clone()
	if p := r.paging(); (r.pagesOnly() || !r.sortedByID()) && p.Page == 0 {
		p.Page = 1
	}
	return &Pager[P, I]{c: c, req: r}
}

// Next fetches the next page. ok is false once there are no more pages.
func (p *Pager[P, I]) Next(ctx context.Context) (page P, ok bool, err error) {
	if p.done {
		return page, false, nil
	}
	page, err = dasCall[P](ctx, p.c, p.req.dasMethod(), p.req.payload())
	if err != nil {
		p.done = true
		return page, false, err
	}
	paging := p.req.paging()
	limit := paging.Limit
	if limit == 0 {
		limit = DASMaxLimit
	}
	n := page.itemCount()
	switch {
	case n == 0 || n < int(limit):
		p.done = true
	case paging.Page > 0:
		paging.Page++
	case paging.Before != "" || paging.After != "":
		// A range is one slice, not a walk.
		p.done = true
	case page.nextCursor() != "":
		paging.Cursor = page.nextCursor()
	default:
		p.done = true
	}
	return page, true, nil
}

// All returns every item on every remaining page.
func (p *Pager[P, I]) All(ctx context.Context) ([]I, error) {
	var items []I
	for {
		page, ok, err := p.Next(ctx)
		if err != nil {
			return nil, err
		}
		if !ok {
			return items, nil
		}
		items = append(items, page.pageItems()...)
	}
}

/* ---------- Responses ---------- */

// Interface is the asset's standard. Values this SDK doesn't list still come through.
type Interface string

const (
	InterfaceV1NFT             Interface = "V1_NFT"
	InterfaceV1Print           Interface = "V1_PRINT"
	InterfaceLegacyNFT         Interface = "LEGACY_NFT"
	InterfaceV2NFT             Interface = "V2_NFT"
	InterfaceProgrammableNFT   Interface = "ProgrammableNFT"
	InterfaceFungibleAsset     Interface = "FungibleAsset"
	InterfaceFungibleToken     Interface = "FungibleToken"
	InterfaceMplCoreAsset      Interface = "MplCoreAsset"
	InterfaceMplCoreCollection Interface = "MplCoreCollection"
	InterfaceMplCoreGroup      Interface = "MplCoreGroup"
	InterfaceMplBubblegumV2    Interface = "MplBubblegumV2"
	InterfaceIdentity          Interface = "Identity"
	InterfaceExecutable        Interface = "Executable"
	InterfaceCustom            Interface = "Custom"
)

type OwnershipModel string

const (
	// OwnershipSingle is for NFTs.
	OwnershipSingle OwnershipModel = "single"
	// OwnershipToken is for fungible tokens.
	OwnershipToken OwnershipModel = "token"
)

type RoyaltyModel string

const (
	RoyaltyCreators RoyaltyModel = "creators"
	RoyaltyFanout   RoyaltyModel = "fanout"
	RoyaltySingle   RoyaltyModel = "single"
)

type Scope string

const (
	ScopeFull      Scope = "full"
	ScopeRoyalty   Scope = "royalty"
	ScopeMetadata  Scope = "metadata"
	ScopeExtension Scope = "extension"
)

type Asset struct {
	Interface       Interface    `json:"interface"`
	ID              string       `json:"id"`
	LastIndexedSlot *uint64      `json:"last_indexed_slot,omitempty"`
	Content         *Content     `json:"content,omitempty"`
	Authorities     []Authority  `json:"authorities,omitempty"`
	Compression     *Compression `json:"compression,omitempty"`
	Grouping        []Group      `json:"grouping,omitempty"`
	Royalty         *Royalty     `json:"royalty,omitempty"`
	Creators        []Creator    `json:"creators,omitempty"`
	Ownership       *Ownership   `json:"ownership,omitempty"`
	Supply          *Supply      `json:"supply,omitempty"`
	Mutable         bool         `json:"mutable"`
	Burnt           bool         `json:"burnt"`
	TokenInfo       *TokenInfo   `json:"token_info,omitempty"`
	// MintExtensions holds the Token-2022 mint extensions.
	MintExtensions json.RawMessage `json:"mint_extensions,omitempty"`
	Inscription    json.RawMessage `json:"inscription,omitempty"`
	// Plugins holds the MPL Core plugins.
	Plugins                json.RawMessage `json:"plugins,omitempty"`
	UnknownPlugins         json.RawMessage `json:"unknown_plugins,omitempty"`
	ExternalPlugins        json.RawMessage `json:"external_plugins,omitempty"`
	UnknownExternalPlugins json.RawMessage `json:"unknown_external_plugins,omitempty"`
	MplCoreInfo            *MplCoreInfo    `json:"mpl_core_info,omitempty"`
	Uses                   *Uses           `json:"uses,omitempty"`
}

// Name is the asset's name, or "" if it has none.
func (a *Asset) Name() string {
	if a.Content == nil {
		return ""
	}
	return a.Content.Metadata.Name
}

func (a *Asset) Symbol() string {
	if a.Content == nil {
		return ""
	}
	return a.Content.Metadata.Symbol
}

func (a *Asset) Image() string {
	if a.Content == nil {
		return ""
	}
	return a.Content.Links.Image
}

// Owner is the asset's owner, or "" for fungible tokens, which have many holders.
func (a *Asset) Owner() string {
	if a.Ownership == nil {
		return ""
	}
	return a.Ownership.Owner
}

// Collection is the collection address, or "" if the asset isn't in one.
func (a *Asset) Collection() string {
	for _, g := range a.Grouping {
		if g.GroupKey == "collection" {
			return g.GroupValue
		}
	}
	return ""
}

func (a *Asset) IsCompressed() bool { return a.Compression != nil && a.Compression.Compressed }

type Content struct {
	Schema   string      `json:"$schema"`
	JSONURI  string      `json:"json_uri"`
	Files    []AssetFile `json:"files,omitempty"`
	Metadata Metadata    `json:"metadata"`
	Links    Links       `json:"links"`
	Category string      `json:"category,omitempty"`
}

type AssetFile struct {
	URI      string          `json:"uri,omitempty"`
	CDNURI   string          `json:"cdn_uri,omitempty"`
	Mime     string          `json:"mime,omitempty"`
	Quality  json.RawMessage `json:"quality,omitempty"`
	Contexts []string        `json:"contexts,omitempty"`
}

type Metadata struct {
	Name          string      `json:"name"`
	Symbol        string      `json:"symbol"`
	Description   string      `json:"description,omitempty"`
	Attributes    []Attribute `json:"attributes,omitempty"`
	TokenStandard string      `json:"token_standard,omitempty"`
	// JSONName is the name in the off-chain JSON.
	JSONName string `json:"json_name,omitempty"`
}

// Attribute is a metadata trait. Metadata is written by hand, so Value can be any JSON value
// and a numeric TraitType is read as text.
type Attribute struct {
	TraitType LooseString `json:"trait_type"`
	Value     any         `json:"value"`
}

// LooseString reads a JSON string, number or bool as text, and null as "".
type LooseString string

func (s *LooseString) UnmarshalJSON(b []byte) error {
	var str string
	if err := json.Unmarshal(b, &str); err == nil {
		*s = LooseString(str)
		return nil
	}
	if string(b) == "null" {
		*s = ""
		return nil
	}
	*s = LooseString(strings.TrimSpace(string(b)))
	return nil
}

// Links holds the convenience links; Other has any the SDK doesn't name.
type Links struct {
	Image        string
	AnimationURL string
	ExternalURL  string
	Other        map[string]any
}

func (l *Links) UnmarshalJSON(b []byte) error {
	var all map[string]any
	if err := json.Unmarshal(b, &all); err != nil {
		return err
	}
	take := func(key string) string {
		v, _ := all[key].(string)
		delete(all, key)
		return v
	}
	*l = Links{Image: take("image"), AnimationURL: take("animation_url"), ExternalURL: take("external_url")}
	if len(all) > 0 {
		l.Other = all
	}
	return nil
}

func (l Links) MarshalJSON() ([]byte, error) {
	out := map[string]any{}
	for k, v := range l.Other {
		out[k] = v
	}
	for k, v := range map[string]string{"image": l.Image, "animation_url": l.AnimationURL, "external_url": l.ExternalURL} {
		if v != "" {
			out[k] = v
		}
	}
	return json.Marshal(out)
}

type Authority struct {
	Address string  `json:"address"`
	Scopes  []Scope `json:"scopes"`
}

type Compression struct {
	Eligible    bool   `json:"eligible"`
	Compressed  bool   `json:"compressed"`
	DataHash    string `json:"data_hash"`
	CreatorHash string `json:"creator_hash"`
	AssetHash   string `json:"asset_hash"`
	Tree        string `json:"tree"`
	Seq         uint64 `json:"seq"`
	LeafID      uint64 `json:"leaf_id"`
	// Bubblegum v2 only.
	CollectionHash string `json:"collection_hash,omitempty"`
	AssetDataHash  string `json:"asset_data_hash,omitempty"`
	Flags          *uint8 `json:"flags,omitempty"`
}

type Group struct {
	GroupKey   string `json:"group_key"`
	GroupValue string `json:"group_value"`
	// Verified is false on unverified collections, which appear only with ShowUnverifiedCollections.
	Verified *bool `json:"verified,omitempty"`
	// CollectionMetadata is set with ShowCollectionMetadata.
	CollectionMetadata json.RawMessage `json:"collection_metadata,omitempty"`
}

type Royalty struct {
	RoyaltyModel        RoyaltyModel `json:"royalty_model"`
	Target              *string      `json:"target"`
	Percent             float64      `json:"percent"`
	BasisPoints         uint32       `json:"basis_points"`
	PrimarySaleHappened bool         `json:"primary_sale_happened"`
	Locked              bool         `json:"locked"`
}

type Creator struct {
	Address string `json:"address"`
	// Share is the percent of royalties.
	Share    uint8 `json:"share"`
	Verified bool  `json:"verified"`
}

type Ownership struct {
	// Owner is empty for fungible tokens.
	Owner           string         `json:"owner"`
	OwnershipModel  OwnershipModel `json:"ownership_model"`
	Frozen          bool           `json:"frozen"`
	Delegated       bool           `json:"delegated"`
	Delegate        *string        `json:"delegate"`
	NonTransferable *bool          `json:"non_transferable,omitempty"`
}

type Supply struct {
	PrintMaxSupply     *uint64 `json:"print_max_supply"`
	PrintCurrentSupply *uint64 `json:"print_current_supply"`
	EditionNonce       *uint8  `json:"edition_nonce"`
	// Prints only.
	EditionNumber     *uint64 `json:"edition_number,omitempty"`
	MasterEditionMint string  `json:"master_edition_mint,omitempty"`
}

type TokenInfo struct {
	Symbol string `json:"symbol,omitempty"`
	// Balance is the queried owner's balance, in base units.
	Balance *uint64 `json:"balance,omitempty"`
	// Supply is the total supply, in base units.
	Supply                 *uint64    `json:"supply,omitempty"`
	Decimals               *uint8     `json:"decimals,omitempty"`
	TokenProgram           string     `json:"token_program,omitempty"`
	AssociatedTokenAddress string     `json:"associated_token_address,omitempty"`
	MintAuthority          string     `json:"mint_authority,omitempty"`
	FreezeAuthority        string     `json:"freeze_authority,omitempty"`
	PriceInfo              *PriceInfo `json:"price_info,omitempty"`
}

type PriceInfo struct {
	PricePerToken float64 `json:"price_per_token"`
	// TotalPrice is the value of the owner's balance.
	TotalPrice *float64 `json:"total_price,omitempty"`
	Currency   string   `json:"currency"`
}

type MplCoreInfo struct {
	NumMinted          *uint64 `json:"num_minted,omitempty"`
	CurrentSize        *uint64 `json:"current_size,omitempty"`
	PluginsJSONVersion *uint32 `json:"plugins_json_version,omitempty"`
}

type Uses struct {
	UseMethod string `json:"use_method"`
	Remaining uint64 `json:"remaining"`
	Total     uint64 `json:"total"`
}

// NativeBalance is the owner's SOL balance, with ShowNativeBalance.
type NativeBalance struct {
	Lamports    uint64   `json:"lamports"`
	PricePerSol *float64 `json:"price_per_sol,omitempty"`
	// TotalPrice is in USD.
	TotalPrice *float64 `json:"total_price,omitempty"`
}

type AssetProof struct {
	Root string `json:"root"`
	// Proof holds the sibling hashes from the leaf up to the root.
	Proof           []string `json:"proof"`
	NodeIndex       uint64   `json:"node_index"`
	Leaf            string   `json:"leaf"`
	TreeID          string   `json:"tree_id"`
	LastIndexedSlot *uint64  `json:"last_indexed_slot,omitempty"`
}

// ProofPath returns the proof nodes a transaction must pass, leaving out the canopyDepth top
// levels the tree already stores on chain.
func (p *AssetProof) ProofPath(canopyDepth int) []string {
	return p.Proof[:max(0, len(p.Proof)-canopyDepth)]
}

type AssetWithProof struct {
	Asset Asset      `json:"asset"`
	Proof AssetProof `json:"proof"`
}

type AssetList struct {
	LastIndexedSlot *uint64 `json:"last_indexed_slot,omitempty"`
	// Total is the number of items in this page.
	Total         uint32         `json:"total"`
	Limit         uint32         `json:"limit"`
	Page          *uint32        `json:"page,omitempty"`
	Cursor        string         `json:"cursor,omitempty"`
	Before        string         `json:"before,omitempty"`
	After         string         `json:"after,omitempty"`
	Items         []Asset        `json:"items"`
	NativeBalance *NativeBalance `json:"nativeBalance,omitempty"`
}

func (l AssetList) itemCount() int     { return len(l.Items) }
func (l AssetList) nextCursor() string { return l.Cursor }
func (l AssetList) pageItems() []Asset { return l.Items }

type TokenAccount struct {
	Address string `json:"address"`
	Mint    string `json:"mint"`
	Owner   string `json:"owner"`
	// Amount is in base units.
	Amount          uint64  `json:"amount"`
	Delegate        *string `json:"delegate"`
	DelegatedAmount uint64  `json:"delegated_amount"`
	CloseAuthority  *string `json:"close_authority"`
	Frozen          bool    `json:"frozen"`
	// TokenExtensions holds the Token-2022 account extensions.
	TokenExtensions json.RawMessage `json:"token_extensions,omitempty"`
}

type TokenAccountList struct {
	LastIndexedSlot *uint64        `json:"last_indexed_slot,omitempty"`
	Total           uint32         `json:"total"`
	Limit           uint32         `json:"limit"`
	Page            *uint32        `json:"page,omitempty"`
	Cursor          string         `json:"cursor,omitempty"`
	TokenAccounts   []TokenAccount `json:"token_accounts"`
}

func (l TokenAccountList) itemCount() int            { return len(l.TokenAccounts) }
func (l TokenAccountList) nextCursor() string        { return l.Cursor }
func (l TokenAccountList) pageItems() []TokenAccount { return l.TokenAccounts }

type NftEdition struct {
	Mint           string `json:"mint"`
	EditionAddress string `json:"edition_address"`
	Edition        uint64 `json:"edition"`
}

type NftEditions struct {
	MasterEditionAddress string `json:"master_edition_address"`
	// Supply is the number of editions printed so far.
	Supply uint64 `json:"supply"`
	// MaxSupply is nil when unlimited.
	MaxSupply *uint64      `json:"max_supply"`
	Total     uint32       `json:"total"`
	Limit     uint32       `json:"limit"`
	Page      *uint32      `json:"page,omitempty"`
	Cursor    string       `json:"cursor,omitempty"`
	Editions  []NftEdition `json:"editions"`
}

func (e NftEditions) itemCount() int          { return len(e.Editions) }
func (e NftEditions) nextCursor() string      { return e.Cursor }
func (e NftEditions) pageItems() []NftEdition { return e.Editions }

// AssetSignature is a transaction that changed a compressed asset, sent as a
// [signature, instruction] pair.
type AssetSignature struct {
	Signature string
	// Instruction is the Bubblegum instruction, such as MintToCollectionV1, Transfer or Burn.
	Instruction string
}

func (s *AssetSignature) UnmarshalJSON(b []byte) error {
	var pair [2]string
	if err := json.Unmarshal(b, &pair); err != nil {
		return err
	}
	s.Signature, s.Instruction = pair[0], pair[1]
	return nil
}

func (s AssetSignature) MarshalJSON() ([]byte, error) {
	return json.Marshal([2]string{s.Signature, s.Instruction})
}

type AssetSignatures struct {
	LastIndexedSlot *uint64          `json:"last_indexed_slot,omitempty"`
	Total           uint32           `json:"total"`
	Limit           uint32           `json:"limit"`
	Page            *uint32          `json:"page,omitempty"`
	Items           []AssetSignature `json:"items"`
}

func (s AssetSignatures) itemCount() int              { return len(s.Items) }
func (s AssetSignatures) nextCursor() string          { return "" }
func (s AssetSignatures) pageItems() []AssetSignature { return s.Items }
