package rpc

import "encoding/json"

type GetTransactionsResult struct {
	Data            []json.RawMessage
	PaginationToken *string
}

type GetTransactionsOptions struct {
	TransactionDetails             *string                 `json:"transactionDetails,omitempty"`
	Limit                          *uint32                 `json:"limit,omitempty"`
	SortOrder                      *string                 `json:"sortOrder,omitempty"`
	PaginationToken                *string                 `json:"paginationToken,omitempty"`
	Commitment                     *string                 `json:"commitment,omitempty"`
	Encoding                       *string                 `json:"encoding,omitempty"`
	MaxSupportedTransactionVersion *uint32                 `json:"maxSupportedTransactionVersion,omitempty"`
	MinContextSlot                 *uint64                 `json:"minContextSlot,omitempty"`
	Filters                        *GetTransactionsFilters `json:"filters,omitempty"`
}

func NewGetTransactionsOptions() *GetTransactionsOptions { return &GetTransactionsOptions{} }

func (o *GetTransactionsOptions) TransactionDetailsSet(v string) *GetTransactionsOptions {
	o.TransactionDetails = &v
	return o
}

func (o *GetTransactionsOptions) LimitSet(v uint32) *GetTransactionsOptions {
	o.Limit = &v
	return o
}

func (o *GetTransactionsOptions) SortOrderSet(v string) *GetTransactionsOptions {
	o.SortOrder = &v
	return o
}

func (o *GetTransactionsOptions) PaginationTokenSet(v string) *GetTransactionsOptions {
	o.PaginationToken = &v
	return o
}

func (o *GetTransactionsOptions) CommitmentSet(v string) *GetTransactionsOptions {
	o.Commitment = &v
	return o
}

func (o *GetTransactionsOptions) FiltersSet(f *GetTransactionsFilters) *GetTransactionsOptions {
	o.Filters = f
	return o
}

type GetTransactionsFilters struct {
	TokenAccounts *string              `json:"tokenAccounts,omitempty"`
	BlockTime     *RangeFilter[int64]  `json:"blockTime,omitempty"`
	Slot          *RangeFilter[uint64] `json:"slot,omitempty"`
	Signature     *RangeFilter[string] `json:"signature,omitempty"`
	Status        *string              `json:"status,omitempty"`
}

func NewGetTransactionsFilters() *GetTransactionsFilters { return &GetTransactionsFilters{} }

func (f *GetTransactionsFilters) TokenAccountsSet(v string) *GetTransactionsFilters {
	f.TokenAccounts = &v
	return f
}

func (f *GetTransactionsFilters) BlockTimeSet(r RangeFilter[int64]) *GetTransactionsFilters {
	f.BlockTime = &r
	return f
}

func (f *GetTransactionsFilters) SlotSet(r RangeFilter[uint64]) *GetTransactionsFilters {
	f.Slot = &r
	return f
}

func (f *GetTransactionsFilters) StatusSet(v string) *GetTransactionsFilters {
	f.Status = &v
	return f
}

type RangeFilter[T any] struct {
	Gte *T `json:"gte,omitempty"`
	Gt  *T `json:"gt,omitempty"`
	Lte *T `json:"lte,omitempty"`
	Lt  *T `json:"lt,omitempty"`
	Eq  *T `json:"eq,omitempty"`
}
