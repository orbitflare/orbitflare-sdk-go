package geyser

import pb "github.com/orbitflare/orbitflare-sdk-go/proto/geyser"

type Commitment int32

const (
	Processed Commitment = 0
	Confirmed Commitment = 1
	Finalized Commitment = 2
)

type Memcmp struct {
	offset uint64
	data   memcmpData
}

type memcmpKind int

const (
	memcmpBytes memcmpKind = iota
	memcmpBase58
	memcmpBase64
)

type memcmpData struct {
	kind  memcmpKind
	bytes []byte
	str   string
}

func MemcmpBytes(offset uint64, data []byte) Memcmp {
	return Memcmp{offset: offset, data: memcmpData{kind: memcmpBytes, bytes: data}}
}

func MemcmpBase58(offset uint64, data string) Memcmp {
	return Memcmp{offset: offset, data: memcmpData{kind: memcmpBase58, str: data}}
}

func MemcmpBase64(offset uint64, data string) Memcmp {
	return Memcmp{offset: offset, data: memcmpData{kind: memcmpBase64, str: data}}
}

func (m Memcmp) toProto() *pb.SubscribeRequestFilterAccountsFilterMemcmp {
	out := &pb.SubscribeRequestFilterAccountsFilterMemcmp{Offset: m.offset}
	switch m.data.kind {
	case memcmpBase58:
		out.Data = &pb.SubscribeRequestFilterAccountsFilterMemcmp_Base58{Base58: m.data.str}
	case memcmpBase64:
		out.Data = &pb.SubscribeRequestFilterAccountsFilterMemcmp_Base64{Base64: m.data.str}
	default:
		out.Data = &pb.SubscribeRequestFilterAccountsFilterMemcmp_Bytes{Bytes: m.data.bytes}
	}
	return out
}

type Lamports struct {
	kind  lamportsKind
	value uint64
}

type lamportsKind int

const (
	lamportsEq lamportsKind = iota
	lamportsNe
	lamportsLt
	lamportsGt
)

func LamportsEq(v uint64) Lamports { return Lamports{kind: lamportsEq, value: v} }

func LamportsNe(v uint64) Lamports { return Lamports{kind: lamportsNe, value: v} }

func LamportsLt(v uint64) Lamports { return Lamports{kind: lamportsLt, value: v} }

func LamportsGt(v uint64) Lamports { return Lamports{kind: lamportsGt, value: v} }

func (l Lamports) toProto() *pb.SubscribeRequestFilterAccountsFilterLamports {
	out := &pb.SubscribeRequestFilterAccountsFilterLamports{}
	switch l.kind {
	case lamportsNe:
		out.Cmp = &pb.SubscribeRequestFilterAccountsFilterLamports_Ne{Ne: l.value}
	case lamportsLt:
		out.Cmp = &pb.SubscribeRequestFilterAccountsFilterLamports_Lt{Lt: l.value}
	case lamportsGt:
		out.Cmp = &pb.SubscribeRequestFilterAccountsFilterLamports_Gt{Gt: l.value}
	default:
		out.Cmp = &pb.SubscribeRequestFilterAccountsFilterLamports_Eq{Eq: l.value}
	}
	return out
}

type TransactionFilter struct {
	vote            *bool
	failed          *bool
	signature       *string
	accountInclude  []string
	accountExclude  []string
	accountRequired []string
}

func NewTransactionFilter() *TransactionFilter { return &TransactionFilter{} }

func (f *TransactionFilter) Vote(v bool) *TransactionFilter        { f.vote = &v; return f }
func (f *TransactionFilter) Failed(v bool) *TransactionFilter      { f.failed = &v; return f }
func (f *TransactionFilter) Signature(s string) *TransactionFilter { f.signature = &s; return f }

func (f *TransactionFilter) AccountInclude(accounts ...string) *TransactionFilter {
	f.accountInclude = append(f.accountInclude, accounts...)
	return f
}

func (f *TransactionFilter) AccountExclude(accounts ...string) *TransactionFilter {
	f.accountExclude = append(f.accountExclude, accounts...)
	return f
}

func (f *TransactionFilter) AccountRequired(accounts ...string) *TransactionFilter {
	f.accountRequired = append(f.accountRequired, accounts...)
	return f
}

func (f *TransactionFilter) toProto() *pb.SubscribeRequestFilterTransactions {
	return &pb.SubscribeRequestFilterTransactions{
		Vote:            f.vote,
		Failed:          f.failed,
		Signature:       f.signature,
		AccountInclude:  f.accountInclude,
		AccountExclude:  f.accountExclude,
		AccountRequired: f.accountRequired,
	}
}

type accountConditionKind int

const (
	condMemcmp accountConditionKind = iota
	condDatasize
	condTokenAccountState
	condLamports
)

type accountCondition struct {
	kind     accountConditionKind
	memcmp   Memcmp
	datasize uint64
	boolVal  bool
	lamports Lamports
}

type AccountFilter struct {
	account              []string
	owner                []string
	conditions           []accountCondition
	nonemptyTxnSignature *bool
}

func NewAccountFilter() *AccountFilter { return &AccountFilter{} }

func (f *AccountFilter) Account(accounts ...string) *AccountFilter {
	f.account = append(f.account, accounts...)
	return f
}

func (f *AccountFilter) Owner(owners ...string) *AccountFilter {
	f.owner = append(f.owner, owners...)
	return f
}

func (f *AccountFilter) Memcmp(m Memcmp) *AccountFilter {
	f.conditions = append(f.conditions, accountCondition{kind: condMemcmp, memcmp: m})
	return f
}

func (f *AccountFilter) Datasize(size uint64) *AccountFilter {
	f.conditions = append(f.conditions, accountCondition{kind: condDatasize, datasize: size})
	return f
}

func (f *AccountFilter) TokenAccountState(state bool) *AccountFilter {
	f.conditions = append(f.conditions, accountCondition{kind: condTokenAccountState, boolVal: state})
	return f
}

func (f *AccountFilter) Lamports(l Lamports) *AccountFilter {
	f.conditions = append(f.conditions, accountCondition{kind: condLamports, lamports: l})
	return f
}

func (f *AccountFilter) NonemptyTxnSignature(v bool) *AccountFilter {
	f.nonemptyTxnSignature = &v
	return f
}

func (f *AccountFilter) toProto() *pb.SubscribeRequestFilterAccounts {
	filters := make([]*pb.SubscribeRequestFilterAccountsFilter, 0, len(f.conditions))
	for _, c := range f.conditions {
		out := &pb.SubscribeRequestFilterAccountsFilter{}
		switch c.kind {
		case condMemcmp:
			out.Filter = &pb.SubscribeRequestFilterAccountsFilter_Memcmp{Memcmp: c.memcmp.toProto()}
		case condDatasize:
			out.Filter = &pb.SubscribeRequestFilterAccountsFilter_Datasize{Datasize: c.datasize}
		case condTokenAccountState:
			out.Filter = &pb.SubscribeRequestFilterAccountsFilter_TokenAccountState{TokenAccountState: c.boolVal}
		case condLamports:
			out.Filter = &pb.SubscribeRequestFilterAccountsFilter_Lamports{Lamports: c.lamports.toProto()}
		}
		filters = append(filters, out)
	}
	return &pb.SubscribeRequestFilterAccounts{
		Account:              f.account,
		Owner:                f.owner,
		Filters:              filters,
		NonemptyTxnSignature: f.nonemptyTxnSignature,
	}
}

type SlotFilter struct {
	filterByCommitment *bool
	interslotUpdates   *bool
}

func NewSlotFilter() *SlotFilter { return &SlotFilter{} }

func (f *SlotFilter) FilterByCommitment(v bool) *SlotFilter { f.filterByCommitment = &v; return f }
func (f *SlotFilter) InterslotUpdates(v bool) *SlotFilter   { f.interslotUpdates = &v; return f }

func (f *SlotFilter) toProto() *pb.SubscribeRequestFilterSlots {
	return &pb.SubscribeRequestFilterSlots{
		FilterByCommitment: f.filterByCommitment,
		InterslotUpdates:   f.interslotUpdates,
	}
}

type BlockFilter struct {
	accountInclude      []string
	includeTransactions *bool
	includeAccounts     *bool
	includeEntries      *bool
}

func NewBlockFilter() *BlockFilter { return &BlockFilter{} }

func (f *BlockFilter) AccountInclude(accounts ...string) *BlockFilter {
	f.accountInclude = append(f.accountInclude, accounts...)
	return f
}

func (f *BlockFilter) IncludeTransactions(v bool) *BlockFilter { f.includeTransactions = &v; return f }
func (f *BlockFilter) IncludeAccounts(v bool) *BlockFilter     { f.includeAccounts = &v; return f }
func (f *BlockFilter) IncludeEntries(v bool) *BlockFilter      { f.includeEntries = &v; return f }

func (f *BlockFilter) toProto() *pb.SubscribeRequestFilterBlocks {
	return &pb.SubscribeRequestFilterBlocks{
		AccountInclude:      f.accountInclude,
		IncludeTransactions: f.includeTransactions,
		IncludeAccounts:     f.includeAccounts,
		IncludeEntries:      f.includeEntries,
	}
}

type DeshredTransactionFilter struct {
	vote            *bool
	accountInclude  []string
	accountExclude  []string
	accountRequired []string
}

func NewDeshredTransactionFilter() *DeshredTransactionFilter { return &DeshredTransactionFilter{} }

func (f *DeshredTransactionFilter) Vote(v bool) *DeshredTransactionFilter { f.vote = &v; return f }

func (f *DeshredTransactionFilter) AccountInclude(accounts ...string) *DeshredTransactionFilter {
	f.accountInclude = append(f.accountInclude, accounts...)
	return f
}

func (f *DeshredTransactionFilter) AccountExclude(accounts ...string) *DeshredTransactionFilter {
	f.accountExclude = append(f.accountExclude, accounts...)
	return f
}

func (f *DeshredTransactionFilter) AccountRequired(accounts ...string) *DeshredTransactionFilter {
	f.accountRequired = append(f.accountRequired, accounts...)
	return f
}

func (f *DeshredTransactionFilter) toProto() *pb.SubscribeRequestFilterDeshredTransactions {
	return &pb.SubscribeRequestFilterDeshredTransactions{
		Vote:            f.vote,
		AccountInclude:  f.accountInclude,
		AccountExclude:  f.accountExclude,
		AccountRequired: f.accountRequired,
	}
}

type SubscribeRequestBuilder struct {
	accounts           map[string]*AccountFilter
	slots              map[string]*SlotFilter
	transactions       map[string]*TransactionFilter
	transactionsStatus map[string]*TransactionFilter
	blocks             map[string]*BlockFilter
	blocksMeta         []string
	entry              []string
	commitment         *Commitment
	accountsDataSlice  [][2]uint64
	fromSlot           *uint64
}

func NewSubscribeRequestBuilder() *SubscribeRequestBuilder {
	return &SubscribeRequestBuilder{
		accounts:           map[string]*AccountFilter{},
		slots:              map[string]*SlotFilter{},
		transactions:       map[string]*TransactionFilter{},
		transactionsStatus: map[string]*TransactionFilter{},
		blocks:             map[string]*BlockFilter{},
	}
}

func (b *SubscribeRequestBuilder) Accounts(name string, filter *AccountFilter) *SubscribeRequestBuilder {
	b.accounts[name] = filter
	return b
}

func (b *SubscribeRequestBuilder) Slots(name string, filter *SlotFilter) *SubscribeRequestBuilder {
	b.slots[name] = filter
	return b
}

func (b *SubscribeRequestBuilder) Transactions(name string, filter *TransactionFilter) *SubscribeRequestBuilder {
	b.transactions[name] = filter
	return b
}

func (b *SubscribeRequestBuilder) TransactionsStatus(name string, filter *TransactionFilter) *SubscribeRequestBuilder {
	b.transactionsStatus[name] = filter
	return b
}

func (b *SubscribeRequestBuilder) Blocks(name string, filter *BlockFilter) *SubscribeRequestBuilder {
	b.blocks[name] = filter
	return b
}

func (b *SubscribeRequestBuilder) BlocksMeta(name string) *SubscribeRequestBuilder {
	b.blocksMeta = append(b.blocksMeta, name)
	return b
}

func (b *SubscribeRequestBuilder) Entry(name string) *SubscribeRequestBuilder {
	b.entry = append(b.entry, name)
	return b
}

func (b *SubscribeRequestBuilder) Commitment(c Commitment) *SubscribeRequestBuilder {
	b.commitment = &c
	return b
}

func (b *SubscribeRequestBuilder) AccountsDataSlice(offset, length uint64) *SubscribeRequestBuilder {
	b.accountsDataSlice = append(b.accountsDataSlice, [2]uint64{offset, length})
	return b
}

func (b *SubscribeRequestBuilder) FromSlot(slot uint64) *SubscribeRequestBuilder {
	b.fromSlot = &slot
	return b
}

func (b *SubscribeRequestBuilder) Build() *pb.SubscribeRequest {
	accounts := make(map[string]*pb.SubscribeRequestFilterAccounts, len(b.accounts))
	for k, v := range b.accounts {
		accounts[k] = v.toProto()
	}
	slots := make(map[string]*pb.SubscribeRequestFilterSlots, len(b.slots))
	for k, v := range b.slots {
		slots[k] = v.toProto()
	}
	transactions := make(map[string]*pb.SubscribeRequestFilterTransactions, len(b.transactions))
	for k, v := range b.transactions {
		transactions[k] = v.toProto()
	}
	transactionsStatus := make(map[string]*pb.SubscribeRequestFilterTransactions, len(b.transactionsStatus))
	for k, v := range b.transactionsStatus {
		transactionsStatus[k] = v.toProto()
	}
	blocks := make(map[string]*pb.SubscribeRequestFilterBlocks, len(b.blocks))
	for k, v := range b.blocks {
		blocks[k] = v.toProto()
	}
	blocksMeta := make(map[string]*pb.SubscribeRequestFilterBlocksMeta, len(b.blocksMeta))
	for _, k := range b.blocksMeta {
		blocksMeta[k] = &pb.SubscribeRequestFilterBlocksMeta{}
	}
	entry := make(map[string]*pb.SubscribeRequestFilterEntry, len(b.entry))
	for _, k := range b.entry {
		entry[k] = &pb.SubscribeRequestFilterEntry{}
	}
	dataSlice := make([]*pb.SubscribeRequestAccountsDataSlice, 0, len(b.accountsDataSlice))
	for _, s := range b.accountsDataSlice {
		dataSlice = append(dataSlice, &pb.SubscribeRequestAccountsDataSlice{Offset: s[0], Length: s[1]})
	}

	req := &pb.SubscribeRequest{
		Accounts:           accounts,
		Slots:              slots,
		Transactions:       transactions,
		TransactionsStatus: transactionsStatus,
		Blocks:             blocks,
		BlocksMeta:         blocksMeta,
		Entry:              entry,
		AccountsDataSlice:  dataSlice,
		Ping:               &pb.SubscribeRequestPing{Id: 1},
		FromSlot:           b.fromSlot,
	}
	if b.commitment != nil {
		c := pb.CommitmentLevel(*b.commitment)
		req.Commitment = &c
	}
	return req
}
