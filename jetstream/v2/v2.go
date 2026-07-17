package v2

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"time"

	"google.golang.org/grpc"

	"github.com/orbitflare/orbitflare-sdk-go/internal/streaming"
	"github.com/orbitflare/orbitflare-sdk-go/orbitflare"
	pb "github.com/orbitflare/orbitflare-sdk-go/proto/jetstreamv2"
)

type (
	TxFilter                      = pb.TxFilter
	SubscribeTransactionsResponse = pb.SubscribeTransactionsResponse
	SlotEvent                     = pb.SlotEvent
)

type TransactionFilter struct {
	accountInclude    []string
	accountExclude    []string
	accountRequired   []string
	includeEnrichment bool
}

func NewTransactionFilter() *TransactionFilter { return &TransactionFilter{} }

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

func (f *TransactionFilter) IncludeEnrichment(v bool) *TransactionFilter {
	f.includeEnrichment = v
	return f
}

func (f *TransactionFilter) toProto() *pb.SubscribeRequestFilterTransactions {
	return &pb.SubscribeRequestFilterTransactions{
		AccountInclude:    f.accountInclude,
		AccountExclude:    f.accountExclude,
		AccountRequired:   f.accountRequired,
		IncludeEnrichment: f.includeEnrichment,
	}
}

func (f *TransactionFilter) WithID(filterID string) *pb.TxFilter {
	return &pb.TxFilter{FilterId: filterID, Filter: f.toProto()}
}

type Client struct {
	cfg    *streaming.Config
	ctx    context.Context
	cancel context.CancelFunc
}

func (c *Client) Close() { c.cancel() }

type filterCmdKind int

const (
	cmdAdd filterCmdKind = iota
	cmdRemove
	cmdPing
)

type filterCommand struct {
	kind   filterCmdKind
	add    []*pb.TxFilter
	remove []string
	pingID int32
}

type TxFilterHandle struct {
	cmdCh chan filterCommand
	done  <-chan struct{}
}

func (h TxFilterHandle) AddFilters(filters []*pb.TxFilter) error {
	return h.send(filterCommand{kind: cmdAdd, add: filters})
}

func (h TxFilterHandle) RemoveFilters(filterIDs []string) error {
	return h.send(filterCommand{kind: cmdRemove, remove: filterIDs})
}

func (h TxFilterHandle) Ping(pingID int32) error {
	return h.send(filterCommand{kind: cmdPing, pingID: pingID})
}

func (h TxFilterHandle) send(cmd filterCommand) error {
	select {
	case <-h.done:
		return &orbitflare.StreamError{Message: "stream closed"}
	default:
	}
	select {
	case h.cmdCh <- cmd:
		return nil
	case <-h.done:
		return &orbitflare.StreamError{Message: "stream closed"}
	}
}

type TransactionStream struct {
	ch     <-chan streaming.Result[*pb.SubscribeTransactionsResponse]
	handle TxFilterHandle
	cancel context.CancelFunc
}

func (s *TransactionStream) Next(ctx context.Context) (*pb.SubscribeTransactionsResponse, error) {
	select {
	case r, ok := <-s.ch:
		if !ok {
			return nil, io.EOF
		}
		return r.Update, r.Err
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (s *TransactionStream) Handle() TxFilterHandle { return s.handle }

func (s *TransactionStream) AddFilters(filters []*pb.TxFilter) error {
	return s.handle.AddFilters(filters)
}

func (s *TransactionStream) RemoveFilters(filterIDs []string) error {
	return s.handle.RemoveFilters(filterIDs)
}

func (s *TransactionStream) Close() { s.cancel() }

type SlotStream struct {
	ch     <-chan streaming.Result[*pb.SlotEvent]
	cancel context.CancelFunc
}

func (s *SlotStream) Next(ctx context.Context) (*pb.SlotEvent, error) {
	select {
	case r, ok := <-s.ch:
		if !ok {
			return nil, io.EOF
		}
		return r.Update, r.Err
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (s *SlotStream) Close() { s.cancel() }

func (c *Client) SubscribeTransactions(filters ...*pb.TxFilter) *TransactionStream {
	ctx, cancel := context.WithCancel(c.ctx)
	ch := make(chan streaming.Result[*pb.SubscribeTransactionsResponse], c.cfg.ChannelCapacity)
	cmdCh := make(chan filterCommand, 256)

	active := make(map[string]*pb.TxFilter, len(filters))
	for _, f := range filters {
		active[f.GetFilterId()] = f
	}

	go streaming.RunStream(ctx, c.cfg, "v2 tx", ch,
		func(rctx context.Context, conn *grpc.ClientConn, onConnected func()) error {
			return runTxConnection(rctx, conn, active, cmdCh, onConnected, ch, c.cfg)
		})

	return &TransactionStream{
		ch:     ch,
		handle: TxFilterHandle{cmdCh: cmdCh, done: ctx.Done()},
		cancel: cancel,
	}
}

func (c *Client) SubscribeSlots() *SlotStream {
	ctx, cancel := context.WithCancel(c.ctx)
	ch := make(chan streaming.Result[*pb.SlotEvent], c.cfg.ChannelCapacity)

	go streaming.RunStream(ctx, c.cfg, "v2 slots", ch,
		func(rctx context.Context, conn *grpc.ClientConn, onConnected func()) error {
			return runSlotConnection(rctx, conn, onConnected, ch)
		})

	return &SlotStream{ch: ch, cancel: cancel}
}

func (c *Client) Ping(ctx context.Context, count int32) (int32, error) {
	return streaming.UnaryWithFailover(ctx, c.cfg, func(ctx context.Context, conn *grpc.ClientConn) (int32, error) {
		resp, err := pb.NewJetstreamV2Client(conn).Ping(ctx, &pb.PingRequest{Count: count})
		if err != nil {
			return 0, streaming.RecvError(err)
		}
		return resp.GetCount(), nil
	})
}

func (c *Client) GetVersion(ctx context.Context) (string, error) {
	return streaming.UnaryWithFailover(ctx, c.cfg, func(ctx context.Context, conn *grpc.ClientConn) (string, error) {
		resp, err := pb.NewJetstreamV2Client(conn).GetVersion(ctx, &pb.GetVersionRequest{})
		if err != nil {
			return "", streaming.RecvError(err)
		}
		return resp.GetVersion(), nil
	})
}

func addFiltersReq(filters []*pb.TxFilter) *pb.SubscribeTransactionsRequest {
	return &pb.SubscribeTransactionsRequest{
		Payload: &pb.SubscribeTransactionsRequest_AddFilters{AddFilters: &pb.AddFilters{Filters: filters}},
	}
}

func pingReq(pingID int32) *pb.SubscribeTransactionsRequest {
	return &pb.SubscribeTransactionsRequest{
		Payload: &pb.SubscribeTransactionsRequest_Ping{Ping: &pb.Ping{PingId: pingID}},
	}
}

func runTxConnection(ctx context.Context, conn *grpc.ClientConn, active map[string]*pb.TxFilter, cmdCh chan filterCommand, onConnected func(), out chan<- streaming.Result[*pb.SubscribeTransactionsResponse], cfg *streaming.Config) error {
	stream, err := pb.NewJetstreamV2Client(conn).SubscribeTransactions(ctx)
	if err != nil {
		return orbitflare.NewTransportError(err)
	}

	if len(active) > 0 {
		filters := make([]*pb.TxFilter, 0, len(active))
		for _, f := range active {
			filters = append(filters, f)
		}
		if err := stream.Send(addFiltersReq(filters)); err != nil {
			return &orbitflare.StreamError{Message: "outbound closed"}
		}
	}
	onConnected()

	msgCh := make(chan *pb.SubscribeTransactionsResponse, 64)
	errCh := make(chan error, 1)
	readerDone := make(chan struct{})
	defer close(readerDone)
	go func() {
		for {
			resp, err := stream.Recv()
			if err != nil {
				select {
				case errCh <- err:
				case <-readerDone:
				}
				return
			}
			select {
			case msgCh <- resp:
			case <-readerDone:
				return
			}
		}
	}()

	ticker := time.NewTicker(cfg.PingInterval)
	defer ticker.Stop()
	var pingID int32 = 1
	var intervalsSinceTraffic uint32

	for {
		select {
		case <-ctx.Done():
			return nil

		case err := <-errCh:
			return streaming.RecvError(err)

		case resp := <-msgCh:
			intervalsSinceTraffic = 0
			select {
			case out <- streaming.Result[*pb.SubscribeTransactionsResponse]{Update: resp}:
			case <-ctx.Done():
				return nil
			}

		case cmd := <-cmdCh:
			switch cmd.kind {
			case cmdAdd:
				for _, f := range cmd.add {
					active[f.GetFilterId()] = f
				}
				if err := stream.Send(addFiltersReq(cmd.add)); err != nil {
					return &orbitflare.StreamError{Message: "outbound closed"}
				}
			case cmdRemove:
				for _, id := range cmd.remove {
					delete(active, id)
				}
				req := &pb.SubscribeTransactionsRequest{
					Payload: &pb.SubscribeTransactionsRequest_RemoveFilters{RemoveFilters: &pb.RemoveFilters{FilterIds: cmd.remove}},
				}
				if err := stream.Send(req); err != nil {
					return &orbitflare.StreamError{Message: "outbound closed"}
				}
			case cmdPing:
				if err := stream.Send(pingReq(cmd.pingID)); err != nil {
					return &orbitflare.StreamError{Message: "outbound closed"}
				}
			}

		case <-ticker.C:
			if intervalsSinceTraffic >= cfg.MaxMissedPongs {
				return &orbitflare.StreamError{Message: fmt.Sprintf("no traffic after %d pings", cfg.MaxMissedPongs)}
			}
			if err := stream.Send(pingReq(pingID)); err != nil {
				return &orbitflare.StreamError{Message: "outbound closed"}
			}
			pingID++
			intervalsSinceTraffic++
		}
	}
}

func runSlotConnection(ctx context.Context, conn *grpc.ClientConn, onConnected func(), out chan<- streaming.Result[*pb.SlotEvent]) error {
	stream, err := pb.NewJetstreamV2Client(conn).SubscribeSlots(ctx, &pb.SubscribeSlotsRequest{})
	if err != nil {
		return orbitflare.NewTransportError(err)
	}
	onConnected()

	for {
		event, err := stream.Recv()
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return streaming.RecvError(err)
		}
		select {
		case out <- streaming.Result[*pb.SlotEvent]{Update: event}:
		case <-ctx.Done():
			return nil
		}
	}
}

type Builder struct {
	cfg *streaming.ConfigBuilder
}

func NewBuilder() *Builder { return &Builder{cfg: streaming.NewConfigBuilder()} }

func (b *Builder) URL(url string) *Builder                 { b.cfg.URL(url); return b }
func (b *Builder) URLs(urls ...string) *Builder            { b.cfg.URLs(urls...); return b }
func (b *Builder) FallbackURL(url string) *Builder         { b.cfg.FallbackURL(url); return b }
func (b *Builder) FallbackURLs(urls ...string) *Builder    { b.cfg.FallbackURLs(urls...); return b }
func (b *Builder) Retry(p orbitflare.RetryPolicy) *Builder { b.cfg.Retry(p); return b }
func (b *Builder) Timeout(d time.Duration) *Builder        { b.cfg.Timeout(d); return b }
func (b *Builder) Keepalive(d time.Duration) *Builder      { b.cfg.Keepalive(d); return b }
func (b *Builder) PingInterval(d time.Duration) *Builder   { b.cfg.PingInterval(d); return b }
func (b *Builder) MaxMissedPongs(n uint32) *Builder        { b.cfg.MaxMissedPongs(n); return b }
func (b *Builder) ChannelCapacity(n int) *Builder          { b.cfg.ChannelCapacity(n); return b }
func (b *Builder) Logger(l *slog.Logger) *Builder          { b.cfg.Logger(l); return b }

func (b *Builder) Build() (*Client, error) {
	cfg, err := b.cfg.Build("ORBITFLARE_JETSTREAM_URL")
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithCancel(context.Background())
	return &Client{cfg: cfg, ctx: ctx, cancel: cancel}, nil
}
