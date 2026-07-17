package jetstream

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"time"

	"google.golang.org/grpc"

	"github.com/orbitflare/orbitflare-sdk-go/internal/streaming"
	"github.com/orbitflare/orbitflare-sdk-go/orbitflare"
	pb "github.com/orbitflare/orbitflare-sdk-go/proto/jetstream"
)

type SubscribeUpdate = pb.SubscribeUpdate

type TransactionFilter struct {
	accountInclude  []string
	accountExclude  []string
	accountRequired []string
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

func (f *TransactionFilter) toProto() *pb.SubscribeRequestFilterTransactions {
	return &pb.SubscribeRequestFilterTransactions{
		AccountInclude:  f.accountInclude,
		AccountExclude:  f.accountExclude,
		AccountRequired: f.accountRequired,
	}
}

type SubscribeRequestBuilder struct {
	transactions map[string]*TransactionFilter
}

func NewSubscribeRequestBuilder() *SubscribeRequestBuilder {
	return &SubscribeRequestBuilder{transactions: map[string]*TransactionFilter{}}
}

func (b *SubscribeRequestBuilder) Transactions(name string, filter *TransactionFilter) *SubscribeRequestBuilder {
	b.transactions[name] = filter
	return b
}

func (b *SubscribeRequestBuilder) Build() *pb.SubscribeRequest {
	transactions := make(map[string]*pb.SubscribeRequestFilterTransactions, len(b.transactions))
	for k, v := range b.transactions {
		transactions[k] = v.toProto()
	}
	return &pb.SubscribeRequest{
		Transactions: transactions,
		Ping:         &pb.SubscribeRequestPing{Id: 1},
	}
}

type Client struct {
	cfg    *streaming.Config
	ctx    context.Context
	cancel context.CancelFunc
}

func (c *Client) Close() { c.cancel() }

type Stream struct {
	ch     <-chan streaming.Result[*pb.SubscribeUpdate]
	cancel context.CancelFunc
}

func (s *Stream) Next(ctx context.Context) (*pb.SubscribeUpdate, error) {
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

func (s *Stream) Close() { s.cancel() }

func (c *Client) Subscribe(request *pb.SubscribeRequest) *Stream {
	ctx, cancel := context.WithCancel(c.ctx)
	ch := make(chan streaming.Result[*pb.SubscribeUpdate], c.cfg.ChannelCapacity)

	go streaming.RunStream(ctx, c.cfg, "jetstream", ch,
		func(rctx context.Context, conn *grpc.ClientConn, onConnected func()) error {
			return runConnection(rctx, conn, request, onConnected, ch, c.cfg)
		})

	return &Stream{ch: ch, cancel: cancel}
}

func runConnection(ctx context.Context, conn *grpc.ClientConn, request *pb.SubscribeRequest, onConnected func(), out chan<- streaming.Result[*pb.SubscribeUpdate], cfg *streaming.Config) error {
	stream, err := pb.NewJetstreamClient(conn).Subscribe(ctx)
	if err != nil {
		return orbitflare.NewTransportError(err)
	}
	if err := stream.Send(request); err != nil {
		return &orbitflare.StreamError{Message: "outbound closed"}
	}
	onConnected()

	msgCh := make(chan *pb.SubscribeUpdate, 64)
	errCh := make(chan error, 1)
	readerDone := make(chan struct{})
	defer close(readerDone)
	go func() {
		for {
			update, err := stream.Recv()
			if err != nil {
				select {
				case errCh <- err:
				case <-readerDone:
				}
				return
			}
			select {
			case msgCh <- update:
			case <-readerDone:
				return
			}
		}
	}()

	ticker := time.NewTicker(cfg.PingInterval)
	defer ticker.Stop()
	var pingID int32 = 1
	var missed uint32

	for {
		select {
		case <-ctx.Done():
			return nil

		case err := <-errCh:
			return streaming.RecvError(err)

		case update := <-msgCh:
			if update.GetPong() != nil {
				missed = 0
				continue
			}
			select {
			case out <- streaming.Result[*pb.SubscribeUpdate]{Update: update}:
			case <-ctx.Done():
				return nil
			}

		case <-ticker.C:
			if missed >= cfg.MaxMissedPongs {
				return &orbitflare.StreamError{Message: fmt.Sprintf("no pong after %d pings", cfg.MaxMissedPongs)}
			}
			ping := &pb.SubscribeRequest{Ping: &pb.SubscribeRequestPing{Id: pingID}}
			if err := stream.Send(ping); err != nil {
				return &orbitflare.StreamError{Message: "outbound closed"}
			}
			pingID++
			missed++
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
