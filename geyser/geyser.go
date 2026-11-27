package geyser

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"time"

	"google.golang.org/protobuf/proto"

	"github.com/orbitflare/orbitflare-sdk-go/internal/events"
	"github.com/orbitflare/orbitflare-sdk-go/internal/streaming"
	"github.com/orbitflare/orbitflare-sdk-go/orbitflare"
	pb "github.com/orbitflare/orbitflare-sdk-go/proto/geyser"
)

type SubscribeUpdate = pb.SubscribeUpdate

type SubscribeRequest = pb.SubscribeRequest

const replayInfoTimeout = 10 * time.Second

type ConnectInfo struct {
	Endpoint  string
	Attempt   uint32
	Reconnect bool
	LastSlot  *uint64
}

type Client struct {
	cfg    *streaming.Config
	ctx    context.Context
	cancel context.CancelFunc
}

func (c *Client) Close() { c.cancel() }

type Handle struct {
	cmdCh chan *pb.SubscribeRequest
	done  <-chan struct{}
}

func (h Handle) Update(request *pb.SubscribeRequest) error {
	select {
	case <-h.done:
		return &orbitflare.StreamError{Message: "stream closed"}
	default:
	}
	select {
	case h.cmdCh <- request:
		return nil
	case <-h.done:
		return &orbitflare.StreamError{Message: "stream closed"}
	}
}

type Stream struct {
	ch     <-chan streaming.Result[*pb.SubscribeUpdate]
	handle Handle
	hub    *events.Hub
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

func (s *Stream) Handle() Handle { return s.handle }

func (s *Stream) Update(request *pb.SubscribeRequest) error { return s.handle.Update(request) }

func (s *Stream) Events() *orbitflare.ConnectionEvents { return s.hub.Subscribe() }

func (s *Stream) Close() { s.cancel() }

type session struct {
	fixed           *pb.SubscribeRequest
	build           func(ConnectInfo) *pb.SubscribeRequest
	commands        chan *pb.SubscribeRequest
	connectedBefore bool
	lastSlot        *uint64
}

func (s *session) request(info ConnectInfo) *pb.SubscribeRequest {
	if s.build != nil {
		return proto.Clone(s.build(info)).(*pb.SubscribeRequest)
	}
	return proto.Clone(s.fixed).(*pb.SubscribeRequest)
}

func (s *session) updated(request *pb.SubscribeRequest) *pb.SubscribeRequest {
	update := proto.Clone(request).(*pb.SubscribeRequest)
	update.FromSlot = nil
	if s.build == nil {
		s.fixed = update
	}
	return update
}

func (c *Client) Subscribe(request *pb.SubscribeRequest) *Stream {
	return c.spawn(&session{fixed: proto.Clone(request).(*pb.SubscribeRequest)})
}

func (c *Client) SubscribeWith(build func(ConnectInfo) *pb.SubscribeRequest) *Stream {
	return c.spawn(&session{build: build})
}

func (c *Client) spawn(sess *session) *Stream {
	ctx, cancel := context.WithCancel(c.ctx)
	ch := make(chan streaming.Result[*pb.SubscribeUpdate], c.cfg.ChannelCapacity)
	sess.commands = make(chan *pb.SubscribeRequest, 256)
	hub := events.NewHub()

	go streaming.RunStream(ctx, c.cfg, "grpc", ch, hub,
		func(rctx context.Context, conn *streaming.Conn) error {
			return runConnection(rctx, conn, sess, ch, c.cfg)
		})

	return &Stream{
		ch:     ch,
		handle: Handle{cmdCh: sess.commands, done: ctx.Done()},
		hub:    hub,
		cancel: cancel,
	}
}

func slotOf(update *pb.SubscribeUpdate) (uint64, bool) {
	switch u := update.GetUpdateOneof().(type) {
	case *pb.SubscribeUpdate_Slot:
		return u.Slot.GetSlot(), true
	case *pb.SubscribeUpdate_Transaction:
		return u.Transaction.GetSlot(), true
	case *pb.SubscribeUpdate_TransactionStatus:
		return u.TransactionStatus.GetSlot(), true
	case *pb.SubscribeUpdate_Account:
		return u.Account.GetSlot(), true
	case *pb.SubscribeUpdate_Block:
		return u.Block.GetSlot(), true
	case *pb.SubscribeUpdate_BlockMeta:
		return u.BlockMeta.GetSlot(), true
	case *pb.SubscribeUpdate_Entry:
		return u.Entry.GetSlot(), true
	}
	return 0, false
}

func replayFrom(ctx context.Context, client pb.GeyserClient, conn *streaming.Conn, cfg *streaming.Config, wanted uint64) *uint64 {
	rctx, cancel := context.WithTimeout(ctx, replayInfoTimeout)
	defer cancel()
	info, err := client.SubscribeReplayInfo(rctx, &pb.SubscribeReplayInfoRequest{})
	if err != nil {
		cfg.Logger.Debug("replay window lookup failed; using from_slot as is", "error", err)
		return &wanted
	}
	if info.FirstAvailable == nil {
		cfg.Logger.Warn("endpoint does not support replay; starting live", "requested", wanted)
		conn.Emit(orbitflare.ReplayUnavailable{Requested: wanted})
		return nil
	}
	available := *info.FirstAvailable
	if wanted < available {
		cfg.Logger.Warn("from_slot is older than the replay window; starting at the oldest available slot",
			"requested", wanted, "available", available)
		conn.Emit(orbitflare.ReplayClamped{Requested: wanted, Available: available})
		return &available
	}
	return &wanted
}

func runConnection(ctx context.Context, conn *streaming.Conn, sess *session, out chan<- streaming.Result[*pb.SubscribeUpdate], cfg *streaming.Config) error {
	client := pb.NewGeyserClient(conn.ClientConn)

	for drained := false; !drained; {
		select {
		case update := <-sess.commands:
			sess.updated(update)
		default:
			drained = true
		}
	}
	request := sess.request(ConnectInfo{
		Endpoint:  conn.Endpoint,
		Attempt:   conn.Attempt,
		Reconnect: sess.connectedBefore,
		LastSlot:  sess.lastSlot,
	})
	if request.FromSlot != nil {
		request.FromSlot = replayFrom(ctx, client, conn, cfg, *request.FromSlot)
	}

	stream, err := client.Subscribe(ctx)
	if err != nil {
		return streaming.RecvError(err)
	}
	if err := stream.Send(request); err != nil {
		_, rerr := stream.Recv()
		return streaming.RecvError(rerr)
	}
	if err := streaming.Header(stream, func() error { _, err := stream.Recv(); return err }); err != nil {
		return err
	}
	conn.Connected()
	sess.connectedBefore = true

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
			missed = 0
			conn.Message()
			if update.GetPong() != nil {
				continue
			}
			if slot, ok := slotOf(update); ok && (sess.lastSlot == nil || slot > *sess.lastSlot) {
				sess.lastSlot = &slot
			}
			select {
			case out <- streaming.Result[*pb.SubscribeUpdate]{Update: update}:
			case <-ctx.Done():
				return nil
			}

		case update := <-sess.commands:
			if err := stream.Send(sess.updated(update)); err != nil {
				return &orbitflare.StreamError{Message: "outbound closed"}
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

func (b *Builder) URL(url string) *Builder { b.cfg.URL(url); return b }

func (b *Builder) URLs(urls ...string) *Builder { b.cfg.URLs(urls...); return b }

func (b *Builder) FallbackURL(url string) *Builder { b.cfg.FallbackURL(url); return b }

func (b *Builder) FallbackURLs(urls ...string) *Builder { b.cfg.FallbackURLs(urls...); return b }

func (b *Builder) APIKey(key string) *Builder { b.cfg.APIKey(key); return b }

func (b *Builder) Retry(p orbitflare.RetryPolicy) *Builder { b.cfg.Retry(p); return b }

func (b *Builder) Timeout(d time.Duration) *Builder { b.cfg.Timeout(d); return b }

func (b *Builder) Keepalive(d time.Duration) *Builder { b.cfg.Keepalive(d); return b }

func (b *Builder) PingInterval(d time.Duration) *Builder { b.cfg.PingInterval(d); return b }

func (b *Builder) MaxMissedPongs(n uint32) *Builder { b.cfg.MaxMissedPongs(n); return b }

func (b *Builder) ChannelCapacity(n int) *Builder { b.cfg.ChannelCapacity(n); return b }

func (b *Builder) MaxDecodingMessageSize(n int) *Builder { b.cfg.MaxDecodingMessageSize(n); return b }

func (b *Builder) Logger(l *slog.Logger) *Builder { b.cfg.Logger(l); return b }

func (b *Builder) Build() (*Client, error) {
	cfg, err := b.cfg.Build("ORBITFLARE_GRPC_URL")
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithCancel(context.Background())
	return &Client{cfg: cfg, ctx: ctx, cancel: cancel}, nil
}
