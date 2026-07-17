package ws

import (
	"context"
	"encoding/json"
	"log/slog"
	"strings"
	"sync/atomic"
	"time"

	"github.com/gorilla/websocket"

	"github.com/orbitflare/orbitflare-sdk-go/internal/endpoint"
	"github.com/orbitflare/orbitflare-sdk-go/orbitflare"
)

type Client struct {
	bg     *background
	cancel context.CancelFunc
}

func (c *Client) Close() { c.cancel() }

func (c *Client) AccountSubscribe(ctx context.Context, address, commitment string) (*Subscription, error) {
	return c.subscribe(ctx, "accountSubscribe", "accountUnsubscribe",
		[]any{address, map[string]any{"encoding": "base64", "commitment": commitment}})
}

func (c *Client) LogsSubscribe(ctx context.Context, mentions []string, commitment string) (*Subscription, error) {
	var filter any = "all"
	if len(mentions) > 0 {
		filter = map[string]any{"mentions": mentions}
	}
	return c.subscribe(ctx, "logsSubscribe", "logsUnsubscribe",
		[]any{filter, map[string]any{"commitment": commitment}})
}

func (c *Client) SlotSubscribe(ctx context.Context) (*Subscription, error) {
	return c.subscribe(ctx, "slotSubscribe", "slotUnsubscribe", []any{})
}

func (c *Client) SignatureSubscribe(ctx context.Context, signature, commitment string) (*Subscription, error) {
	return c.subscribe(ctx, "signatureSubscribe", "signatureUnsubscribe",
		[]any{signature, map[string]any{"commitment": commitment}})
}

func (c *Client) subscribe(ctx context.Context, method, unsubMethod string, params any) (*Subscription, error) {
	events := make(chan json.RawMessage, 256)
	reply := make(chan subReply, 1)
	cmd := subscribeCommand{method: method, params: params, events: events, reply: reply}

	select {
	case c.bg.cmd <- cmd:
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-c.bg.done:
		return nil, &orbitflare.StreamError{Message: "ws background task gone"}
	}

	select {
	case r := <-reply:
		if r.err != nil {
			return nil, r.err
		}
		return &Subscription{
			events:      events,
			subID:       r.subID,
			unsubMethod: unsubMethod,
			cmd:         c.bg.cmd,
			done:        c.bg.done,
		}, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-c.bg.done:
		return nil, &orbitflare.StreamError{Message: "ws subscribe response lost"}
	}
}

type Subscription struct {
	events      chan json.RawMessage
	subID       uint64
	unsubMethod string
	cmd         chan command
	done        chan struct{}
}

func (s *Subscription) Next(ctx context.Context) (event json.RawMessage, ok bool) {
	select {
	case v, open := <-s.events:
		return v, open
	case <-ctx.Done():
		return nil, false
	case <-s.done:
		return nil, false
	}
}

func (s *Subscription) Unsubscribe() {
	select {
	case s.cmd <- unsubscribeCommand{subID: s.subID, method: s.unsubMethod}:
	case <-s.done:
	}
}

type command interface{ isCommand() }

type subscribeCommand struct {
	method string
	params any
	events chan json.RawMessage
	reply  chan subReply
}

type unsubscribeCommand struct {
	subID  uint64
	method string
}

func (subscribeCommand) isCommand()   {}
func (unsubscribeCommand) isCommand() {}

type subReply struct {
	subID uint64
	err   error
}

type activeSub struct {
	method string
	params any
	events chan json.RawMessage
}

type background struct {
	endpoints      *endpoint.Set
	cmd            chan command
	retry          orbitflare.RetryPolicy
	pingInterval   time.Duration
	maxMissedPongs uint32
	logger         *slog.Logger
	done           chan struct{}
	ctx            context.Context
}

func (bg *background) run(initial *websocket.Conn) {
	defer close(bg.done)

	subs := []activeSub{}

	if err := bg.runConnection(initial, &subs); err == nil {
		return
	}

	var attempt uint32
	for {
		attempt++
		if !bg.retry.HasAttemptsLeft(attempt) {
			bg.logger.Error("ws reconnect attempts exhausted")
			return
		}

		delay := bg.retry.DelayForAttempt(attempt)
		bg.logger.Warn("ws reconnecting", "attempt", attempt, "delay_ms", delay.Milliseconds())
		select {
		case <-bg.ctx.Done():
			return
		case <-time.After(delay):
		}

		conn, idx, err := dial(bg.ctx, bg.endpoints, bg.logger)
		if err != nil {
			if bg.ctx.Err() != nil {
				return
			}
			bg.logger.Warn("ws reconnect failed", "error", err)
			continue
		}
		attempt = 0
		bg.endpoints.MarkSuccess(idx)

		if err := bg.runConnection(conn, &subs); err == nil {
			return
		} else {
			bg.endpoints.MarkFailure(idx)
			bg.logger.Warn("ws connection lost", "error", err)
		}
	}
}

func (bg *background) runConnection(conn *websocket.Conn, subs *[]activeSub) error {
	defer conn.Close()

	subIDToIdx := map[uint64]int{}
	pending := map[uint64]pendingEntry{}
	var nextID uint64 = 1

	send := func(method string, params any, id uint64) error {
		msg, _ := json.Marshal(rpcRequest{JSONRPC: "2.0", ID: id, Method: method, Params: params})
		return conn.WriteMessage(websocket.TextMessage, msg)
	}

	for idx, sub := range *subs {
		id := nextID
		nextID++
		if err := send(sub.method, sub.params, id); err != nil {
			return orbitflare.NewTransportError(err)
		}
		pending[id] = pendingEntry{resubscribeIdx: idx}
	}

	var missed atomic.Uint32
	conn.SetPongHandler(func(string) error { missed.Store(0); return nil })

	msgCh := make(chan []byte, 64)
	errCh := make(chan error, 1)
	readerDone := make(chan struct{})
	defer close(readerDone)
	go reader(conn, msgCh, errCh, readerDone)

	ticker := time.NewTicker(bg.pingInterval)
	defer ticker.Stop()

	for {
		select {
		case <-bg.ctx.Done():
			return nil

		case err := <-errCh:
			return orbitflare.NewTransportError(err)

		case data := <-msgCh:
			missed.Store(0)
			bg.handleText(data, subs, subIDToIdx, pending, conn)

		case <-ticker.C:
			if missed.Load() >= bg.maxMissedPongs {
				return &orbitflare.StreamError{Message: "no pong after pings"}
			}
			deadline := time.Now().Add(bg.pingInterval)
			if err := conn.WriteControl(websocket.PingMessage, nil, deadline); err != nil {
				return &orbitflare.StreamError{Message: "ws ping send failed"}
			}
			missed.Add(1)

		case cmd := <-bg.cmd:
			switch c := cmd.(type) {
			case subscribeCommand:
				id := nextID
				nextID++
				if err := send(c.method, c.params, id); err != nil {
					c.reply <- subReply{err: &orbitflare.StreamError{Message: "ws send failed"}}
					return &orbitflare.StreamError{Message: "ws send failed"}
				}
				pending[id] = pendingEntry{
					resubscribeIdx: -1,
					method:         c.method,
					params:         c.params,
					events:         c.events,
					reply:          c.reply,
				}
			case unsubscribeCommand:
				if idx, ok := subIDToIdx[c.subID]; ok {
					delete(subIDToIdx, c.subID)
					*subs = append((*subs)[:idx], (*subs)[idx+1:]...)
					rebuildIndex(idx, subIDToIdx)
				}
				id := nextID
				nextID++
				_ = send(c.method, []any{c.subID}, id)
			}
		}
	}
}

type pendingEntry struct {
	resubscribeIdx int
	method         string
	params         any
	events         chan json.RawMessage
	reply          chan subReply
}

func (bg *background) handleText(text []byte, subs *[]activeSub, subIDToIdx map[uint64]int, pending map[uint64]pendingEntry, conn *websocket.Conn) {
	var msg struct {
		ID     *uint64          `json:"id"`
		Result json.RawMessage  `json:"result"`
		Error  *rpcErrorPayload `json:"error"`
		Method *string          `json:"method"`
		Params *struct {
			Subscription *uint64         `json:"subscription"`
			Result       json.RawMessage `json:"result"`
		} `json:"params"`
	}
	if json.Unmarshal(text, &msg) != nil {
		return
	}

	if msg.ID != nil {
		entry, ok := pending[*msg.ID]
		if !ok {
			return
		}
		delete(pending, *msg.ID)

		if entry.resubscribeIdx < 0 {
			if msg.Error != nil {
				entry.reply <- subReply{err: &orbitflare.RPCError{Code: msg.Error.Code, Message: orDefault(msg.Error.Message, "subscribe failed")}}
				return
			}
			subID, ok := asUint64(msg.Result)
			if !ok {
				entry.reply <- subReply{err: &orbitflare.RPCError{Code: 0, Message: "missing subscription id in response"}}
				return
			}
			idx := len(*subs)
			*subs = append(*subs, activeSub{method: entry.method, params: entry.params, events: entry.events})
			subIDToIdx[subID] = idx
			entry.reply <- subReply{subID: subID}
			return
		}

		if subID, ok := asUint64(msg.Result); ok {
			subIDToIdx[subID] = entry.resubscribeIdx
			bg.logger.Debug("resubscribed", "sub_id", subID, "idx", entry.resubscribeIdx)
		}
		return
	}

	if msg.Params != nil && msg.Params.Subscription != nil {
		subID := *msg.Params.Subscription
		idx, ok := subIDToIdx[subID]
		if !ok {
			bg.autoUnsubscribe(subID, msg.Method, conn)
			return
		}
		sub := (*subs)[idx]
		if msg.Params.Result == nil {
			return
		}
		payload := extractPayload(msg.Params.Result)
		select {
		case sub.events <- payload:
		case <-bg.ctx.Done():
		}
	}
}

func (bg *background) autoUnsubscribe(subID uint64, method *string, conn *websocket.Conn) {
	if method == nil {
		return
	}
	op, ok := strings.CutSuffix(*method, "Notification")
	if !ok {
		return
	}
	unsub := op + "Unsubscribe"
	msg, _ := json.Marshal(rpcRequest{JSONRPC: "2.0", ID: 0, Method: unsub, Params: []any{subID}})
	_ = conn.WriteControl(websocket.TextMessage, msg, time.Now().Add(bg.pingInterval))
	bg.logger.Debug("auto-unsubscribed orphaned subscription", "sub_id", subID, "unsub_method", unsub)
}

func rebuildIndex(removedIdx int, subIDToIdx map[uint64]int) {
	for k, idx := range subIDToIdx {
		if idx > removedIdx {
			subIDToIdx[k] = idx - 1
		}
	}
}

func reader(conn *websocket.Conn, msgCh chan<- []byte, errCh chan<- error, done <-chan struct{}) {
	for {
		mt, data, err := conn.ReadMessage()
		if err != nil {
			select {
			case errCh <- err:
			case <-done:
			}
			return
		}
		if mt != websocket.TextMessage {
			continue
		}
		select {
		case msgCh <- data:
		case <-done:
			return
		}
	}
}

func dial(ctx context.Context, endpoints *endpoint.Set, logger *slog.Logger) (*websocket.Conn, int, error) {
	for i := 0; i < endpoints.Len(); i++ {
		url := endpoints.Get(i)
		conn, _, err := websocket.DefaultDialer.DialContext(ctx, url, nil)
		if err == nil {
			logger.Info("ws connected", "endpoint", orbitflare.SanitizeURL(url))
			return conn, i, nil
		}
		logger.Warn("ws connect failed", "endpoint", orbitflare.SanitizeURL(url), "error", orbitflare.SanitizeURL(err.Error()))
		endpoints.MarkFailure(i)
	}
	return nil, 0, orbitflare.TransportErrorf("all ws endpoints failed")
}

func extractPayload(result json.RawMessage) json.RawMessage {
	var obj map[string]json.RawMessage
	if json.Unmarshal(result, &obj) == nil {
		_, hasCtx := obj["context"]
		if val, hasVal := obj["value"]; hasCtx && hasVal {
			return val
		}
	}
	return result
}

type rpcRequest struct {
	JSONRPC string `json:"jsonrpc"`
	ID      uint64 `json:"id"`
	Method  string `json:"method"`
	Params  any    `json:"params"`
}

type rpcErrorPayload struct {
	Code    int64  `json:"code"`
	Message string `json:"message"`
}

func asUint64(raw json.RawMessage) (uint64, bool) {
	if len(raw) == 0 {
		return 0, false
	}
	var n uint64
	if json.Unmarshal(raw, &n) != nil {
		return 0, false
	}
	return n, true
}

func orDefault(s, def string) string {
	if s == "" {
		return def
	}
	return s
}
