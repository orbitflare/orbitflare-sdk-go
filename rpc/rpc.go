package rpc

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"strconv"
	"time"

	"github.com/orbitflare/orbitflare-sdk-go/internal/endpoint"
	"github.com/orbitflare/orbitflare-sdk-go/orbitflare"
)

const splTokenProgramID = "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA"

type Client struct {
	http       *http.Client
	endpoints  *endpoint.Set
	apiKey     string
	commitment string
	retry      orbitflare.RetryPolicy
	logger     *slog.Logger
}

type jsonRPCRequest struct {
	JSONRPC string `json:"jsonrpc"`
	ID      int    `json:"id"`
	Method  string `json:"method"`
	Params  any    `json:"params"`
}

func (c *Client) Commitment() string { return c.commitment }

func (c *Client) Request(ctx context.Context, method string, params any) (json.RawMessage, error) {
	body, err := json.Marshal(jsonRPCRequest{JSONRPC: "2.0", ID: 1, Method: method, Params: params})
	if err != nil {
		return nil, &orbitflare.SerializationError{Message: err.Error()}
	}
	return c.execute(ctx, body, method)
}

func (c *Client) RequestRaw(ctx context.Context, body string) (json.RawMessage, error) {
	var parsed map[string]json.RawMessage
	if err := json.Unmarshal([]byte(body), &parsed); err != nil {
		return nil, &orbitflare.SerializationError{Message: err.Error()}
	}
	method := "raw"
	if m, ok := parsed["method"]; ok {
		var s string
		if json.Unmarshal(m, &s) == nil {
			method = s
		}
	}
	return c.execute(ctx, []byte(body), method)
}

func (c *Client) GetSlot(ctx context.Context) (uint64, error) {
	raw, err := c.Request(ctx, "getSlot", []any{map[string]any{"commitment": c.commitment}})
	if err != nil {
		return 0, err
	}
	var slot uint64
	if err := json.Unmarshal(raw, &slot); err != nil {
		return 0, &orbitflare.SerializationError{Message: "expected u64 for slot"}
	}
	return slot, nil
}

func (c *Client) GetBalance(ctx context.Context, address string) (uint64, error) {
	raw, err := c.Request(ctx, "getBalance", []any{address, map[string]any{"commitment": c.commitment}})
	if err != nil {
		return 0, err
	}
	var wrap struct {
		Value uint64 `json:"value"`
	}
	if err := json.Unmarshal(raw, &wrap); err != nil {
		return 0, &orbitflare.SerializationError{Message: "expected balance value"}
	}
	return wrap.Value, nil
}

func (c *Client) GetAccountInfo(ctx context.Context, address string) (json.RawMessage, error) {
	raw, err := c.Request(ctx, "getAccountInfo", []any{address, map[string]any{"encoding": "base64", "commitment": c.commitment}})
	if err != nil {
		return nil, err
	}
	return extractValue(raw), nil
}

func (c *Client) GetMultipleAccounts(ctx context.Context, addresses []string) ([]json.RawMessage, error) {
	all := make([]json.RawMessage, 0, len(addresses))
	for i := 0; i < len(addresses); i += 100 {
		end := min(i+100, len(addresses))
		raw, err := c.Request(ctx, "getMultipleAccounts", []any{addresses[i:end], map[string]any{"encoding": "base64", "commitment": c.commitment}})
		if err != nil {
			return nil, err
		}
		var wrap struct {
			Value []json.RawMessage `json:"value"`
		}
		if err := json.Unmarshal(raw, &wrap); err != nil {
			return nil, &orbitflare.SerializationError{Message: "expected accounts array"}
		}
		for _, item := range wrap.Value {
			all = append(all, nullToNil(item))
		}
	}
	return all, nil
}

func (c *Client) GetLatestBlockhash(ctx context.Context) (blockhash string, lastValidBlockHeight uint64, err error) {
	raw, err := c.Request(ctx, "getLatestBlockhash", []any{map[string]any{"commitment": c.commitment}})
	if err != nil {
		return "", 0, err
	}
	var wrap struct {
		Value struct {
			Blockhash            string `json:"blockhash"`
			LastValidBlockHeight uint64 `json:"lastValidBlockHeight"`
		} `json:"value"`
	}
	if err := json.Unmarshal(raw, &wrap); err != nil {
		return "", 0, &orbitflare.SerializationError{Message: "missing blockhash"}
	}
	if wrap.Value.Blockhash == "" {
		return "", 0, &orbitflare.SerializationError{Message: "missing blockhash"}
	}
	return wrap.Value.Blockhash, wrap.Value.LastValidBlockHeight, nil
}

func (c *Client) GetTransaction(ctx context.Context, signature string) (json.RawMessage, error) {
	return c.Request(ctx, "getTransaction", []any{signature, map[string]any{
		"encoding":                       "json",
		"commitment":                     c.commitment,
		"maxSupportedTransactionVersion": 0,
	}})
}

func (c *Client) GetSignaturesForAddress(ctx context.Context, address string, limit uint32) ([]json.RawMessage, error) {
	raw, err := c.Request(ctx, "getSignaturesForAddress", []any{address, map[string]any{"limit": limit, "commitment": c.commitment}})
	if err != nil {
		return nil, err
	}
	return decodeArray(raw)
}

func (c *Client) GetProgramAccounts(ctx context.Context, programID string) ([]json.RawMessage, error) {
	raw, err := c.Request(ctx, "getProgramAccounts", []any{programID, map[string]any{"encoding": "base64", "commitment": c.commitment}})
	if err != nil {
		return nil, err
	}
	return decodeArray(raw)
}

func (c *Client) GetRecentPrioritizationFees(ctx context.Context, addresses []string) ([]json.RawMessage, error) {
	raw, err := c.Request(ctx, "getRecentPrioritizationFees", []any{addresses})
	if err != nil {
		return nil, err
	}
	return decodeArray(raw)
}

func (c *Client) SendTransaction(ctx context.Context, txBase64 string) (string, error) {
	raw, err := c.Request(ctx, "sendTransaction", []any{txBase64, map[string]any{
		"encoding":            "base64",
		"skipPreflight":       false,
		"preflightCommitment": c.commitment,
	}})
	if err != nil {
		return "", err
	}
	var sig string
	if err := json.Unmarshal(raw, &sig); err != nil {
		return "", &orbitflare.SerializationError{Message: "expected signature string"}
	}
	return sig, nil
}

func (c *Client) SimulateTransaction(ctx context.Context, txBase64 string) (json.RawMessage, error) {
	return c.Request(ctx, "simulateTransaction", []any{txBase64, map[string]any{"encoding": "base64", "commitment": c.commitment}})
}

func (c *Client) GetTokenAccountsByOwner(ctx context.Context, owner, mint, programID string) ([]json.RawMessage, error) {
	var filter map[string]any
	switch {
	case mint != "":
		filter = map[string]any{"mint": mint}
	case programID != "":
		filter = map[string]any{"programId": programID}
	default:
		filter = map[string]any{"programId": splTokenProgramID}
	}
	raw, err := c.Request(ctx, "getTokenAccountsByOwner", []any{owner, filter, map[string]any{"encoding": "jsonParsed", "commitment": c.commitment}})
	if err != nil {
		return nil, err
	}
	var wrap struct {
		Value []json.RawMessage `json:"value"`
	}
	if err := json.Unmarshal(raw, &wrap); err != nil {
		return nil, &orbitflare.SerializationError{Message: "expected array"}
	}
	return wrap.Value, nil
}

func (c *Client) GetTransactionsForAddress(ctx context.Context, address string, options *GetTransactionsOptions) (GetTransactionsResult, error) {
	if options == nil {
		options = &GetTransactionsOptions{}
	}
	raw, err := c.Request(ctx, "getTransactionsForAddress", []any{address, options})
	if err != nil {
		return GetTransactionsResult{}, err
	}
	var wrap struct {
		Data            []json.RawMessage `json:"data"`
		PaginationToken *string           `json:"paginationToken"`
	}
	if err := json.Unmarshal(raw, &wrap); err != nil {
		return GetTransactionsResult{}, &orbitflare.SerializationError{Message: err.Error()}
	}
	return GetTransactionsResult{Data: wrap.Data, PaginationToken: wrap.PaginationToken}, nil
}

func (c *Client) execute(ctx context.Context, body []byte, method string) (json.RawMessage, error) {
	var attempt uint32
	for {
		attempt++
		idx := c.endpoints.Pick()
		url := c.endpoints.Get(idx)

		c.logger.Debug("rpc request", "method", method, "endpoint", orbitflare.SanitizeURL(url), "attempt", attempt)
		result, err := c.post(ctx, url, body)
		if err == nil {
			c.endpoints.MarkSuccess(idx)
			return result, nil
		}
		c.endpoints.MarkFailure(idx)

		if attempt < uint32(c.endpoints.Len()) {
			c.logger.Warn("failing over", "method", method, "endpoint", orbitflare.SanitizeURL(url), "error", err)
			continue
		}

		if !orbitflare.IsRetryable(err) || !c.retry.HasAttemptsLeft(attempt) {
			return nil, orbitflare.WithEndpoint(err, url)
		}

		delay := c.retry.DelayForAttempt(attempt)
		if ra := orbitflare.RetryAfter(err); ra != nil {
			delay = *ra
		}
		c.logger.Warn("retrying", "method", method, "endpoint", orbitflare.SanitizeURL(url), "attempt", attempt, "delay_ms", delay.Milliseconds(), "error", err)
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(delay):
		}
	}
}

func (c *Client) resolveAPIKey() string {
	if c.apiKey != "" {
		return c.apiKey
	}
	return os.Getenv("ORBITFLARE_LICENSE_KEY")
}

func (c *Client) post(ctx context.Context, url string, body []byte) (json.RawMessage, error) {
	authURL := url
	if key := c.resolveAPIKey(); key != "" {
		authURL = orbitflare.ApplyAPIKey(url, key)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, authURL, bytes.NewReader(body))
	if err != nil {
		return nil, orbitflare.NewTransportError(err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.http.Do(req)
	if err != nil {
		return nil, classifyDoError(err)
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusTooManyRequests {
		var retryAfter *time.Duration
		if v := resp.Header.Get("Retry-After"); v != "" {
			if secs, perr := strconv.ParseUint(v, 10, 64); perr == nil {
				d := time.Duration(secs) * time.Second
				retryAfter = &d
			}
		}
		io.Copy(io.Discard, resp.Body)
		return nil, &orbitflare.RateLimitedError{RetryAfter: retryAfter}
	}

	text, _ := io.ReadAll(resp.Body)

	if resp.StatusCode >= 500 {
		return nil, orbitflare.NewTransportError(fmt.Errorf("HTTP %d: %s", resp.StatusCode, trimSpace(text)))
	}

	var parsed map[string]json.RawMessage
	if json.Unmarshal(text, &parsed) == nil {
		if rpcErr, ok := parsed["error"]; ok {
			var e struct {
				Code    int64  `json:"code"`
				Message string `json:"message"`
			}
			e.Message = "unknown rpc error"
			_ = json.Unmarshal(rpcErr, &e)
			return nil, &orbitflare.RPCError{Code: e.Code, Message: e.Message}
		}
		if resp.StatusCode >= 200 && resp.StatusCode < 300 {
			return parsed["result"], nil
		}
	}

	return nil, &orbitflare.RPCError{Code: -int64(resp.StatusCode), Message: trimSpace(text)}
}

func classifyDoError(err error) error {
	if errors.Is(err, context.DeadlineExceeded) {
		return orbitflare.ErrTimeout
	}
	var ne net.Error
	if errors.As(err, &ne) && ne.Timeout() {
		return orbitflare.ErrTimeout
	}
	return orbitflare.NewTransportError(err)
}

func extractValue(raw json.RawMessage) json.RawMessage {
	var wrap struct {
		Value json.RawMessage `json:"value"`
	}
	if json.Unmarshal(raw, &wrap) != nil {
		return nil
	}
	return nullToNil(wrap.Value)
}

func decodeArray(raw json.RawMessage) ([]json.RawMessage, error) {
	var arr []json.RawMessage
	if err := json.Unmarshal(raw, &arr); err != nil {
		return nil, &orbitflare.SerializationError{Message: "expected array"}
	}
	return arr, nil
}

func nullToNil(v json.RawMessage) json.RawMessage {
	if len(v) == 0 || string(v) == "null" {
		return nil
	}
	return v
}

func trimSpace(b []byte) string {
	return string(bytes.TrimSpace(b))
}
