package rpc

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/orbitflare/orbitflare-sdk-go/orbitflare"
)

func singleAttempt() orbitflare.RetryPolicy {
	p := orbitflare.DefaultRetryPolicy()
	p.MaxAttempts = 1
	return p
}

func mockRPC(t *testing.T, body string) *Client {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		io.WriteString(w, body)
	}))
	t.Cleanup(srv.Close)
	c, err := NewBuilder().URL(srv.URL).Retry(singleAttempt()).Build()
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	return c
}

func ctx() context.Context { return context.Background() }

func TestGetSlot(t *testing.T) {
	c := mockRPC(t, `{"jsonrpc":"2.0","result":12345,"id":1}`)
	slot, err := c.GetSlot(ctx())
	if err != nil || slot != 12345 {
		t.Fatalf("GetSlot = %d, %v", slot, err)
	}
}

func TestGetBalance(t *testing.T) {
	c := mockRPC(t, `{"jsonrpc":"2.0","result":{"value":999000000},"id":1}`)
	b, err := c.GetBalance(ctx(), "addr")
	if err != nil || b != 999000000 {
		t.Fatalf("GetBalance = %d, %v", b, err)
	}
}

func TestGetLatestBlockhash(t *testing.T) {
	c := mockRPC(t, `{"jsonrpc":"2.0","result":{"value":{"blockhash":"abc123","lastValidBlockHeight":500}},"id":1}`)
	hash, height, err := c.GetLatestBlockhash(ctx())
	if err != nil || hash != "abc123" || height != 500 {
		t.Fatalf("GetLatestBlockhash = %q,%d,%v", hash, height, err)
	}
}

func TestGetAccountInfoMissing(t *testing.T) {
	c := mockRPC(t, `{"jsonrpc":"2.0","result":{"value":null},"id":1}`)
	acct, err := c.GetAccountInfo(ctx(), "missing")
	if err != nil || acct != nil {
		t.Fatalf("expected nil account, got %s, %v", acct, err)
	}
}

func TestGetAccountInfoPresent(t *testing.T) {
	c := mockRPC(t, `{"jsonrpc":"2.0","result":{"value":{"lamports":100,"owner":"11111"}},"id":1}`)
	acct, err := c.GetAccountInfo(ctx(), "addr")
	if err != nil || acct == nil {
		t.Fatalf("expected account, got %s, %v", acct, err)
	}
	if lamports := gjsonInt(t, acct, "lamports"); lamports != 100 {
		t.Fatalf("lamports = %d", lamports)
	}
}

func TestGetMultipleAccounts(t *testing.T) {
	c := mockRPC(t, `{"jsonrpc":"2.0","result":{"value":[{"lamports":10},null,{"lamports":30}]},"id":1}`)
	accts, err := c.GetMultipleAccounts(ctx(), []string{"a", "b", "c"})
	if err != nil || len(accts) != 3 {
		t.Fatalf("len = %d, %v", len(accts), err)
	}
	if accts[0] == nil || accts[1] != nil || accts[2] == nil {
		t.Fatalf("null alignment wrong: %v", accts)
	}
	if gjsonInt(t, accts[0], "lamports") != 10 || gjsonInt(t, accts[2], "lamports") != 30 {
		t.Fatalf("values wrong")
	}
}

func TestGetTransaction(t *testing.T) {
	c := mockRPC(t, `{"jsonrpc":"2.0","result":{"slot":100,"meta":{"fee":5000}},"id":1}`)
	tx, err := c.GetTransaction(ctx(), "sig123")
	if err != nil {
		t.Fatal(err)
	}
	if gjsonInt(t, tx, "slot") != 100 {
		t.Fatalf("slot wrong")
	}
}

func TestGetSignaturesForAddress(t *testing.T) {
	c := mockRPC(t, `{"jsonrpc":"2.0","result":[{"signature":"aaa","slot":1},{"signature":"bbb","slot":2}],"id":1}`)
	sigs, err := c.GetSignaturesForAddress(ctx(), "addr", 2)
	if err != nil || len(sigs) != 2 {
		t.Fatalf("len = %d, %v", len(sigs), err)
	}
}

func TestGetProgramAccounts(t *testing.T) {
	c := mockRPC(t, `{"jsonrpc":"2.0","result":[{"pubkey":"a"},{"pubkey":"b"}],"id":1}`)
	accts, err := c.GetProgramAccounts(ctx(), "program")
	if err != nil || len(accts) != 2 {
		t.Fatalf("len = %d, %v", len(accts), err)
	}
}

func TestGetRecentPrioritizationFees(t *testing.T) {
	c := mockRPC(t, `{"jsonrpc":"2.0","result":[{"slot":1,"prioritizationFee":100},{"slot":2,"prioritizationFee":200}],"id":1}`)
	fees, err := c.GetRecentPrioritizationFees(ctx(), []string{"addr"})
	if err != nil || len(fees) != 2 {
		t.Fatalf("len = %d, %v", len(fees), err)
	}
}

func TestSimulateTransaction(t *testing.T) {
	c := mockRPC(t, `{"jsonrpc":"2.0","result":{"value":{"err":null,"logs":["log1"]}},"id":1}`)
	res, err := c.SimulateTransaction(ctx(), "base64tx")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(res), `"err":null`) {
		t.Fatalf("unexpected sim result: %s", res)
	}
}

func TestGetTokenAccountsByOwner(t *testing.T) {
	c := mockRPC(t, `{"jsonrpc":"2.0","result":{"value":[{"pubkey":"tok1"}]},"id":1}`)
	toks, err := c.GetTokenAccountsByOwner(ctx(), "owner", "", "")
	if err != nil || len(toks) != 1 {
		t.Fatalf("len = %d, %v", len(toks), err)
	}
}

func TestSendTransaction(t *testing.T) {
	c := mockRPC(t, `{"jsonrpc":"2.0","result":"5K8F2jABCDEFtestsignature","id":1}`)
	sig, err := c.SendTransaction(ctx(), "base64txdata")
	if err != nil || sig != "5K8F2jABCDEFtestsignature" {
		t.Fatalf("sig = %q, %v", sig, err)
	}
}

func TestRequestCustomMethod(t *testing.T) {
	c := mockRPC(t, `{"jsonrpc":"2.0","result":{"epoch":100},"id":1}`)
	res, err := c.Request(ctx(), "getEpochInfo", []any{})
	if err != nil || gjsonInt(t, res, "epoch") != 100 {
		t.Fatalf("res = %s, %v", res, err)
	}
}

func TestRequestRaw(t *testing.T) {
	c := mockRPC(t, `{"jsonrpc":"2.0","result":"ok","id":1}`)
	res, err := c.RequestRaw(ctx(), `{"jsonrpc":"2.0","id":1,"method":"getHealth","params":[]}`)
	if err != nil || string(res) != `"ok"` {
		t.Fatalf("res = %s, %v", res, err)
	}
}

func TestCommitmentDefaultAndOverride(t *testing.T) {
	def := mockRPC(t, `{"jsonrpc":"2.0","result":0,"id":1}`)
	if def.Commitment() != "confirmed" {
		t.Fatalf("default commitment = %q", def.Commitment())
	}
	fin, err := NewBuilder().URL("http://x").Commitment("finalized").Build()
	if err != nil {
		t.Fatal(err)
	}
	if fin.Commitment() != "finalized" {
		t.Fatalf("override commitment = %q", fin.Commitment())
	}
}

func TestRPCErrorExtracted(t *testing.T) {
	c := mockRPC(t, `{"jsonrpc":"2.0","error":{"code":-32600,"message":"invalid request"},"id":1}`)
	_, err := c.GetSlot(ctx())
	var re *orbitflare.RPCError
	if !asError(err, &re) || re.Code != -32600 || !strings.Contains(re.Message, "invalid request") {
		t.Fatalf("expected RPCError -32600, got %v", err)
	}
}

func TestRPCErrorFromNon200JSONBody(t *testing.T) {
	c := mockRPCStatus(t, 403, `{"jsonrpc":"2.0","error":{"code":-32404,"message":"IP not whitelisted"}}`)
	_, err := c.GetSlot(ctx())
	var re *orbitflare.RPCError
	if !asError(err, &re) || re.Code != -32404 || !strings.Contains(re.Message, "IP not whitelisted") {
		t.Fatalf("expected RPCError -32404, got %v", err)
	}
}

func TestServerErrorIsRetryable(t *testing.T) {
	c := mockRPCStatus(t, 500, "internal error")
	_, err := c.GetSlot(ctx())
	if !orbitflare.IsRetryable(err) {
		t.Fatalf("expected retryable, got %v", err)
	}
}

func TestRateLimit429(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Retry-After", "10")
		w.WriteHeader(429)
	}))
	t.Cleanup(srv.Close)
	c, _ := NewBuilder().URL(srv.URL).Retry(singleAttempt()).Timeout(2 * time.Second).Build()
	_, err := c.GetSlot(ctx())
	var rl *orbitflare.RateLimitedError
	if !asError(err, &rl) || rl.RetryAfter == nil || *rl.RetryAfter != 10*time.Second {
		t.Fatalf("expected RateLimited 10s, got %v", err)
	}
}

func TestFailoverToSecondEndpoint(t *testing.T) {
	bad := serverStatus(t, 500, "down")
	good := serverStatus(t, 200, `{"jsonrpc":"2.0","result":42,"id":1}`)
	c, _ := NewBuilder().URLs(bad, good).Retry(singleAttempt()).Build()
	slot, err := c.GetSlot(ctx())
	if err != nil || slot != 42 {
		t.Fatalf("slot = %d, %v", slot, err)
	}
}

func TestFailoverOnNonRetryableError(t *testing.T) {
	bad := serverStatus(t, 403, `{"jsonrpc":"2.0","error":{"code":-32404,"message":"IP not whitelisted"}}`)
	good := serverStatus(t, 200, `{"jsonrpc":"2.0","result":99,"id":1}`)
	c, _ := NewBuilder().URLs(bad, good).Retry(singleAttempt()).Build()
	slot, err := c.GetSlot(ctx())
	if err != nil || slot != 99 {
		t.Fatalf("slot = %d, %v", slot, err)
	}
}

func TestRetriesBeforeFailing(t *testing.T) {
	var n atomic.Uint32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if n.Add(1) <= 2 {
			w.WriteHeader(500)
			io.WriteString(w, "down")
			return
		}
		io.WriteString(w, `{"jsonrpc":"2.0","result":777,"id":1}`)
	}))
	t.Cleanup(srv.Close)
	c, _ := NewBuilder().URL(srv.URL).Retry(orbitflare.RetryPolicy{
		InitialDelay: 10 * time.Millisecond,
		MaxDelay:     50 * time.Millisecond,
		Multiplier:   1.5,
		MaxAttempts:  5,
	}).Build()
	slot, err := c.GetSlot(ctx())
	if err != nil || slot != 777 {
		t.Fatalf("slot = %d, %v", slot, err)
	}
	if n.Load() < 3 {
		t.Fatalf("expected >=3 attempts, got %d", n.Load())
	}
}

func TestQuarantinedEndpointSkipped(t *testing.T) {
	bad := serverStatus(t, 500, "down")
	good := serverStatus(t, 200, `{"jsonrpc":"2.0","result":1,"id":1}`)
	c, _ := NewBuilder().URLs(bad, good).Retry(singleAttempt()).Build()
	if _, err := c.GetSlot(ctx()); err != nil {
		t.Fatalf("first call: %v", err)
	}
	slot, err := c.GetSlot(ctx())
	if err != nil || slot != 1 {
		t.Fatalf("slot = %d, %v", slot, err)
	}
}

func TestFailoverNoHangUnderInfiniteRetry(t *testing.T) {
	bad := serverStatus(t, 500, "down")
	good := serverStatus(t, 200, `{"jsonrpc":"2.0","result":7,"id":1}`)
	c, _ := NewBuilder().URLs(bad, good).Build()

	ctxT, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	slot, err := c.GetSlot(ctxT)
	if err != nil || slot != 7 {
		t.Fatalf("slot = %d, %v", slot, err)
	}
}

func TestErrorIncludesEndpointContext(t *testing.T) {
	c := mockRPCStatus(t, 500, "boom")
	_, err := c.GetSlot(ctx())
	if err == nil || !strings.Contains(err.Error(), "127.0.0.1") {
		t.Fatalf("error should contain endpoint: %v", err)
	}
}

func TestAPIKeyInjectedIntoURL(t *testing.T) {
	var gotQuery string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotQuery = r.URL.RawQuery
		io.WriteString(w, `{"jsonrpc":"2.0","result":1,"id":1}`)
	}))
	t.Cleanup(srv.Close)
	c, _ := NewBuilder().URL(srv.URL).APIKey("ORBIT-TEST-KEY").Build()
	if _, err := c.GetSlot(ctx()); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(gotQuery, "api_key=ORBIT-TEST-KEY") {
		t.Fatalf("query should contain api_key: %q", gotQuery)
	}
}

func TestFallbackURLMethod(t *testing.T) {
	bad := serverStatus(t, 500, "down")
	good := serverStatus(t, 200, `{"jsonrpc":"2.0","result":55,"id":1}`)
	c, _ := NewBuilder().URL(bad).FallbackURL(good).Retry(singleAttempt()).Build()
	slot, err := c.GetSlot(ctx())
	if err != nil || slot != 55 {
		t.Fatalf("slot = %d, %v", slot, err)
	}
}

func TestTimeoutTriggersError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-time.After(3 * time.Second):
			io.WriteString(w, `{"jsonrpc":"2.0","result":1,"id":1}`)
		case <-r.Context().Done():
		}
	}))
	t.Cleanup(srv.Close)
	c, _ := NewBuilder().URL(srv.URL).Timeout(100 * time.Millisecond).Retry(singleAttempt()).Build()
	_, err := c.GetSlot(ctx())
	if !orbitflare.IsRetryable(err) {
		t.Fatalf("expected retryable timeout, got %v", err)
	}
}

func TestGetTransactionsForAddressBasic(t *testing.T) {
	c := mockRPC(t, `{"jsonrpc":"2.0","result":{"data":[{"signature":"aaa","slot":1},{"signature":"bbb","slot":2}]},"id":1}`)
	res, err := c.GetTransactionsForAddress(ctx(), "addr", nil)
	if err != nil || len(res.Data) != 2 || res.PaginationToken != nil {
		t.Fatalf("res = %+v, %v", res, err)
	}
}

func TestGetTransactionsForAddressPagination(t *testing.T) {
	c := mockRPC(t, `{"jsonrpc":"2.0","result":{"data":[{"signature":"aaa"}],"paginationToken":"387936002:541"},"id":1}`)
	res, err := c.GetTransactionsForAddress(ctx(), "addr", NewGetTransactionsOptions().LimitSet(1))
	if err != nil || len(res.Data) != 1 || res.PaginationToken == nil || *res.PaginationToken != "387936002:541" {
		t.Fatalf("res = %+v, %v", res, err)
	}
}

func TestGetTransactionsForAddressSendsAllOptions(t *testing.T) {
	body := captureRequest(t, `{"jsonrpc":"2.0","result":{"data":[]},"id":1}`, func(c *Client) {
		opts := NewGetTransactionsOptions().
			TransactionDetailsSet("full").
			LimitSet(50).
			SortOrderSet("asc").
			PaginationTokenSet("100:5").
			CommitmentSet("finalized").
			FiltersSet(NewGetTransactionsFilters().
				TokenAccountsSet("all").
				StatusSet("succeeded").
				BlockTimeSet(RangeFilter[int64]{Gte: ptr(int64(1704067200)), Lte: ptr(int64(1706745600))}))
		c.GetTransactionsForAddress(ctx(), "addr", opts)
	})
	p := body["params"].([]any)[1].(map[string]any)
	if p["transactionDetails"] != "full" || p["limit"].(float64) != 50 || p["sortOrder"] != "asc" ||
		p["paginationToken"] != "100:5" || p["commitment"] != "finalized" {
		t.Fatalf("options not serialized: %v", p)
	}
	f := p["filters"].(map[string]any)
	if f["tokenAccounts"] != "all" || f["status"] != "succeeded" {
		t.Fatalf("filters wrong: %v", f)
	}
	bt := f["blockTime"].(map[string]any)
	if bt["gte"].(float64) != 1704067200 || bt["lte"].(float64) != 1706745600 {
		t.Fatalf("blockTime wrong: %v", bt)
	}
}

func TestGetTransactionsForAddressSlotFilter(t *testing.T) {
	body := captureRequest(t, `{"jsonrpc":"2.0","result":{"data":[]},"id":1}`, func(c *Client) {
		opts := NewGetTransactionsOptions().FiltersSet(NewGetTransactionsFilters().
			SlotSet(RangeFilter[uint64]{Gt: ptr(uint64(300000000)), Lt: ptr(uint64(400000000))}))
		c.GetTransactionsForAddress(ctx(), "addr", opts)
	})
	slot := body["params"].([]any)[1].(map[string]any)["filters"].(map[string]any)["slot"].(map[string]any)
	if slot["gt"].(float64) != 300000000 || slot["lt"].(float64) != 400000000 {
		t.Fatalf("slot range wrong: %v", slot)
	}
	if _, ok := slot["gte"]; ok {
		t.Fatalf("gte should be omitted")
	}
	if _, ok := slot["lte"]; ok {
		t.Fatalf("lte should be omitted")
	}
}

func TestGetTransactionsForAddressOmitsUnset(t *testing.T) {
	body := captureRequest(t, `{"jsonrpc":"2.0","result":{"data":[]},"id":1}`, func(c *Client) {
		c.GetTransactionsForAddress(ctx(), "addr", NewGetTransactionsOptions())
	})
	p := body["params"].([]any)[1].(map[string]any)
	for _, k := range []string{"transactionDetails", "limit", "filters"} {
		if _, ok := p[k]; ok {
			t.Fatalf("%s should be omitted, params: %v", k, p)
		}
	}
}

func mockRPCStatus(t *testing.T, status int, body string) *Client {
	t.Helper()
	c, _ := NewBuilder().URL(serverStatus(t, status, body)).Retry(singleAttempt()).Build()
	return c
}

func serverStatus(t *testing.T, status int, body string) string {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(status)
		io.WriteString(w, body)
	}))
	t.Cleanup(srv.Close)
	return srv.URL
}

func captureRequest(t *testing.T, respBody string, fn func(*Client)) map[string]any {
	t.Helper()
	var captured []byte
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		captured, _ = io.ReadAll(r.Body)
		io.WriteString(w, respBody)
	}))
	t.Cleanup(srv.Close)
	c, _ := NewBuilder().URL(srv.URL).Retry(singleAttempt()).Build()
	fn(c)
	var body map[string]any
	if err := json.Unmarshal(captured, &body); err != nil {
		t.Fatalf("unmarshal request: %v (body=%s)", err, captured)
	}
	return body
}

func gjsonInt(t *testing.T, raw json.RawMessage, key string) int64 {
	t.Helper()
	var m map[string]json.RawMessage
	if err := json.Unmarshal(raw, &m); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	var n int64
	if err := json.Unmarshal(m[key], &n); err != nil {
		t.Fatalf("unmarshal %s: %v", key, err)
	}
	return n
}

func asError(err error, target any) bool {
	switch tp := target.(type) {
	case **orbitflare.RPCError:
		return errors.As(err, tp)
	case **orbitflare.RateLimitedError:
		return errors.As(err, tp)
	}
	return false
}

func ptr[T any](v T) *T { return &v }
