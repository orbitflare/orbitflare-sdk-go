<p align="center">
  <img src="https://raw.githubusercontent.com/orbitflare/orbitflare-sdk-go/main/assets/banner.png" alt="orbitflare-sdk-go" width="100%">
</p>

<p align="center">
  <a href="https://pkg.go.dev/github.com/orbitflare/orbitflare-sdk-go"><img src="https://img.shields.io/badge/go.dev-reference-3CAB9C?style=flat-square&labelColor=041815" alt="Go Reference"></a>
  <a href="https://docs.orbitflare.com/sdk/overview"><img src="https://img.shields.io/badge/docs-orbitflare.com-3CAB9C?style=flat-square&labelColor=041815" alt="Documentation"></a>
</p>

# orbitflare-sdk-go

Go SDK for [OrbitFlare](https://orbitflare.com) - RPC, gRPC (Yellowstone Geyser), Jetstream, and WebSocket clients for Solana.

## Install

```bash
go get github.com/orbitflare/orbitflare-sdk-go
```

Requires Go 1.25 or later. Import only the clients you use; each lives in its own package.

```go
import (
    "github.com/orbitflare/orbitflare-sdk-go/rpc"
    "github.com/orbitflare/orbitflare-sdk-go/ws"
    "github.com/orbitflare/orbitflare-sdk-go/geyser"
    "github.com/orbitflare/orbitflare-sdk-go/jetstream"
    jetstreamv2 "github.com/orbitflare/orbitflare-sdk-go/jetstream/v2"
)
```

## RPC

```go
package main

import (
    "context"
    "fmt"

    "github.com/orbitflare/orbitflare-sdk-go/rpc"
)

func main() {
    client, err := rpc.NewBuilder().
        URL("http://ny.rpc.orbitflare.com").
        APIKey("ORBIT-...").
        Commitment("confirmed").
        Build()
    if err != nil {
        panic(err)
    }

    ctx := context.Background()

    slot, _ := client.GetSlot(ctx)
    balance, _ := client.GetBalance(ctx, "CKs1E69a2e9TmH4mKKLrXFF8kD3ZnwKjoEuXa6sz9WqX")
    blockhash, lastValid, _ := client.GetLatestBlockhash(ctx)

    health, _ := client.Request(ctx, "getHealth", []any{})
    raw, _ := client.RequestRaw(ctx, `{"jsonrpc":"2.0","id":1,"method":"getHealth","params":[]}`)

    fmt.Println(slot, balance, blockhash, lastValid, string(health), string(raw))
}
```

### Typed helpers

| Method | Returns |
|---|---|
| `GetSlot(ctx)` | Current slot (`uint64`) |
| `GetBalance(ctx, address)` | Lamports (`uint64`) |
| `GetAccountInfo(ctx, address)` | Account data (`json.RawMessage`) |
| `GetMultipleAccounts(ctx, addresses)` | Slice of accounts (auto-chunks to 100) |
| `GetLatestBlockhash(ctx)` | `(blockhash, lastValidBlockHeight)` |
| `GetTransaction(ctx, signature)` | Full transaction with metadata |
| `GetSignaturesForAddress(ctx, address, limit)` | Recent signatures |
| `GetProgramAccounts(ctx, programID)` | All accounts owned by a program |
| `GetRecentPrioritizationFees(ctx, addresses)` | Recent priority fees |
| `SendTransaction(ctx, txBase64)` | Signature string |
| `SimulateTransaction(ctx, txBase64)` | Simulation result |
| `GetTokenAccountsByOwner(ctx, owner, mint, programID)` | Token accounts |
| `GetTransactionsForAddress(ctx, address, options)` | Full transaction history with filters and pagination (OrbitFlare-specific) |
| `Request(ctx, method, params)` | Any RPC method by name |
| `RequestRaw(ctx, body)` | Raw JSON-RPC body string |

## DAS API

The Digital Asset Standard methods read NFTs, compressed NFTs, MPL Core assets and SPL tokens. They are on the RPC `Client` and use the same endpoint.

```go
asset, err := client.GetAsset(ctx, "GJfYbfW7gufzFYfm8gFatrUE71wYsjay6FwzDkTa3vaD", nil)
fmt.Println(asset.Name(), asset.Owner())

portfolio, err := client.GetAssetsByOwner(ctx, &rpc.GetAssetsByOwnerRequest{
    OwnerAddress: "86xCnPeV69n6t3DnyGvkKobf9FdN2H9oiVDdaMpo2MMY",
    Options:      &rpc.DisplayOptions{ShowFungible: true, ShowNativeBalance: true},
})

tokens, err := client.SearchAssets(ctx, &rpc.SearchAssetsRequest{
    OwnerAddress: "86xCnPeV69n6t3DnyGvkKobf9FdN2H9oiVDdaMpo2MMY",
    TokenType:    rpc.TokenTypeFungible,
    Burnt:        rpc.Ptr(false),
})

// Every asset in a collection, page by page.
all, err := client.AssetPages(rpc.AssetsInCollection("CDDyz6MHYY8972DPCXiEeReCxGqJSCbxHnimrvoyWrFF")).All(ctx)
```

| Method | Returns |
|---|---|
| `GetAsset(ctx, id, options)` | `*Asset` |
| `GetAssetBatch(ctx, ids, options)` | `[]*Asset` in request order, `nil` where not found (auto-chunks to 1,000) |
| `GetAssetProof(ctx, id)` | `*AssetProof` for a compressed asset |
| `GetAssetProofBatch(ctx, ids)` | Proofs keyed by asset ID (auto-chunks to 1,000) |
| `GetAssetWithProof(ctx, id, options)` | Asset and proof in one call, for Bubblegum instructions |
| `GetAssetsByOwner(ctx, req)` | `*AssetList`, with `NativeBalance` when asked |
| `GetAssetsByGroup(ctx, req)` | `*AssetList` |
| `GetAssetsByCreator(ctx, req)` | `*AssetList` |
| `GetAssetsByAuthority(ctx, req)` | `*AssetList` |
| `SearchAssets(ctx, req)` | `*AssetList` |
| `GetTokenAccounts(ctx, req)` | `*TokenAccountList`, by owner, mint or both |
| `GetNftEditions(ctx, req)` | `*NftEditions` of a master edition |
| `GetSignaturesForAsset(ctx, req)` | `*AssetSignatures` (signature and instruction pairs) |
| `AssetPages`, `TokenAccountPages`, `NftEditionPages`, `SignaturePages` | A `Pager` that walks every page: `Next(ctx)` or `All(ctx)` |

`options` may be `nil`. Requests embed `Paging` (`Page`, `Limit`, `Cursor`, `Before`, `After`) and take `SortBy`; use one paging style per request. Pagers copy the request, so it isn't changed. `Asset` has `Name()`, `Symbol()`, `Image()`, `Owner()`, `Collection()` and `IsCompressed()`, and `AssetProof.ProofPath(canopyDepth)` trims the proof for trees with a canopy.

## gRPC (Yellowstone Geyser)

Build the subscription with the typed filter builder:

```go
package main

import (
    "context"
    "errors"
    "fmt"
    "io"

    "github.com/orbitflare/orbitflare-sdk-go/geyser"
)

func main() {
    client, err := geyser.NewBuilder().
        URL("http://ny.rpc.orbitflare.com:10000").
        Build()
    if err != nil {
        panic(err)
    }
    defer client.Close()

    request := geyser.NewSubscribeRequestBuilder().
        Transactions("pumpfun", geyser.NewTransactionFilter().
            Vote(false).
            Failed(false).
            AccountInclude("6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P").
            AccountRequired("So11111111111111111111111111111111111111112")).
        Accounts("wsol-token-accounts", geyser.NewAccountFilter().
            Owner("TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA").
            Datasize(165).
            Memcmp(geyser.MemcmpBase58(0, "So11111111111111111111111111111111111111112")).
            Lamports(geyser.LamportsGt(0))).
        Slots("slots", geyser.NewSlotFilter().FilterByCommitment(true)).
        Commitment(geyser.Confirmed).
        Build()

    stream := client.Subscribe(request)
    defer stream.Close()

    ctx := context.Background()
    for {
        update, err := stream.Next(ctx)
        if errors.Is(err, io.EOF) {
            return
        }
        if err != nil {
            panic(err)
        }
        if tx := update.GetTransaction(); tx != nil {
            fmt.Printf("slot=%d\n", tx.GetSlot())
        }
    }
}
```

`Subscribe` accepts a `*geyser.SubscribeRequest` (a re-export of the raw proto type), so you can also construct the request by hand instead of using the builder.

Available filters: `TransactionFilter`, `AccountFilter` (with `Memcmp`, `Datasize`, `Lamports`, `TokenAccountState`), `SlotFilter`, `BlockFilter`, and `DeshredTransactionFilter`.

## Jetstream

```go
package main

import (
    "context"
    "errors"
    "fmt"
    "io"

    "github.com/orbitflare/orbitflare-sdk-go/jetstream"
)

func main() {
    client, err := jetstream.NewBuilder().
        URL("http://ny.jetstream.orbitflare.com").
        Build()
    if err != nil {
        panic(err)
    }
    defer client.Close()

    request := jetstream.NewSubscribeRequestBuilder().
        Transactions("raydium", jetstream.NewTransactionFilter().
            AccountInclude("675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8")).
        Build()

    stream := client.Subscribe(request)
    defer stream.Close()

    ctx := context.Background()
    for {
        update, err := stream.Next(ctx)
        if errors.Is(err, io.EOF) {
            return
        }
        if err != nil {
            panic(err)
        }
        if tx := update.GetTransaction(); tx != nil {
            fmt.Printf("slot=%d\n", tx.GetSlot())
        }
    }
}
```

### Jetstream v2

v2 (`jetstream/v2`) runs on the same endpoint and auth as v1 and is fully additive. Filters are managed at runtime over a bidirectional stream (no reconnect to change them), every response carries a monotonic `Sequence` for gap detection, slot lifecycle events are a separate stream, and transactions can opt into enrichment (fee payer, program ids, compute-unit price, loaded address-table addresses, and more).

```go
package main

import (
    "context"
    "errors"
    "fmt"
    "io"

    jetstreamv2 "github.com/orbitflare/orbitflare-sdk-go/jetstream/v2"
)

func main() {
    client, err := jetstreamv2.NewBuilder().
        URL("http://ny.jetstream.orbitflare.com").
        Build()
    if err != nil {
        panic(err)
    }
    defer client.Close()

    ctx := context.Background()

    version, _ := client.GetVersion(ctx)
    fmt.Printf("version=%s\n", version)

    filter := jetstreamv2.NewTransactionFilter().
        AccountInclude("6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P").
        IncludeEnrichment(true).
        WithID("pumpfun")

    stream := client.SubscribeTransactions(filter)
    defer stream.Close()

    for {
        resp, err := stream.Next(ctx)
        if errors.Is(err, io.EOF) {
            return
        }
        if err != nil {
            panic(err)
        }
        fmt.Printf("seq=%d\n", resp.GetSequence())
    }
}
```

Add or remove filters on a live stream without reconnecting:

```go
handle := stream.Handle()
handle.AddFilters([]*jetstreamv2.TxFilter{
    jetstreamv2.NewTransactionFilter().AccountInclude("...").WithID("raydium"),
})
handle.RemoveFilters([]string{"pumpfun"})
```

Slot lifecycle events are a separate server stream:

```go
slots := client.SubscribeSlots()
defer slots.Close()

for {
    event, err := slots.Next(ctx)
    if errors.Is(err, io.EOF) {
        break
    }
    if err != nil {
        panic(err)
    }
    fmt.Printf("slot=%d status=%s\n", event.GetSlot(), event.GetStatus())
}
```

## WebSocket

```go
package main

import (
    "context"
    "fmt"

    "github.com/orbitflare/orbitflare-sdk-go/ws"
)

func main() {
    ctx := context.Background()

    client, err := ws.NewBuilder().
        URL("ws://ny.rpc.orbitflare.com").
        APIKey("ORBIT-...").
        Build(ctx)
    if err != nil {
        panic(err)
    }
    defer client.Close()

    sub, err := client.SlotSubscribe(ctx)
    if err != nil {
        panic(err)
    }
    defer sub.Unsubscribe()

    for {
        event, ok := sub.Next(ctx)
        if !ok {
            break
        }
        fmt.Println(string(event))
    }
}
```

Subscription methods: `AccountSubscribe`, `LogsSubscribe`, `SlotSubscribe`, `SignatureSubscribe`. All auto-resubscribe on reconnect.

## Endpoint failover

All clients support multiple endpoints with automatic failover and health tracking. The first URL is the primary; the rest are fallbacks.

```go
client, err := rpc.NewBuilder().
    URLs(
        "http://ny.rpc.orbitflare.com",
        "http://fra.rpc.orbitflare.com",
        "http://ams.rpc.orbitflare.com",
    ).
    Build()
```

Failing endpoints are quarantined with exponential cooldown (10s, 20s, 40s, max 60s) and automatically retried once the cooldown expires. Healthy endpoints are always preferred.

## Retry

RPC calls retry on transient errors (5xx, 429, connection resets, Solana error codes -32005, -32007, -32014, -32015, -32016) with exponential backoff before failing over to the next endpoint. 429 responses with a `Retry-After` header are respected.

gRPC, Jetstream, and WebSocket connections use active ping/pong to detect dead connections. Configurable via the builder:

```go
client, err := geyser.NewBuilder().
    URL("http://ny.rpc.orbitflare.com:10000").
    PingInterval(15 * time.Second). // send a ping every 15s (default: 10s)
    MaxMissedPongs(5).              // kill the connection after 5 missed pongs (default: 3)
    Build()
```

All three streaming clients reconnect automatically on disconnection. WebSocket also re-subscribes all active subscriptions after reconnecting.

Configure retry behavior with a `RetryPolicy`:

```go
import (
    "time"

    "github.com/orbitflare/orbitflare-sdk-go/orbitflare"
)

client, err := rpc.NewBuilder().
    URL("http://ny.rpc.orbitflare.com").
    Retry(orbitflare.RetryPolicy{
        InitialDelay: 200 * time.Millisecond,
        MaxDelay:     15 * time.Second,
        Multiplier:   2.0,
        MaxAttempts:  5,
    }).
    Build()
```

## Streams

Every streaming client returns a value plus an error from `Next(ctx)`. `io.EOF` signals a clean end (including after `Close`); any other non-nil error is fatal for that stream. `Next` also returns `ctx.Err()` if the passed context is cancelled.

```go
for {
    update, err := stream.Next(ctx)
    if errors.Is(err, io.EOF) {
        break // stream closed
    }
    if err != nil {
        // fatal stream error
        break
    }
    // handle update
}
```

## Errors

Errors are typed. Match them with `errors.As`:

```go
var rpcErr *orbitflare.RPCError
if errors.As(err, &rpcErr) {
    fmt.Println(rpcErr.Code, rpcErr.Message)
}
```

Types: `TransportError`, `RPCError`, `GRPCError`, `RateLimitedError`, `AuthError`, `StreamError`, `ConfigError`, `SerializationError`, plus `ErrTimeout`. `orbitflare.IsRetryable(err)` reports whether an error is transient.

## Logging

Set `ORBITFLARE_LOG` (`trace|debug|info|warn|error|silent`, default `silent`) for connection lifecycle logs, or pass your own `*slog.Logger` to a streaming builder with `.Logger(...)`.

## Environment variables

| Variable | Used by | Purpose |
|---|---|---|
| `ORBITFLARE_LICENSE_KEY` | RPC, WebSocket | API key appended to endpoint URLs |
| `ORBITFLARE_RPC_URL` | RPC | Default endpoint if `URL()` is not called |
| `ORBITFLARE_WS_URL` | WebSocket | Default endpoint if `URL()` is not called |
| `ORBITFLARE_GRPC_URL` | gRPC | Default endpoint if `URL()` is not called |
| `ORBITFLARE_JETSTREAM_URL` | Jetstream | Default endpoint if `URL()` is not called |
| `ORBITFLARE_LOG` | all | Log level for connection lifecycle events |

## Examples

Runnable programs live in [`examples/`](examples): `rpc`, `das`, `ws`, `geyser`, `jetstream`, and `jetstream_v2`.

```bash
ORBITFLARE_LICENSE_KEY=ORBIT-... go run ./examples/rpc
```
