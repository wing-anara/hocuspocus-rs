# hocuspocus-rs

An idiomatic, high-performance **Rust reimplementation of [Hocuspocus](https://hocuspocus.dev)** — the
backend for [Yjs](https://github.com/yjs/yjs) collaborative editing. It speaks the
**exact same WebSocket wire protocol** as the TypeScript server, so it is a drop-in
replacement for `@hocuspocus/provider` and `y-websocket` clients, with **horizontal
scaling via Redis**, pluggable persistence, and a familiar extension/hook system.

Built on [`yrs`](https://github.com/y-crdt/y-crdt) (the official Rust Yjs port),
[`tokio`](https://tokio.rs) and [`axum`](https://github.com/tokio-rs/axum).

## Why

The Node implementation is excellent but pays the Node tax: a multi-megabyte
runtime baseline, GC pauses, and single-threaded JS for CRDT merges. This port
keeps the protocol and ergonomics identical while running each document as its
own lightweight async task across all cores.

Head-to-head against the Node `@hocuspocus/server`, driven by the **identical
external WebSocket client** (Apple Silicon, release builds):

| Metric | hocuspocus-rs | Node hocuspocus | Rust advantage |
|---|---|---|---|
| Idle RSS | **4 MiB** | 63 MiB | **16× lower** |
| Peak RSS — 200 clients on 1 doc (max fan-out) | **59 MiB** | 107 MiB | 1.8× lower |
| Peak RSS — 200 clients / 20 docs | **61 MiB** | 87 MiB | 1.4× lower |
| Send throughput | **245K updates/s** | 36K updates/s | **6.8×** |
| Broadcast throughput | **1.22M frames/s** | 95K frames/s | **12.9×** |
| CRDT merge | native `yrs`, no GC | JS + GC pauses | |
| Concurrency | per-document tasks across all cores | single event loop | |
| Wire protocol | identical | identical | |

Rust is lower memory in *every* regime (idle and under load) and 7–13× higher
throughput.

## Drop-in compatibility (verified)

The real, unmodified `@hocuspocus/provider` from npm syncs against this server:

```
provider A synced
provider B synced
PASS: provider B received: "hello from the TypeScript provider"
```

The wire protocol is implemented byte-for-byte from the TypeScript source: the
`varString(documentName) varUint(messageType) …` envelope, the
`Sync`/`Awareness`/`Auth`/`QueryAwareness`/`SyncReply`/`Stateless`/`SyncStatus`/`Close`
message types, the `AuthMessageType` handshake, session-aware routing keys
(`documentName\0sessionId`), and the lib0 variable-length encoding (provided by
`yrs`, itself a faithful port of `y-protocols`).

## Quick start

```rust
use std::sync::Arc;
use hocuspocus::Hocuspocus;
use hocuspocus::extensions::sqlite::SqliteStorage;
use hocuspocus::extensions::logger::Logger;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let server = Hocuspocus::builder()
        .port(1234)
        .extension(Arc::new(SqliteStorage::open("data.sqlite")?.extension()))
        .extension(Arc::new(Logger::new()))
        .build();
    server.listen().await
}
```

```bash
cargo run --release --example server
# point any @hocuspocus/provider or y-websocket client at ws://127.0.0.1:1234
```

## Horizontal scaling

Run any number of nodes behind a load balancer, all sharing one Redis:

```rust
use hocuspocus::extensions::redis::{RedisConfig, RedisScaling};

let redis = RedisScaling::connect(RedisConfig::new("redis://127.0.0.1:6379")).await?;
let server = Hocuspocus::builder()
    .extension(redis)                                  // priority 1000: replicates + locks
    .extension(Arc::new(SqliteStorage::open("data.sqlite")?.extension()))
    .build();
```

Each node publishes updates, awareness and stateless broadcasts to a
per-document Redis channel, prefixed with a unique node identifier so it ignores
its own echoes; persistence is guarded by a Redis `SET NX PX` lock so two nodes
never write the same document concurrently. An integration test
(`tests/redis_scaling.rs`) proves an edit on node A reaches a client on node B.

## Architecture

- **Per-document actor** (`document.rs`): each loaded document is a Tokio task
  that owns its `yrs` `Awareness`/`Doc` and its connections. All mutations for a
  document are sequential (matching Hocuspocus semantics) while distinct
  documents run concurrently across the runtime. No global document lock.
- **Connection layer** (`server.rs`): axum WebSocket upgrade → per-socket reader
  loop + writer task. Handles the auth handshake, pre-auth message queueing with
  size/count limits, session-aware multiplexing (multiple documents per socket),
  idle timeout, and document routing.
- **Extension/hook system** (`extension.rs`): an async `Extension` trait with the
  full lifecycle hook surface, run in descending `priority()` order.
- **Debounced persistence** (`document.rs`): `debounce` + `max_debounce`
  coalescing, store-on-last-disconnect, and document unload to reclaim memory.

## Feature parity

| Capability | Status |
|---|---|
| Yjs sync protocol (SyncStep1/2, Update) | ✅ |
| Awareness (presence) relay + cleanup on disconnect | ✅ |
| Auth handshake (token, permission-denied, read-only) | ✅ |
| Stateless / QueryAwareness / SyncStatus / Close | ✅ |
| Session-aware multiplexing (`doc\0sessionId`) | ✅ |
| Pre-auth queue limits & idle timeout | ✅ |
| Lifecycle hooks (connect, authenticate, load, change, store, awareness, disconnect, unload, …) | ✅ |
| Debounced store + `maxDebounce` + unload/reload | ✅ |
| Generic `Storage` trait + SQLite backend (same schema) | ✅ |
| Redis horizontal scaling (pub/sub + lock) | ✅ |
| Webhook (HMAC-SHA256 signed events) | ✅\* |
| Throttle (per-IP sliding window) | ✅ |
| Logger | ✅ |
| Programmatic edits (direct connection) | ✅ (`ServerShared::direct_apply`) |
| S3 backend | via the `Storage` trait (not bundled) |
| Webhook Yjs↔app transformer | sends hex update (no server-side transformer) |
| `onUpgrade` / `onRequest` HTTP hooks | partial (welcome response) |

\* The webhook `change` event carries the hex-encoded Yjs update rather than a
transformed document tree, since there is no server-side transformer.

## Benchmarks

Apple Silicon, `--release`. Micro-benchmarks (`cargo bench`):

| Benchmark | Result |
|---|---|
| Wire frame encode | ~24 ns/frame (**41 M frames/s**) |
| Wire frame decode | ~44 ns/frame (**22 M frames/s**) |
| Apply 1000 incremental updates (1 doc) | ~1.5 ms (**~660 K updates/s**) |

Two design choices keep memory flat under heavy broadcast load:

- **Shared broadcast frames.** Each update is encoded once into a refcounted
  `Bytes` and shared across all recipients (one allocation per fan-out, not one
  per client). This alone cut peak RSS under a 200-client single-document blast
  from ~300 MiB to ~59 MiB.
- **Bounded per-client queues + slow-client eviction.** A client that falls
  `outbound_capacity` frames behind is dropped and reconnects/resyncs (Yjs is
  built for this), so one slow consumer can't balloon server memory.

Documents unload and memory is reclaimed once the last client disconnects
(`resident docs: 0`).

```bash
# server-only memory: run a server, then hammer it from a separate process
cargo run --release --example bench_server &
HP_ADDR=127.0.0.1:1234 cargo run --release --example loadtest -- 200 100 20
```

## Cargo features

- `sqlite` (default) — SQLite persistence via `rusqlite` (bundled).
- `redis-scaling` — Redis horizontal scaling.
- `webhook` — HMAC-signed webhooks via `reqwest`.
- `full` — all of the above.

## Tests

```bash
cargo test --features full           # unit + protocol + persistence
redis-server --daemonize yes         # for the scaling test
cargo test --features redis-scaling --test redis_scaling
```

## License

MIT
