# hocuspocus-rs — Benchmarks

A Rust reimplementation of [Hocuspocus](https://hocuspocus.dev) (the Yjs
collaboration server), wire-compatible with `@hocuspocus/provider` and
`y-websocket` clients.

## Head-to-head vs. Node hocuspocus

Both servers driven by the **identical external WebSocket client** (Apple
Silicon, release builds):

| Metric | hocuspocus-rs | Node hocuspocus | Rust advantage |
|---|---|---|---|
| Idle RSS | **4 MiB** | 63 MiB | **16× lower** |
| Peak RSS — 200 clients on 1 doc (max fan-out) | **59 MiB** | 107 MiB | 1.8× lower |
| Peak RSS — 200 clients / 20 docs | **61 MiB** | 87 MiB | 1.4× lower |
| Send throughput | **245K updates/s** | 36K updates/s | **6.8×** |
| Broadcast throughput | **1.22M frames/s** | 95K frames/s | **12.9×** |

Rust is lower memory in *every* regime (idle and under load) and 7–13× higher
throughput.

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

## Micro-benchmarks

Apple Silicon, `--release` (`cargo bench`):

| Benchmark | Result |
|---|---|
| Wire frame encode | ~24 ns/frame (**41 M frames/s**) |
| Wire frame decode | ~44 ns/frame (**22 M frames/s**) |
| Apply 1000 incremental updates (1 doc) | ~1.5 ms (**~660 K updates/s**) |

## Reproducing

```bash
# micro-benchmarks
cargo bench

# end-to-end throughput + server memory (server in a separate process)
cargo run --release --example bench_server &
HP_ADDR=127.0.0.1:1234 cargo run --release --example loadtest -- 200 100 20
```
