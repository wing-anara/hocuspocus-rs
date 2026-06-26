//! A bare in-memory server for head-to-head benchmarking against a bare Node
//! `@hocuspocus/server` (no persistence, no logging). Binds 127.0.0.1:1234.
//!
//! Run: `cargo run --release --example bench_server`

use hocuspocus::Hocuspocus;

#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let server = Hocuspocus::builder()
        .port(1234)
        .address("127.0.0.1")
        .build(); // no extensions: documents live purely in memory
    server.listen().await
}
