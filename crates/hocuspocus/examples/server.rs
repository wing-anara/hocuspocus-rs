//! A minimal Hocuspocus server with SQLite persistence and logging.
//!
//! Run with: `cargo run --release --example server`
//! Then point any `@hocuspocus/provider` or `y-websocket` client at
//! `ws://127.0.0.1:1234`.

use std::sync::Arc;

use hocuspocus::extensions::logger::Logger;
use hocuspocus::extensions::sqlite::SqliteStorage;
use hocuspocus::Hocuspocus;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "info".into()),
        )
        .init();

    let storage = SqliteStorage::open("hocuspocus.sqlite")?;

    let server = Hocuspocus::builder()
        .port(1234)
        .address("127.0.0.1")
        .extension(Arc::new(storage.extension()))
        .extension(Arc::new(Logger::new()))
        .build();

    server.listen().await
}
