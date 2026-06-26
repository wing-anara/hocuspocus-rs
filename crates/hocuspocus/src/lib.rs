//! # Hocuspocus (Rust)
//!
//! An idiomatic, high-performance Rust reimplementation of the
//! [Hocuspocus](https://hocuspocus.dev) collaboration server. It speaks the
//! exact same WebSocket wire protocol as the TypeScript server, so it is a
//! drop-in replacement for `@hocuspocus/provider` and `y-websocket` clients,
//! with horizontal scaling via Redis and pluggable persistence.
//!
//! ## Quick start
//!
//! ```no_run
//! use hocuspocus::{Hocuspocus, Configuration};
//!
//! #[tokio::main]
//! async fn main() -> anyhow::Result<()> {
//!     let server = Hocuspocus::builder().port(1234).build();
//!     server.listen().await
//! }
//! ```

pub mod config;
pub mod context;
pub mod document;
pub mod error;
pub mod extension;
pub mod origin;
pub mod protocol;
pub mod server;

pub mod extensions;

pub use config::Configuration;
pub use context::{Context, RequestMeta};
pub use error::{CloseEvent, HookError, HookResult};
pub use extension::{
    AuthenticatePayload, AwarenessPayload, ChangePayload, ConnectionConfig, ConnectionPayload,
    DocumentPayload, Extension, StatelessPayload,
};
pub use origin::Origin;
pub use server::{Hocuspocus, ServerBuilder, ServerShared};

// Re-export yrs so downstream extensions/users share the exact same types.
pub use yrs;
