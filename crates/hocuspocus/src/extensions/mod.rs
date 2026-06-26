//! Bundled extensions: persistence backends, logging, throttling, webhooks and
//! Redis-based horizontal scaling. Each mirrors the equivalent
//! `@hocuspocus/extension-*` package.

pub mod database;
pub mod logger;
pub mod throttle;

#[cfg(feature = "sqlite")]
pub mod sqlite;

#[cfg(feature = "webhook")]
pub mod webhook;

#[cfg(feature = "redis-scaling")]
pub mod redis;

pub use database::{Database, Storage};
pub use logger::Logger;
pub use throttle::Throttle;
