//! Per-connection request context shared across hooks.
//!
//! In Hocuspocus the `context` object is an arbitrary, mutable bag that hooks
//! enrich (e.g. `onAuthenticate` attaches the resolved user). We model it as a
//! thread-safe typed-erased map of JSON values plus the request metadata that
//! hooks read (headers and query parameters).

use std::collections::HashMap;
use std::sync::Arc;

use parking_lot::RwLock;
use serde_json::Value;

/// A mutable, shareable bag of values attached to a connection and passed to
/// every hook for that connection.
#[derive(Clone, Default)]
pub struct Context {
    inner: Arc<RwLock<HashMap<String, Value>>>,
}

impl Context {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn set(&self, key: impl Into<String>, value: Value) {
        self.inner.write().insert(key.into(), value);
    }

    pub fn get(&self, key: &str) -> Option<Value> {
        self.inner.read().get(key).cloned()
    }

    pub fn get_str(&self, key: &str) -> Option<String> {
        self.inner
            .read()
            .get(key)
            .and_then(|v| v.as_str().map(|s| s.to_string()))
    }
}

/// Immutable metadata about the HTTP request that opened a connection.
#[derive(Clone, Default)]
pub struct RequestMeta {
    /// Lower-cased header name -> value.
    pub headers: HashMap<String, String>,
    /// Query string parameters parsed from the connection URL.
    pub parameters: HashMap<String, String>,
    /// Remote address as seen by the server (or via `x-real-ip` /
    /// `x-forwarded-for` when behind a proxy).
    pub remote_ip: Option<String>,
}

impl RequestMeta {
    /// Resolve the client IP preferring proxy headers, matching the throttle
    /// extension's behaviour (`x-real-ip` then `x-forwarded-for`).
    pub fn client_ip(&self) -> Option<String> {
        self.headers
            .get("x-real-ip")
            .or_else(|| self.headers.get("x-forwarded-for"))
            .map(|s| s.split(',').next().unwrap_or(s).trim().to_string())
            .or_else(|| self.remote_ip.clone())
    }
}
