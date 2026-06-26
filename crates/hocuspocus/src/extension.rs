//! The extension/hook system. Mirrors Hocuspocus's lifecycle hooks.
//!
//! Implement [`Extension`] and register it with the server. Every method has a
//! default no-op implementation, so an extension only overrides the hooks it
//! cares about. Hooks run sequentially in descending `priority()` order
//! (default `100`), exactly like the TypeScript server.

use std::sync::atomic::{AtomicBool, Ordering};

use async_trait::async_trait;
use yrs::Doc;

use crate::context::{Context, RequestMeta};
use crate::error::{HookError, HookResult};
use crate::origin::Origin;

/// Per-connection authorization state, mutable by `on_authenticate`.
pub struct ConnectionConfig {
    read_only: AtomicBool,
    authenticated: AtomicBool,
}

impl Default for ConnectionConfig {
    fn default() -> Self {
        Self {
            read_only: AtomicBool::new(false),
            authenticated: AtomicBool::new(false),
        }
    }
}

impl ConnectionConfig {
    pub fn set_read_only(&self, value: bool) {
        self.read_only.store(value, Ordering::SeqCst);
    }
    pub fn read_only(&self) -> bool {
        self.read_only.load(Ordering::SeqCst)
    }
    pub(crate) fn set_authenticated(&self, value: bool) {
        self.authenticated.store(value, Ordering::SeqCst);
    }
    pub fn is_authenticated(&self) -> bool {
        self.authenticated.load(Ordering::SeqCst)
    }
}

/// Shared payload fields present on most connection-scoped hooks.
pub struct ConnectionPayload<'a> {
    pub document_name: &'a str,
    pub socket_id: &'a str,
    pub context: &'a Context,
    pub request: &'a RequestMeta,
    pub connection_config: &'a ConnectionConfig,
    pub provider_version: Option<&'a str>,
}

/// Payload for `on_authenticate`, adding the submitted token.
pub struct AuthenticatePayload<'a> {
    pub token: &'a str,
    pub document_name: &'a str,
    pub socket_id: &'a str,
    pub context: &'a Context,
    pub request: &'a RequestMeta,
    pub connection_config: &'a ConnectionConfig,
}

/// Document-scoped payload (load/store/change/unload hooks).
pub struct DocumentPayload<'a> {
    pub document_name: &'a str,
    /// Number of active connections at the time the hook fires.
    pub clients_count: usize,
}

/// Payload for change hooks, carrying the applied update and its origin.
pub struct ChangePayload<'a> {
    pub document_name: &'a str,
    pub update: &'a [u8],
    pub origin: Origin,
    pub clients_count: usize,
}

/// Payload for awareness hooks.
pub struct AwarenessPayload<'a> {
    pub document_name: &'a str,
    /// Encoded awareness update for the changed clients.
    pub update: &'a [u8],
    pub added: &'a [u64],
    pub updated: &'a [u64],
    pub removed: &'a [u64],
    pub origin: Origin,
}

/// Payload for stateless messages received from a client.
pub struct StatelessPayload<'a> {
    pub document_name: &'a str,
    pub socket_id: &'a str,
    pub payload: &'a str,
}

/// The lifecycle hook surface. Methods return [`HookResult`]; returning `Err`
/// rejects the operation (and, for connection hooks, closes the socket).
#[async_trait]
pub trait Extension: Send + Sync + 'static {
    /// Higher runs first. Default `100`.
    fn priority(&self) -> i32 {
        100
    }

    /// Human-readable name (for logging/diagnostics).
    fn name(&self) -> &str {
        "extension"
    }

    /// Server configured. Fired once at startup.
    async fn on_configure(&self) -> HookResult {
        Ok(())
    }

    /// Fired once at startup with a handle to the shared server state. Used by
    /// extensions that need to reach the document registry out of band (e.g.
    /// the Redis subscriber pushing replicated updates into document actors).
    async fn on_server_ready(&self, _server: std::sync::Arc<crate::server::ServerShared>) {}

    /// Server is listening on its socket.
    async fn on_listen(&self, _port: u16) -> HookResult {
        Ok(())
    }

    /// A WebSocket connected (before authentication).
    async fn on_connect(&self, _payload: &ConnectionPayload<'_>) -> HookResult {
        Ok(())
    }

    /// Authenticate a connection. Return `Err(HookError::forbidden())` to deny;
    /// mutate `payload.connection_config` to grant read-only access.
    async fn on_authenticate(&self, _payload: &AuthenticatePayload<'_>) -> HookResult {
        Ok(())
    }

    /// Load a document's persisted state. Apply fetched updates onto `doc`
    /// (e.g. `doc.transact_mut().apply_update(...)`). Multiple extensions may
    /// contribute.
    async fn on_load_document(
        &self,
        _doc: &Doc,
        _payload: &DocumentPayload<'_>,
    ) -> HookResult {
        Ok(())
    }

    /// Fired after all `on_load_document` hooks complete.
    async fn after_load_document(
        &self,
        _doc: &Doc,
        _payload: &DocumentPayload<'_>,
    ) -> HookResult {
        Ok(())
    }

    /// Fired before an inbound update is applied. Return `Err` to reject.
    async fn before_handle_message(&self, _payload: &ChangePayload<'_>) -> HookResult {
        Ok(())
    }

    /// Document content changed.
    async fn on_change(&self, _payload: &ChangePayload<'_>) -> HookResult {
        Ok(())
    }

    /// Persist a document. `state` is the full Yjs state as a v1 update.
    async fn on_store_document(
        &self,
        _state: &[u8],
        _payload: &DocumentPayload<'_>,
    ) -> HookResult {
        Ok(())
    }

    /// Fired after `on_store_document` completes.
    async fn after_store_document(&self, _payload: &DocumentPayload<'_>) -> HookResult {
        Ok(())
    }

    /// Awareness state changed.
    async fn on_awareness_update(&self, _payload: &AwarenessPayload<'_>) -> HookResult {
        Ok(())
    }

    /// Fired before a stateless payload is broadcast to connections.
    async fn before_broadcast_stateless(
        &self,
        _document_name: &str,
        _payload: &str,
    ) -> HookResult {
        Ok(())
    }

    /// A stateless message was received from a client.
    async fn on_stateless(&self, _payload: &StatelessPayload<'_>) -> HookResult {
        Ok(())
    }

    /// A connection closed.
    async fn on_disconnect(&self, _payload: &ConnectionPayload<'_>) -> HookResult {
        Ok(())
    }

    /// A document is about to be unloaded from memory. Return `Err` to abort.
    async fn before_unload_document(&self, _payload: &DocumentPayload<'_>) -> HookResult {
        Ok(())
    }

    /// A document was removed from memory.
    async fn after_unload_document(&self, _document_name: &str) -> HookResult {
        Ok(())
    }

    /// Server shutting down.
    async fn on_destroy(&self) -> HookResult {
        Ok(())
    }
}

/// Helper so extensions can early-return a forbidden error tersely.
pub fn deny(reason: impl Into<String>) -> HookError {
    HookError {
        message: reason.into(),
        close: Some(crate::error::FORBIDDEN),
    }
}
