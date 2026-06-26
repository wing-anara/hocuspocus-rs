//! Server configuration, mirroring the defaults in `Hocuspocus.ts` and
//! `Server.ts`.

use std::time::Duration;

/// Configuration for a [`crate::Hocuspocus`] server instance. Construct with
/// [`Configuration::default`] and override fields, or use [`crate::ServerBuilder`].
#[derive(Clone)]
pub struct Configuration {
    /// Optional instance name (used in logs).
    pub name: Option<String>,
    /// Address to bind (default `0.0.0.0`).
    pub address: String,
    /// Port to bind (default `80`).
    pub port: u16,
    /// Idle timeout: close a connection after this long with no messages.
    pub timeout: Duration,
    /// Debounce window before persisting a changed document.
    pub debounce: Duration,
    /// Hard upper bound on how long persistence can be debounced.
    pub max_debounce: Duration,
    /// When the last connection closes, persist + unload immediately rather
    /// than keeping the document warm.
    pub unload_immediately: bool,
    /// Quiet mode: suppress the startup banner.
    pub quiet: bool,
    /// Max bytes buffered from a single connection before it authenticates.
    pub max_unauthenticated_queue_size: usize,
    /// Max messages buffered from a single connection before it authenticates.
    pub max_unauthenticated_queue_messages: usize,
    /// Max distinct documents a connection may address before authenticating.
    pub max_pending_documents: usize,
    /// Enable Yjs garbage collection on documents.
    pub gc: bool,
    /// Per-connection outbound queue depth (frames). A client that falls this
    /// far behind is evicted (and reconnects/resyncs), bounding server memory
    /// under broadcast load.
    pub outbound_capacity: usize,
}

impl Default for Configuration {
    fn default() -> Self {
        Self {
            name: None,
            address: "0.0.0.0".to_string(),
            port: 80,
            timeout: Duration::from_secs(60),
            debounce: Duration::from_secs(2),
            max_debounce: Duration::from_secs(10),
            unload_immediately: true,
            quiet: false,
            max_unauthenticated_queue_size: 5 * 1024 * 1024,
            max_unauthenticated_queue_messages: 1_000,
            max_pending_documents: 100,
            gc: true,
            outbound_capacity: 2048,
        }
    }
}
