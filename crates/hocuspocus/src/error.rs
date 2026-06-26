//! Error types and WebSocket close codes, mirroring
//! `packages/common/src/CloseEvents.ts`.

/// A WebSocket close code + reason pair.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CloseEvent {
    pub code: u16,
    pub reason: &'static str,
}

impl CloseEvent {
    pub const fn new(code: u16, reason: &'static str) -> Self {
        Self { code, reason }
    }
}

/// Frame too large.
pub const MESSAGE_TOO_BIG: CloseEvent = CloseEvent::new(1009, "Message Too Big");
/// Ask the client to reset its document view.
pub const RESET_CONNECTION: CloseEvent = CloseEvent::new(4205, "Reset Connection");
/// Authentication required and missing/invalid.
pub const UNAUTHORIZED: CloseEvent = CloseEvent::new(4401, "Unauthorized");
/// Understood but refused (failed `onAuthenticate`).
pub const FORBIDDEN: CloseEvent = CloseEvent::new(4403, "Forbidden");
/// Timed out waiting for the client.
pub const CONNECTION_TIMEOUT: CloseEvent = CloseEvent::new(4408, "Connection Timeout");

/// Error returned by lifecycle hooks. A hook may reject a connection or
/// operation; the `close` field, when set, controls the WebSocket close frame.
#[derive(Debug, thiserror::Error)]
#[error("{message}")]
pub struct HookError {
    pub message: String,
    pub close: Option<CloseEvent>,
}

impl HookError {
    pub fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
            close: None,
        }
    }

    /// A rejection that should close the socket with the given event.
    pub fn closing(close: CloseEvent) -> Self {
        Self {
            message: close.reason.to_string(),
            close: Some(close),
        }
    }

    /// Convenience: deny authentication (4403 Forbidden).
    pub fn forbidden() -> Self {
        Self::closing(FORBIDDEN)
    }

    /// Convenience: unauthorized (4401).
    pub fn unauthorized() -> Self {
        Self::closing(UNAUTHORIZED)
    }
}

impl From<anyhow::Error> for HookError {
    fn from(e: anyhow::Error) -> Self {
        HookError::new(e.to_string())
    }
}

pub type HookResult = Result<(), HookError>;

/// Sentinel that, when returned from a store hook, tells the server to stop
/// running later store hooks and retry later (used by the Redis extension when
/// another node holds the document lock).
#[derive(Debug, thiserror::Error)]
#[error("skip further hooks")]
pub struct SkipFurtherHooks;
