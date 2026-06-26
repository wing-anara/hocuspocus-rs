//! Logging extension, mirroring `@hocuspocus/extension-logger`.

use async_trait::async_trait;
use tracing::info;

use crate::error::HookResult;
use crate::extension::{
    ChangePayload, ConnectionPayload, DocumentPayload, Extension,
};
use yrs::Doc;

/// Logs document lifecycle events via `tracing`.
#[derive(Default)]
pub struct Logger {
    prefix: Option<String>,
}

impl Logger {
    pub fn new() -> Self {
        Self::default()
    }
    pub fn with_prefix(prefix: impl Into<String>) -> Self {
        Self {
            prefix: Some(prefix.into()),
        }
    }
    fn tag(&self) -> &str {
        self.prefix.as_deref().unwrap_or("hocuspocus")
    }
}

#[async_trait]
impl Extension for Logger {
    fn name(&self) -> &str {
        "logger"
    }

    async fn on_connect(&self, payload: &ConnectionPayload<'_>) -> HookResult {
        info!(target: "hocuspocus", "[{}] New connection to \"{}\".", self.tag(), payload.document_name);
        Ok(())
    }

    async fn on_load_document(&self, _doc: &Doc, payload: &DocumentPayload<'_>) -> HookResult {
        info!(target: "hocuspocus", "[{}] Loaded document \"{}\".", self.tag(), payload.document_name);
        Ok(())
    }

    async fn on_change(&self, payload: &ChangePayload<'_>) -> HookResult {
        info!(target: "hocuspocus", "[{}] Document \"{}\" changed.", self.tag(), payload.document_name);
        Ok(())
    }

    async fn on_store_document(&self, _state: &[u8], payload: &DocumentPayload<'_>) -> HookResult {
        info!(target: "hocuspocus", "[{}] Store \"{}\".", self.tag(), payload.document_name);
        Ok(())
    }

    async fn on_disconnect(&self, payload: &ConnectionPayload<'_>) -> HookResult {
        info!(target: "hocuspocus", "[{}] Connection to \"{}\" closed.", self.tag(), payload.document_name);
        Ok(())
    }
}
