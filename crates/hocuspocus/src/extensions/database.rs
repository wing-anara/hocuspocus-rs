//! Generic persistence extension, mirroring `@hocuspocus/extension-database`.
//!
//! Implement [`Storage`] for any backend (a SQL table, a KV store, an object
//! store) and wrap it in [`Database`] to get `on_load_document` /
//! `on_store_document` wired up. Documents are persisted as Yjs v1 state
//! updates (`encode_state_as_update_v1`).

use async_trait::async_trait;
use std::sync::Arc;
use yrs::updates::decoder::Decode;
use yrs::{Doc, Transact, Update};

use crate::error::{HookError, HookResult};
use crate::extension::{DocumentPayload, Extension};

/// A pluggable document store. `fetch` returns the persisted Yjs state (a v1
/// update) or `None` if the document is new; `store` persists the given state.
#[async_trait]
pub trait Storage: Send + Sync + 'static {
    async fn fetch(&self, document_name: &str) -> anyhow::Result<Option<Vec<u8>>>;
    async fn store(&self, document_name: &str, state: &[u8]) -> anyhow::Result<()>;
}

/// Extension adapter that wires a [`Storage`] backend into the document
/// lifecycle.
pub struct Database<S: Storage> {
    storage: Arc<S>,
}

impl<S: Storage> Database<S> {
    pub fn new(storage: S) -> Self {
        Self {
            storage: Arc::new(storage),
        }
    }

    pub fn from_arc(storage: Arc<S>) -> Self {
        Self { storage }
    }
}

#[async_trait]
impl<S: Storage> Extension for Database<S> {
    fn name(&self) -> &str {
        "database"
    }

    async fn on_load_document(&self, doc: &Doc, payload: &DocumentPayload<'_>) -> HookResult {
        match self.storage.fetch(payload.document_name).await {
            Ok(Some(state)) => {
                let update = Update::decode_v1(&state)
                    .map_err(|e| HookError::new(format!("invalid stored update: {e}")))?;
                let mut txn = doc.transact_mut();
                txn.apply_update(update)
                    .map_err(|e| HookError::new(format!("apply stored update: {e}")))?;
                Ok(())
            }
            Ok(None) => Ok(()),
            Err(e) => Err(HookError::new(e.to_string())),
        }
    }

    async fn on_store_document(&self, state: &[u8], payload: &DocumentPayload<'_>) -> HookResult {
        self.storage
            .store(payload.document_name, state)
            .await
            .map_err(|e| HookError::new(e.to_string()))
    }
}

/// A simple in-memory [`Storage`] backend, useful for tests and ephemeral
/// deployments.
#[derive(Default, Clone)]
pub struct MemoryStorage {
    inner: Arc<dashmap::DashMap<String, Vec<u8>>>,
}

impl MemoryStorage {
    pub fn new() -> Self {
        Self::default()
    }
}

#[async_trait]
impl Storage for MemoryStorage {
    async fn fetch(&self, document_name: &str) -> anyhow::Result<Option<Vec<u8>>> {
        Ok(self.inner.get(document_name).map(|v| v.clone()))
    }
    async fn store(&self, document_name: &str, state: &[u8]) -> anyhow::Result<()> {
        self.inner.insert(document_name.to_string(), state.to_vec());
        Ok(())
    }
}
