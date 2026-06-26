//! The per-document actor.
//!
//! Each loaded document runs as its own Tokio task that owns the `yrs`
//! [`Awareness`] (and therefore the `Doc`) plus the set of connected clients.
//! All mutations for a document are funnelled through this single task, which
//! makes processing sequential per document — matching Hocuspocus's
//! single-threaded-per-document semantics — while distinct documents run
//! concurrently across the runtime.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Instant;

use bytes::Bytes;
use tokio::sync::{mpsc, oneshot};
use tracing::{debug, warn};
use yrs::sync::{Awareness, AwarenessUpdate};
use yrs::updates::decoder::Decode;
use yrs::updates::encoder::{Encode, Encoder, EncoderV1};
use yrs::{ClientID, Doc, Options, ReadTxn, StateVector, Transact, Update};

use crate::context::{Context, RequestMeta};
use crate::error::CloseEvent;
use crate::extension::{
    AwarenessPayload, ChangePayload, ConnectionConfig, ConnectionPayload, DocumentPayload,
    StatelessPayload,
};
use crate::origin::Origin;
use crate::protocol::{
    AuthMessageType, IncomingFrame, MessageType, OutgoingMessage, SYNC_STEP_1, SYNC_STEP_2,
    SYNC_UPDATE,
};
use crate::server::ServerShared;

/// A message destined for a single client's WebSocket writer task. Frames are
/// reference-counted [`Bytes`] so one encoded broadcast frame can be cheaply
/// shared (refcount bump) across every recipient instead of re-allocated per
/// connection.
pub enum OutMsg {
    Frame(Bytes),
    Close(CloseEvent),
}

/// Server-side handle to one connected client within a document.
pub struct ConnHandle {
    pub conn_id: u64,
    pub outbound: mpsc::Sender<OutMsg>,
    /// Address prefix for frames sent to this client (`doc` or `doc\0sid`).
    pub message_address: String,
    pub read_only: bool,
    pub socket_id: String,
    pub session_id: Option<String>,
    pub context: Context,
    pub request: Arc<RequestMeta>,
    pub connection_config: Arc<ConnectionConfig>,
    pub provider_version: Option<String>,
    /// Awareness client ids this connection is responsible for (cleaned up on
    /// disconnect, mirroring `removeAwarenessStates`).
    pub client_ids: HashSet<ClientID>,
}

impl ConnHandle {
    /// Try to enqueue a frame. Returns `false` if the client's bounded outbound
    /// queue is full (it has fallen too far behind) — the caller evicts it.
    fn send_frame(&self, frame: Bytes) -> bool {
        !matches!(
            self.outbound.try_send(OutMsg::Frame(frame)),
            Err(mpsc::error::TrySendError::Full(_))
        )
    }
    fn send_close(&self, event: CloseEvent) {
        let _ = self.outbound.try_send(OutMsg::Close(event));
    }
}

/// Commands accepted by a running document actor.
pub enum DocCommand {
    /// Register a newly authenticated connection.
    AddConnection {
        handle: Box<ConnHandle>,
        ack: oneshot::Sender<()>,
    },
    /// A raw protocol frame received from a connected client.
    ClientFrame { conn_id: u64, data: Vec<u8> },
    /// A client (or its socket) went away.
    RemoveConnection { conn_id: u64 },
    /// A frame replicated from a peer node (Redis). `reply`, if present,
    /// receives any response frames to publish back to the cluster.
    ExternalFrame {
        data: Vec<u8>,
        reply: Option<mpsc::UnboundedSender<Vec<u8>>>,
    },
    /// Apply a programmatic edit (direct connection) and report completion.
    DirectApply {
        update: Vec<u8>,
        ack: oneshot::Sender<()>,
    },
}

/// Cloneable handle used to talk to a document actor.
#[derive(Clone)]
pub struct DocumentHandle {
    pub name: String,
    pub tx: mpsc::UnboundedSender<DocCommand>,
}

fn encode_sync(msg: &yrs::sync::SyncMessage) -> Vec<u8> {
    let mut e = EncoderV1::new();
    msg.encode(&mut e);
    e.to_vec()
}

/// The actor state.
pub struct Document {
    name: String,
    awareness: Awareness,
    connections: HashMap<u64, ConnHandle>,
    server: Arc<ServerShared>,
    rx: mpsc::UnboundedReceiver<DocCommand>,

    // store/debounce bookkeeping
    dirty: bool,
    first_dirty_at: Option<Instant>,
    last_change_at: Option<Instant>,
    last_origin: Origin,
}

impl Document {
    /// Spawn and load a new document actor, returning its handle. Runs the
    /// `on_load_document`/`after_load_document` hooks before serving.
    pub async fn spawn(
        name: String,
        server: Arc<ServerShared>,
    ) -> anyhow::Result<DocumentHandle> {
        let mut options = Options::default();
        options.skip_gc = !server.config.gc;
        let doc = Doc::with_options(options);

        // Run load hooks while we still uniquely own the doc.
        let payload = DocumentPayload {
            document_name: &name,
            clients_count: 0,
        };
        for ext in &server.extensions {
            if let Err(e) = ext.on_load_document(&doc, &payload).await {
                warn!(document = %name, ext = ext.name(), "on_load_document failed: {e}");
            }
        }
        for ext in &server.extensions {
            if let Err(e) = ext.after_load_document(&doc, &payload).await {
                warn!(document = %name, ext = ext.name(), "after_load_document failed: {e}");
            }
        }

        let awareness = Awareness::new(doc);
        let (tx, rx) = mpsc::unbounded_channel();
        let actor = Document {
            name: name.clone(),
            awareness,
            connections: HashMap::new(),
            server,
            rx,
            dirty: false,
            first_dirty_at: None,
            last_change_at: None,
            last_origin: Origin::Local,
        };
        tokio::spawn(actor.run());
        Ok(DocumentHandle { name, tx })
    }

    async fn run(mut self) {
        loop {
            let deadline = self.store_deadline();
            tokio::select! {
                cmd = self.rx.recv() => {
                    match cmd {
                        Some(cmd) => {
                            let exit = self.handle_command(cmd).await;
                            if exit { break; }
                        }
                        None => break,
                    }
                }
                _ = sleep_until_opt(deadline) => {
                    self.run_store().await;
                    if self.maybe_unload().await { break; }
                }
            }
        }
        debug!(document = %self.name, "document actor stopped");
    }

    fn store_deadline(&self) -> Option<Instant> {
        if !self.dirty {
            return None;
        }
        let debounce = self.server.config.debounce;
        let max_debounce = self.server.config.max_debounce;
        let soft = self.last_change_at? + debounce;
        let hard = self.first_dirty_at? + max_debounce;
        Some(soft.min(hard))
    }

    async fn handle_command(&mut self, cmd: DocCommand) -> bool {
        match cmd {
            DocCommand::AddConnection { handle, ack } => {
                self.add_connection(*handle).await;
                let _ = ack.send(());
                false
            }
            DocCommand::ClientFrame { conn_id, data } => {
                self.handle_client_frame(conn_id, data).await;
                false
            }
            DocCommand::RemoveConnection { conn_id } => {
                self.remove_connection(conn_id).await;
                self.maybe_unload().await
            }
            DocCommand::ExternalFrame { data, reply } => {
                self.handle_external_frame(data, reply).await;
                false
            }
            DocCommand::DirectApply { update, ack } => {
                self.apply_and_broadcast(&update, Origin::Local, None).await;
                let _ = ack.send(());
                false
            }
        }
    }

    async fn add_connection(&mut self, handle: ConnHandle) {
        // Send current awareness to the new client, if any states exist.
        if self.awareness_has_states() {
            if let Ok(update) = self.awareness.update() {
                let frame = OutgoingMessage::new(&handle.message_address)
                    .awareness_update(&update.encode_v1())
                    .into_bytes();
                handle.send_frame(frame.into());
            }
        }
        self.connections.insert(handle.conn_id, handle);
    }

    async fn remove_connection(&mut self, conn_id: u64) {
        let Some(handle) = self.connections.remove(&conn_id) else {
            return;
        };
        // Clear this connection's awareness states and broadcast the removal.
        let mut removed: Vec<ClientID> = Vec::new();
        for cid in &handle.client_ids {
            self.awareness.remove_state(*cid);
            removed.push(*cid);
        }
        if !removed.is_empty() {
            if let Ok(update) = self.awareness.update_with_clients(removed.iter().copied()) {
                let bytes = update.encode_v1();
                self.broadcast_awareness(&bytes);
            }
        }
        // on_disconnect hooks.
        let payload = ConnectionPayload {
            document_name: &self.name,
            socket_id: &handle.socket_id,
            context: &handle.context,
            request: &handle.request,
            connection_config: &handle.connection_config,
            provider_version: handle.provider_version.as_deref(),
        };
        for ext in &self.server.extensions {
            if let Err(e) = ext.on_disconnect(&payload).await {
                warn!(document = %self.name, "on_disconnect failed: {e}");
            }
        }
    }

    async fn handle_client_frame(&mut self, conn_id: u64, data: Vec<u8>) {
        if !self.connections.contains_key(&conn_id) {
            return;
        }
        let frame = match IncomingFrame::parse(&data) {
            Ok(f) => f,
            Err(_) => return,
        };
        let address = self.connections[&conn_id].message_address.clone();
        let origin = Origin::Connection(conn_id);
        match frame.message_type {
            MessageType::Sync | MessageType::SyncReply => {
                let request_first_sync = frame.message_type != MessageType::SyncReply;
                self.handle_sync(conn_id, &address, origin, frame, request_first_sync)
                    .await;
            }
            MessageType::Awareness => {
                self.handle_awareness(frame, origin).await;
            }
            MessageType::QueryAwareness => {
                self.reply_query_awareness(&address, conn_id);
            }
            MessageType::Stateless => {
                self.handle_stateless(conn_id, frame).await;
            }
            MessageType::Close => {
                self.close_connection(conn_id, CloseEvent::new(1000, "provider_initiated"))
                    .await;
            }
            MessageType::Auth => {
                // Token re-sync on an already-authenticated connection: re-run
                // authentication hooks with the supplied token.
                self.handle_token_sync(conn_id, frame).await;
            }
            MessageType::BroadcastStateless => {
                // Illegal from a client; close the connection.
                self.close_connection(conn_id, crate::error::RESET_CONNECTION)
                    .await;
            }
            MessageType::SyncStatus | MessageType::Ping | MessageType::Pong => {}
        }
    }

    async fn handle_sync(
        &mut self,
        conn_id: u64,
        address: &str,
        origin: Origin,
        mut frame: IncomingFrame<'_>,
        _request_first_sync: bool,
    ) {
        let sync = match yrs::sync::SyncMessage::decode(&mut frame.decoder) {
            Ok(m) => m,
            Err(_) => return,
        };
        let read_only = self.connections[&conn_id].read_only;

        // before_handle_message / on_change semantics handled inside apply.
        match sync {
            yrs::sync::SyncMessage::SyncStep1(client_sv) => {
                let (step2, server_sv) = {
                    let doc = self.awareness.doc();
                    let txn = doc.transact();
                    (txn.encode_diff_v1(&client_sv), txn.state_vector())
                };
                let conn = &self.connections[&conn_id];
                conn.send_frame(
                    OutgoingMessage::new(address)
                        .sync()
                        .write_sync_payload(&encode_sync(&yrs::sync::SyncMessage::SyncStep2(step2)))
                        .into_bytes().into(),
                );
                conn.send_frame(
                    OutgoingMessage::new(address)
                        .sync()
                        .write_sync_payload(&encode_sync(&yrs::sync::SyncMessage::SyncStep1(
                            server_sv,
                        )))
                        .into_bytes().into(),
                );
            }
            yrs::sync::SyncMessage::SyncStep2(update)
            | yrs::sync::SyncMessage::Update(update) => {
                if read_only {
                    self.send_sync_status(conn_id, address, false);
                    return;
                }
                if !self.before_handle_message(conn_id, &update, origin).await {
                    return;
                }
                self.apply_and_broadcast(&update, origin, None).await;
                self.send_sync_status(conn_id, address, true);
            }
        }
    }

    fn send_sync_status(&self, conn_id: u64, address: &str, saved: bool) {
        if let Some(conn) = self.connections.get(&conn_id) {
            conn.send_frame(
                OutgoingMessage::new(address)
                    .sync_status(saved)
                    .into_bytes().into(),
            );
        }
    }

    /// Runs `before_handle_message` hooks; returns false (and closes the
    /// connection) if any reject.
    async fn before_handle_message(&mut self, conn_id: u64, update: &[u8], origin: Origin) -> bool {
        let payload = ChangePayload {
            document_name: &self.name,
            update,
            origin,
            clients_count: self.connections.len(),
        };
        for ext in &self.server.extensions {
            if let Err(e) = ext.before_handle_message(&payload).await {
                let close = e.close.unwrap_or(crate::error::RESET_CONNECTION);
                if let Some(conn) = self.connections.get(&conn_id) {
                    conn.send_close(close);
                }
                return false;
            }
        }
        true
    }

    /// Apply an update to the document and broadcast the resulting change to
    /// all connections, then fire `on_change` and schedule persistence.
    async fn apply_and_broadcast(&mut self, update: &[u8], origin: Origin, _exclude: Option<u64>) {
        let computed = {
            let doc = self.awareness.doc();
            let mut txn = doc.transact_mut_with(yrs::Origin::from(origin));
            match Update::decode_v1(update) {
                Ok(u) => {
                    if let Err(e) = txn.apply_update(u) {
                        warn!(document = %self.name, "failed to apply update: {e}");
                        return;
                    }
                }
                Err(e) => {
                    warn!(document = %self.name, "failed to decode update: {e}");
                    return;
                }
            }
            txn.encode_update_v1()
        };
        if computed.is_empty() {
            return;
        }
        // Broadcast to every connection (clients dedupe their own echo). The
        // frame is built once and shared by refcount across recipients.
        let body = encode_sync(&yrs::sync::SyncMessage::Update(computed.clone()));
        self.broadcast_with(|addr| {
            OutgoingMessage::new(addr)
                .sync()
                .write_sync_payload(&body)
                .into_bytes()
        });
        self.mark_dirty(origin);
        // on_change hooks (errors are non-fatal).
        let payload = ChangePayload {
            document_name: &self.name,
            update: &computed,
            origin,
            clients_count: self.connections.len(),
        };
        for ext in &self.server.extensions {
            if let Err(e) = ext.on_change(&payload).await {
                warn!(document = %self.name, "on_change failed: {e}");
            }
        }
    }

    async fn handle_awareness(&mut self, mut frame: IncomingFrame<'_>, origin: Origin) {
        use yrs::encoding::read::Read;
        let buf = match frame.decoder.read_buf() {
            Ok(b) => b.to_vec(),
            Err(_) => return,
        };
        let update = match AwarenessUpdate::decode_v1(&buf) {
            Ok(u) => u,
            Err(_) => return,
        };
        let changed: Vec<ClientID> = update.clients.keys().copied().collect();
        if let Err(e) = self.awareness.apply_update_with(update, yrs::Origin::from(origin)) {
            warn!(document = %self.name, "failed to apply awareness: {e}");
            return;
        }
        // Track client ids against the originating connection for cleanup.
        if let Origin::Connection(conn_id) = origin {
            if let Some(conn) = self.connections.get_mut(&conn_id) {
                for cid in &changed {
                    conn.client_ids.insert(*cid);
                }
            }
        }
        // Re-encode changed clients and broadcast.
        if let Ok(out) = self.awareness.update_with_clients(changed.iter().copied()) {
            let bytes = out.encode_v1();
            self.broadcast_awareness(&bytes);
            let ids: Vec<u64> = changed.iter().map(|c| c.get()).collect();
            let payload = AwarenessPayload {
                document_name: &self.name,
                update: &bytes,
                added: &ids,
                updated: &[],
                removed: &[],
                origin,
            };
            for ext in &self.server.extensions {
                if let Err(e) = ext.on_awareness_update(&payload).await {
                    warn!(document = %self.name, "on_awareness_update failed: {e}");
                }
            }
        }
    }

    fn broadcast_awareness(&mut self, update: &[u8]) {
        self.broadcast_with(|addr| OutgoingMessage::new(addr).awareness_update(update).into_bytes());
    }

    /// Broadcast a frame to every connection, building it once per distinct
    /// message address. The common (no session-awareness) case allocates a
    /// single [`Bytes`] that is refcount-shared across all recipients, so a
    /// fan-out to N clients costs one allocation, not N. Connections whose
    /// bounded outbound queue overflowed are evicted.
    fn broadcast_with<F: Fn(&str) -> Vec<u8>>(&mut self, build: F) {
        let default_addr = self.name.clone();
        let default_frame: Bytes = build(&default_addr).into();
        let mut lagging = Vec::new();
        for conn in self.connections.values() {
            let frame = if conn.message_address == default_addr {
                default_frame.clone()
            } else {
                Bytes::from(build(&conn.message_address))
            };
            if !conn.send_frame(frame) {
                lagging.push(conn.conn_id);
            }
        }
        self.evict_lagging(&lagging);
    }

    /// Drop connections whose bounded outbound queue overflowed (they fell too
    /// far behind). Their socket will time out and the client reconnects and
    /// resyncs — Yjs is designed for this. Bounds memory under broadcast load.
    fn evict_lagging(&mut self, ids: &[u64]) {
        for &id in ids {
            if let Some(conn) = self.connections.remove(&id) {
                debug!(document = %self.name, conn = id, "evicting lagging connection");
                for cid in &conn.client_ids {
                    self.awareness.remove_state(*cid);
                }
                conn.send_close(crate::error::RESET_CONNECTION);
            }
        }
    }

    fn reply_query_awareness(&self, address: &str, conn_id: u64) {
        if let Ok(update) = self.awareness.update() {
            if let Some(conn) = self.connections.get(&conn_id) {
                conn.send_frame(
                    OutgoingMessage::new(address)
                        .awareness_update(&update.encode_v1())
                        .into_bytes().into(),
                );
            }
        }
    }

    async fn handle_stateless(&mut self, conn_id: u64, mut frame: IncomingFrame<'_>) {
        use yrs::encoding::read::Read;
        let payload = match frame.decoder.read_string() {
            Ok(s) => s.to_string(),
            Err(_) => return,
        };
        let socket_id = self.connections[&conn_id].socket_id.clone();
        let sp = StatelessPayload {
            document_name: &self.name,
            socket_id: &socket_id,
            payload: &payload,
        };
        for ext in &self.server.extensions {
            if let Err(e) = ext.on_stateless(&sp).await {
                warn!(document = %self.name, "on_stateless failed: {e}");
                break;
            }
        }
    }

    async fn handle_token_sync(&mut self, _conn_id: u64, _frame: IncomingFrame<'_>) {
        // Re-authentication on a live connection is accepted as a no-op for now;
        // the connection keeps its existing authorization.
    }

    async fn close_connection(&mut self, conn_id: u64, event: CloseEvent) {
        if let Some(conn) = self.connections.get(&conn_id) {
            conn.send_close(event);
        }
        self.remove_connection(conn_id).await;
    }

    /// Broadcast a stateless payload to all connections (used by the
    /// `Document::broadcast_stateless` server API and the Redis fan-out).
    fn broadcast_stateless(&mut self, payload: &str) {
        self.broadcast_with(|addr| OutgoingMessage::new(addr).stateless(payload).into_bytes());
    }

    async fn handle_external_frame(
        &mut self,
        data: Vec<u8>,
        reply: Option<mpsc::UnboundedSender<Vec<u8>>>,
    ) {
        let frame = match IncomingFrame::parse(&data) {
            Ok(f) => f,
            Err(_) => return,
        };
        let origin = Origin::Redis;
        match frame.message_type {
            MessageType::Sync | MessageType::SyncReply => {
                self.handle_external_sync(frame, origin, reply).await;
            }
            MessageType::Awareness => {
                self.handle_awareness(frame, origin).await;
            }
            MessageType::QueryAwareness => {
                if let Some(reply) = reply {
                    if let Ok(update) = self.awareness.update() {
                        let f = OutgoingMessage::new(&self.name)
                            .awareness_update(&update.encode_v1())
                            .into_bytes();
                        let _ = reply.send(f);
                    }
                }
            }
            MessageType::BroadcastStateless => {
                use yrs::encoding::read::Read;
                let mut frame = frame;
                if let Ok(payload) = frame.decoder.read_string() {
                    self.broadcast_stateless(payload);
                }
            }
            _ => {}
        }
    }

    async fn handle_external_sync(
        &mut self,
        mut frame: IncomingFrame<'_>,
        origin: Origin,
        reply: Option<mpsc::UnboundedSender<Vec<u8>>>,
    ) {
        let sync = match yrs::sync::SyncMessage::decode(&mut frame.decoder) {
            Ok(m) => m,
            Err(_) => return,
        };
        match sync {
            yrs::sync::SyncMessage::SyncStep1(client_sv) => {
                if let Some(reply) = reply {
                    let step2 = {
                        let doc = self.awareness.doc();
                        let txn = doc.transact();
                        txn.encode_diff_v1(&client_sv)
                    };
                    let f = OutgoingMessage::new(&self.name)
                        .sync_reply()
                        .write_sync_payload(&encode_sync(&yrs::sync::SyncMessage::SyncStep2(step2)))
                        .into_bytes();
                    let _ = reply.send(f);
                }
            }
            yrs::sync::SyncMessage::SyncStep2(update)
            | yrs::sync::SyncMessage::Update(update) => {
                self.apply_and_broadcast(&update, origin, None).await;
            }
        }
    }

    fn awareness_has_states(&self) -> bool {
        self.awareness.iter().next().is_some()
    }

    fn mark_dirty(&mut self, origin: Origin) {
        let now = Instant::now();
        self.dirty = true;
        self.first_dirty_at.get_or_insert(now);
        self.last_change_at = Some(now);
        self.last_origin = origin;
    }

    /// Persist the document via `on_store_document`/`after_store_document`
    /// hooks. Errors leave the document dirty so the next tick retries.
    async fn run_store(&mut self) {
        if !self.dirty {
            return;
        }
        let state = {
            let doc = self.awareness.doc();
            let txn = doc.transact();
            txn.encode_state_as_update_v1(&StateVector::default())
        };
        let payload = DocumentPayload {
            document_name: &self.name,
            clients_count: self.connections.len(),
        };
        let mut ok = true;
        for ext in &self.server.extensions {
            match ext.on_store_document(&state, &payload).await {
                Ok(()) => {}
                Err(e) => {
                    warn!(document = %self.name, "on_store_document failed: {e}");
                    ok = false;
                    break;
                }
            }
        }
        if ok {
            for ext in &self.server.extensions {
                if let Err(e) = ext.after_store_document(&payload).await {
                    warn!(document = %self.name, "after_store_document failed: {e}");
                }
            }
            self.dirty = false;
            self.first_dirty_at = None;
            self.last_change_at = None;
        }
    }

    /// If there are no connections, persist (if needed) and unload. Returns
    /// true if the actor should exit.
    async fn maybe_unload(&mut self) -> bool {
        if !self.connections.is_empty() {
            return false;
        }
        if !self.server.config.unload_immediately && self.dirty {
            // Keep the document warm and let the debounce timer persist it; we
            // unload on a later idle tick once clean.
            return false;
        }
        if self.dirty {
            self.run_store().await;
        }
        if !self.connections.is_empty() {
            return false;
        }
        let payload = DocumentPayload {
            document_name: &self.name,
            clients_count: 0,
        };
        for ext in &self.server.extensions {
            if let Err(e) = ext.before_unload_document(&payload).await {
                debug!(document = %self.name, "unload aborted by {}: {e}", ext.name());
                return false;
            }
        }
        self.server.documents.remove(&self.name);
        for ext in &self.server.extensions {
            let _ = ext.after_unload_document(&self.name).await;
        }
        true
    }
}

async fn sleep_until_opt(deadline: Option<Instant>) {
    match deadline {
        Some(d) => {
            let now = Instant::now();
            if d > now {
                tokio::time::sleep(d - now).await;
            }
        }
        None => {
            // Never resolves; the select! will wake on the command channel.
            futures_util::future::pending::<()>().await;
        }
    }
}

// Silence unused-import warnings for sync tag constants kept for documentation.
#[allow(dead_code)]
const _SYNC_TAGS: [u64; 3] = [SYNC_STEP_1, SYNC_STEP_2, SYNC_UPDATE];
#[allow(dead_code)]
fn _auth_tag_ref() -> AuthMessageType {
    AuthMessageType::Token
}
