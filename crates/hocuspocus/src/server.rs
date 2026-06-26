//! The server: connection acceptance, the authentication handshake, document
//! routing, and the document registry. Ports `Hocuspocus.ts`, `Server.ts` and
//! `ClientConnection.ts`.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Instant;

use axum::extract::ws::{CloseFrame, Message, WebSocket, WebSocketUpgrade};
use axum::extract::{ConnectInfo, FromRequestParts, State};
use axum::response::{IntoResponse, Response};
use axum::routing::any;
use axum::Router;
use bytes::Bytes;
use dashmap::DashMap;
use futures_util::{SinkExt, StreamExt};
use tokio::sync::{mpsc, oneshot, Mutex};
use tracing::{debug, info, warn};

use crate::config::Configuration;
use crate::context::{Context, RequestMeta};
use crate::document::{ConnHandle, DocCommand, Document, DocumentHandle, OutMsg};
use crate::error::{CloseEvent, CONNECTION_TIMEOUT, FORBIDDEN, RESET_CONNECTION};
use crate::extension::{
    AuthenticatePayload, ConnectionConfig, ConnectionPayload, Extension,
};
use crate::protocol::{
    parse_routing_key, AuthMessageType, IncomingFrame, MessageType, OutgoingMessage,
};

/// Shared, cloneable server state behind an `Arc`.
pub struct ServerShared {
    pub config: Configuration,
    pub extensions: Vec<Arc<dyn Extension>>,
    pub documents: DashMap<String, DocumentHandle>,
    creating: DashMap<String, Arc<Mutex<()>>>,
    next_conn_id: AtomicU64,
    next_socket_id: AtomicU64,
    /// Live count of open WebSocket sockets, maintained by the socket handler so
    /// the cap can shed before the handshake (and thus before auth).
    active_connections: AtomicUsize,
}

/// Decrements the active-socket counter on drop, so the count is correct on
/// every exit path (over-cap close, error, or normal disconnect).
struct ConnectionGuard(Arc<ServerShared>);

impl Drop for ConnectionGuard {
    fn drop(&mut self) {
        self.0.active_connections.fetch_sub(1, Ordering::Relaxed);
    }
}

impl ServerShared {
    /// Get the handle for `name`, spawning and loading the document actor if it
    /// is not currently resident in memory.
    pub async fn document(self: &Arc<Self>, name: &str) -> anyhow::Result<DocumentHandle> {
        if let Some(h) = self.documents.get(name) {
            return Ok(h.clone());
        }
        let lock = self
            .creating
            .entry(name.to_string())
            .or_insert_with(|| Arc::new(Mutex::new(())))
            .clone();
        let _g = lock.lock().await;
        if let Some(h) = self.documents.get(name) {
            return Ok(h.clone());
        }
        let handle = Document::spawn(name.to_string(), self.clone()).await?;
        self.documents.insert(name.to_string(), handle.clone());
        self.creating.remove(name);
        Ok(handle)
    }

    /// Total number of resident documents.
    pub fn documents_count(&self) -> usize {
        self.documents.len()
    }

    /// Live count of open WebSocket sockets.
    pub fn active_connection_count(&self) -> usize {
        self.active_connections.load(Ordering::Relaxed)
    }

    /// Programmatically apply an update to a document (server-side edit),
    /// loading it if necessary. Mirrors `openDirectConnection().transact`.
    pub async fn direct_apply(self: &Arc<Self>, name: &str, update: Vec<u8>) -> anyhow::Result<()> {
        let handle = self.document(name).await?;
        let (ack, rx) = oneshot::channel();
        handle
            .tx
            .send(DocCommand::DirectApply { update, ack })
            .map_err(|_| anyhow::anyhow!("document actor unavailable"))?;
        let _ = rx.await;
        Ok(())
    }
}

/// A configured Hocuspocus server.
pub struct Hocuspocus {
    shared: Arc<ServerShared>,
}

/// Builder for a [`Hocuspocus`] server.
pub struct ServerBuilder {
    config: Configuration,
    extensions: Vec<Arc<dyn Extension>>,
}

impl Default for ServerBuilder {
    fn default() -> Self {
        Self {
            config: Configuration::default(),
            extensions: Vec::new(),
        }
    }
}

impl ServerBuilder {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn configuration(mut self, config: Configuration) -> Self {
        self.config = config;
        self
    }

    pub fn port(mut self, port: u16) -> Self {
        self.config.port = port;
        self
    }

    pub fn address(mut self, address: impl Into<String>) -> Self {
        self.config.address = address.into();
        self
    }

    /// Register an extension. Extensions run in descending priority order.
    pub fn extension(mut self, ext: Arc<dyn Extension>) -> Self {
        self.extensions.push(ext);
        self
    }

    pub fn build(mut self) -> Hocuspocus {
        // Sort by descending priority (stable, matching the TS sort).
        self.extensions
            .sort_by(|a, b| b.priority().cmp(&a.priority()));
        Hocuspocus {
            shared: Arc::new(ServerShared {
                config: self.config,
                extensions: self.extensions,
                documents: DashMap::new(),
                creating: DashMap::new(),
                next_conn_id: AtomicU64::new(1),
                next_socket_id: AtomicU64::new(1),
                active_connections: AtomicUsize::new(0),
            }),
        }
    }
}

impl Hocuspocus {
    pub fn builder() -> ServerBuilder {
        ServerBuilder::new()
    }

    /// Access the shared state (for tests and embedding).
    pub fn shared(&self) -> Arc<ServerShared> {
        self.shared.clone()
    }

    /// Build the Axum router that serves the WebSocket protocol.
    pub fn router(&self) -> Router {
        Router::new()
            .route("/", any(ws_handler))
            .fallback(any(ws_handler))
            .with_state(self.shared.clone())
    }

    /// Run `on_configure` and `on_server_ready` hooks. Call this before serving
    /// the router directly (it is called automatically by [`Hocuspocus::listen`]).
    pub async fn prepare(&self) {
        for ext in &self.shared.extensions {
            if let Err(e) = ext.on_configure().await {
                warn!("on_configure failed: {e}");
            }
        }
        for ext in &self.shared.extensions {
            ext.on_server_ready(self.shared.clone()).await;
        }
    }

    /// Bind and serve until the process is told to stop. Runs `on_configure`
    /// and `on_listen` hooks.
    pub async fn listen(self) -> anyhow::Result<()> {
        self.prepare().await;
        let addr: SocketAddr = format!("{}:{}", self.shared.config.address, self.shared.config.port)
            .parse()?;
        let listener = tokio::net::TcpListener::bind(addr).await?;
        let bound = listener.local_addr()?;
        if !self.shared.config.quiet {
            info!("Hocuspocus (Rust) listening on {bound}");
        }
        for ext in &self.shared.extensions {
            if let Err(e) = ext.on_listen(bound.port()).await {
                warn!("on_listen failed: {e}");
            }
        }
        let app = self
            .router()
            .into_make_service_with_connect_info::<SocketAddr>();
        axum::serve(listener, app).await?;
        for ext in &self.shared.extensions {
            let _ = ext.on_destroy().await;
        }
        Ok(())
    }
}

async fn ws_handler(
    State(shared): State<Arc<ServerShared>>,
    ConnectInfo(remote): ConnectInfo<SocketAddr>,
    req: axum::extract::Request,
) -> Response {
    let (mut parts, _body) = req.into_parts();

    let mut meta = RequestMeta::default();
    for (k, v) in parts.headers.iter() {
        if let Ok(val) = v.to_str() {
            meta.headers
                .insert(k.as_str().to_ascii_lowercase(), val.to_string());
        }
    }
    if let Some(q) = parts.uri.query() {
        for (k, v) in form_urlencoded_parse(q) {
            meta.parameters.insert(k, v);
        }
    }
    meta.remote_ip = Some(remote.ip().to_string());

    // Shed over-capacity connections before completing the handshake, so a
    // connection storm never reaches authentication/persistence work.
    let max = shared.config.max_connections;
    if max > 0 && shared.active_connections.load(Ordering::Relaxed) >= max {
        return (
            axum::http::StatusCode::SERVICE_UNAVAILABLE,
            "server at capacity",
        )
            .into_response();
    }

    match WebSocketUpgrade::from_request_parts(&mut parts, &shared).await {
        Ok(ws) => ws.on_upgrade(move |socket| handle_socket(socket, shared, meta)),
        Err(_) => {
            // Plain HTTP request (no upgrade): the Hocuspocus default response.
            "Welcome to Hocuspocus (Rust)!".into_response()
        }
    }
}

/// Minimal `application/x-www-form-urlencoded` query parser (avoids a dep).
fn form_urlencoded_parse(q: &str) -> Vec<(String, String)> {
    q.split('&')
        .filter(|p| !p.is_empty())
        .map(|pair| {
            let mut it = pair.splitn(2, '=');
            let k = it.next().unwrap_or("").to_string();
            let v = it.next().unwrap_or("").to_string();
            (percent_decode(&k), percent_decode(&v))
        })
        .collect()
}

fn percent_decode(s: &str) -> String {
    let bytes = s.replace('+', " ");
    let mut out = Vec::with_capacity(bytes.len());
    let raw = bytes.as_bytes();
    let mut i = 0;
    while i < raw.len() {
        if raw[i] == b'%' && i + 2 < raw.len() {
            if let Ok(b) = u8::from_str_radix(&bytes[i + 1..i + 3], 16) {
                out.push(b);
                i += 3;
                continue;
            }
        }
        out.push(raw[i]);
        i += 1;
    }
    String::from_utf8_lossy(&out).into_owned()
}

async fn handle_socket(socket: WebSocket, shared: Arc<ServerShared>, meta: RequestMeta) {
    // Count this socket for the duration of its life; the guard decrements on
    // every exit path so the cap reflects live connections.
    shared.active_connections.fetch_add(1, Ordering::Relaxed);
    let _conn_guard = ConnectionGuard(shared.clone());
    let socket_id = format!("ws-{}", shared.next_socket_id.fetch_add(1, Ordering::Relaxed));
    let (mut sink, mut stream) = socket.split();
    let (out_tx, mut out_rx) = mpsc::channel::<OutMsg>(shared.config.outbound_capacity);

    // Writer task: drains outbound frames to the socket.
    let writer = tokio::spawn(async move {
        while let Some(msg) = out_rx.recv().await {
            match msg {
                OutMsg::Frame(bytes) => {
                    if sink.send(Message::Binary(Bytes::from(bytes))).await.is_err() {
                        break;
                    }
                }
                OutMsg::Close(ev) => {
                    let _ = sink
                        .send(Message::Close(Some(CloseFrame {
                            code: ev.code,
                            reason: ev.reason.to_string().into(),
                        })))
                        .await;
                    break;
                }
            }
        }
    });

    let mut client = ClientConnection::new(shared, socket_id, out_tx, Arc::new(meta));

    loop {
        let deadline = client.idle_deadline();
        tokio::select! {
            msg = stream.next() => {
                match msg {
                    Some(Ok(Message::Binary(b))) => {
                        client.handle_frame(b.to_vec()).await;
                    }
                    Some(Ok(Message::Close(_))) | None => break,
                    Some(Ok(_)) => {}
                    Some(Err(_)) => break,
                }
            }
            _ = sleep_until(deadline) => {
                client.terminate(CONNECTION_TIMEOUT);
                break;
            }
        }
    }

    client.cleanup().await;
    writer.abort();
}

async fn sleep_until(deadline: Instant) {
    let now = Instant::now();
    if deadline > now {
        tokio::time::sleep(deadline - now).await;
    }
}

struct Established {
    doc: DocumentHandle,
    conn_id: u64,
}

/// Per-socket state: authentication, queueing, and document routing.
struct ClientConnection {
    shared: Arc<ServerShared>,
    socket_id: String,
    out_tx: mpsc::Sender<OutMsg>,
    request: Arc<RequestMeta>,
    established: HashMap<String, Established>,
    queue: HashMap<String, Vec<Vec<u8>>>,
    queued_bytes: usize,
    queued_messages: usize,
    established_at: Instant,
    last_message_at: Instant,
    authenticated: bool,
}

impl ClientConnection {
    fn new(
        shared: Arc<ServerShared>,
        socket_id: String,
        out_tx: mpsc::Sender<OutMsg>,
        request: Arc<RequestMeta>,
    ) -> Self {
        let now = Instant::now();
        Self {
            shared,
            socket_id,
            out_tx,
            request,
            established: HashMap::new(),
            queue: HashMap::new(),
            queued_bytes: 0,
            queued_messages: 0,
            established_at: now,
            last_message_at: now,
            authenticated: false,
        }
    }

    /// Absolute idle deadline. Pre-auth uses a fixed deadline from the time the
    /// socket opened (so an unauthenticated flood can't keep it alive); once
    /// authenticated it slides with the last received message.
    fn idle_deadline(&self) -> Instant {
        let reference = if self.authenticated {
            self.last_message_at
        } else {
            self.established_at
        };
        reference + self.shared.config.timeout
    }

    fn terminate(&self, event: CloseEvent) {
        let _ = self.out_tx.try_send(OutMsg::Close(event));
    }

    async fn handle_frame(&mut self, data: Vec<u8>) {
        self.last_message_at = Instant::now();

        // Single-byte Ping/Pong frames carry no document address.
        if data.len() == 1 {
            return;
        }

        let (raw_key, mtype) = match IncomingFrame::parse(&data) {
            Ok(f) => (f.routing_key, f.message_type),
            Err(_) => return,
        };

        if let Some(est) = self.established.get(&raw_key) {
            let _ = est.doc.tx.send(DocCommand::ClientFrame {
                conn_id: est.conn_id,
                data,
            });
            return;
        }

        if mtype == MessageType::Auth {
            self.authenticate(raw_key, data).await;
        } else {
            self.enqueue(raw_key, data);
        }
    }

    fn enqueue(&mut self, raw_key: String, data: Vec<u8>) {
        let cfg = &self.shared.config;
        // Enforce pre-auth queue limits, mirroring the TS connection.
        let is_new_doc = !self.queue.contains_key(&raw_key);
        if is_new_doc && self.queue.len() + 1 > cfg.max_pending_documents {
            self.terminate(RESET_CONNECTION);
            return;
        }
        if self.queued_messages + 1 > cfg.max_unauthenticated_queue_messages
            || self.queued_bytes + data.len() > cfg.max_unauthenticated_queue_size
        {
            self.terminate(RESET_CONNECTION);
            return;
        }
        self.queued_bytes += data.len();
        self.queued_messages += 1;
        self.queue.entry(raw_key).or_default().push(data);
    }

    async fn authenticate(&mut self, raw_key: String, data: Vec<u8>) {
        let (doc_name, session_id) = {
            let (d, s) = parse_routing_key(&raw_key);
            (d.to_string(), s.map(|s| s.to_string()))
        };

        // Parse: address, opcode(Auth), authType(Token), token, [version].
        let mut frame = match IncomingFrame::parse(&data) {
            Ok(f) => f,
            Err(_) => return,
        };
        let auth_type: u64 = match frame.decoder.read_var() {
            Ok(v) => v,
            Err(_) => return,
        };
        if auth_type != AuthMessageType::Token as u64 {
            return;
        }
        use yrs::encoding::read::Read;
        let token = frame.decoder.read_string().unwrap_or("").to_string();
        let provider_version = frame.decoder.read_string().ok().map(|s| s.to_string());

        let context = Context::new();
        let connection_config = Arc::new(ConnectionConfig::default());

        // on_connect hooks.
        {
            let payload = ConnectionPayload {
                document_name: &doc_name,
                socket_id: &self.socket_id,
                context: &context,
                request: &self.request,
                connection_config: &connection_config,
                provider_version: provider_version.as_deref(),
            };
            for ext in &self.shared.extensions {
                if let Err(e) = ext.on_connect(&payload).await {
                    debug!(document = %doc_name, "on_connect denied: {e}");
                    self.deny(&raw_key, e.message);
                    return;
                }
            }
        }

        // on_authenticate hooks.
        {
            let payload = AuthenticatePayload {
                token: &token,
                document_name: &doc_name,
                socket_id: &self.socket_id,
                context: &context,
                request: &self.request,
                connection_config: &connection_config,
            };
            for ext in &self.shared.extensions {
                if let Err(e) = ext.on_authenticate(&payload).await {
                    debug!(document = %doc_name, "on_authenticate denied: {e}");
                    self.deny(&raw_key, e.message);
                    return;
                }
            }
        }

        connection_config.set_authenticated(true);
        self.authenticated = true;
        let read_only = connection_config.read_only();

        // Tell the client it is authenticated.
        let _ = self.out_tx.try_send(OutMsg::Frame(
            OutgoingMessage::new(&raw_key)
                .authenticated(read_only)
                .into_bytes()
                .into(),
        ));

        // Resolve (and load) the document, registering this connection.
        let conn_id = self.shared.next_conn_id.fetch_add(1, Ordering::Relaxed);
        let handle = match self.register_connection(
            &doc_name,
            &raw_key,
            conn_id,
            session_id,
            read_only,
            context,
            connection_config,
            provider_version,
        )
        .await
        {
            Some(h) => h,
            None => {
                self.terminate(RESET_CONNECTION);
                return;
            }
        };

        self.established.insert(raw_key.clone(), Established { doc: handle.clone(), conn_id });

        // Drain any messages queued for this routing key before authentication.
        if let Some(pending) = self.queue.remove(&raw_key) {
            for msg in pending {
                self.queued_messages = self.queued_messages.saturating_sub(1);
                self.queued_bytes = self.queued_bytes.saturating_sub(msg.len());
                let _ = handle.tx.send(DocCommand::ClientFrame { conn_id, data: msg });
            }
        }
    }

    #[allow(clippy::too_many_arguments)]
    async fn register_connection(
        &self,
        doc_name: &str,
        raw_key: &str,
        conn_id: u64,
        session_id: Option<String>,
        read_only: bool,
        context: Context,
        connection_config: Arc<ConnectionConfig>,
        provider_version: Option<String>,
    ) -> Option<DocumentHandle> {
        // Retry once if the resident actor exited between lookup and add.
        for _ in 0..2 {
            let handle = self.shared.document(doc_name).await.ok()?;
            let conn = ConnHandle {
                conn_id,
                outbound: self.out_tx.clone(),
                message_address: raw_key.to_string(),
                read_only,
                socket_id: self.socket_id.clone(),
                session_id: session_id.clone(),
                context: context.clone(),
                request: self.request.clone(),
                connection_config: connection_config.clone(),
                provider_version: provider_version.clone(),
                client_ids: Default::default(),
            };
            let (ack, rx) = oneshot::channel();
            if handle
                .tx
                .send(DocCommand::AddConnection {
                    handle: Box::new(conn),
                    ack,
                })
                .is_err()
            {
                // Actor gone; drop stale registry entry and retry.
                self.shared.documents.remove(doc_name);
                continue;
            }
            if rx.await.is_ok() {
                return Some(handle);
            }
            self.shared.documents.remove(doc_name);
        }
        None
    }

    fn deny(&self, raw_key: &str, reason: String) {
        let _ = self.out_tx.try_send(OutMsg::Frame(
            OutgoingMessage::new(raw_key)
                .permission_denied(&reason)
                .into_bytes()
                .into(),
        ));
        self.terminate(FORBIDDEN);
    }

    async fn cleanup(&mut self) {
        for (_key, est) in self.established.drain() {
            let _ = est.doc.tx.send(DocCommand::RemoveConnection {
                conn_id: est.conn_id,
            });
        }
    }
}
