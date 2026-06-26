//! Horizontal scaling via Redis pub/sub, mirroring
//! `@hocuspocus/extension-redis`.
//!
//! Every server node publishes document updates, awareness changes and
//! stateless broadcasts to a Redis channel; peer nodes apply them to their
//! resident copies. Each published message is prefixed with the node's unique
//! identifier so a node ignores its own echoes. Persistence is guarded by a
//! Redis lock so two nodes never write the same document concurrently.
//!
//! Wire format of a published message (matching the TS extension):
//! `[identifier_len: u8][identifier: utf8][frame: OutgoingMessage bytes]`.

use std::sync::Arc;

use async_trait::async_trait;
use dashmap::DashMap;
use futures_util::StreamExt;
use redis::AsyncCommands;
use tokio::sync::mpsc;
use tracing::{debug, warn};
use yrs::sync::SyncMessage;
use yrs::updates::encoder::{Encode, Encoder, EncoderV1};
use yrs::{Doc, ReadTxn, Transact};

use crate::document::DocCommand;
use crate::error::{HookError, HookResult};
use crate::extension::{AwarenessPayload, ChangePayload, DocumentPayload, Extension};
use crate::protocol::{parse_routing_key, IncomingFrame};
use crate::server::ServerShared;

/// Configuration for the Redis scaling extension.
pub struct RedisConfig {
    /// `redis://host:port` connection URL.
    pub url: String,
    /// Unique per-node identifier (defaults to a random UUID).
    pub identifier: String,
    /// Redis key namespace.
    pub prefix: String,
    /// Lock TTL in milliseconds.
    pub lock_timeout_ms: u64,
}

impl RedisConfig {
    pub fn new(url: impl Into<String>) -> Self {
        Self {
            url: url.into(),
            identifier: format!("host-{}", uuid::Uuid::new_v4()),
            prefix: "hocuspocus".to_string(),
            lock_timeout_ms: 1000,
        }
    }
    pub fn identifier(mut self, id: impl Into<String>) -> Self {
        self.identifier = id.into();
        self
    }
    pub fn prefix(mut self, prefix: impl Into<String>) -> Self {
        self.prefix = prefix.into();
        self
    }
}

fn message_prefix(identifier: &str) -> Vec<u8> {
    let mut p = Vec::with_capacity(1 + identifier.len());
    p.push(identifier.len() as u8);
    p.extend_from_slice(identifier.as_bytes());
    p
}

/// The Redis horizontal-scaling extension.
pub struct RedisScaling {
    identifier: String,
    prefix: String,
    lock_timeout_ms: u64,
    client: redis::Client,
    // MultiplexedConnection is cheaply cloneable and safe for concurrent use,
    // so publishes/locks clone it instead of serialising through a Mutex.
    pub_conn: redis::aio::MultiplexedConnection,
    message_prefix: Vec<u8>,
    locks: DashMap<String, String>,
}

impl RedisScaling {
    /// Connect to Redis and build the extension.
    pub async fn connect(config: RedisConfig) -> anyhow::Result<Arc<Self>> {
        let client = redis::Client::open(config.url.clone())?;
        let pub_conn = client.get_multiplexed_async_connection().await?;
        Ok(Arc::new(Self {
            message_prefix: message_prefix(&config.identifier),
            identifier: config.identifier,
            prefix: config.prefix,
            lock_timeout_ms: config.lock_timeout_ms,
            client,
            pub_conn,
            locks: DashMap::new(),
        }))
    }

    fn channel(&self, document_name: &str) -> String {
        format!("{}:{}", self.prefix, document_name)
    }

    fn lock_key(&self, document_name: &str) -> String {
        format!("{}:{}:lock", self.prefix, document_name)
    }

    /// Publish a frame to peers without blocking the caller. Document-actor
    /// hooks (`on_change`, `on_awareness_update`, ...) call this on every edit;
    /// awaiting the Redis round-trip here serialised the actor's frame
    /// processing (~one RTT per keystroke), so the replication publish is fired
    /// on a detached task. Yjs updates and awareness are order-independent, so
    /// out-of-order delivery to peers is safe.
    fn publish(&self, document_name: &str, frame: &[u8]) {
        let mut payload = Vec::with_capacity(self.message_prefix.len() + frame.len());
        payload.extend_from_slice(&self.message_prefix);
        payload.extend_from_slice(frame);
        let channel = self.channel(document_name);
        let mut conn = self.pub_conn.clone();
        tokio::spawn(async move {
            if let Err(e) = conn.publish::<_, _, ()>(channel, payload).await {
                warn!("redis publish failed: {e}");
            }
        });
    }
}

/// Spawn the subscriber loop and the reply publisher. Pattern-subscribes to
/// `{prefix}:*` and routes decoded frames into the matching local document
/// actor (if resident). Only needs cheaply-cloneable primitives.
fn spawn_subscriber(
    client: redis::Client,
    identifier: String,
    prefix: String,
    server: Arc<ServerShared>,
) {
    tokio::spawn(async move {
        let prefix_bytes = message_prefix(&identifier);

        // Reply publisher: document actors push response frames here; we wrap
        // them with our identifier prefix and publish to the right channel.
        let (reply_tx, mut reply_rx) = mpsc::unbounded_channel::<Vec<u8>>();
        {
            let client = client.clone();
            let prefix = prefix.clone();
            let prefix_bytes = prefix_bytes.clone();
            tokio::spawn(async move {
                let mut conn = match client.get_multiplexed_async_connection().await {
                    Ok(c) => c,
                    Err(e) => {
                        warn!("redis reply connection failed: {e}");
                        return;
                    }
                };
                while let Some(frame) = reply_rx.recv().await {
                    if let Ok(parsed) = IncomingFrame::parse(&frame) {
                        let (doc, _) = parse_routing_key(&parsed.routing_key);
                        let channel = format!("{prefix}:{doc}");
                        let mut payload =
                            Vec::with_capacity(prefix_bytes.len() + frame.len());
                        payload.extend_from_slice(&prefix_bytes);
                        payload.extend_from_slice(&frame);
                        let _: Result<(), _> = conn.publish::<_, _, ()>(channel, payload).await;
                    }
                }
            });
        }

        let mut pubsub = match client.get_async_pubsub().await {
            Ok(p) => p,
            Err(e) => {
                warn!("redis pubsub connect failed: {e}");
                return;
            }
        };
        let pattern = format!("{prefix}:*");
        if let Err(e) = pubsub.psubscribe(&pattern).await {
            warn!("redis psubscribe failed: {e}");
            return;
        }
        debug!("redis subscriber listening on {pattern}");

        let mut stream = pubsub.on_message();
        while let Some(msg) = stream.next().await {
            let payload = msg.get_payload_bytes().to_vec();
            if payload.is_empty() {
                continue;
            }
            let id_len = payload[0] as usize;
            if payload.len() < 1 + id_len {
                continue;
            }
            if payload[1..1 + id_len] == prefix_bytes[1..] {
                continue; // our own message
            }
            let frame = &payload[1 + id_len..];
            let Ok(parsed) = IncomingFrame::parse(frame) else {
                continue;
            };
            let (doc_name, _) = parse_routing_key(&parsed.routing_key);
            if let Some(handle) = server.documents.get(doc_name) {
                let _ = handle.tx.send(DocCommand::ExternalFrame {
                    data: frame.to_vec(),
                    reply: Some(reply_tx.clone()),
                });
            }
        }
        warn!("redis subscriber stream ended");
    });
}

fn encode_sync_frame(document_name: &str, msg: &SyncMessage) -> Vec<u8> {
    use yrs::encoding::write::Write;
    let mut e = EncoderV1::new();
    e.write_string(document_name);
    e.write_var(0u64); // MessageType::Sync
    msg.encode(&mut e);
    e.to_vec()
}

#[async_trait]
impl Extension for RedisScaling {
    fn name(&self) -> &str {
        "redis"
    }

    /// Higher priority so the store lock is acquired before the database
    /// extension persists.
    fn priority(&self) -> i32 {
        1000
    }

    async fn on_server_ready(&self, server: Arc<ServerShared>) {
        spawn_subscriber(
            self.client.clone(),
            self.identifier.clone(),
            self.prefix.clone(),
            server,
        );
    }

    async fn after_load_document(&self, doc: &Doc, payload: &DocumentPayload<'_>) -> HookResult {
        // Ask peers for the current state and awareness.
        let sv = {
            let txn = doc.transact();
            txn.state_vector()
        };
        let sync_frame = encode_sync_frame(payload.document_name, &SyncMessage::SyncStep1(sv));
        self.publish(payload.document_name, &sync_frame);

        use yrs::encoding::write::Write;
        let mut e = EncoderV1::new();
        e.write_string(payload.document_name);
        e.write_var(3u64); // MessageType::QueryAwareness
        self.publish(payload.document_name, &e.to_vec());
        Ok(())
    }

    async fn on_change(&self, payload: &ChangePayload<'_>) -> HookResult {
        if payload.origin.is_redis() {
            return Ok(()); // already replicated by the originating node
        }
        let frame = encode_sync_frame(
            payload.document_name,
            &SyncMessage::Update(payload.update.to_vec()),
        );
        self.publish(payload.document_name, &frame);
        Ok(())
    }

    async fn on_awareness_update(&self, payload: &AwarenessPayload<'_>) -> HookResult {
        if payload.origin.is_redis() {
            return Ok(());
        }
        use yrs::encoding::write::Write;
        let mut e = EncoderV1::new();
        e.write_string(payload.document_name);
        e.write_var(1u64); // MessageType::Awareness
        e.write_buf(payload.update);
        self.publish(payload.document_name, &e.to_vec());
        Ok(())
    }

    async fn before_broadcast_stateless(
        &self,
        document_name: &str,
        payload: &str,
    ) -> HookResult {
        use yrs::encoding::write::Write;
        let mut e = EncoderV1::new();
        e.write_string(document_name);
        e.write_var(6u64); // MessageType::BroadcastStateless
        e.write_string(payload);
        self.publish(document_name, &e.to_vec());
        Ok(())
    }

    async fn on_store_document(&self, _state: &[u8], payload: &DocumentPayload<'_>) -> HookResult {
        // Acquire a single-instance Redis lock (SET NX PX). If we can't, skip
        // the remaining (persistence) hooks; the document stays dirty and is
        // retried, matching the TS `SkipFurtherHooksError` behaviour.
        let key = self.lock_key(payload.document_name);
        let token = uuid::Uuid::new_v4().to_string();
        let mut conn = self.pub_conn.clone();
        let acquired: Option<String> = redis::cmd("SET")
            .arg(&key)
            .arg(&token)
            .arg("NX")
            .arg("PX")
            .arg(self.lock_timeout_ms)
            .query_async(&mut conn)
            .await
            .unwrap_or(None);
        if acquired.is_some() {
            self.locks.insert(key, token);
            Ok(())
        } else {
            Err(HookError::new("another node holds the document lock"))
        }
    }

    async fn after_store_document(&self, payload: &DocumentPayload<'_>) -> HookResult {
        let key = self.lock_key(payload.document_name);
        if let Some((_, token)) = self.locks.remove(&key) {
            let script = redis::Script::new(
                "if redis.call('get', KEYS[1]) == ARGV[1] then return redis.call('del', KEYS[1]) else return 0 end",
            );
            let mut conn = self.pub_conn.clone();
            let _: Result<i64, _> = script.key(&key).arg(token).invoke_async(&mut conn).await;
        }
        Ok(())
    }
}
