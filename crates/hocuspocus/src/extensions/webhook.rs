//! Webhook extension, mirroring `@hocuspocus/extension-webhook`.
//!
//! Emits HMAC-SHA256 signed JSON POSTs on lifecycle events. The signature is
//! sent in the `X-Hocuspocus-Signature-256: sha256=<hex>` header, identical to
//! the TS extension. Because there is no server-side Yjs↔app transformer here,
//! the `change` event carries the hex-encoded Yjs update rather than a
//! transformed document tree.

use async_trait::async_trait;
use hmac::{Hmac, Mac};
use serde_json::json;
use sha2::Sha256;

use crate::error::HookResult;
use crate::extension::{ChangePayload, ConnectionPayload, Extension};

type HmacSha256 = Hmac<Sha256>;

/// Events the webhook can emit.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WebhookEvent {
    Change,
    Connect,
    Disconnect,
}

impl WebhookEvent {
    fn as_str(self) -> &'static str {
        match self {
            WebhookEvent::Change => "change",
            WebhookEvent::Connect => "connect",
            WebhookEvent::Disconnect => "disconnect",
        }
    }
}

/// HMAC-signed webhook notifier.
pub struct Webhook {
    url: String,
    secret: String,
    events: Vec<WebhookEvent>,
    client: reqwest::Client,
}

impl Webhook {
    pub fn new(url: impl Into<String>, secret: impl Into<String>) -> Self {
        Self {
            url: url.into(),
            secret: secret.into(),
            events: vec![
                WebhookEvent::Change,
                WebhookEvent::Connect,
                WebhookEvent::Disconnect,
            ],
            client: reqwest::Client::new(),
        }
    }

    pub fn events(mut self, events: Vec<WebhookEvent>) -> Self {
        self.events = events;
        self
    }

    fn signature(&self, body: &str) -> String {
        let mut mac =
            HmacSha256::new_from_slice(self.secret.as_bytes()).expect("HMAC accepts any key size");
        mac.update(body.as_bytes());
        format!("sha256={}", hex::encode(mac.finalize().into_bytes()))
    }

    async fn send(&self, event: WebhookEvent, payload: serde_json::Value) {
        if !self.events.contains(&event) {
            return;
        }
        let body = json!({ "event": event.as_str(), "payload": payload }).to_string();
        let sig = self.signature(&body);
        let res = self
            .client
            .post(&self.url)
            .header("X-Hocuspocus-Signature-256", sig)
            .header("Content-Type", "application/json")
            .body(body)
            .send()
            .await;
        if let Err(e) = res {
            tracing::warn!("webhook POST failed: {e}");
        }
    }
}

#[async_trait]
impl Extension for Webhook {
    fn name(&self) -> &str {
        "webhook"
    }

    async fn on_connect(&self, payload: &ConnectionPayload<'_>) -> HookResult {
        self.send(
            WebhookEvent::Connect,
            json!({
                "documentName": payload.document_name,
                "socketId": payload.socket_id,
                "requestParameters": payload.request.parameters,
            }),
        )
        .await;
        Ok(())
    }

    async fn on_disconnect(&self, payload: &ConnectionPayload<'_>) -> HookResult {
        self.send(
            WebhookEvent::Disconnect,
            json!({
                "documentName": payload.document_name,
                "socketId": payload.socket_id,
            }),
        )
        .await;
        Ok(())
    }

    async fn on_change(&self, payload: &ChangePayload<'_>) -> HookResult {
        self.send(
            WebhookEvent::Change,
            json!({
                "documentName": payload.document_name,
                "update": hex::encode(payload.update),
            }),
        )
        .await;
        Ok(())
    }
}
