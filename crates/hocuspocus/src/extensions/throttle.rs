//! Connection throttling extension, mirroring `@hocuspocus/extension-throttle`.
//!
//! Per-IP sliding window: at most `throttle` connection attempts per
//! `considered` window; exceeding it bans the IP for `ban_time`.

use std::collections::HashMap;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use parking_lot::Mutex;

use crate::error::{HookError, HookResult};
use crate::extension::{ConnectionPayload, Extension};

struct Inner {
    connections: HashMap<String, Vec<Instant>>,
    banned: HashMap<String, Instant>,
}

/// Rate-limits inbound connections by client IP.
pub struct Throttle {
    throttle: usize,
    considered: Duration,
    ban_time: Duration,
    inner: Mutex<Inner>,
}

impl Throttle {
    /// `throttle`: allowed attempts per window. `considered`: window length.
    /// `ban_time`: ban duration once exceeded.
    pub fn new(throttle: usize, considered: Duration, ban_time: Duration) -> Self {
        Self {
            throttle,
            considered,
            ban_time,
            inner: Mutex::new(Inner {
                connections: HashMap::new(),
                banned: HashMap::new(),
            }),
        }
    }

    /// Defaults matching the TS extension: 15 attempts / 60s, 5 minute ban.
    pub fn with_defaults() -> Self {
        Self::new(15, Duration::from_secs(60), Duration::from_secs(5 * 60))
    }

    fn should_block(&self, ip: &str) -> bool {
        let now = Instant::now();
        let mut inner = self.inner.lock();

        if let Some(&banned_at) = inner.banned.get(ip) {
            if now.duration_since(banned_at) < self.ban_time {
                return true;
            }
            inner.banned.remove(ip);
        }

        let window = self.considered;
        let entry = inner.connections.entry(ip.to_string()).or_default();
        entry.push(now);
        entry.retain(|t| now.duration_since(*t) < window);

        if entry.len() > self.throttle {
            inner.banned.insert(ip.to_string(), now);
            return true;
        }
        false
    }
}

#[async_trait]
impl Extension for Throttle {
    fn name(&self) -> &str {
        "throttle"
    }

    async fn on_connect(&self, payload: &ConnectionPayload<'_>) -> HookResult {
        let ip = payload.request.client_ip().unwrap_or_default();
        if !ip.is_empty() && self.should_block(&ip) {
            return Err(HookError::forbidden());
        }
        Ok(())
    }
}
