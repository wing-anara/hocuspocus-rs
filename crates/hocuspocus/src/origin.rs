//! Transaction origin tracking, mirroring `TransactionOrigin` in `types.ts`.
//!
//! Origins let hooks and the broadcast logic tell where a mutation came from:
//! a specific WebSocket connection, a peer node via Redis, or a server-local
//! direct connection. We serialise the origin into the `yrs` [`yrs::Origin`]
//! so it survives through `transact_mut_with`.

/// Where a document mutation originated.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Origin {
    /// A WebSocket client connection (carries its connection id).
    Connection(u64),
    /// Replicated from another server node via Redis.
    Redis,
    /// A server-local direct connection / programmatic edit.
    Local,
}

impl Origin {
    pub fn encode(self) -> Vec<u8> {
        match self {
            Origin::Connection(id) => {
                let mut v = Vec::with_capacity(9);
                v.push(1);
                v.extend_from_slice(&id.to_le_bytes());
                v
            }
            Origin::Redis => vec![2],
            Origin::Local => vec![3],
        }
    }

    pub fn decode(bytes: &[u8]) -> Option<Self> {
        match bytes.first()? {
            1 if bytes.len() == 9 => {
                let mut id = [0u8; 8];
                id.copy_from_slice(&bytes[1..9]);
                Some(Origin::Connection(u64::from_le_bytes(id)))
            }
            2 => Some(Origin::Redis),
            3 => Some(Origin::Local),
            _ => None,
        }
    }

    /// `true` if this update came from a peer node (used to avoid re-publishing
    /// it back to Redis).
    pub fn is_redis(self) -> bool {
        matches!(self, Origin::Redis)
    }
}

impl From<Origin> for yrs::Origin {
    fn from(o: Origin) -> Self {
        yrs::Origin::from(o.encode().as_slice())
    }
}
