//! Byte-for-byte compatible implementation of the Hocuspocus WebSocket wire
//! protocol.
//!
//! Every frame is `varString(documentName) varUint(messageType) <payload>`,
//! using the lib0 variable-length encoding (provided here by `yrs`'s
//! `EncoderV1`/`DecoderV1`, which are wire-identical to `lib0`). The only
//! exceptions are [`MessageType::Ping`]/[`MessageType::Pong`], which are sent
//! as a single bare byte with no document-name prefix.
//!
//! The sync sub-protocol (`SyncStep1`/`SyncStep2`/`Update`) and the awareness
//! update encoding are reused from `yrs::sync`, which is itself a faithful port
//! of `y-protocols`, so they interoperate with `@hocuspocus/provider` and
//! `y-websocket` clients unchanged.

use yrs::encoding::read::{Cursor, Error as ReadError, Read};
use yrs::encoding::write::Write;
use yrs::updates::decoder::DecoderV1;
use yrs::updates::encoder::{Encoder, EncoderV1};

/// Top-level message opcodes. Values are fixed by the protocol and must match
/// `packages/common/src/types.ts`'s `MessageType` exactly.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum MessageType {
    Sync = 0,
    Awareness = 1,
    Auth = 2,
    QueryAwareness = 3,
    /// Same wire shape as [`MessageType::Sync`] but signals to the client that
    /// it must not respond with another `SyncStep1`.
    SyncReply = 4,
    Stateless = 5,
    /// Server-internal opcode used by the Redis extension to fan a stateless
    /// payload across nodes. A client may never send this.
    BroadcastStateless = 6,
    Close = 7,
    SyncStatus = 8,
    Ping = 9,
    Pong = 10,
}

impl MessageType {
    pub fn from_u64(v: u64) -> Option<Self> {
        Some(match v {
            0 => Self::Sync,
            1 => Self::Awareness,
            2 => Self::Auth,
            3 => Self::QueryAwareness,
            4 => Self::SyncReply,
            5 => Self::Stateless,
            6 => Self::BroadcastStateless,
            7 => Self::Close,
            8 => Self::SyncStatus,
            9 => Self::Ping,
            10 => Self::Pong,
            _ => return None,
        })
    }
}

/// Authentication sub-message tags, matching `AuthMessageType` in
/// `packages/common/src/auth.ts`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum AuthMessageType {
    Token = 0,
    PermissionDenied = 1,
    Authenticated = 2,
}

/// Sub-message tags for the Yjs sync protocol (`y-protocols/sync`).
pub const SYNC_STEP_1: u64 = 0;
pub const SYNC_STEP_2: u64 = 1;
pub const SYNC_UPDATE: u64 = 2;

/// A decoded inbound frame. The `document_name` is the raw routing key as sent
/// by the client (which may be `documentName\0sessionId`).
pub struct IncomingFrame<'a> {
    pub routing_key: String,
    pub message_type: MessageType,
    /// Decoder positioned immediately after the message-type varuint.
    pub decoder: DecoderV1<'a>,
}

impl<'a> IncomingFrame<'a> {
    /// Parse the envelope (routing key + message type) of a frame, leaving the
    /// decoder positioned at the payload. Returns `None` for single-byte
    /// Ping/Pong frames (handled separately by the connection layer).
    pub fn parse(data: &'a [u8]) -> Result<Self, ReadError> {
        let mut decoder = DecoderV1::new(Cursor::new(data));
        let routing_key = decoder.read_string()?.to_string();
        let raw_type: u64 = decoder.read_var()?;
        let message_type =
            MessageType::from_u64(raw_type).ok_or(ReadError::UnexpectedValue)?;
        Ok(Self {
            routing_key,
            message_type,
            decoder,
        })
    }
}

/// Split a routing key `documentName\0sessionId` into its parts. A plain
/// document name yields `(name, None)`.
pub fn parse_routing_key(key: &str) -> (&str, Option<&str>) {
    match key.find('\0') {
        Some(idx) => (&key[..idx], Some(&key[idx + 1..])),
        None => (key, None),
    }
}

/// Build a routing key from a document name and optional session id.
pub fn message_address(document_name: &str, session_id: Option<&str>) -> String {
    match session_id {
        Some(sid) => format!("{document_name}\0{sid}"),
        None => document_name.to_string(),
    }
}

/// Builder for outbound frames. Mirrors `OutgoingMessage.ts`: each frame begins
/// with the document address varString, then the opcode, then the payload.
pub struct OutgoingMessage {
    encoder: EncoderV1,
}

impl OutgoingMessage {
    /// Start a new frame addressed to `address` (a document name or
    /// `documentName\0sessionId` routing key).
    pub fn new(address: &str) -> Self {
        let mut encoder = EncoderV1::new();
        encoder.write_string(address);
        Self { encoder }
    }

    pub fn write_opcode(&mut self, ty: MessageType) -> &mut Self {
        self.encoder.write_var(ty as u64);
        self
    }

    /// Begin a `Sync` message (caller appends a sync sub-message next).
    pub fn sync(mut self) -> Self {
        self.write_opcode(MessageType::Sync);
        self
    }

    /// Begin a `SyncReply` message.
    pub fn sync_reply(mut self) -> Self {
        self.write_opcode(MessageType::SyncReply);
        self
    }

    /// Append a raw, already-encoded sync sub-message body (e.g. a `SyncStep1`
    /// or `Update` produced via `yrs::sync::SyncMessage::encode`).
    pub fn write_sync_payload(mut self, payload: &[u8]) -> Self {
        self.encoder.write_all(payload);
        self
    }

    /// Write an awareness update frame: opcode 1 followed by the awareness
    /// update wrapped as a varUint8Array.
    pub fn awareness_update(mut self, update: &[u8]) -> Self {
        self.write_opcode(MessageType::Awareness);
        self.encoder.write_buf(update);
        self
    }

    pub fn query_awareness(mut self) -> Self {
        self.write_opcode(MessageType::QueryAwareness);
        self
    }

    pub fn authenticated(mut self, read_only: bool) -> Self {
        self.write_opcode(MessageType::Auth);
        self.encoder.write_var(AuthMessageType::Authenticated as u64);
        self.encoder
            .write_string(if read_only { "readonly" } else { "read-write" });
        self
    }

    pub fn permission_denied(mut self, reason: &str) -> Self {
        self.write_opcode(MessageType::Auth);
        self.encoder
            .write_var(AuthMessageType::PermissionDenied as u64);
        self.encoder.write_string(reason);
        self
    }

    pub fn token_sync_request(mut self) -> Self {
        self.write_opcode(MessageType::Auth);
        self.encoder.write_var(AuthMessageType::Token as u64);
        self
    }

    pub fn stateless(mut self, payload: &str) -> Self {
        self.write_opcode(MessageType::Stateless);
        self.encoder.write_string(payload);
        self
    }

    pub fn broadcast_stateless(mut self, payload: &str) -> Self {
        self.write_opcode(MessageType::BroadcastStateless);
        self.encoder.write_string(payload);
        self
    }

    pub fn sync_status(mut self, update_saved: bool) -> Self {
        self.write_opcode(MessageType::SyncStatus);
        self.encoder.write_var(if update_saved { 1u64 } else { 0u64 });
        self
    }

    pub fn close_message(mut self, reason: &str) -> Self {
        self.write_opcode(MessageType::Close);
        self.encoder.write_string(reason);
        self
    }

    pub fn into_bytes(self) -> Vec<u8> {
        self.encoder.to_vec()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roundtrip_auth_token_frame() {
        // Build the frame a client sends: address + Auth + Token + token + version.
        let mut enc = EncoderV1::new();
        enc.write_string("doc1");
        enc.write_var(MessageType::Auth as u64);
        enc.write_var(AuthMessageType::Token as u64);
        enc.write_string("secret-token");
        enc.write_string("3.0.0");
        let bytes = enc.to_vec();

        let mut frame = IncomingFrame::parse(&bytes).unwrap();
        assert_eq!(frame.routing_key, "doc1");
        assert_eq!(frame.message_type, MessageType::Auth);
        let auth_type: u64 = frame.decoder.read_var().unwrap();
        assert_eq!(auth_type, AuthMessageType::Token as u64);
        assert_eq!(frame.decoder.read_string().unwrap(), "secret-token");
        assert_eq!(frame.decoder.read_string().unwrap(), "3.0.0");
    }

    #[test]
    fn routing_key_split() {
        assert_eq!(parse_routing_key("doc"), ("doc", None));
        assert_eq!(parse_routing_key("doc\0sid"), ("doc", Some("sid")));
        assert_eq!(message_address("doc", Some("sid")), "doc\0sid");
        assert_eq!(message_address("doc", None), "doc");
    }

    #[test]
    fn authenticated_frame_shape() {
        let bytes = OutgoingMessage::new("doc").authenticated(false).into_bytes();
        let mut dec = DecoderV1::new(Cursor::new(&bytes));
        assert_eq!(dec.read_string().unwrap(), "doc");
        let ty: u64 = dec.read_var().unwrap();
        assert_eq!(ty, MessageType::Auth as u64);
        let at: u64 = dec.read_var().unwrap();
        assert_eq!(at, AuthMessageType::Authenticated as u64);
        assert_eq!(dec.read_string().unwrap(), "read-write");
    }
}
