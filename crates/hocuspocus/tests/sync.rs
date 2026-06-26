//! End-to-end protocol tests: drive the server with a raw WebSocket client
//! speaking the exact Hocuspocus wire protocol, exercising the auth handshake
//! and Yjs sync between two clients.

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use hocuspocus::extensions::database::{Database, MemoryStorage};
use hocuspocus::Hocuspocus;
use tokio::net::TcpListener;
use tokio_tungstenite::tungstenite::Message as WsMessage;
use yrs::encoding::read::Read as _;
use yrs::encoding::write::Write as _;
use yrs::sync::SyncMessage;
use yrs::updates::decoder::{Decode, Decoder as _, DecoderV1};
use yrs::updates::encoder::{Encode, Encoder, EncoderV1};
use yrs::{Doc, GetString, StateVector, Text, Transact, Update};

const AUTH: u64 = 2;
const SYNC: u64 = 0;
const AWARENESS: u64 = 1;

async fn start_server(storage: MemoryStorage) -> SocketAddr {
    let server = Hocuspocus::builder()
        .extension(Arc::new(Database::new(storage)))
        .build();
    let app = server
        .router()
        .into_make_service_with_connect_info::<SocketAddr>();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    // Give the listener a beat.
    tokio::time::sleep(Duration::from_millis(50)).await;
    addr
}

type Ws = tokio_tungstenite::WebSocketStream<
    tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>,
>;

async fn connect(addr: SocketAddr) -> Ws {
    let (ws, _) = tokio_tungstenite::connect_async(format!("ws://{addr}/"))
        .await
        .unwrap();
    ws
}

fn auth_frame(doc: &str, token: &str) -> Vec<u8> {
    let mut e = EncoderV1::new();
    e.write_string(doc);
    e.write_var(AUTH);
    e.write_var(0u64); // AuthMessageType::Token
    e.write_string(token);
    e.write_string("rust-test");
    e.to_vec()
}

fn sync_frame(doc: &str, msg: &SyncMessage) -> Vec<u8> {
    let mut e = EncoderV1::new();
    e.write_string(doc);
    e.write_var(SYNC);
    msg.encode(&mut e);
    e.to_vec()
}

async fn send(ws: &mut Ws, bytes: Vec<u8>) {
    ws.send(WsMessage::Binary(bytes.into())).await.unwrap();
}

/// Read the next binary frame, returning (opcode, remaining decoder bytes).
async fn next_binary(ws: &mut Ws) -> Vec<u8> {
    loop {
        match tokio::time::timeout(Duration::from_secs(5), ws.next())
            .await
            .expect("timed out waiting for frame")
        {
            Some(Ok(WsMessage::Binary(b))) => return b.to_vec(),
            Some(Ok(_)) => continue,
            other => panic!("unexpected ws message: {other:?}"),
        }
    }
}

fn parse(frame: &[u8]) -> (String, u64, Vec<u8>) {
    let mut d = DecoderV1::new(yrs::encoding::read::Cursor::new(frame));
    let addr = d.read_string().unwrap().to_string();
    let opcode: u64 = d.read_var().unwrap();
    let rest = d.read_to_end().unwrap().to_vec();
    (addr, opcode, rest)
}

/// Authenticate and perform the initial sync, returning once authenticated.
async fn authenticate(ws: &mut Ws, doc: &str) {
    send(ws, auth_frame(doc, "token")).await;
    // First server frame should be the Auth/Authenticated message.
    let frame = next_binary(ws).await;
    let (_addr, opcode, rest) = parse(&frame);
    assert_eq!(opcode, AUTH, "expected auth response");
    let mut d = DecoderV1::new(yrs::encoding::read::Cursor::new(&rest));
    let auth_type: u64 = d.read_var().unwrap();
    assert_eq!(auth_type, 2, "expected Authenticated");
    assert_eq!(d.read_string().unwrap(), "read-write");
}

#[tokio::test]
async fn two_clients_sync_a_document() {
    let storage = MemoryStorage::new();
    let addr = start_server(storage).await;

    // --- Client A: connect, auth, push an edit ---
    let mut a = connect(addr).await;
    authenticate(&mut a, "note-1").await;

    // SyncStep1 with empty state vector.
    send(&mut a, sync_frame("note-1", &SyncMessage::SyncStep1(StateVector::default()))).await;

    // Build an edit locally and send it as an update.
    let local = Doc::new();
    let text = local.get_or_insert_text("content");
    let update = {
        let mut txn = local.transact_mut();
        text.push(&mut txn, "hello world");
        txn.encode_update_v1()
    };
    send(&mut a, sync_frame("note-1", &SyncMessage::Update(update))).await;

    // Drain a few frames so the server processes the update (sync status, etc.).
    for _ in 0..3 {
        let _ = tokio::time::timeout(Duration::from_millis(300), next_binary(&mut a)).await;
    }

    // --- Client B: connect, auth, sync, expect A's edit ---
    let mut b = connect(addr).await;
    authenticate(&mut b, "note-1").await;
    send(&mut b, sync_frame("note-1", &SyncMessage::SyncStep1(StateVector::default()))).await;

    let remote = Doc::new();
    let rtext = remote.get_or_insert_text("content");

    // Read frames until we've applied a SyncStep2 / Update carrying content.
    let mut got = String::new();
    for _ in 0..6 {
        let Ok(frame) = tokio::time::timeout(Duration::from_secs(2), next_binary(&mut b)).await
        else {
            break;
        };
        let (_addr, opcode, rest) = parse(&frame);
        if opcode == SYNC {
            let mut d = DecoderV1::new(yrs::encoding::read::Cursor::new(&rest));
            if let Ok(msg) = SyncMessage::decode(&mut d) {
                match msg {
                    SyncMessage::SyncStep2(u) | SyncMessage::Update(u) => {
                        if let Ok(upd) = Update::decode_v1(&u) {
                            let mut txn = remote.transact_mut();
                            let _ = txn.apply_update(upd);
                        }
                    }
                    SyncMessage::SyncStep1(_) => {}
                }
            }
        }
        let txn = remote.transact();
        got = rtext.get_string(&txn);
        if got.contains("hello world") {
            break;
        }
    }

    assert_eq!(got, "hello world", "client B should observe client A's edit");
}

#[tokio::test]
async fn awareness_is_relayed_between_clients() {
    let addr = start_server(MemoryStorage::new()).await;

    let mut a = connect(addr).await;
    authenticate(&mut a, "room").await;
    send(&mut a, sync_frame("room", &SyncMessage::SyncStep1(StateVector::default()))).await;

    let mut b = connect(addr).await;
    authenticate(&mut b, "room").await;

    // A publishes awareness (client id 42, some state).
    let adoc = Doc::new();
    let mut awareness = yrs::sync::Awareness::with_clock(adoc, yrs::sync::time::SystemClock);
    awareness.set_local_state_raw(r#"{"user":{"name":"alice"}}"#);
    let update = awareness.update().unwrap();
    let mut e = EncoderV1::new();
    e.write_string("room");
    e.write_var(AWARENESS);
    e.write_buf(update.encode_v1());
    send(&mut a, e.to_vec()).await;

    // B should receive an awareness frame mentioning alice.
    let mut saw_alice = false;
    for _ in 0..6 {
        let Ok(frame) = tokio::time::timeout(Duration::from_secs(2), next_binary(&mut b)).await
        else {
            break;
        };
        let (_addr, opcode, rest) = parse(&frame);
        if opcode == AWARENESS {
            let text = String::from_utf8_lossy(&rest);
            if text.contains("alice") {
                saw_alice = true;
                break;
            }
        }
    }
    assert!(saw_alice, "client B should receive client A's awareness state");
}
