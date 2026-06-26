//! Persistence + unload + reload round trip: an edit made by one client is
//! stored, the document is unloaded once idle, and a later client reloads it
//! from storage.

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use hocuspocus::extensions::database::{Database, MemoryStorage, Storage};
use hocuspocus::{Configuration, Hocuspocus};
use tokio::net::TcpListener;
use tokio_tungstenite::tungstenite::Message as WsMessage;
use yrs::encoding::read::{Cursor, Read as _};
use yrs::encoding::write::Write as _;
use yrs::sync::SyncMessage;
use yrs::updates::decoder::{Decode, Decoder as _, DecoderV1};
use yrs::updates::encoder::{Encode, Encoder, EncoderV1};
use yrs::{Doc, GetString, StateVector, Text, Transact, Update};

type Ws = tokio_tungstenite::WebSocketStream<
    tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>,
>;

async fn connect(addr: SocketAddr) -> Ws {
    let (ws, _) = tokio_tungstenite::connect_async(format!("ws://{addr}/"))
        .await
        .unwrap();
    ws
}

fn auth_frame(doc: &str) -> Vec<u8> {
    let mut e = EncoderV1::new();
    e.write_string(doc);
    e.write_var(2u64);
    e.write_var(0u64);
    e.write_string("token");
    e.write_string("rust-test");
    e.to_vec()
}

fn sync_frame(doc: &str, msg: &SyncMessage) -> Vec<u8> {
    let mut e = EncoderV1::new();
    e.write_string(doc);
    e.write_var(0u64);
    msg.encode(&mut e);
    e.to_vec()
}

async fn send(ws: &mut Ws, bytes: Vec<u8>) {
    ws.send(WsMessage::Binary(bytes.into())).await.unwrap();
}

async fn next_binary(ws: &mut Ws) -> Option<Vec<u8>> {
    match tokio::time::timeout(Duration::from_secs(3), ws.next()).await {
        Ok(Some(Ok(WsMessage::Binary(b)))) => Some(b.to_vec()),
        Ok(Some(Ok(_))) => Some(Vec::new()),
        _ => None,
    }
}

fn parse(frame: &[u8]) -> (u64, Vec<u8>) {
    let mut d = DecoderV1::new(Cursor::new(frame));
    let _addr = d.read_string().unwrap().to_string();
    let opcode: u64 = d.read_var().unwrap();
    let rest = d.read_to_end().unwrap().to_vec();
    (opcode, rest)
}

async fn authenticate(ws: &mut Ws, doc: &str) {
    send(ws, auth_frame(doc)).await;
    let _ = next_binary(ws).await; // Authenticated
}

#[tokio::test]
async fn document_is_persisted_and_reloaded() {
    let storage = MemoryStorage::new();
    let mut config = Configuration::default();
    config.debounce = Duration::from_millis(50);
    config.max_debounce = Duration::from_millis(100);
    // unload_immediately stays true (default): store + unload on last disconnect.

    let server = Hocuspocus::builder()
        .configuration(config)
        .extension(Arc::new(Database::from_arc(Arc::new(storage.clone()))))
        .build();
    let app = server
        .router()
        .into_make_service_with_connect_info::<SocketAddr>();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    tokio::time::sleep(Duration::from_millis(50)).await;

    // Client A writes content, then disconnects.
    {
        let mut a = connect(addr).await;
        authenticate(&mut a, "doc").await;
        send(&mut a, sync_frame("doc", &SyncMessage::SyncStep1(StateVector::default()))).await;

        let local = Doc::new();
        let text = local.get_or_insert_text("content");
        let update = {
            let mut txn = local.transact_mut();
            text.push(&mut txn, "persisted!");
            txn.encode_update_v1()
        };
        send(&mut a, sync_frame("doc", &SyncMessage::Update(update))).await;
        // Wait for the sync status ack so the server has applied the update.
        let _ = next_binary(&mut a).await;
        let _ = next_binary(&mut a).await;
        // Drop the socket -> disconnect.
    }

    // Allow store + unload to run.
    tokio::time::sleep(Duration::from_millis(400)).await;

    // Storage should now contain the document.
    let stored = storage.fetch("doc").await.unwrap();
    assert!(stored.is_some(), "document should have been persisted");

    // Client B connects fresh; the server reloads from storage.
    let mut b = connect(addr).await;
    authenticate(&mut b, "doc").await;
    send(&mut b, sync_frame("doc", &SyncMessage::SyncStep1(StateVector::default()))).await;

    let remote = Doc::new();
    let rtext = remote.get_or_insert_text("content");
    let mut got = String::new();
    for _ in 0..6 {
        let Some(frame) = next_binary(&mut b).await else { break };
        if frame.is_empty() {
            continue;
        }
        let (opcode, rest) = parse(&frame);
        if opcode == 0 {
            let mut d = DecoderV1::new(Cursor::new(&rest));
            if let Ok(SyncMessage::SyncStep2(u)) | Ok(SyncMessage::Update(u)) =
                SyncMessage::decode(&mut d)
            {
                if let Ok(upd) = Update::decode_v1(&u) {
                    let mut txn = remote.transact_mut();
                    let _ = txn.apply_update(upd);
                }
            }
        }
        got = rtext.get_string(&remote.transact());
        if got.contains("persisted!") {
            break;
        }
    }
    assert_eq!(got, "persisted!", "client B should reload persisted content");
}
