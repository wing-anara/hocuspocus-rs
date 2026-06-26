//! Horizontal scaling test: two independent server nodes backed by the same
//! Redis. An edit made on node A must propagate to a client connected to node
//! B. Requires a Redis on 127.0.0.1:6379; skipped (passes trivially) otherwise.

#![cfg(feature = "redis-scaling")]

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use hocuspocus::extensions::database::{Database, MemoryStorage};
use hocuspocus::extensions::redis::{RedisConfig, RedisScaling};
use hocuspocus::Hocuspocus;
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

async fn redis_available() -> bool {
    match redis::Client::open("redis://127.0.0.1:6379") {
        Ok(c) => c.get_multiplexed_async_connection().await.is_ok(),
        Err(_) => false,
    }
}

async fn start_node(identifier: &str) -> SocketAddr {
    let redis = RedisScaling::connect(
        RedisConfig::new("redis://127.0.0.1:6379").identifier(identifier),
    )
    .await
    .unwrap();
    let server = Hocuspocus::builder()
        .extension(redis)
        .extension(Arc::new(Database::new(MemoryStorage::new())))
        .build();
    server.prepare().await;
    let app = server
        .router()
        .into_make_service_with_connect_info::<SocketAddr>();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    addr
}

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
    let _ = next_binary(ws).await;
}

#[tokio::test]
async fn edit_on_node_a_reaches_client_on_node_b() {
    if !redis_available().await {
        eprintln!("skipping: no redis on 127.0.0.1:6379");
        return;
    }

    let node_a = start_node("node-a").await;
    let node_b = start_node("node-b").await;
    tokio::time::sleep(Duration::from_millis(150)).await;

    // Client B connects to node B first and syncs the (empty) doc.
    let mut b = connect(node_b).await;
    authenticate(&mut b, "shared").await;
    send(&mut b, sync_frame("shared", &SyncMessage::SyncStep1(StateVector::default()))).await;
    // Drain initial sync frames.
    for _ in 0..3 {
        let _ = tokio::time::timeout(Duration::from_millis(200), next_binary(&mut b)).await;
    }

    // Client A connects to node A and writes content.
    let mut a = connect(node_a).await;
    authenticate(&mut a, "shared").await;
    send(&mut a, sync_frame("shared", &SyncMessage::SyncStep1(StateVector::default()))).await;
    let local = Doc::new();
    let text = local.get_or_insert_text("content");
    let update = {
        let mut txn = local.transact_mut();
        text.push(&mut txn, "cross-node!");
        txn.encode_update_v1()
    };
    send(&mut a, sync_frame("shared", &SyncMessage::Update(update))).await;

    // Client B should receive the update relayed via Redis.
    let remote = Doc::new();
    let rtext = remote.get_or_insert_text("content");
    let mut got = String::new();
    for _ in 0..12 {
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
        if got.contains("cross-node!") {
            break;
        }
    }
    assert_eq!(got, "cross-node!", "edit on node A should reach client on node B via Redis");
}
