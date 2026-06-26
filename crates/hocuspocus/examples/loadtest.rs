//! End-to-end load test: spins up the server in-process, connects N WebSocket
//! clients across M documents, has each push a stream of updates, and reports
//! sustained message throughput plus process RSS.
//!
//! Run: `cargo run --release --example loadtest -- [clients] [updates_per_client] [docs]`

use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use futures_util::{SinkExt, StreamExt};
use hocuspocus::extensions::database::{Database, MemoryStorage};
use hocuspocus::Hocuspocus;
use tokio::net::TcpListener;
use tokio_tungstenite::tungstenite::Message as WsMessage;
use yrs::encoding::write::Write as _;
use yrs::sync::SyncMessage;
use yrs::updates::encoder::{Encode, Encoder, EncoderV1};
use yrs::{Doc, StateVector, Text, Transact};

fn auth_frame(doc: &str) -> Vec<u8> {
    let mut e = EncoderV1::new();
    e.write_string(doc);
    e.write_var(2u64);
    e.write_var(0u64);
    e.write_string("token");
    e.write_string("loadtest");
    e.to_vec()
}

fn sync_frame(doc: &str, msg: &SyncMessage) -> Vec<u8> {
    let mut e = EncoderV1::new();
    e.write_string(doc);
    e.write_var(0u64);
    msg.encode(&mut e);
    e.to_vec()
}

#[cfg(target_os = "macos")]
fn rss_bytes() -> u64 {
    // mach task_basic_info resident_size via `ps` fallback for portability.
    use std::process::Command;
    let pid = std::process::id();
    let out = Command::new("ps")
        .args(["-o", "rss=", "-p", &pid.to_string()])
        .output()
        .ok();
    out.and_then(|o| String::from_utf8(o.stdout).ok())
        .and_then(|s| s.trim().parse::<u64>().ok())
        .map(|kb| kb * 1024)
        .unwrap_or(0)
}

#[cfg(not(target_os = "macos"))]
fn rss_bytes() -> u64 {
    use std::fs;
    fs::read_to_string("/proc/self/statm")
        .ok()
        .and_then(|s| s.split_whitespace().nth(1).map(|x| x.to_string()))
        .and_then(|pages| pages.parse::<u64>().ok())
        .map(|pages| pages * 4096)
        .unwrap_or(0)
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let mut args = std::env::args().skip(1);
    let clients: usize = args.next().and_then(|s| s.parse().ok()).unwrap_or(200);
    let updates_per_client: usize = args.next().and_then(|s| s.parse().ok()).unwrap_or(200);
    let docs: usize = args.next().and_then(|s| s.parse().ok()).unwrap_or(20);

    // Either drive an external server (HP_ADDR=host:port) — so its RSS can be
    // measured independently — or start one in-process.
    let external = std::env::var("HP_ADDR").ok();
    let (addr, shared) = if let Some(ext) = external {
        (ext.parse::<SocketAddr>()?, None)
    } else {
        let server = Hocuspocus::builder()
            .extension(Arc::new(Database::new(MemoryStorage::new())))
            .build();
        let shared = server.shared();
        let app = server
            .router()
            .into_make_service_with_connect_info::<SocketAddr>();
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let addr = listener.local_addr()?;
        tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        tokio::time::sleep(Duration::from_millis(100)).await;
        (addr, Some(shared))
    };

    let rss_idle = rss_bytes();
    println!(
        "hocuspocus-rs loadtest: {clients} clients x {updates_per_client} updates across {docs} docs"
    );

    let received = Arc::new(AtomicU64::new(0));
    let start = Instant::now();

    let mut handles = Vec::new();
    for c in 0..clients {
        let addr = addr;
        let doc_name = format!("doc-{}", c % docs);
        let received = received.clone();
        handles.push(tokio::spawn(async move {
            let (mut ws, _) = tokio_tungstenite::connect_async(format!("ws://{addr}/"))
                .await
                .unwrap();
            ws.send(WsMessage::Binary(auth_frame(&doc_name).into()))
                .await
                .unwrap();
            ws.send(WsMessage::Binary(
                sync_frame(&doc_name, &SyncMessage::SyncStep1(StateVector::default())).into(),
            ))
            .await
            .unwrap();

            // Reader task: count inbound frames.
            let recv = received.clone();
            let (mut sink, mut stream) = ws.split();
            let reader = tokio::spawn(async move {
                while let Some(Ok(msg)) = stream.next().await {
                    if let WsMessage::Binary(_) = msg {
                        recv.fetch_add(1, Ordering::Relaxed);
                    }
                }
            });

            // Writer: stream incremental updates.
            let local = Doc::new();
            let text = local.get_or_insert_text("content");
            for i in 0..updates_per_client {
                let update = {
                    let mut txn = local.transact_mut();
                    text.push(&mut txn, &format!("{i},"));
                    txn.encode_update_v1()
                };
                if sink
                    .send(WsMessage::Binary(
                        sync_frame(&doc_name, &SyncMessage::Update(update)).into(),
                    ))
                    .await
                    .is_err()
                {
                    break;
                }
            }
            // Give the server a moment to flush broadcasts; optionally hold the
            // connection open so server memory can be sampled under live load.
            let hold = std::env::var("HP_HOLD")
                .ok()
                .and_then(|s| s.parse::<u64>().ok())
                .unwrap_or(0);
            tokio::time::sleep(Duration::from_millis(200 + hold * 1000)).await;
            reader.abort();
        }));
    }

    for h in handles {
        let _ = h.await;
    }
    let elapsed = start.elapsed();
    let rss_peak = rss_bytes();

    let total_sent = (clients * updates_per_client) as u64;
    let recv_total = received.load(Ordering::Relaxed);
    println!("---");
    println!("updates sent:        {total_sent}");
    println!("frames received:     {recv_total}");
    println!("wall time:           {:.3}s", elapsed.as_secs_f64());
    println!(
        "send throughput:     {:.0} updates/s",
        total_sent as f64 / elapsed.as_secs_f64()
    );
    println!(
        "broadcast throughput:{:.0} frames/s",
        recv_total as f64 / elapsed.as_secs_f64()
    );
    if let Some(shared) = &shared {
        println!("resident docs:       {}", shared.documents_count());
    }
    println!(
        "RSS idle -> peak:    {:.1} MiB -> {:.1} MiB",
        rss_idle as f64 / 1048576.0,
        rss_peak as f64 / 1048576.0
    );
    Ok(())
}
