//! Micro-benchmarks for the hot paths: wire-frame encode/decode and CRDT
//! update application.

use criterion::{black_box, criterion_group, criterion_main, Criterion, Throughput};
use hocuspocus::protocol::{IncomingFrame, OutgoingMessage};
use yrs::sync::SyncMessage;
use yrs::updates::decoder::Decode;
use yrs::updates::encoder::{Encode, Encoder, EncoderV1};
use yrs::{Doc, GetString, Text, Transact, Update};

fn make_update(content: &str) -> Vec<u8> {
    let doc = Doc::new();
    let text = doc.get_or_insert_text("content");
    let mut txn = doc.transact_mut();
    text.push(&mut txn, content);
    txn.encode_update_v1()
}

fn encode_sync(msg: &SyncMessage) -> Vec<u8> {
    let mut e = EncoderV1::new();
    msg.encode(&mut e);
    e.to_vec()
}

fn bench_frame_encode(c: &mut Criterion) {
    let update = make_update("the quick brown fox jumps over the lazy dog");
    let body = encode_sync(&SyncMessage::Update(update));
    let mut group = c.benchmark_group("frame_encode");
    group.throughput(Throughput::Elements(1));
    group.bench_function("sync_update", |b| {
        b.iter(|| {
            let frame = OutgoingMessage::new(black_box("my-document"))
                .sync()
                .write_sync_payload(black_box(&body))
                .into_bytes();
            black_box(frame);
        });
    });
    group.finish();
}

fn bench_frame_decode(c: &mut Criterion) {
    let update = make_update("the quick brown fox jumps over the lazy dog");
    let body = encode_sync(&SyncMessage::Update(update));
    let frame = OutgoingMessage::new("my-document")
        .sync()
        .write_sync_payload(&body)
        .into_bytes();

    let mut group = c.benchmark_group("frame_decode");
    group.throughput(Throughput::Elements(1));
    group.bench_function("sync_update", |b| {
        b.iter(|| {
            let mut parsed = IncomingFrame::parse(black_box(&frame)).unwrap();
            let msg = SyncMessage::decode(&mut parsed.decoder).unwrap();
            black_box(msg);
        });
    });
    group.finish();
}

fn bench_apply_updates(c: &mut Criterion) {
    let writer = Doc::new();
    let wtext = writer.get_or_insert_text("content");
    let mut updates = Vec::with_capacity(1000);
    for i in 0..1000 {
        let mut txn = writer.transact_mut();
        wtext.push(&mut txn, &format!("{i} "));
        updates.push(txn.encode_update_v1());
    }

    let mut group = c.benchmark_group("apply_updates");
    group.throughput(Throughput::Elements(updates.len() as u64));
    group.bench_function("1000_incremental", |b| {
        b.iter(|| {
            let doc = Doc::new();
            let _t = doc.get_or_insert_text("content");
            for u in &updates {
                let upd = Update::decode_v1(u).unwrap();
                let mut txn = doc.transact_mut();
                txn.apply_update(upd).unwrap();
            }
            let t = doc.get_or_insert_text("content");
            let txn = doc.transact();
            black_box(t.get_string(&txn).len());
        });
    });
    group.finish();
}

criterion_group!(benches, bench_frame_encode, bench_frame_decode, bench_apply_updates);
criterion_main!(benches);
