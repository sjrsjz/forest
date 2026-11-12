//! Reproducible comparison against expanded structural names for this workload.
//! cargo run -p forest-protocol --example symbols --release --locked --offline
use std::{
    collections::{BTreeMap, HashSet},
    hint::black_box,
    time::{Duration, Instant},
};

use bincode::Options;
use forest_protocol::{Value, abi};
use forest_serde::adt::{Symbol, VariantPayloadValue};
use serde::{Deserialize, Serialize};

// The former derived Value layout, including all branches so the baseline has
// the same enum dispatch as the original implementation, not a reduced DTO.
#[derive(Serialize, Deserialize)]
enum Expanded {
    I32(i32),
    I64(i64),
    Bool(bool),
    String(String),
    Bytes(Vec<u8>),
    Some(Box<Expanded>),
    None,
    Tuple(Vec<Expanded>),
    Array(Vec<Expanded>),
    Map(Vec<(Expanded, Expanded)>),
    Variant(String, ExpandedPayload),
    Record(BTreeMap<String, Expanded>),
}

#[derive(Serialize, Deserialize)]
enum ExpandedPayload {
    Unnamed(Vec<Expanded>),
    Named(BTreeMap<String, Expanded>),
}

fn expanded(value: &Value) -> Expanded {
    match value {
        Value::I32(n) => Expanded::I32(*n),
        Value::I64(n) => Expanded::I64(*n),
        Value::Bool(v) => Expanded::Bool(*v),
        Value::String(v) => Expanded::String(v.clone()),
        Value::Bytes(v) => Expanded::Bytes(v.clone()),
        Value::Tuple(items) => Expanded::Tuple(items.iter().map(expanded).collect()),
        Value::Array(items) => Expanded::Array(items.iter().map(expanded).collect()),
        Value::Map(items) => Expanded::Map(
            items
                .iter()
                .map(|(k, v)| (expanded(k), expanded(v)))
                .collect(),
        ),
        Value::Variant(tag, payload) => Expanded::Variant(
            tag.to_string(),
            match payload {
                VariantPayloadValue::Unnamed(items) => {
                    ExpandedPayload::Unnamed(items.iter().map(expanded).collect())
                }
                VariantPayloadValue::Named(fields) => ExpandedPayload::Named(
                    fields
                        .iter()
                        .map(|(name, v)| (name.to_string(), expanded(v)))
                        .collect(),
                ),
            },
        ),
        Value::Record(fields) => Expanded::Record(
            fields
                .iter()
                .map(|(name, value)| (name.to_string(), expanded(value)))
                .collect(),
        ),
    }
}

fn median(mut run: impl FnMut(), iterations: u32) -> Duration {
    run();
    let mut samples: Vec<_> = (0..7)
        .map(|_| {
            let start = Instant::now();
            for _ in 0..iterations {
                run();
            }
            start.elapsed() / iterations
        })
        .collect();
    samples.sort();
    samples[samples.len() / 2]
}

fn expanded_codec() -> impl Options {
    bincode::DefaultOptions::new()
        .with_fixint_encoding()
        .with_little_endian()
        .with_limit(abi::MAX_FRAME_BYTES as u64)
        .reject_trailing_bytes()
}

fn byte_buffers() {
    let value = Value::Bytes(vec![0xa5; 4 * 1024 * 1024]);
    let old = expanded(&value);
    let old_bytes = expanded_codec().serialize(&old).unwrap();
    let bytes = abi::encode(&value).unwrap();
    assert_eq!(abi::decode::<Value>(&bytes).unwrap(), value);
    let old_encode = median(
        || {
            black_box(expanded_codec().serialize(black_box(&old)).unwrap());
        },
        10,
    );
    let encode = median(
        || {
            black_box(abi::encode(black_box(&value)).unwrap());
        },
        10,
    );
    let old_decode = median(
        || {
            black_box(
                expanded_codec()
                    .deserialize::<Expanded>(black_box(&old_bytes))
                    .unwrap(),
            );
        },
        10,
    );
    let decode = median(
        || {
            black_box(abi::decode::<Value>(black_box(&bytes)).unwrap());
        },
        10,
    );
    println!(
        "bytes_payload=4MiB encoded={} -> {} encode={old_encode:?} -> {encode:?} decode={old_decode:?} -> {decode:?}",
        old_bytes.len(),
        bytes.len()
    );
}

fn main() {
    for (rows, iterations) in [(1, 10_000), (10_000, 10)] {
        let fields = [
            "message_identifier",
            "source_group_identifier",
            "sender_identifier",
            "received_timestamp",
            "sequence_number",
            "context_generation",
            "archive_position",
            "embedding_generation",
        ];
        let value = Value::Array(
            (0..rows)
                .map(|row| {
                    Value::Record(
                        fields
                            .iter()
                            .map(|name| (Symbol::from_static(name), Value::I64(row)))
                            .collect(),
                    )
                })
                .collect(),
        );
        let old = expanded(&value);
        let codec = expanded_codec;
        let old_bytes = codec().serialize(&old).unwrap();
        let bytes = abi::encode(&value).unwrap();
        assert_eq!(abi::decode::<Value>(&bytes).unwrap(), value);
        let Expanded::Array(old_rows) = codec().deserialize::<Expanded>(&old_bytes).unwrap() else {
            unreachable!()
        };
        let Value::Array(new_rows) = abi::decode::<Value>(&bytes).unwrap() else {
            unreachable!()
        };
        let old_names: HashSet<_> = old_rows
            .iter()
            .flat_map(|row| {
                let Expanded::Record(fields) = row else {
                    unreachable!()
                };
                fields.keys().map(|name| name.as_ptr())
            })
            .collect();
        let names: HashSet<_> = new_rows
            .iter()
            .flat_map(|row| {
                let Value::Record(fields) = row else {
                    unreachable!()
                };
                fields.keys().map(|name| name.as_str().as_ptr())
            })
            .collect();
        println!(
            "rows={rows} distinct name buffers={} -> {}",
            old_names.len(),
            names.len()
        );
        drop(old_rows);
        drop(new_rows);
        let old_encode = median(
            || {
                black_box(codec().serialize(black_box(&old)).unwrap());
            },
            iterations,
        );
        let encode = median(
            || {
                black_box(abi::encode(black_box(&value)).unwrap());
            },
            iterations,
        );
        let old_decode = median(
            || {
                black_box(
                    codec()
                        .deserialize::<Expanded>(black_box(&old_bytes))
                        .unwrap(),
                );
            },
            iterations,
        );
        let decode = median(
            || {
                black_box(abi::decode::<Value>(black_box(&bytes)).unwrap());
            },
            iterations,
        );
        println!(
            "rows={rows} bytes={} -> {} ({:.1}% smaller) encode={old_encode:?} -> {encode:?} decode={old_decode:?} -> {decode:?}",
            old_bytes.len(),
            bytes.len(),
            100.0 * (1.0 - bytes.len() as f64 / old_bytes.len() as f64)
        );
    }
    byte_buffers();
}
