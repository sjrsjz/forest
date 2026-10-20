use crate::Value;
use bincode::Options;
use serde::{Deserialize, Serialize, de::DeserializeOwned};

pub const MAX_FRAME_BYTES: usize = 16 * 1024 * 1024;
pub const PENDING: i64 = -1;

/// ABI control integers; these never grant identity or survive an instance.
pub type TaskId = u64;
pub type OperationId = u64;

pub fn encode<T: Serialize>(value: &T) -> Result<Vec<u8>, bincode::Error> {
    bincode::DefaultOptions::new()
        .with_fixint_encoding()
        .with_little_endian()
        .with_limit(MAX_FRAME_BYTES as u64)
        .reject_trailing_bytes()
        .serialize(value)
}

pub fn decode<T: DeserializeOwned>(bytes: &[u8]) -> Result<T, bincode::Error> {
    bincode::DefaultOptions::new()
        .with_fixint_encoding()
        .with_little_endian()
        .with_limit(MAX_FRAME_BYTES as u64)
        .reject_trailing_bytes()
        .deserialize(bytes)
}

pub const fn buffer(ptr: u32, len: u32) -> u64 {
    ((len as u64) << 32) | ptr as u64
}

pub const fn split_buffer(buffer: u64) -> (u32, u32) {
    (buffer as u32, (buffer >> 32) as u32)
}

#[derive(Debug, Serialize, Deserialize)]
pub enum Step {
    Pending,
    Output(Value),
    Lifecycle(Result<(), String>),
}
