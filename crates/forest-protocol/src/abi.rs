use crate::Value;
use bincode::Options;
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use std::io::{self, Write};

/// A frame length is represented by a wasm32 `u32`, not a model/context budget.
pub const MAX_FRAME_BYTES: usize = u32::MAX as usize;
pub const PENDING: i64 = -1;

/// ABI control integers; these never grant identity or survive an instance.
pub type TaskId = u64;
pub type OperationId = u64;

pub fn encode<T: Serialize>(value: &T) -> Result<Vec<u8>, bincode::Error> {
    encode_with_limit(value, MAX_FRAME_BYTES)
}

fn encode_with_limit<T: Serialize>(value: &T, limit: usize) -> Result<Vec<u8>, bincode::Error> {
    let mut writer = FrameWriter {
        bytes: Vec::new(),
        limit,
        full: false,
    };
    // Bincode's bounded serialize/serialize_into first serializes for sizing.
    // Enforce the byte limit in the writer instead: one symbol collection and
    // one output traversal, without buffering a second copy of the value tree.
    let result = bincode::DefaultOptions::new()
        .with_fixint_encoding()
        .with_little_endian()
        .with_no_limit()
        .reject_trailing_bytes()
        .serialize_into(&mut writer, value);
    if writer.full {
        return Err(Box::new(bincode::ErrorKind::SizeLimit));
    }
    result?;
    Ok(writer.bytes)
}

struct FrameWriter {
    bytes: Vec<u8>,
    limit: usize,
    full: bool,
}

impl Write for FrameWriter {
    #[inline]
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        if bytes.len() > self.limit - self.bytes.len() {
            self.full = true;
            return Err(io::Error::other(
                "ABI frame exceeds its length representation",
            ));
        }
        self.bytes.extend_from_slice(bytes);
        Ok(bytes.len())
    }

    #[inline]
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

pub fn decode<T: DeserializeOwned>(bytes: &[u8]) -> Result<T, bincode::Error> {
    bincode::DefaultOptions::new()
        .with_fixint_encoding()
        .with_little_endian()
        .with_limit(bytes.len() as u64)
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
    Lifecycle(Result<(), Value>),
    FrameTooLarge,
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::cell::Cell;

    #[test]
    fn encoding_traverses_once_and_enforces_the_actual_byte_limit() {
        struct Counted<'a>(&'a Cell<usize>);
        impl Serialize for Counted<'_> {
            fn serialize<S: serde::Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
                self.0.set(self.0.get() + 1);
                42_i32.serialize(s)
            }
        }
        let count = Cell::new(0);
        assert_eq!(
            encode_with_limit(&Counted(&count), 4).unwrap(),
            42_i32.to_le_bytes()
        );
        assert_eq!(count.get(), 1);
        assert!(matches!(
            *encode_with_limit(&42_i32, 3).unwrap_err(),
            bincode::ErrorKind::SizeLimit
        ));
        let value = Value::some(Value::String("hello".into()));
        let bytes = encode(&value).unwrap();
        assert_eq!(decode::<Value>(&bytes).unwrap(), value);
        let mut trailing = bytes.clone();
        trailing.push(0);
        assert!(decode::<Value>(&trailing).is_err());
        assert!(decode::<Value>(&bytes[..bytes.len() - 1]).is_err());
    }
}
