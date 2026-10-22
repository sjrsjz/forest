//! Public Schema/Value contract. Consumers may keep their own copy.
use forest_serde::{DeriveSchema, DeriveValue};

pub const OBJECT: &str = "/module/media";

#[derive(Clone, Debug, PartialEq, DeriveSchema, DeriveValue)]
pub struct Blob {
    pub mime: String,
    pub bytes: Vec<u8>,
}

/// A stable message-attachment identity and its retrievable original source.
#[derive(Clone, Debug, DeriveSchema, DeriveValue)]
pub struct Source {
    pub reference: String,
    pub source: String,
}

#[derive(Clone, Debug, DeriveSchema, DeriveValue)]
pub enum Error {
    NotFound,
    InvalidInput(String),
    Storage(String),
    Fetch(String),
    Host(String),
}
