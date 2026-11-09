//! Package-owned copy of Media's /get Schema/Value contract.
use forest_serde::{DeriveSchema, DeriveValue};

pub const OBJECT: &str = "/module/media";

#[derive(Clone, Debug, PartialEq, DeriveSchema, DeriveValue)]
pub struct Blob {
    pub mime: String,
    pub bytes: Vec<u8>,
}

#[derive(Clone, Debug, DeriveSchema, DeriveValue)]
pub enum Error {
    NotFound,
    InvalidInput(String),
    Storage(String),
    Fetch(String),
    Host(String),
}
