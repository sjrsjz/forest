//! Public Schema/Value ports. Consumers keep local mappings of these types.
use forest_onebot::message::{Message, Segment};
use forest_serde::{DeriveSchema, DeriveValue};

pub const OBJECT: &str = "/module/markdown";

#[derive(Clone, Debug, DeriveSchema, DeriveValue)]
pub struct Format {
    pub group_id: Option<i64>,
    pub source: String,
}

#[derive(Clone, Debug, DeriveSchema, DeriveValue)]
pub struct Part {
    pub segments: Vec<Segment>,
    pub source_characters: i64,
    pub source_lines: i64,
}

#[derive(Clone, Debug, DeriveSchema, DeriveValue)]
pub struct Encode {
    pub group_id: i64,
    pub message: Message,
}

#[derive(Clone, Debug, PartialEq, Eq, DeriveSchema, DeriveValue)]
pub enum Fragment {
    Text(String),
    Image(String),
}

#[derive(Clone, Debug, PartialEq, Eq, DeriveSchema, DeriveValue)]
pub enum Error {
    Protocol(String),
    Input(String),
}
