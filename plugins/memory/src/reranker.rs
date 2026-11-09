//! Local mapping of /module/qwen-vl-reranker; no implementation crate dependency.
use forest_serde::{DeriveSchema, DeriveValue};

pub const OBJECT: &str = "/module/qwen-vl-reranker";
pub const RANK: &str = "/rank";

#[derive(Clone, Debug, DeriveSchema, DeriveValue)]
pub enum Input {
    Text(String),
    Image(String),
}
#[derive(Clone, Debug, DeriveSchema, DeriveValue)]
pub struct Request {
    pub query: Input,
    pub documents: Vec<Input>,
}
#[derive(Clone, Debug, DeriveSchema, DeriveValue)]
pub struct Scores {
    pub json: String,
}
#[derive(Clone, Debug, PartialEq, Eq, DeriveSchema, DeriveValue)]
pub enum Error {
    NotConfigured,
    Host(String),
    Http {
        status: i32,
        request_id: Option<String>,
        body: String,
    },
    Response(String),
}
