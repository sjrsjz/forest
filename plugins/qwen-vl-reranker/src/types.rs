//! Package-owned Schema/Value contract; floating point scores travel as JSON.
use forest_serde::{DeriveSchema, DeriveValue};
use serde::{Deserialize, Serialize};

pub const OBJECT: &str = "/module/qwen-vl-reranker";
pub const RANK: &str = "/rank";
pub const CONNECTION: &str = "/connection";
pub const MODEL: &str = "Qwen/Qwen3-VL-Reranker-8B";

/// SiliconFlow accepts one text or one image per query/document. Mixed objects
/// silently lose their image; the contract deliberately cannot express them.
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

/// One unmodified model score per document, in input order (not API rank order).
#[derive(Clone, Debug, DeriveSchema, DeriveValue)]
pub struct Scores {
    pub json: String,
}

#[derive(Clone, Serialize, Deserialize, DeriveSchema, DeriveValue)]
pub struct Connection {
    pub endpoint: String,
    pub api_key: String,
    pub batch_size: i64,
}
impl Default for Connection {
    fn default() -> Self {
        Self {
            endpoint: "https://api.siliconflow.cn/v1/rerank".into(),
            api_key: String::new(),
            batch_size: 64,
        }
    }
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
