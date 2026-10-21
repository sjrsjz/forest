//! Plugin-local Rust mapping. Floating point data is an explicit JSON payload.
use forest_serde::{DeriveSchema, DeriveValue};
use serde::{Deserialize, Serialize};

pub const OBJECT: &str = "/module/qwen-vl-embedding";
pub const EMBED: &str = "/embed";
pub const CONNECTION: &str = "/connection";
pub const MODEL: &str = "Qwen/Qwen3-VL-Embedding-8B";
pub const DIMENSIONS: usize = 2048;

/// Image inputs are provider-accessible URLs or temporary base64/data URLs.
/// Neither image content nor requests are persisted by this provider module.
#[derive(Clone, Debug, DeriveSchema, DeriveValue)]
pub enum Input {
    Text(String),
    Image(String),
}

/// One vector per input, in input order; each vector has DIMENSIONS components.
/// JSON carries float32 values without adding non-hashable values to Forest ADT.
#[derive(Clone, Debug, DeriveSchema, DeriveValue)]
pub struct Embeddings {
    json: String,
}
impl Embeddings {
    pub fn vectors(&self) -> Result<Vec<Vec<f32>>, serde_json::Error> {
        serde_json::from_str(&self.json)
    }

    /// Encode vectors after the provider has qualified their dimensions and values.
    pub fn from_vectors(vectors: &[Vec<f32>]) -> Self {
        Self {
            json: serde_json::to_string(vectors).expect("validated finite embedding vectors"),
        }
    }
}

/// Only connection settings are mutable. The model and space are package constants.
#[derive(Clone, Serialize, Deserialize, DeriveSchema, DeriveValue)]
pub struct Connection {
    pub endpoint: String,
    pub api_key: String,
}
impl Default for Connection {
    fn default() -> Self {
        Self {
            endpoint: "https://api.siliconflow.cn/v1/embeddings".into(),
            api_key: String::new(),
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
    },
    Response(String),
    ModelMismatch(String),
    Count {
        expected: i64,
        actual: i64,
    },
    Dimensions {
        index: i64,
        actual: i64,
    },
    InvalidVector {
        index: i64,
    },
}
