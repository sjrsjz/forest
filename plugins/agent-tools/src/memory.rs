//! Public Schema/Value contract, owned by this package.
use crate::{
    content::{MemoryContent, MemoryId, MemoryUnit},
    openai::ImageUrl,
};
use forest_onebot::forest::GroupMessage;
use forest_serde::{DeriveSchema, DeriveValue};
use serde::{Deserialize, Serialize};

pub const OBJECT: &str = "/module/memory";
#[derive(Clone, Debug, DeriveSchema, DeriveValue)]
pub enum Incoming {
    Group(Box<GroupMessage>),
    Generated {
        group: i64,
        key: String,
        content: MemoryContent,
    },
}
#[derive(Clone, Debug, DeriveSchema, DeriveValue)]
pub struct Accepted {
    pub unit: MemoryUnit,
    pub fresh: bool,
}
#[derive(Clone, Debug, Serialize, Deserialize, DeriveSchema, DeriveValue)]
pub struct RecallLink {
    pub trigger: MemoryId,
    pub recalled: MemoryId,
}
#[derive(Clone, Debug, DeriveSchema, DeriveValue)]
pub struct Region {
    pub units: Vec<MemoryUnit>,
    pub seeds: Vec<MemoryId>,
    /// Scores and edges are observational data with float values encoded as JSON.
    pub graph_json: String,
}
#[derive(Clone, Debug, DeriveSchema, DeriveValue)]
pub struct Projection {
    pub units: Vec<MemoryUnit>,
    pub images: Vec<ImageInput>,
}
#[derive(Clone, Debug, DeriveSchema, DeriveValue)]
pub struct ImageInput {
    pub reference: String,
    pub image: ImageUrl,
}
#[derive(Clone, Debug, DeriveSchema, DeriveValue)]
pub struct Project {
    pub ids: Vec<MemoryId>,
    pub images: bool,
}
#[derive(Clone, Debug, DeriveSchema, DeriveValue)]
pub struct Delivery {
    pub turn: MemoryId,
    pub self_id: i64,
    pub message_id: i32,
}
#[derive(Clone, Debug, Serialize, Deserialize, DeriveSchema, DeriveValue)]
pub struct Settings {
    pub endpoint: String,
    pub api_key: String,
    pub collection: String,
    pub retrieval_json: String,
}
impl Default for Settings {
    fn default() -> Self {
        Self {
        endpoint: "http://127.0.0.1:6333".into(), api_key: String::new(),
        collection: "forest_qwen_vl_2048_cq".into(),
        retrieval_json: r#"{"seed_threshold":0.55,"seed_limit":8,"sequence_weight":0.55,"association_weight":0.9,"activation_min":0.15,"association_limit":16,"node_limit":64,"edge_limit":256,"result_limit":24}"#.into(),
    }
    }
}
#[derive(Clone, Debug, DeriveSchema, DeriveValue)]
pub enum Error {
    Storage(String),
    Host(String),
    InvalidInput(String),
    NotFound,
    Embedding(String),
    Index(String),
    Media(String),
}
impl From<forest_protocol::DispatchError> for Error {
    fn from(error: forest_protocol::DispatchError) -> Self {
        Self::Host(error.to_string())
    }
}
impl From<forest_protocol::service::ServiceError> for Error {
    fn from(error: forest_protocol::service::ServiceError) -> Self {
        Self::Storage(error.to_string())
    }
}
