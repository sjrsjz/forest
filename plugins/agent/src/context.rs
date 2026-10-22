//! Local interface mapping. Other modules use the Schema/Value contract.
use crate::{
    content::{MemoryId, MemoryUnit, ModelTurn, ToolResult},
    provider::ImageUrl,
};
use forest_onebot::forest::GroupMessage;
use forest_serde::{DeriveSchema, DeriveValue};

pub const OBJECT: &str = "/module/context";
pub const FORMAT: &str = "/format";
pub const AGENT_PROMPT: &str = "/prompt/agent";

#[derive(Clone, Debug, DeriveSchema, DeriveValue)]
pub struct FormatInput {
    pub identity: PromptIdentity,
    pub items: Vec<ContextItem>,
    pub images: Vec<ImageInput>,
    pub multimodal: bool,
}

#[derive(Clone, Debug, DeriveSchema, DeriveValue)]
pub struct PromptIdentity {
    pub bot_id: i64,
    pub bot_name: Option<String>,
    pub group_id: i64,
    pub group_name: Option<String>,
}

#[derive(Clone, Debug, DeriveSchema, DeriveValue)]
pub enum ContextItem {
    GroupMessage {
        id: MemoryId,
        message: Box<GroupMessage>,
        trigger: bool,
    },
    ModelTurn(ModelTurn),
    ToolResult(ToolResult),
    Recall(RecallBatch),
}

/// One retrieval operation, immediately before the group message that requested it.
#[derive(Clone, Debug, DeriveSchema, DeriveValue)]
pub struct RecallBatch {
    pub trigger: MemoryId,
    pub units: Vec<MemoryUnit>,
    pub seeds: Vec<MemoryId>,
    pub graph_json: String,
}

/// One mapping per managed media reference. No resource is fetched by Context.
#[derive(Clone, Debug, DeriveSchema, DeriveValue)]
pub struct ImageInput {
    pub reference: String,
    pub image: ImageUrl,
}

#[derive(Clone, Debug, PartialEq, Eq, DeriveSchema, DeriveValue)]
pub enum Error {
    Cq(forest_onebot::cq::Error),
    /// Inline bytes must be managed before appearing as a message file reference.
    InlineMedia,
}
