//! Local mapping of the memory content schema, without a storage implementation.
use crate::provider::Assistant;
use forest_onebot::forest::GroupMessage;
use forest_serde::{DeriveSchema, DeriveValue};
use serde::{Deserialize, Serialize};

#[derive(
    Clone,
    Debug,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Hash,
    Serialize,
    Deserialize,
    DeriveSchema,
    DeriveValue,
)]
pub struct MemoryId {
    pub group: i64,
    pub sequence: i64,
}

#[derive(Clone, Debug, Serialize, Deserialize, DeriveSchema, DeriveValue)]
pub struct MemoryUnit {
    pub id: MemoryId,
    pub content: MemoryContent,
}

#[derive(Clone, Debug, Serialize, Deserialize, DeriveSchema, DeriveValue)]
pub enum MemoryContent {
    GroupMessage(Box<GroupMessage>),
    ModelTurn(ModelTurn),
    ToolResult(ToolResult),
}

/// A complete assistant response; tool execution is recorded separately.
#[derive(Clone, Debug, Serialize, Deserialize, DeriveSchema, DeriveValue)]
pub struct ModelTurn {
    pub time: i64,
    pub assistant: Assistant,
}

#[derive(Clone, Debug, Serialize, Deserialize, DeriveSchema, DeriveValue)]
pub struct ToolResult {
    pub time: i64,
    pub tool_call_id: String,
    pub content: String,
}
