//! Local interface mapping. Other modules use the Schema/Value contract.
use crate::{
    content::{MemoryContent, MemoryUnit},
    provider::ImageUrl,
};
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
    Content(MemoryContent),
    Recall(MemoryUnit),
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
