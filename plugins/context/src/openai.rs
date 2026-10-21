//! Local OpenAI message representation; the wire contract is Schema/Value.
use forest_serde::{DeriveSchema, DeriveValue};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Serialize, Deserialize, DeriveSchema, DeriveValue)]
#[serde(tag = "role", rename_all = "snake_case")]
pub enum Message {
    System {
        content: String,
    },
    Developer {
        content: String,
    },
    User {
        content: Content,
    },
    Assistant(Assistant),
    Tool {
        tool_call_id: String,
        content: String,
    },
}

#[derive(Clone, Debug, Serialize, Deserialize, DeriveSchema, DeriveValue)]
#[serde(untagged)]
pub enum Content {
    Text(String),
    Parts(Vec<ContentPart>),
}

#[derive(Clone, Debug, Serialize, Deserialize, DeriveSchema, DeriveValue)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ContentPart {
    Text { text: String },
    ImageUrl { image_url: ImageUrl },
}

#[derive(Clone, Debug, Serialize, Deserialize, DeriveSchema, DeriveValue)]
pub struct ImageUrl {
    pub url: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub detail: Option<ImageDetail>,
}

#[derive(Clone, Debug, Serialize, Deserialize, DeriveSchema, DeriveValue)]
#[serde(rename_all = "snake_case")]
pub enum ImageDetail {
    Auto,
    Low,
    High,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, DeriveSchema, DeriveValue)]
pub struct Assistant {
    pub content: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tool_calls: Option<Vec<ToolCall>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub refusal: Option<String>,
    /// Compatible services may require this exact value on tool continuations.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reasoning_content: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize, DeriveSchema, DeriveValue)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ToolCall {
    Function { id: String, function: FunctionCall },
}

#[derive(Clone, Debug, Serialize, Deserialize, DeriveSchema, DeriveValue)]
pub struct FunctionCall {
    pub name: String,
    /// Native protocol JSON text; the tool executor, not Provider, interprets it.
    pub arguments: String,
}
