//! Plugin-local Rust mapping of the provider's Schema/Value contract.
use forest_serde::{DeriveSchema, DeriveValue};
use serde::{Deserialize, Serialize};

pub const OBJECT: &str = "/module/openai-provider";
pub const COMPLETE: &str = "/complete";
pub const CAPABILITIES: &str = "/capabilities";
pub const SETTINGS: &str = "/settings";

#[derive(Clone, Debug, Default, DeriveSchema, DeriveValue)]
pub struct Request {
    pub messages: Vec<Message>,
    pub tools: Vec<Tool>,
    pub tool_choice: Option<ToolChoice>,
}

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

#[derive(Clone, Debug, DeriveSchema, DeriveValue)]
pub struct Tool {
    pub name: String,
    pub description: Option<String>,
    /// JSON Schema is embedded as an object, never as a quoted API string.
    pub parameters_json: String,
    pub strict: Option<bool>,
}

#[derive(Clone, Debug, DeriveSchema, DeriveValue)]
pub enum ToolChoice {
    Auto,
    None,
    Required,
    Function(String),
}

#[derive(Clone, Debug, DeriveSchema, DeriveValue)]
pub struct Completion {
    pub id: String,
    pub model: String,
    pub created: i64,
    pub choices: Vec<Choice>,
    pub usage: Option<Usage>,
}

#[derive(Clone, Debug, DeriveSchema, DeriveValue)]
pub struct Choice {
    pub index: i64,
    pub message: Assistant,
    pub finish_reason: FinishReason,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, DeriveSchema, DeriveValue)]
#[serde(rename_all = "snake_case")]
pub enum FinishReason {
    Stop,
    Length,
    ToolCalls,
    ContentFilter,
}

#[derive(Clone, Debug, Serialize, Deserialize, DeriveSchema, DeriveValue)]
pub struct Usage {
    pub prompt_tokens: i64,
    pub completion_tokens: i64,
    pub total_tokens: i64,
    pub prompt_tokens_details: Option<PromptTokenDetails>,
    pub completion_tokens_details: Option<CompletionTokenDetails>,
    pub prompt_cache_hit_tokens: Option<i64>,
    pub prompt_cache_miss_tokens: Option<i64>,
}

#[derive(Clone, Debug, Serialize, Deserialize, DeriveSchema, DeriveValue)]
pub struct PromptTokenDetails {
    pub cached_tokens: Option<i64>,
}

#[derive(Clone, Debug, Serialize, Deserialize, DeriveSchema, DeriveValue)]
pub struct CompletionTokenDetails {
    pub reasoning_tokens: Option<i64>,
}

#[derive(Clone, Debug, PartialEq, Eq, DeriveSchema, DeriveValue)]
pub struct Capabilities {
    pub model: String,
    /// Declaration for Context projection; Provider does not rewrite messages.
    pub multimodal: bool,
}

#[derive(Clone, Serialize, Deserialize, DeriveSchema, DeriveValue)]
pub struct Settings {
    /// Full Chat Completions endpoint, not a base URL to be guessed at runtime.
    pub endpoint: String,
    pub api_key: String,
    pub model: String,
    pub multimodal: bool,
    pub output_limit: OutputLimit,
    pub reasoning_effort: Option<String>,
}

#[derive(Clone, Default, Serialize, Deserialize, DeriveSchema, DeriveValue)]
pub enum OutputLimit {
    #[default]
    ProviderDefault,
    CompletionTokens(i64),
    Tokens(i64),
}

impl Default for Settings {
    fn default() -> Self {
        Self {
            endpoint: "https://api.openai.com/v1/chat/completions".into(),
            api_key: String::new(),
            model: String::new(),
            multimodal: false,
            output_limit: OutputLimit::ProviderDefault,
            reasoning_effort: None,
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
    ToolSchema {
        name: String,
        reason: String,
    },
    Response(String),
}
