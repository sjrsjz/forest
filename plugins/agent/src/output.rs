//! Package-owned wire contract; consumers may keep their own copy.
use forest_onebot::message::Message;
use forest_serde::{DeriveSchema, DeriveValue};

pub const OBJECT: &str = "/module/message-output";
pub const SEND: &str = "/send";
pub const SEND_MARKDOWN: &str = "/send_markdown";

#[derive(Clone, Debug, DeriveSchema, DeriveValue)]
pub struct MarkdownOutput {
    pub group_id: i64,
    pub bot_id: i64,
    pub bot_name: Option<String>,
    pub source: String,
}

#[derive(Clone, Debug, DeriveSchema, DeriveValue)]
pub struct DeliveryReport {
    pub sent: Vec<forest_onebot::forest::SentMessage>,
    pub outcome: Result<(), Error>,
}

#[derive(Clone, Debug, DeriveSchema, DeriveValue)]
pub struct GroupOutput {
    pub group_id: i64,
    pub bot_id: i64,
    pub bot_name: Option<String>,
    pub message: Message,
}

#[derive(Clone, Debug, DeriveSchema, DeriveValue)]
pub enum Error {
    InvalidMessage(String),
    InvalidConfiguration(String),
    Media(String),
    Dispatch(String),
    OneBot(forest_onebot::forest::OneBotError),
}
