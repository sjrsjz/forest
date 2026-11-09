//! Local copy of the message output package's public Schema/Value contract.
use forest_onebot::message::Message;
use forest_serde::{DeriveSchema, DeriveValue};

pub const OBJECT: &str = "/module/message-output";
pub const SEND: &str = "/send";

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
