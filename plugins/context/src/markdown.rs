//! Local mapping of /module/markdown /encode.
use forest_onebot::message::Message;
use forest_serde::{DeriveSchema, DeriveValue};
pub const OBJECT: &str = "/module/markdown";
#[derive(Clone, Debug, DeriveSchema, DeriveValue)]
pub struct Encode {
    pub group_id: i64,
    pub message: Message,
}

#[derive(Clone, Debug, PartialEq, Eq, DeriveSchema, DeriveValue)]
pub enum Fragment {
    Text(String),
    Image(String),
}

#[derive(Clone, Debug, PartialEq, Eq, DeriveSchema, DeriveValue)]
pub enum Error {
    Protocol(String),
    Input(String),
}
