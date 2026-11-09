//! Local copy of Typst's public Schema/Value contract, not its implementation.
use forest_serde::{DeriveSchema, DeriveValue};

pub const OBJECT: &str = "/module/typst";

#[derive(Clone, Debug, DeriveSchema, DeriveValue)]
pub struct Invocation {
    pub group_id: i64,
    pub bot_id: i64,
    pub name: String,
    pub arguments_json: String,
}

#[derive(Clone, Debug, DeriveSchema, DeriveValue)]
pub enum Error {
    UnknownTool(String),
    Arguments(String),
    Execution(String),
}
