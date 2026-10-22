//! Local mapping of the tool gateway's public Schema/Value contract.
use forest_serde::{DeriveSchema, DeriveValue};

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
