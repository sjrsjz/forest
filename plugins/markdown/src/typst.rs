//! Local mapping of the Typst package's typesetting port.
use forest_serde::{DeriveSchema, DeriveValue};

pub const OBJECT: &str = "/module/typst";

#[derive(Clone, Debug, DeriveSchema, DeriveValue)]
pub enum Error {
    UnknownTool(String),
    Arguments(String),
    Execution(String),
}
