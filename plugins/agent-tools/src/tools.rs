use forest_serde::{DeriveSchema, DeriveValue};
use serde::{Deserialize, Serialize};
#[derive(Clone, Debug, Serialize, Deserialize, DeriveSchema, DeriveValue)]
pub struct Tool {
    pub name: String,
    pub description: Option<String>,
    /// JSON Schema is embedded as an object, never as a quoted API string.
    pub parameters_json: String,
    pub strict: Option<bool>,
}
