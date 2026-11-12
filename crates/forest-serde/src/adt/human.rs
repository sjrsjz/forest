//! Named, readable Serde representation. Binary documents use the symbol codec.
use std::collections::BTreeMap;

use serde::{Deserialize, Deserializer, Serialize, Serializer};

use super::{Symbol, Value, VariantPayloadValue, binary, map_serde};

#[derive(Serialize, Deserialize)]
#[serde(remote = "Value")]
enum HumanValue {
    I32(i32),
    I64(i64),
    Bool(bool),
    String(String),
    Bytes(Vec<u8>),
    Tuple(Vec<Value>),
    Array(Vec<Value>),
    Map(#[serde(with = "map_serde")] BTreeMap<Value, Value>),
    Variant(Symbol, VariantPayloadValue),
    Record(BTreeMap<Symbol, Value>),
}

impl Serialize for Value {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        if serializer.is_human_readable() {
            HumanValue::serialize(self, serializer)
        } else {
            binary::serialize_value(self, serializer)
        }
    }
}

impl<'de> Deserialize<'de> for Value {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        if deserializer.is_human_readable() {
            HumanValue::deserialize(deserializer)
        } else {
            binary::deserialize_value(deserializer)
        }
    }
}
