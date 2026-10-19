//! JSON object keys cannot carry arbitrary Value trees. Encode map entries as pairs.
use std::collections::BTreeMap;

use serde::{Deserialize, Deserializer, Serializer, ser::SerializeSeq};

use super::Value;

pub fn serialize<S: Serializer>(
    values: &BTreeMap<Value, Value>,
    serializer: S,
) -> Result<S::Ok, S::Error> {
    let mut entries = serializer.serialize_seq(Some(values.len()))?;
    for entry in values {
        entries.serialize_element(&entry)?;
    }
    entries.end()
}

pub fn deserialize<'de, D: Deserializer<'de>>(
    deserializer: D,
) -> Result<BTreeMap<Value, Value>, D::Error> {
    let entries = Vec::<(Value, Value)>::deserialize(deserializer)?;
    let mut values = BTreeMap::new();
    for (key, value) in entries {
        if values.insert(key, value).is_some() {
            return Err(serde::de::Error::custom("duplicate encoded map key"));
        }
    }
    Ok(values)
}
