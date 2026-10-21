//! Bounded display projection. The actual stored Value is never modified.
use forest_protocol::Value;
use forest_serde::adt::VariantPayloadValue;
use serde::Serialize;

#[derive(Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub(crate) enum ValueTree {
    Scalar {
        type_name: &'static str,
        text: String,
    },
    Bytes {
        length: usize,
        preview_hex: String,
    },
    Sequence {
        type_name: &'static str,
        length: usize,
        items: Vec<ValueTree>,
        truncated: bool,
    },
    Record {
        fields: Vec<Field>,
        truncated: bool,
    },
    Map {
        length: usize,
        entries: Vec<Pair>,
        truncated: bool,
    },
    Variant {
        tag: String,
        body: Box<ValueTree>,
    },
    Omitted,
}
#[derive(Serialize)]
pub(crate) struct Field {
    name: String,
    value: ValueTree,
}
#[derive(Serialize)]
pub(crate) struct Pair {
    key: ValueTree,
    value: ValueTree,
}

pub(crate) fn tree(value: &Value) -> ValueTree {
    project(value, 0, &mut 256)
}
fn scalar(type_name: &'static str, text: String) -> ValueTree {
    ValueTree::Scalar { type_name, text }
}
fn fields<'a>(
    items: impl Iterator<Item = (&'a String, &'a Value)>,
    length: usize,
    depth: usize,
    budget: &mut usize,
) -> ValueTree {
    let mut fields = Vec::new();
    for (name, value) in items {
        if *budget == 0 {
            break;
        }
        fields.push(Field {
            name: name.clone(),
            value: project(value, depth + 1, budget),
        });
    }
    let truncated = fields.len() < length;
    ValueTree::Record { fields, truncated }
}
fn sequence(items: &[Value], name: &'static str, depth: usize, budget: &mut usize) -> ValueTree {
    let mut output = Vec::new();
    for item in items {
        if *budget == 0 {
            break;
        }
        output.push(project(item, depth + 1, budget));
    }
    let truncated = output.len() < items.len();
    ValueTree::Sequence {
        type_name: name,
        length: items.len(),
        items: output,
        truncated,
    }
}
fn project(value: &Value, depth: usize, budget: &mut usize) -> ValueTree {
    if *budget == 0 {
        return ValueTree::Omitted;
    }
    *budget -= 1;
    if depth >= 24 {
        return ValueTree::Omitted;
    }
    match value {
        Value::I32(v) => scalar("i32", v.to_string()),
        Value::I64(v) => scalar("i64", v.to_string()),
        Value::Bool(v) => scalar("bool", v.to_string()),
        Value::String(v) => {
            let mut text = v.chars().take(2048).collect::<String>();
            if v.chars().nth(2048).is_some() {
                text.push('…');
            }
            scalar("string", text)
        }
        Value::Bytes(v) => ValueTree::Bytes {
            length: v.len(),
            preview_hex: v.iter().take(32).map(|v| format!("{v:02x}")).collect(),
        },
        Value::None => scalar("option", "None".into()),
        Value::Some(v) => ValueTree::Variant {
            tag: "Some".into(),
            body: Box::new(project(v, depth + 1, budget)),
        },
        Value::Tuple(v) => sequence(v, "tuple", depth, budget),
        Value::Array(v) => sequence(v, "array", depth, budget),
        Value::Record(v) => fields(v.iter(), v.len(), depth, budget),
        Value::Map(v) => {
            let mut entries = Vec::new();
            for (key, value) in v {
                if *budget < 2 {
                    break;
                }
                entries.push(Pair {
                    key: project(key, depth + 1, budget),
                    value: project(value, depth + 1, budget),
                });
            }
            let truncated = entries.len() < v.len();
            ValueTree::Map {
                length: v.len(),
                entries,
                truncated,
            }
        }
        Value::Variant(tag, payload) => ValueTree::Variant {
            tag: tag.clone(),
            body: Box::new(match payload {
                VariantPayloadValue::Unnamed(v) => sequence(v, "tuple", depth + 1, budget),
                VariantPayloadValue::Named(v) => fields(v.iter(), v.len(), depth + 1, budget),
            }),
        },
    }
}
