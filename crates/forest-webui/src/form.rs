//! Configuration widgets derived from declared types, independent of any plugin.
use forest_protocol::{Schema, Value};
use forest_serde::adt::{SchemaEnum, VariantPayloadSchema, VariantPayloadValue};
use serde::Serialize;

#[derive(Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub(crate) enum Form {
    String {
        value: String,
    },
    Bool {
        value: bool,
    },
    Integer {
        value: String,
        bits: u8,
    },
    Record {
        fields: Vec<Field>,
    },
    Tuple {
        items: Vec<Form>,
    },
    List {
        items: Vec<Form>,
        template: Box<Form>,
    },
    Optional {
        enabled: bool,
        inner: Box<Form>,
    },
    Variant {
        selected: String,
        cases: Vec<Case>,
    },
    Text {
        value: String,
    },
}
#[derive(Serialize)]
pub(crate) struct Field {
    name: String,
    control: Form,
}
#[derive(Serialize)]
pub(crate) struct Case {
    name: String,
    payload: Form,
}

pub(crate) fn project(schema: &Schema, value: &Value) -> Form {
    control(schema, schema.root(), value)
}

fn resolve<'a>(schema: &'a Schema, mut node: &'a SchemaEnum) -> Option<&'a SchemaEnum> {
    let mut visited = std::collections::BTreeSet::new();
    while let SchemaEnum::Ref(index) = node {
        if !visited.insert(*index) {
            return None;
        }
        node = &schema.schemas()[*index];
    }
    Some(node)
}

// New optional values and unselected enum payloads need an initial widget value.
// Composite or recursive cases stay editable as Forest text instead of guessing.
fn initial(schema: &Schema, node: &SchemaEnum) -> Option<Value> {
    Some(match resolve(schema, node)? {
        SchemaEnum::String => Value::String(String::new()),
        SchemaEnum::Bool => Value::Bool(false),
        SchemaEnum::I32 => Value::I32(0),
        SchemaEnum::I64 => Value::I64(0),
        _ => return None,
    })
}

fn enum_case(
    schema: &Schema,
    node: &VariantPayloadSchema,
    current: Option<&VariantPayloadValue>,
) -> Option<Form> {
    match (node, current) {
        (VariantPayloadSchema::Unnamed(nodes), current) => {
            let values = match current {
                Some(VariantPayloadValue::Unnamed(values)) => values.clone(),
                None => nodes
                    .iter()
                    .map(|n| initial(schema, n))
                    .collect::<Option<Vec<_>>>()?,
                _ => return None,
            };
            Some(control(
                schema,
                &SchemaEnum::Tuple(nodes.clone()),
                &Value::Tuple(values),
            ))
        }
        (VariantPayloadSchema::Named(nodes), current) => {
            let values = match current {
                Some(VariantPayloadValue::Named(values)) => values.clone(),
                None => nodes
                    .iter()
                    .map(|(name, n)| Some((name.clone(), initial(schema, n)?)))
                    .collect::<Option<_>>()?,
                _ => return None,
            };
            Some(control(
                schema,
                &SchemaEnum::Record(nodes.clone()),
                &Value::Record(values),
            ))
        }
    }
}

fn control(schema: &Schema, node: &SchemaEnum, value: &Value) -> Form {
    match (resolve(schema, node), value) {
        (Some(SchemaEnum::String), Value::String(value)) => Form::String {
            value: value.clone(),
        },
        (Some(SchemaEnum::Bool), Value::Bool(value)) => Form::Bool { value: *value },
        (Some(SchemaEnum::I32), Value::I32(value)) => Form::Integer {
            value: value.to_string(),
            bits: 32,
        },
        (Some(SchemaEnum::I64), Value::I32(value)) => Form::Integer {
            value: value.to_string(),
            bits: 32,
        },
        (Some(SchemaEnum::I64), Value::I64(value)) => Form::Integer {
            value: value.to_string(),
            bits: 64,
        },
        (Some(SchemaEnum::Record(nodes)), Value::Record(values)) => Form::Record {
            fields: values
                .iter()
                .map(|(name, value)| Field {
                    name: name.clone(),
                    control: nodes
                        .get(name)
                        .map(|n| control(schema, n, value))
                        .unwrap_or_else(|| text(value)),
                })
                .collect(),
        },
        (Some(SchemaEnum::Tuple(nodes)), Value::Tuple(values)) => Form::Tuple {
            items: values
                .iter()
                .enumerate()
                .map(|(index, value)| {
                    nodes
                        .get(index)
                        .map(|n| control(schema, n, value))
                        .unwrap_or_else(|| text(value))
                })
                .collect(),
        },
        (Some(SchemaEnum::Array(inner)), Value::Array(values)) => match initial(schema, inner) {
            Some(value) => Form::List {
                items: values
                    .iter()
                    .map(|value| control(schema, inner, value))
                    .collect(),
                template: Box::new(control(schema, inner, &value)),
            },
            None => text(value),
        },
        (Some(SchemaEnum::Option(inner)), Value::Some(value)) => Form::Optional {
            enabled: true,
            inner: Box::new(control(schema, inner, value)),
        },
        (Some(SchemaEnum::Option(inner)), Value::None) => match initial(schema, inner) {
            Some(value) => Form::Optional {
                enabled: false,
                inner: Box::new(control(schema, inner, &value)),
            },
            None => text(value),
        },
        (Some(SchemaEnum::Variant(nodes)), Value::Variant(selected, payload)) => {
            let cases = nodes
                .iter()
                .map(|(name, node)| {
                    Some(Case {
                        name: name.clone(),
                        payload: enum_case(schema, node, (name == selected).then_some(payload))?,
                    })
                })
                .collect::<Option<Vec<_>>>();
            match cases {
                Some(cases) => Form::Variant {
                    selected: selected.clone(),
                    cases,
                },
                None => text(value),
            }
        }
        _ => text(value),
    }
}

fn text(value: &Value) -> Form {
    Form::Text {
        value: value.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use forest_protocol::{AsSchema, AsValue};
    use forest_serde::{DeriveSchema, DeriveValue};

    #[derive(DeriveSchema, DeriveValue)]
    enum Selection {
        Default,
        Count(i64),
    }
    #[derive(DeriveSchema, DeriveValue)]
    struct Example {
        label: String,
        enabled: bool,
        count: i64,
        items: Vec<i64>,
        note: Option<String>,
        selection: Selection,
    }

    #[test]
    fn forms_keep_integer_precision_enum_payloads_and_extra_fields() {
        let mut value = Example {
            label: "a\n\"b".into(),
            enabled: true,
            count: i64::MAX,
            items: vec![i64::MAX],
            note: None,
            selection: Selection::Count(16384),
        }
        .as_value();
        let Value::Record(values) = &mut value else {
            unreachable!()
        };
        let extra = Value::Bytes(vec![0, 255]);
        let extra_text = extra.to_string();
        values.insert("extra".into(), extra);
        let json = serde_json::to_value(project(&Example::as_schema(), &value)).unwrap();
        let fields = json["fields"].as_array().unwrap();
        let field = |name: &str| &fields.iter().find(|f| f["name"] == name).unwrap()["control"];
        assert_eq!(field("count")["value"], i64::MAX.to_string());
        assert_eq!(field("items")["kind"], "list");
        assert_eq!(field("items")["items"][0]["value"], i64::MAX.to_string());
        assert_eq!(field("items")["template"]["bits"], 64);
        assert_eq!(field("note")["enabled"], false);
        assert_eq!(field("note")["inner"]["kind"], "string");
        let selection = field("selection");
        assert_eq!(selection["selected"], "Count");
        let count = selection["cases"]
            .as_array()
            .unwrap()
            .iter()
            .find(|c| c["name"] == "Count")
            .unwrap();
        assert_eq!(count["payload"]["items"][0]["value"], "16384");
        assert_eq!(count["payload"]["items"][0]["bits"], 64);
        assert_eq!(field("extra")["value"], extra_text);
    }

    #[test]
    fn recursive_optional_keeps_its_original_text() {
        let schema =
            Schema::from_schemas(vec![SchemaEnum::Option(Box::new(SchemaEnum::Ref(0)))]).unwrap();
        assert!(matches!(project(&schema,&Value::None),Form::Text { value } if value == "null"));
    }
}
