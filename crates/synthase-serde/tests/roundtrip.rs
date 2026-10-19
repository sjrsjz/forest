use std::collections::{BTreeMap, HashMap};

use synthase_serde::adt::{
    AsSchema, AsValue, DecodeError, Schema, SchemaEnum, Value, ValueKind, VariantPayloadValue,
};
use synthase_serde::parser::{parse_schema, parse_value};
use synthase_serde::{DeriveSchema, DeriveValue};

#[derive(Debug, PartialEq, DeriveSchema, DeriveValue)]
struct Node<T> {
    item: T,
    next: Option<Box<Node<T>>>,
}

#[derive(Debug, PartialEq, DeriveSchema, DeriveValue)]
struct State<T> {
    id: i64,
    values: T,
}

#[derive(Debug, PartialEq, DeriveSchema, DeriveValue)]
struct Prefix(i64);

#[derive(Debug, PartialEq, DeriveSchema, DeriveValue)]
struct Unit;

#[derive(Debug, PartialEq, DeriveSchema, DeriveValue)]
struct EmptyNamed {}

#[derive(Debug, PartialEq, DeriveSchema, DeriveValue)]
struct EmptyTuple();

#[derive(Debug, PartialEq, DeriveSchema, DeriveValue)]
enum Reply<T> {
    Data(T),
    Named { value: T },
    Unit,
    EmptyNamed {},
    EmptyTuple(),
}

#[derive(Debug, PartialEq, DeriveSchema, DeriveValue)]
enum Never {}

#[test]
fn empty_enums_have_no_decodable_value() {
    let value = Value::Variant("anything".into(), VariantPayloadValue::Unnamed(vec![]));
    assert!(!value.check(&Never::as_schema(), false));
    assert_eq!(
        Never::from_value(value),
        Err(DecodeError::UnknownVariant {
            tag: "anything".into()
        })
    );
}

#[test]
fn arbitrary_map_keys_roundtrip_without_json_object_key_restrictions() {
    let value = Value::Map(BTreeMap::from([
        (
            Value::Tuple(vec![Value::I32(1)]),
            Value::String("tuple key".into()),
        ),
        (Value::I64(2), Value::Bytes(vec![0, 255])),
    ]));
    assert_eq!(
        serde_json::from_slice::<Value>(&serde_json::to_vec(&value).unwrap()).unwrap(),
        value
    );
    assert_eq!(
        bincode::deserialize::<Value>(&bincode::serialize(&value).unwrap()).unwrap(),
        value
    );
    assert_eq!(parse_value(&value.to_string()).unwrap(), value);
    assert!(
        serde_json::from_str::<Value>(
            r#"{"Map":[[{"I32":1},{"Bool":false}],[{"I32":1},{"Bool":true}]]}"#
        )
        .is_err()
    );
}

#[test]
fn recursive_generic_state_roundtrips_through_both_codecs() {
    let state = Node {
        item: 42_i64,
        next: Some(Box::new(Node {
            item: -1,
            next: None,
        })),
    };
    let schema = Node::<i64>::as_schema();
    let value = state.as_value();
    assert!(value.check(&schema, false));

    let json_schema = serde_json::to_vec(&schema).unwrap();
    assert_eq!(
        serde_json::from_slice::<Schema>(&json_schema).unwrap(),
        schema
    );
    let binary_schema = bincode::serialize(&schema).unwrap();
    assert_eq!(
        bincode::deserialize::<Schema>(&binary_schema).unwrap(),
        schema
    );
    let json_value = serde_json::to_vec(&value).unwrap();
    let binary_value = bincode::serialize(&value).unwrap();
    assert_eq!(
        Node::<i64>::from_value(serde_json::from_slice(&json_value).unwrap()).unwrap(),
        state
    );
    assert_eq!(
        Node::<i64>::from_value(bincode::deserialize(&binary_value).unwrap()).unwrap(),
        state
    );
    assert_eq!(parse_schema(&schema.to_string()).unwrap(), schema);
    assert_eq!(parse_value(&value.to_string()).unwrap(), value);

    for _ in 0..32 {
        assert_eq!(
            serde_json::to_vec(&Node::<i64>::as_schema()).unwrap(),
            json_schema
        );
        assert_eq!(
            bincode::serialize(&Node::<i64>::as_schema()).unwrap(),
            binary_schema
        );
    }
}

#[test]
fn invalid_schemas_cannot_be_constructed_or_deserialized() {
    assert!(Schema::from_schemas(vec![]).is_none());
    assert!(Schema::from_schemas(vec![SchemaEnum::Ref(1)]).is_none());
    assert!(serde_json::from_str::<Schema>("[]").is_err());
    assert!(serde_json::from_str::<Schema>(r#"[{"Ref":1}]"#).is_err());
    assert!(parse_schema("").is_err());
}

#[test]
fn numeric_and_record_projections_agree_with_validation() {
    let value = Value::Record(BTreeMap::from([
        ("id".into(), Value::I32(7)),
        ("values".into(), Value::Array(vec![Value::I32(8)])),
        ("extra".into(), Value::Bool(true)),
    ]));
    assert!(value.check(&State::<Vec<i64>>::as_schema(), false));
    assert_eq!(
        State::<Vec<i64>>::from_value(value).unwrap(),
        State {
            id: 7,
            values: vec![8]
        }
    );
    assert!(!Value::I64(7).check(&i32::as_schema(), false));
    assert!(i32::from_value(Value::I64(7)).is_err());
}

#[test]
fn tuple_prefix_projection_agrees_with_validation() {
    let value = Value::Tuple(vec![Value::I32(7), Value::Bool(true)]);
    assert!(value.check(&Prefix::as_schema(), false));
    assert_eq!(Prefix::from_value(value.clone()).unwrap(), Prefix(7));
    assert!(value.check(&<(i64,)>::as_schema(), false));
    assert_eq!(<(i64,)>::from_value(value).unwrap(), (7,));
    assert!(Prefix::from_value(Value::Tuple(vec![])).is_err());
}

#[test]
fn empty_structural_projections_agree_with_validation() {
    let values = [
        Value::Record(BTreeMap::from([("extra".into(), Value::Bool(true))])),
        Value::Tuple(vec![Value::I32(1)]),
    ];
    for value in values {
        assert!(value.check(&Unit::as_schema(), false));
        assert_eq!(Unit::from_value(value.clone()).unwrap(), Unit);
        assert!(value.check(&EmptyNamed::as_schema(), false));
        assert_eq!(
            EmptyNamed::from_value(value.clone()).unwrap(),
            EmptyNamed {}
        );
        assert!(value.check(&EmptyTuple::as_schema(), false));
        assert_eq!(EmptyTuple::from_value(value.clone()).unwrap(), EmptyTuple());
        assert!(value.check(&<()>::as_schema(), false));
        <()>::from_value(value.clone()).unwrap();
        assert!(value.check(&<[i64; 0]>::as_schema(), false));
        assert_eq!(<[i64; 0]>::from_value(value).unwrap(), [0_i64; 0]);
    }
}

#[test]
fn enum_and_result_payload_projections_agree_with_validation() {
    let data = Value::Variant(
        "Data".into(),
        VariantPayloadValue::Unnamed(vec![Value::I32(5), Value::Bool(true)]),
    );
    assert!(data.check(&Reply::<i64>::as_schema(), false));
    assert_eq!(Reply::<i64>::from_value(data).unwrap(), Reply::Data(5));
    let named = Value::Variant(
        "Named".into(),
        VariantPayloadValue::Named(BTreeMap::from([
            ("value".into(), Value::I32(6)),
            ("extra".into(), Value::None),
        ])),
    );
    assert!(named.check(&Reply::<i64>::as_schema(), false));
    assert_eq!(
        Reply::<i64>::from_value(named).unwrap(),
        Reply::Named { value: 6 }
    );
    for (tag, expected) in [
        ("Unit", Reply::Unit),
        ("EmptyNamed", Reply::EmptyNamed {}),
        ("EmptyTuple", Reply::EmptyTuple()),
    ] {
        let value = Value::Variant(
            tag.into(),
            VariantPayloadValue::Named(BTreeMap::from([("extra".into(), Value::I32(1))])),
        );
        assert!(value.check(&Reply::<i64>::as_schema(), false));
        assert_eq!(Reply::<i64>::from_value(value).unwrap(), expected);
    }
    let result: Result<i64, String> = Err("failure".into());
    assert!(
        result
            .as_value()
            .check(&Result::<i64, String>::as_schema(), false)
    );
    assert_eq!(
        Result::<i64, String>::from_value(result.as_value()).unwrap(),
        result
    );
    assert!(
        Result::<i64, String>::from_value(Value::Variant(
            "Ok".into(),
            VariantPayloadValue::Unnamed(vec![])
        ))
        .is_err()
    );
}

#[test]
fn arrays_are_not_limited_to_macro_generated_lengths() {
    let value = [7_i64; 64].as_value();
    assert!(value.check(&<[i64; 64]>::as_schema(), false));
    assert_eq!(<[i64; 64]>::from_value(value).unwrap(), [7; 64]);
    assert_eq!(
        <[i64; 1]>::from_value(Value::Tuple(vec![Value::I32(7), Value::Bool(true)])).unwrap(),
        [7]
    );
}

#[test]
fn map_key_conversion_cannot_silently_drop_entries() {
    let value = Value::Map(BTreeMap::from([
        (Value::I32(1), Value::Bool(false)),
        (Value::I64(1), Value::Bool(true)),
    ]));
    assert_eq!(
        HashMap::<i64, bool>::from_value(value.clone()),
        Err(DecodeError::DuplicateMapKey)
    );
    assert_eq!(
        BTreeMap::<i64, bool>::from_value(value),
        Err(DecodeError::DuplicateMapKey)
    );
}

#[test]
fn conversion_errors_preserve_structured_location_and_can_be_serialized() {
    let value = Value::Record(BTreeMap::from([
        ("id".into(), Value::I32(1)),
        (
            "values".into(),
            Value::Array(vec![Value::String("wrong".into())]),
        ),
    ]));
    let error = State::<Vec<i64>>::from_value(value).unwrap_err();
    assert_eq!(
        error,
        DecodeError::TypeMismatch {
            expected: ValueKind::I64,
            actual: ValueKind::String
        }
        .at_index(0)
        .at_field("values")
    );
    let encoded = serde_json::to_vec(&error).unwrap();
    assert_eq!(
        serde_json::from_slice::<DecodeError>(&encoded).unwrap(),
        error
    );
    #[derive(Debug, DeriveValue)]
    struct RequiredOption {
        item: Option<i64>,
    }
    assert_eq!(
        RequiredOption::from_value(Value::Record(BTreeMap::new())).unwrap_err(),
        DecodeError::MissingField {
            field: "item".into()
        }
    );
}

#[test]
fn textual_roundtrips_preserve_escaping_tags_and_nested_payloads() {
    let text = "quote: \" slash: \\ newline: \n control: \u{7f} emoji: 🦀";
    let nested = [
        Value::Tuple(vec![Value::I32(1), Value::I32(2)]),
        Value::Record(BTreeMap::from([(text.into(), Value::String(text.into()))])),
    ];
    for tag in ["", "123", "true", "some", "map", "标签", text] {
        let empty = Value::Variant(tag.into(), VariantPayloadValue::Unnamed(vec![]));
        assert_eq!(parse_value(&empty.to_string()).unwrap(), empty);
        for value in &nested {
            let value = Value::Variant(
                tag.into(),
                VariantPayloadValue::Unnamed(vec![value.clone()]),
            );
            assert_eq!(parse_value(&value.to_string()).unwrap(), value);
        }
    }
    assert_eq!(
        parse_value(&Value::String(text.into()).to_string()).unwrap(),
        Value::String(text.into())
    );
    let schema = Schema::from_schemas(vec![SchemaEnum::Record(BTreeMap::from([
        ("true".into(), SchemaEnum::String),
        (text.into(), SchemaEnum::I64),
    ]))])
    .unwrap();
    assert_eq!(parse_schema(&schema.to_string()).unwrap(), schema);
    assert!(parse_value(r#""invalid\q""#).is_err());
}
