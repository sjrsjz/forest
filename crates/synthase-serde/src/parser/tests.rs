use std::collections::BTreeMap;

use super::*;
use crate::adt::{Schema, SchemaEnum, VariantPayloadSchema};

// ============================================================================
// Value Parser Tests
// ============================================================================

#[test]
fn test_primitives() {
    assert_eq!(parse_value("null").unwrap(), Value::None);
    assert_eq!(parse_value("true").unwrap(), Value::Bool(true));
    assert_eq!(parse_value("false").unwrap(), Value::Bool(false));
    assert_eq!(parse_value("42").unwrap(), Value::I32(42));
    assert_eq!(parse_value("123i32").unwrap(), Value::I32(123));
    assert_eq!(parse_value("123i64").unwrap(), Value::I64(123));
    assert_eq!(
        parse_value(r#""hello world""#).unwrap(),
        Value::String("hello world".to_string())
    );
}

#[test]
fn test_standalone_ident() {
    assert_eq!(
        parse_value("Disconnected").unwrap(),
        Value::Variant(
            "Disconnected".to_string(),
            VariantPayloadValue::Unnamed(vec![])
        )
    );
    assert_eq!(
        parse_value("UserLoggedOut").unwrap(),
        Value::Variant(
            "UserLoggedOut".to_string(),
            VariantPayloadValue::Unnamed(vec![])
        )
    );
}

#[test]
fn test_collections() {
    assert_eq!(
        parse_value("[1, 2, 3,]").unwrap(),
        Value::Array(vec![Value::I32(1), Value::I32(2), Value::I32(3)])
    );
    assert_eq!(
        parse_value("(\"a\", false)").unwrap(),
        Value::Tuple(vec![Value::String("a".to_string()), Value::Bool(false)])
    );
    assert_eq!(
        parse_value("some 42").unwrap(),
        Value::Some(Box::new(Value::I32(42)))
    );
}

#[test]
fn test_record() {
    let mut expected_map = BTreeMap::new();
    expected_map.insert("id".to_string(), Value::I32(100));
    expected_map.insert("name".to_string(), Value::String("synthase".to_string()));
    assert_eq!(
        parse_value(r#"{ id: 100, "name": "synthase", }"#).unwrap(),
        Value::Record(expected_map)
    );
}

#[test]
fn test_variants_with_payload() {
    assert_eq!(
        parse_value("Ok::200").unwrap(),
        Value::Variant(
            "Ok".to_string(),
            VariantPayloadValue::Unnamed(vec![Value::I32(200)])
        )
    );
    assert_eq!(
        parse_value("Point::(10, 20)").unwrap(),
        Value::Variant(
            "Point".to_string(),
            VariantPayloadValue::Unnamed(vec![Value::I32(10), Value::I32(20)])
        )
    );
    let mut map = BTreeMap::new();
    map.insert("err_code".to_string(), Value::I32(404));
    assert_eq!(
        parse_value("Error::{ err_code: 404 }").unwrap(),
        Value::Variant("Error".to_string(), VariantPayloadValue::Named(map))
    );
}

#[test]
fn test_complex_tree() {
    let result = parse_value(
        r#"
        {
            name: "Agent", status: Active,
            config: some Option :: { retries: 3, ignore_errors: [true, false] },
            rules: map { 1: "a", "any_key": null, (10, 20): "b" }
        }
    "#,
    );
    assert!(result.is_ok());
}

#[test]
fn test_bytes() {
    if let Ok(Value::Bytes(b)) = parse_value(r#"b"SGVsbG8=""#) {
        assert_eq!(String::from_utf8(b).unwrap(), "Hello");
    }
}

// ============================================================================
// Helpers for building Schema in tests
// ============================================================================

fn s(schemas: Vec<SchemaEnum>) -> Schema {
    Schema::from_schemas(schemas).unwrap()
}

fn record(fields: Vec<(&str, SchemaEnum)>) -> SchemaEnum {
    SchemaEnum::Record(
        fields
            .into_iter()
            .map(|(k, v)| (k.to_string(), v))
            .collect(),
    )
}

fn variant(variants: Vec<(&str, VariantPayloadSchema)>) -> SchemaEnum {
    SchemaEnum::Variant(
        variants
            .into_iter()
            .map(|(k, v)| (k.to_string(), v))
            .collect(),
    )
}

fn vunit() -> VariantPayloadSchema {
    VariantPayloadSchema::Unnamed(vec![])
}

fn vunnamed(types: Vec<SchemaEnum>) -> VariantPayloadSchema {
    VariantPayloadSchema::Unnamed(types)
}

fn vnamed(fields: Vec<(&str, SchemaEnum)>) -> VariantPayloadSchema {
    VariantPayloadSchema::Named(
        fields
            .into_iter()
            .map(|(k, v)| (k.to_string(), v))
            .collect(),
    )
}

fn option(inner: SchemaEnum) -> SchemaEnum {
    SchemaEnum::Option(Box::new(inner))
}

fn array(inner: SchemaEnum) -> SchemaEnum {
    SchemaEnum::Array(Box::new(inner))
}

fn map(k: SchemaEnum, v: SchemaEnum) -> SchemaEnum {
    SchemaEnum::Map(Box::new(k), Box::new(v))
}

fn tuple(types: Vec<SchemaEnum>) -> SchemaEnum {
    SchemaEnum::Tuple(types)
}

fn ref_(idx: usize) -> SchemaEnum {
    SchemaEnum::Ref(idx)
}

// ============================================================================
// Schema Type Parser Tests — comparing whole Schema via Eq
// ============================================================================

#[test]
fn test_type_record() {
    let ts = parse_schema(r#"schema Point = { x: i32, y: i64 };"#).unwrap();
    let expected = s(vec![record(vec![
        ("x", SchemaEnum::I32),
        ("y", SchemaEnum::I64),
    ])]);
    assert_eq!(ts, expected);
}

#[test]
fn test_type_variant() {
    let ts = parse_schema(r#"schema Option = variant { Some(string), None };"#).unwrap();
    let expected = s(vec![variant(vec![
        ("Some", vunnamed(vec![SchemaEnum::String])),
        ("None", vunit()),
    ])]);
    assert_eq!(ts, expected);
}

#[test]
fn test_variant_with_record_payload() {
    let ts = parse_schema(
        r#"
        schema WebEvent = variant {
            PageLoad,
            KeyPress { key: string, modifiers: i32 },
            Click(i32, i64),
        };
    "#,
    )
    .unwrap();
    let expected = s(vec![variant(vec![
        ("PageLoad", vunit()),
        (
            "KeyPress",
            vnamed(vec![
                ("key", SchemaEnum::String),
                ("modifiers", SchemaEnum::I32),
            ]),
        ),
        ("Click", vunnamed(vec![SchemaEnum::I32, SchemaEnum::I64])),
    ])]);
    assert_eq!(ts, expected);
}

#[test]
fn test_compound_types() {
    let ts = parse_schema(
        r#"
        schema Container = {
            opt: optional i32,
            lst: [string],
            kv:  map string => i64,
            arr: [bool],
        };
    "#,
    )
    .unwrap();
    let expected = s(vec![record(vec![
        ("opt", option(SchemaEnum::I32)),
        ("lst", array(SchemaEnum::String)),
        ("kv", map(SchemaEnum::String, SchemaEnum::I64)),
        ("arr", array(SchemaEnum::Bool)),
    ])]);
    assert_eq!(ts, expected);
}

#[test]
fn test_nested_compound_types() {
    let ts = parse_schema(
        r#"
        schema Nested = {
            deep: optional [optional i32],
        };
    "#,
    )
    .unwrap();
    let expected = s(vec![record(vec![(
        "deep",
        option(array(option(SchemaEnum::I32))),
    )])]);
    assert_eq!(ts, expected);
}

#[test]
fn test_tuple_and_unit_types() {
    let ts = parse_schema(
        r#"
        schema WithTuples = {
            pair: (i32, i64),
            unit: (),
        };
    "#,
    )
    .unwrap();
    let expected = s(vec![record(vec![
        ("pair", tuple(vec![SchemaEnum::I32, SchemaEnum::I64])),
        ("unit", tuple(vec![])),
    ])]);
    assert_eq!(ts, expected);
}

#[test]
fn test_type_reference() {
    let ts = parse_schema(
        r#"
        schema Point = { x: i32, y: i64 };
        schema Line = { start: Point, end: Point };
    "#,
    )
    .unwrap();
    let expected = s(vec![
        record(vec![("start", ref_(1)), ("end", ref_(1))]),
        record(vec![("x", SchemaEnum::I32), ("y", SchemaEnum::I64)]),
    ]);
    assert_eq!(ts, expected);
}

#[test]
fn test_recursive_type() {
    let ts = parse_schema(r#"schema List = variant { Cons(i32, List), Nil };"#).unwrap();
    let expected = s(vec![variant(vec![
        ("Cons", vunnamed(vec![SchemaEnum::I32, ref_(0)])),
        ("Nil", vunit()),
    ])]);
    assert_eq!(ts, expected);
}

#[test]
fn test_mutually_recursive_types() {
    let ts = parse_schema(
        r#"
        schema A = { b: B };
        schema B = { a: A };
    "#,
    )
    .unwrap();
    let expected = s(vec![
        record(vec![("a", ref_(1))]),
        record(vec![("b", ref_(0))]),
    ]);
    assert_eq!(ts, expected);
}

#[test]
fn test_forward_reference() {
    let ts = parse_schema(
        r#"
        schema Parent = { child: Child };
        schema Child = { name: string };
    "#,
    )
    .unwrap();
    let expected = s(vec![
        record(vec![("name", SchemaEnum::String)]),
        record(vec![("child", ref_(0))]),
    ]);
    assert_eq!(ts, expected);
}

#[test]
fn test_complex_graph() {
    let ts = parse_schema(r#"schema Tree = variant { Node(i32, [Tree]), Leaf };"#).unwrap();
    let expected = s(vec![variant(vec![
        ("Node", vunnamed(vec![SchemaEnum::I32, array(ref_(0))])),
        ("Leaf", vunit()),
    ])]);
    assert_eq!(ts, expected);
}

#[test]
fn test_three_way_recursion() {
    let ts = parse_schema(
        r#"
        schema Expr = { left: [Expr], op: Op, right: [Expr] };
        schema Op = variant { Add, Mul, Custom(Fn) };
        schema Fn = { name: string };
    "#,
    )
    .unwrap();
    // Fn=0 (root), Expr=1, Op=2
    let expected = s(vec![
        record(vec![("name", SchemaEnum::String)]),
        record(vec![
            ("left", array(ref_(1))),
            ("op", ref_(2)),
            ("right", array(ref_(1))),
        ]),
        variant(vec![
            ("Add", vunit()),
            ("Mul", vunit()),
            ("Custom", vunnamed(vec![ref_(0)])),
        ]),
    ]);
    assert_eq!(ts, expected);
}

#[test]
fn test_duplicate_type_error() {
    let result = parse_schema(
        r#"
        schema Foo = { x: i32 };
        schema Foo = variant { Bar, Baz };
    "#,
    );
    assert!(matches!(result, Err(SchemaParseError::DuplicateTypeName(ref n)) if n == "Foo"));
}

#[test]
fn test_unknown_type_error() {
    let result = parse_schema("schema User = { profile: UnknownType };");
    assert!(matches!(result, Err(SchemaParseError::UnknownType(ref n)) if n == "UnknownType"));
}

#[test]
fn test_trailing_commas() {
    let ts = parse_schema(r#"schema Point = { x: i32, y: i64, };"#).unwrap();
    let expected = s(vec![record(vec![
        ("x", SchemaEnum::I32),
        ("y", SchemaEnum::I64),
    ])]);
    assert_eq!(ts, expected);
}

#[test]
fn test_parse_error() {
    assert!(parse_schema("schema Broken = { field i32, };").is_err());
}

#[test]
fn test_all_primitives() {
    let ts = parse_schema(
        r#"
        schema AllTypes = { a: i32, b: i64, c: bool, d: string, e: bytes 32 };
    "#,
    )
    .unwrap();
    let expected = s(vec![record(vec![
        ("a", SchemaEnum::I32),
        ("b", SchemaEnum::I64),
        ("c", SchemaEnum::Bool),
        ("d", SchemaEnum::String),
        ("e", SchemaEnum::Bytes(32)),
    ])]);
    assert_eq!(ts, expected);
}

#[test]
fn test_comments_in_types() {
    let ts = parse_schema(
        r#"
        // A 2D point
        schema Point = { x: i32 /* horizontal */, y: i64 };
    "#,
    )
    .unwrap();
    assert!(!ts.schemas().is_empty());
}

#[test]
fn test_schema_is_well_defined() {
    let ts = parse_schema(
        r#"
        schema Json = variant {
            Null, Bool(bool), Number(i64), Str(string), Array([Json]), Object(map string => Json),
        };
    "#,
    )
    .unwrap();
    assert!(ts.is_well_defined());
}

#[test]
fn test_empty_record() {
    let ts = parse_schema(r#"schema Unit = {};"#).unwrap();
    let expected = s(vec![record(vec![])]);
    assert_eq!(ts, expected);
}

#[test]
fn test_unicode_type_names() {
    let ts = parse_schema(
        r#"
        schema Point = { x: i32, y: i64 };
        schema Line = { Start: Point, End: Point };
    "#,
    )
    .unwrap();
    // Line=0 (root), Point=1
    let expected = s(vec![
        record(vec![("Start", ref_(1)), ("End", ref_(1))]),
        record(vec![("x", SchemaEnum::I32), ("y", SchemaEnum::I64)]),
    ]);
    assert_eq!(ts, expected);
}

#[test]
fn test_export_root() {
    let ts = parse_schema(
        r#"
        schema Point = { x: i32, y: i64 };
        schema Line = { start: Point, end: Point };
        export Point;
    "#,
    )
    .unwrap();
    let expected = s(vec![
        record(vec![("x", SchemaEnum::I32), ("y", SchemaEnum::I64)]),
        record(vec![("start", ref_(0)), ("end", ref_(0))]),
    ]);
    assert_eq!(ts, expected);
}

#[test]
fn test_export_recursive_root() {
    let ts = parse_schema(
        r#"
        schema A = { b: B };
        schema B = { a: A };
        export A;
    "#,
    )
    .unwrap();
    // A=0 (export), B=1
    let expected = s(vec![
        record(vec![("b", ref_(1))]),
        record(vec![("a", ref_(0))]),
    ]);
    assert_eq!(ts, expected);
}

// ============================================================================
// Value ↔ Schema validation tests
// ============================================================================

#[test]
fn test_check_record_valid() {
    let schema = parse_schema(r#"schema Root = { x: i32, y: i64 };"#).unwrap();
    let value = parse_value(r#"{ x: 42, y: 100i64 }"#).unwrap();
    assert!(value.check(&schema, false));
}

#[test]
fn test_check_record_extra_field() {
    let schema = parse_schema(r#"schema Root = { x: i32 };"#).unwrap();
    let value = parse_value(r#"{ x: 42, y: 99 }"#).unwrap();
    assert!(value.check(&schema, false));
}

#[test]
fn test_check_record_missing_field() {
    let schema = parse_schema(r#"schema Root = { x: i32, y: i64 };"#).unwrap();
    let value = parse_value(r#"{ x: 42 }"#).unwrap();
    assert!(!value.check(&schema, false));
}

#[test]
fn test_check_record_wrong_type() {
    let schema = parse_schema(r#"schema Root = { x: i32 };"#).unwrap();
    let value = parse_value(r#"{ x: "hello" }"#).unwrap();
    assert!(!value.check(&schema, false));
}

#[test]
fn test_check_variant_valid() {
    let schema = parse_schema(r#"schema Root = variant { Ok(i32), Err(string) };"#).unwrap();
    let ok_val = parse_value("Ok::200").unwrap();
    let err_val = parse_value(r#"Err::"boom""#).unwrap();
    assert!(ok_val.check(&schema, false));
    assert!(err_val.check(&schema, false));
}

#[test]
fn test_check_variant_unknown_tag() {
    let schema = parse_schema(r#"schema Root = variant { Ok(i32), Err(string) };"#).unwrap();
    let value = parse_value("Unknown::42").unwrap();
    assert!(!value.check(&schema, false));
}

#[test]
fn test_check_option_some() {
    let schema = parse_schema(r#"schema Root = optional i32;"#).unwrap();
    assert!(parse_value("some 42").unwrap().check(&schema, false));
}

#[test]
fn test_check_option_none() {
    let schema = parse_schema(r#"schema Root = optional i32;"#).unwrap();
    assert!(parse_value("null").unwrap().check(&schema, false));
}

#[test]
fn test_check_array_valid() {
    let schema = parse_schema(r#"schema Root = [i32];"#).unwrap();
    let value = parse_value("[1, 2, 3]").unwrap();
    assert!(value.check(&schema, false));
}

#[test]
fn test_check_array_wrong_element() {
    let schema = parse_schema(r#"schema Root = [i32];"#).unwrap();
    let value = parse_value(r#"["a", "b"]"#).unwrap();
    assert!(!value.check(&schema, false));
}

#[test]
fn test_check_tuple_valid() {
    let schema = parse_schema(r#"schema Root = (i32, string, bool);"#).unwrap();
    let value = parse_value(r#"(-1, "hi", true)"#).unwrap();
    assert!(value.check(&schema, false));
}

#[test]
fn test_check_tuple_too_short() {
    let schema = parse_schema(r#"schema Root = (i32, string);"#).unwrap();
    let value = parse_value(r#"(42,)"#).unwrap();
    assert!(!value.check(&schema, false));
}

#[test]
fn test_check_map_valid() {
    let schema = parse_schema(r#"schema Root = map string => i32;"#).unwrap();
    let value = parse_value(r#"map { "a": 1, "b": 2 }"#).unwrap();
    assert!(value.check(&schema, false));
}

#[test]
fn test_check_map_wrong_value_type() {
    let schema = parse_schema(r#"schema Root = map string => i32;"#).unwrap();
    let value = parse_value(r#"{ "a": true }"#).unwrap();
    assert!(!value.check(&schema, false));
}

#[test]
fn test_check_i32_promotes_to_i64() {
    let schema = parse_schema(r#"schema Root = i64;"#).unwrap();
    let value = parse_value("42").unwrap();
    assert!(value.check(&schema, false));
}

#[test]
fn test_check_recursive_type() {
    let schema = parse_schema(r#"schema List = variant { Cons(i32, List), Nil };"#).unwrap();
    let value = parse_value("Cons::(1, Cons::(2, Nil))").unwrap();
    assert!(value.check(&schema, false));
}

#[test]
fn test_check_recursive_type_invalid() {
    let schema = parse_schema(r#"schema List = variant { Cons(i32, List), Nil };"#).unwrap();
    let value = parse_value("Cons::(1, Cons::(true, Nil))").unwrap();
    assert!(!value.check(&schema, false));
}

#[test]
fn test_check_array_matches_tuple_loose() {
    let schema = parse_schema(r#"schema Root = (i32, i64, bool);"#).unwrap();
    let value = parse_value("[42, 99i64, true]").unwrap();
    assert!(!value.check(&schema, false));
    assert!(value.check(&schema, true));
}

#[test]
fn test_check_named_variant_payload() {
    let schema = parse_schema(r#"schema Root = variant { Click { x: i32, y: i64 } };"#).unwrap();
    let value = parse_value("Click::{ x: 10, y: 20i64 }").unwrap();
    assert!(value.check(&schema, false));
}

#[test]
fn test_check_bytes() {
    let schema = parse_schema(r#"schema Root = bytes 5;"#).unwrap();
    let value = parse_value(r#"b"SGVsbG8=""#).unwrap();
    assert!(value.check(&schema, false));
}

#[test]
fn test_check_type_reference() {
    let schema = parse_schema(
        r#"
        schema Point = { x: i32, y: i64 };
        export Point;
    "#,
    )
    .unwrap();
    let value = parse_value(r#"{ x: 3, y: 4i64 }"#).unwrap();
    assert!(value.check(&schema, false));
}

#[test]
fn test_check_mutually_recursive() {
    let schema = parse_schema(
        r#"
        schema A = { b: B };
        schema B = variant { None, Some(A) };
        export A;
    "#,
    )
    .unwrap();
    let value = parse_value(r#"{ b: Some::({ b: None }) }"#).unwrap();
    assert!(value.check(&schema, false));
}

#[test]
fn test_extreme_complex_parse_and_check() {
    // Define a highly complex, self-referencing Schema, simulating the recursive structure of JSON.
    let complex_schema_def = r#"
        // Recursive JSON type.
        schema Json = variant {
            Null,                                   // unit variant
            Bool(bool),                             // unnamed single
            Number(i64),                            // unnamed single
            Str(string),                            // unnamed single
            Array([Json]),                          // generic array of self
            Object(map string => Json),              // generic map to self
        };
        export Json;
    "#;

    let schema =
        crate::parser::parse_schema(complex_schema_def).expect("Failed to parse complex schema");
    assert!(schema.is_well_defined(), "Schema should be well-defined");

    // Construct a deeply nested JSON value covering all variants.
    let json_value_str = r#"
        Object::map {
            "name": Str::"synthase",
            "version": Number::1,
            "active": Bool::true,
            "nothing": Null,
            "tags": Array::[
                Str::"wasm",
                Str::"scheduler",
                Null,
                Number::42,
                Bool::false,
            ],
            "metadata": Object::map {
                "created": Str::"2025-01-01",
                "verified": Bool::true,
                "count": Number::100,
            },
            "extra": Null,
        }
    "#;

    let value = crate::parser::parse_value(json_value_str)
        .expect("Failed to parse complex JSON-like value");
    assert!(
        value.check(&schema, true),
        "Value should match schema in loose mode"
    );
    assert!(
        value.check(&schema, false),
        "Value should match schema in strict mode"
    );

    // Test another deeply nested invalid value (missing required fields but won't affect check, as only the variant matching part is verified).
    let illegal_value = r#"
        Object::map {
            "bad": Wrong::"nope",
        }
    "#;
    let illegal_val = crate::parser::parse_value(illegal_value)
        .expect("Parsing should succeed even if value is invalid for schema");
    assert!(
        !illegal_val.check(&schema, false),
        "Value with unknown variant should fail check"
    );

    // Test high-level combinations of nested Map, Array, Bytes, Option, and Tuples.
    let super_complex_schema = r#"
        schema Packet = {
            header: {
                id: (i32, i64),                     // Tuple.
                flags: [bool],                      // Array.
                auth: optional string,              // Optional field.
            },
            payload: variant {
                Text(string),
                Binary(bytes 32),                   // Fixed-length byte array.
                Events([variant { Click(i32, i64), Key(string) }]),
            },
            meta: map string => optional [i64],       // Complex Map.
        };
        export Packet;
    "#;

    let super_schema = crate::parser::parse_schema(super_complex_schema)
        .expect("Failed to parse super complex schema");
    assert!(super_schema.is_well_defined());
    println!("Parsed super complex schema: {}", super_schema);

    // Construct a compliant complex value for the schema
    let super_value_str = r#"
        {
            header: {
                id: (10, 20i64),
                flags: [true, false, true],
                auth: some "secret_token",
            },
            payload: Binary::b"QUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUE=",  // 32-byte base64.
            meta: map {
                "user": some [1i64, 2i64, 3i64],
                "guest": null,
            },
        }
    "#;

    let super_value =
        crate::parser::parse_value(super_value_str).expect("Failed to parse super complex value");
    assert!(super_value.check(&super_schema, true));
    assert!(super_value.check(&super_schema, false));
    println!("Parsed super complex value: {}", super_value);

    // Test Unicode identifiers and trailing commas.
    let unicode_schema = r#"
        schema Point = { x: i32, y: i64, };
        schema Line = { Start: Point, End: Point, };
        export Line;
    "#;
    let unicode_schema =
        crate::parser::parse_schema(unicode_schema).expect("Unicode schema should parse");
    let unicode_value = r#"
        {
            Start: { x: 5, y: 10i64, },
            End: { x: 15, y: 20i64, },
        }
    "#;
    let unicode_val =
        crate::parser::parse_value(unicode_value).expect("Unicode value should parse");
    assert!(unicode_val.check(&unicode_schema, false));
}

#[test]
fn test_any() {
    let any_schema = parse_schema(r#"schema Any = Any; // 0 = ref 0"#).unwrap();
    let other_schema = parse_schema(r#"schema Other = { field: i32 };"#).unwrap();
    let value = parse_value(r#"some "hello""#).unwrap();
    assert!(value.check(&any_schema, false));
    assert!(other_schema.subof(&any_schema));
    assert!(!any_schema.subof(&other_schema));
}

#[test]
fn test_any_subtyping_basic() {
    // Any type: direct infinite cycle.
    let any_schema = parse_schema(r#"schema Any = Any;"#).unwrap();
    let any_schema_2 = parse_schema(r#"schema Top = Top;"#).unwrap();

    // Common types.
    let i32_schema = parse_schema(r#"schema Int = i32;"#).unwrap();
    let record_schema = parse_schema(r#"schema Rec = { field: i32 };"#).unwrap();

    // Guarded Recursive type.
    let list_schema = parse_schema(r#"schema List = optional { val: i32, next: List };"#).unwrap();

    // 1. Any <: Any (True)
    assert!(
        any_schema.subof(&any_schema),
        "Any should be a subtype of itself"
    );
    assert!(
        any_schema.subof(&any_schema_2),
        "Different pure Ref cycles should be equivalent to Any"
    );
    assert!(any_schema_2.subof(&any_schema));

    // 2. T <: Any (True)
    assert!(
        i32_schema.subof(&any_schema),
        "i32 should be a subtype of Any"
    );
    assert!(
        record_schema.subof(&any_schema),
        "Record should be a subtype of Any"
    );
    assert!(
        list_schema.subof(&any_schema),
        "Guarded Recursive type should be a subtype of Any"
    );

    // 3. Any is not a subtype of T (False - satisfying the core requirement of Any being the Top type).
    assert!(
        !any_schema.subof(&i32_schema),
        "Any should NOT be a subtype of i32"
    );
    assert!(
        !any_schema.subof(&record_schema),
        "Any should NOT be a subtype of Record"
    );
    assert!(
        !any_schema.subof(&list_schema),
        "Any should NOT be a subtype of List"
    );
}

#[test]
fn test_any_subtyping_indirect_cycles() {
    // Indirect mutual reference with no structural guarding; non-guarded recursion, equivalent to Any.
    let mutual_any = parse_schema(
        r#"
            schema A = B;
            schema B = C;
            schema C = A;
        "#,
    )
    .unwrap();

    let i32_schema = parse_schema(r#"schema Int = i32;"#).unwrap();
    let any_schema = parse_schema(r#"schema Any = Any;"#).unwrap();

    // Any <: MutualAny <: Any
    assert!(
        mutual_any.subof(&any_schema),
        "Mutual cycle should act as Any"
    );
    assert!(
        any_schema.subof(&mutual_any),
        "Any should act as Mutual cycle Any"
    );

    // T <: MutualAny (True)
    assert!(i32_schema.subof(&mutual_any));

    // MutualAny </: T (False)
    assert!(!mutual_any.subof(&i32_schema));
}

#[test]
fn test_any_subtyping_nested_composition() {
    // Test placing Any inside other compound data structures to see if Covariance and Contravariance logic is maintained correctly.

    // --- 1. Record Width/Depth ---
    let rec_any =
        parse_schema(r#"schema Any = Any; schema Rec = { id: i32, payload: Any };"#).unwrap();
    let rec_str = parse_schema(r#"schema RecStr = { id: i32, payload: string };"#).unwrap();
    let rec_i64 = parse_schema(r#"schema RecI64 = { id: i32, payload: i64 };"#).unwrap();

    assert!(rec_str.subof(&rec_any), "{{id, string}} <: {{id, Any}}");
    assert!(rec_i64.subof(&rec_any), "{{id, i64}} <: {{id, Any}}");
    assert!(!rec_any.subof(&rec_str), "{{id, Any}} </: {{id, string}}");

    // --- 2. Array ---
    let array_any = parse_schema(r#"schema Any = Any; schema Arr = [Any];"#).unwrap();
    let array_i32 = parse_schema(r#"schema ArrI32 = [i32];"#).unwrap();

    assert!(array_i32.subof(&array_any), "[i32] <: [Any]");
    assert!(!array_any.subof(&array_i32), "[Any] </: [i32]");

    // --- 3. Tuple (Width & Depth) ---
    let tuple_any = parse_schema(r#"schema Any = Any; schema Tup = (i32, Any);"#).unwrap();
    let tuple_str_bool = parse_schema(r#"schema TupStr = (i32, string, bool);"#).unwrap();

    // Tuple Width Subtyping: longer tuples can be subtypes of shorter ones.
    assert!(
        tuple_str_bool.subof(&tuple_any),
        "(i32, string, bool) <: (i32, Any)"
    );
    assert!(
        !tuple_any.subof(&tuple_str_bool),
        "(i32, Any) </: (i32, string, bool)"
    );

    // --- 4. Variant ---
    let variant_any =
        parse_schema(r#"schema Any = Any; schema V = variant { A(Any), B };"#).unwrap();
    let variant_i32 = parse_schema(r#"schema V = variant { A(i32), B };"#).unwrap();

    assert!(
        variant_i32.subof(&variant_any),
        "variant {{ A(i32) }} <: variant {{ A(Any) }}"
    );
    assert!(
        !variant_any.subof(&variant_i32),
        "variant {{ A(Any) }} </: variant {{ A(i32) }}"
    );

    // --- 5. Map (Key is invariant, Value is covariant) ---
    let map_any = parse_schema(r#"schema Any = Any; schema M = map string => Any;"#).unwrap();
    let map_str = parse_schema(r#"schema M = map string => string;"#).unwrap();

    assert!(
        map_str.subof(&map_any),
        "map string => string <: map string => Any"
    );
    assert!(
        !map_any.subof(&map_str),
        "map string => Any </: map string => string"
    );
}

#[test]
fn test_any_value_checking_comprehensive() {
    let any_schema = parse_schema(r#"schema Any = Any;"#).unwrap();
    let mutual_any_schema = parse_schema(r#"schema A = B; schema B = A;"#).unwrap();

    // Construct values with various data shapes.
    let test_values = vec![
        r#"100"#,                       // i32
        r#"-999i64"#,                   // i64
        r#"true"#,                      // bool
        r#""just a string""#,           // string
        r#"b"d2FzbQ==""#,               // bytes (base64)
        r#"null"#,                      // none/null
        r#"some 42"#,                   // option
        r#"[1, 2, 3, "mixed?"]"#,       // array
        r#"{ "x": 10, "y": 20 }"#,      // object / record
        r#"(10, 20i64, true)"#,         // tuple
        r#"MapName::map { "k": "v" }"#, // map
    ];

    for val_str in test_values {
        let value = parse_value(val_str)
            .unwrap_or_else(|_| panic!("Failed to parse JSON value: {}", val_str));

        // Since Any is the Top type, it should successfully validate (check) data of any shape.
        assert!(
            value.check(&any_schema, false),
            "Value {} should match 'Any' schema in strict mode",
            val_str
        );
        assert!(
            value.check(&mutual_any_schema, false),
            "Value {} should match 'Mutual Any' schema in strict mode",
            val_str
        );
    }
}
