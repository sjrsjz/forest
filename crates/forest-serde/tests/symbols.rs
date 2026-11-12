use std::{
    collections::{BTreeMap, hash_map::DefaultHasher},
    hash::{Hash, Hasher},
    io::Cursor,
};

use forest_serde::{
    DeriveSchema, DeriveValue,
    adt::{AsSchema, AsValue, Schema, Value, VariantPayloadValue},
    parser::{parse_schema, parse_value},
    symbol::{
        FIRST_DYNAMIC_SYMBOL, NONE_SYMBOL, NONE_TAG, RESERVED_SYMBOLS, SOME_SYMBOL, SOME_TAG,
    },
};

#[test]
fn decoded_names_share_storage_and_survive_the_input_document() {
    let original = parse_value("[{ shared: 1 }, { shared: 2 }]").unwrap();
    let bytes = bincode::serialize(&original).unwrap();
    let decoded: Value = bincode::deserialize(&bytes).unwrap();
    drop(bytes);
    let Value::Array(rows) = decoded else {
        unreachable!()
    };
    let name = |row: &Value| {
        let Value::Record(fields) = row else {
            unreachable!()
        };
        fields.first_key_value().unwrap().0.clone()
    };
    let first = name(&rows[0]);
    let second = name(&rows[1]);
    assert_eq!(first.as_str().as_ptr(), second.as_str().as_ptr());
    let static_name = forest_serde::symbol::Symbol::from_static("shared");
    assert_eq!(first, static_name);
    let mut map = BTreeMap::from([(first.clone(), 1)]);
    assert_eq!(map.insert(static_name, 2), Some(1));
    assert_eq!(map.get("shared"), Some(&2));
    drop(rows);
    assert_eq!(first.as_str(), "shared");
}

#[derive(Debug, PartialEq, DeriveSchema, DeriveValue)]
enum OrdinaryOption<T> {
    None,
    Some(T),
}

#[test]
fn optional_is_the_same_structural_enum_in_every_representation() {
    let option = Option::<i64>::as_schema();
    let ordinary = OrdinaryOption::<i64>::as_schema();
    assert_eq!(option, ordinary);
    assert_eq!(
        option,
        parse_schema("schema T = variant { None, Some(i64) };").unwrap()
    );
    assert_eq!(option, parse_schema("schema T = optional i64;").unwrap());
    for (rust, explicit) in [
        (None::<i64>, OrdinaryOption::<i64>::None),
        (Some(42_i64), OrdinaryOption::Some(42_i64)),
    ] {
        let value = rust.as_value();
        assert_eq!(value, explicit.as_value());
        assert_eq!(OrdinaryOption::from_value(value.clone()).unwrap(), explicit);
        assert_eq!(
            Option::<i64>::from_value(explicit.as_value()).unwrap(),
            rust
        );
        assert_eq!(parse_value(&value.to_string()).unwrap(), value);
        let json = serde_json::to_vec(&value).unwrap();
        assert_eq!(serde_json::from_slice::<Value>(&json).unwrap(), value);
        assert!(std::str::from_utf8(&json).unwrap().contains("Variant"));
    }
    assert_ne!(Some(None::<i64>).as_value(), None::<Option<i64>>.as_value());
    assert_eq!(
        Option::<Option<i64>>::from_value(Some(None::<i64>).as_value()).unwrap(),
        Some(None)
    );
}

#[test]
fn reserved_names_have_documented_indices_and_no_wire_strings() {
    assert!(RESERVED_SYMBOLS.contains(&(NONE_SYMBOL, NONE_TAG)));
    assert!(RESERVED_SYMBOLS.contains(&(SOME_SYMBOL, SOME_TAG)));
    // Independent ABI vector: table count, Variant tag, symbol, Unnamed tag, item count.
    let expected =
        bincode::serialize(&(Vec::<String>::new(), (8_u32, (NONE_SYMBOL, (0_u32, 0_u64)))))
            .unwrap();
    assert_eq!(bincode::serialize(&Value::none()).unwrap(), expected);
    assert_eq!(
        bincode::deserialize::<Value>(&expected).unwrap(),
        Value::none()
    );
    let value = Value::some(Value::I32(42));
    let expected = bincode::serialize(&(
        Vec::<String>::new(),
        (8_u32, (SOME_SYMBOL, (0_u32, vec![(0_u32, 42_i32)]))),
    ))
    .unwrap();
    assert_eq!(bincode::serialize(&value).unwrap(), expected);
    assert_eq!(bincode::deserialize::<Value>(&expected).unwrap(), value);
    // Reserving a name does not constrain ordinary enum payloads bearing it.
    let unusual = Value::Variant(
        SOME_TAG.into(),
        VariantPayloadValue::Named(BTreeMap::from([("item".into(), Value::Bool(true))])),
    );
    let bytes = bincode::serialize(&unusual).unwrap();
    assert_eq!(bincode::deserialize::<Value>(&bytes).unwrap(), unusual);
    assert!(Option::<bool>::from_value(unusual).is_err());
}

#[test]
fn all_structural_names_share_one_table_including_nested_payloads_and_map_keys() {
    let value = parse_value(
        r#"[
        Row::{ field: 7i64, flag: true },
        Row::{ field: 8i64, flag: false },
        map { Key::({ field: b"AAE=" }): some ("ordinary text", -2, false) },
        null
    ]"#,
    )
    .unwrap();
    let bytes = bincode::serialize(&value).unwrap();
    let names: Vec<String> = bincode::deserialize_from(&mut Cursor::new(&bytes)).unwrap();
    assert_eq!(names, ["Key", "Row", "field", "flag"]);
    assert_eq!(bincode::deserialize::<Value>(&bytes).unwrap(), value);
    let schema = parse_schema("schema T = { field: i64, flag: bool, text: string, fixed: bytes 2, data: bytes, list: [T], choice: variant { Row { field: i32 }, Key(optional T) }, pair: (i32, string), mapping: map string => T };").unwrap();
    let bytes = bincode::serialize(&schema).unwrap();
    assert_eq!(bincode::deserialize::<Schema>(&bytes).unwrap(), schema);
}

#[test]
fn local_symbol_numbers_do_not_change_equality_order_hash_or_map_keys() {
    let document = |names: Vec<&str>, symbol: u32| {
        bincode::serialize(&(names, (9_u32, vec![(symbol, (1_u32, 7_i64))]))).unwrap()
    };
    let first: Value = bincode::deserialize(&document(vec!["key"], FIRST_DYNAMIC_SYMBOL)).unwrap();
    let second: Value =
        bincode::deserialize(&document(vec!["unused", "key"], FIRST_DYNAMIC_SYMBOL + 1)).unwrap();
    let hash = |value: &Value| {
        let mut state = DefaultHasher::new();
        value.hash(&mut state);
        state.finish()
    };
    assert_eq!(first, second);
    assert_eq!(first.cmp(&second), std::cmp::Ordering::Equal);
    assert_eq!(hash(&first), hash(&second));
    let mut map = BTreeMap::from([(first, Value::I32(1))]);
    assert_eq!(map.insert(second, Value::I32(2)), Some(Value::I32(1)));
    assert_eq!(map.len(), 1);
}

#[test]
fn malformed_symbol_documents_are_rejected_at_the_decode_boundary() {
    for names in [vec!["field", "field"], vec![NONE_TAG], vec![SOME_TAG]] {
        let bytes = bincode::serialize(&(names, (0_u32, 7_i32))).unwrap();
        assert!(bincode::deserialize::<Value>(&bytes).is_err());
    }
    let unknown = bincode::serialize(&(
        Vec::<String>::new(),
        (8_u32, (FIRST_DYNAMIC_SYMBOL, (0_u32, 0_u64))),
    ))
    .unwrap();
    assert!(bincode::deserialize::<Value>(&unknown).is_err());
    let duplicate = bincode::serialize(&(
        vec!["field"],
        (
            9_u32,
            vec![
                (FIRST_DYNAMIC_SYMBOL, (0_u32, 1_i32)),
                (FIRST_DYNAMIC_SYMBOL, (0_u32, 2_i32)),
            ],
        ),
    ))
    .unwrap();
    assert!(bincode::deserialize::<Value>(&duplicate).is_err());
    let invalid_ref = bincode::serialize(&(Vec::<String>::new(), vec![(10_u32, 1_u64)])).unwrap();
    assert!(bincode::deserialize::<Schema>(&invalid_ref).is_err());
    let bytes = bincode::serialize(&Value::some(Value::String("payload".into()))).unwrap();
    for end in 0..bytes.len() {
        assert!(bincode::deserialize::<Value>(&bytes[..end]).is_err());
    }
}
