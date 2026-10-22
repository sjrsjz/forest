use std::collections::{BTreeMap, HashMap};

use crate::adt::{Schema, SchemaEnum, VariantPayloadSchema};

// ============================================================================
// AST Types
// ============================================================================

#[derive(Debug, Clone, PartialEq)]
pub enum AstSchema {
    I32,
    I64,
    Bool,
    String,
    Bytes(usize),
    ByteBuffer,
    Option(Box<AstSchema>),
    Map(Box<AstSchema>, Box<AstSchema>),
    Array(Box<AstSchema>),
    Tuple(Vec<AstSchema>),
    Ident(String),
    Record(BTreeMap<String, AstSchema>),
    Variant(BTreeMap<String, AstVariantPayload>),
}

#[derive(Debug, Clone, PartialEq)]
pub enum AstVariantPayload {
    Unnamed(Vec<AstSchema>),
    Named(BTreeMap<String, AstSchema>),
}

#[derive(Debug, Clone, PartialEq)]
pub struct AstDocument {
    pub declarations: Vec<(String, AstSchema)>,
    pub export: Option<String>,
}

// ============================================================================
// Error Type
// ============================================================================

#[derive(Debug, Clone, PartialEq)]
pub enum SchemaParseError {
    EmptyDocument,
    LexicalError { location: usize },
    LalrpopError(String),
    DuplicateTypeName(String),
    UnknownType(String),
}

impl std::fmt::Display for SchemaParseError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SchemaParseError::EmptyDocument => write!(f, "schema document has no root type"),
            SchemaParseError::LexicalError { location } => {
                write!(f, "Lexical error at offset {}", location)
            }
            SchemaParseError::LalrpopError(msg) => {
                write!(f, "Parse error: {}", msg)
            }
            SchemaParseError::DuplicateTypeName(name) => {
                write!(f, "Duplicate type name: '{}'", name)
            }
            SchemaParseError::UnknownType(name) => {
                write!(f, "Unknown type: '{}'", name)
            }
        }
    }
}

impl std::error::Error for SchemaParseError {}

pub fn convert_lalrpop_error(
    err: lalrpop_util::ParseError<usize, crate::parser::lexer::SchemaToken, usize>,
) -> SchemaParseError {
    use lalrpop_util::ParseError as LPError;
    match err {
        LPError::InvalidToken { location } => SchemaParseError::LexicalError { location },
        LPError::UnrecognizedEof { location, expected } => SchemaParseError::LalrpopError(format!(
            "Unexpected EOF at {}. Expected: {}",
            location,
            expected.join(", ")
        )),
        LPError::UnrecognizedToken { token, expected } => SchemaParseError::LalrpopError(format!(
            "Unexpected token at {}-{}. Expected: {}",
            token.0,
            token.2,
            expected.join(", ")
        )),
        LPError::ExtraToken { token } => {
            SchemaParseError::LalrpopError(format!("Extra token at {}-{}", token.0, token.2))
        }
        LPError::User { error } => SchemaParseError::LexicalError { location: error },
    }
}

// ============================================================================
// Resolver
// ============================================================================

fn resolve_type(
    ast: &AstSchema,
    name_to_index: &HashMap<String, usize>,
) -> Result<SchemaEnum, SchemaParseError> {
    match ast {
        AstSchema::I32 => Ok(SchemaEnum::I32),
        AstSchema::I64 => Ok(SchemaEnum::I64),
        AstSchema::Bool => Ok(SchemaEnum::Bool),
        AstSchema::String => Ok(SchemaEnum::String),
        AstSchema::Bytes(n) => Ok(SchemaEnum::Bytes(*n)),
        AstSchema::ByteBuffer => Ok(SchemaEnum::ByteBuffer),
        AstSchema::Option(inner) => Ok(SchemaEnum::Option(Box::new(resolve_type(
            inner,
            name_to_index,
        )?))),
        AstSchema::Array(inner) => Ok(SchemaEnum::Array(Box::new(resolve_type(
            inner,
            name_to_index,
        )?))),
        AstSchema::Map(k, v) => Ok(SchemaEnum::Map(
            Box::new(resolve_type(k, name_to_index)?),
            Box::new(resolve_type(v, name_to_index)?),
        )),
        AstSchema::Tuple(types) => {
            let resolved: Result<Vec<_>, _> = types
                .iter()
                .map(|t| resolve_type(t, name_to_index))
                .collect();
            Ok(SchemaEnum::Tuple(resolved?))
        }
        AstSchema::Ident(name) => name_to_index
            .get(name.as_str())
            .map(|&idx| SchemaEnum::Ref(idx))
            .ok_or_else(|| SchemaParseError::UnknownType(name.clone())),
        AstSchema::Record(fields) => {
            let mut map = BTreeMap::new();
            for (k, v) in fields {
                map.insert(k.clone(), resolve_type(v, name_to_index)?);
            }
            Ok(SchemaEnum::Record(map))
        }
        AstSchema::Variant(variants) => {
            let mut map = BTreeMap::new();
            for (tag, payload) in variants {
                let resolved = resolve_payload(payload, name_to_index)?;
                map.insert(tag.clone(), resolved);
            }
            Ok(SchemaEnum::Variant(map))
        }
    }
}

fn resolve_payload(
    payload: &AstVariantPayload,
    name_to_index: &HashMap<String, usize>,
) -> Result<VariantPayloadSchema, SchemaParseError> {
    match payload {
        AstVariantPayload::Unnamed(types) => {
            let resolved: Result<Vec<_>, _> = types
                .iter()
                .map(|t| resolve_type(t, name_to_index))
                .collect();
            Ok(VariantPayloadSchema::Unnamed(resolved?))
        }
        AstVariantPayload::Named(fields) => {
            let mut map = BTreeMap::new();
            for (k, v) in fields {
                map.insert(k.clone(), resolve_type(v, name_to_index)?);
            }
            Ok(VariantPayloadSchema::Named(map))
        }
    }
}

pub fn resolve_document(doc: &AstDocument) -> Result<Schema, SchemaParseError> {
    if doc.declarations.is_empty() {
        return Err(SchemaParseError::EmptyDocument);
    }

    let n = doc.declarations.len();

    // Detect duplicates
    let mut seen = HashMap::new();
    for (name, _) in &doc.declarations {
        if seen.contains_key(name) {
            return Err(SchemaParseError::DuplicateTypeName(name.clone()));
        }
        seen.insert(name.clone(), 0);
    }

    // Determine root type name
    let root_name = doc.export.as_ref().unwrap_or(&doc.declarations[n - 1].0);

    // Validate export refers to a declared type
    if !seen.contains_key(root_name) {
        return Err(SchemaParseError::UnknownType(root_name.clone()));
    }

    // Build name→index: root gets 0, other types get 1..n in declaration order
    let mut name_to_index: HashMap<String, usize> = HashMap::new();
    name_to_index.insert(root_name.clone(), 0);
    let mut next_idx = 1usize;
    for (name, _) in &doc.declarations {
        if name != root_name && !name_to_index.contains_key(name) {
            name_to_index.insert(name.clone(), next_idx);
            next_idx += 1;
        }
    }

    // Build schemas: root at 0, then other types in declaration order
    let mut schemas: Vec<SchemaEnum> = Vec::with_capacity(n);

    // Root
    let root_decl = doc
        .declarations
        .iter()
        .find(|(n, _)| n == root_name)
        .unwrap();
    schemas.push(resolve_type(&root_decl.1, &name_to_index)?);

    // Dependencies in declaration order
    for (name, ast) in &doc.declarations {
        if name != root_name {
            schemas.push(resolve_type(ast, &name_to_index)?);
        }
    }

    Ok(Schema::from_schemas(schemas)
        .expect("Schema generated by type parser should be well-defined"))
}
