use lalrpop_util::lalrpop_mod;
use std::fmt;

use crate::{
    adt::{Schema, Value, VariantPayloadValue},
    parser::lexer::ValueToken,
};
use lalrpop_util::ParseError as LalrpopError;
use logos::Logos;

lalrpop_mod!(value_parser, "/parser/value_grammar.rs");
lalrpop_mod!(schema_syntax, "/parser/schema_grammar.rs");

pub mod lexer;
pub mod schema_parser;
#[cfg(test)]
mod tests;

use lexer::SchemaToken;
use schema_parser::{SchemaParseError, resolve_document};

fn build_payload(val: Value) -> VariantPayloadValue {
    match val {
        Value::Tuple(items) => VariantPayloadValue::Unnamed(items),
        Value::Record(map) => VariantPayloadValue::Named(map),
        other => VariantPayloadValue::Unnamed(vec![other]),
    }
}

// ============================================================================
// Value Parse Error
// ============================================================================

#[derive(Debug, Clone, PartialEq)]
pub enum ParseError {
    LexicalError {
        location: usize,
    },

    UnexpectedToken {
        start: usize,
        end: usize,
        found: ValueToken,
        expected: Vec<String>,
    },

    UnexpectedEof {
        location: usize,
        expected: Vec<String>,
    },

    ExtraToken {
        start: usize,
        end: usize,
        found: ValueToken,
    },
}

impl fmt::Display for ParseError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ParseError::LexicalError { location } => {
                write!(
                    f,
                    "Lexical Error: Unrecognized or invalid characters at offset {}",
                    location
                )
            }
            ParseError::UnexpectedToken {
                start,
                end,
                found,
                expected,
            } => {
                write!(
                    f,
                    "Syntax Error: Unexpected token '{:?}' at {}-{}. Expected one of: {}",
                    found,
                    start,
                    end,
                    expected.join(", ")
                )
            }
            ParseError::UnexpectedEof { location, expected } => {
                write!(
                    f,
                    "Syntax Error: Unexpected end of input at offset {}. Expected one of: {}",
                    location,
                    expected.join(", ")
                )
            }
            ParseError::ExtraToken { start, end, found } => {
                write!(
                    f,
                    "Syntax Error: Extra unparsed token '{:?}' at {}-{}.",
                    found, start, end
                )
            }
        }
    }
}

impl std::error::Error for ParseError {}

impl From<LalrpopError<usize, ValueToken, usize>> for ParseError {
    fn from(err: LalrpopError<usize, ValueToken, usize>) -> Self {
        match err {
            LalrpopError::InvalidToken { location } => ParseError::LexicalError { location },
            LalrpopError::UnrecognizedEof { location, expected } => {
                ParseError::UnexpectedEof { location, expected }
            }
            LalrpopError::UnrecognizedToken {
                token: (start, found, end),
                expected,
            } => ParseError::UnexpectedToken {
                start,
                end,
                found,
                expected,
            },
            LalrpopError::ExtraToken {
                token: (start, found, end),
            } => ParseError::ExtraToken { start, end, found },
            LalrpopError::User { error } => ParseError::LexicalError { location: error },
        }
    }
}

// ============================================================================
// Value Parser
// ============================================================================

pub fn parse_value(input: &str) -> Result<Value, ParseError> {
    let lexer = ValueToken::lexer(input);

    let spanned_iter = lexer.spanned().map(|(token_res, span)| match token_res {
        Ok(token) => Ok((span.start, token, span.end)),
        Err(_) => Err(span.start),
    });

    let parser = value_parser::ExprParser::new();
    parser.parse(spanned_iter).map_err(|e| e.into())
}

// ============================================================================
// Schema Parser
// ============================================================================

/// Parse type declarations into a `Schema`, supporting forward references,
/// recursive types, and mutually recursive types via `Ref` indices.
///
/// The last declared type is the root (index 0). Use `export Name;` to
/// explicitly designate a different root.
///
/// Syntax:
/// ```text
/// schema Point = { x: i32, y: i64 };
/// schema MaybePoint = variant { Some(Point), None };
/// schema List = variant { Cons(i32, List), Nil };
/// export List;
/// ```
pub fn parse_schema(input: &str) -> Result<Schema, SchemaParseError> {
    let lexer = SchemaToken::lexer(input);

    let spanned_iter = lexer.spanned().map(|(token_res, span)| match token_res {
        Ok(token) => Ok((span.start, token, span.end)),
        Err(_) => Err(span.start),
    });

    let parser = schema_syntax::DocumentParser::new();
    let doc = parser
        .parse(spanned_iter)
        .map_err(schema_parser::convert_lalrpop_error)?;

    resolve_document(&doc)
}
