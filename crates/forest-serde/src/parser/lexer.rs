use base64::{Engine, engine::general_purpose::STANDARD as BASE64_STD};
use logos::{Lexer, Logos};

fn parse_base64(lex: &mut Lexer<ValueToken>) -> Option<Vec<u8>> {
    let slice = lex.slice();
    if slice.len() >= 3 {
        let b64_content = &slice[2..slice.len() - 1];
        BASE64_STD.decode(b64_content).ok()
    } else {
        None
    }
}

fn parse_hex(lex: &mut Lexer<ValueToken>) -> Option<Vec<u8>> {
    let slice = lex.slice();
    if slice.len() >= 2 {
        let hex_content = &slice[2..];
        hex::decode(hex_content).ok()
    } else {
        None
    }
}

fn extract_string(lex: &mut Lexer<ValueToken>) -> Option<String> {
    serde_json::from_str(lex.slice()).ok()
}

fn extract_string_schema(lex: &mut Lexer<SchemaToken>) -> Option<String> {
    serde_json::from_str(lex.slice()).ok()
}

/// Tokens for the value parser, including identifiers, literals, and comments.
#[derive(Logos, Debug, PartialEq, Clone)]
#[logos(skip r"[ \t\n\f\r]+")]
pub enum ValueToken {
    #[regex(r"//[^\n]*", logos::skip, allow_greedy = true)]
    LineComment,
    #[regex(r"/\*([^*]|\*[^/])*\*/", logos::skip, allow_greedy = true)]
    BlockComment,

    #[regex(r#"b"[A-Za-z0-9+/=]*""#, parse_base64, allow_greedy = true)]
    #[regex(r#"b0x[0-9a-fA-F]+"#, parse_hex)]
    Bytes(Vec<u8>),

    #[regex(r"[\p{L}_][\p{L}\p{N}_]*", |lex| lex.slice().to_string())]
    Ident(String),

    #[regex(r"-?[0-9]+(i32)?", |lex| {
        let s = lex.slice().trim_end_matches("i32");
        s.parse::<i32>().ok()
    })]
    Int32(i32),

    #[regex(r"-?[0-9]+i64", |lex| {
        let s = lex.slice().trim_end_matches("i64");
        s.parse::<i64>().ok()
    })]
    Int64(i64),

    #[regex(r#""([^"\\]|\\.)*""#, extract_string)]
    String(String),

    #[token("true")]
    True,
    #[token("false")]
    False,
    #[token("null")]
    Null,
    #[token("some")]
    Some,
    #[token("map")]
    Map,

    #[token("::")]
    DoubleColon,
    #[token(":")]
    Colon,
    #[token(",")]
    Comma,
    #[token("(")]
    LParen,
    #[token(")")]
    RParen,
    #[token("[")]
    LBracket,
    #[token("]")]
    RBracket,
    #[token("{")]
    LBrace,
    #[token("}")]
    RBrace,
}

#[derive(Logos, Debug, PartialEq, Clone)]
#[logos(skip r"[ \t\n\r\f]+")]
pub enum SchemaToken {
    #[regex(r"//[^\n]*", logos::skip, allow_greedy = true)]
    LineComment,
    #[regex(r"/\*([^*]|\*[^/])*\*/", logos::skip, allow_greedy = true)]
    BlockComment,

    #[token("export")]
    Export,
    #[token("schema")]
    SchemaDecl,

    #[token("i32")]
    I32,
    #[token("i64")]
    I64,
    #[token("bool")]
    Bool,
    #[token("string")]
    String,
    #[token("bytes")]
    Bytes,
    #[token("optional")]
    Optional,
    #[token("map")]
    Map,
    #[token("variant")]
    Variant,

    #[token("=")]
    Eq,
    #[token(";")]
    SemiColon,
    #[token(",")]
    Comma,
    #[token(":")]
    Colon,
    #[token("(")]
    LParen,
    #[token(")")]
    RParen,
    #[token("[")]
    LBracket,
    #[token("]")]
    RBracket,
    #[token("{")]
    LBrace,
    #[token("}")]
    RBrace,
    #[token("=>")]
    FatArrow,

    #[regex(r"[\p{L}_][\p{L}\p{N}_]*", |lex| lex.slice().to_string())]
    #[regex(r#""([^"\\]|\\.)*""#, extract_string_schema)]
    Ident(String),

    #[regex(r"[0-9]+", |lex| lex.slice().parse::<usize>().ok())]
    Size(usize),
}
