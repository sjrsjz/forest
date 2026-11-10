use std::{io, path::PathBuf, str::Utf8Error};

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("package I/O at {path}: {source}")]
    Io {
        path: PathBuf,
        #[source]
        source: io::Error,
    },
    #[error("invalid package TOML: {0}")]
    Toml(#[from] toml::de::Error),
    #[error("cannot encode package TOML: {0}")]
    Encode(#[from] toml::ser::Error),
    #[error("invalid package path: {0}")]
    InvalidPath(String),
    #[error("package path is not UTF-8: {0:?}")]
    NonUtf8Path(PathBuf),
    #[error("package entry is not a regular file or directory: {0}")]
    UnsupportedEntry(String),
    #[error("invalid SHA-256 for {path}: {value}")]
    InvalidChecksum { path: String, value: String },
    #[error("missing SHA-256 for {0}")]
    MissingChecksum(String),
    #[error("SHA-256 mismatch for {path}: expected {expected}, got {actual}")]
    ChecksumMismatch {
        path: String,
        expected: String,
        actual: String,
    },
    #[error("invalid UTF-8 in {path}: {source}")]
    Utf8 {
        path: String,
        #[source]
        source: Utf8Error,
    },
    #[error("invalid Schema in {path}: {source}")]
    Schema {
        path: String,
        #[source]
        source: forest_serde::parser::schema_parser::SchemaParseError,
    },
    #[error("invalid Value in {path}: {source}")]
    Value {
        path: String,
        #[source]
        source: forest_serde::parser::ParseError,
    },
    #[error("invalid ACL subject {subject}: {source}")]
    Subject {
        subject: String,
        #[source]
        source: forest_util::token::PathParseError,
    },
    #[error(transparent)]
    Manifest(#[from] forest_protocol::ManifestError),
}

pub(crate) fn io(path: impl Into<PathBuf>, source: io::Error) -> Error {
    Error::Io {
        path: path.into(),
        source,
    }
}
