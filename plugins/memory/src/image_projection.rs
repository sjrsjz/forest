//! Package-owned Schema/Value contract; no implementation is linked.
use crate::managed;
use forest_serde::{DeriveSchema, DeriveValue};

pub const OBJECT: &str = "/module/image-projection";
pub const PREPARE: &str = "/prepare";
pub const THUMBNAIL: &str = "/thumbnail";

#[derive(Clone, Debug, DeriveSchema, DeriveValue)]
pub struct Thumbnail {
    pub reference: String,
    pub max_side: i32,
}

#[derive(Clone, Debug, DeriveSchema, DeriveValue)]
pub enum Error {
    Media(managed::Error),
    Image(String),
    Storage(String),
    Host(String),
}
