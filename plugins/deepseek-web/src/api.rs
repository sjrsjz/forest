//! DeepSeek's JSON boundary; no provider SDK objects enter module dispatch.
use crate::contract::Error;
use serde::{Deserialize, Serialize};

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Search {
    pub query: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub count: Option<u8>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ori_question: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub localization: Option<String>,
    #[serde(default)]
    pub need_content: bool,
}

impl Search {
    pub fn validate(&self) -> Result<(), Error> {
        let valid_query = |value: &str| !value.trim().is_empty() && value.chars().count() <= 128;
        if !valid_query(&self.query)
            || self
                .ori_question
                .as_deref()
                .is_some_and(|s| !valid_query(s))
            || self.count.is_some_and(|n| !(1..=50).contains(&n))
        {
            return Err(Error::Arguments(
                "query/ori_question require 1–128 nonblank Unicode characters; count is 1–50"
                    .into(),
            ));
        }
        Ok(())
    }
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Fetch {
    pub url: String,
}

impl Fetch {
    pub fn validate(&self) -> Result<(), Error> {
        let url = url::Url::parse(&self.url).map_err(|e| Error::Arguments(e.to_string()))?;
        if self.url.chars().count() > 8192
            || !matches!(url.scheme(), "http" | "https")
            || !url.has_host()
            || !url.username().is_empty()
            || url.password().is_some()
        {
            return Err(Error::Arguments(
                "url must be HTTP(S), at most 8192 characters, with a host and no credentials"
                    .into(),
            ));
        }
        Ok(())
    }
}

#[derive(Deserialize, Serialize)]
pub struct SearchResults {
    pub results: Vec<SearchResult>,
}

#[derive(Deserialize, Serialize)]
pub struct SearchResult {
    pub rerank_score: f64,
    pub title: String,
    pub url: String,
    pub snippet: String,
    pub summary: String,
    pub content: String,
    pub site_name: Option<String>,
    pub site_icon: Option<String>,
    pub date: Option<String>,
    pub authority: i32,
}

#[derive(Deserialize)]
pub struct Fetched {
    pub title: String,
    pub content: String,
}

#[derive(Serialize)]
pub struct Page {
    pub url: String,
    pub title: String,
    pub content: String,
}
