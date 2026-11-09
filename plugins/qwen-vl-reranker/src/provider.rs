//! External JSON is qualified once; no storage, selection or graph policy here.
use crate::types::{Error, Input, MODEL};
use serde::{Deserialize, Serialize};

#[derive(Serialize)]
pub(crate) struct Request<'a> {
    model: &'static str,
    query: Content<'a>,
    documents: Vec<Content<'a>>,
    return_documents: bool,
}

#[derive(Serialize)]
#[serde(untagged)]
enum Content<'a> {
    Text { text: &'a str },
    Image { image: &'a str },
}
impl<'a> From<&'a Input> for Content<'a> {
    fn from(input: &'a Input) -> Self {
        match input {
            Input::Text(text) => Self::Text { text },
            Input::Image(image) => Self::Image { image },
        }
    }
}
impl<'a> Request<'a> {
    pub fn new(query: &'a Input, documents: &'a [Input]) -> Self {
        Self {
            model: MODEL,
            query: query.into(),
            documents: documents.iter().map(Content::from).collect(),
            return_documents: false,
        }
    }
}

#[derive(Deserialize)]
struct Response {
    results: Vec<Score>,
}
#[derive(Deserialize)]
struct Score {
    index: usize,
    relevance_score: f64,
}

pub(crate) fn decode(bytes: &[u8], expected: usize) -> Result<Vec<f64>, Error> {
    let mut response: Response =
        serde_json::from_slice(bytes).map_err(|e| Error::Response(e.to_string()))?;
    if response.results.len() != expected {
        return Err(Error::Response(
            "score count differs from document count".into(),
        ));
    }
    response.results.sort_unstable_by_key(|s| s.index);
    response
        .results
        .into_iter()
        .enumerate()
        .map(|(index, score)| {
            if score.index != index || !score.relevance_score.is_finite() {
                return Err(Error::Response("invalid document index or score".into()));
            }
            Ok(score.relevance_score)
        })
        .collect()
}
