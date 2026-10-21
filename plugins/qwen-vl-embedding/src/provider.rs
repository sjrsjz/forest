//! Concrete SiliconFlow request/response mapping, with no host or storage access.
use crate::api::{DIMENSIONS, Embeddings, Error, Input, MODEL};
use serde::{Deserialize, Serialize};

#[derive(Serialize)]
pub(crate) struct Request<'a> {
    model: &'static str,
    dimensions: usize,
    encoding_format: &'static str,
    input: Vec<Content<'a>>,
}

#[derive(Serialize)]
#[serde(untagged)]
enum Content<'a> {
    Text { text: &'a str },
    Image { image: &'a str },
}

impl<'a> Request<'a> {
    pub fn new(inputs: &'a [Input]) -> Self {
        Self {
            model: MODEL,
            dimensions: DIMENSIONS,
            encoding_format: "float",
            input: inputs
                .iter()
                .map(|input| match input {
                    Input::Text(text) => Content::Text { text },
                    Input::Image(image) => Content::Image { image },
                })
                .collect(),
        }
    }
}

#[derive(Deserialize)]
struct Response {
    model: String,
    data: Vec<Vector>,
}

#[derive(Deserialize)]
struct Vector {
    index: usize,
    embedding: Vec<f32>,
}

/// Qualify the external result once, then publish normalized float32 vectors.
pub(crate) fn decode(bytes: &[u8], expected: usize) -> Result<Embeddings, Error> {
    let mut response: Response =
        serde_json::from_slice(bytes).map_err(|error| Error::Response(error.to_string()))?;
    if response.model != MODEL {
        return Err(Error::ModelMismatch(response.model));
    }
    if response.data.len() != expected {
        return Err(Error::Count {
            expected: expected as i64,
            actual: response.data.len() as i64,
        });
    }
    response.data.sort_unstable_by_key(|vector| vector.index);
    let vectors = response
        .data
        .into_iter()
        .enumerate()
        .map(|(index, mut vector)| {
            if vector.index != index {
                return Err(Error::Response("duplicate or missing input index".into()));
            }
            if vector.embedding.len() != DIMENSIONS {
                return Err(Error::Dimensions {
                    index: index as i64,
                    actual: vector.embedding.len() as i64,
                });
            }
            let squared_norm: f64 = vector
                .embedding
                .iter()
                .map(|value| f64::from(*value).powi(2))
                .sum();
            if !squared_norm.is_finite() || squared_norm <= 0.0 {
                return Err(Error::InvalidVector {
                    index: index as i64,
                });
            }
            let norm = squared_norm.sqrt();
            for value in &mut vector.embedding {
                *value = (f64::from(*value) / norm) as f32;
            }
            Ok(vector.embedding)
        })
        .collect::<Result<Vec<_>, _>>()?;
    Ok(Embeddings::from_vectors(&vectors))
}
