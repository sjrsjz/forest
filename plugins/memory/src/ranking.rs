use crate::{Memory, content::*, image_projection, managed, projection, reranker, types::Error};
use base64::{Engine, engine::general_purpose::STANDARD};
use forest_protocol::service::LogLevel;
use forest_sdk::host;
use std::collections::BTreeMap;

async fn input(
    view: projection::Representation,
    max_side: i32,
) -> Result<Option<reranker::Input>, Error> {
    match view {
        projection::Representation::Text(text) => Ok(Some(reranker::Input::Text(text))),
        projection::Representation::Image(reference) => {
            let thumbnail: Result<managed::Blob, image_projection::Error> = host::call(
                image_projection::OBJECT.parse().unwrap(),
                image_projection::THUMBNAIL.parse().unwrap(),
                &image_projection::Thumbnail {
                    reference: reference.clone(),
                    max_side,
                },
            )
            .await?;
            match thumbnail {
                Ok(blob) => Ok(Some(reranker::Input::Image(format!(
                    "data:{};base64,{}",
                    blob.mime,
                    STANDARD.encode(blob.bytes)
                )))),
                Err(error) => {
                    let _ = host::log(
                        LogLevel::Warn,
                        format!("rerank image unavailable: {reference}: {error:?}"),
                    )
                    .await;
                    Ok(None)
                }
            }
        }
    }
}

impl Memory {
    pub async fn rank_seeds(
        &self,
        query: &MemoryUnit,
        candidates: &[MemoryUnit],
    ) -> Result<Vec<(MemoryId, f64)>, Error> {
        let queries = projection::ranking_representations(query).await?;
        // Keep references/spans here, not all image bytes from the candidate set.
        let mut views = Vec::new();
        for unit in candidates {
            for view in projection::ranking_representations(unit).await? {
                views.push((unit.id.clone(), view));
            }
        }
        if views.is_empty() {
            return Ok(Vec::new());
        }
        let mut scores: BTreeMap<MemoryId, f64> = BTreeMap::new();
        let mut calls = 0;
        for query in queries {
            let Some(query) = input(query, self.retrieval.rerank_image_max_side).await? else {
                continue;
            };
            for batch in views.chunks(self.retrieval.rerank_batch_size) {
                let mut documents = Vec::new();
                let mut owners = Vec::new();
                for (id, view) in batch {
                    if let Some(document) =
                        input(view.clone(), self.retrieval.rerank_image_max_side).await?
                    {
                        documents.push(document);
                        owners.push(id.clone());
                    }
                }
                if documents.is_empty() {
                    continue;
                }
                let response: Result<reranker::Scores, reranker::Error> = host::call(
                    reranker::OBJECT.parse().unwrap(),
                    reranker::RANK.parse().unwrap(),
                    &reranker::Request {
                        query: query.clone(),
                        documents,
                    },
                )
                .await?;
                calls += 1;
                let response = response.map_err(|e| Error::Index(format!("rerank: {e:?}")))?;
                let values: Vec<f64> = serde_json::from_str(&response.json)
                    .map_err(|e| Error::Index(format!("rerank scores: {e}")))?;
                if values.len() != owners.len() {
                    return Err(Error::Index(
                        "rerank score count differs from representations".into(),
                    ));
                }
                for (id, score) in owners.into_iter().zip(values) {
                    scores
                        .entry(id)
                        .and_modify(|current| *current = current.max(score))
                        .or_insert(score);
                }
            }
        }
        let mut seeds = scores
            .into_iter()
            .filter(|(_, score)| *score >= self.retrieval.rerank_threshold)
            .collect::<Vec<_>>();
        seeds.sort_by(|a, b| b.1.total_cmp(&a.1).then(a.0.cmp(&b.0)));
        seeds.truncate(self.retrieval.seed_limit);
        let _ = host::log(
            LogLevel::Info,
            format!(
                "recall rerank: {} candidates, {} representations, {} batches, {} seeds",
                candidates.len(),
                views.len(),
                calls,
                seeds.len()
            ),
        )
        .await;
        Ok(seeds)
    }
}
