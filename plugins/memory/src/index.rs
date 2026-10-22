use crate::{Memory, content::*, embedding, projection::Representation, store::*, types::*};
use forest_protocol::{
    CallContext,
    service::{HttpRequest, LogLevel},
};
use forest_sdk::host;
use serde::Deserialize;
use serde_json::{Value, json};
use std::{collections::BTreeMap, rc::Rc};

#[derive(Deserialize)]
struct Envelope<T> {
    result: T,
}
#[derive(Deserialize)]
struct Query {
    points: Vec<Hit>,
}
#[derive(Deserialize)]
struct Hit {
    score: f64,
    payload: Payload,
}
#[derive(Deserialize)]
struct Payload {
    group_id: i64,
    sequence: i64,
}

impl Memory {
    async fn qdrant(&self, method: &str, suffix: &str, body: Value) -> Result<Value, Error> {
        let mut headers = vec![("content-type".into(), "application/json".into())];
        if !self.settings.api_key.is_empty() {
            headers.push(("api-key".into(), self.settings.api_key.clone()));
        }
        let response = host::http_request(HttpRequest {
            method: method.into(),
            url: format!(
                "{}/collections/{}{}",
                self.settings.endpoint.trim_end_matches('/'),
                self.settings.collection,
                suffix
            ),
            headers,
            body: if method == "GET" {
                vec![]
            } else {
                serde_json::to_vec(&body).unwrap()
            },
        })
        .await??;
        if response.status == 404 && method == "GET" {
            return Ok(Value::Null);
        }
        if !(200..300).contains(&response.status) {
            return Err(Error::Index(format!("Qdrant HTTP {}", response.status)));
        }
        serde_json::from_slice(&response.body).map_err(|e| Error::Index(e.to_string()))
    }
    pub async fn ensure_index(&self) -> Result<(), Error> {
        let mut ready = self.index_ready.lock().await;
        if *ready {
            return Ok(());
        }
        let existing = self.qdrant("GET", "", Value::Null).await?;
        if existing.is_null() {
            self.qdrant(
                "PUT",
                "",
                json!({"vectors":{"size":embedding::DIMENSIONS,"distance":"Cosine"}}),
            )
            .await?;
        } else {
            let vectors = &existing["result"]["config"]["params"]["vectors"];
            if vectors["size"] != embedding::DIMENSIONS || vectors["distance"] != "Cosine" {
                return Err(Error::Index(
                    "collection differs from embedding space".into(),
                ));
            }
        }
        *ready = true;
        Ok(())
    }
    pub async fn embed(&self, representation: Representation) -> Result<Vec<f32>, Error> {
        let input = match representation {
            Representation::Text(v) => embedding::Input::Text(v),
            Representation::Image(r) => embedding::Input::Image(self.image(&r).await?),
        };
        let response: Result<embedding::Embeddings, embedding::Error> = host::call(
            embedding::OBJECT.parse().unwrap(),
            embedding::EMBED.parse().unwrap(),
            &vec![input],
        )
        .await?;
        let vectors = response
            .map_err(|e| Error::Embedding(format!("{e:?}")))?
            .vectors()
            .map_err(|e| Error::Embedding(e.to_string()))?;
        let mut vectors = vectors.into_iter();
        vectors
            .next()
            .ok_or_else(|| Error::Embedding("empty embedding response".into()))
    }
    pub async fn advance(self: Rc<Self>, _: CallContext, _: ()) -> Result<(), Error> {
        // The interface is serial. A durable pending item is settled only after processing.
        self.wake_pending.set(false);
        loop {
            let media = execute(vec![stmt(
                "SELECT reference FROM media WHERE state='pending' ORDER BY rowid LIMIT 1",
                vec![],
            )])
            .await?;
            let have_media = !media[0].rows.is_empty();
            if let Some(row) = media[0].rows.first() {
                let reference = string(&row[0])?;
                if let Err(error) = self.image(reference).await {
                    let _ = host::log(
                        LogLevel::Warn,
                        format!("media acquisition {reference}: {error:?}"),
                    )
                    .await;
                    // A storage failure must also settle this attempt instead of spinning.
                    execute(vec![stmt("UPDATE media SET state='failed',error=? WHERE reference=? AND state='pending'",vec![text(format!("{error:?}")),text(reference)])]).await?;
                }
            }
            let pending=execute(vec![stmt("SELECT point_id,group_id,sequence,representation FROM vectors WHERE space=? AND state='pending' ORDER BY rowid LIMIT 1",vec![text(&self.settings.collection)])]).await?;
            let Some(row) = pending[0].rows.first() else {
                if have_media {
                    continue;
                }
                return Ok(());
            };
            let point = string(&row[0])?.to_owned();
            let group = number(&row[1])?;
            let sequence = number(&row[2])?;
            let representation: Representation = decode(string(&row[3])?)?;
            let result=async {
                let vector=self.embed(representation).await?;
                self.ensure_index().await?;
                let output=self.qdrant("PUT","/points?wait=true",json!({"points":[{"id":point,"vector":vector,"payload":{"group_id":group,"sequence":sequence}}]})).await?;
                if output["result"]["status"]!="completed" {return Err(Error::Index("upsert was not applied".into()));}
                Ok::<_,Error>(())
            }.await;
            let (state, error) = match result {
                Ok(()) => ("published", forest_protocol::service::SqlValue::Null),
                Err(error) => {
                    let _ = host::log(
                        LogLevel::Warn,
                        format!("index {group}/{sequence}: {error:?}"),
                    )
                    .await;
                    ("failed", text(format!("{error:?}")))
                }
            };
            execute(vec![stmt(
                "UPDATE vectors SET state=?,error=? WHERE space=? AND point_id=?",
                vec![
                    text(state),
                    error,
                    text(&self.settings.collection),
                    text(point),
                ],
            )])
            .await?;
        }
    }
    pub async fn wake(&self) -> Result<(), Error> {
        if self.wake_pending.replace(true) {
            return Ok(());
        }
        if let Err(error) =
            host::send(self.object.clone(), "/index/advance".parse().unwrap(), &()).await
        {
            self.wake_pending.set(false);
            return Err(error.into());
        }
        Ok(())
    }
    pub async fn seeds(&self, query: &MemoryUnit) -> Result<Vec<(MemoryId, f64)>, Error> {
        let representations = crate::projection::representations(query)?;
        if representations.is_empty() {
            return Ok(vec![]);
        }
        self.ensure_index().await?;
        let mut candidates: BTreeMap<MemoryId, f64> = BTreeMap::new();
        for representation in representations {
            let vector = self.embed(representation).await?;
            let response=self.qdrant("POST","/points/query",json!({"query":vector,"limit":self.retrieval.seed_limit*4,"with_payload":true,"score_threshold":self.retrieval.seed_threshold})).await?;
            let response: Envelope<Query> =
                serde_json::from_value(response).map_err(|e| Error::Index(e.to_string()))?;
            for hit in response.result.points {
                if hit.score < self.retrieval.seed_threshold {
                    continue;
                }
                candidates
                    .entry(MemoryId {
                        group: hit.payload.group_id,
                        sequence: hit.payload.sequence,
                    })
                    .and_modify(|s| *s = s.max(hit.score))
                    .or_insert(hit.score);
            }
        }
        let ids = candidates.keys().cloned().collect::<Vec<_>>();
        let mut seeds = read(&ids, true)
            .await?
            .into_iter()
            .map(|u| {
                let score = candidates[&u.id];
                (u.id, score)
            })
            .collect::<Vec<_>>();
        seeds.sort_by(|a, b| b.1.total_cmp(&a.1).then(a.0.cmp(&b.0)));
        seeds.truncate(self.retrieval.seed_limit);
        Ok(seeds)
    }
}
