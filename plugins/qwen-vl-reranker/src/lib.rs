//! Stateless, hot-reloadable Qwen VL reranking provider.
mod provider;
pub mod types;

use forest_protocol::{manifest::callers, service::HttpRequest, *};
use forest_sdk::{Handlers, Module, host};
use std::rc::Rc;
use types::*;

pub struct Reranker {
    connection: Connection,
    batch_size: usize,
}
#[derive(Clone, Debug, forest_serde::DeriveSchema, forest_serde::DeriveValue)]
pub enum LifecycleError {
    Host(forest_sdk::HostError),
    InvalidBatchSize(i64),
}
impl From<forest_sdk::HostError> for LifecycleError {
    fn from(error: forest_sdk::HostError) -> Self {
        Self::Host(error)
    }
}
impl Module for Reranker {
    type LifecycleError = LifecycleError;

    async fn create(context: CreateContext) -> Result<Self, Self::LifecycleError> {
        let connection: Connection = host::config_get(context.object, CONNECTION.parse().unwrap())
            .await
            .map_err(forest_sdk::HostError::Dispatch)?
            .map_err(forest_sdk::HostError::Service)?;
        let batch_size = usize::try_from(connection.batch_size)
            .ok()
            .filter(|size| *size > 0)
            .ok_or(LifecycleError::InvalidBatchSize(connection.batch_size))?;
        Ok(Self {
            connection,
            batch_size,
        })
    }
    fn handlers() -> Handlers<Self> {
        Handlers::new().with(
            RANK.parse().unwrap(),
            callers(vec![
                SubjectPattern::Path("/module/**".parse().unwrap()),
                SubjectPattern::Path("/user/admin".parse().unwrap()),
            ]),
            Self::rank,
        )
    }
}
impl Reranker {
    async fn rank(self: Rc<Self>, _: CallContext, request: Request) -> Result<Scores, Error> {
        if request.documents.is_empty() {
            return Ok(Scores { json: "[]".into() });
        }
        if self.connection.api_key.is_empty() {
            return Err(Error::NotConfigured);
        }
        let mut scores = Vec::with_capacity(request.documents.len());
        for documents in request.documents.chunks(self.batch_size) {
            let response = host::http_request(HttpRequest {
                timeout_ms: None,
                method: "POST".into(),
                url: self.connection.endpoint.clone(),
                headers: vec![
                    (
                        "Authorization".into(),
                        format!("Bearer {}", self.connection.api_key),
                    ),
                    ("Content-Type".into(), "application/json".into()),
                ],
                body: serde_json::to_vec(&provider::Request::new(&request.query, documents))
                    .expect("rerank inputs contain only strings and booleans"),
            })
            .await
            .map_err(|e| Error::Host(e.to_string()))?
            .map_err(|e| Error::Host(e.to_string()))?;
            if !(200..300).contains(&response.status) {
                return Err(Error::Http {
                    status: i32::from(response.status),
                    request_id: response
                        .headers
                        .iter()
                        .find(|(name, _)| name.eq_ignore_ascii_case("x-siliconcloud-trace-id"))
                        .map(|(_, value)| value.clone()),
                    body: String::from_utf8_lossy(&response.body).into_owned(),
                });
            }
            scores.extend(provider::decode(&response.body, documents.len())?);
        }
        Ok(Scores {
            json: serde_json::to_string(&scores).expect("validated finite scores"),
        })
    }
}
forest_sdk::export_module!(super::Reranker);

pub fn manifest() -> Manifest {
    Manifest {
        events: vec![],
        activated: None,
        package: PackageInfo {
            id: OBJECT.parse().unwrap(),
            version: "0.1.0".into(),
            name: Some("Qwen VL Reranker".into()),
            description: Some("Qwen3-VL-Reranker-8B text/image relevance scoring".into()),
            ..Default::default()
        },
        module: "module.wasm".into(),
        lifecycle_error: <Reranker as Module>::LifecycleError::as_schema(),
        configs: vec![ConfigDeclaration {
            key: CONNECTION.parse().unwrap(),
            schema: Connection::as_schema(),
            default: Connection::default().as_value(),
            access: AccessPolicy {
                readers: vec![SubjectPattern::SelfModule],
                writers: vec![SubjectPattern::Path("/user/admin".parse().unwrap())],
                callers: vec![],
            },
        }],
        provides: Reranker::handlers().declarations(),
        uses: vec![],
        subscriptions: vec![],
    }
}
