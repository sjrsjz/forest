//! Hot-reloadable multimodal embedding provider. No memory database is required.
mod provider;
pub mod types;

use crate::types::{CONNECTION, Connection, EMBED, Embeddings, Error, Input, OBJECT};
use forest_protocol::{manifest::callers, service::HttpRequest, *};
use forest_sdk::{Handlers, Module, host};
use std::rc::Rc;

pub struct QwenVl {
    connection: Connection,
}

impl Module for QwenVl {
    async fn create(context: CreateContext) -> Result<Self, String> {
        let connection = host::config_get(context.object, CONNECTION.parse().unwrap())
            .await
            .map_err(|error| error.to_string())?
            .map_err(|error| error.to_string())?;
        Ok(Self { connection })
    }

    fn handlers() -> Handlers<Self> {
        Handlers::new().with(
            EMBED.parse().unwrap(),
            true,
            callers(vec![
                SubjectPattern::Path("/module/**".parse().unwrap()),
                SubjectPattern::Path("/user/admin".parse().unwrap()),
            ]),
            Self::embed,
        )
    }
}

impl QwenVl {
    async fn embed(
        self: Rc<Self>,
        _: CallContext,
        inputs: Vec<Input>,
    ) -> Result<Embeddings, Error> {
        if inputs.is_empty() {
            return Ok(Embeddings::from_vectors(&[]));
        }
        if self.connection.api_key.is_empty() {
            return Err(Error::NotConfigured);
        }
        let response = host::http_request(HttpRequest {
            method: "POST".into(),
            url: self.connection.endpoint.clone(),
            headers: vec![
                (
                    "Authorization".into(),
                    format!("Bearer {}", self.connection.api_key),
                ),
                ("Content-Type".into(), "application/json".into()),
            ],
            body: serde_json::to_vec(&provider::Request::new(&inputs))
                .expect("embedding request contains only text and integer fields"),
        })
        .await
        .map_err(|error| Error::Host(error.to_string()))?
        .map_err(|error| Error::Host(error.to_string()))?;
        if !(200..300).contains(&response.status) {
            return Err(Error::Http {
                status: i32::from(response.status),
                request_id: response
                    .headers
                    .iter()
                    .find(|(name, _)| name.eq_ignore_ascii_case("x-siliconcloud-trace-id"))
                    .map(|(_, value)| value.clone()),
            });
        }
        provider::decode(&response.body, inputs.len())
    }
}

forest_sdk::export_module!(super::QwenVl);

pub fn manifest() -> Manifest {
    Manifest {
        package: PackageInfo {
            id: OBJECT.parse().unwrap(),
            version: "0.1.0".into(),
            name: Some("Qwen VL Embedding".into()),
            description: Some("Qwen3-VL-Embedding-8B, 2048 dimensions, L2 normalized".into()),
        },
        module: "module.wasm".into(),
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
        provides: QwenVl::handlers().declarations(),
        uses: vec![],
        subscriptions: vec![],
    }
}
