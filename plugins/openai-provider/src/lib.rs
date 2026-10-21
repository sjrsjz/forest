//! OpenAI-compatible provider, loaded and hot-reloaded as an ordinary Wasm package.
mod provider;
pub mod types;

use crate::types::{
    CAPABILITIES, COMPLETE, Capabilities, Completion, Error, OBJECT, Request, SETTINGS, Settings,
};
use forest_protocol::{manifest::callers, service::HttpRequest, *};
use forest_sdk::{Handlers, Module, host};
use std::rc::Rc;

pub struct OpenAi {
    settings: Settings,
}

impl Module for OpenAi {
    async fn create(context: CreateContext) -> Result<Self, String> {
        let settings = host::config_get(context.object, SETTINGS.parse().unwrap())
            .await
            .map_err(|error| error.to_string())?
            .map_err(|error| error.to_string())?;
        Ok(Self { settings })
    }

    fn handlers() -> Handlers<Self> {
        let access = callers(vec![
            SubjectPattern::Path("/module/**".parse().unwrap()),
            SubjectPattern::Path("/user/admin".parse().unwrap()),
        ]);
        Handlers::new()
            .with(
                COMPLETE.parse().unwrap(),
                true,
                access.clone(),
                Self::complete,
            )
            .with(
                CAPABILITIES.parse().unwrap(),
                true,
                access,
                Self::capabilities,
            )
    }
}

impl OpenAi {
    async fn capabilities(self: Rc<Self>, _: CallContext, _: ()) -> Result<Capabilities, Error> {
        Ok(Capabilities {
            model: self.settings.model.clone(),
            multimodal: self.settings.multimodal,
        })
    }

    async fn complete(
        self: Rc<Self>,
        _: CallContext,
        request: Request,
    ) -> Result<Completion, Error> {
        if self.settings.api_key.is_empty() || self.settings.model.is_empty() {
            return Err(Error::NotConfigured);
        }
        let body = provider::encode(&self.settings, &request)?;
        let response = host::http_request(HttpRequest {
            method: "POST".into(),
            url: self.settings.endpoint.clone(),
            headers: vec![
                (
                    "Authorization".into(),
                    format!("Bearer {}", self.settings.api_key),
                ),
                ("Content-Type".into(), "application/json".into()),
            ],
            body,
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
                    .find(|(name, _)| name.eq_ignore_ascii_case("x-request-id"))
                    .map(|(_, value)| value.clone()),
            });
        }
        provider::decode(&response.body)
    }
}

forest_sdk::export_module!(super::OpenAi);

pub fn manifest() -> Manifest {
    Manifest {
        package: PackageInfo {
            id: OBJECT.parse().unwrap(),
            version: "0.1.0".into(),
            name: Some("OpenAI Chat Completions".into()),
            description: Some(
                "Typed messages and native function tools; configured model and image capability"
                    .into(),
            ),
        },
        module: "module.wasm".into(),
        configs: vec![ConfigDeclaration {
            key: SETTINGS.parse().unwrap(),
            schema: Settings::as_schema(),
            default: Settings::default().as_value(),
            access: AccessPolicy {
                readers: vec![SubjectPattern::SelfModule],
                writers: vec![SubjectPattern::Path("/user/admin".parse().unwrap())],
                callers: vec![],
            },
        }],
        provides: OpenAi::handlers().declarations(),
        uses: vec![],
        subscriptions: vec![],
    }
}
