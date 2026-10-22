//! Independent, hot-reloadable search/fetch tools. Credentials belong to this module.
mod api;
pub mod contract;

use contract::{Error, Invocation};
use forest_protocol::{manifest::callers, service::HttpRequest, *};
use forest_sdk::{Handlers, Module, host};
use forest_serde::{DeriveSchema, DeriveValue};
use serde::{Serialize, de::DeserializeOwned};
use std::rc::Rc;

pub const OBJECT: &str = "/module/deepseek-web";
pub const CONNECTION: &str = "/connection";

#[derive(Clone, Debug, DeriveSchema, DeriveValue)]
pub struct Connection {
    /// API prefix, including /v1. Only the host administrator supplies this value.
    pub endpoint: String,
    pub api_key: String,
}

impl Default for Connection {
    fn default() -> Self {
        Self {
            endpoint: "https://api.deepseek.com/v1".into(),
            api_key: String::new(),
        }
    }
}

pub struct DeepSeekWeb {
    connection: Connection,
}

impl Module for DeepSeekWeb {
    async fn create(context: CreateContext) -> Result<Self, String> {
        let connection: Connection = host::config_get(context.object, CONNECTION.parse().unwrap())
            .await
            .map_err(|e| e.to_string())?
            .map_err(|e| e.to_string())?;
        let url = url::Url::parse(&connection.endpoint).map_err(|e| e.to_string())?;
        if !matches!(url.scheme(), "http" | "https")
            || !url.has_host()
            || url.query().is_some()
            || url.fragment().is_some()
        {
            return Err("endpoint must be an HTTP(S) API prefix without query/fragment".into());
        }
        Ok(Self { connection })
    }

    fn handlers() -> Handlers<Self> {
        let access = callers(vec![
            SubjectPattern::Path("/module/agent-tools".parse().unwrap()),
            SubjectPattern::Path("/user/admin".parse().unwrap()),
        ]);
        Handlers::new()
            .with(
                "/search".parse().unwrap(),
                true,
                access.clone(),
                Self::search,
            )
            .with("/fetch".parse().unwrap(), true, access, Self::fetch)
    }
}

impl DeepSeekWeb {
    async fn request<I: Serialize, O: DeserializeOwned>(
        &self,
        method: &str,
        input: &I,
    ) -> Result<O, Error> {
        if self.connection.api_key.is_empty() {
            return Err(Error::Execution("DeepSeek Web is not configured".into()));
        }
        let response = host::http_request(HttpRequest {
            method: "POST".into(),
            url: format!(
                "{}/tools/{method}",
                self.connection.endpoint.trim_end_matches('/')
            ),
            headers: vec![
                (
                    "Authorization".into(),
                    format!("Bearer {}", self.connection.api_key),
                ),
                ("Content-Type".into(), "application/json".into()),
            ],
            body: serde_json::to_vec(input).expect("serializable API request"),
        })
        .await
        .map_err(|e| Error::Execution(e.to_string()))?
        .map_err(|e| Error::Execution(e.to_string()))?;
        if !(200..300).contains(&response.status) {
            let request_id = response
                .headers
                .iter()
                .find(|(name, _)| name.eq_ignore_ascii_case("x-request-id"))
                .map(|(_, value)| value.as_str());
            return Err(Error::Execution(format!(
                "DeepSeek {method} HTTP {}, request_id={request_id:?}",
                response.status
            )));
        }
        serde_json::from_slice(&response.body)
            .map_err(|e| Error::Execution(format!("DeepSeek {method} response: {e}")))
    }

    async fn search(self: Rc<Self>, _: CallContext, input: Invocation) -> Result<String, Error> {
        let args: api::Search = serde_json::from_str(&input.arguments_json)
            .map_err(|e| Error::Arguments(e.to_string()))?;
        args.validate()?;
        let results: api::SearchResults = self.request("search", &args).await?;
        Ok(serde_json::to_string(&results).expect("serializable search results"))
    }

    async fn fetch(self: Rc<Self>, _: CallContext, input: Invocation) -> Result<String, Error> {
        let args: api::Fetch = serde_json::from_str(&input.arguments_json)
            .map_err(|e| Error::Arguments(e.to_string()))?;
        args.validate()?;
        let page: api::Fetched = self.request("fetch", &args).await?;
        Ok(serde_json::to_string(&api::Page {
            url: args.url,
            title: page.title,
            content: page.content,
        })
        .expect("serializable fetched page"))
    }
}

forest_sdk::export_module!(super::DeepSeekWeb);

pub fn manifest() -> Manifest {
    Manifest {
        package: PackageInfo {
            id: OBJECT.parse().unwrap(),
            version: "0.1.0".into(),
            name: Some("DeepSeek Web".into()),
            description: Some("Web search summaries and fetched Markdown via DeepSeek APIs".into()),
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
        provides: DeepSeekWeb::handlers().declarations(),
        uses: vec![],
        subscriptions: vec![],
    }
}
