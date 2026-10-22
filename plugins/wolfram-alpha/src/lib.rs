//! Wolfram Alpha LLM API adapter, independent of the Agent and host implementation.
pub mod contract;

use contract::{Error, Invocation};
use forest_protocol::{manifest::callers, service::HttpRequest, *};
use forest_sdk::{Handlers, Module, host};
use forest_serde::{DeriveSchema, DeriveValue};
use serde::{Deserialize, Serialize};
use std::rc::Rc;

pub const OBJECT: &str = "/module/wolfram-alpha";
pub const CONNECTION: &str = "/connection";

#[derive(Clone, DeriveSchema, DeriveValue)]
pub struct Connection {
    pub endpoint: String,
    pub app_id: String,
}

impl Default for Connection {
    fn default() -> Self {
        Self {
            endpoint: "https://www.wolframalpha.com/api/v1/llm-api".into(),
            app_id: String::new(),
        }
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Query {
    query: String,
    #[serde(default = "default_max_chars")]
    max_chars: u32,
}

fn default_max_chars() -> u32 {
    4000
}

#[derive(Serialize)]
#[serde(tag = "status", rename_all = "snake_case")]
enum Answer<'a> {
    Complete { query: &'a str, result: &'a str },
    NotInterpreted { query: &'a str, suggestion: &'a str },
}

pub struct WolframAlpha {
    connection: Connection,
}

impl Module for WolframAlpha {
    async fn create(context: CreateContext) -> Result<Self, String> {
        let connection: Connection = host::config_get(context.object, CONNECTION.parse().unwrap())
            .await
            .map_err(|e| e.to_string())?
            .map_err(|e| e.to_string())?;
        let url = url::Url::parse(&connection.endpoint).map_err(|e| e.to_string())?;
        if !matches!(url.scheme(), "http" | "https")
            || !url.has_host()
            || !url.username().is_empty()
            || url.password().is_some()
            || url.query().is_some()
            || url.fragment().is_some()
        {
            return Err(
                "endpoint must be an HTTP(S) URL without credentials, query or fragment".into(),
            );
        }
        Ok(Self { connection })
    }

    fn handlers() -> Handlers<Self> {
        Handlers::new().with(
            "/query".parse().unwrap(),
            true,
            callers(vec![
                SubjectPattern::Path("/module/agent-tools".parse().unwrap()),
                SubjectPattern::Path("/user/admin".parse().unwrap()),
            ]),
            Self::query,
        )
    }
}

impl WolframAlpha {
    async fn query(self: Rc<Self>, _: CallContext, input: Invocation) -> Result<String, Error> {
        let args: Query = serde_json::from_str(&input.arguments_json)
            .map_err(|e| Error::Arguments(e.to_string()))?;
        let query = args.query.trim();
        if query.is_empty()
            || query.chars().count() > 2000
            || query.contains(['\n', '\r'])
            || !(500..=6000).contains(&args.max_chars)
        {
            return Err(Error::Arguments(
                "query requires 1–2000 nonblank characters on one line; max_chars is 500–6000"
                    .into(),
            ));
        }
        if self.connection.app_id.trim().is_empty() {
            return Err(Error::Execution("Wolfram Alpha is not configured".into()));
        }
        let mut url = url::Url::parse(&self.connection.endpoint).expect("validated endpoint");
        url.query_pairs_mut()
            .append_pair("input", query)
            .append_pair("maxchars", &args.max_chars.to_string());
        // The AppID stays in a header, never in a model-visible URL or result.
        let response = host::http_request(HttpRequest {
            method: "GET".into(),
            url: url.into(),
            headers: vec![(
                "Authorization".into(),
                format!("Bearer {}", self.connection.app_id),
            )],
            body: vec![],
        })
        .await
        .map_err(|e| Error::Execution(e.to_string()))?
        .map_err(|e| Error::Execution(e.to_string()))?;
        // 501 is a query interpretation result, with suggestions the model can use.
        if !(200..300).contains(&response.status) && response.status != 501 {
            return Err(Error::Execution(format!(
                "Wolfram Alpha HTTP {}",
                response.status
            )));
        }
        let text = String::from_utf8(response.body)
            .map_err(|e| Error::Execution(format!("Wolfram Alpha response is not UTF-8: {e}")))?;
        let text = text.trim();
        if text.is_empty() {
            return Err(Error::Execution(
                "Wolfram Alpha returned an empty response".into(),
            ));
        }
        let answer = if response.status == 501 {
            Answer::NotInterpreted {
                query,
                suggestion: text,
            }
        } else {
            Answer::Complete {
                query,
                result: text,
            }
        };
        Ok(serde_json::to_string(&answer).expect("serializable computation result"))
    }
}

forest_sdk::export_module!(super::WolframAlpha);

pub fn manifest() -> Manifest {
    Manifest {
        package: PackageInfo {
            id: OBJECT.parse().unwrap(),
            version: "0.1.0".into(),
            name: Some("Wolfram Alpha".into()),
            description: Some(
                "Exact computation and scientific knowledge via the Wolfram Alpha LLM API".into(),
            ),
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
        provides: WolframAlpha::handlers().declarations(),
        uses: vec![],
        subscriptions: vec![],
    }
}
