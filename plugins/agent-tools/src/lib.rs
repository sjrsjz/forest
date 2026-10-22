//! Tool names select explicitly declared module routes. No arbitrary host reflection.
pub mod content;
pub mod contract;
pub mod memory;
pub mod openai;
pub mod tools;
use contract::{Catalog, Error, Invocation, OBJECT};
use forest_protocol::{manifest::callers, *};
use forest_sdk::{Handlers, Module, host};
use serde::{Deserialize, Serialize};
use std::{collections::BTreeMap, rc::Rc};

#[derive(
    Clone, Debug, Serialize, Deserialize, forest_serde::DeriveSchema, forest_serde::DeriveValue,
)]
pub struct Route {
    pub tool: tools::Tool,
    pub object: String,
    pub interface: String,
}
pub struct Gateway {
    routes: BTreeMap<String, ResolvedRoute>,
}
struct ResolvedRoute {
    tool: tools::Tool,
    object: Token,
    interface: Token,
}
impl Module for Gateway {
    async fn create(context: CreateContext) -> Result<Self, String> {
        let routes: Vec<Route> = host::config_get(context.object, "/routes".parse().unwrap())
            .await
            .map_err(|e| e.to_string())?
            .map_err(|e| e.to_string())?;
        let mut table = BTreeMap::new();
        for route in routes {
            let _: serde_json::Value =
                serde_json::from_str(&route.tool.parameters_json).map_err(|e| e.to_string())?;
            let route = ResolvedRoute {
                tool: route.tool,
                object: route.object.parse().map_err(|e| format!("{e}"))?,
                interface: route.interface.parse().map_err(|e| format!("{e}"))?,
            };
            if table.insert(route.tool.name.clone(), route).is_some() {
                return Err("duplicate tool name".into());
            }
        }
        Ok(Self { routes: table })
    }
    fn handlers() -> Handlers<Self> {
        let agent = callers(vec![
            SubjectPattern::Path("/module/agent".parse().unwrap()),
            SubjectPattern::Path("/user/admin".parse().unwrap()),
        ]);
        let internal = callers(vec![SubjectPattern::SelfModule]);
        Handlers::new()
            .with(
                "/describe".parse().unwrap(),
                true,
                agent.clone(),
                Self::describe,
            )
            .with("/invoke".parse().unwrap(), true, agent, Self::invoke)
            .with("/http".parse().unwrap(), true, internal.clone(), Self::http)
            .with("/memory".parse().unwrap(), true, internal, Self::memory)
    }
}
impl Gateway {
    async fn describe(self: Rc<Self>, _: CallContext, _: ()) -> Result<Catalog, Error> {
        Ok(self.routes.values().map(|r| r.tool.clone()).collect())
    }
    async fn invoke(self: Rc<Self>, _: CallContext, input: Invocation) -> Result<String, Error> {
        let route = self
            .routes
            .get(&input.name)
            .ok_or_else(|| Error::UnknownTool(input.name.clone()))?;
        let value: serde_json::Value = serde_json::from_str(&input.arguments_json)
            .map_err(|e| Error::Arguments(e.to_string()))?;
        if !value.is_object() {
            return Err(Error::Arguments(
                "tool arguments must be a JSON object".into(),
            ));
        }
        let output: Result<String, Error> =
            host::call(route.object.clone(), route.interface.clone(), &input)
                .await
                .map_err(|e| Error::Execution(e.to_string()))?;
        output
    }
    async fn http(self: Rc<Self>, _: CallContext, input: Invocation) -> Result<String, Error> {
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Request {
            url: String,
            #[serde(default = "get")]
            method: String,
            #[serde(default)]
            headers: BTreeMap<String, String>,
            #[serde(default)]
            body: String,
        }
        fn get() -> String {
            "GET".into()
        }
        let args: Request = serde_json::from_str(&input.arguments_json)
            .map_err(|e| Error::Arguments(e.to_string()))?;
        if !(args.url.starts_with("https://") || args.url.starts_with("http://")) {
            return Err(Error::Arguments("HTTP(S) URL required".into()));
        }
        let response = host::http_request(forest_protocol::service::HttpRequest {
            method: args.method,
            url: args.url,
            headers: args.headers.into_iter().collect(),
            body: args.body.into_bytes(),
        })
        .await
        .map_err(|e| Error::Execution(e.to_string()))?
        .map_err(|e| Error::Execution(e.to_string()))?;
        let body = String::from_utf8(response.body)
            .map_err(|_| Error::Execution("HTTP response is not UTF-8 text".into()))?;
        Ok(
            serde_json::json!({"status":response.status,"headers":response.headers,"body":body})
                .to_string(),
        )
    }
    async fn memory(self: Rc<Self>, _: CallContext, input: Invocation) -> Result<String, Error> {
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Request {
            ids: Vec<content::MemoryId>,
        }
        let args: Request = serde_json::from_str(&input.arguments_json)
            .map_err(|e| Error::Arguments(e.to_string()))?;
        if args.ids.len() > 16 {
            return Err(Error::Arguments("at most 16 memory units per call".into()));
        }
        let units: Result<Vec<content::MemoryUnit>, memory::Error> = host::call(
            memory::OBJECT.parse().unwrap(),
            "/read".parse().unwrap(),
            &args.ids,
        )
        .await
        .map_err(|e| Error::Execution(e.to_string()))?;
        let ids = units
            .map_err(|e| Error::Execution(format!("{e:?}")))?
            .into_iter()
            .map(|u| u.id)
            .collect();
        let projection: Result<memory::Projection, memory::Error> = host::call(
            memory::OBJECT.parse().unwrap(),
            "/project".parse().unwrap(),
            &memory::Project { ids, images: false },
        )
        .await
        .map_err(|e| Error::Execution(e.to_string()))?;
        let mut units = projection
            .map_err(|e| Error::Execution(format!("{e:?}")))?
            .units;
        for unit in &mut units {
            if let content::MemoryContent::ModelTurn(turn) = &mut unit.content {
                turn.assistant.reasoning_content = None;
            }
        }
        serde_json::to_string(&units).map_err(|e| Error::Execution(e.to_string()))
    }
}
forest_sdk::export_module!(super::Gateway);
fn route(
    name: &str,
    description: &str,
    parameters: serde_json::Value,
    object: &str,
    interface: &str,
) -> Route {
    Route {
        tool: tools::Tool {
            name: name.into(),
            description: Some(description.into()),
            parameters_json: parameters.to_string(),
            strict: None,
        },
        object: object.into(),
        interface: interface.into(),
    }
}

pub fn default_routes() -> Vec<Route> {
    vec![
        route(
            "web_search",
            "Default tool for web research and finding sources. Search with DeepSeek and return titles, URLs and query-relevant summaries. Search first, then use web_fetch to read selected pages. need_content defaults to false; request full content only when needed.",
            serde_json::json!({
                "type":"object",
                "properties":{
                    "query":{"type":"string","minLength":1,"maxLength":128,"description":"Search query; supports site:, -site:, quoted phrases and date filters."},
                    "count":{"type":"integer","minimum":1,"maximum":50,"default":10},
                    "ori_question":{"type":"string","minLength":1,"maxLength":128,"description":"Optional original user question."},
                    "localization":{"type":"string","description":"Optional IP address for localized search ranking."},
                    "need_content":{"type":"boolean","default":false}
                },
                "required":["query"],"additionalProperties":false
            }),
            "/module/deepseek-web",
            "/search",
        ),
        route(
            "web_fetch",
            "Default tool for reading a webpage URL. Fetch through DeepSeek and return its title, source URL and extracted Markdown content. Use this for webpage reading instead of requesting raw HTML with http_request.",
            serde_json::json!({
                "type":"object",
                "properties":{"url":{"type":"string","minLength":1,"maxLength":8192,"format":"uri","description":"HTTP(S) URL with a host and no embedded credentials."}},
                "required":["url"],"additionalProperties":false
            }),
            "/module/deepseek-web",
            "/fetch",
        ),
        route(
            "http_request",
            "Low-level HTTP(S) for explicit API operations and raw HTTP diagnostics. Returns status, headers and the complete UTF-8 body. For web research use web_search; for webpage content use web_fetch. No automatic retries.",
            serde_json::json!({
                "type":"object",
                "properties":{"url":{"type":"string"},"method":{"type":"string"},"headers":{"type":"object","additionalProperties":{"type":"string"}},"body":{"type":"string"}},
                "required":["url"],"additionalProperties":false
            }),
            OBJECT,
            "/http",
        ),
        route(
            "wolfram_alpha",
            "Exact mathematics, scientific computation, units, dates and computable facts via Wolfram Alpha. Translate the query to concise single-line English. Use 6*10^14 instead of 6e14 for scientific notation. If status is not_interpreted, use the returned suggestions to reformulate the query. Image URLs in results are source links, not managed media references.",
            serde_json::json!({
                "type":"object",
                "properties":{
                    "query":{"type":"string","minLength":1,"maxLength":2000,"description":"A concise single-line query in English, e.g. integrate x^2 sin(x) from 0 to pi."},
                    "max_chars":{"type":"integer","minimum":500,"maximum":6000,"default":4000,"description":"Result character limit requested from Wolfram Alpha."}
                },
                "required":["query"],"additionalProperties":false
            }),
            "/module/wolfram-alpha",
            "/query",
        ),
        route(
            "typst_render",
            "Render a Typst document, equations or tables into PNG pages. Use Typst syntax, not Markdown or LaTeX. Defaults: 480pt width, automatic height, 12pt Chinese text, 2 pixels/pt; source can override page/text settings. Chinese and mathematical fonts are included. Returns page dimensions, managed media references and ready-to-send CQ image codes; include the returned cq in your assistant reply to send the images. No image bytes appear in the tool result. Optional images maps relative virtual file names to existing media: references for image(\"name.png\"). File system, package downloads and current date are unavailable. Compilation diagnostics are returned for you to correct the source.",
            serde_json::json!({
                "type":"object",
                "properties":{
                    "source":{"type":"string","minLength":1,"maxLength":100000,"description":"Typst source, for example: = 积分结果\n$ integral_0^pi x^2 sin(x) dif x = pi^2 - 4 $"},
                    "images":{"type":"object","additionalProperties":{"type":"string"},"description":"Virtual relative image file names mapped to managed media references."}
                },
                "required":["source"],"additionalProperties":false
            }),
            "/module/typst",
            "/render",
        ),
        route(
            "memory_read",
            "Read up to 16 archived memory units by original group and sequence. Original source is preserved.",
            serde_json::json!({
                "type":"object",
                "properties":{"ids":{"type":"array","maxItems":16,"items":{"type":"object","properties":{"group":{"type":"integer"},"sequence":{"type":"integer"}},"required":["group","sequence"],"additionalProperties":false}}},
                "required":["ids"],"additionalProperties":false
            }),
            OBJECT,
            "/memory",
        ),
    ]
}

pub fn manifest() -> Manifest {
    manifest_with_routes(default_routes())
}
/// Packaging an external route records its exact uses contract and ACL is still checked.
pub fn manifest_with_routes(routes: Vec<Route>) -> Manifest {
    let mut uses = routes
        .iter()
        .map(|r| InterfaceUse {
            object: r.object.parse().expect("valid route object"),
            interface: r.interface.parse().expect("valid route interface"),
            input: Invocation::as_schema(),
            output: Result::<String, Error>::as_schema(),
        })
        .collect::<Vec<_>>();
    uses.sort_by(|a, b| (&a.object, &a.interface).cmp(&(&b.object, &b.interface)));
    uses.dedup_by(|a, b| a.object == b.object && a.interface == b.interface);
    uses.extend([
        InterfaceUse {
            object: memory::OBJECT.parse().unwrap(),
            interface: "/read".parse().unwrap(),
            input: Vec::<content::MemoryId>::as_schema(),
            output: Result::<Vec<content::MemoryUnit>, memory::Error>::as_schema(),
        },
        InterfaceUse {
            object: memory::OBJECT.parse().unwrap(),
            interface: "/project".parse().unwrap(),
            input: memory::Project::as_schema(),
            output: Result::<memory::Projection, memory::Error>::as_schema(),
        },
    ]);
    Manifest {
        package: PackageInfo {
            id: OBJECT.parse().unwrap(),
            version: "0.1.0".into(),
            name: Some("Agent tool gateway".into()),
            description: Some("Native tool calls to explicit module interfaces".into()),
        },
        module: "module.wasm".into(),
        configs: vec![ConfigDeclaration {
            key: "/routes".parse().unwrap(),
            schema: Vec::<Route>::as_schema(),
            default: routes.as_value(),
            access: AccessPolicy {
                readers: vec![SubjectPattern::SelfModule],
                writers: vec![SubjectPattern::Path("/user/admin".parse().unwrap())],
                callers: vec![],
            },
        }],
        provides: Gateway::handlers().declarations(),
        uses,
        subscriptions: vec![],
    }
}
