//! Independent image generation; only managed references leave this package.
mod api;
pub mod config;
pub mod contract;
pub mod media;
mod result;

use contract::{Error, Invocation};
use forest_protocol::{manifest::callers, *};
use forest_sdk::{Handlers, Module, host};
use serde::Deserialize;
use std::rc::Rc;

pub const OBJECT: &str = "/module/ai-paint";

#[derive(Clone, Copy, Default, Deserialize)]
#[serde(rename_all = "snake_case")]
enum Style {
    #[default]
    Anime,
    Photo,
    None,
}

#[derive(Clone, Copy, Default, Deserialize)]
#[serde(rename_all = "snake_case")]
enum Size {
    Portrait,
    Landscape,
    #[default]
    Square,
}
impl Size {
    fn value(self, sizes: &config::Sizes) -> &str {
        match self {
            Self::Portrait => &sizes.portrait,
            Self::Landscape => &sizes.landscape,
            Self::Square => &sizes.square,
        }
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Request {
    prompt: String,
    #[serde(default)]
    style: Style,
    #[serde(default)]
    size: Size,
    seed: Option<i64>,
}
impl Request {
    fn prompt(&self) -> String {
        match self.style {
            Style::Anime => format!("anime style, {}", self.prompt),
            Style::Photo => format!("photo-realistic, {}", self.prompt),
            Style::None => self.prompt.clone(),
        }
    }
}

pub struct Painter {
    client: api::Client,
}

async fn setting<T: AsValue>(object: &Token, key: &str) -> Result<T, forest_sdk::HostError> {
    Ok(host::config_get(object.clone(), key.parse().unwrap()).await??)
}

pub use api::ConfigurationError;
#[derive(Clone, Debug, forest_serde::DeriveSchema, forest_serde::DeriveValue)]
pub enum LifecycleError {
    Host(forest_sdk::HostError),
    Configuration(ConfigurationError),
}
impl From<forest_sdk::HostError> for LifecycleError {
    fn from(error: forest_sdk::HostError) -> Self {
        Self::Host(error)
    }
}

impl Module for Painter {
    type LifecycleError = LifecycleError;

    async fn create(context: CreateContext) -> Result<Self, Self::LifecycleError> {
        let backend: config::Backend = setting(&context.object, config::BACKEND).await?;
        let client = match backend {
            config::Backend::OpenAiImages => {
                api::Client::OpenAiImages(setting(&context.object, config::OPENAI_IMAGES).await?)
            }
            config::Backend::SiliconFlow => {
                api::Client::SiliconFlow(setting(&context.object, config::SILICONFLOW).await?)
            }
            config::Backend::SwarmUi => {
                api::Client::SwarmUi(setting(&context.object, config::SWARM_UI).await?)
            }
        };
        client.validate().map_err(LifecycleError::Configuration)?;
        Ok(Self { client })
    }
    fn handlers() -> Handlers<Self> {
        Handlers::new().with(
            "/generate".parse().unwrap(),
            true,
            callers(vec![
                SubjectPattern::Path("/module/agent-tools".parse().unwrap()),
                SubjectPattern::Path("/user/admin".parse().unwrap()),
            ]),
            Self::generate,
        )
    }
}

impl Painter {
    async fn generate(self: Rc<Self>, _: CallContext, input: Invocation) -> Result<String, Error> {
        let args: Request = serde_json::from_str(&input.arguments_json)
            .map_err(|e| Error::Arguments(e.to_string()))?;
        if args.prompt.trim().is_empty() || args.prompt.chars().count() > 8000 {
            return Err(Error::Arguments(
                "prompt must contain 1–8000 nonblank characters".into(),
            ));
        }
        let blob = self.client.generate(args).await?;
        let mime = blob.mime.clone();
        let reference: Result<String, media::Error> = host::call(
            media::OBJECT.parse().unwrap(),
            "/put".parse().unwrap(),
            &blob,
        )
        .await
        .map_err(|e| Error::Execution(e.to_string()))?;
        let reference = reference.map_err(|e| Error::Execution(format!("{e:?}")))?;
        Ok(serde_json::json!({"reference":reference,"mime":mime,"markdown":format!("![generated image]({reference})")}).to_string())
    }
}

forest_sdk::export_module!(super::Painter);

fn declaration<T: AsSchema + AsValue>(key: &str, value: T) -> ConfigDeclaration {
    ConfigDeclaration {
        key: key.parse().unwrap(),
        schema: T::as_schema(),
        default: value.as_value(),
        access: AccessPolicy {
            readers: vec![SubjectPattern::SelfModule],
            writers: vec![SubjectPattern::Path("/user/admin".parse().unwrap())],
            callers: vec![],
        },
    }
}
pub fn manifest() -> Manifest {
    Manifest {
        package: PackageInfo {
            id: OBJECT.parse().unwrap(),
            version: "0.1.0".into(),
            name: Some("AI painting".into()),
            description: Some(
                "SwarmUI, Nano-Banana-compatible and SiliconFlow image generation into managed media"
                    .into(),
            ),
            ..Default::default()
        },
        module: "module.wasm".into(),
        lifecycle_error: <Painter as Module>::LifecycleError::as_schema(),
        configs: vec![
            declaration(config::BACKEND, config::Backend::SiliconFlow),
            declaration(config::OPENAI_IMAGES, config::OpenAiImages::default()),
            declaration(config::SILICONFLOW, config::SiliconFlow::default()),
            declaration(config::SWARM_UI, config::SwarmUi::default()),
        ],
        provides: Painter::handlers().declarations(),
        uses: vec![InterfaceUse {
            object: media::OBJECT.parse().unwrap(),
            interface: "/put".parse().unwrap(),
            input: media::Blob::as_schema(),
            output: Result::<String,media::Error>::as_schema(),
        }],
        subscriptions: vec![],
    }
}
