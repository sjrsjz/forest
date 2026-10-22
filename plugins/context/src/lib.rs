//! Hot-reloadable context formatter; the only instance state is its prompt.
mod format;
pub mod memory;
pub mod openai;
pub mod types;

use crate::{
    openai::Message,
    types::{AGENT_PROMPT, Error, FORMAT, FormatInput, OBJECT},
};
use forest_protocol::{manifest::callers, *};
use forest_sdk::{Handlers, Module, host};
use std::rc::Rc;

const DEFAULT_PROMPT: &str = "Be helpful, natural, and attentive to the participants in the conversation. Clearly distinguish established information, historical evidence, and uncertainty.";

pub struct Context {
    prompt: String,
}

impl Module for Context {
    async fn create(context: CreateContext) -> Result<Self, String> {
        let prompt = host::config_get(context.object, AGENT_PROMPT.parse().unwrap())
            .await
            .map_err(|error| error.to_string())?
            .map_err(|error| error.to_string())?;
        Ok(Self { prompt })
    }

    fn handlers() -> Handlers<Self> {
        Handlers::new().with(
            FORMAT.parse().unwrap(),
            true,
            callers(vec![
                SubjectPattern::Path("/module/**".parse().unwrap()),
                SubjectPattern::Path("/user/admin".parse().unwrap()),
            ]),
            Self::format,
        )
    }
}

impl Context {
    async fn format(
        self: Rc<Self>,
        _: CallContext,
        input: FormatInput,
    ) -> Result<Vec<Message>, Error> {
        format::format(&self.prompt, input)
    }
}

forest_sdk::export_module!(super::Context);

pub fn manifest() -> Manifest {
    Manifest {
        package: PackageInfo {
            id: OBJECT.parse().unwrap(),
            version: "0.1.0".into(),
            name: Some("Context formatter".into()),
            description: Some(
                "Ordered content to OpenAI messages with a configurable Agent prompt".into(),
            ),
        },
        module: "module.wasm".into(),
        configs: vec![ConfigDeclaration {
            key: AGENT_PROMPT.parse().unwrap(),
            schema: String::as_schema(),
            default: DEFAULT_PROMPT.to_owned().as_value(),
            access: AccessPolicy {
                readers: vec![SubjectPattern::SelfModule],
                writers: vec![SubjectPattern::Path("/user/admin".parse().unwrap())],
                callers: vec![],
            },
        }],
        provides: Context::handlers().declarations(),
        uses: vec![],
        subscriptions: vec![],
    }
}
