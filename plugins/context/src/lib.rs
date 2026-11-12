//! Hot-reloadable context formatter; the only instance state is its prompt.
mod format;
pub mod markdown;
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
    type LifecycleError = forest_sdk::HostError;

    async fn create(context: CreateContext) -> Result<Self, Self::LifecycleError> {
        let prompt = host::config_get(context.object, AGENT_PROMPT.parse().unwrap()).await??;
        Ok(Self { prompt })
    }

    fn handlers() -> Handlers<Self> {
        Handlers::new().with(
            FORMAT.parse().unwrap(),
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
        let mut messages = std::collections::BTreeMap::new();
        for item in &input.items {
            match item {
                types::ContextItem::GroupMessage { id, message, .. } => {
                    messages.insert((id.group, id.sequence), message.message.clone());
                }
                types::ContextItem::Recall(batch) => {
                    for unit in &batch.units {
                        if let memory::MemoryContent::GroupMessage(message) = &unit.content {
                            messages
                                .insert((unit.id.group, unit.id.sequence), message.message.clone());
                        }
                    }
                }
                _ => {}
            }
        }
        let keys = messages.keys().copied().collect::<Vec<_>>();
        let inputs = messages
            .into_iter()
            .map(|((group_id, _), message)| markdown::Encode { group_id, message })
            .collect::<Vec<_>>();
        let encoded: Result<Vec<Vec<markdown::Fragment>>, markdown::Error> = host::call(
            markdown::OBJECT.parse().unwrap(),
            "/encode".parse().unwrap(),
            &inputs,
        )
        .await
        .map_err(|e| Error::Projection(e.to_string()))?;
        let encoded = encoded.map_err(|e| Error::Projection(format!("{e:?}")))?;
        Ok(format::format(
            &self.prompt,
            input,
            keys.into_iter().zip(encoded).collect(),
        ))
    }
}

forest_sdk::export_module!(super::Context);

pub fn manifest() -> Manifest {
    Manifest {
        events: vec![],
        activated: None,
        package: PackageInfo {
            id: OBJECT.parse().unwrap(),
            version: "0.1.0".into(),
            name: Some("Context formatter".into()),
            description: Some(
                "Ordered content to OpenAI messages with a configurable Agent prompt".into(),
            ),
            ..Default::default()
        },
        module: "module.wasm".into(),
        lifecycle_error: <Context as Module>::LifecycleError::as_schema(),
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
        uses: vec![InterfaceUse {
            object: markdown::OBJECT.parse().unwrap(),
            interface: "/encode".parse().unwrap(),
            input: Vec::<markdown::Encode>::as_schema(),
            output: Result::<Vec<Vec<markdown::Fragment>>, markdown::Error>::as_schema(),
        }],
        subscriptions: vec![],
    }
}
