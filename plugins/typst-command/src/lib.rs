//! The /typst group command; rendering and media delivery remain separate modules.
mod command;
mod output;
mod renderer;

use forest_onebot::{
    forest::{self as onebot, GroupMessage, SentMessage},
    message::{Message, Segment},
};
use forest_protocol::{manifest::callers, *};
use forest_sdk::{Handlers, Module, host};
use forest_serde::{DeriveSchema, DeriveValue};
use serde::Deserialize;
use std::rc::Rc;

pub const OBJECT: &str = "/module/typst-command";

#[derive(Clone, Debug, Default, DeriveSchema, DeriveValue)]
pub struct Settings {
    /// An empty list enables the explicit command in every group.
    pub allowed_groups: Vec<i64>,
}

pub struct Command {
    settings: Settings,
}

impl Module for Command {
    type LifecycleError = forest_sdk::HostError;

    async fn create(context: CreateContext) -> Result<Self, Self::LifecycleError> {
        let settings = host::config_get(context.object, "/settings".parse().unwrap()).await??;
        Ok(Self { settings })
    }

    fn handlers() -> Handlers<Self> {
        Handlers::new().with(
            "/group_message".parse().unwrap(),
            callers(vec![SubjectPattern::Path(onebot::OBJECT.parse().unwrap())]),
            Self::on_message,
        )
    }
}

#[derive(Deserialize)]
struct Document {
    pages: Vec<Page>,
}

#[derive(Deserialize)]
struct Page {
    reference: String,
}

#[derive(Deserialize)]
struct Diagnostics {
    diagnostics: Vec<Diagnostic>,
}

#[derive(Deserialize)]
struct Diagnostic {
    message: String,
    line: Option<usize>,
    column: Option<usize>,
    hints: Vec<String>,
}

fn error_text(error: renderer::Error) -> String {
    match error {
        renderer::Error::UnknownTool(message) | renderer::Error::Arguments(message) => message,
        renderer::Error::Execution(message) => {
            // The renderer returns diagnostic JSON for source errors, plain text for I/O errors.
            let Ok(report) = serde_json::from_str::<Diagnostics>(&message) else {
                return message;
            };
            report
                .diagnostics
                .into_iter()
                .map(|d| {
                    let location = match (d.line, d.column) {
                        (Some(line), Some(column)) => format!("第 {line} 行，第 {column} 列："),
                        _ => String::new(),
                    };
                    let mut text = format!("{location}{}", d.message);
                    for hint in d.hints {
                        text.push_str(&format!("\n提示：{hint}"));
                    }
                    text
                })
                .collect::<Vec<_>>()
                .join("\n")
        }
    }
}

impl Command {
    async fn on_message(self: Rc<Self>, _: CallContext, event: GroupMessage) -> Result<(), String> {
        if event.user_id == event.self_id
            || (!self.settings.allowed_groups.is_empty()
                && !self.settings.allowed_groups.contains(&event.group_id))
        {
            return Ok(());
        }
        let Some(source) = command::source(&event) else {
            return Ok(());
        };
        let content = match source {
            Ok(source) if source.is_empty() => vec![Segment::text(command::HELP)],
            Ok(source) => match render(&event, source).await {
                Ok(pages) => pages,
                Err(error) => vec![Segment::text(format!("Typst 渲染失败：\n{error}"))],
            },
            Err(error) => vec![Segment::text(error)],
        };
        let mut segments = vec![Segment::Reply {
            id: event.message_id.to_string(),
        }];
        segments.extend(content);
        let sent: Result<SentMessage, output::Error> = host::call(
            output::OBJECT.parse().unwrap(),
            output::SEND.parse().unwrap(),
            &output::GroupOutput {
                group_id: event.group_id,
                bot_id: event.self_id,
                bot_name: None,
                message: Message::Segments(segments),
            },
        )
        .await
        .map_err(|e| e.to_string())?;
        // Delivery errors are returned once, without retrying a possibly sent message.
        sent.map(|_| ()).map_err(|e| format!("{e:?}"))
    }
}

async fn render(event: &GroupMessage, source: String) -> Result<Vec<Segment>, String> {
    let rendered: Result<String, renderer::Error> = host::call(
        renderer::OBJECT.parse().unwrap(),
        "/render".parse().unwrap(),
        &renderer::Invocation {
            group_id: event.group_id,
            bot_id: event.self_id,
            name: "typst_render".into(),
            arguments_json: serde_json::json!({ "source": source }).to_string(),
        },
    )
    .await
    .map_err(|e| e.to_string())?;
    let json = rendered.map_err(error_text)?;
    let document: Document = serde_json::from_str(&json).map_err(|e| e.to_string())?;
    Ok(document
        .pages
        .into_iter()
        .map(|page| Segment::image(page.reference))
        .collect())
}

forest_sdk::export_module!(super::Command);

pub fn manifest() -> Manifest {
    Manifest {
        events: vec![],
        activated: None,
        package: PackageInfo {
            id: OBJECT.parse().unwrap(),
            version: "0.1.0".into(),
            name: Some("Typst group command".into()),
            description: Some("Render /typst source directly from group messages".into()),
            ..Default::default()
        },
        module: "module.wasm".into(),
        lifecycle_error: <Command as Module>::LifecycleError::as_schema(),
        configs: vec![ConfigDeclaration {
            key: "/settings".parse().unwrap(),
            schema: Settings::as_schema(),
            default: Settings::default().as_value(),
            access: AccessPolicy {
                readers: vec![SubjectPattern::SelfModule],
                writers: vec![SubjectPattern::Path("/user/admin".parse().unwrap())],
                callers: vec![],
            },
        }],
        provides: Command::handlers().declarations(),
        uses: vec![
            InterfaceUse {
                object: renderer::OBJECT.parse().unwrap(),
                interface: "/render".parse().unwrap(),
                input: renderer::Invocation::as_schema(),
                output: Result::<String, renderer::Error>::as_schema(),
            },
            InterfaceUse {
                object: output::OBJECT.parse().unwrap(),
                interface: output::SEND.parse().unwrap(),
                input: output::GroupOutput::as_schema(),
                output: Result::<SentMessage, output::Error>::as_schema(),
            },
        ],
        subscriptions: vec![EventSubscription {
            source: onebot::OBJECT.parse().unwrap(),
            event: onebot::GROUP_MESSAGE.parse().unwrap(),
            interface: "/group_message".parse().unwrap(),
        }],
    }
}
