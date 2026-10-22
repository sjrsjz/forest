//! Exact NPUCraft help replies, independent of the Agent.
use forest_onebot::{
    cq,
    forest::{self as onebot, GroupMessage, OneBotError, SendGroupMessage, SentMessage},
    message::{Message, Segment},
};
use forest_protocol::{manifest::callers, *};
use forest_sdk::{Handlers, Module, host};
use forest_serde::{DeriveSchema, DeriveValue};
use std::rc::Rc;

pub const OBJECT: &str = "/module/npucraft";

#[derive(Clone, Debug, DeriveSchema, DeriveValue)]
pub struct Settings {
    /// Empty means all groups, matching the original plugin.
    pub allowed_groups: Vec<i64>,
    pub triggers: Vec<String>,
    pub reply: String,
}
impl Default for Settings {
    fn default() -> Self {
        Self {
            allowed_groups: vec![],
            triggers: vec!["地图".into(), "卫星地图".into(), "ciallo".into()],
            reply: include_str!("reply.txt").trim().into(),
        }
    }
}
pub struct NpuCraft {
    settings: Settings,
}
impl Module for NpuCraft {
    async fn create(context: CreateContext) -> Result<Self, String> {
        let settings = host::config_get(context.object, "/settings".parse().unwrap())
            .await
            .map_err(|e| e.to_string())?
            .map_err(|e| e.to_string())?;
        Ok(Self { settings })
    }
    fn handlers() -> Handlers<Self> {
        Handlers::new().with(
            "/group_message".parse().unwrap(),
            true,
            callers(vec![SubjectPattern::Path(onebot::OBJECT.parse().unwrap())]),
            Self::on_message,
        )
    }
}
impl NpuCraft {
    async fn on_message(
        self: Rc<Self>,
        _: CallContext,
        message: GroupMessage,
    ) -> Result<(), String> {
        if message.user_id == message.self_id
            || (!self.settings.allowed_groups.is_empty()
                && !self.settings.allowed_groups.contains(&message.group_id))
        {
            return Ok(());
        }
        let text = cq::encode(&message.message).map_err(|e| e.to_string())?;
        if !self.settings.triggers.iter().any(|t| t == text.trim()) {
            return Ok(());
        }
        let sent: Result<SentMessage, OneBotError> = host::call(
            onebot::OBJECT.parse().unwrap(),
            onebot::SEND_GROUP_MESSAGE.parse().unwrap(),
            &SendGroupMessage {
                group_id: message.group_id,
                message: Message::Segments(vec![Segment::text(&self.settings.reply)]),
                auto_escape: Some(true),
            },
        )
        .await
        .map_err(|e| e.to_string())?;
        sent.map(|_| ()).map_err(|e| format!("{e:?}"))
    }
}
forest_sdk::export_module!(super::NpuCraft);

pub fn manifest() -> Manifest {
    Manifest {
        package: PackageInfo {
            id: OBJECT.parse().unwrap(),
            version: "0.1.0".into(),
            name: Some("NPUCraft help".into()),
            description: Some("Exact map and server help replies".into()),
        },
        module: "module.wasm".into(),
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
        provides: NpuCraft::handlers().declarations(),
        uses: vec![InterfaceUse {
            object: onebot::OBJECT.parse().unwrap(),
            interface: onebot::SEND_GROUP_MESSAGE.parse().unwrap(),
            input: SendGroupMessage::as_schema(),
            output: <Result<SentMessage, OneBotError>>::as_schema(),
        }],
        subscriptions: vec![EventSubscription {
            source: onebot::OBJECT.parse().unwrap(),
            event: onebot::GROUP_MESSAGE.parse().unwrap(),
            interface: "/group_message".parse().unwrap(),
        }],
    }
}
