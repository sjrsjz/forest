pub mod contract;
mod format;
pub mod managed;

use base64::{Engine, engine::general_purpose::STANDARD};
use contract::{Error, GroupOutput, OBJECT, SEND};
use forest_onebot::{
    cq,
    forest::{self as onebot, OneBotError, SendGroupMessage, SentMessage},
    message::Segment,
};
use forest_protocol::{manifest::callers, *};
use forest_sdk::{Handlers, Module, host};
use forest_serde::{DeriveSchema, DeriveValue};
use std::{collections::BTreeSet, rc::Rc};

#[derive(Clone, Debug, DeriveSchema, DeriveValue)]
pub struct GroupRule {
    pub group_id: i64,
    pub enabled: bool,
}

#[derive(Clone, Debug, DeriveSchema, DeriveValue)]
pub struct Settings {
    pub enabled_by_default: bool,
    pub groups: Vec<GroupRule>,
    pub character_threshold: i64,
    pub line_threshold: i64,
}
impl Default for Settings {
    fn default() -> Self {
        Self {
            enabled_by_default: true,
            groups: vec![],
            character_threshold: 1000,
            line_threshold: 20,
        }
    }
}
impl Settings {
    fn validate(&self) -> Result<(), Error> {
        for value in [self.character_threshold, self.line_threshold] {
            if value <= 0 || usize::try_from(value).is_err() {
                return Err(Error::InvalidConfiguration(
                    "thresholds must be positive and fit the guest address space".into(),
                ));
            }
        }
        let mut groups = BTreeSet::new();
        if self.groups.iter().any(|g| !groups.insert(g.group_id)) {
            return Err(Error::InvalidConfiguration(
                "one override per group is required".into(),
            ));
        }
        Ok(())
    }
}

pub struct Output {
    object: Token,
}
impl Module for Output {
    async fn create(context: CreateContext) -> Result<Self, String> {
        Ok(Self {
            object: context.object,
        })
    }
    fn handlers() -> Handlers<Self> {
        Handlers::new().with(
            SEND.parse().unwrap(),
            true,
            callers(vec![
                SubjectPattern::Path("/module/agent".parse().unwrap()),
                SubjectPattern::Path("/module/onebot-tools".parse().unwrap()),
                SubjectPattern::Path("/user/admin".parse().unwrap()),
            ]),
            Self::send,
        )
    }
}
impl Output {
    async fn send(
        self: Rc<Self>,
        _: CallContext,
        input: GroupOutput,
    ) -> Result<SentMessage, Error> {
        let settings: Settings =
            host::config_get(self.object.clone(), "/settings".parse().unwrap())
                .await
                .map_err(|e| Error::Dispatch(e.to_string()))?
                .map_err(|e| Error::InvalidConfiguration(e.to_string()))?;
        settings.validate()?;
        let mut segments = cq::segments(&input.message)
            .map_err(|e| Error::InvalidMessage(e.to_string()))?
            .into_owned();
        if segments.is_empty() {
            return Err(Error::InvalidMessage("message must not be empty".into()));
        }
        for segment in &segments {
            if !matches!(
                segment,
                Segment::Text { .. }
                    | Segment::At { .. }
                    | Segment::Reply { .. }
                    | Segment::Face { .. }
                    | Segment::Image(_)
            ) {
                return Err(Error::InvalidMessage(
                    "supported output: text, at, reply, face, managed image".into(),
                ));
            }
        }
        for segment in &mut segments {
            if let Segment::Image(image) = segment {
                let blob: Result<managed::Blob, managed::Error> = host::call(
                    managed::OBJECT.parse().unwrap(),
                    "/get".parse().unwrap(),
                    &image.file,
                )
                .await
                .map_err(|e| Error::Dispatch(e.to_string()))?;
                let blob = blob.map_err(|e| Error::Media(format!("{e:?}")))?;
                image.file = format!("base64://{}", STANDARD.encode(blob.bytes));
                image.options = Default::default();
            }
        }
        let message = format::format(&input, segments, &settings);
        let sent: Result<SentMessage, OneBotError> = host::call(
            onebot::OBJECT.parse().unwrap(),
            onebot::SEND_GROUP_MESSAGE.parse().unwrap(),
            &SendGroupMessage {
                group_id: input.group_id,
                message,
                auto_escape: Some(false),
            },
        )
        .await
        .map_err(|e| Error::Dispatch(e.to_string()))?;
        sent.map_err(Error::OneBot)
    }
}
forest_sdk::export_module!(super::Output);

pub fn manifest() -> Manifest {
    Manifest {
        package: PackageInfo {
            id: OBJECT.parse().unwrap(),
            version: "0.1.0".into(),
            name: Some("Message output".into()),
            description: Some("Managed CQ output with per-group long-message forwarding".into()),
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
        provides: Output::handlers().declarations(),
        uses: vec![
            InterfaceUse {
                object: managed::OBJECT.parse().unwrap(),
                interface: "/get".parse().unwrap(),
                input: String::as_schema(),
                output: <Result<managed::Blob, managed::Error>>::as_schema(),
            },
            InterfaceUse {
                object: onebot::OBJECT.parse().unwrap(),
                interface: onebot::SEND_GROUP_MESSAGE.parse().unwrap(),
                input: SendGroupMessage::as_schema(),
                output: <Result<SentMessage, OneBotError>>::as_schema(),
            },
        ],
        subscriptions: vec![],
    }
}
