//! The #forest-test Wasm plugin.
use forest_onebot::{
    forest::{self as onebot, GroupMessage, OneBotError, SendGroupMessage, SentMessage},
    message::{Message, Segment},
};
use forest_protocol::{manifest::callers, *};
use forest_sdk::{AsSchema, AsValue, CallContext, CreateContext, Handlers, Module, host};
use std::{cell::Cell, rc::Rc};

pub struct Hello {
    object: Token,
    replies: Cell<i64>,
}
impl Module for Hello {
    async fn create(context: CreateContext) -> Result<Self, String> {
        let replies: i64 =
            host::config_get(context.object.clone(), "/state/replies".parse().unwrap())
                .await
                .map_err(|e| e.to_string())?
                .map_err(|e| e.to_string())?;
        Ok(Self {
            object: context.object,
            replies: Cell::new(replies),
        })
    }
    fn handlers() -> Handlers<Self> {
        Handlers::new().with(
            "/group_message".parse().unwrap(),
            false,
            callers(vec![SubjectPattern::Path(onebot::OBJECT.parse().unwrap())]),
            Self::on_message,
        )
    }
}
impl Hello {
    async fn on_message(self: Rc<Self>, _: CallContext, event: GroupMessage) -> Result<(), String> {
        if !is_command(&event) {
            return Ok(());
        }
        let request = SendGroupMessage {
            group_id: event.group_id,
            message: Message::Cq("hello world".into()),
            auto_escape: Some(true),
        };
        let sent: Result<SentMessage, OneBotError> = host::call(
            onebot::OBJECT.parse().unwrap(),
            onebot::SEND_GROUP_MESSAGE.parse().unwrap(),
            &request,
        )
        .await
        .map_err(|error| error.to_string())?;
        sent.map_err(|error| format!("{error:?}"))?;
        let replies = self.replies.get() + 1;
        host::config_set(
            self.object.clone(),
            "/state/replies".parse().unwrap(),
            &replies,
        )
        .await
        .map_err(|e| e.to_string())?
        .map_err(|e| e.to_string())?;
        self.replies.set(replies);
        Ok(())
    }
}
forest_sdk::export_module!(super::Hello);

fn is_command(event: &GroupMessage) -> bool {
    match &event.message {
        Message::Cq(text) => text.trim() == "#forest-test",
        Message::Segments(segments) => {
            let mut text = String::new();
            for segment in segments {
                match segment {
                    Segment::Text { text: part } => text.push_str(part),
                    Segment::At { qq } if qq == &event.self_id.to_string() => {}
                    _ => return false,
                }
            }
            text.trim() == "#forest-test"
        }
        Message::Single(segment) => {
            matches!(&**segment, Segment::Text {text} if text.trim() == "#forest-test")
        }
    }
}

pub fn manifest() -> Manifest {
    Manifest {
        package: PackageInfo {
            id: "/module/forest-test".parse().unwrap(),
            version: "0.1.0".into(),
            name: Some("Forest Test".into()),
            description: Some("Reply hello world to #forest-test".into()),
        },
        module: "module.wasm".into(),
        configs: vec![ConfigDeclaration {
            key: "/state/replies".parse().unwrap(),
            schema: i64::as_schema(),
            default: 0i64.as_value(),
            access: AccessPolicy {
                readers: vec![SubjectPattern::SelfModule],
                writers: vec![SubjectPattern::SelfModule],
                callers: vec![],
            },
        }],
        provides: Hello::handlers().declarations(),
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
