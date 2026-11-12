use forest_onebot::{
    forest::{self as onebot, GroupMessage, OneBotError, SendGroupMessage, SentMessage},
    message::{Message, Segment},
};
use forest_protocol::{manifest::callers, *};
use forest_sdk::{CallContext, CreateContext, DeriveSchema, DeriveValue, Handlers, Module, host};
use std::{convert::Infallible, rc::Rc};

#[derive(Debug, DeriveSchema, DeriveValue)]
pub struct Greeting {
    pub name: String,
}

#[derive(Debug, DeriveSchema, DeriveValue)]
pub enum Error {
    Dispatch(DispatchError),
    OneBot(OneBotError),
}

impl From<DispatchError> for Error {
    fn from(error: DispatchError) -> Self {
        Self::Dispatch(error)
    }
}

pub struct Hello {
    reply: String,
}

impl Module for Hello {
    type LifecycleError = forest_sdk::HostError;

    async fn create(context: CreateContext) -> Result<Self, Self::LifecycleError> {
        let reply = host::config_get(context.object, "/settings/reply".parse().unwrap()).await??;
        Ok(Self { reply })
    }

    fn handlers() -> Handlers<Self> {
        Handlers::new()
            .with(
                "/group_message".parse().unwrap(),
                callers(vec![SubjectPattern::Path(onebot::OBJECT.parse().unwrap())]),
                Self::on_message,
            )
            .with(
                "/greet".parse().unwrap(),
                callers(vec![
                    SubjectPattern::Path("/module/example-consumer".parse().unwrap()),
                    SubjectPattern::Path("/user/admin".parse().unwrap()),
                ]),
                Self::greet,
            )
    }
}

impl Hello {
    async fn on_message(self: Rc<Self>, _: CallContext, event: GroupMessage) -> Result<(), Error> {
        if !is_command(&event) {
            return Ok(());
        }
        let request = SendGroupMessage {
            group_id: event.group_id,
            message: Message::Segments(vec![Segment::Text {
                text: self.reply.clone(),
            }]),
            auto_escape: None,
        };
        let sent: Result<SentMessage, OneBotError> = host::call(
            onebot::OBJECT.parse().unwrap(),
            onebot::SEND_GROUP_MESSAGE.parse().unwrap(),
            &request,
        )
        .await?;
        sent.map_err(Error::OneBot)?;
        Ok(())
    }

    async fn greet(
        self: Rc<Self>,
        _: CallContext,
        request: Greeting,
    ) -> Result<String, Infallible> {
        Ok(format!("{}, {}", self.reply, request.name))
    }
}

fn is_command(event: &GroupMessage) -> bool {
    match &event.message {
        Message::Cq(text) => text.trim() == "#forest-hello",
        Message::Segments(segments) => {
            let mut text = String::new();
            for segment in segments {
                match segment {
                    Segment::Text { text: part } => text.push_str(part),
                    Segment::At { qq } if qq == &event.self_id.to_string() => {}
                    _ => return false,
                }
            }
            text.trim() == "#forest-hello"
        }
        Message::Single(segment) => {
            matches!(&**segment, Segment::Text { text } if text.trim() == "#forest-hello")
        }
    }
}

forest_sdk::export_module!(super::Hello);

pub fn manifest() -> Manifest {
    Manifest {
        events: vec![],
        activated: None,
        package: PackageInfo {
            id: "/module/example-hello".parse().unwrap(),
            version: "0.1.0".into(),
            name: Some("Hello module example".into()),
            description: Some(
                "Reply to #forest-hello and expose a typed greeting interface".into(),
            ),
            ..Default::default()
        },
        module: "module.wasm".into(),
        lifecycle_error: <Hello as Module>::LifecycleError::as_schema(),
        configs: vec![ConfigDeclaration {
            key: "/settings/reply".parse().unwrap(),
            schema: String::as_schema(),
            default: "hello world".to_owned().as_value(),
            access: AccessPolicy {
                readers: vec![SubjectPattern::SelfModule],
                writers: vec![SubjectPattern::Path("/user/admin".parse().unwrap())],
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
