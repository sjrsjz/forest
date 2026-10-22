//! Authenticated group administration, implemented entirely as a Wasm frontend.
mod command;
pub mod control;
use command::Command;
use forest_onebot::{
    cq,
    forest::{self as onebot, GroupMessage, OneBotError, SendGroupMessage, SentMessage},
    message::{Message, Segment},
};
use forest_protocol::{manifest::callers, *};
use forest_sdk::{Handlers, Module, host};
use forest_serde::{DeriveSchema, DeriveValue};
use std::rc::Rc;

pub const OBJECT: &str = "/module/shell";
#[derive(Clone, Debug, Default, DeriveSchema, DeriveValue)]
pub struct Settings {
    pub administrators: Vec<i64>,
    pub allowed_groups: Vec<i64>,
}
pub struct Shell {
    object: Token,
}
impl Module for Shell {
    async fn create(context: CreateContext) -> Result<Self, String> {
        Ok(Self {
            object: context.object,
        })
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
impl Shell {
    async fn on_message(
        self: Rc<Self>,
        context: CallContext,
        message: GroupMessage,
    ) -> Result<(), String> {
        if message.user_id == message.self_id || message.anonymous.is_some() {
            return Ok(());
        }
        let segments = cq::segments(&message.message).map_err(|e| e.to_string())?;
        let mut text = String::new();
        for segment in segments.iter() {
            match segment {
                Segment::Text { text: part } => text.push_str(part),
                _ => return Ok(()),
            }
        }
        let mut words = text.trim().splitn(2, char::is_whitespace);
        if words.next() != Some("#forest") {
            return Ok(());
        }
        // Read once per command; an ACL edit takes effect without replacing this instance.
        let settings: Settings =
            host::config_get(self.object.clone(), "/settings".parse().unwrap())
                .await
                .map_err(|e| e.to_string())?
                .map_err(|e| e.to_string())?;
        if !settings.allowed_groups.contains(&message.group_id) {
            return Ok(());
        }
        let authorized = settings.administrators.contains(&message.user_id)
            && context.origin
                == format!("/user/onebot/{}", message.user_id)
                    .parse::<Token>()
                    .unwrap();
        let reply = if !authorized {
            "没有群聊管理权限。".into()
        } else {
            match command::parse(words.next().unwrap_or_default()) {
                Ok(command) => self
                    .execute(command)
                    .await
                    .unwrap_or_else(|e| format!("执行失败：{e}")),
                Err(error) => error,
            }
        };
        send(message.group_id, reply).await
    }
    async fn execute(&self, command: Command) -> Result<String, String> {
        Ok(match command {
            Command::Help => command::HELP.into(),
            Command::Status => {
                let state: control::Snapshot = call("/snapshot", &()).await?;
                let serving = state
                    .modules
                    .iter()
                    .filter(|m| matches!(m.phase, control::Phase::Serving))
                    .count();
                format!(
                    "Forest：{} 个模块，{serving} 个 Serving；调度并行度 {}。",
                    state.modules.len(),
                    state.parallelism
                )
            }
            Command::Modules => {
                let state: control::Snapshot = call("/snapshot", &()).await?;
                state
                    .modules
                    .into_iter()
                    .map(|m| {
                        format!(
                            "{} · {:?} · {} · 队列 {}",
                            m.object, m.phase, m.version, m.queued
                        )
                    })
                    .collect::<Vec<_>>()
                    .join("\n")
            }
            Command::Show(object) => {
                let state: control::Snapshot = call("/snapshot", &()).await?;
                let module = state
                    .modules
                    .into_iter()
                    .find(|m| m.object == object)
                    .ok_or_else(|| "模块未加载".to_owned())?;
                format!(
                    "{}\n状态：{:?}\n版本：{}\n队列：{}\n依赖：\n{}",
                    module.object,
                    module.phase,
                    module.version,
                    module.queued,
                    module
                        .dependencies
                        .into_iter()
                        .map(|d| format!("{} {}", d.object, d.interface))
                        .collect::<Vec<_>>()
                        .join("\n")
                )
            }
            Command::Packages => {
                let packages: Vec<control::LocalPackage> = call("/packages", &()).await?;
                packages
                    .into_iter()
                    .map(|p| match p {
                        control::LocalPackage::Ready {
                            directory,
                            object,
                            version,
                        } => format!("{object} · {version} · {directory}"),
                        control::LocalPackage::Invalid { directory, message } => {
                            format!("{directory} · 无效包：{message}")
                        }
                    })
                    .collect::<Vec<_>>()
                    .join("\n")
            }
            Command::Load(object) => {
                call::<_, ()>("/load", &object).await?;
                format!("已加载 {object}")
            }
            Command::Reload(request) => {
                call::<_, ()>("/reload", &request).await?;
                format!("已重载 {}", request.object)
            }
            Command::Unload(request) => {
                call::<_, ()>("/unload", &request).await?;
                format!("已卸载 {}", request.object)
            }
            Command::Force(object) => {
                call::<_, ()>("/force", &object).await?;
                format!("已请求强制中断 {object}")
            }
            Command::Keys(object) => {
                let keys: Vec<String> = call("/config/keys", &object).await?;
                keys.into_iter()
                    .map(|k| k.to_string())
                    .collect::<Vec<_>>()
                    .join("\n")
            }
            Command::Get(target) => {
                let entry: control::ConfigEntry = call("/config/get", &target).await?;
                format!(
                    "{} {}\n{}\n当前值：\n{}",
                    target.object, target.key, entry.schema, entry.value
                )
            }
            Command::Set(request) => {
                call::<_, ()>("/config/set", &request).await?;
                format!("已保存 {} {}。", request.target.object, request.target.key)
            }
        })
    }
}
async fn call<I: AsValue, O: AsValue>(path: &str, input: &I) -> Result<O, String> {
    let output: Result<O, control::Error> = host::call(
        control::OBJECT.parse().unwrap(),
        path.parse().unwrap(),
        input,
    )
    .await
    .map_err(|e| e.to_string())?;
    output.map_err(|e| match e {
        control::Error::CallingModule => {
            "本入口不能同步重载、卸载或强制中断 Shell 自身；请使用 WebUI。".into()
        }
        other => format!("{other:?}"),
    })
}
async fn send(group: i64, mut text: String) -> Result<(), String> {
    if let Some((index, _)) = text.char_indices().nth(3500) {
        text.truncate(index);
        text.push_str("\n……内容较长，完整内容请在 WebUI 查看。");
    }
    let sent: Result<SentMessage, OneBotError> = host::call(
        onebot::OBJECT.parse().unwrap(),
        onebot::SEND_GROUP_MESSAGE.parse().unwrap(),
        &SendGroupMessage {
            group_id: group,
            message: Message::Segments(vec![Segment::text(text)]),
            auto_escape: Some(true),
        },
    )
    .await
    .map_err(|e| e.to_string())?;
    sent.map(|_| ()).map_err(|e| format!("{e:?}"))
}
forest_sdk::export_module!(super::Shell);

pub fn manifest() -> Manifest {
    fn dependency<I: AsSchema, O: AsSchema>(path: &str) -> InterfaceUse {
        InterfaceUse {
            object: control::OBJECT.parse().unwrap(),
            interface: path.parse().unwrap(),
            input: I::as_schema(),
            output: <Result<O, control::Error>>::as_schema(),
        }
    }
    let mut uses = vec![
        dependency::<(), control::Snapshot>("/snapshot"),
        dependency::<(), Vec<control::LocalPackage>>("/packages"),
        dependency::<String, ()>("/load"),
        dependency::<control::Stop, ()>("/reload"),
        dependency::<control::Stop, ()>("/unload"),
        dependency::<String, ()>("/force"),
        dependency::<String, Vec<String>>("/config/keys"),
        dependency::<control::ConfigAddress, control::ConfigEntry>("/config/get"),
        dependency::<control::ConfigValue, ()>("/config/set"),
    ];
    uses.push(InterfaceUse {
        object: onebot::OBJECT.parse().unwrap(),
        interface: onebot::SEND_GROUP_MESSAGE.parse().unwrap(),
        input: SendGroupMessage::as_schema(),
        output: <Result<SentMessage, OneBotError>>::as_schema(),
    });
    Manifest {
        package: PackageInfo {
            id: OBJECT.parse().unwrap(),
            version: "0.1.0".into(),
            name: Some("Group shell".into()),
            description: Some("Authorized Forest administration over group messages".into()),
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
        provides: Shell::handlers().declarations(),
        uses,
        subscriptions: vec![EventSubscription {
            source: onebot::OBJECT.parse().unwrap(),
            event: onebot::GROUP_MESSAGE.parse().unwrap(),
            interface: "/group_message".parse().unwrap(),
        }],
    }
}
