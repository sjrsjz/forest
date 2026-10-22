pub mod content;
pub mod context;
mod delivery;
pub mod memory;
pub mod provider;
mod state;
pub mod tools;
pub mod types;
mod workflow;
use forest_onebot::{
    cq,
    forest::{self as onebot, GroupMessage},
    message::Segment,
};
use forest_protocol::{manifest::callers, *};
use forest_sdk::{Handlers, Module, host};
use state::*;
use std::{cell::RefCell, collections::BTreeMap, rc::Rc};
use types::*;

pub struct Agent {
    object: Token,
    settings: Settings,
    sessions: RefCell<BTreeMap<i64, Shared>>,
}
impl Module for Agent {
    async fn create(context: CreateContext) -> Result<Self, String> {
        let settings: Settings =
            host::config_get(context.object.clone(), "/settings".parse().unwrap())
                .await
                .map_err(|e| e.to_string())?
                .map_err(|e| e.to_string())?;
        if settings.low_water <= 0 || settings.high_water <= settings.low_water {
            return Err("require 0 < low_water < high_water".into());
        }
        let snapshots: Vec<Snapshot> =
            host::config_get(context.object.clone(), "/state/sessions".parse().unwrap())
                .await
                .map_err(|e| e.to_string())?
                .map_err(|e| e.to_string())?;
        Ok(Self {
            object: context.object,
            settings,
            sessions: RefCell::new(
                snapshots
                    .into_iter()
                    .map(|s| (s.group, Rc::new(RefCell::new(Session::restore(s)))))
                    .collect(),
            ),
        })
    }
    fn handlers() -> Handlers<Self> {
        Handlers::new()
            .with(
                "/group_message".parse().unwrap(),
                true,
                callers(vec![SubjectPattern::Path(onebot::OBJECT.parse().unwrap())]),
                Self::on_message,
            )
            .with(
                "/status".parse().unwrap(),
                true,
                callers(vec![SubjectPattern::Path("/user/admin".parse().unwrap())]),
                Self::status,
            )
    }
    async fn destroy(self: Rc<Self>) -> Result<(), String> {
        let snapshots = self
            .sessions
            .borrow()
            .values()
            .map(|s| s.borrow().data.clone())
            .collect::<Vec<_>>();
        host::config_set(
            self.object.clone(),
            "/state/sessions".parse().unwrap(),
            &snapshots,
        )
        .await
        .map_err(|e| e.to_string())?
        .map_err(|e| e.to_string())
    }
}
impl Agent {
    async fn on_message(
        self: Rc<Self>,
        _: CallContext,
        message: GroupMessage,
    ) -> Result<(), Error> {
        // Self echoes refer to generated turns via /delivery; never create duplicate dialogue.
        if message.user_id == message.self_id
            || !self.settings.allowed_groups.contains(&message.group_id)
        {
            return Ok(());
        }
        let trigger = cq::segments(&message.message)
            .map_err(|e| Error::Protocol(e.to_string()))?
            .iter()
            .any(|s| matches!(s,Segment::At {qq} if qq==&message.self_id.to_string()));
        let session = self
            .sessions
            .borrow_mut()
            .entry(message.group_id)
            .or_insert_with(|| {
                Rc::new(RefCell::new(Session::new(
                    message.group_id,
                    message.self_id,
                )))
            })
            .clone();
        {
            let mut s = session.borrow_mut();
            s.data.pending.push(Queued {
                message,
                accepted: None,
                trigger,
                restored: false,
            });
            if s.owned {
                return Ok(());
            }
            s.owned = true;
        }
        let _owner = Owner(session.clone());
        let result = self.drive(&session).await;
        if let Err(error) = &result {
            self.note(&session, format!("{error:?}")).await;
            self.report(&session, &format!("{error:?}")).await;
        }
        result
    }
    async fn status(self: Rc<Self>, _: CallContext, _: ()) -> Result<Vec<Status>, Error> {
        Ok(self
            .sessions
            .borrow()
            .values()
            .map(|s| {
                let s = s.borrow();
                Status {
                    group: s.data.group,
                    working: s.working,
                    window_units: s.data.window.iter().map(Block::size).sum::<usize>() as i64,
                    queued: (s.data.pending.len() + s.data.staged.len()) as i64,
                    diagnostics: s.data.diagnostics.clone(),
                }
            })
            .collect())
    }
}
forest_sdk::export_module!(super::Agent);
fn dependency<I: AsSchema, O: AsSchema>(object: &str, interface: &str) -> InterfaceUse {
    InterfaceUse {
        object: object.parse().unwrap(),
        interface: interface.parse().unwrap(),
        input: I::as_schema(),
        output: O::as_schema(),
    }
}
pub fn manifest() -> Manifest {
    let config = AccessPolicy {
        readers: vec![SubjectPattern::SelfModule],
        writers: vec![SubjectPattern::Path("/user/admin".parse().unwrap())],
        callers: vec![],
    };
    Manifest {
        package: PackageInfo {
            id: OBJECT.parse().unwrap(),
            version: "0.1.0".into(),
            name: Some("Group Agent".into()),
            description: Some(
                "One working loop per group, queued continuation and append-only active context"
                    .into(),
            ),
        },
        module: "module.wasm".into(),
        configs: vec![
            ConfigDeclaration {
                key: "/settings".parse().unwrap(),
                schema: Settings::as_schema(),
                default: Settings::default().as_value(),
                access: config,
            },
            ConfigDeclaration {
                key: "/state/sessions".parse().unwrap(),
                schema: Vec::<Snapshot>::as_schema(),
                default: Vec::<Snapshot>::new().as_value(),
                access: AccessPolicy {
                    readers: vec![SubjectPattern::SelfModule],
                    writers: vec![SubjectPattern::SelfModule],
                    callers: vec![],
                },
            },
        ],
        provides: Agent::handlers().declarations(),
        uses: vec![
            dependency::<memory::Incoming, Result<memory::Accepted, memory::Error>>(
                memory::OBJECT,
                "/accept",
            ),
            dependency::<Vec<content::MemoryId>, Result<(), memory::Error>>(
                memory::OBJECT,
                "/archive",
            ),
            dependency::<Vec<memory::RecallLink>, Result<(), memory::Error>>(
                memory::OBJECT,
                "/link",
            ),
            dependency::<memory::Delivery, Result<(), memory::Error>>(memory::OBJECT, "/delivery"),
            dependency::<content::MemoryId, Result<memory::Region, memory::Error>>(
                memory::OBJECT,
                "/recall",
            ),
            dependency::<memory::Project, Result<memory::Projection, memory::Error>>(
                memory::OBJECT,
                "/project",
            ),
            dependency::<String, Result<String, memory::Error>>(memory::OBJECT, "/media/read"),
            dependency::<context::FormatInput, Result<Vec<provider::Message>, context::Error>>(
                context::OBJECT,
                context::FORMAT,
            ),
            dependency::<(), Result<provider::Capabilities, provider::Error>>(
                provider::OBJECT,
                provider::CAPABILITIES,
            ),
            dependency::<provider::Request, Result<provider::Completion, provider::Error>>(
                provider::OBJECT,
                provider::COMPLETE,
            ),
            dependency::<(), Result<tools::Catalog, tools::Error>>(tools::OBJECT, "/describe"),
            dependency::<tools::Invocation, Result<String, tools::Error>>(tools::OBJECT, "/invoke"),
            dependency::<onebot::SendGroupMessage, Result<onebot::SentMessage, onebot::OneBotError>>(
                onebot::OBJECT,
                onebot::SEND_GROUP_MESSAGE,
            ),
        ],
        subscriptions: vec![EventSubscription {
            source: onebot::OBJECT.parse().unwrap(),
            event: onebot::GROUP_MESSAGE.parse().unwrap(),
            interface: "/group_message".parse().unwrap(),
        }],
    }
}
