//! OneBot v11 transport and event ingress, entirely within an ordinary Wasm package.
mod actions;
mod connection;

use forest_onebot::forest::{
    self as contract, GroupMessage, OneBotError, Query, SendGroupMessage, SendPrivateMessage,
    SentMessage,
};
use forest_protocol::{manifest::callers, service::IoEvent, *};
use forest_sdk::{Handlers, Module, host};
use forest_serde::{DeriveSchema, DeriveValue};
use futures::channel::oneshot;
use std::{
    cell::{Cell, RefCell},
    collections::HashMap,
    rc::Rc,
};

pub const OBJECT: &str = contract::OBJECT;

#[derive(Clone, Debug, DeriveSchema, DeriveValue)]
pub struct Settings {
    pub websocket: String,
    pub token: String,
    pub connect_timeout_ms: i64,
    pub action_timeout_ms: i64,
    pub reconnect_min_ms: i64,
    pub reconnect_max_ms: i64,
    pub allowed_groups: Vec<i64>,
}
impl Default for Settings {
    fn default() -> Self {
        Self {
            websocket: "ws://127.0.0.1:8080".into(),
            token: String::new(),
            connect_timeout_ms: 10_000,
            action_timeout_ms: 30_000,
            reconnect_min_ms: 1_000,
            reconnect_max_ms: 30_000,
            allowed_groups: vec![],
        }
    }
}

#[derive(Debug, DeriveSchema, DeriveValue)]
pub enum LifecycleError {
    Host(forest_sdk::HostError),
    InvalidSettings,
}
impl From<forest_sdk::HostError> for LifecycleError {
    fn from(value: forest_sdk::HostError) -> Self {
        Self::Host(value)
    }
}

pub struct Adapter {
    settings: Settings,
    socket: Cell<Option<u64>>,
    next: Cell<u64>,
    pending: RefCell<HashMap<String, oneshot::Sender<Result<Vec<u8>, OneBotError>>>>,
    reconnect: Cell<bool>,
    delay: Cell<u64>,
}
impl Module for Adapter {
    type LifecycleError = LifecycleError;
    async fn create(context: CreateContext) -> Result<Self, LifecycleError> {
        let settings: Settings = host::config_get(context.object, "/settings".parse().unwrap())
            .await
            .map_err(forest_sdk::HostError::Dispatch)?
            .map_err(forest_sdk::HostError::Service)?;
        if settings.connect_timeout_ms <= 0
            || settings.action_timeout_ms <= 0
            || settings.reconnect_min_ms <= 0
            || settings.reconnect_max_ms < settings.reconnect_min_ms
        {
            return Err(LifecycleError::InvalidSettings);
        }
        Ok(Self {
            delay: Cell::new(settings.reconnect_min_ms as u64),
            settings,
            socket: Cell::new(None),
            next: Cell::new(0),
            pending: RefCell::new(HashMap::new()),
            reconnect: Cell::new(false),
        })
    }
    fn handlers() -> Handlers<Self> {
        let clients = callers(vec![
            SubjectPattern::Path("/module/**".parse().unwrap()),
            SubjectPattern::Path("/user/admin".parse().unwrap()),
        ]);
        Handlers::new()
            .with(
                "/activated".parse().unwrap(),
                callers(vec![SubjectPattern::Path(
                    "/system/lifecycle".parse().unwrap(),
                )]),
                Self::activated,
            )
            .with(
                "/io".parse().unwrap(),
                callers(vec![SubjectPattern::Path("/system/io".parse().unwrap())]),
                Self::on_io,
            )
            .with(
                contract::SEND_GROUP_MESSAGE.parse().unwrap(),
                clients.clone(),
                Self::send_group,
            )
            .with(
                contract::SEND_PRIVATE_MESSAGE.parse().unwrap(),
                clients.clone(),
                Self::send_private,
            )
            .with(contract::QUERY.parse().unwrap(), clients, Self::query)
    }
}

impl Adapter {
    fn group_allowed(&self, group: i64) -> bool {
        self.settings.allowed_groups.is_empty() || self.settings.allowed_groups.contains(&group)
    }
}

forest_sdk::export_module!(super::Adapter);

pub fn manifest() -> Manifest {
    Manifest {
        package: PackageInfo {
            id: OBJECT.parse().unwrap(),
            version: "0.1.0".into(),
            name: Some("OneBot v11 adapter".into()),
            description: Some(
                "Wasm protocol adapter using generic host WebSocket notifications".into(),
            ),
            ..Default::default()
        },
        module: "module.wasm".into(),
        lifecycle_error: LifecycleError::as_schema(),
        activated: Some("/activated".parse().unwrap()),
        configs: vec![ConfigDeclaration {
            key: "/settings".parse().unwrap(),
            schema: Settings::as_schema(),
            default: Settings::default().as_value(),
            access: AccessPolicy {
                readers: vec![
                    SubjectPattern::SelfModule,
                    SubjectPattern::Path("/user/admin".parse().unwrap()),
                ],
                writers: vec![SubjectPattern::Path("/user/admin".parse().unwrap())],
                callers: vec![],
            },
        }],
        provides: Adapter::handlers().declarations(),
        uses: vec![],
        subscriptions: vec![],
        events: vec![EventDeclaration {
            path: contract::GROUP_MESSAGE.parse().unwrap(),
            input: GroupMessage::as_schema(),
            access: AccessPolicy {
                readers: vec![SubjectPattern::Path("/module/**".parse().unwrap())],
                writers: vec![],
                callers: vec![],
            },
        }],
    }
}
