//! Host-only read capability. No guest execution, writable store, or management
//! handle crosses this boundary. These views are never scheduling authorities.
use super::*;
use serde::Serialize;
use std::{
    collections::BTreeMap,
    time::{SystemTime, UNIX_EPOCH},
};

#[derive(Clone)]
pub struct Inspector {
    core: Weak<Core>,
}

#[derive(Debug, thiserror::Error)]
pub enum InspectError {
    #[error("host stopped")]
    HostStopped,
    #[error(transparent)]
    Configuration(#[from] forest_config::ConfigError),
}

#[derive(Clone, Debug, Serialize)]
pub struct RuntimeSnapshot {
    pub sampled_at_ms: u64,
    pub parallelism: usize,
    pub memory_limit_bytes: usize,
    pub modules: Vec<ModuleSnapshot>,
    pub hosts: Vec<HostSnapshot>,
}
#[derive(Clone, Debug, Serialize)]
pub struct ModuleSnapshot {
    pub object: Token,
    pub published: ReleaseView,
    pub lifecycle: LifecycleSnapshot,
    pub interfaces: Vec<InterfaceView>,
    pub uses: Vec<UseView>,
    pub subscriptions: Vec<SubscriptionView>,
    pub budget: BudgetView,
    pub queued_next: usize,
}
#[derive(Clone, Debug, Serialize)]
pub struct ReleaseView {
    pub version: String,
    pub name: Option<String>,
    pub description: Option<String>,
}
impl From<&PackageInfo> for ReleaseView {
    fn from(info: &PackageInfo) -> Self {
        Self {
            version: info.version.clone(),
            name: info.name.clone(),
            description: info.description.clone(),
        }
    }
}
#[derive(Clone, Debug, Serialize)]
pub struct InstanceView {
    pub id: String,
    pub release: ReleaseView,
}
impl From<&Instance> for InstanceView {
    fn from(instance: &Instance) -> Self {
        Self {
            id: instance.id.to_string(),
            release: (&instance.package.manifest.package).into(),
        }
    }
}
#[derive(Clone, Debug, Serialize)]
#[serde(tag = "phase", rename_all = "snake_case")]
pub enum LifecycleSnapshot {
    Creating {
        instance: InstanceView,
    },
    Serving {
        instance: InstanceView,
    },
    Stopping {
        instance: InstanceView,
        mode: StopMode,
        replacement: Option<ReleaseView>,
    },
    Destroying {
        instance: InstanceView,
        replacement: Option<ReleaseView>,
    },
    Releasing {
        instance: InstanceView,
        replacement: Option<ReleaseView>,
    },
    Failed {
        reason: String,
    },
}
#[derive(Clone, Debug, Serialize)]
pub struct InterfaceView {
    pub path: Token,
    pub reentrant: bool,
    pub queued: usize,
}
#[derive(Clone, Debug, Serialize)]
pub struct UseView {
    pub object: Token,
    pub interface: Token,
}
#[derive(Clone, Debug, Serialize)]
pub struct SubscriptionView {
    pub source: Token,
    pub event: Token,
    pub interface: Token,
}
#[derive(Clone, Debug, Serialize)]
pub struct BudgetView {
    pub requests_used: usize,
    pub requests_max: usize,
    pub input_bytes_used: usize,
    pub input_bytes_max: usize,
}
#[derive(Clone, Debug, Serialize)]
pub struct HostSnapshot {
    pub object: Token,
    pub interfaces: Vec<InterfaceView>,
    pub events: Vec<Token>,
}

struct CapturedModule {
    manifest: Arc<Manifest>,
    lifecycle: LifecycleSnapshot,
    budget: BudgetView,
    queues: HashMap<Token, usize>,
    queued_next: usize,
}
fn replacement(after: &After) -> Option<ReleaseView> {
    match after {
        After::Reload { package, .. } => Some((&package.manifest.package).into()),
        _ => None,
    }
}
fn deferred(after: &After) -> usize {
    match after {
        After::Reload { inbox, .. } => inbox.len(),
        _ => 0,
    }
}
fn capture(slot: &Slot, options: &RuntimeOptions) -> CapturedModule {
    let (lifecycle, live, queued_next) = match &slot.state {
        State::Creating {
            instance, inbox, ..
        } => (
            LifecycleSnapshot::Creating {
                instance: instance.as_ref().into(),
            },
            None,
            match inbox {
                CreatingInbox::Closed => 0,
                CreatingInbox::Deferred(queue) => queue.len(),
            },
        ),
        State::Serving(live) => (
            LifecycleSnapshot::Serving {
                instance: live.instance.as_ref().into(),
            },
            Some(live),
            0,
        ),
        State::Stopping { live, mode, after } => (
            LifecycleSnapshot::Stopping {
                instance: live.instance.as_ref().into(),
                mode: *mode,
                replacement: replacement(after),
            },
            Some(live),
            deferred(after),
        ),
        State::Destroying { instance, after } => (
            LifecycleSnapshot::Destroying {
                instance: instance.as_ref().into(),
                replacement: replacement(after),
            },
            None,
            deferred(after),
        ),
        State::Releasing { instance, after } => (
            LifecycleSnapshot::Releasing {
                instance: instance.as_ref().into(),
                replacement: replacement(after),
            },
            None,
            deferred(after),
        ),
        State::Failed(reason) => (
            LifecycleSnapshot::Failed {
                reason: reason.clone(),
            },
            None,
            0,
        ),
    };
    CapturedModule {
        manifest: slot.published.clone(),
        lifecycle,
        queued_next,
        queues: live
            .map(|live| {
                live.interfaces
                    .iter()
                    .map(|(path, inbox)| (path.clone(), inbox.queue.len()))
                    .collect()
            })
            .unwrap_or_default(),
        budget: BudgetView {
            requests_used: options.max_requests - slot.budget.requests.available_permits(),
            requests_max: options.max_requests,
            input_bytes_used: options.max_input_bytes - slot.budget.bytes.available_permits(),
            input_bytes_max: options.max_input_bytes,
        },
    }
}

impl Scheduler {
    /// Explicit host debug capability. Never registered as a module interface.
    pub fn inspector(&self) -> Inspector {
        Inspector {
            core: Arc::downgrade(&self.core),
        }
    }
}
impl Inspector {
    fn host(&self) -> std::result::Result<Arc<Core>, InspectError> {
        self.core.upgrade().ok_or(InspectError::HostStopped)
    }
    pub fn snapshot(&self) -> std::result::Result<RuntimeSnapshot, InspectError> {
        let core = self.host()?;
        let (modules, natives, events) = {
            let directory = core.directory.lock().unwrap();
            (
                directory
                    .slots
                    .values()
                    .map(|slot| capture(slot, &core.options))
                    .collect::<Vec<_>>(),
                directory
                    .natives
                    .values()
                    .map(|native| {
                        (
                            native.object.clone(),
                            native.declaration.path.clone(),
                            native.declaration.reentrant,
                        )
                    })
                    .collect::<Vec<_>>(),
                directory.events.keys().cloned().collect::<Vec<_>>(),
            )
        };
        let mut modules: Vec<_> = modules
            .into_iter()
            .map(|captured| {
                let m = captured.manifest;
                ModuleSnapshot {
                    object: m.package.id.clone(),
                    published: (&m.package).into(),
                    lifecycle: captured.lifecycle,
                    budget: captured.budget,
                    queued_next: captured.queued_next,
                    interfaces: m
                        .provides
                        .iter()
                        .map(|p| InterfaceView {
                            path: p.path.clone(),
                            reentrant: p.reentrant,
                            queued: *captured.queues.get(&p.path).unwrap_or(&0),
                        })
                        .collect(),
                    uses: m
                        .uses
                        .iter()
                        .map(|u| UseView {
                            object: u.object.clone(),
                            interface: u.interface.clone(),
                        })
                        .collect(),
                    subscriptions: m
                        .subscriptions
                        .iter()
                        .map(|s| SubscriptionView {
                            source: s.source.clone(),
                            event: s.event.clone(),
                            interface: s.interface.clone(),
                        })
                        .collect(),
                }
            })
            .collect();
        modules.sort_by(|a, b| a.object.cmp(&b.object));
        let mut hosts = BTreeMap::new();
        for (object, path, reentrant) in natives {
            hosts
                .entry(object.clone())
                .or_insert_with(|| HostSnapshot {
                    object,
                    interfaces: vec![],
                    events: vec![],
                })
                .interfaces
                .push(InterfaceView {
                    path,
                    reentrant,
                    queued: 0,
                });
        }
        for (object, event) in events {
            hosts
                .entry(object.clone())
                .or_insert_with(|| HostSnapshot {
                    object,
                    interfaces: vec![],
                    events: vec![],
                })
                .events
                .push(event);
        }
        let hosts = hosts
            .into_values()
            .map(|mut h| {
                h.interfaces.sort_by(|a, b| a.path.cmp(&b.path));
                h.events.sort();
                h
            })
            .collect();
        Ok(RuntimeSnapshot {
            sampled_at_ms: timestamp(),
            parallelism: core.options.parallelism,
            memory_limit_bytes: core.options.memory_bytes,
            modules,
            hosts,
        })
    }
    pub async fn config_entities(&self) -> std::result::Result<Vec<Token>, InspectError> {
        Ok(self
            .host()?
            .services
            .config
            .read(|view| view.entities())
            .await?)
    }
    pub async fn config_keys(
        &self,
        object: Token,
    ) -> std::result::Result<Vec<forest_config::EntryHeader>, InspectError> {
        Ok(self
            .host()?
            .services
            .config
            .read(move |view| view.headers(&object))
            .await?)
    }
    pub async fn config_entry(
        &self,
        object: Token,
        key: Token,
    ) -> std::result::Result<forest_config::Entry, InspectError> {
        Ok(self.host()?.services.config.entry(&object, &key).await?)
    }
}
fn timestamp() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock before Unix epoch")
        .as_millis() as u64
}
