mod api;
mod dispatch;
pub(crate) mod inspection;
mod lifecycle;

use crate::{
    Package, operations::Operations, package::AccessTables, ready::Mailbox, services::Services,
};
use anyhow::{Context, Result, ensure};
use forest_protocol::{abi, manifest::equivalent, *};
use forest_util::acl::{Access, AclEntry, AclTable};
use std::{
    collections::{HashMap, VecDeque},
    future::Future,
    path::{Path, PathBuf},
    pin::Pin,
    sync::{
        Arc, Mutex, Weak,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
};
use tokio::sync::{OwnedSemaphorePermit, Semaphore, oneshot};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ModulePhase {
    Creating,
    Serving,
    Stopping,
    Destroying,
    Releasing,
    Failed,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub enum StopMode {
    Drain,
    AtYield,
    Force,
}

pub(crate) enum HostAccess<'a> {
    Communication,
    Configuration(&'a forest_protocol::service::ConfigKey, Access),
    Network,
    File,
    Release,
    Wait,
    Basic,
}
impl HostAccess<'_> {
    fn cancellable(&self) -> bool {
        !matches!(self, Self::Configuration(..) | Self::File)
    }
}

#[derive(Clone, Debug)]
pub struct RuntimeOptions {
    pub parallelism: usize,
    pub max_requests: usize,
    pub max_input_bytes: usize,
    pub memory_bytes: usize,
    pub data_directory: PathBuf,
    pub http_proxy: Option<String>,
}
impl Default for RuntimeOptions {
    fn default() -> Self {
        Self {
            parallelism: 2,
            max_requests: 1024,
            max_input_bytes: 64 * 1024 * 1024,
            memory_bytes: 256 * 1024 * 1024,
            data_directory: PathBuf::from("data/modules"),
            http_proxy: None,
        }
    }
}

type NativeFuture = Pin<Box<dyn Future<Output = Value> + Send>>;
pub struct NativeInterface {
    object: Token,
    declaration: InterfaceDeclaration,
    handler: Arc<dyn Fn(CallContext, Value) -> NativeFuture + Send + Sync>,
}
impl NativeInterface {
    pub fn new<I, T, E, F, Fut>(
        object: Token,
        path: Token,
        reentrant: bool,
        access: AccessPolicy,
        handler: F,
    ) -> Self
    where
        I: AsSchema + AsValue,
        T: AsSchema + AsValue,
        E: AsSchema + AsValue,
        F: Fn(CallContext, I) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<T, E>> + Send + 'static,
    {
        Self {
            object,
            declaration: InterfaceDeclaration {
                path,
                input: I::as_schema(),
                output: <Result<T, E>>::as_schema(),
                reentrant,
                access,
            },
            handler: Arc::new(move |context, value| {
                let input = I::from_value(value).expect("validated native input");
                let future = handler(context, input);
                Box::pin(async move { future.await.as_value() })
            }),
        }
    }
}
struct RegisteredNative {
    object: Token,
    declaration: InterfaceDeclaration,
    handler: Arc<dyn Fn(CallContext, Value) -> NativeFuture + Send + Sync>,
    access: AclTable,
    gate: NativeGate,
}
enum NativeGate {
    Parallel,
    Serial(Arc<Semaphore>),
}
#[derive(Clone)]
pub struct EventDefinition {
    pub source: Token,
    pub event: Token,
    pub input: Schema,
}

#[derive(Clone)]
pub struct Scheduler {
    pub(crate) core: Arc<Core>,
}
pub(crate) struct Core {
    pub engine: wasmtime::Engine,
    pub options: RuntimeOptions,
    pub services: Services,
    pub directory: Mutex<Directory>,
    pub run_queue: Arc<crate::fair::RunQueue>,
    next_instance: AtomicU64,
    _ticker: EpochTicker,
}
pub(crate) struct Directory {
    pub slots: HashMap<Token, Slot>,
    natives: HashMap<(Token, Token), Arc<RegisteredNative>>,
    native_budgets: HashMap<Token, Arc<Budget>>,
    events: HashMap<(Token, Token), EventDefinition>,
}
pub(crate) struct Slot {
    pub published: Arc<Manifest>,
    pub access: Arc<AccessTables>,
    pub budget: Arc<Budget>,
    pub state: State,
}
impl Directory {
    pub fn has_native(&self, object: &Token, interface: &Token) -> bool {
        self.natives
            .contains_key(&(object.clone(), interface.clone()))
    }
}
pub(crate) enum State {
    Creating {
        instance: Arc<Instance>,
        inbox: CreatingInbox,
        completion: ManagementReply,
    },
    Serving(Live),
    Stopping {
        live: Live,
        mode: StopMode,
        after: After,
    },
    Destroying {
        instance: Arc<Instance>,
        after: After,
    },
    Releasing {
        instance: Arc<Instance>,
        after: After,
    },
    Failed(String),
}
pub(crate) struct Live {
    pub instance: Arc<Instance>,
    pub interfaces: HashMap<Token, Inbox>,
}
pub(crate) struct Inbox {
    pub queue: VecDeque<Request>,
    pub gate: Gate,
}
pub(crate) enum Gate {
    Parallel,
    Serial(Option<SerialPermit>),
}
pub(crate) struct SerialPermit {
    interface: Token,
}
pub(crate) enum InterfacePermit {
    Parallel,
    Serial(SerialPermit),
}
pub(crate) enum After {
    Unload(ManagementReply),
    Reload {
        package: Package,
        inbox: VecDeque<Request>,
        completion: ManagementReply,
    },
    Fail(String),
}
type ManagementReply = oneshot::Sender<std::result::Result<(), String>>;
pub(crate) enum CreatingInbox {
    Closed,
    Deferred(VecDeque<Request>),
}

pub(crate) struct Instance {
    pub id: u64,
    pub object: Token,
    pub package: Package,
    pub core: Weak<Core>,
    pub mailbox: Arc<Mailbox>,
    pub operations: Operations,
    pub force: AtomicBool,
}
pub(crate) struct Budget {
    requests: Arc<Semaphore>,
    bytes: Arc<Semaphore>,
}
pub(crate) struct Admission {
    _request: OwnedSemaphorePermit,
    _bytes: OwnedSemaphorePermit,
}
pub(crate) struct Request {
    pub invocation: Invocation,
    pub bytes: Vec<u8>,
    pub output: Schema,
    pub admission: Admission,
    pub outcome: Outcome,
}
pub(crate) enum Outcome {
    Call(oneshot::Sender<DispatchResult<Value>>),
    Send,
}
pub(crate) struct Started {
    pub request: Request,
    pub permit: InterfacePermit,
}

impl Outcome {
    pub fn settle(self, result: DispatchResult<Value>) {
        match self {
            Self::Call(reply) => {
                let _ = reply.send(result);
            }
            Self::Send => {
                if let Err(error) = result {
                    eprintln!("send failed after acceptance: {error}");
                }
            }
        }
    }
}
impl Budget {
    fn new(options: &RuntimeOptions) -> Self {
        Self {
            requests: Arc::new(Semaphore::new(options.max_requests)),
            bytes: Arc::new(Semaphore::new(options.max_input_bytes)),
        }
    }
    fn reserve(&self, bytes: usize) -> DispatchResult<Admission> {
        let request = self
            .requests
            .clone()
            .try_acquire_owned()
            .map_err(|_| DispatchError::QueueFull)?;
        let bytes = self
            .bytes
            .clone()
            .try_acquire_many_owned(bytes.try_into().map_err(|_| DispatchError::QueueFull)?)
            .map_err(|_| DispatchError::QueueFull)?;
        Ok(Admission {
            _request: request,
            _bytes: bytes,
        })
    }
}
impl State {
    pub fn phase(&self) -> ModulePhase {
        match self {
            Self::Creating { .. } => ModulePhase::Creating,
            Self::Serving(_) => ModulePhase::Serving,
            Self::Stopping { .. } => ModulePhase::Stopping,
            Self::Destroying { .. } => ModulePhase::Destroying,
            Self::Releasing { .. } => ModulePhase::Releasing,
            Self::Failed(_) => ModulePhase::Failed,
        }
    }
    pub fn instance(&self) -> Option<&Arc<Instance>> {
        match self {
            Self::Creating { instance, .. }
            | Self::Destroying { instance, .. }
            | Self::Releasing { instance, .. } => Some(instance),
            Self::Serving(live) | Self::Stopping { live, .. } => Some(&live.instance),
            Self::Failed(_) => None,
        }
    }
}

impl Live {
    pub fn new(instance: Arc<Instance>) -> Self {
        let interfaces = instance
            .package
            .manifest
            .provides
            .iter()
            .map(|d| {
                (
                    d.path.clone(),
                    Inbox {
                        queue: VecDeque::new(),
                        gate: if d.reentrant {
                            Gate::Parallel
                        } else {
                            Gate::Serial(Some(SerialPermit {
                                interface: d.path.clone(),
                            }))
                        },
                    },
                )
            })
            .collect();
        Self {
            instance,
            interfaces,
        }
    }
    pub fn enqueue(&mut self, request: Request) {
        let interface = request.invocation.interface.clone();
        let inbox = self
            .interfaces
            .get_mut(&interface)
            .expect("validated interface");
        inbox.queue.push_back(request);
        if matches!(inbox.gate, Gate::Parallel | Gate::Serial(Some(_))) {
            self.instance.mailbox.start(interface);
        }
    }
    pub fn begin(&mut self, interface: &Token) -> Started {
        let inbox = self.interfaces.get_mut(interface).expect("ready interface");
        let permit = match &mut inbox.gate {
            Gate::Parallel => InterfacePermit::Parallel,
            Gate::Serial(permit) => {
                InterfacePermit::Serial(permit.take().expect("ready interface permit"))
            }
        };
        let request = inbox.queue.pop_front().expect("ready interface queue");
        if !inbox.queue.is_empty() && matches!(inbox.gate, Gate::Parallel) {
            self.instance.mailbox.start(interface.clone());
        }
        Started { request, permit }
    }
    pub fn complete(&mut self, permit: InterfacePermit) {
        if let InterfacePermit::Serial(permit) = permit {
            let interface = permit.interface.clone();
            let inbox = self.interfaces.get_mut(&interface).unwrap();
            let Gate::Serial(slot) = &mut inbox.gate else {
                unreachable!()
            };
            assert!(slot.replace(permit).is_none());
            if !inbox.queue.is_empty() {
                self.instance.mailbox.start(interface);
            }
        }
    }
    pub fn pending(&self) -> bool {
        self.interfaces
            .values()
            .any(|inbox| !inbox.queue.is_empty())
    }
    pub fn reject_queued(self, error: &DispatchError) {
        for inbox in self.interfaces.into_values() {
            for request in inbox.queue {
                request.outcome.settle(Err(error.clone()));
            }
        }
    }
}
struct EpochTicker {
    stop: Arc<AtomicBool>,
    thread: Option<std::thread::JoinHandle<()>>,
}
impl EpochTicker {
    fn start(engine: wasmtime::Engine) -> std::io::Result<Self> {
        let stop = Arc::new(AtomicBool::new(false));
        let signal = stop.clone();
        let thread = std::thread::Builder::new()
            .name("forest-epoch".into())
            .spawn(move || {
                while !signal.load(Ordering::Acquire) {
                    std::thread::sleep(std::time::Duration::from_millis(5));
                    engine.increment_epoch();
                }
            })?;
        Ok(Self {
            stop,
            thread: Some(thread),
        })
    }
}
impl Drop for EpochTicker {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Release);
        self.thread
            .take()
            .unwrap()
            .join()
            .expect("epoch ticker panicked");
    }
}
