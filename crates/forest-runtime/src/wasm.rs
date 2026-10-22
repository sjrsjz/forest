use crate::{
    core::{Core, HostAccess, Instance, ModulePhase},
    ready::TaskSignal,
    services::Resources,
};
use anyhow::{Context, Result, ensure};
use forest_protocol::{
    abi::{self, Step},
    service::*,
    *,
};
use forest_util::acl::Access;
use serde::de::DeserializeOwned;
use std::sync::{Arc, atomic::Ordering};
use wasmtime::{Caller, Memory, Store, TypedFunc, UpdateDeadline};

pub(crate) struct Frame {
    pub signal: Arc<TaskSignal>,
    pub context: CallContext,
}
struct HostState {
    instance: Arc<Instance>,
    resources: Arc<Resources>,
    frame: Frame,
    limits: wasmtime::StoreLimits,
}

pub(crate) struct Wasm {
    run_queue: Arc<crate::fair::RunQueue>,
    store: Store<HostState>,
    memory: Memory,
    alloc: TypedFunc<u32, u32>,
    free: TypedFunc<(u32, u32), ()>,
    create: TypedFunc<(u64, u32, u32), ()>,
    invoke: TypedFunc<(u64, u32, u32), ()>,
    poll: TypedFunc<u64, u64>,
    destroy: TypedFunc<u64, ()>,
}
pub(crate) enum Begin<'a> {
    Create(&'a [u8]),
    Invoke(&'a [u8]),
    Resume,
    Destroy,
}

impl Wasm {
    pub async fn instantiate(
        core: &Core,
        instance: Arc<Instance>,
        resources: Arc<Resources>,
        frame: Frame,
    ) -> Result<Self> {
        let limits = wasmtime::StoreLimitsBuilder::new()
            .memory_size(core.options.memory_bytes)
            .instances(1)
            .memories(1)
            .build();
        let mut store = Store::new(
            &core.engine,
            HostState {
                instance: instance.clone(),
                resources,
                frame,
                limits,
            },
        );
        store.limiter(|state| &mut state.limits);
        store.set_epoch_deadline(1);
        store.epoch_deadline_callback(|context| {
            if context.data().instance.force.load(Ordering::Acquire) {
                anyhow::bail!("externally interrupted");
            }
            Ok(UpdateDeadline::Yield(1))
        });
        let linker = imports(&core.engine)?;
        let guest = core
            .run_queue
            .drive(
                instance.id,
                linker.instantiate_async(&mut store, &instance.package.module),
            )
            .await?;
        Ok(Self {
            run_queue: core.run_queue.clone(),
            memory: guest
                .get_memory(&mut store, "memory")
                .context("ABI memory")?,
            alloc: guest.get_typed_func(&mut store, "forest_alloc")?,
            free: guest.get_typed_func(&mut store, "forest_free")?,
            create: guest.get_typed_func(&mut store, "forest_create")?,
            invoke: guest.get_typed_func(&mut store, "forest_invoke")?,
            poll: guest.get_typed_func(&mut store, "forest_poll")?,
            destroy: guest.get_typed_func(&mut store, "forest_destroy")?,
            store,
        })
    }
    fn check_force(&self) -> Result<()> {
        ensure!(
            !self.store.data().instance.force.load(Ordering::Acquire),
            "externally interrupted"
        );
        Ok(())
    }
    pub async fn advance(&mut self, frame: Frame, begin: Begin<'_>) -> Result<Step> {
        let queue = self.run_queue.clone();
        queue
            .drive(
                self.store.data().instance.id,
                self.advance_inner(frame, begin),
            )
            .await
    }
    async fn advance_inner(&mut self, frame: Frame, begin: Begin<'_>) -> Result<Step> {
        self.check_force()?;
        let task = frame.signal.id;
        self.store.data_mut().frame = frame;
        match begin {
            Begin::Create(bytes) | Begin::Invoke(bytes) => {
                let ptr = self
                    .alloc
                    .call_async(&mut self.store, bytes.len() as u32)
                    .await?;
                self.check_force()?;
                self.memory.write(&mut self.store, ptr as usize, bytes)?;
                let function = if matches!(begin, Begin::Create(_)) {
                    &self.create
                } else {
                    &self.invoke
                };
                function
                    .call_async(&mut self.store, (task, ptr, bytes.len() as u32))
                    .await?;
                self.check_force()?;
                self.free
                    .call_async(&mut self.store, (ptr, bytes.len() as u32))
                    .await?;
            }
            Begin::Destroy => {
                self.destroy.call_async(&mut self.store, task).await?;
            }
            Begin::Resume => {}
        }
        self.check_force()?;
        let output = self.poll.call_async(&mut self.store, task).await?;
        self.check_force()?;
        let (ptr, len) = abi::split_buffer(output);
        ensure!(len as usize <= abi::MAX_FRAME_BYTES, "ABI output too large");
        let mut bytes = vec![0; len as usize];
        self.memory.read(&self.store, ptr as usize, &mut bytes)?;
        let step = abi::decode(&bytes)?;
        self.check_force()?;
        self.free.call_async(&mut self.store, (ptr, len)).await?;
        self.check_force()?;
        Ok(step)
    }
}

#[derive(Clone, Copy)]
enum Import {
    Call,
    Send,
    ConfigGet,
    ConfigSet,
    ConfigReset,
    Http,
    WsConnect,
    WsSend,
    WsReceive,
    WsClose,
    FileRead,
    FileWrite,
    FileRemove,
    Sqlite,
    Sleep,
    Log,
    Now,
}

fn imports(engine: &wasmtime::Engine) -> Result<wasmtime::Linker<HostState>> {
    let mut linker = wasmtime::Linker::new(engine);
    for (name, operation) in [
        ("call", Import::Call),
        ("send", Import::Send),
        ("config_get", Import::ConfigGet),
        ("config_set", Import::ConfigSet),
        ("config_reset", Import::ConfigReset),
        ("http_request", Import::Http),
        ("ws_connect", Import::WsConnect),
        ("ws_send", Import::WsSend),
        ("ws_receive", Import::WsReceive),
        ("ws_close", Import::WsClose),
        ("file_read", Import::FileRead),
        ("file_write", Import::FileWrite),
        ("file_remove", Import::FileRemove),
        ("sqlite", Import::Sqlite),
        ("sleep", Import::Sleep),
        ("log", Import::Log),
        ("now", Import::Now),
    ] {
        linker.func_wrap(
            "forest_host",
            name,
            move |mut caller: Caller<'_, HostState>, ptr: u32, len: u32| -> Result<u64> {
                submit(&mut caller, operation, ptr, len)
            },
        )?;
    }
    linker.func_wrap(
        "forest_host",
        "operation_status",
        |caller: Caller<'_, HostState>, id: u64| -> Result<i64> {
            ensure!(
                !caller.data().instance.force.load(Ordering::Acquire),
                "externally interrupted"
            );
            caller
                .data()
                .instance
                .operations
                .status(caller.data().frame.signal.id, id)
        },
    )?;
    linker.func_wrap(
        "forest_host",
        "operation_take",
        |mut caller: Caller<'_, HostState>, id: u64, ptr: u32, len: u32| -> Result<()> {
            ensure!(
                !caller.data().instance.force.load(Ordering::Acquire),
                "externally interrupted"
            );
            let instance = caller.data().instance.clone();
            let task = caller.data().frame.signal.id;
            let memory = caller
                .get_export("memory")
                .and_then(|e| e.into_memory())
                .context("ABI memory")?;
            instance.operations.take(task, id, |bytes| {
                ensure!(
                    bytes.len() == len as usize,
                    "incorrect operation result buffer length"
                );
                memory.write(&mut caller, ptr as usize, bytes)?;
                Ok(())
            })
        },
    )?;
    linker.func_wrap(
        "forest_host",
        "operation_release",
        |caller: Caller<'_, HostState>, id: u64| -> Result<()> {
            let state = caller.data();
            let core = state.instance.core.upgrade().context("host stopped")?;
            let cleanup =
                core.permission(&state.instance, HostAccess::Release)? == ModulePhase::Destroying;
            state
                .instance
                .operations
                .release(state.frame.signal.id, id, cleanup)
        },
    )?;
    linker.func_wrap(
        "forest_host",
        "task_wake",
        |caller: Caller<'_, HostState>, id: u64| {
            caller.data().instance.mailbox.wake(id);
        },
    )?;
    Ok(linker)
}

fn read<T: DeserializeOwned>(caller: &mut Caller<'_, HostState>, ptr: u32, len: u32) -> Result<T> {
    ensure!(len as usize <= abi::MAX_FRAME_BYTES, "ABI input too large");
    let memory = caller
        .get_export("memory")
        .and_then(|e| e.into_memory())
        .context("ABI memory")?;
    let mut bytes = vec![0; len as usize];
    memory.read(&*caller, ptr as usize, &mut bytes)?;
    Ok(abi::decode(&bytes)?)
}

fn submit(
    caller: &mut Caller<'_, HostState>,
    operation: Import,
    ptr: u32,
    len: u32,
) -> Result<u64> {
    let core = caller
        .data()
        .instance
        .core
        .upgrade()
        .context("host stopped")?;
    let instance = caller.data().instance.clone();
    let resources = caller.data().resources.clone();
    let signal = caller.data().frame.signal.clone();
    let origin = caller.data().frame.context.origin.clone();
    macro_rules! launch {
        ($access:expr,$future:expr) => {
            core.submit(&instance, &signal, $access, $future)
        };
    }
    Ok(match operation {
        Import::Call => {
            let input: CallRequest = read(caller, ptr, len)?;
            let service = core.clone();
            let source = instance.clone();
            let context = CallContext {
                origin,
                caller: instance.object.clone(),
            };
            launch!(HostAccess::Communication, async move {
                service.call(context, Some(&source), input).await
            })
        }
        Import::Send => {
            let input: CallRequest = read(caller, ptr, len)?;
            let service = core.clone();
            let source = instance.clone();
            let context = CallContext {
                origin,
                caller: instance.object.clone(),
            };
            launch!(HostAccess::Communication, async move {
                service.send(context, Some(&source), input)
            })
        }
        Import::ConfigGet => {
            let input: ConfigKey = read(caller, ptr, len)?;
            let auth = input.clone();
            let service = core.services.config.clone();
            launch!(HostAccess::Configuration(&auth, Access::Read), async move {
                Ok(service
                    .entry(&input.object, &input.key)
                    .await
                    .map(|e| e.value().clone())
                    .map_err(|e| ServiceError::Storage(e.to_string())))
            })
        }
        Import::ConfigSet => {
            let input: ConfigSet = read(caller, ptr, len)?;
            let auth = input.target.clone();
            let service = core.clone();
            launch!(
                HostAccess::Configuration(&auth, Access::Write),
                async move {
                    Ok(service
                        .services
                        .config_set(input)
                        .await
                        .map_err(|e| ServiceError::Storage(e.to_string())))
                }
            )
        }
        Import::ConfigReset => {
            let input: ConfigKey = read(caller, ptr, len)?;
            let auth = input.clone();
            let service = core.services.config.clone();
            launch!(
                HostAccess::Configuration(&auth, Access::Write),
                async move {
                    Ok(service
                        .reset(&input.object, &input.key)
                        .await
                        .map_err(|e| ServiceError::Storage(e.to_string())))
                }
            )
        }
        Import::Http => {
            let input: HttpRequest = read(caller, ptr, len)?;
            let service = core.clone();
            launch!(HostAccess::Network, async move {
                Ok(service.services.http(input).await)
            })
        }
        Import::WsConnect => {
            let input: WebSocketRequest = read(caller, ptr, len)?;
            launch!(HostAccess::Network, async move {
                Ok(resources.ws_connect(input).await)
            })
        }
        Import::WsSend => {
            let input: WebSocketSend = read(caller, ptr, len)?;
            launch!(HostAccess::Network, async move {
                Ok(resources.ws_send(input).await)
            })
        }
        Import::WsReceive => {
            let input: u64 = read(caller, ptr, len)?;
            launch!(HostAccess::Network, async move {
                Ok(resources.ws_receive(input).await)
            })
        }
        Import::WsClose => {
            let input: u64 = read(caller, ptr, len)?;
            launch!(HostAccess::Release, async move {
                Ok(resources.ws_close(input).await)
            })
        }
        Import::FileRead => {
            let input: String = read(caller, ptr, len)?;
            launch!(HostAccess::File, async move {
                Ok(resources.file_read(input).await)
            })
        }
        Import::FileWrite => {
            let input: FileWrite = read(caller, ptr, len)?;
            launch!(HostAccess::File, async move {
                Ok(resources.file_write(input).await)
            })
        }
        Import::FileRemove => {
            let input: String = read(caller, ptr, len)?;
            launch!(HostAccess::File, async move {
                Ok(resources.file_remove(input).await)
            })
        }
        Import::Sqlite => {
            let input: Vec<SqlStatement> = read(caller, ptr, len)?;
            launch!(HostAccess::Database, async move {
                Ok(resources.database.execute(input).await)
            })
        }
        Import::Sleep => {
            let input: i64 = read(caller, ptr, len)?;
            launch!(HostAccess::Wait, async move {
                if input < 0 {
                    return Err(DispatchError::InputContract);
                }
                tokio::time::sleep(std::time::Duration::from_millis(input as u64)).await;
                Ok(())
            })
        }
        Import::Log => {
            let input: LogRecord = read(caller, ptr, len)?;
            let object = instance.object.clone();
            launch!(HostAccess::Basic, async move {
                eprintln!("[{:?}] {}: {}", input.level, object, input.message);
                Ok(())
            })
        }
        Import::Now => {
            let _: () = read(caller, ptr, len)?;
            launch!(HostAccess::Basic, async move {
                Ok(std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .expect("system clock before Unix epoch")
                    .as_millis() as i64)
            })
        }
    })
}
