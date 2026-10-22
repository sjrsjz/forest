use crate::{
    core::{Core, Instance, Started, StopMode},
    ready::TaskSignal,
    services::Resources,
    wasm::{Begin, Frame, Wasm},
};
use anyhow::{Result, bail, ensure};
use forest_protocol::{
    abi::{self, Step},
    *,
};
use std::{
    collections::HashMap,
    sync::{Arc, atomic::Ordering},
};

pub(crate) enum Turn {
    Start(Box<Started>),
    Resume(u64),
    Wait,
    Stop(StopMode),
}
struct GuestTask {
    signal: Arc<TaskSignal>,
    started: Started,
}
struct Fault {
    error: DispatchError,
    tasks: Vec<GuestTask>,
}

pub(crate) fn spawn(core: Arc<Core>, instance: Arc<Instance>) {
    tokio::spawn(async move {
        let resources = core.services.mount(&instance.package.manifest).await;
        let result = match &resources {
            Ok(resources) => run_guest(&core, &instance, resources.clone()).await,
            Err(error) => Err(Fault {
                error: DispatchError::ExecutionFailed(error.to_string()),
                tasks: vec![],
            }),
        };
        let (error, retired) = match result {
            Ok(()) => (None, vec![]),
            Err(Fault { error, tasks }) => (
                Some(if instance.force.load(Ordering::Acquire) {
                    DispatchError::Interrupted
                } else {
                    error
                }),
                tasks,
            ),
        };
        if let Some(error) = &error {
            let level = if matches!(error, DispatchError::Interrupted) {
                service::LogLevel::Info
            } else {
                service::LogLevel::Error
            };
            core.logs.write(
                level,
                instance.object.clone(),
                format!("instance {} stopped: {error}", instance.id),
            );
        }
        core.begin_releasing(&instance, error.clone());
        let error = error.unwrap_or(DispatchError::Interrupted);
        let mut held = Vec::new();
        for task in retired {
            instance.mailbox.retire(task.signal.id);
            let Started {
                request, permit, ..
            } = task.started;
            let crate::core::Request {
                outcome, admission, ..
            } = request;
            outcome.settle(Err(error.clone()));
            held.push((admission, permit));
        }
        instance.operations.finish().await;
        if let Ok(resources) = resources {
            resources.close();
        }
        drop(held);
        core.finish_releasing(&instance);
        core.logs.write(
            service::LogLevel::Info,
            instance.object.clone(),
            format!("instance {} released", instance.id),
        );
    });
}

async fn run_guest(
    core: &Arc<Core>,
    instance: &Arc<Instance>,
    resources: Arc<Resources>,
) -> std::result::Result<(), Fault> {
    let initial = instance.mailbox.register(0);
    let frame = own_frame(instance, initial.clone());
    let mut wasm = Wasm::instantiate(core, instance.clone(), resources, frame)
        .await
        .map_err(fault)?;
    let input = abi::encode(&CreateContext {
        object: instance.object.clone(),
    })
    .map_err(fault)?;
    lifecycle(&mut wasm, instance, initial.clone(), Begin::Create(&input))
        .await
        .map_err(fault)?;
    instance.operations.finish_task(initial.id).await;
    instance.mailbox.retire(initial.id);
    core.activate(instance).map_err(fault)?;

    let mut next = 1u64;
    let mut tasks: HashMap<u64, GuestTask> = HashMap::new();
    let stop = loop {
        let notified = instance.mailbox.changed.notified();
        let (task, fresh) = match core.turn(instance, tasks.is_empty()) {
            Turn::Wait => {
                notified.await;
                continue;
            }
            Turn::Stop(mode) => break mode,
            Turn::Start(started) => {
                let signal = instance.mailbox.register(next);
                next = next.checked_add(1).expect("task id exhaustion");
                (
                    GuestTask {
                        signal,
                        started: *started,
                    },
                    TaskEntry::New,
                )
            }
            Turn::Resume(id) => {
                if !core.may_resume(instance) {
                    continue;
                }
                (
                    tasks.remove(&id).expect("live ready task"),
                    TaskEntry::Resume,
                )
            }
        };
        instance.mailbox.consume_wake(task.signal.id);
        let frame = Frame {
            signal: task.signal.clone(),
            context: task.started.request.invocation.context.clone(),
        };
        let begin = match fresh {
            TaskEntry::New => Begin::Invoke(&task.started.request.bytes),
            TaskEntry::Resume => Begin::Resume,
        };
        let result = wasm.advance(frame, begin).await;
        match result {
            Ok(Step::Pending) => {
                tasks.insert(task.signal.id, task);
            }
            Ok(Step::Output(value)) if value.check(&task.started.output, false) => {
                instance.operations.finish_task(task.signal.id).await;
                instance.mailbox.retire(task.signal.id);
                core.complete(instance, task.started, Ok(value));
            }
            other => {
                let error = if instance.force.load(Ordering::Acquire) {
                    DispatchError::Interrupted
                } else {
                    match other {
                        Err(error) => DispatchError::ExecutionFailed(error.to_string()),
                        Ok(_) => DispatchError::OutputContract,
                    }
                };
                tasks.insert(task.signal.id, task);
                return Err(Fault {
                    error,
                    tasks: tasks.into_values().collect(),
                });
            }
        }
        tokio::task::yield_now().await;
    };

    if stop == StopMode::Force || instance.force.load(Ordering::Acquire) {
        return Err(Fault {
            error: DispatchError::Interrupted,
            tasks: tasks.into_values().collect(),
        });
    }
    let mut held = Vec::new();
    for (_, task) in tasks {
        instance.mailbox.retire(task.signal.id);
        let Started {
            request, permit, ..
        } = task.started;
        let crate::core::Request {
            outcome, admission, ..
        } = request;
        outcome.settle(Err(DispatchError::Interrupted));
        held.push((admission, permit));
    }
    core.reject_old_queue(instance, &DispatchError::Interrupted);
    instance.operations.quiesce().await;
    drop(held);
    if stop == StopMode::Force || !core.enter_destroying(instance) {
        return Ok(());
    }
    let cleanup = instance.mailbox.register(next);
    let result = lifecycle(&mut wasm, instance, cleanup.clone(), Begin::Destroy).await;
    instance.mailbox.retire(cleanup.id);
    result.map_err(fault)
}

enum TaskEntry {
    New,
    Resume,
}

fn own_frame(instance: &Instance, signal: Arc<TaskSignal>) -> Frame {
    Frame {
        signal,
        context: CallContext {
            origin: instance.object.clone(),
            caller: instance.object.clone(),
        },
    }
}

async fn lifecycle(
    wasm: &mut Wasm,
    instance: &Instance,
    signal: Arc<TaskSignal>,
    begin: Begin<'_>,
) -> Result<()> {
    instance.mailbox.consume_wake(signal.id);
    let mut step = wasm
        .advance(own_frame(instance, signal.clone()), begin)
        .await?;
    loop {
        match step {
            Step::Lifecycle(result) => return result.map_err(anyhow::Error::msg),
            Step::Output(_) => bail!("lifecycle returned ordinary interface output"),
            Step::Pending => {}
        }
        loop {
            let notified = instance.mailbox.changed.notified();
            ensure!(
                !instance.force.load(Ordering::Acquire),
                "externally interrupted"
            );
            if instance.mailbox.contains_wake(signal.id) {
                break;
            }
            notified.await;
        }
        instance.mailbox.consume_wake(signal.id);
        step = wasm
            .advance(own_frame(instance, signal.clone()), Begin::Resume)
            .await?;
    }
}
fn fault(error: impl std::fmt::Display) -> Fault {
    Fault {
        error: DispatchError::ExecutionFailed(error.to_string()),
        tasks: vec![],
    }
}
