use crate::{InstanceFailure, LifecyclePhase};
use crate::{
    core::{Core, Instance, Started, StopMode},
    ready::TaskSignal,
    services::Resources,
    wasm::{Begin, Frame, Wasm},
};
use forest_protocol::{
    abi::{self, Step},
    *,
};
use std::{
    collections::HashMap,
    sync::{Arc, atomic::Ordering},
};
use tokio::sync::oneshot;

pub(crate) enum Activation {
    Serve,
    Discard,
}
pub(crate) struct PreparedDriver {
    pub ready: oneshot::Receiver<Result<(), InstanceFailure>>,
    pub activate: oneshot::Sender<Activation>,
}

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
    error: InstanceFailure,
    tasks: Vec<GuestTask>,
}
impl From<InstanceFailure> for Fault {
    fn from(error: InstanceFailure) -> Self {
        Self {
            error,
            tasks: vec![],
        }
    }
}

pub(crate) fn spawn(core: Arc<Core>, instance: Arc<Instance>) -> PreparedDriver {
    let (ready, receive) = oneshot::channel();
    let (activate, activation) = oneshot::channel();
    tokio::spawn(async move {
        let mut ready = Some(ready);
        let resources = core.services.mount(&instance.package.manifest).await;
        let result = match &resources {
            Ok(resources) => {
                run_guest(&core, &instance, resources.clone(), &mut ready, activation).await
            }
            Err(error) => Err(Fault {
                error: DispatchError::ExecutionFailed(error.to_string()).into(),
                tasks: vec![],
            }),
        };
        let (error, retired) = match result {
            Ok(()) => (None, vec![]),
            Err(Fault { error, tasks }) => (
                Some(if instance.force.load(Ordering::Acquire) {
                    DispatchError::Interrupted.into()
                } else {
                    error
                }),
                tasks,
            ),
        };
        if let Some(error) = &error {
            let level = if matches!(
                error,
                InstanceFailure::Dispatch {
                    error: DispatchError::Interrupted
                }
            ) {
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
        if let Some(ready) = ready.take() {
            let _ = ready.send(Err(error
                .clone()
                .unwrap_or_else(|| DispatchError::Interrupted.into())));
        }
        let error = error
            .unwrap_or_else(|| DispatchError::Interrupted.into())
            .dispatch_error(&instance.object);
        let mut held = Vec::new();
        for task in retired {
            instance.mailbox.retire(task.signal.id);
            let Started { request, .. } = task.started;
            let crate::core::Request {
                outcome, admission, ..
            } = request;
            outcome.settle(Err(error.clone()));
            held.push(admission);
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
    PreparedDriver {
        ready: receive,
        activate,
    }
}

async fn run_guest(
    core: &Arc<Core>,
    instance: &Arc<Instance>,
    resources: Arc<Resources>,
    ready: &mut Option<oneshot::Sender<Result<(), InstanceFailure>>>,
    mut activation: oneshot::Receiver<Activation>,
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
        .map_err(Fault::from)?;
    instance.operations.finish_task(initial.id).await;
    instance.mailbox.retire(initial.id);
    let _ = ready
        .take()
        .expect("initialization completion")
        .send(Ok(()));
    let decision = loop {
        let notified = instance.mailbox.changed.notified();
        if instance.force.load(Ordering::Acquire) {
            return Err(Fault::from(InstanceFailure::from(
                DispatchError::Interrupted,
            )));
        }
        tokio::select! {
            decision = &mut activation => break decision.unwrap_or(Activation::Discard),
            _ = notified => {}
        }
    };
    if matches!(decision, Activation::Discard) {
        core.abort_creation(instance);
        let cleanup = instance.mailbox.register(1);
        let result = lifecycle(&mut wasm, instance, cleanup.clone(), Begin::Destroy).await;
        instance.mailbox.retire(cleanup.id);
        return result.map_err(Fault::from);
    }

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
            Ok(Step::FrameTooLarge) => {
                instance.operations.finish_task(task.signal.id).await;
                instance.mailbox.retire(task.signal.id);
                core.complete(instance, task.started, Err(DispatchError::FrameTooLarge));
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
                    error: error.into(),
                    tasks: tasks.into_values().collect(),
                });
            }
        }
        tokio::task::yield_now().await;
    };

    if stop == StopMode::Force || instance.force.load(Ordering::Acquire) {
        return Err(Fault {
            error: DispatchError::Interrupted.into(),
            tasks: tasks.into_values().collect(),
        });
    }
    let mut held = Vec::new();
    for (_, task) in tasks {
        instance.mailbox.retire(task.signal.id);
        let Started { request, .. } = task.started;
        let crate::core::Request {
            outcome, admission, ..
        } = request;
        outcome.settle(Err(DispatchError::Interrupted));
        held.push(admission);
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
    result.map_err(Fault::from)
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
) -> std::result::Result<(), InstanceFailure> {
    let phase = match &begin {
        Begin::Create(_) => LifecyclePhase::Create,
        Begin::Destroy => LifecyclePhase::Destroy,
        _ => unreachable!("lifecycle begins with create or destroy"),
    };
    instance.mailbox.consume_wake(signal.id);
    let mut step = wasm
        .advance(own_frame(instance, signal.clone()), begin)
        .await
        .map_err(|error| fault(error).error)?;
    loop {
        match step {
            Step::Lifecycle(Ok(())) => return Ok(()),
            Step::Lifecycle(Err(error)) => {
                let schema = &instance.package.manifest.lifecycle_error;
                if !error.check(schema, false) {
                    return Err(DispatchError::OutputContract.into());
                }
                return Err(InstanceFailure::Lifecycle {
                    phase,
                    schema: schema.clone(),
                    error,
                });
            }
            Step::Output(_) => return Err(DispatchError::OutputContract.into()),
            Step::FrameTooLarge => return Err(DispatchError::FrameTooLarge.into()),
            Step::Pending => {}
        }
        loop {
            let notified = instance.mailbox.changed.notified();
            if instance.force.load(Ordering::Acquire) {
                return Err(DispatchError::Interrupted.into());
            }
            if instance.mailbox.contains_wake(signal.id) {
                break;
            }
            notified.await;
        }
        instance.mailbox.consume_wake(signal.id);
        step = wasm
            .advance(own_frame(instance, signal.clone()), Begin::Resume)
            .await
            .map_err(|error| fault(error).error)?;
    }
}
fn fault(error: impl std::fmt::Display) -> Fault {
    Fault {
        error: DispatchError::ExecutionFailed(error.to_string()).into(),
        tasks: vec![],
    }
}
