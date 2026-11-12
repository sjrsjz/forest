use forest_protocol::{manifest::callers, *};
use forest_runtime::{ModulePhase, NativeInterface, RuntimeOptions, Scheduler, StopMode};
use std::{
    path::{Path, PathBuf},
    process::Command,
    sync::{Arc, Mutex, OnceLock},
    time::Duration,
};
use tokio::sync::Notify;

struct Artifacts {
    _directory: tempfile::TempDir,
    first: PathBuf,
    second: PathBuf,
}
fn artifacts() -> &'static Artifacts {
    static BUILT: OnceLock<Artifacts> = OnceLock::new();
    BUILT.get_or_init(|| {
        let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("../..")
            .canonicalize()
            .unwrap();
        let directory = tempfile::tempdir().unwrap();
        for (feature, name) in [(None, "first.wasm"), (Some("second"), "second.wasm")] {
            let mut command = Command::new("cargo");
            command.args([
                "build",
                "--offline",
                "--locked",
                "--target",
                "wasm32-unknown-unknown",
                "-p",
                "forest-scheduler-probe",
                "--lib",
            ]);
            if let Some(feature) = feature {
                command.args(["--features", feature]);
            }
            assert!(
                command
                    .current_dir(&root)
                    .env("CARGO_BUILD_JOBS", "2")
                    .status()
                    .unwrap()
                    .success()
            );
            std::fs::copy(
                root.join("target/wasm32-unknown-unknown/debug/forest_scheduler_probe.wasm"),
                directory.path().join(name),
            )
            .unwrap();
        }
        Artifacts {
            first: directory.path().join("first.wasm"),
            second: directory.path().join("second.wasm"),
            _directory: directory,
        }
    })
}
fn package(parent: &Path, name: &str, object: &str, second: bool) -> PathBuf {
    let path = parent.join(name);
    std::fs::create_dir(&path).unwrap();
    let artifacts = artifacts();
    std::fs::copy(
        if second {
            &artifacts.second
        } else {
            &artifacts.first
        },
        path.join("module.wasm"),
    )
    .unwrap();
    let mut manifest = forest_scheduler_probe::manifest(object.parse().unwrap());
    manifest.package.version = if second { "2".into() } else { "1".into() };
    forest_package::write(&path, &manifest).unwrap();
    path
}
#[derive(Default)]
struct Observations {
    values: Mutex<Vec<i64>>,
    changed: Notify,
}
impl Observations {
    fn add(&self, value: i64) {
        self.values.lock().unwrap().push(value);
        self.changed.notify_one();
    }
    fn count(&self) -> usize {
        self.values.lock().unwrap().len()
    }
    async fn until(&self, count: usize) {
        tokio::time::timeout(Duration::from_secs(15), async {
            loop {
                let changed = self.changed.notified();
                if self.count() >= count {
                    return;
                }
                changed.await;
            }
        })
        .await
        .unwrap();
    }
}
async fn call<I: AsValue, T: AsValue>(
    s: &Scheduler,
    object: &str,
    interface: &str,
    input: &I,
) -> T {
    tokio::time::timeout(
        Duration::from_secs(15),
        s.call::<I, Result<T, String>>(
            "/user/test".parse().unwrap(),
            object.parse().unwrap(),
            interface.parse().unwrap(),
            input,
        ),
    )
    .await
    .unwrap()
    .unwrap()
    .unwrap()
}
async fn phase(s: &Scheduler, object: &str, wanted: ModulePhase) {
    tokio::time::timeout(Duration::from_secs(15), async {
        while s.phase(&object.parse().unwrap()) != Some(wanted) {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
}
async fn waiting(s: &Scheduler, object: &str) {
    tokio::time::timeout(Duration::from_secs(15), async {
        loop {
            if s.inspector().snapshot().unwrap().requests.iter().any(|r| {
                r.objects.iter().any(|o| o.to_string() == object)
                    && matches!(r.state, forest_runtime::RequestState::Waiting)
            }) {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
}
fn instance_failure(error: &anyhow::Error) -> &forest_runtime::InstanceFailure {
    error
        .chain()
        .find_map(|cause| cause.downcast_ref())
        .expect("typed instance failure in management error chain")
}
fn observe(s: &Scheduler) -> Arc<Observations> {
    let observations = Arc::new(Observations::default());
    let target = observations.clone();
    s.register_native(NativeInterface::new(
        "/core/probe".parse().unwrap(),
        "/observe".parse().unwrap(),
        callers(vec![SubjectPattern::Path("/module/**".parse().unwrap())]),
        move |_, value: i64| {
            let target = target.clone();
            async move {
                target.add(value);
                Ok::<_, String>(value)
            }
        },
    ))
    .unwrap();
    observations
}

fn gated_observe(
    s: &Scheduler,
) -> (
    tokio::sync::mpsc::UnboundedReceiver<i64>,
    Arc<tokio::sync::Semaphore>,
) {
    let (started, events) = tokio::sync::mpsc::unbounded_channel();
    let gate = Arc::new(tokio::sync::Semaphore::new(0));
    let held = gate.clone();
    s.register_native(NativeInterface::new(
        "/core/probe".parse().unwrap(),
        "/observe".parse().unwrap(),
        callers(vec![SubjectPattern::Path("/module/**".parse().unwrap())]),
        move |_, value: i64| {
            let started = started.clone();
            let held = held.clone();
            async move {
                if value > 0 {
                    started.send(value).unwrap();
                    held.acquire().await.unwrap().forget();
                }
                Ok::<_, String>(value)
            }
        },
    ))
    .unwrap();
    (events, gate)
}

fn peer_package(
    parent: &Path,
    name: &str,
    object: &str,
    peer: &str,
    second: bool,
    create_delay: i64,
) -> PathBuf {
    let path = package(parent, name, object, second);
    let mut manifest = forest_package::read_manifest(&path).unwrap();
    manifest.uses.push(InterfaceUse {
        object: peer.parse().unwrap(),
        interface: "/chain".parse().unwrap(),
        input: <(Token, i64)>::as_schema(),
        output: <Result<i64, String>>::as_schema(),
    });
    manifest
        .configs
        .iter_mut()
        .find(|c| c.key.to_string() == "/lifecycle/create-delay")
        .unwrap()
        .default = create_delay.as_value();
    manifest
        .configs
        .iter_mut()
        .find(|c| c.key.to_string() == "/lifecycle/peer")
        .unwrap()
        .default = forest_scheduler_probe::PeerConfig::Read(peer.parse().unwrap()).as_value();
    manifest
        .configs
        .iter_mut()
        .find(|c| c.key.to_string() == "/counter")
        .unwrap()
        .access
        .readers
        .push(SubjectPattern::Path("/module/**".parse().unwrap()));
    forest_package::write(&path, &manifest).unwrap();
    path
}

#[tokio::test(flavor = "current_thread")]
async fn pending_reload_keeps_ingress_and_cancellation_preserves_the_instance() {
    let temp = tempfile::tempdir().unwrap();
    let scheduler = Scheduler::in_memory(RuntimeOptions {
        data_directory: temp.path().join("data"),
        ..Default::default()
    })
    .await
    .unwrap();
    let (mut entered, gate) = gated_observe(&scheduler);
    let object = "/module/probe-a";
    let first = scheduler
        .package(package(temp.path(), "old", object, false))
        .await
        .unwrap();
    let second = scheduler
        .package(package(temp.path(), "new", object, true))
        .await
        .unwrap();
    scheduler.load(first).await.unwrap();
    let preparing = scheduler
        .request_reload_from(
            vec![forest_runtime::ReloadTarget {
                object: object.parse().unwrap(),
                directory: temp.path().join("new"),
            }],
            StopMode::Drain,
        )
        .unwrap();
    preparing.cancel().unwrap();
    assert!(matches!(
        preparing.wait().await,
        Err(forest_runtime::ManagementFailure::Cancelled)
    ));
    let before = scheduler.inspector().snapshot().unwrap().modules[0]
        .lifecycle
        .clone();
    let active_scheduler = scheduler.clone();
    let active =
        tokio::spawn(
            async move { call::<_, i64>(&active_scheduler, object, "/serial", &0i64).await },
        );
    assert_eq!(entered.recv().await, Some(1));
    let request = scheduler
        .request_reload(vec![second.clone()], StopMode::Drain)
        .unwrap();
    waiting(&scheduler, object).await;
    assert_eq!(
        scheduler.phase(&object.parse().unwrap()),
        Some(ModulePhase::Serving)
    );
    assert_eq!(
        call::<_, i64>(&scheduler, object, "/implementation", &()).await,
        1
    );
    request.cancel().unwrap();
    assert!(matches!(
        request.wait().await,
        Err(forest_runtime::ManagementFailure::Cancelled)
    ));
    let after = scheduler.inspector().snapshot().unwrap().modules[0]
        .lifecycle
        .clone();
    assert_eq!(
        serde_json::to_value(before).unwrap(),
        serde_json::to_value(after).unwrap()
    );
    assert_eq!(
        call::<_, i64>(&scheduler, object, "/destroyed", &()).await,
        0
    );
    gate.add_permits(1);
    assert_eq!(active.await.unwrap(), 1);
    scheduler.reload(second, StopMode::Drain).await.unwrap();
    assert_eq!(
        call::<_, i64>(&scheduler, object, "/implementation", &()).await,
        2
    );
    scheduler.shutdown(StopMode::Drain).await.unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn drain_reload_waits_for_started_lock_waiters() {
    let temp = tempfile::tempdir().unwrap();
    let scheduler = Scheduler::in_memory(RuntimeOptions {
        max_requests: 4,
        data_directory: temp.path().join("data"),
        ..Default::default()
    })
    .await
    .unwrap();
    let (mut entered, gate) = gated_observe(&scheduler);
    let object = "/module/probe-a";
    let mut packages = Vec::new();
    for (name, second) in [("old", false), ("new", true)] {
        let path = package(temp.path(), name, object, second);
        let mut manifest = forest_package::read_manifest(&path).unwrap();
        manifest
            .configs
            .iter_mut()
            .find(|c| c.key.to_string() == "/lifecycle/create-delay")
            .unwrap()
            .default = 300i64.as_value();
        forest_package::write(&path, &manifest).unwrap();
        packages.push(scheduler.package(path).await.unwrap());
    }
    scheduler.load(packages.remove(0)).await.unwrap();
    let spawn_call = || {
        let scheduler = scheduler.clone();
        tokio::spawn(async move { call::<_, i64>(&scheduler, object, "/serial", &0i64).await })
    };
    let send = || {
        scheduler.send(
            "/user/test".parse().unwrap(),
            object.parse().unwrap(),
            "/serial".parse().unwrap(),
            &0i64,
        )
    };
    let active = spawn_call();
    assert_eq!(entered.recv().await, Some(1));
    let first = spawn_call();
    tokio::time::timeout(Duration::from_secs(15), async {
        while scheduler.inspector().snapshot().unwrap().modules[0]
            .budget
            .requests_used
            != 2
        {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    send().unwrap();
    let third = spawn_call();
    let reload = scheduler.request_reload(packages, StopMode::Drain).unwrap();
    waiting(&scheduler, object).await;
    tokio::time::timeout(Duration::from_secs(15), async {
        loop {
            let snapshot = scheduler.inspector().snapshot().unwrap();
            let module = &snapshot.modules[0];
            if module.budget.requests_used == 4 && module.interfaces.iter().all(|i| i.queued == 0) {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert!(matches!(send(), Err(DispatchError::QueueFull)));
    gate.add_permits(1);
    assert_eq!(active.await.unwrap(), 1);
    assert_eq!(entered.recv().await, Some(2));
    assert!(matches!(
        reload.snapshot().state,
        forest_runtime::RequestState::Waiting
    ));
    assert_eq!(
        scheduler.inspector().snapshot().unwrap().modules[0]
            .published
            .version,
        "1"
    );
    // These tasks already entered Wasm and waited on the host lock. They finish
    // on the old instance; none can be migrated as an unstarted request.
    gate.add_permits(3);
    assert_eq!(first.await.unwrap(), 2);
    assert_eq!(third.await.unwrap(), 4);
    assert_eq!(entered.recv().await, Some(3));
    assert_eq!(entered.recv().await, Some(4));
    tokio::time::timeout(Duration::from_secs(15), reload.wait())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        scheduler.inspector().snapshot().unwrap().modules[0]
            .published
            .version,
        "2"
    );
    gate.add_permits(1);
    assert_eq!(
        call::<_, i64>(&scheduler, object, "/serial", &0i64).await,
        1
    );
    scheduler.shutdown(StopMode::Drain).await.unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn mutex_wait_does_not_block_other_interfaces_and_trap_kills_the_entire_instance() {
    let temp = tempfile::tempdir().unwrap();
    let scheduler = Scheduler::in_memory(RuntimeOptions {
        data_directory: temp.path().join("data"),
        ..Default::default()
    })
    .await
    .unwrap();
    let (mut entered, gate) = gated_observe(&scheduler);
    let object = "/module/probe-a";
    let first = scheduler
        .package(package(temp.path(), "old", object, false))
        .await
        .unwrap();
    let next = scheduler
        .package(package(temp.path(), "new", object, true))
        .await
        .unwrap();
    scheduler.load(first).await.unwrap();
    let start = |interface: &'static str| {
        let scheduler = scheduler.clone();
        tokio::spawn(async move {
            scheduler
                .call::<_, Result<i64, String>>(
                    "/user/test".parse().unwrap(),
                    object.parse().unwrap(),
                    interface.parse().unwrap(),
                    &0i64,
                )
                .await
        })
    };
    let holder = start("/serial");
    assert_eq!(entered.recv().await, Some(1));
    let waiter = start("/serial-alt");
    tokio::time::timeout(Duration::from_secs(15), async {
        loop {
            let snapshot = scheduler.inspector().snapshot().unwrap();
            let module = &snapshot.modules[0];
            if module.budget.requests_used == 2 && module.interfaces.iter().all(|i| i.queued == 0) {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert_eq!(
        call::<_, i64>(&scheduler, object, "/implementation", &()).await,
        1
    );
    assert!(
        entered.try_recv().is_err(),
        "shared lock must exclude the second interface"
    );
    // Both instances use the literal key /serial, with independent lock tables.
    let other = "/module/probe-b";
    let other_package = scheduler
        .package(package(temp.path(), "other", other, false))
        .await
        .unwrap();
    scheduler.load(other_package).await.unwrap();
    let other_scheduler = scheduler.clone();
    let other_holder =
        tokio::spawn(
            async move { call::<_, i64>(&other_scheduler, other, "/serial", &0i64).await },
        );
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(15), entered.recv())
            .await
            .unwrap(),
        Some(1),
        "a lock in one Wasm instance must not block another instance"
    );
    let trap = scheduler
        .call::<_, Result<(), String>>(
            "/user/test".parse().unwrap(),
            object.parse().unwrap(),
            "/trap".parse().unwrap(),
            &(),
        )
        .await;
    assert!(matches!(trap, Err(DispatchError::ExecutionFailed(_))));
    assert!(holder.await.unwrap().is_err());
    assert!(waiter.await.unwrap().is_err());
    phase(&scheduler, object, ModulePhase::Failed).await;
    assert!(
        entered.try_recv().is_err(),
        "trap must not resume another guest task"
    );
    assert!(!other_holder.is_finished());
    assert_eq!(
        call::<_, i64>(&scheduler, other, "/implementation", &()).await,
        1
    );
    scheduler.load(next).await.unwrap();
    gate.add_permits(3); // Old independent native call, other instance, replacement.
    assert_eq!(other_holder.await.unwrap(), 1);
    assert_eq!(
        call::<_, i64>(&scheduler, object, "/serial", &0i64).await,
        1
    );
    assert_eq!(
        call::<_, i64>(&scheduler, object, "/destroyed", &()).await,
        0
    );
    scheduler.shutdown(StopMode::Drain).await.unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn cyclic_calls_can_return_through_a_module_waiting_for_reload() {
    let temp = tempfile::tempdir().unwrap();
    let scheduler = Scheduler::in_memory(RuntimeOptions {
        data_directory: temp.path().join("data"),
        ..Default::default()
    })
    .await
    .unwrap();
    let (mut entered, gate) = gated_observe(&scheduler);
    let a = "/module/probe-a";
    let b = "/module/probe-b";
    let first = scheduler
        .package(peer_package(temp.path(), "a", a, b, false, 0))
        .await
        .unwrap();
    let other = scheduler
        .package(peer_package(temp.path(), "b", b, a, false, 0))
        .await
        .unwrap();
    let replacement = scheduler
        .package(peer_package(temp.path(), "a-new", a, b, true, 0))
        .await
        .unwrap();
    scheduler
        .load_roots(vec![first, other], &[a.parse().unwrap()])
        .await
        .unwrap();
    let active_scheduler = scheduler.clone();
    let active = tokio::spawn(async move {
        call::<_, i64>(
            &active_scheduler,
            a,
            "/chain",
            &(b.parse::<Token>().unwrap(), 2i64),
        )
        .await
    });
    assert_eq!(entered.recv().await, Some(501));
    let request = scheduler
        .request_reload(vec![replacement], StopMode::Drain)
        .unwrap();
    waiting(&scheduler, a).await;
    gate.add_permits(1);
    assert_eq!(
        active.await.unwrap(),
        1,
        "B's callback must still run on old A"
    );
    tokio::time::timeout(Duration::from_secs(15), request.wait())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        call::<_, i64>(&scheduler, a, "/implementation", &()).await,
        2
    );
    assert_eq!(
        call::<_, i64>(&scheduler, b, "/implementation", &()).await,
        1
    );
    scheduler
        .unload_many(
            vec![a.parse().unwrap(), b.parse().unwrap()],
            StopMode::Drain,
        )
        .await
        .unwrap();
    assert!(scheduler.inspector().snapshot().unwrap().modules.is_empty());
}

#[tokio::test(flavor = "current_thread")]
async fn cyclic_group_waits_for_joint_quiescence_and_publishes_all_candidates_together() {
    let temp = tempfile::tempdir().unwrap();
    let scheduler = Scheduler::in_memory(RuntimeOptions {
        data_directory: temp.path().join("data"),
        ..Default::default()
    })
    .await
    .unwrap();
    let (mut entered, gate) = gated_observe(&scheduler);
    let a = "/module/probe-a";
    let b = "/module/probe-b";
    let mut old = Vec::new();
    let mut new = Vec::new();
    for (name, object, peer, delay) in [("a", a, b, 0), ("b", b, a, 300)] {
        let old_path = peer_package(temp.path(), name, object, peer, false, delay);
        let mut declaration = forest_package::read_manifest(&old_path).unwrap();
        if object == a {
            declaration.uses.retain(|u| u.object.to_string() != b);
        }
        if object == b {
            declaration
                .provides
                .retain(|p| p.path.to_string() != "/chain");
        }
        forest_package::write(&old_path, &declaration).unwrap();
        old.push(scheduler.package(old_path).await.unwrap());
        new.push(
            scheduler
                .package(peer_package(
                    temp.path(),
                    &format!("{name}-new"),
                    object,
                    peer,
                    true,
                    delay,
                ))
                .await
                .unwrap(),
        );
    }
    scheduler.load_many(old).await.unwrap();
    let active_scheduler = scheduler.clone();
    let active =
        tokio::spawn(async move { call::<_, i64>(&active_scheduler, b, "/serial", &0i64).await });
    entered.recv().await.unwrap();
    let request = scheduler.request_reload(new, StopMode::Drain).unwrap();
    waiting(&scheduler, a).await;
    assert_eq!(
        call::<_, i64>(&scheduler, a, "/implementation", &()).await,
        1,
        "idle A must stay open while B is busy"
    );
    gate.add_permits(1);
    active.await.unwrap();
    let id = request.snapshot().id;
    let completion = tokio::spawn(request.wait());
    let mut saw_creating = false;
    tokio::time::timeout(Duration::from_secs(15), async {
        loop {
            let snapshot = scheduler.inspector().snapshot().unwrap();
            saw_creating |= snapshot.modules.iter().any(|m| {
                matches!(
                    m.lifecycle,
                    forest_runtime::LifecycleSnapshot::Creating { .. }
                )
            });
            if saw_creating {
                assert!(matches!(
                    scheduler.cancel(id),
                    Err(forest_runtime::CancelError::AlreadyCommitted)
                        | Err(forest_runtime::CancelError::NotFound)
                ));
            }
            let serving = snapshot
                .modules
                .iter()
                .filter(|m| {
                    matches!(
                        m.lifecycle,
                        forest_runtime::LifecycleSnapshot::Serving { .. }
                    )
                })
                .collect::<Vec<_>>();
            assert!(
                serving.is_empty() || serving.len() == 2,
                "partial group publication"
            );
            if serving.len() == 2 {
                assert_eq!(serving[0].published.version, serving[1].published.version);
            }
            if completion.is_finished() {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    completion.await.unwrap().unwrap();
    assert!(saw_creating);
    assert!(
        scheduler
            .inspector()
            .snapshot()
            .unwrap()
            .modules
            .iter()
            .all(|m| m.published.version == "2")
    );
    scheduler.shutdown(StopMode::Drain).await.unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn failed_group_initialization_never_publishes_a_successful_member() {
    let temp = tempfile::tempdir().unwrap();
    let scheduler = Scheduler::in_memory(RuntimeOptions {
        data_directory: temp.path().join("data"),
        ..Default::default()
    })
    .await
    .unwrap();
    observe(&scheduler);
    let a = peer_package(
        temp.path(),
        "a",
        "/module/probe-a",
        "/module/probe-b",
        false,
        0,
    );
    let b = peer_package(
        temp.path(),
        "b",
        "/module/probe-b",
        "/module/probe-a",
        false,
        0,
    );
    let mut manifest = forest_package::read_manifest(&b).unwrap();
    manifest
        .configs
        .iter_mut()
        .find(|c| c.key.to_string() == "/lifecycle/failure")
        .unwrap()
        .default = forest_scheduler_probe::FailureStage::Create.as_value();
    forest_package::write(&b, &manifest).unwrap();
    let error = scheduler
        .load_many(vec![
            scheduler.package(a).await.unwrap(),
            scheduler.package(b).await.unwrap(),
        ])
        .await
        .unwrap_err();
    assert!(matches!(
        instance_failure(&error),
        forest_runtime::InstanceFailure::Lifecycle {
            phase: forest_runtime::LifecyclePhase::Create,
            ..
        }
    ));
    let snapshot = scheduler.inspector().snapshot().unwrap();
    assert!(snapshot.requests.is_empty());
    assert!(snapshot.modules.iter().all(|m| matches!(
        m.lifecycle,
        forest_runtime::LifecycleSnapshot::Failed { .. }
    )));
    assert!(snapshot.modules.iter().all(|m| m.budget.requests_used == 0));
    scheduler.shutdown(StopMode::Drain).await.unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn pending_reload_can_be_explicitly_changed_to_force() {
    let temp = tempfile::tempdir().unwrap();
    let scheduler = Scheduler::in_memory(RuntimeOptions {
        data_directory: temp.path().join("data"),
        ..Default::default()
    })
    .await
    .unwrap();
    let (mut entered, gate) = gated_observe(&scheduler);
    let object = "/module/probe-a";
    let first = scheduler
        .package(package(temp.path(), "old", object, false))
        .await
        .unwrap();
    let second = scheduler
        .package(package(temp.path(), "new", object, true))
        .await
        .unwrap();
    scheduler.load(first).await.unwrap();
    let active_scheduler = scheduler.clone();
    let active = tokio::spawn(async move {
        active_scheduler
            .call::<_, Result<i64, String>>(
                "/user/test".parse().unwrap(),
                object.parse().unwrap(),
                "/serial".parse().unwrap(),
                &0i64,
            )
            .await
    });
    entered.recv().await.unwrap();
    let request = scheduler
        .request_reload(vec![second], StopMode::Drain)
        .unwrap();
    waiting(&scheduler, object).await;
    scheduler.force(&object.parse().unwrap()).unwrap();
    tokio::time::timeout(Duration::from_secs(15), request.wait())
        .await
        .unwrap()
        .unwrap();
    assert!(matches!(
        active.await.unwrap(),
        Err(DispatchError::Interrupted)
    ));
    assert_eq!(
        call::<_, i64>(&scheduler, object, "/implementation", &()).await,
        2
    );
    assert_eq!(
        call::<_, i64>(&scheduler, object, "/destroyed", &()).await,
        0
    );
    gate.add_permits(1);
    scheduler.shutdown(StopMode::Drain).await.unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn forced_shutdown_interrupts_an_already_committed_initialization() {
    let temp = tempfile::tempdir().unwrap();
    let scheduler = Scheduler::in_memory(RuntimeOptions {
        data_directory: temp.path().join("data"),
        ..Default::default()
    })
    .await
    .unwrap();
    observe(&scheduler);
    let object = "/module/probe-a";
    let first = scheduler
        .package(peer_package(
            temp.path(),
            "old",
            object,
            object,
            false,
            60_000,
        ))
        .await
        .unwrap();
    let second = scheduler
        .package(peer_package(
            temp.path(),
            "new",
            object,
            object,
            true,
            60_000,
        ))
        .await
        .unwrap();
    scheduler.load(first).await.unwrap();
    let request = scheduler
        .request_reload(vec![second], StopMode::Drain)
        .unwrap();
    phase(&scheduler, object, ModulePhase::Creating).await;
    tokio::time::timeout(Duration::from_secs(15), scheduler.shutdown(StopMode::Force))
        .await
        .unwrap()
        .unwrap();
    assert!(request.wait().await.is_err());
    let snapshot = scheduler.inspector().snapshot().unwrap();
    assert!(snapshot.modules.is_empty());
    assert!(snapshot.requests.is_empty());
}

#[tokio::test(flavor = "current_thread")]
async fn lifecycle_rejections_preserve_schema_value_and_stage() {
    use forest_runtime::{InstanceFailure, LifecyclePhase};
    use forest_scheduler_probe::{FailureStage, LifecycleError};
    for (stage, wanted) in [
        (FailureStage::Create, LifecyclePhase::Create),
        (FailureStage::Destroy, LifecyclePhase::Destroy),
    ] {
        let temp = tempfile::tempdir().unwrap();
        let scheduler = Scheduler::in_memory(RuntimeOptions {
            data_directory: temp.path().join("data"),
            ..Default::default()
        })
        .await
        .unwrap();
        observe(&scheduler);
        let object: Token = "/module/lifecycle".parse().unwrap();
        let path = package(temp.path(), "package", &object.to_string(), false);
        let mut manifest = forest_scheduler_probe::manifest(object.clone());
        manifest
            .configs
            .iter_mut()
            .find(|c| c.key.to_string() == "/counter")
            .unwrap()
            .default = 73i64.as_value();
        manifest
            .configs
            .iter_mut()
            .find(|c| c.key.to_string() == "/lifecycle/failure")
            .unwrap()
            .default = stage.as_value();
        forest_package::write(&path, &manifest).unwrap();
        let package = scheduler.package(path).await.unwrap();
        let result = scheduler.load(package).await;
        let error = if wanted == LifecyclePhase::Create {
            result.unwrap_err()
        } else {
            result.unwrap();
            scheduler
                .unload(&object, StopMode::Drain)
                .await
                .unwrap_err()
        };
        let failure = instance_failure(&error);
        let InstanceFailure::Lifecycle {
            phase: actual,
            schema,
            error,
        } = failure
        else {
            panic!("module rejection was confused with dispatch failure: {failure:?}")
        };
        assert_eq!(*actual, wanted);
        assert!(manifest::equivalent(schema, &LifecycleError::as_schema()));
        assert_eq!(
            LifecycleError::from_value(error.clone()).unwrap(),
            match wanted {
                LifecyclePhase::Create => LifecycleError::CreateRejected { counter: 73 },
                LifecyclePhase::Destroy => LifecycleError::DestroyRejected { counter: 73 },
            }
        );
        if wanted == LifecyclePhase::Create {
            phase(&scheduler, &object.to_string(), ModulePhase::Failed).await;
            assert!(matches!(
                scheduler.failure(&object),
                Some(InstanceFailure::Lifecycle { .. })
            ));
            let snapshot = scheduler.inspector().snapshot().unwrap();
            let json = serde_json::to_value(snapshot).unwrap();
            assert_eq!(
                json["modules"][0]["lifecycle"]["failure"]["kind"],
                "lifecycle"
            );
        } else {
            assert_eq!(scheduler.phase(&object), None);
        }
        scheduler.shutdown(StopMode::Drain).await.unwrap();
    }
}

#[tokio::test(flavor = "current_thread")]
async fn lifecycle_host_errors_stay_typed_and_invalid_payloads_are_contract_failures() {
    use forest_runtime::InstanceFailure;
    use forest_scheduler_probe::LifecycleError;
    for mismatched_schema in [false, true] {
        let temp = tempfile::tempdir().unwrap();
        let scheduler = Scheduler::in_memory(RuntimeOptions {
            data_directory: temp.path().join("data"),
            ..Default::default()
        })
        .await
        .unwrap();
        observe(&scheduler);
        let object: Token = "/module/lifecycle".parse().unwrap();
        let path = package(temp.path(), "package", &object.to_string(), false);
        let mut manifest = forest_scheduler_probe::manifest(object);
        manifest
            .configs
            .iter_mut()
            .find(|c| c.key.to_string() == "/counter")
            .unwrap()
            .access
            .readers
            .clear();
        if mismatched_schema {
            manifest.lifecycle_error = bool::as_schema();
        }
        forest_package::write(&path, &manifest).unwrap();
        let error = scheduler
            .load(scheduler.package(path).await.unwrap())
            .await
            .unwrap_err();
        let failure = instance_failure(&error);
        if mismatched_schema {
            assert!(matches!(
                failure,
                InstanceFailure::Dispatch {
                    error: DispatchError::OutputContract
                }
            ));
        } else {
            let InstanceFailure::Lifecycle { error, .. } = failure else {
                panic!("{failure:?}")
            };
            let LifecycleError::Host(host) = LifecycleError::from_value(error.clone()).unwrap()
            else {
                panic!("host error lost its category")
            };
            assert_eq!(host, DispatchError::Denied.into());
        }
        scheduler.shutdown(StopMode::Drain).await.unwrap();
    }
}

#[tokio::test(flavor = "current_thread")]
async fn actual_sdk_concurrency_reload_trap_preemption_and_host_io() {
    let temp = tempfile::tempdir().unwrap();
    let scheduler = Scheduler::new(
        temp.path().join("state.sqlite3"),
        RuntimeOptions {
            max_requests: 2,
            data_directory: temp.path().join("data"),
            ..RuntimeOptions::default()
        },
    )
    .await
    .unwrap();
    let observations = observe(&scheduler);
    let first = scheduler
        .package(package(temp.path(), "a1", "/module/probe-a", false))
        .await
        .unwrap();
    let second = scheduler
        .package(package(temp.path(), "a2", "/module/probe-a", true))
        .await
        .unwrap();
    let other_path = package(temp.path(), "b1", "/module/probe-b", false);
    let mut manifest = forest_scheduler_probe::manifest("/module/probe-b".parse().unwrap());
    manifest.uses.push(InterfaceUse {
        object: "/module/probe-a".parse().unwrap(),
        interface: "/echo".parse().unwrap(),
        input: String::as_schema(),
        output: <Result<String, String>>::as_schema(),
    });
    forest_package::write(&other_path, &manifest).unwrap();
    let other = scheduler.package(other_path).await.unwrap();
    let plan = scheduler
        .plan_load(
            vec![other.clone(), first.clone()],
            &["/module/probe-b".parse().unwrap()],
        )
        .unwrap();
    assert_eq!(
        plan.iter()
            .map(|p| p.manifest().package.id.to_string())
            .collect::<Vec<_>>(),
        ["/module/probe-a", "/module/probe-b"]
    );
    scheduler
        .load_roots(
            vec![other, first.clone()],
            &["/module/probe-b".parse().unwrap()],
        )
        .await
        .unwrap();
    assert!(
        scheduler
            .unload(&"/module/probe-a".parse().unwrap(), StopMode::Drain)
            .await
            .is_err(),
        "provider unload requires dependent removal first"
    );
    assert_eq!(
        observations.count(),
        0,
        "create communication was rejected as data before native execution"
    );
    let inspected = scheduler.inspector().snapshot().unwrap();
    assert_eq!(inspected.modules.len(), 2);
    assert_eq!(inspected.hosts.len(), 2);
    assert_eq!(inspected.modules[0].published.version, "1");
    assert!(matches!(
        inspected.modules[0].lifecycle,
        forest_runtime::LifecycleSnapshot::Serving { .. }
    ));
    assert_eq!(
        call::<_, String>(&scheduler, "/module/probe-a", "/echo", &"typed".to_owned()).await,
        "typed"
    );
    assert_eq!(
        call::<_, i64>(&scheduler, "/module/probe-a", "/nested", &()).await,
        11,
        "child wakers and self-wakes must both survive Pending"
    );
    let denied = scheduler
        .call::<_, Result<String, String>>(
            "/user/unauthorized".parse().unwrap(),
            "/module/probe-a".parse().unwrap(),
            "/echo".parse().unwrap(),
            &"x".to_owned(),
        )
        .await;
    assert!(matches!(denied, Err(DispatchError::Denied)));
    let (one, two) = tokio::join!(
        call::<_, i64>(&scheduler, "/module/probe-a", "/serial", &20i64),
        call::<_, i64>(&scheduler, "/module/probe-a", "/serial", &20i64)
    );
    assert_eq!((one, two), (1, 2));
    assert_eq!(&*observations.values.lock().unwrap(), &[1, -1, 2, -2]);
    let offset = observations.count();
    let (_, _) = tokio::join!(
        call::<_, i64>(&scheduler, "/module/probe-a", "/parallel", &20i64),
        call::<_, i64>(&scheduler, "/module/probe-a", "/parallel", &20i64)
    );
    let values = observations.values.lock().unwrap()[offset..].to_vec();
    assert!(values[0] > 0 && values[1] > 0 && values[2] < 0 && values[3] < 0);
    assert_eq!(
        call::<_, i64>(&scheduler, "/module/probe-a", "/increment", &()).await,
        1
    );

    // A pending reload keeps old ingress open; all admitted work stays bounded.
    let offset = observations.count();
    let s = scheduler.clone();
    let active = tokio::spawn(async move {
        s.call::<_, Result<i64, String>>(
            "/user/test".parse().unwrap(),
            "/module/probe-a".parse().unwrap(),
            "/serial".parse().unwrap(),
            &200i64,
        )
        .await
    });
    observations.until(offset + 1).await;
    let s = scheduler.clone();
    let candidate = second.clone();
    let reload = tokio::spawn(async move { s.reload(candidate, StopMode::Drain).await });
    waiting(&scheduler, "/module/probe-a").await;
    assert_eq!(
        call::<_, i64>(&scheduler, "/module/probe-a", "/implementation", &()).await,
        1
    );
    let s = scheduler.clone();
    let deferred =
        tokio::spawn(async move { call::<_, i64>(&s, "/module/probe-a", "/serial", &0i64).await });
    tokio::time::timeout(Duration::from_secs(15), async {
        while scheduler.inspector().snapshot().unwrap().modules[0]
            .budget
            .requests_used
            != 2
        {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    let full = scheduler.send(
        "/user/test".parse().unwrap(),
        "/module/probe-a".parse().unwrap(),
        "/implementation".parse().unwrap(),
        &(),
    );
    assert!(matches!(full, Err(DispatchError::QueueFull)));
    let completed = active.await.unwrap().unwrap().unwrap();
    reload.await.unwrap().unwrap();
    assert_eq!(
        scheduler.inspector().snapshot().unwrap().modules[0]
            .published
            .version,
        "2"
    );
    assert_eq!(
        deferred.await.unwrap(),
        completed + 1,
        "lock waiters finish on the old instance"
    );
    assert_eq!(
        call::<_, i64>(&scheduler, "/module/probe-a", "/increment", &()).await,
        2,
        "reload retains SQLite state"
    );
    assert_eq!(
        call::<_, i64>(&scheduler, "/module/probe-a", "/destroyed", &()).await,
        1
    );

    let offset = observations.count();
    let s = scheduler.clone();
    let old = tokio::spawn(async move {
        s.call::<_, Result<i64, String>>(
            "/user/test".parse().unwrap(),
            "/module/probe-a".parse().unwrap(),
            "/serial".parse().unwrap(),
            &60_000i64,
        )
        .await
    });
    observations.until(offset + 1).await;
    scheduler
        .reload(first.clone(), StopMode::AtYield)
        .await
        .unwrap();
    assert!(matches!(
        old.await.unwrap(),
        Err(DispatchError::Interrupted)
    ));
    assert_eq!(
        call::<_, i64>(&scheduler, "/module/probe-a", "/implementation", &()).await,
        1
    );
    assert_eq!(
        call::<_, i64>(&scheduler, "/module/probe-a", "/destroyed", &()).await,
        2
    );

    assert_eq!(
        call::<_, String>(
            &scheduler,
            "/module/probe-a",
            "/file",
            &"folder/value".to_owned()
        )
        .await,
        "managed"
    );
    let escape = scheduler
        .call::<_, Result<String, String>>(
            "/user/test".parse().unwrap(),
            "/module/probe-a".parse().unwrap(),
            "/file".parse().unwrap(),
            &"../escape".to_owned(),
        )
        .await
        .unwrap();
    assert!(escape.is_err());
    assert!(!temp.path().join("escape").exists());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let (mut stream, _) = listener.accept().await.unwrap();
        let mut request = [0; 4096];
        let _ = stream.read(&mut request).await.unwrap();
        stream
            .write_all(
                b"HTTP/1.1 404 Not Found\r\nContent-Length: 7\r\nConnection: close\r\n\r\nmissing",
            )
            .await
            .unwrap();
    });
    assert_eq!(
        call::<_, String>(
            &scheduler,
            "/module/probe-a",
            "/http",
            &format!("http://{address}")
        )
        .await,
        "404:missing"
    );
    server.await.unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        use futures::{SinkExt, StreamExt};
        let (stream, _) = listener.accept().await.unwrap();
        let mut ws = tokio_tungstenite::accept_async(stream).await.unwrap();
        let message = ws.next().await.unwrap().unwrap();
        ws.send(message).await.unwrap();
        let _ = ws.next().await;
    });
    assert_eq!(
        call::<_, String>(
            &scheduler,
            "/module/probe-a",
            "/ws",
            &format!("ws://{address}")
        )
        .await,
        "probe"
    );
    server.await.unwrap();

    // Single-thread Tokio still progresses: epoch ticks come from a host thread.
    let offset = observations.count();
    let s = scheduler.clone();
    let spin = tokio::spawn(async move {
        s.call::<_, Result<(), String>>(
            "/user/test".parse().unwrap(),
            "/module/probe-a".parse().unwrap(),
            "/spin".parse().unwrap(),
            &(),
        )
        .await
    });
    observations.until(offset + 1).await;
    let s = scheduler.clone();
    let blocked = tokio::spawn(async move {
        s.call::<_, Result<String, String>>(
            "/user/test".parse().unwrap(),
            "/module/probe-a".parse().unwrap(),
            "/echo".parse().unwrap(),
            &"same instance".to_owned(),
        )
        .await
    });
    assert_eq!(
        call::<_, String>(
            &scheduler,
            "/module/probe-b",
            "/echo",
            &"other instance".to_owned()
        )
        .await,
        "other instance"
    );
    assert!(
        !blocked.is_finished(),
        "passive yield must retain the spinning instance's exclusive advancement"
    );
    scheduler
        .unload(&"/module/probe-b".parse().unwrap(), StopMode::Drain)
        .await
        .unwrap();
    scheduler
        .unload(&"/module/probe-a".parse().unwrap(), StopMode::Force)
        .await
        .unwrap();
    assert!(matches!(
        blocked.await.unwrap(),
        Err(DispatchError::Interrupted)
    ));
    assert!(matches!(
        spin.await.unwrap(),
        Err(DispatchError::Interrupted)
    ));
    scheduler.load(first.clone()).await.unwrap();
    assert_eq!(
        call::<_, i64>(&scheduler, "/module/probe-a", "/destroyed", &()).await,
        2,
        "Force skips destroy"
    );
    let trapped = scheduler
        .call::<_, Result<(), String>>(
            "/user/test".parse().unwrap(),
            "/module/probe-a".parse().unwrap(),
            "/trap".parse().unwrap(),
            &(),
        )
        .await;
    assert!(matches!(trapped, Err(DispatchError::ExecutionFailed(_))));
    phase(&scheduler, "/module/probe-a", ModulePhase::Failed).await;
    let failed = scheduler
        .call::<_, Result<String, String>>(
            "/user/test".parse().unwrap(),
            "/module/probe-a".parse().unwrap(),
            "/echo".parse().unwrap(),
            &"x".to_owned(),
        )
        .await;
    assert!(matches!(failed, Err(DispatchError::Unavailable(_))));
    scheduler.load(first).await.unwrap();
    assert_eq!(
        call::<_, i64>(&scheduler, "/module/probe-a", "/increment", &()).await,
        3
    );
    scheduler.shutdown(StopMode::Drain).await.unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn reloading_defaults_preserves_destroyed_state_and_rejects_schema_changes() {
    let temp = tempfile::tempdir().unwrap();
    let store = forest_config::ConfigStore::open_in_memory().await.unwrap();
    let scheduler = Scheduler::with_store(
        store.clone(),
        RuntimeOptions {
            data_directory: temp.path().join("data"),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    let _observations = observe(&scheduler);
    let object = "/module/defaults";
    let path = package(temp.path(), "defaults", object, false);
    scheduler
        .load(scheduler.package(&path).await.unwrap())
        .await
        .unwrap();
    assert_eq!(
        call::<_, i64>(&scheduler, object, "/increment", &()).await,
        1
    );
    let mut declaration: Manifest = forest_package::read_manifest(&path).unwrap();
    let counter = declaration
        .configs
        .iter_mut()
        .find(|c| c.key.to_string() == "/counter")
        .unwrap();
    counter.default = 99i64.as_value();
    forest_package::write(&path, &declaration).unwrap();
    let replacement = scheduler.package(&path).await.unwrap();
    scheduler
        .reload(replacement, StopMode::Drain)
        .await
        .unwrap();
    let entry = store
        .entry(&object.parse().unwrap(), &"/counter".parse().unwrap())
        .await
        .unwrap();
    assert_eq!(entry.default_value(), &99i64.as_value());
    assert_eq!(entry.value(), &1i64.as_value()); // The outgoing destroy's write survived.
    assert_eq!(
        call::<_, i64>(&scheduler, object, "/increment", &()).await,
        2
    );
    let before = scheduler.inspector().snapshot().unwrap().modules[0]
        .lifecycle
        .clone();
    let counter = declaration
        .configs
        .iter_mut()
        .find(|c| c.key.to_string() == "/counter")
        .unwrap();
    counter.schema = bool::as_schema();
    counter.default = true.as_value();
    forest_package::write(&path, &declaration).unwrap();
    let incompatible = scheduler.package(&path).await.unwrap();
    assert!(
        scheduler
            .reload(incompatible, StopMode::Drain)
            .await
            .is_err()
    );
    let after = scheduler.inspector().snapshot().unwrap().modules[0]
        .lifecycle
        .clone();
    assert_eq!(
        serde_json::to_value(before).unwrap(),
        serde_json::to_value(after).unwrap()
    );
    scheduler
        .unload(&object.parse().unwrap(), StopMode::Drain)
        .await
        .unwrap();
    store
        .reset(&object.parse().unwrap(), &"/counter".parse().unwrap())
        .await
        .unwrap();
    assert_eq!(
        store
            .get(
                &object.parse().unwrap(),
                &forest_config::Key::<i64>::new("/counter".parse().unwrap())
            )
            .await
            .unwrap(),
        99
    );
    scheduler.shutdown(StopMode::Drain).await.unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn variant_contracts_load_reload_and_reject_before_stopping_the_live_instance() {
    let temp = tempfile::tempdir().unwrap();
    let scheduler = Scheduler::in_memory(RuntimeOptions {
        data_directory: temp.path().join("data"),
        ..Default::default()
    })
    .await
    .unwrap();
    let observations = observe(&scheduler);
    scheduler
        .register_native(NativeInterface::new(
            "/core/variance".parse().unwrap(),
            "/variance".parse().unwrap(),
            callers(vec![SubjectPattern::Path("/module/**".parse().unwrap())]),
            |_, input: i64| async move { if input < 0 { Err(9i32) } else { Ok(7i32) } },
        ))
        .unwrap();
    scheduler
        .register_event(forest_runtime::EventDefinition {
            source: "/core/variance-events".parse().unwrap(),
            event: "/number".parse().unwrap(),
            input: i32::as_schema(),
        })
        .unwrap();
    let object = "/module/variance-provider";
    let mut candidates = Vec::new();
    for (second, name) in [(false, "variance-old"), (true, "variance-new")] {
        let path = package(temp.path(), name, object, second);
        let mut declaration: Manifest = forest_package::read_manifest(&path).unwrap();
        let interface = declaration
            .provides
            .iter_mut()
            .find(|i| i.path.to_string() == "/variance")
            .unwrap();
        if !second {
            interface.input = i32::as_schema();
            interface.output = <Result<i64, i64>>::as_schema();
        }
        interface.access = callers(vec![
            SubjectPattern::Path("/user/test".parse().unwrap()),
            SubjectPattern::Path("/module/variance-consumer".parse().unwrap()),
            SubjectPattern::Path("/core/variance-events".parse().unwrap()),
        ]);
        declaration.subscriptions.push(EventSubscription {
            source: "/core/variance-events".parse().unwrap(),
            event: "/number".parse().unwrap(),
            interface: "/variance".parse().unwrap(),
        });
        forest_package::write(&path, &declaration).unwrap();
        candidates.push(scheduler.package(&path).await.unwrap());
    }
    let first = candidates.remove(0);
    let second = candidates.remove(0);
    let caller = "/module/variance-consumer";
    let path = package(temp.path(), "variance-caller", caller, false);
    let mut declaration: Manifest = forest_package::read_manifest(&path).unwrap();
    for target in [object, "/core/variance"] {
        declaration.uses.push(InterfaceUse {
            object: target.parse().unwrap(),
            interface: "/variance".parse().unwrap(),
            input: i32::as_schema(),
            output: <Result<i64, i64>>::as_schema(),
        });
    }
    forest_package::write(&path, &declaration).unwrap();
    let consumer = scheduler.package(&path).await.unwrap();
    scheduler.load(first.clone()).await.unwrap();
    scheduler.load(consumer.clone()).await.unwrap();
    assert_eq!(
        call::<_, i64>(&scheduler, caller, "/use-native-variance", &1i32).await,
        7
    );
    assert_eq!(
        call::<_, i64>(&scheduler, caller, "/use-native-variance", &-1i32).await,
        -9
    );
    assert_eq!(
        call::<_, i64>(&scheduler, caller, "/use-variance", &1i32).await,
        7
    );
    assert_eq!(
        call::<_, i64>(&scheduler, object, "/increment", &()).await,
        1
    );

    // Requests during Pending still execute on the old instance.
    let offset = observations.count();
    let active_scheduler = scheduler.clone();
    let active =
        tokio::spawn(
            async move { call::<_, i64>(&active_scheduler, object, "/serial", &200i64).await },
        );
    observations.until(offset + 1).await;
    let reload_scheduler = scheduler.clone();
    let reload =
        tokio::spawn(async move { reload_scheduler.reload(second, StopMode::Drain).await });
    waiting(&scheduler, object).await;
    let deferred_scheduler = scheduler.clone();
    let deferred = tokio::spawn(async move {
        call::<_, i64>(&deferred_scheduler, caller, "/use-variance", &-1i32).await
    });
    assert_eq!(deferred.await.unwrap(), -9);
    active.await.unwrap();
    reload.await.unwrap().unwrap();
    assert_eq!(
        call::<_, i32>(&scheduler, object, "/variance", &i64::MAX).await,
        7
    );
    assert_eq!(
        call::<_, i64>(&scheduler, object, "/increment", &()).await,
        2
    );
    assert_eq!(
        call::<_, i64>(&scheduler, object, "/implementation", &()).await,
        2
    );
    // Existing and newly loaded users can both keep the broader expected output.
    scheduler
        .unload(&caller.parse().unwrap(), StopMode::Drain)
        .await
        .unwrap();
    scheduler.load(consumer).await.unwrap();
    assert_eq!(
        call::<_, i64>(&scheduler, caller, "/use-variance", &1i32).await,
        7
    );
    let notifications = scheduler.notify(
        &"/core/variance-events".parse().unwrap(),
        &"/number".parse().unwrap(),
        "/user/test".parse().unwrap(),
        &1i32,
    );
    assert_eq!(notifications.len(), 1);
    assert!(notifications[0].1.is_ok());

    // Reverting would narrow inputs and widen outputs. No stop mode bypasses it.
    let offset = observations.count();
    let active_scheduler = scheduler.clone();
    let active =
        tokio::spawn(
            async move { call::<_, i64>(&active_scheduler, object, "/serial", &200i64).await },
        );
    observations.until(offset + 1).await;
    let before = scheduler
        .inspector()
        .snapshot()
        .unwrap()
        .modules
        .into_iter()
        .find(|m| m.object.to_string() == object)
        .unwrap();
    let forest_runtime::LifecycleSnapshot::Serving { instance: before } = before.lifecycle else {
        panic!("serving instance")
    };
    for mode in [StopMode::Drain, StopMode::AtYield, StopMode::Force] {
        let error = scheduler.reload(first.clone(), mode).await.unwrap_err();
        assert!(error.to_string().contains("/variance"));
        let after = scheduler
            .inspector()
            .snapshot()
            .unwrap()
            .modules
            .into_iter()
            .find(|m| m.object.to_string() == object)
            .unwrap();
        let forest_runtime::LifecycleSnapshot::Serving { instance: after } = after.lifecycle else {
            panic!("rejected replacement changed phase")
        };
        assert_eq!(after.id, before.id);
        assert_eq!(after.release.version, "2");
    }
    active.await.unwrap();
    assert_eq!(
        call::<_, i64>(&scheduler, object, "/destroyed", &()).await,
        1
    );

    // The narrowed output is checked against the actual executing instance.
    let path = temp.path().join("variance-new");
    let mut declaration: Manifest = forest_package::read_manifest(&path).unwrap();
    declaration
        .provides
        .iter_mut()
        .find(|i| i.path.to_string() == "/implementation")
        .unwrap()
        .output = <Result<i32, String>>::as_schema();
    forest_package::write(&path, &declaration).unwrap();
    let dishonest = scheduler.package(&path).await.unwrap();
    let offset = observations.count();
    let active_scheduler = scheduler.clone();
    let active =
        tokio::spawn(
            async move { call::<_, i64>(&active_scheduler, object, "/serial", &200i64).await },
        );
    observations.until(offset + 1).await;
    let reload_scheduler = scheduler.clone();
    let reload =
        tokio::spawn(async move { reload_scheduler.reload(dishonest, StopMode::Drain).await });
    waiting(&scheduler, object).await;
    assert_eq!(
        call::<_, i64>(&scheduler, object, "/implementation", &()).await,
        2
    );
    active.await.unwrap();
    reload.await.unwrap().unwrap();
    let result = scheduler
        .call::<_, Result<i64, String>>(
            "/user/test".parse().unwrap(),
            object.parse().unwrap(),
            "/implementation".parse().unwrap(),
            &(),
        )
        .await;
    assert!(matches!(result, Err(DispatchError::OutputContract)));
    phase(&scheduler, object, ModulePhase::Failed).await;
    scheduler.shutdown(StopMode::Drain).await.unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn force_waits_for_submitted_sqlite_write_before_recreating_instance() {
    let temp = tempfile::tempdir().unwrap();
    let store = forest_config::ConfigStore::open(temp.path().join("config.sqlite3"))
        .await
        .unwrap();
    let scheduler = Scheduler::with_store(
        store.clone(),
        RuntimeOptions {
            data_directory: temp.path().join("data"),
            ..RuntimeOptions::default()
        },
    )
    .await
    .unwrap();
    let observations = observe(&scheduler);
    let package = scheduler
        .package(package(temp.path(), "p", "/module/probe-a", false))
        .await
        .unwrap();
    scheduler.load(package.clone()).await.unwrap();
    let (entered, ready) = tokio::sync::oneshot::channel();
    let release = Arc::new((Mutex::new(false), std::sync::Condvar::new()));
    let gate = release.clone();
    let worker = store.clone();
    let blocker = tokio::spawn(async move {
        worker
            .update(move |_| {
                entered.send(()).unwrap();
                let (lock, changed) = &*gate;
                let mut released = lock.lock().unwrap();
                while !*released {
                    released = changed.wait(released).unwrap();
                }
                Ok(((), vec![]))
            })
            .await
            .unwrap()
    });
    ready.await.unwrap();
    let s = scheduler.clone();
    let writing = tokio::spawn(async move {
        s.call::<_, Result<(), String>>(
            "/user/test".parse().unwrap(),
            "/module/probe-a".parse().unwrap(),
            "/atomic-write".parse().unwrap(),
            &42i64,
        )
        .await
    });
    observations.until(1).await;
    let s = scheduler.clone();
    let stopped = tokio::spawn(async move {
        s.unload(&"/module/probe-a".parse().unwrap(), StopMode::Force)
            .await
    });
    phase(&scheduler, "/module/probe-a", ModulePhase::Releasing).await;
    assert!(!stopped.is_finished());
    {
        let (lock, changed) = &*release;
        *lock.lock().unwrap() = true;
        changed.notify_one();
    }
    blocker.await.unwrap();
    stopped.await.unwrap().unwrap();
    assert!(matches!(
        writing.await.unwrap(),
        Err(DispatchError::Interrupted)
    ));
    scheduler.load(package).await.unwrap();
    assert_eq!(
        call::<_, i64>(&scheduler, "/module/probe-a", "/increment", &()).await,
        43
    );
    scheduler.shutdown(StopMode::Drain).await.unwrap();
    store.close().await.unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn large_frames_cross_guest_and_host_boundaries_without_trapping() {
    let temp = tempfile::tempdir().unwrap();
    let scheduler = Scheduler::new(
        temp.path().join("config.sqlite3"),
        RuntimeOptions {
            data_directory: temp.path().join("data"),
            ..RuntimeOptions::default()
        },
    )
    .await
    .unwrap();
    observe(&scheduler);
    let package = scheduler
        .package(package(temp.path(), "large", "/module/probe-a", false))
        .await
        .unwrap();
    scheduler.load(package).await.unwrap();
    // The old 16 MiB ceiling failed in SDK input, host results and exported output.
    let input = vec![0x5au8; 24 * 1024 * 1024];
    let output = scheduler
        .call::<_, Result<Vec<u8>, String>>(
            "/user/test".parse().unwrap(),
            "/module/probe-a".parse().unwrap(),
            "/payload".parse().unwrap(),
            &input,
        )
        .await
        .unwrap()
        .unwrap();
    assert_eq!(output, input);
    assert_eq!(
        scheduler.phase(&"/module/probe-a".parse().unwrap()),
        Some(ModulePhase::Serving)
    );
    assert_eq!(
        call::<_, String>(
            &scheduler,
            "/module/probe-a",
            "/echo",
            &"still serving".to_owned()
        )
        .await,
        "still serving"
    );
    scheduler.shutdown(StopMode::Drain).await.unwrap();
}
