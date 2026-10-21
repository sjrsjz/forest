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
    std::fs::write(
        path.join("manifest.json"),
        serde_json::to_vec(&manifest).unwrap(),
    )
    .unwrap();
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
fn observe(s: &Scheduler) -> Arc<Observations> {
    let observations = Arc::new(Observations::default());
    let target = observations.clone();
    s.register_native(NativeInterface::new(
        "/core/probe".parse().unwrap(),
        "/observe".parse().unwrap(),
        true,
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
    std::fs::write(
        other_path.join("manifest.json"),
        serde_json::to_vec(&manifest).unwrap(),
    )
    .unwrap();
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
    assert_eq!(inspected.hosts.len(), 1);
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

    // Accepted old work drains, new work stays bounded and runs on new code.
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
    phase(&scheduler, "/module/probe-a", ModulePhase::Stopping).await;
    let s = scheduler.clone();
    let deferred =
        tokio::spawn(
            async move { call::<_, i64>(&s, "/module/probe-a", "/implementation", &()).await },
        );
    tokio::task::yield_now().await;
    let full = scheduler.send(
        "/user/test".parse().unwrap(),
        "/module/probe-a".parse().unwrap(),
        "/implementation".parse().unwrap(),
        &(),
    );
    assert!(matches!(full, Err(DispatchError::QueueFull)));
    assert!(active.await.unwrap().unwrap().is_ok());
    reload.await.unwrap().unwrap();
    assert_eq!(
        scheduler.inspector().snapshot().unwrap().modules[0]
            .published
            .version,
        "2"
    );
    assert_eq!(deferred.await.unwrap(), 2);
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
            true,
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
        let mut declaration: Manifest =
            serde_json::from_slice(&std::fs::read(path.join("manifest.json")).unwrap()).unwrap();
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
        std::fs::write(
            path.join("manifest.json"),
            serde_json::to_vec(&declaration).unwrap(),
        )
        .unwrap();
        candidates.push(scheduler.package(&path).await.unwrap());
    }
    let first = candidates.remove(0);
    let second = candidates.remove(0);
    let caller = "/module/variance-consumer";
    let path = package(temp.path(), "variance-caller", caller, false);
    let mut declaration: Manifest =
        serde_json::from_slice(&std::fs::read(path.join("manifest.json")).unwrap()).unwrap();
    for target in [object, "/core/variance"] {
        declaration.uses.push(InterfaceUse {
            object: target.parse().unwrap(),
            interface: "/variance".parse().unwrap(),
            input: i32::as_schema(),
            output: <Result<i64, i64>>::as_schema(),
        });
    }
    std::fs::write(
        path.join("manifest.json"),
        serde_json::to_vec(&declaration).unwrap(),
    )
    .unwrap();
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

    // A request admitted under the old contract executes on the new instance.
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
    phase(&scheduler, object, ModulePhase::Stopping).await;
    let deferred_scheduler = scheduler.clone();
    let deferred = tokio::spawn(async move {
        call::<_, i64>(&deferred_scheduler, caller, "/use-variance", &-1i32).await
    });
    tokio::time::timeout(Duration::from_secs(15), async {
        while scheduler
            .inspector()
            .snapshot()
            .unwrap()
            .modules
            .iter()
            .find(|m| m.object.to_string() == object)
            .unwrap()
            .queued_next
            == 0
        {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    active.await.unwrap();
    reload.await.unwrap().unwrap();
    assert_eq!(deferred.await.unwrap(), -9);
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

    // A guest that lies about a narrowed output must fail even for a deferred
    // request that was admitted while the old, broader contract was published.
    let path = temp.path().join("variance-new");
    let mut declaration: Manifest =
        serde_json::from_slice(&std::fs::read(path.join("manifest.json")).unwrap()).unwrap();
    declaration
        .provides
        .iter_mut()
        .find(|i| i.path.to_string() == "/implementation")
        .unwrap()
        .output = <Result<i32, String>>::as_schema();
    std::fs::write(
        path.join("manifest.json"),
        serde_json::to_vec(&declaration).unwrap(),
    )
    .unwrap();
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
    phase(&scheduler, object, ModulePhase::Stopping).await;
    let result = scheduler
        .call::<_, Result<i64, String>>(
            "/user/test".parse().unwrap(),
            object.parse().unwrap(),
            "/implementation".parse().unwrap(),
            &(),
        )
        .await;
    assert!(matches!(result, Err(DispatchError::OutputContract)));
    active.await.unwrap();
    reload.await.unwrap().unwrap();
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
