use super::*;

#[tokio::test(flavor = "current_thread")]
async fn activation_has_its_own_admission_and_can_await_without_blocking_handlers() {
    let temp = tempfile::tempdir().unwrap();
    let scheduler = Scheduler::in_memory(RuntimeOptions {
        data_directory: temp.path().join("data"),
        max_requests: 1,
        ..Default::default()
    })
    .await
    .unwrap();
    let (mut entered, gate) = gated_observe(&scheduler);
    let object = "/module/probe-a";
    let path = package(temp.path(), "activation", object, false);
    let mut manifest = forest_package::read_manifest(&path).unwrap();
    manifest.activated = Some("/activated".parse().unwrap());
    forest_package::write(&path, &manifest).unwrap();
    let loaded = scheduler.package(path).await.unwrap();
    scheduler.load(loaded.clone()).await.unwrap();
    assert_eq!(entered.recv().await, Some(600));
    let snapshot = scheduler.inspector().snapshot().unwrap();
    assert_eq!(snapshot.modules[0].budget.control_tasks_used, 1);
    assert_eq!(
        call::<_, String>(&scheduler, object, "/echo", &"ready".to_owned()).await,
        "ready"
    );
    let s = scheduler.clone();
    let reload = tokio::spawn(async move { s.reload(loaded, StopMode::Drain).await });
    waiting(&scheduler, object).await;
    assert!(!reload.is_finished());
    gate.add_permits(2);
    assert_eq!(entered.recv().await, Some(601));
    reload.await.unwrap().unwrap();
    assert_eq!(entered.recv().await, Some(600));
    gate.add_permits(2);
    assert_eq!(entered.recv().await, Some(601));
    scheduler.shutdown(StopMode::Drain).await.unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn cyclic_event_subscriptions_preserve_the_issuing_identity_and_contracts() {
    let temp = tempfile::tempdir().unwrap();
    let store = forest_config::ConfigStore::open(temp.path().join("config.sqlite3"))
        .await
        .unwrap();
    let scheduler = Scheduler::with_store(
        store.clone(),
        RuntimeOptions {
            data_directory: temp.path().join("data"),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    let observations = observe(&scheduler);
    let mut packages = Vec::new();
    let mut paths = Vec::new();
    for (name, peer) in [("a", "b"), ("b", "a")] {
        let object = format!("/module/{name}");
        let path = package(temp.path(), name, &object, false);
        let mut manifest = forest_package::read_manifest(&path).unwrap();
        manifest.events.push(EventDeclaration {
            path: "/sample".parse().unwrap(),
            input: String::as_schema(),
            access: AccessPolicy {
                readers: vec![SubjectPattern::Path("/module/**".parse().unwrap())],
                writers: vec![],
                callers: vec![],
            },
        });
        manifest.subscriptions.push(EventSubscription {
            source: format!("/module/{peer}").parse().unwrap(),
            event: "/sample".parse().unwrap(),
            interface: "/on-event".parse().unwrap(),
        });
        forest_package::write(&path, &manifest).unwrap();
        packages.push(scheduler.package(&path).await.unwrap());
        paths.push(path);
    }
    assert_eq!(
        scheduler
            .plan_load(packages.clone(), &["/module/a".parse().unwrap()])
            .unwrap()
            .len(),
        2
    );
    scheduler
        .load_roots(packages, &["/module/a".parse().unwrap()])
        .await
        .unwrap();
    assert_eq!(
        call::<_, Vec<bool>>(
            &scheduler,
            "/module/a",
            "/publish",
            &(
                "payload".to_owned(),
                Some("/user/admin".parse::<Token>().unwrap())
            )
        )
        .await,
        vec![true]
    );
    observations.until(1).await;
    let entry = store
        .entry(
            &"/module/b".parse().unwrap(),
            &"/last-event".parse().unwrap(),
        )
        .await
        .unwrap();
    let (origin, caller, text) =
        <(Origin, Token, String)>::from_value(entry.value().clone()).unwrap();
    assert_eq!(
        origin,
        Origin::External {
            issuer: "/module/a".parse().unwrap(),
            subject: "/user/admin".parse().unwrap()
        }
    );
    assert_eq!(caller.to_string(), "/module/a");
    assert_eq!(text, "payload");
    assert_eq!(
        call::<_, Vec<bool>>(
            &scheduler,
            "/module/b",
            "/publish",
            &("inherited".to_owned(), None::<Token>)
        )
        .await,
        vec![true]
    );
    observations.until(2).await;
    let entry = store
        .entry(
            &"/module/a".parse().unwrap(),
            &"/last-event".parse().unwrap(),
        )
        .await
        .unwrap();
    let (origin, _, _) = <(Origin, Token, String)>::from_value(entry.value().clone()).unwrap();
    assert_eq!(origin, Origin::Principal("/user/test".parse().unwrap()));
    let mut incompatible = forest_package::read_manifest(&paths[0]).unwrap();
    incompatible.events[0].input = i64::as_schema();
    forest_package::write(&paths[0], &incompatible).unwrap();
    let replacement = scheduler.package(&paths[0]).await.unwrap();
    assert!(
        scheduler
            .reload(replacement, StopMode::Drain)
            .await
            .is_err()
    );
    scheduler.shutdown(StopMode::Drain).await.unwrap();
    store.close().await.unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn io_deadline_covers_response_body_and_host_supplies_entropy_and_monotonic_time() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let temp = tempfile::tempdir().unwrap();
    let scheduler = Scheduler::in_memory(RuntimeOptions {
        data_directory: temp.path().join("data"),
        ..Default::default()
    })
    .await
    .unwrap();
    observe(&scheduler);
    let object = "/module/probe-a";
    let loaded = scheduler
        .package(package(temp.path(), "io", object, false))
        .await
        .unwrap();
    scheduler.load(loaded).await.unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let server = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        let mut request = [0; 2048];
        socket.read(&mut request).await.unwrap();
        socket
            .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 100\r\n\r\nx")
            .await
            .unwrap();
        // Complete headers, deliberately incomplete body. Cancellation must close it.
        assert_eq!(socket.read(&mut request).await.unwrap(), 0);
    });
    assert!(call::<_, bool>(&scheduler, object, "/http-deadline", &(url, 50i64)).await);
    server.await.unwrap();
    let (bytes, before, after): (Vec<u8>, i64, i64) =
        call(&scheduler, object, "/entropy-clock", &()).await;
    assert_eq!(bytes.len(), 32);
    assert!(after >= before + 3);
    assert_eq!(
        scheduler.phase(&object.parse().unwrap()),
        Some(ModulePhase::Serving)
    );
    scheduler.shutdown(StopMode::Drain).await.unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn idle_watch_does_not_hold_drain_and_old_callbacks_do_not_transfer() {
    use futures::SinkExt;
    let temp = tempfile::tempdir().unwrap();
    let scheduler = Scheduler::in_memory(RuntimeOptions {
        data_directory: temp.path().join("data"),
        ..Default::default()
    })
    .await
    .unwrap();
    let (mut entered, gate) = gated_observe(&scheduler);
    let object = "/module/probe-a";
    let loaded = scheduler
        .package(package(temp.path(), "watch", object, false))
        .await
        .unwrap();
    scheduler.load(loaded.clone()).await.unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("ws://{}", listener.local_addr().unwrap());
    let server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        tokio_tungstenite::accept_async(stream).await.unwrap()
    });
    let _: i64 = call(&scheduler, object, "/watch", &url).await;
    let mut socket = server.await.unwrap();
    socket
        .send(tokio_tungstenite::tungstenite::Message::Text("701".into()))
        .await
        .unwrap();
    assert_eq!(entered.recv().await, Some(701));
    // A delivered callback has touched this instance and must finish before Drain.
    let s = scheduler.clone();
    let replacement = loaded.clone();
    let reload = tokio::spawn(async move { s.reload(replacement, StopMode::Drain).await });
    waiting(&scheduler, object).await;
    assert!(!reload.is_finished());
    gate.add_permits(1);
    tokio::time::timeout(Duration::from_secs(10), reload)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(
        call::<_, String>(&scheduler, object, "/echo", &"new".to_owned()).await,
        "new"
    );
    // An idle socket and a far-future timer are native resources, not live guest tasks.
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("ws://{}", listener.local_addr().unwrap());
    let server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        tokio_tungstenite::accept_async(stream).await.unwrap()
    });
    let _: i64 = call(&scheduler, object, "/watch", &url).await;
    let idle = server.await.unwrap();
    let _: i64 = call(&scheduler, object, "/timer", &60_000i64).await;
    tokio::time::timeout(
        Duration::from_secs(10),
        scheduler.reload(loaded, StopMode::Drain),
    )
    .await
    .unwrap()
    .unwrap();
    assert!(entered.try_recv().is_err());
    drop((socket, idle));
    scheduler.shutdown(StopMode::Drain).await.unwrap();
}
