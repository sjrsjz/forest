use forest_onebot::forest::{self as api, OneBotError, Query};
use forest_protocol::AsValue;
use forest_runtime::{RuntimeOptions, Scheduler, StopMode};
use futures::{SinkExt, StreamExt};
use std::{path::PathBuf, process::Command, time::Duration};
use tokio_tungstenite::tungstenite::Message;

async fn connected(scheduler: &Scheduler, count: usize) {
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let entries = scheduler.logs().reader().after(None).entries;
            if entries
                .iter()
                .filter(|e| e.message == "OneBot connected")
                .count()
                >= count
            {
                return;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
}
async fn query(
    scheduler: &Scheduler,
) -> Result<Result<String, OneBotError>, forest_protocol::DispatchError> {
    scheduler
        .call(
            "/user/admin".parse().unwrap(),
            api::OBJECT.parse().unwrap(),
            api::QUERY.parse().unwrap(),
            &Query::LoginInfo,
        )
        .await
}

#[tokio::test(flavor = "current_thread")]
async fn receipt_loss_reconnect_reload_and_force_use_only_generic_host_resources() {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .canonicalize()
        .unwrap();
    assert!(
        Command::new("cargo")
            .current_dir(&root)
            .env("CARGO_BUILD_JOBS", "2")
            .args([
                "build",
                "--locked",
                "--offline",
                "--target",
                "wasm32-unknown-unknown",
                "-p",
                "forest-onebot-module",
                "--lib"
            ])
            .status()
            .unwrap()
            .success()
    );
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("onebot");
    std::fs::create_dir(&path).unwrap();
    std::fs::copy(
        root.join("target/wasm32-unknown-unknown/debug/forest_onebot_module.wasm"),
        path.join("module.wasm"),
    )
    .unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mut manifest = forest_onebot_module::manifest();
    manifest.configs[0].default = forest_onebot_module::Settings {
        websocket: format!("ws://{}", listener.local_addr().unwrap()),
        action_timeout_ms: 5000,
        reconnect_min_ms: 20,
        reconnect_max_ms: 20,
        ..Default::default()
    }
    .as_value();
    forest_package::write(&path, &manifest).unwrap();
    let (sent, received) = tokio::sync::oneshot::channel();
    let server = tokio::spawn(async move {
        let mut sent = Some(sent);
        for round in 1..=3 {
            let (stream, _) = listener.accept().await.unwrap();
            let mut socket = tokio_tungstenite::accept_async(stream).await.unwrap();
            let action = socket.next().await.unwrap().unwrap();
            let action: serde_json::Value = serde_json::from_slice(&action.into_data()).unwrap();
            assert_eq!(action["action"], "get_login_info");
            if round == 1 {
                // The request may have reached the server. Never replay it.
                socket.close(None).await.unwrap();
                continue;
            }
            socket.send(Message::Text(serde_json::json!({"status":"ok","retcode":0,"data":{"user_id":10,"nickname":"test"},"echo":action["echo"]}).to_string().into())).await.unwrap();
            if round == 3 {
                let action = socket.next().await.unwrap().unwrap();
                let action: serde_json::Value =
                    serde_json::from_slice(&action.into_data()).unwrap();
                assert_eq!(action["action"], "get_login_info");
                sent.take().unwrap().send(()).unwrap();
            }
            // Drain reload / Force must drop the old listener and connection.
            assert!(!matches!(socket.next().await, Some(Ok(Message::Text(_)))));
        }
    });
    let scheduler = Scheduler::in_memory(RuntimeOptions {
        max_requests: 1,
        data_directory: temp.path().join("data"),
        ..Default::default()
    })
    .await
    .unwrap();
    let loaded = scheduler.package(path).await.unwrap();
    scheduler.load(loaded.clone()).await.unwrap();
    connected(&scheduler, 1).await;
    assert!(matches!(
        query(&scheduler).await.unwrap(),
        Err(OneBotError::ReceiptLost)
    ));
    connected(&scheduler, 2).await;
    assert!(query(&scheduler).await.unwrap().unwrap().contains("test"));
    tokio::time::timeout(
        Duration::from_secs(10),
        scheduler.reload(loaded, StopMode::Drain),
    )
    .await
    .unwrap()
    .unwrap();
    connected(&scheduler, 3).await;
    assert!(query(&scheduler).await.unwrap().unwrap().contains("test"));
    let s = scheduler.clone();
    let pending = tokio::spawn(async move { query(&s).await });
    received.await.unwrap();
    scheduler
        .unload(&api::OBJECT.parse().unwrap(), StopMode::Force)
        .await
        .unwrap();
    assert!(matches!(
        pending.await.unwrap(),
        Err(forest_protocol::DispatchError::Interrupted)
    ));
    tokio::time::timeout(Duration::from_secs(5), server)
        .await
        .unwrap()
        .unwrap();
    scheduler.shutdown(StopMode::Drain).await.unwrap();
}
