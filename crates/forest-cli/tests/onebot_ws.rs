use futures::{SinkExt, StreamExt};
use std::{
    path::PathBuf,
    process::{Command, Stdio},
    time::Duration,
};
use tokio_tungstenite::tungstenite::Message;

struct Host(std::process::Child);
impl Drop for Host {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn executable_receives_group_command_and_correlates_onebot_receipt() {
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
                "forest-test",
                "--lib"
            ])
            .status()
            .unwrap()
            .success()
    );
    for restricted in [true, false] {
        run(&root, restricted).await;
    }
}

async fn run(root: &std::path::Path, restricted: bool) {
    let temp = tempfile::tempdir().unwrap();
    let package = temp.path().join("package");
    std::fs::create_dir(&package).unwrap();
    std::fs::copy(
        root.join("target/wasm32-unknown-unknown/debug/forest_test.wasm"),
        package.join("module.wasm"),
    )
    .unwrap();
    std::fs::write(
        package.join("manifest.json"),
        serde_json::to_vec(&forest_test::manifest()).unwrap(),
    )
    .unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let (ready, initialized) = tokio::sync::oneshot::channel();
    let server = tokio::spawn(async move {
        initialized.await.unwrap();
        for round in 1..=2 {
            let (stream, _) = tokio::time::timeout(Duration::from_secs(15), listener.accept())
                .await
                .unwrap()
                .unwrap();
            let mut socket = tokio_tungstenite::accept_async(stream).await.unwrap();
            for group in [7, 42] {
                let unsupported = serde_json::json!({"post_type":"message","message_type":"group","sub_type":"normal","group_id":group,"message":[{"type":"unrecognized_extension","data":{}}]});
                socket
                    .send(Message::Text(unsupported.to_string().into()))
                    .await
                    .unwrap();
            }
            let target = if restricted { 42 } else { 7 };
            let event = serde_json::json!({"time":1,"self_id":10,"post_type":"message","message_type":"group","sub_type":"normal","message_id":round,"group_id":target,"user_id":20,"anonymous":null,
            "message":[{"type":"text","data":{"text":"#forest-test"}}],"raw_message":"#forest-test","sender":{"user_id":20,"nickname":"test"}});
            if restricted {
                let mut excluded = event.clone();
                excluded["group_id"] = 7.into();
                socket
                    .send(Message::Text(excluded.to_string().into()))
                    .await
                    .unwrap();
            }
            socket
                .send(Message::Text(event.to_string().into()))
                .await
                .unwrap();
            let action = tokio::time::timeout(Duration::from_secs(15), socket.next())
                .await
                .unwrap()
                .unwrap()
                .unwrap();
            let action: serde_json::Value = serde_json::from_slice(&action.into_data()).unwrap();
            assert_eq!(action["action"], "send_group_msg");
            assert_eq!(action["params"]["group_id"], target);
            assert_eq!(action["params"]["message"], "hello world");
            socket.send(Message::Text(serde_json::json!({"status":"ok","retcode":0,"data":{"message_id":123},"echo":action["echo"]}).to_string().into())).await.unwrap();
            socket.close(None).await.unwrap();
        }
    });
    let mut command = Command::new(env!("CARGO_BIN_EXE_forest"));
    command
        .current_dir(temp.path())
        .args(["--package"])
        .arg(&package)
        .args([
            "--events-ws",
            &format!("ws://{address}"),
            "--token-env",
            "FOREST_TEST_EMPTY_TOKEN",
        ]);
    if restricted {
        command.args(["--allow-group", "42"]);
    }
    let mut child = Host(
        command
            .stdout(Stdio::null())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap(),
    );
    let stderr = child.0.stderr.take().unwrap();
    let read = tokio::task::spawn_blocking(move || {
        use std::io::{BufRead, BufReader};
        let mut reader = BufReader::new(stderr);
        let mut line = String::new();
        reader.read_line(&mut line).unwrap();
        assert!(line.contains("is Serving"), "{line}");
        reader
    });
    let _reader = tokio::time::timeout(Duration::from_secs(15), read)
        .await
        .unwrap()
        .unwrap();
    ready.send(()).unwrap();
    server.await.unwrap();
    assert!(
        child.0.try_wait().unwrap().is_none(),
        "disconnect must not stop the host"
    );
    let store = forest_config::ConfigStore::open(temp.path().join("data/forest.sqlite3"))
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let entry = store
                .entry(
                    &"/module/forest-test".parse().unwrap(),
                    &"/state/replies".parse().unwrap(),
                )
                .await
                .unwrap();
            if entry.value() == &forest_protocol::Value::I64(2) {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    drop(child);
    store.close().await.unwrap();
}
