use forest_onebot::{Client, http::HttpTransport};
use forest_protocol::{SubjectPattern, Token};
use forest_runtime::{RuntimeOptions, Scheduler, StopMode, onebot};
use std::{path::PathBuf, process::Command, sync::OnceLock, time::Duration};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
    sync::oneshot,
};

fn artifact() -> &'static PathBuf {
    static ARTIFACT: OnceLock<PathBuf> = OnceLock::new();
    ARTIFACT.get_or_init(|| {
        let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("../..")
            .canonicalize()
            .unwrap();
        let status = Command::new("cargo")
            .args([
                "build",
                "--offline",
                "--locked",
                "--target",
                "wasm32-unknown-unknown",
                "-p",
                "forest-test",
                "--lib",
            ])
            .current_dir(&root)
            .env("CARGO_BUILD_JOBS", "2")
            .status()
            .unwrap();
        assert!(status.success(), "build the actual SDK fixture");
        root.join("target/wasm32-unknown-unknown/debug/forest_test.wasm")
    })
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn group_command_uses_real_wasm_native_acl_http_and_persistent_configuration() {
    let temp = tempfile::tempdir().unwrap();
    let package = temp.path().join("package");
    std::fs::create_dir(&package).unwrap();
    std::fs::copy(artifact(), package.join("module.wasm")).unwrap();
    std::fs::write(
        package.join("manifest.json"),
        serde_json::to_vec(&forest_test::manifest()).unwrap(),
    )
    .unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap())
        .parse()
        .unwrap();
    let (sent, received) = oneshot::channel();
    let server = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        let mut bytes = Vec::new();
        let mut chunk = [0u8; 2048];
        let (header_end, length) = loop {
            let count = stream.read(&mut chunk).await.unwrap();
            assert!(count > 0);
            bytes.extend_from_slice(&chunk[..count]);
            if let Some(end) = bytes.windows(4).position(|part| part == b"\r\n\r\n") {
                let headers = std::str::from_utf8(&bytes[..end]).unwrap();
                assert!(headers.starts_with("POST /send_group_msg "));
                let length = headers
                    .lines()
                    .find_map(|line| {
                        let (name, value) = line.split_once(':')?;
                        name.eq_ignore_ascii_case("content-length")
                            .then(|| value.trim().parse::<usize>().unwrap())
                    })
                    .unwrap();
                break (end + 4, length);
            }
        };
        while bytes.len() < header_end + length {
            let count = stream.read(&mut chunk).await.unwrap();
            assert!(count > 0);
            bytes.extend_from_slice(&chunk[..count]);
        }
        let body: serde_json::Value =
            serde_json::from_slice(&bytes[header_end..header_end + length]).unwrap();
        sent.send(body).unwrap();
        let response = br#"{"status":"ok","retcode":0,"data":{"message_id":123}}"#;
        stream.write_all(format!("HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",response.len()).as_bytes()).await.unwrap();
        stream.write_all(response).await.unwrap();
    });
    let database = temp.path().join("forest.sqlite3");
    let scheduler = Scheduler::new(
        &database,
        RuntimeOptions {
            data_directory: temp.path().join("modules"),
            ..RuntimeOptions::default()
        },
    )
    .await
    .unwrap();
    let client = reqwest::Client::builder().no_proxy().build().unwrap();
    let ingress = onebot::install(
        &scheduler,
        Client::new(HttpTransport::with_client(url, None, client).unwrap()),
        onebot::GroupScope::only([42]),
        vec![SubjectPattern::Path("/module/forest-test".parse().unwrap())],
    )
    .unwrap();
    // The optional test scope still blocks outgoing messages before transport.
    let denied = scheduler
        .call::<_, Result<forest_onebot::forest::SentMessage, forest_onebot::forest::OneBotError>>(
            "/module/forest-test".parse().unwrap(),
            "/core/onebot".parse().unwrap(),
            "/send_group_message".parse().unwrap(),
            &forest_onebot::forest::SendGroupMessage {
                group_id: 7,
                message: forest_onebot::message::Message::Cq("not sent".into()),
                auto_escape: Some(true),
            },
        )
        .await
        .unwrap();
    assert!(matches!(
        denied,
        Err(forest_onebot::forest::OneBotError::GroupDenied)
    ));
    scheduler
        .load(scheduler.package(&package).await.unwrap())
        .await
        .unwrap();
    let event = serde_json::json!({"time":1,"self_id":10,"post_type":"message","message_type":"group","sub_type":"normal", "message_id":1,"group_id":42,"user_id":20,"anonymous":null,
        "message":[{"type":"text","data":{"text":"#forest-test"}}],"raw_message":"#forest-test","sender":{"user_id":20,"nickname":"test user"}});
    let report = ingress
        .receive(&serde_json::to_vec(&event).unwrap())
        .unwrap();
    assert_eq!(report.accepted, 1);
    assert!(report.rejected.is_empty());
    let body = tokio::time::timeout(Duration::from_secs(15), received)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(body["group_id"], 42);
    assert_eq!(body["message"], "hello world");
    assert_eq!(body["auto_escape"], true);
    server.await.unwrap();
    scheduler
        .unload(&"/module/forest-test".parse().unwrap(), StopMode::Drain)
        .await
        .unwrap();
    let store = forest_config::ConfigStore::open(&database).await.unwrap();
    let entry = store
        .entry(
            &"/module/forest-test".parse::<Token>().unwrap(),
            &"/state/replies".parse().unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(entry.value(), &forest_protocol::Value::I64(1));
    store.close().await.unwrap();
}
