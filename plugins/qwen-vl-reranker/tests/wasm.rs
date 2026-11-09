use axum::{Json, Router, extract::State, http::StatusCode, routing::post};
use forest_config::{ConfigStore, Declaration, Key};
use forest_protocol::{DispatchError, Token};
use forest_qwen_vl_reranker::{manifest, types::*};
use forest_runtime::{ModulePhase, RuntimeOptions, Scheduler, StopMode};
use serde_json::{Value, json};
use std::{
    collections::VecDeque,
    path::PathBuf,
    process::Command,
    sync::{Arc, Mutex},
};
use tokio::sync::oneshot;

struct Reply {
    status: StatusCode,
    body: Value,
    pause: Option<(oneshot::Sender<()>, oneshot::Receiver<()>)>,
}
type Remote = Arc<Mutex<(VecDeque<Reply>, Vec<Value>)>>;
async fn respond(
    State(remote): State<Remote>,
    Json(body): Json<Value>,
) -> (StatusCode, Json<Value>) {
    assert_eq!(body["model"], MODEL);
    assert_eq!(body["return_documents"], false);
    assert_eq!(
        body["query"],
        json!({"image":"data:image/png;base64,query"})
    );
    let reply = {
        let mut state = remote.lock().unwrap();
        state.1.push(body);
        state.0.pop_front().expect("no retries")
    };
    if let Some((started, release)) = reply.pause {
        started.send(()).unwrap();
        release.await.unwrap();
    }
    (reply.status, Json(reply.body))
}
fn response(left: f64, right: f64) -> Value {
    json!({"results":[{"index":1,"relevance_score":right},{"index":0,"relevance_score":left}]})
}
fn input() -> Request {
    Request {
        query: Input::Image("data:image/png;base64,query".into()),
        documents: vec![
            Input::Text("first".into()),
            Input::Image("data:image/png;base64,document".into()),
        ],
    }
}
async fn invoke(scheduler: &Scheduler, request: &Request) -> Result<Scores, Error> {
    scheduler
        .call(
            "/user/admin".parse().unwrap(),
            OBJECT.parse().unwrap(),
            RANK.parse().unwrap(),
            request,
        )
        .await
        .unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn real_wasm_scoring_batches_errors_and_drain_reload() {
    let workspace = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .canonicalize()
        .unwrap();
    assert!(
        Command::new("cargo")
            .args([
                "build",
                "--offline",
                "--locked",
                "-j",
                "2",
                "--target",
                "wasm32-unknown-unknown",
                "-p",
                "forest-qwen-vl-reranker",
                "--lib",
            ])
            .current_dir(&workspace)
            .status()
            .unwrap()
            .success()
    );
    let temp = tempfile::tempdir().unwrap();
    let directory = temp.path().join("package");
    std::fs::create_dir(&directory).unwrap();
    std::fs::write(
        directory.join("manifest.json"),
        serde_json::to_vec(&manifest()).unwrap(),
    )
    .unwrap();
    std::fs::copy(
        workspace.join("target/wasm32-unknown-unknown/debug/forest_qwen_vl_reranker.wasm"),
        directory.join("module.wasm"),
    )
    .unwrap();
    let (started, waiting) = oneshot::channel();
    let (release, blocked) = oneshot::channel();
    let remote: Remote = Arc::new(Mutex::new((
        VecDeque::from([
            Reply {
                status: StatusCode::OK,
                body: response(0.2, 0.9),
                pause: Some((started, blocked)),
            },
            Reply {
                status: StatusCode::OK,
                body: response(0.8, 0.1),
                pause: None,
            },
            Reply {
                status: StatusCode::OK,
                body: response(0.3, 0.7),
                pause: None,
            },
            Reply {
                status: StatusCode::OK,
                body: json!({"results":[{"index":0,"relevance_score":0.5},{"index":0,"relevance_score":0.8}]}),
                pause: None,
            },
            Reply {
                status: StatusCode::PAYMENT_REQUIRED,
                body: json!({"message":"out of quota"}),
                pause: None,
            },
        ]),
        Vec::new(),
    )));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("http://{}/rerank", listener.local_addr().unwrap());
    let app = Router::new()
        .route("/rerank", post(respond))
        .with_state(remote.clone());
    let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    let store = ConfigStore::open(temp.path().join("config.sqlite3"))
        .await
        .unwrap();
    let object: Token = OBJECT.parse().unwrap();
    let declaration = &manifest().configs[0];
    store
        .register(
            object.clone(),
            [Declaration::new(
                declaration.key.clone(),
                declaration.schema.clone(),
                declaration.default.clone(),
            )
            .unwrap()],
        )
        .await
        .unwrap();
    store
        .set(
            &object,
            &Key::new(CONNECTION.parse().unwrap()),
            &Connection {
                endpoint,
                api_key: "test-key".into(),
                batch_size: 2,
            },
        )
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
    let package = scheduler.package(&directory).await.unwrap();
    scheduler.load(package.clone()).await.unwrap();
    assert!(matches!(
        scheduler
            .call::<_, Result<Scores, Error>>(
                "/user/stranger".parse().unwrap(),
                object.clone(),
                RANK.parse().unwrap(),
                &input()
            )
            .await,
        Err(DispatchError::Denied)
    ));
    let empty = invoke(
        &scheduler,
        &Request {
            documents: vec![],
            ..input()
        },
    )
    .await
    .unwrap();
    assert_eq!(empty.json, "[]");
    let mut request = input();
    request.documents.extend(input().documents);
    let current = scheduler.clone();
    let work = tokio::spawn(async move { invoke(&current, &request).await });
    waiting.await.unwrap();
    let current = scheduler.clone();
    let reload =
        tokio::spawn(async move { current.reload(package, StopMode::Drain).await.unwrap() });
    while scheduler.phase(&object) != Some(ModulePhase::Stopping) {
        tokio::task::yield_now().await;
    }
    release.send(()).unwrap();
    let scores: Vec<f64> = serde_json::from_str(&work.await.unwrap().unwrap().json).unwrap();
    assert_eq!(scores, vec![0.2, 0.9, 0.8, 0.1]);
    reload.await.unwrap();
    let scores: Vec<f64> =
        serde_json::from_str(&invoke(&scheduler, &input()).await.unwrap().json).unwrap();
    assert_eq!(scores, vec![0.3, 0.7]);
    assert!(matches!(
        invoke(&scheduler, &input()).await,
        Err(Error::Response(_))
    ));
    let failure = invoke(&scheduler, &input()).await.unwrap_err();
    assert!(
        matches!(failure, Error::Http { status: 402, body, .. } if body.contains("out of quota"))
    );
    assert_eq!(scheduler.phase(&object), Some(ModulePhase::Serving));
    {
        let state = remote.lock().unwrap();
        assert!(state.0.is_empty());
        assert_eq!(state.1.len(), 5);
        assert_eq!(
            state.1[0]["documents"],
            json!([{"text":"first"},{"image":"data:image/png;base64,document"}])
        );
    }
    scheduler.shutdown(StopMode::Drain).await.unwrap();
    server.abort();
    drop(scheduler);
    store.close().await.unwrap();
}
