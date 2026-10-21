use axum::{Json, Router, extract::State, http::StatusCode, routing::post};
use forest_config::{ConfigStore, Declaration, Key};
use forest_protocol::{DispatchError, Token};
use forest_qwen_vl_embedding::manifest;
use forest_qwen_vl_embedding::types::{
    CONNECTION, Connection, DIMENSIONS, EMBED, Embeddings, Error, Input, MODEL, OBJECT,
};
use forest_runtime::{ModulePhase, RuntimeOptions, Scheduler, StopMode};
use serde_json::{Value, json};
use std::{
    collections::VecDeque,
    path::PathBuf,
    process::Command,
    sync::{Arc, Mutex},
};
use tokio::{net::TcpListener, sync::oneshot};

struct Reply {
    status: StatusCode,
    body: Value,
    started: Option<oneshot::Sender<()>>,
    release: Option<oneshot::Receiver<()>>,
}

type Replies = Arc<Mutex<VecDeque<Reply>>>;

async fn respond(
    State(replies): State<Replies>,
    Json(input): Json<Value>,
) -> (StatusCode, Json<Value>) {
    assert_eq!(input["model"], MODEL);
    assert_eq!(input["dimensions"], DIMENSIONS);
    assert_eq!(input["encoding_format"], "float");
    assert_eq!(input["input"][0], json!({"text":"a dog"}));
    assert_eq!(
        input["input"][1],
        json!({"image":"https://example.com/dog.png"})
    );
    let reply = replies
        .lock()
        .unwrap()
        .pop_front()
        .expect("no automatic retries");
    if let Some(started) = reply.started {
        started.send(()).unwrap();
    }
    if let Some(release) = reply.release {
        release.await.unwrap();
    }
    (reply.status, Json(reply.body))
}

fn success() -> Value {
    let mut first = vec![0.0f32; DIMENSIONS];
    first[0] = 3.0;
    first[1] = 4.0;
    let mut second = vec![0.0f32; DIMENSIONS];
    second[1] = 2.0;
    json!({"model":MODEL,"data":[
        {"index":1,"embedding":second},
        {"index":0,"embedding":first}
    ]})
}

async fn invoke(scheduler: &Scheduler) -> Result<Embeddings, Error> {
    scheduler
        .call(
            "/user/admin".parse().unwrap(),
            OBJECT.parse().unwrap(),
            EMBED.parse().unwrap(),
            &vec![
                Input::Text("a dog".into()),
                Input::Image("https://example.com/dog.png".into()),
            ],
        )
        .await
        .unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn actual_wasm_http_json_vectors_and_reload() {
    tokio::time::timeout(std::time::Duration::from_secs(90), scenario())
        .await
        .unwrap();
}

async fn scenario() {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .canonicalize()
        .unwrap();
    assert!(
        Command::new("cargo")
            .args([
                "build",
                "--offline",
                "--locked",
                "--target",
                "wasm32-unknown-unknown",
                "-p",
                "forest-qwen-vl-embedding",
                "--lib",
            ])
            .env("CARGO_BUILD_JOBS", "2")
            .current_dir(&root)
            .status()
            .unwrap()
            .success()
    );
    let temp = tempfile::tempdir().unwrap();
    let directory = temp.path().join("package");
    std::fs::create_dir(&directory).unwrap();
    std::fs::copy(
        root.join("target/wasm32-unknown-unknown/debug/forest_qwen_vl_embedding.wasm"),
        directory.join("module.wasm"),
    )
    .unwrap();
    std::fs::write(
        directory.join("manifest.json"),
        serde_json::to_vec(&manifest()).unwrap(),
    )
    .unwrap();

    let (started, received) = oneshot::channel();
    let (release, released) = oneshot::channel();
    let replies = Arc::new(Mutex::new(VecDeque::from([
        Reply {
            status: StatusCode::OK,
            body: success(),
            started: Some(started),
            release: Some(released),
        },
        Reply {
            status: StatusCode::OK,
            body: success(),
            started: None,
            release: None,
        },
        Reply {
            status: StatusCode::BAD_REQUEST,
            body: json!({"message":"bad input"}),
            started: None,
            release: None,
        },
        Reply {
            status: StatusCode::OK,
            body: json!({"model":MODEL,"data":[{"index":0,"embedding":[1.0]},{"index":1,"embedding":[1.0]}]}),
            started: None,
            release: None,
        },
    ])));
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("http://{}/embed", listener.local_addr().unwrap());
    let (shutdown, stopped) = oneshot::channel();
    let app = Router::new()
        .route("/embed", post(respond))
        .with_state(replies.clone());
    let server = tokio::spawn(async {
        axum::serve(listener, app)
            .with_graceful_shutdown(async {
                let _ = stopped.await;
            })
            .await
            .unwrap();
    });

    let store = ConfigStore::open(temp.path().join("config.sqlite3"))
        .await
        .unwrap();
    let object: Token = OBJECT.parse().unwrap();
    let config = &manifest().configs[0];
    store
        .register(
            object.clone(),
            [Declaration::new(
                config.key.clone(),
                config.schema.clone(),
                config.default.clone(),
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
            },
        )
        .await
        .unwrap();
    let scheduler = Scheduler::with_store(
        store.clone(),
        RuntimeOptions {
            data_directory: temp.path().join("modules"),
            ..RuntimeOptions::default()
        },
    )
    .await
    .unwrap();
    let package = scheduler.package(&directory).await.unwrap();
    scheduler.load(package.clone()).await.unwrap();
    assert!(matches!(
        scheduler
            .call::<_, Result<Embeddings, Error>>(
                "/user/stranger".parse().unwrap(),
                object.clone(),
                EMBED.parse().unwrap(),
                &Vec::<Input>::new()
            )
            .await,
        Err(DispatchError::Denied)
    ));
    let empty: Result<Embeddings, Error> = scheduler
        .call(
            "/user/admin".parse().unwrap(),
            object.clone(),
            EMBED.parse().unwrap(),
            &Vec::<Input>::new(),
        )
        .await
        .unwrap();
    assert!(empty.unwrap().vectors().unwrap().is_empty());

    let first_scheduler = scheduler.clone();
    let first = tokio::spawn(async move { invoke(&first_scheduler).await });
    received.await.unwrap();
    let reload_scheduler = scheduler.clone();
    let reload = tokio::spawn(async move {
        reload_scheduler
            .reload(package, StopMode::Drain)
            .await
            .unwrap();
    });
    while scheduler.phase(&object) != Some(ModulePhase::Stopping) {
        tokio::task::yield_now().await;
    }
    let second_scheduler = scheduler.clone();
    let second = tokio::spawn(async move { invoke(&second_scheduler).await });
    release.send(()).unwrap();
    let vectors = first.await.unwrap().unwrap().vectors().unwrap();
    assert_eq!(vectors.len(), 2);
    assert_eq!(vectors[0].len(), DIMENSIONS);
    assert!((vectors[0][0] - 0.6).abs() < 1e-6);
    assert!((vectors[0][1] - 0.8).abs() < 1e-6);
    assert_eq!(vectors[1][1], 1.0);
    reload.await.unwrap();
    assert_eq!(second.await.unwrap().unwrap().vectors().unwrap(), vectors);
    assert_eq!(
        invoke(&scheduler).await.unwrap_err(),
        Error::Http {
            status: 400,
            request_id: None
        }
    );
    assert_eq!(
        invoke(&scheduler).await.unwrap_err(),
        Error::Dimensions {
            index: 0,
            actual: 1
        }
    );
    assert_eq!(scheduler.phase(&object), Some(ModulePhase::Serving));
    assert!(replies.lock().unwrap().is_empty());
    scheduler.unload(&object, StopMode::Drain).await.unwrap();
    drop(scheduler);
    store.close().await.unwrap();
    shutdown.send(()).unwrap();
    server.await.unwrap();
}
