use axum::{
    Json, Router,
    extract::{Path, State},
    http::{HeaderMap, StatusCode},
    response::IntoResponse,
    routing::{get, post},
};
use base64::{Engine, engine::general_purpose::STANDARD};
use forest_ai_paint::{
    config::*,
    contract::{Error, Invocation},
};
use forest_config::{ConfigStore, Declaration, Key};
use forest_protocol::{AsSchema, AsValue};
use forest_runtime::{RuntimeOptions, Scheduler, StopMode};
use serde_json::{Value, json};
use std::{
    path::{Path as FilePath, PathBuf},
    process::Command,
    sync::{Arc, Mutex},
};

const PNG: &str =
    "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mP8/x8AAwMCAO+a0FYAAAAASUVORK5CYII=";
type Requests = Arc<Mutex<Vec<(String, HeaderMap, Value)>>>;
#[derive(Clone)]
struct Remote {
    endpoint: String,
    requests: Requests,
}
async fn generate(
    State(state): State<Remote>,
    Path(backend): Path<String>,
    headers: HeaderMap,
    Json(input): Json<Value>,
) -> impl IntoResponse {
    state
        .requests
        .lock()
        .unwrap()
        .push((backend.clone(), headers, input.clone()));
    if input["prompt"].as_str().unwrap().contains("FAIL") {
        return (
            StatusCode::TOO_MANY_REQUESTS,
            Json(json!({"error":"limited"})),
        );
    }
    let body = match backend.as_str() {
        "sf" => json!({"images":[{"url":format!("{}/redirect",state.endpoint)}]}),
        "openai" => json!({"data":[{"b64_json":PNG}]}),
        "openai-url" => json!({"data":[{"url":format!("{}/png",state.endpoint)}]}),
        _ => panic!("unexpected backend"),
    };
    (StatusCode::OK, Json(body))
}
async fn session(
    State(state): State<Remote>,
    headers: HeaderMap,
    Json(input): Json<Value>,
) -> Json<Value> {
    state
        .requests
        .lock()
        .unwrap()
        .push(("session".into(), headers, input));
    Json(json!({"session_id":"test-session"}))
}
async fn swarm(
    State(state): State<Remote>,
    headers: HeaderMap,
    Json(input): Json<Value>,
) -> Json<Value> {
    state
        .requests
        .lock()
        .unwrap()
        .push(("swarm".into(), headers, input));
    Json(json!({"images":[format!("data:image/png;base64,{PNG}")]}))
}
async fn png(headers: HeaderMap) -> impl IntoResponse {
    assert!(!headers.contains_key("authorization"));
    assert!(!headers.contains_key("cookie"));
    (
        [("content-type", "image/png")],
        STANDARD.decode(PNG).unwrap(),
    )
}
fn package(
    root: &FilePath,
    destination: &FilePath,
    artifact: &str,
    manifest: forest_protocol::Manifest,
) {
    std::fs::create_dir(destination).unwrap();
    std::fs::copy(
        root.join(format!(
            "target/wasm32-unknown-unknown/debug/{artifact}.wasm"
        )),
        destination.join("module.wasm"),
    )
    .unwrap();
    std::fs::write(
        destination.join("manifest.json"),
        serde_json::to_vec(&manifest).unwrap(),
    )
    .unwrap();
}
async fn configure<T: AsValue + AsSchema>(store: &ConfigStore, key: &str, value: &T) {
    store
        .set(
            &forest_ai_paint::OBJECT.parse().unwrap(),
            &Key::new(key.parse().unwrap()),
            value,
        )
        .await
        .unwrap();
}
async fn invoke(scheduler: &Scheduler, args: Value) -> Result<String, Error> {
    scheduler
        .call(
            "/module/agent-tools".parse().unwrap(),
            forest_ai_paint::OBJECT.parse().unwrap(),
            "/generate".parse().unwrap(),
            &Invocation {
                group_id: 42,
                bot_id: 10,
                name: "image_generate".into(),
                arguments_json: args.to_string(),
            },
        )
        .await
        .unwrap()
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn wasm_painting_adapters_return_persistent_media_without_credentials_or_inline_bytes() {
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
                "-j",
                "2",
                "--target",
                "wasm32-unknown-unknown",
                "-p",
                "forest-ai-paint",
                "-p",
                "forest-media",
                "--lib"
            ])
            .current_dir(&root)
            .status()
            .unwrap()
            .success()
    );
    let temp = tempfile::tempdir().unwrap();
    let media = temp.path().join("media");
    let painting = temp.path().join("painting");
    package(&root, &media, "forest_media", forest_media::manifest());
    let manifest = forest_ai_paint::manifest();
    package(&root, &painting, "forest_ai_paint", manifest.clone());
    let store = ConfigStore::open(temp.path().join("config.sqlite3"))
        .await
        .unwrap();
    store
        .register(
            manifest.package.id.clone(),
            manifest
                .configs
                .iter()
                .map(|d| {
                    Declaration::new(d.key.clone(), d.schema.clone(), d.default.clone()).unwrap()
                })
                .collect::<Vec<_>>(),
        )
        .await
        .unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("http://{}", listener.local_addr().unwrap());
    let requests = Requests::default();
    let server_state = Remote {
        endpoint: endpoint.clone(),
        requests: requests.clone(),
    };
    let app = Router::new()
        .route("/v/{backend}", post(generate))
        .route("/png", get(png))
        .route(
            "/redirect",
            get(|| async { (StatusCode::TEMPORARY_REDIRECT, [("location", "/png")]) }),
        )
        .route("/API/GetNewSession", post(session))
        .route("/API/GenerateText2Image", post(swarm))
        .with_state(server_state);
    let server = tokio::spawn(async { axum::serve(listener, app).await.unwrap() });
    configure(
        &store,
        SILICONFLOW,
        &SiliconFlow {
            endpoint: format!("{endpoint}/v/sf"),
            api_key: "test-secret".into(),
            ..Default::default()
        },
    )
    .await;
    let scheduler = Scheduler::with_store(
        store.clone(),
        RuntimeOptions {
            data_directory: temp.path().join("data"),
            ..RuntimeOptions::default()
        },
    )
    .await
    .unwrap();
    let painting = scheduler.package(&painting).await.unwrap();
    let media = scheduler.package(&media).await.unwrap();
    scheduler
        .load_roots(
            vec![painting.clone(), media.clone()],
            &[forest_ai_paint::OBJECT.parse().unwrap()],
        )
        .await
        .unwrap();
    let response = invoke(
        &scheduler,
        json!({"prompt":"a cat","size":"portrait","seed":123}),
    )
    .await
    .unwrap();
    assert!(
        !response.contains("base64")
            && !response.contains("test-secret")
            && !response.contains(&endpoint)
    );
    let result: Value = serde_json::from_str(&response).unwrap();
    let reference = result["reference"].as_str().unwrap().to_owned();
    assert_eq!(result["cq"], format!("[CQ:image,file={reference}]"));
    {
        let requests = requests.lock().unwrap();
        let (_, headers, body) = &requests[0];
        assert_eq!(headers["authorization"], "Bearer test-secret");
        assert_eq!(body["model"], "Kwai-Kolors/Kolors");
        assert_eq!(body["image_size"], "720x1440");
        assert_eq!(body["prompt"], "anime style, a cat");
        assert_eq!(body["seed"], 123);
    }
    let count = requests.lock().unwrap().len();
    assert!(
        matches!(invoke(&scheduler,json!({"prompt":"FAIL"})).await,Err(Error::Execution(e)) if e.contains("429"))
    );
    assert_eq!(requests.lock().unwrap().len(), count + 1);
    assert!(matches!(
        invoke(&scheduler, json!({"prompt":" "})).await,
        Err(Error::Arguments(_))
    ));
    assert_eq!(requests.lock().unwrap().len(), count + 1);
    configure(&store, BACKEND, &Backend::OpenAiImages).await;
    let mut cloud = OpenAiImages {
        endpoint: format!("{endpoint}/v/openai"),
        api_key: "test-secret".into(),
        ..Default::default()
    };
    configure(&store, OPENAI_IMAGES, &cloud).await;
    scheduler
        .reload(painting.clone(), StopMode::Drain)
        .await
        .unwrap();
    invoke(
        &scheduler,
        json!({"prompt":"a cat","style":"photo","size":"landscape"}),
    )
    .await
    .unwrap();
    {
        let requests = requests.lock().unwrap();
        let (_, _, body) = requests.last().unwrap();
        assert_eq!(body["size"], "16x9");
        assert_eq!(body["n"], 1);
        assert_eq!(body["prompt"], "photo-realistic, a cat");
    }
    cloud.endpoint = format!("{endpoint}/v/openai-url");
    configure(&store, OPENAI_IMAGES, &cloud).await;
    scheduler
        .reload(painting.clone(), StopMode::Drain)
        .await
        .unwrap();
    invoke(&scheduler, json!({"prompt":"a cat","style":"none"}))
        .await
        .unwrap();
    let count = requests.lock().unwrap().len();
    assert!(matches!(
        invoke(&scheduler, json!({"prompt":"a cat","seed":1})).await,
        Err(Error::Arguments(_))
    ));
    assert_eq!(requests.lock().unwrap().len(), count);
    configure(&store, BACKEND, &Backend::SwarmUi).await;
    configure(
        &store,
        SWARM_UI,
        &SwarmUi {
            endpoint,
            auth_token: "swarm-secret".into(),
            anime_model: "anime-model".into(),
            ..Default::default()
        },
    )
    .await;
    scheduler.reload(painting, StopMode::Drain).await.unwrap();
    invoke(&scheduler, json!({"prompt":"a cat","size":"portrait"}))
        .await
        .unwrap();
    {
        let requests = requests.lock().unwrap();
        let (kind, headers, body) = requests.last().unwrap();
        assert_eq!(kind, "swarm");
        assert_eq!(headers["cookie"], "swarm_token=swarm-secret");
        assert_eq!(body["session_id"], "test-session");
        assert_eq!(body["width"], 720);
        assert_eq!(body["height"], 1440);
        assert_eq!(body["model"], "anime-model");
    }
    scheduler
        .unload(&forest_ai_paint::OBJECT.parse().unwrap(), StopMode::Drain)
        .await
        .unwrap();
    scheduler.reload(media, StopMode::Drain).await.unwrap();
    let blob = scheduler
        .call::<_, Result<forest_media::contract::Blob, forest_media::contract::Error>>(
            "/user/admin".parse().unwrap(),
            "/module/media".parse().unwrap(),
            "/get".parse().unwrap(),
            &reference,
        )
        .await
        .unwrap()
        .unwrap();
    assert_eq!(blob.bytes, STANDARD.decode(PNG).unwrap());
    scheduler.shutdown(StopMode::Drain).await.unwrap();
    store.close().await.unwrap();
    server.abort();
}
