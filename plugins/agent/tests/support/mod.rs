use axum::{
    Json, Router,
    extract::{Query, State},
    http::{HeaderMap, StatusCode},
    routing::{get, post, put},
};
use forest_config::{ConfigStore, Declaration, Key};
use forest_onebot::forest::{
    self as onebot, GroupMessage, GroupSender, OneBotError, SendGroupMessage, SentMessage,
};
use forest_onebot::message::{Message, Segment};
use forest_protocol::{AsSchema, AsValue, SubjectPattern, manifest::callers};
use forest_runtime::{EventDefinition, NativeInterface, Package, RuntimeOptions, Scheduler};
use serde_json::{Value, json};
use std::{
    collections::{BTreeMap, VecDeque},
    path::PathBuf,
    process::Command,
    sync::{Arc, Mutex},
};
use tokio::sync::{mpsc, oneshot};

pub struct Reply {
    pub body: Value,
    pub status: StatusCode,
    pub started: Option<oneshot::Sender<()>>,
    pub release: Option<oneshot::Receiver<()>>,
}
impl Reply {
    pub fn text(text: &str) -> Self {
        Self {
            body: json!({"id":"test","model":"test","created":1700000000,"choices":[{"index":0,"finish_reason":"stop","message":{"role":"assistant","content":text,"reasoning_content":"PRIVATE_THOUGHT"}}]}),
            status: StatusCode::OK,
            started: None,
            release: None,
        }
    }
}
#[derive(Default)]
pub struct Remote {
    pub replies: BTreeMap<i64, VecDeque<Reply>>,
    pub requests: Vec<(i64, Value)>,
    pub embeddings: Vec<Value>,
    pub web_requests: Vec<(String, Value)>,
    pub wolfram_requests: Vec<BTreeMap<String, String>>,
    pub points: BTreeMap<String, Value>,
    pub collection: bool,
    pub seed_filter: Option<(i64, i64)>,
    pub score: Option<f64>,
    pub slow: Option<(oneshot::Sender<()>, oneshot::Receiver<()>)>,
}
pub type Shared = Arc<Mutex<Remote>>;
async fn chat(State(remote): State<Shared>, Json(body): Json<Value>) -> (StatusCode, Json<Value>) {
    let system = body["messages"][0]["content"].as_str().unwrap();
    let group = system
        .split("<conversation group_id=\"")
        .nth(1)
        .unwrap()
        .split('"')
        .next()
        .unwrap()
        .parse()
        .unwrap();
    let reply = {
        let mut r = remote.lock().unwrap();
        r.requests.push((group, body));
        r.replies
            .get_mut(&group)
            .unwrap()
            .pop_front()
            .expect("unexpected model request / retry")
    };
    if let Some(started) = reply.started {
        let _ = started.send(());
    }
    if let Some(release) = reply.release {
        let _ = release.await;
    }
    (reply.status, Json(reply.body))
}
async fn embeddings(
    State(remote): State<Shared>,
    Json(body): Json<Value>,
) -> (StatusCode, Json<Value>) {
    remote.lock().unwrap().embeddings.push(body.clone());
    if body.to_string().contains("EMBED_FAIL") {
        return (StatusCode::SERVICE_UNAVAILABLE, Json(json!({})));
    }
    let mut vector = vec![0.0; 2048];
    vector[0] = 1.0;
    (
        StatusCode::OK,
        Json(
            json!({"model":"Qwen/Qwen3-VL-Embedding-8B","data":body["input"].as_array().unwrap().iter().enumerate().map(|(index,_)|json!({"index":index,"embedding":vector})).collect::<Vec<_>>()}),
        ),
    )
}
async fn collection(State(remote): State<Shared>) -> (StatusCode, Json<Value>) {
    if remote.lock().unwrap().collection {
        (
            StatusCode::OK,
            Json(
                json!({"result":{"config":{"params":{"vectors":{"size":2048,"distance":"Cosine"}}}}}),
            ),
        )
    } else {
        (StatusCode::NOT_FOUND, Json(json!({})))
    }
}
async fn create(State(remote): State<Shared>) -> Json<Value> {
    remote.lock().unwrap().collection = true;
    Json(json!({"result":true}))
}
async fn upsert(State(remote): State<Shared>, Json(body): Json<Value>) -> Json<Value> {
    let mut r = remote.lock().unwrap();
    for point in body["points"].as_array().unwrap() {
        r.points
            .insert(point["id"].as_str().unwrap().into(), point.clone());
    }
    Json(json!({"result":{"status":"completed"}}))
}
async fn query(State(remote): State<Shared>, Json(body): Json<Value>) -> Json<Value> {
    let r = remote.lock().unwrap();
    Json(
        json!({"result":{"points":r.points.values().filter(|p|r.seed_filter.is_none_or(|(g,s)|p["payload"]["group_id"]==g&&p["payload"]["sequence"]==s)).take(body["limit"].as_u64().unwrap() as usize).map(|p|json!({"id":p["id"],"payload":p["payload"],"score":r.score.unwrap_or(1.0)})).collect::<Vec<_>>()}}),
    )
}
async fn slow(State(remote): State<Shared>) -> &'static str {
    let (started, release) = remote
        .lock()
        .unwrap()
        .slow
        .take()
        .expect("one external operation");
    let _ = started.send(());
    let _ = release.await;
    "TOOL_RESULT"
}

async fn web_search(
    State(remote): State<Shared>,
    headers: HeaderMap,
    Json(body): Json<Value>,
) -> (StatusCode, Json<Value>) {
    assert_eq!(headers["authorization"], "Bearer test");
    remote
        .lock()
        .unwrap()
        .web_requests
        .push(("search".into(), body.clone()));
    match body["query"].as_str().unwrap() {
        "FAIL" => (
            StatusCode::TOO_MANY_REQUESTS,
            Json(json!({"error":{"message":"limited"}})),
        ),
        "EMPTY" => (StatusCode::OK, Json(json!({"results":[]}))),
        "MALFORMED" => (
            StatusCode::OK,
            Json(json!({"results":[{"url":"https://example.com"}]})),
        ),
        _ => (
            StatusCode::OK,
            Json(json!({"results":[{
                "rerank_score":2.4,"title":"Web source","url":"https://example.com/article",
                "snippet":"short","summary":"relevant summary",
                "content":if body["need_content"]==true {"full content"} else {""},
                "site_name":null,"site_icon":null,"date":null,"authority":3
            }]})),
        ),
    }
}

async fn web_fetch(
    State(remote): State<Shared>,
    headers: HeaderMap,
    Json(body): Json<Value>,
) -> Json<Value> {
    assert_eq!(headers["authorization"], "Bearer test");
    remote
        .lock()
        .unwrap()
        .web_requests
        .push(("fetch".into(), body));
    Json(json!({"title":"Web source","content":"# Article\nExtracted Markdown"}))
}
async fn wolfram(
    State(remote): State<Shared>,
    headers: HeaderMap,
    Query(params): Query<BTreeMap<String, String>>,
) -> (StatusCode, &'static str) {
    assert_eq!(headers["authorization"], "Bearer wolfram-test");
    assert!(!params.contains_key("appid"));
    let query = params["input"].clone();
    remote.lock().unwrap().wolfram_requests.push(params);
    match query.as_str() {
        "UNKNOWN" => (
            StatusCode::NOT_IMPLEMENTED,
            "Try a different interpretation.",
        ),
        "DENIED" => (
            StatusCode::FORBIDDEN,
            "private provider detail: wolfram-test",
        ),
        "EMPTY" => (StatusCode::OK, " "),
        _ => (StatusCode::OK, "Result:\npi^3/3 + 2"),
    }
}

pub struct Harness {
    pub scheduler: Scheduler,
    pub store: ConfigStore,
    pub remote: Shared,
    pub sent: mpsc::UnboundedReceiver<SendGroupMessage>,
    pub packages: BTreeMap<String, Package>,
    pub endpoint: String,
    pub _root: tempfile::TempDir,
    server: tokio::task::JoinHandle<()>,
}
impl Harness {
    pub async fn new() -> Self {
        let workspace = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..");
        let crates = [
            "forest-memory-module",
            "forest-agent-module",
            "forest-agent-tools",
            "forest-deepseek-web",
            "forest-wolfram-alpha",
            "forest-context-module",
            "forest-openai-provider",
            "forest-qwen-vl-embedding",
        ];
        let mut command = Command::new("cargo");
        command.args([
            "build",
            "--offline",
            "--locked",
            "-j",
            "2",
            "--target",
            "wasm32-unknown-unknown",
            "--lib",
        ]);
        for name in crates {
            command.args(["-p", name]);
        }
        assert!(command.current_dir(&workspace).status().unwrap().success());
        let root = tempfile::tempdir().unwrap();
        let remote = Shared::default();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("http://{}", listener.local_addr().unwrap());
        let app = Router::new()
            .route("/chat", post(chat))
            .route("/embeddings", post(embeddings))
            .route("/slow", get(slow))
            .route("/v1/tools/search", post(web_search))
            .route("/v1/tools/fetch", post(web_fetch))
            .route("/wolfram", get(wolfram))
            .route("/collections/{name}", get(collection).put(create))
            .route("/collections/{name}/points", put(upsert))
            .route("/collections/{name}/points/query", post(query))
            .with_state(remote.clone());
        let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        let store = ConfigStore::open(root.path().join("config.sqlite3"))
            .await
            .unwrap();
        let scheduler = Scheduler::with_store(
            store.clone(),
            RuntimeOptions {
                data_directory: root.path().join("modules"),
                ..Default::default()
            },
        )
        .await
        .unwrap();
        let (tx, sent) = mpsc::unbounded_channel();
        scheduler
            .register_native(NativeInterface::new(
                onebot::OBJECT.parse().unwrap(),
                onebot::SEND_GROUP_MESSAGE.parse().unwrap(),
                true,
                callers(vec![SubjectPattern::Path("/module/agent".parse().unwrap())]),
                move |_, input: SendGroupMessage| {
                    let tx = tx.clone();
                    async move {
                        tx.send(input).unwrap();
                        Ok::<_, OneBotError>(SentMessage { message_id: 999 })
                    }
                },
            ))
            .unwrap();
        scheduler
            .register_event(EventDefinition {
                source: onebot::OBJECT.parse().unwrap(),
                event: onebot::GROUP_MESSAGE.parse().unwrap(),
                input: GroupMessage::as_schema(),
            })
            .unwrap();
        let manifests = [
            forest_qwen_vl_embedding::manifest(),
            forest_openai_provider::manifest(),
            forest_context_module::manifest(),
            forest_memory_module::manifest(),
            forest_agent_tools::manifest(),
            forest_deepseek_web::manifest(),
            forest_wolfram_alpha::manifest(),
            forest_agent_module::manifest(),
        ];
        let mut packages = BTreeMap::new();
        for (manifest, artifact) in manifests.into_iter().zip([
            "forest_qwen_vl_embedding",
            "forest_openai_provider",
            "forest_context_module",
            "forest_memory_module",
            "forest_agent_tools",
            "forest_deepseek_web",
            "forest_wolfram_alpha",
            "forest_agent_module",
        ]) {
            let id = manifest.package.id.clone();
            store
                .register(
                    id.clone(),
                    manifest
                        .configs
                        .iter()
                        .map(|d| {
                            Declaration::new(d.key.clone(), d.schema.clone(), d.default.clone())
                                .unwrap()
                        })
                        .collect::<Vec<_>>(),
                )
                .await
                .unwrap();
            let dir = root.path().join(artifact);
            std::fs::create_dir(&dir).unwrap();
            std::fs::write(
                dir.join("manifest.json"),
                serde_json::to_vec(&manifest).unwrap(),
            )
            .unwrap();
            std::fs::copy(
                workspace.join(format!(
                    "target/wasm32-unknown-unknown/debug/{artifact}.wasm"
                )),
                dir.join("module.wasm"),
            )
            .unwrap();
            packages.insert(id.to_string(), scheduler.package(dir).await.unwrap());
        }
        let result = Self {
            scheduler,
            store,
            remote,
            sent,
            packages,
            endpoint,
            _root: root,
            server,
        };
        result
            .configure(
                "/module/openai-provider",
                "/settings",
                &forest_openai_provider::types::Settings {
                    endpoint: format!("{}/chat", result.endpoint),
                    api_key: "test".into(),
                    model: "test".into(),
                    multimodal: false,
                    output_limit: forest_openai_provider::types::OutputLimit::Tokens(16384),
                    reasoning_effort: Some("high".into()),
                },
            )
            .await;
        result
            .configure(
                "/module/qwen-vl-embedding",
                "/connection",
                &forest_qwen_vl_embedding::types::Connection {
                    endpoint: format!("{}/embeddings", result.endpoint),
                    api_key: "test".into(),
                },
            )
            .await;
        result
            .configure(
                "/module/deepseek-web",
                "/connection",
                &forest_deepseek_web::Connection {
                    endpoint: format!("{}/v1", result.endpoint),
                    api_key: "test".into(),
                },
            )
            .await;
        result
            .configure(
                "/module/wolfram-alpha",
                "/connection",
                &forest_wolfram_alpha::Connection {
                    endpoint: format!("{}/wolfram", result.endpoint),
                    app_id: "wolfram-test".into(),
                },
            )
            .await;
        result
            .configure(
                "/module/memory",
                "/settings",
                &forest_memory_module::types::Settings {
                    endpoint: result.endpoint.clone(),
                    ..Default::default()
                },
            )
            .await;
        result
            .configure(
                "/module/agent",
                "/settings",
                &forest_agent_module::types::Settings {
                    allowed_groups: vec![10, 20],
                    bot_name: Some("TestBot".into()),
                    high_water: 64,
                    low_water: 32,
                },
            )
            .await;
        result
            .scheduler
            .load_roots(
                result.packages.values().cloned().collect(),
                &["/module/agent".parse().unwrap()],
            )
            .await
            .unwrap();
        result
    }
    pub async fn configure<T: AsValue + AsSchema>(&self, object: &str, key: &str, value: &T) {
        self.store
            .set(
                &object.parse().unwrap(),
                &Key::new(key.parse().unwrap()),
                value,
            )
            .await
            .unwrap();
    }
    pub fn reply(&self, group: i64, reply: Reply) {
        self.remote
            .lock()
            .unwrap()
            .replies
            .entry(group)
            .or_default()
            .push_back(reply);
    }
    pub async fn input(
        &self,
        message: &GroupMessage,
    ) -> Result<(), forest_agent_module::types::Error> {
        self.scheduler
            .call(
                onebot::OBJECT.parse().unwrap(),
                "/module/agent".parse().unwrap(),
                "/group_message".parse().unwrap(),
                message,
            )
            .await
            .unwrap()
    }
    pub async fn call<I: AsValue, O: AsValue>(
        &self,
        object: &str,
        interface: &str,
        input: &I,
    ) -> O {
        self.scheduler
            .call(
                "/user/admin".parse().unwrap(),
                object.parse().unwrap(),
                interface.parse().unwrap(),
                input,
            )
            .await
            .unwrap()
    }
    pub async fn finish(self) {
        self.scheduler
            .shutdown(forest_runtime::StopMode::Drain)
            .await
            .unwrap();
        self.server.abort();
        drop(self.scheduler);
        self.store.close().await.unwrap();
    }
}
pub fn message(group: i64, id: i32, user: i64, body: &str, trigger: bool) -> GroupMessage {
    let mut segments = vec![];
    if trigger {
        segments.push(Segment::at("42"));
    }
    segments.push(Segment::text(body));
    GroupMessage {
        time: 1700000000 + id as i64,
        self_id: 42,
        post_type: "message".into(),
        message_type: "group".into(),
        sub_type: "normal".into(),
        message_id: id,
        group_id: group,
        user_id: user,
        anonymous: None,
        message: Message::Segments(segments),
        raw_message: "untrusted raw body".into(),
        sender: GroupSender {
            user_id: Some(user),
            nickname: Some(format!("user-{user}")),
            ..Default::default()
        },
    }
}
pub async fn bounded<F: std::future::Future>(future: F) -> F::Output {
    tokio::time::timeout(std::time::Duration::from_secs(30), future)
        .await
        .expect("integration made no progress")
}
