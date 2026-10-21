use axum::{
    Json, Router,
    extract::State,
    http::{HeaderMap, StatusCode},
    routing::post,
};
use forest_config::{ConfigStore, Declaration, Key};
use forest_openai_provider::manifest;
use forest_openai_provider::types::*;
use forest_protocol::{DispatchError, Token};
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
    body: Value,
    status: StatusCode,
    started: Option<oneshot::Sender<()>>,
    release: Option<oneshot::Receiver<()>>,
}
type Replies = Arc<Mutex<VecDeque<Reply>>>;

fn inspect(body: &Value) {
    assert_eq!(body["stream"], false);
    assert_eq!(body["messages"][0]["role"], "system");
    assert_eq!(body["messages"][1]["role"], "developer");
    assert_eq!(
        body["messages"][2]["content"][1],
        json!({"type":"image_url","image_url":{"url":"https://example.com/image.png","detail":"low"}})
    );
    assert_eq!(body["messages"][3]["content"], "previous body");
    assert_eq!(
        body["messages"][3]["reasoning_content"],
        "retained reasoning"
    );
    assert_eq!(
        body["messages"][3]["tool_calls"][0]["function"]["arguments"],
        r#"{"q":"old"}"#
    );
    assert_eq!(
        body["messages"][4],
        json!({"role":"tool","tool_call_id":"old-call","content":"old-result"})
    );
    assert_eq!(body["tools"][0]["type"], "function");
    assert_eq!(body["tools"][0]["function"]["parameters"]["type"], "object");
    assert_eq!(
        body["tool_choice"],
        json!({"type":"function","function":{"name":"lookup"}})
    );
}

async fn old(
    State(replies): State<Replies>,
    headers: HeaderMap,
    Json(body): Json<Value>,
) -> (StatusCode, HeaderMap, Json<Value>) {
    inspect(&body);
    assert_eq!(headers["authorization"], "Bearer first-key");
    assert_eq!(body["model"], "first-model");
    assert_eq!(body["max_completion_tokens"], 128);
    assert!(body.get("max_tokens").is_none());
    assert_eq!(body["reasoning_effort"], "low");
    respond(replies).await
}

async fn new(
    State(replies): State<Replies>,
    headers: HeaderMap,
    Json(body): Json<Value>,
) -> (StatusCode, HeaderMap, Json<Value>) {
    inspect(&body);
    assert_eq!(headers["authorization"], "Bearer second-key");
    assert_eq!(body["model"], "second-model");
    assert_eq!(body["max_tokens"], 256);
    assert!(body.get("max_completion_tokens").is_none());
    assert!(body.get("reasoning_effort").is_none());
    respond(replies).await
}

async fn respond(replies: Replies) -> (StatusCode, HeaderMap, Json<Value>) {
    let reply = replies
        .lock()
        .unwrap()
        .pop_front()
        .expect("no automatic retry");
    if let Some(started) = reply.started {
        started.send(()).unwrap();
    }
    if let Some(release) = reply.release {
        release.await.unwrap();
    }
    let mut headers = HeaderMap::new();
    headers.insert("x-request-id", "provider-test".parse().unwrap());
    (reply.status, headers, Json(reply.body))
}

fn request() -> Request {
    Request {
        messages: vec![
            Message::System {
                content: "system".into(),
            },
            Message::Developer {
                content: "developer".into(),
            },
            Message::User {
                content: Content::Parts(vec![
                    ContentPart::Text {
                        text: "a picture".into(),
                    },
                    ContentPart::ImageUrl {
                        image_url: ImageUrl {
                            url: "https://example.com/image.png".into(),
                            detail: Some(ImageDetail::Low),
                        },
                    },
                ]),
            },
            Message::Assistant(Assistant {
                content: Some("previous body".into()),
                reasoning_content: Some("retained reasoning".into()),
                tool_calls: Some(vec![ToolCall::Function {
                    id: "old-call".into(),
                    function: FunctionCall {
                        name: "lookup".into(),
                        arguments: r#"{"q":"old"}"#.into(),
                    },
                }]),
                refusal: None,
            }),
            Message::Tool {
                tool_call_id: "old-call".into(),
                content: "old-result".into(),
            },
        ],
        tools: vec![Tool {
            name: "lookup".into(),
            description: Some("search".into()),
            parameters_json:
                r#"{"type":"object","properties":{"q":{"type":"string"}},"required":["q"]}"#.into(),
            strict: Some(false),
        }],
        tool_choice: Some(ToolChoice::Function("lookup".into())),
    }
}

fn completion(tool_calls: bool) -> Value {
    json!({"id":"completion-test","model":"resolved-model-name","created":123,
        "choices":[{"index":0,"finish_reason":if tool_calls {"tool_calls"} else {"stop"},
            "message":{"role":"assistant","content":"Let me check.","reasoning_content":"reasoning is retained", "tool_calls":if tool_calls {json!([
                {"id":"call-a","type":"function","function":{"name":"lookup","arguments":"{not valid JSON"}},
                {"id":"call-b","type":"function","function":{"name":"lookup","arguments":"{\"q\":\"b\"}"}}
            ])} else {Value::Null}}}],
        "usage":{"prompt_tokens":10,"completion_tokens":5,"total_tokens":15,"prompt_tokens_details":{"cached_tokens":4},"completion_tokens_details":{"reasoning_tokens":2}}
    })
}

async fn invoke(scheduler: &Scheduler, request: &Request) -> Result<Completion, Error> {
    scheduler
        .call(
            "/user/admin".parse().unwrap(),
            OBJECT.parse().unwrap(),
            COMPLETE.parse().unwrap(),
            request,
        )
        .await
        .unwrap()
}

async fn capabilities(scheduler: &Scheduler) -> Capabilities {
    let result: Result<Capabilities, Error> = scheduler
        .call(
            "/user/admin".parse().unwrap(),
            OBJECT.parse().unwrap(),
            CAPABILITIES.parse().unwrap(),
            &(),
        )
        .await
        .unwrap();
    result.unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn actual_wasm_native_tools_capabilities_and_reconfiguration() {
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
                "--locked",
                "--offline",
                "--target",
                "wasm32-unknown-unknown",
                "-p",
                "forest-openai-provider",
                "--lib"
            ])
            .current_dir(&root)
            .env("CARGO_BUILD_JOBS", "2")
            .status()
            .unwrap()
            .success()
    );
    let temp = tempfile::tempdir().unwrap();
    let directory = temp.path().join("package");
    std::fs::create_dir(&directory).unwrap();
    std::fs::copy(
        root.join("target/wasm32-unknown-unknown/debug/forest_openai_provider.wasm"),
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
            body: completion(true),
            status: StatusCode::OK,
            started: Some(started),
            release: Some(released),
        },
        Reply {
            body: completion(false),
            status: StatusCode::OK,
            started: None,
            release: None,
        },
        Reply {
            body: json!({"error":"unavailable"}),
            status: StatusCode::BAD_REQUEST,
            started: None,
            release: None,
        },
        Reply {
            body: json!({"choices":[]}),
            status: StatusCode::OK,
            started: None,
            release: None,
        },
    ])));
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base = format!("http://{}", listener.local_addr().unwrap());
    let (shutdown, stopped) = oneshot::channel();
    let app = Router::new()
        .route("/old", post(old))
        .route("/new", post(new))
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
    let key = Key::new(SETTINGS.parse().unwrap());
    store
        .set(
            &object,
            &key,
            &Settings {
                endpoint: format!("{base}/old"),
                api_key: "first-key".into(),
                model: "first-model".into(),
                multimodal: true,
                output_limit: OutputLimit::CompletionTokens(128),
                reasoning_effort: Some("low".into()),
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
    let package = scheduler.package(directory).await.unwrap();
    scheduler.load(package.clone()).await.unwrap();
    assert_eq!(
        capabilities(&scheduler).await,
        Capabilities {
            model: "first-model".into(),
            multimodal: true
        }
    );
    assert!(matches!(
        scheduler
            .call::<_, Result<Capabilities, Error>>(
                "/user/stranger".parse().unwrap(),
                object.clone(),
                CAPABILITIES.parse().unwrap(),
                &()
            )
            .await,
        Err(DispatchError::Denied)
    ));
    let first_scheduler = scheduler.clone();
    let first = tokio::spawn(async move { invoke(&first_scheduler, &request()).await });
    received.await.unwrap();
    store
        .set(
            &object,
            &key,
            &Settings {
                endpoint: format!("{base}/new"),
                api_key: "second-key".into(),
                model: "second-model".into(),
                multimodal: false,
                output_limit: OutputLimit::Tokens(256),
                reasoning_effort: None,
            },
        )
        .await
        .unwrap();
    // The current instance retains its settings until normal reload creates another.
    assert!(capabilities(&scheduler).await.multimodal);
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
    let second = tokio::spawn(async move { invoke(&second_scheduler, &request()).await });
    release.send(()).unwrap();
    let result = first.await.unwrap().unwrap();
    assert_eq!(result.model, "resolved-model-name");
    assert_eq!(
        result.choices[0].message.content.as_deref(),
        Some("Let me check.")
    );
    assert_eq!(
        result.choices[0].message.reasoning_content.as_deref(),
        Some("reasoning is retained")
    );
    assert_eq!(result.choices[0].finish_reason, FinishReason::ToolCalls);
    let calls = result.choices[0].message.tool_calls.as_ref().unwrap();
    assert_eq!(calls.len(), 2);
    let ToolCall::Function { function, .. } = &calls[0];
    assert_eq!(function.arguments, "{not valid JSON");
    assert_eq!(
        result
            .usage
            .unwrap()
            .prompt_tokens_details
            .unwrap()
            .cached_tokens,
        Some(4)
    );
    reload.await.unwrap();
    assert_eq!(
        second.await.unwrap().unwrap().choices[0].finish_reason,
        FinishReason::Stop
    );
    assert_eq!(
        capabilities(&scheduler).await,
        Capabilities {
            model: "second-model".into(),
            multimodal: false
        }
    );
    assert_eq!(
        invoke(&scheduler, &request()).await.unwrap_err(),
        Error::Http {
            status: 400,
            request_id: Some("provider-test".into())
        }
    );
    assert!(matches!(
        invoke(&scheduler, &request()).await,
        Err(Error::Response(_))
    ));
    let mut invalid = request();
    invalid.tools[0].parameters_json = "invalid JSON".into();
    assert!(matches!(
        invoke(&scheduler, &invalid).await,
        Err(Error::ToolSchema { .. })
    ));
    assert!(replies.lock().unwrap().is_empty());
    assert_eq!(scheduler.phase(&object), Some(ModulePhase::Serving));
    scheduler.unload(&object, StopMode::Drain).await.unwrap();
    drop(scheduler);
    store.close().await.unwrap();
    shutdown.send(()).unwrap();
    server.await.unwrap();
}
