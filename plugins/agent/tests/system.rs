mod support;
use forest_agent_module::{content::*, memory, provider, types::Status};
use forest_onebot::{
    cq,
    message::{Message, Segment},
};
use forest_runtime::StopMode;
use serde_json::{Value, json};
use support::*;
use tokio::sync::oneshot;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn actual_wasm_agent_memory_context_provider_and_tools() {
    let mut h = Harness::new().await;
    deepseek_web_tools(&h).await;
    // A cross-group archived source exists; storing and recalling it never loses origin.
    let seed: Result<memory::Accepted, memory::Error> = h
        .call(
            memory::OBJECT,
            "/accept",
            &memory::Incoming::Group(Box::new(message(20, 1, 9, "ARCHIVED_SOURCE", false))),
        )
        .await;
    let seed = seed.unwrap().unit;
    let archive: Result<(), memory::Error> = h
        .call(memory::OBJECT, "/archive", &vec![seed.id.clone()])
        .await;
    archive.unwrap();
    let indexed: Result<(), memory::Error> = h.call(memory::OBJECT, "/index/advance", &()).await;
    indexed.unwrap();
    let raw: Result<Vec<MemoryUnit>, memory::Error> = h
        .call(memory::OBJECT, "/read", &vec![seed.id.clone()])
        .await;
    let MemoryContent::GroupMessage(raw) = &raw.unwrap()[0].content else {
        panic!()
    };
    assert_eq!(raw.raw_message, "untrusted raw body");

    let (started, waiting) = oneshot::channel();
    let (release, blocked) = oneshot::channel();
    h.remote.lock().unwrap().slow = Some((started, blocked));
    let mut first = Reply::text("我查一下[CQ:face,id=1]");
    first.body["choices"][0]["finish_reason"] = json!("tool_calls");
    first.body["choices"][0]["message"]["tool_calls"] = json!([{"id":"fetch-1","type":"function","function":{"name":"http_request","arguments":json!({"url":format!("{}/slow",h.endpoint)}).to_string()}}]);
    h.reply(10, first);
    h.reply(10, Reply::text("查完了"));
    h.reply(20, Reply::text("另一个群正常工作"));
    let scheduler = h.scheduler.clone();
    let first = message(10, 10, 1, "A_START", true);
    let run = tokio::spawn(async move {
        scheduler
            .call::<_, Result<(), forest_agent_module::types::Error>>(
                "/core/onebot".parse().unwrap(),
                "/module/agent".parse().unwrap(),
                "/group_message".parse().unwrap(),
                &first,
            )
            .await
            .unwrap()
            .unwrap()
    });
    bounded(waiting).await.unwrap();
    let announced = bounded(h.sent.recv()).await.unwrap();
    assert_eq!(
        cq::encode(&announced.message).unwrap(),
        "我查一下[CQ:face,id=1]"
    );
    bounded(h.input(&message(10, 11, 2, "B_ORDINARY", false)))
        .await
        .unwrap();
    bounded(h.input(&message(10, 12, 3, "C_ORDINARY", false)))
        .await
        .unwrap();
    bounded(h.input(&message(10, 13, 2, "B_TRIGGER", true)))
        .await
        .unwrap();
    bounded(h.input(&message(10, 14, 3, "C_TRIGGER", true)))
        .await
        .unwrap();
    bounded(h.input(&message(10, 15, 2, "TAIL_NOT_YET", false)))
        .await
        .unwrap();
    bounded(h.input(&message(20, 2, 9, "OTHER_GROUP", true)))
        .await
        .unwrap();
    let other = bounded(h.sent.recv()).await.unwrap();
    assert_eq!(other.group_id, 20);
    let statuses: Result<Vec<Status>, _> = h
        .call::<_, Result<Vec<Status>, forest_agent_module::types::Error>>(
            "/module/agent",
            "/status",
            &(),
        )
        .await;
    let statuses = statuses.unwrap();
    let busy = statuses.iter().find(|s| s.group == 10).unwrap();
    assert!(busy.working);
    assert_eq!(busy.queued, 5);
    release.send(()).unwrap();
    bounded(run).await.unwrap();
    let final_message = bounded(h.sent.recv()).await.unwrap();
    assert_eq!(cq::encode(&final_message.message).unwrap(), "查完了");
    {
        let remote = h.remote.lock().unwrap();
        let requests = remote
            .requests
            .iter()
            .filter(|(g, _)| *g == 10)
            .map(|(_, v)| v)
            .collect::<Vec<_>>();
        assert_eq!(requests.len(), 2);
        let before = requests[0]["messages"].as_array().unwrap();
        let after = requests[1]["messages"].as_array().unwrap();
        assert_eq!(&after[..before.len()], before.as_slice());
        assert_eq!(after[before.len()]["role"], "assistant");
        assert_eq!(after[before.len()]["reasoning_content"], "PRIVATE_THOUGHT");
        assert_eq!(after[before.len() + 1]["role"], "tool");
        assert_eq!(after[before.len() + 1]["tool_call_id"], "fetch-1");
        let text = requests[1].to_string();
        assert!(!text.contains("TAIL_NOT_YET"));
        let bodies = after
            .iter()
            .filter_map(|m| m["content"].as_str())
            .collect::<Vec<_>>();
        let b = bodies.iter().position(|s| s.contains("B_TRIGGER")).unwrap();
        let c = bodies.iter().position(|s| s.contains("C_TRIGGER")).unwrap();
        assert!(bodies[b - 1].contains("<recall group_id=\"20\""));
        assert!(bodies[c - 1].contains("<recall group_id=\"20\""));
        assert!(
            bodies
                .iter()
                .any(|s| s.contains("B_ORDINARY") && s.contains("user_id=\"2\""))
        );
        assert!(
            bodies
                .iter()
                .any(|s| s.contains("C_ORDINARY") && s.contains("user_id=\"3\""))
        );
        assert_eq!(requests[0]["max_tokens"], 16384);
    }
    // A later trigger sees the drained ordinary tail. Silent trimming keeps the window bounded.
    h.reply(10, Reply::text("下一次"));
    h.input(&message(10, 16, 1, "NEXT_WORK", true))
        .await
        .unwrap();
    bounded(h.sent.recv()).await.unwrap();
    assert!(
        h.remote
            .lock()
            .unwrap()
            .requests
            .last()
            .unwrap()
            .1
            .to_string()
            .contains("TAIL_NOT_YET")
    );
    h.configure(
        "/module/agent",
        "/settings",
        &forest_agent_module::types::Settings {
            allowed_groups: vec![10, 20],
            bot_name: None,
            high_water: 8,
            low_water: 4,
        },
    )
    .await;
    h.scheduler
        .reload(h.packages["/module/agent"].clone(), StopMode::Drain)
        .await
        .unwrap();
    for id in 17..30 {
        h.input(&message(10, id, 2, "SILENT", false)).await.unwrap();
    }
    let states: Result<Vec<Status>, forest_agent_module::types::Error> =
        h.call("/module/agent", "/status", &()).await;
    let states = states.unwrap();
    let s = states.iter().find(|s| s.group == 10).unwrap();
    assert!(!s.working);
    assert!(s.window_units <= 8);
    let indexed: Result<(), memory::Error> = h.call(memory::OBJECT, "/index/advance", &()).await;
    indexed.unwrap();
    assert!(
        !h.remote
            .lock()
            .unwrap()
            .embeddings
            .iter()
            .any(|v| v.to_string().contains("PRIVATE_THOUGHT"))
    );

    // No automatic Provider retries. A queued @ clears the previous pending error.
    let (started, waiting) = oneshot::channel();
    let (release, blocked) = oneshot::channel();
    let mut failure = Reply::text("");
    failure.status = axum::http::StatusCode::BAD_GATEWAY;
    failure.started = Some(started);
    failure.release = Some(blocked);
    h.reply(10, failure);
    h.reply(10, Reply::text("后续触发正常"));
    let scheduler = h.scheduler.clone();
    let input = message(10, 30, 1, "FAIL_FIRST", true);
    let run = tokio::spawn(async move {
        scheduler
            .call::<_, Result<(), forest_agent_module::types::Error>>(
                "/core/onebot".parse().unwrap(),
                "/module/agent".parse().unwrap(),
                "/group_message".parse().unwrap(),
                &input,
            )
            .await
            .unwrap()
            .unwrap()
    });
    bounded(waiting).await.unwrap();
    h.input(&message(10, 31, 2, "CONTINUE_AFTER_ERROR", true))
        .await
        .unwrap();
    release.send(()).unwrap();
    bounded(run).await.unwrap();
    assert_eq!(
        cq::encode(&bounded(h.sent.recv()).await.unwrap().message).unwrap(),
        "后续触发正常"
    );
    assert!(h.sent.try_recv().is_err());
    let mut failure = Reply::text("");
    failure.status = axum::http::StatusCode::BAD_GATEWAY;
    h.reply(10, failure);
    h.input(&message(10, 32, 1, "FINAL_ERROR", true))
        .await
        .unwrap();
    assert!(
        cq::encode(&bounded(h.sent.recv()).await.unwrap().message)
            .unwrap()
            .contains("Agent 调用失败")
    );

    // AtYield reload saves queued data without replaying an interrupted tool action.
    let (started, waiting) = oneshot::channel();
    let (release, blocked) = oneshot::channel();
    h.remote.lock().unwrap().slow = Some((started, blocked));
    let mut waiting_reply = Reply::text("RELOAD_TOOL_STARTED");
    waiting_reply.body["choices"][0]["finish_reason"] = json!("tool_calls");
    waiting_reply.body["choices"][0]["message"]["tool_calls"] = json!([{"id":"interrupted-tool","type":"function","function":{"name":"http_request","arguments":json!({"url":format!("{}/slow",h.endpoint)}).to_string()}}]);
    h.reply(10, waiting_reply);
    let scheduler = h.scheduler.clone();
    let input = message(10, 33, 1, "RELOAD_WORK", true);
    let run = tokio::spawn(async move {
        scheduler
            .call::<_, Result<(), forest_agent_module::types::Error>>(
                "/core/onebot".parse().unwrap(),
                "/module/agent".parse().unwrap(),
                "/group_message".parse().unwrap(),
                &input,
            )
            .await
    });
    bounded(waiting).await.unwrap();
    assert_eq!(
        cq::encode(&bounded(h.sent.recv()).await.unwrap().message).unwrap(),
        "RELOAD_TOOL_STARTED"
    );
    h.input(&message(10, 34, 2, "OLD_PENDING_TRIGGER", true))
        .await
        .unwrap();
    bounded(
        h.scheduler
            .reload(h.packages["/module/agent"].clone(), StopMode::AtYield),
    )
    .await
    .unwrap();
    assert!(bounded(run).await.unwrap().is_err());
    let _ = release.send(());
    let states: Result<Vec<Status>, forest_agent_module::types::Error> =
        h.call("/module/agent", "/status", &()).await;
    assert!(states.unwrap().iter().all(|s| !s.working));
    h.reply(10, Reply::text("重载后新的触发"));
    h.input(&message(10, 35, 1, "NEW_AFTER_RELOAD", true))
        .await
        .unwrap();
    let response = bounded(h.sent.recv()).await.unwrap();
    assert_eq!(cq::encode(&response.message).unwrap(), "重载后新的触发");
    let request = h.remote.lock().unwrap().requests.last().unwrap().1.clone();
    assert!(request.to_string().contains("OLD_PENDING_TRIGGER"));
    assert!(
        !request["messages"]
            .as_array()
            .unwrap()
            .iter()
            .any(|message| {
                message["role"] == "assistant"
                    && message["tool_calls"]
                        .to_string()
                        .contains("interrupted-tool")
            })
    );
    assert!(h.sent.try_recv().is_err());

    memory_index_and_media(&mut h).await;
    bounded_graph(&h).await;
    h.finish().await;
}

async fn deepseek_web_tools(h: &Harness) {
    use forest_agent_tools::contract::{Catalog, Error, Invocation};
    async fn invoke(h: &Harness, name: &str, args: Value) -> Result<String, Error> {
        h.call(
            "/module/agent-tools",
            "/invoke",
            &Invocation {
                group_id: 10,
                bot_id: 42,
                name: name.into(),
                arguments_json: args.to_string(),
            },
        )
        .await
    }
    let catalog: Result<Catalog, Error> = h.call("/module/agent-tools", "/describe", &()).await;
    let catalog = catalog.unwrap();
    for name in ["web_search", "web_fetch", "http_request", "memory_read"] {
        assert!(catalog.iter().any(|tool| tool.name == name));
    }
    let query = "文".repeat(128); // Unicode characters, not UTF-8 byte length.
    let result = invoke(
        h,
        "web_search",
        json!({"query":query,"count":3,"ori_question":null}),
    )
    .await
    .unwrap();
    let result: Value = serde_json::from_str(&result).unwrap();
    assert_eq!(result["results"][0]["summary"], "relevant summary");
    assert_eq!(result["results"][0]["rerank_score"], 2.4);
    assert_eq!(result["results"][0]["content"], "");
    {
        let remote = h.remote.lock().unwrap();
        let (method, body) = remote.web_requests.last().unwrap();
        assert_eq!(method, "search");
        assert_eq!(body["query"], query);
        assert_eq!(body["count"], 3);
        assert_eq!(body["need_content"], false);
        assert!(body.get("ori_question").is_none());
        assert!(body.get("api_key").is_none());
    }
    let result = invoke(
        h,
        "web_search",
        json!({"query":"content","need_content":true}),
    )
    .await
    .unwrap();
    assert_eq!(
        serde_json::from_str::<Value>(&result).unwrap()["results"][0]["content"],
        "full content"
    );
    let result = invoke(h, "web_fetch", json!({"url":"https://example.com/article"}))
        .await
        .unwrap();
    let result: Value = serde_json::from_str(&result).unwrap();
    assert_eq!(result["url"], "https://example.com/article");
    assert_eq!(result["content"], "# Article\nExtracted Markdown");
    let count = h.remote.lock().unwrap().web_requests.len();
    for (name, args) in [
        ("web_search", json!({"query":" "})),
        ("web_search", json!({"query":"文".repeat(129)})),
        ("web_search", json!({"query":"valid","count":51})),
        (
            "web_fetch",
            json!({"url":"https://user:password@example.com"}),
        ),
        ("web_fetch", json!({"url":"file:///tmp/secret"})),
    ] {
        assert!(matches!(
            invoke(h, name, args).await,
            Err(Error::Arguments(_))
        ));
    }
    assert_eq!(h.remote.lock().unwrap().web_requests.len(), count);
    assert!(
        matches!(invoke(h,"web_search",json!({"query":"FAIL"})).await,Err(Error::Execution(e)) if e.contains("429"))
    );
    assert_eq!(h.remote.lock().unwrap().web_requests.len(), count + 1); // No retry or raw HTTP fallback.
    assert!(matches!(
        invoke(h, "web_search", json!({"query":"MALFORMED"})).await,
        Err(Error::Execution(_))
    ));
    let result = invoke(h, "web_search", json!({"query":"EMPTY"}))
        .await
        .unwrap();
    assert_eq!(
        serde_json::from_str::<Value>(&result).unwrap()["results"],
        json!([])
    );
}

async fn memory_index_and_media(h: &mut Harness) {
    let mut picture = message(20, 50, 9, "", false);
    picture.message = Message::Segments(vec![
        Segment::at("42"),
        Segment::image("base64://iVBORw0KGgo="),
    ]);
    let accepted: Result<memory::Accepted, memory::Error> = h
        .call(
            memory::OBJECT,
            "/accept",
            &memory::Incoming::Group(Box::new(picture.clone())),
        )
        .await;
    let image = accepted.unwrap().unit;
    let repeated: Result<memory::Accepted, memory::Error> = h
        .call(
            memory::OBJECT,
            "/accept",
            &memory::Incoming::Group(Box::new(picture)),
        )
        .await;
    let repeated = repeated.unwrap();
    assert!(!repeated.fresh);
    assert_eq!(repeated.unit.id, image.id);
    let mut ids = vec![image.id.clone()];
    for (key, assistant) in [
        (
            "reasoning-only",
            provider::Assistant {
                reasoning_content: Some("EXCLUDE_REASONING".into()),
                ..Default::default()
            },
        ),
        (
            "call-only",
            provider::Assistant {
                tool_calls: Some(vec![provider::ToolCall::Function {
                    id: "c".into(),
                    function: provider::FunctionCall {
                        name: "CUSTOM_TOOL".into(),
                        arguments: "{}".into(),
                    },
                }]),
                ..Default::default()
            },
        ),
        (
            "failed-embedding",
            provider::Assistant {
                content: Some("EMBED_FAIL".into()),
                ..Default::default()
            },
        ),
    ] {
        let a: Result<memory::Accepted, memory::Error> = h
            .call(
                memory::OBJECT,
                "/accept",
                &memory::Incoming::Generated {
                    group: 20,
                    key: key.into(),
                    content: MemoryContent::ModelTurn(ModelTurn {
                        time: 1700000010,
                        assistant,
                    }),
                },
            )
            .await;
        ids.push(a.unwrap().unit.id);
    }
    let before = h.remote.lock().unwrap().embeddings.len();
    let result: Result<(), memory::Error> = h.call(memory::OBJECT, "/archive", &ids).await;
    result.unwrap();
    let result: Result<(), memory::Error> = h.call(memory::OBJECT, "/index/advance", &()).await;
    result.unwrap();
    let projected: Result<memory::Projection, memory::Error> = h
        .call(
            memory::OBJECT,
            "/project",
            &memory::Project {
                ids: vec![image.id.clone()],
                images: true,
            },
        )
        .await;
    let projected = projected.unwrap();
    assert_eq!(projected.images.len(), 1);
    assert!(
        projected.images[0]
            .image
            .url
            .starts_with("data:image/png;base64,")
    );
    let MemoryContent::GroupMessage(projected_message) = &projected.units[0].content else {
        panic!()
    };
    let cq = cq::encode(&projected_message.message).unwrap();
    assert!(cq.contains("[CQ:at,qq=42]"));
    assert!(cq.contains("[CQ:image,file=media:"));
    assert!(!cq.contains("base64"));
    let embeddings = h.remote.lock().unwrap().embeddings[before..].to_vec();
    assert!(
        embeddings
            .iter()
            .any(|v| v["input"][0]["image"].is_string())
    );
    assert!(
        embeddings
            .iter()
            .any(|v| v.to_string().contains("CUSTOM_TOOL"))
    );
    assert!(
        !embeddings
            .iter()
            .any(|v| v.to_string().contains("EXCLUDE_REASONING"))
    );
    let result: Result<memory::Region, memory::Error> =
        h.call(memory::OBJECT, "/recall", &image.id).await;
    let result = result.unwrap();
    assert!(!result.seeds.is_empty());
    let graph: Value = serde_json::from_str(&result.graph_json).unwrap();
    assert!(graph["nodes"].as_array().unwrap().len() <= 64);
    let count = h.remote.lock().unwrap().embeddings.len();
    let result: Result<(), memory::Error> = h.call(memory::OBJECT, "/index/advance", &()).await;
    result.unwrap();
    assert_eq!(h.remote.lock().unwrap().embeddings.len(), count);
    let status: Result<String, memory::Error> = h.call(memory::OBJECT, "/status", &()).await;
    let status: Value = serde_json::from_str(&status.unwrap()).unwrap();
    assert!(status["index"]["failed"].as_i64().unwrap() > 0);
    // Reopening the module-owned DB keeps immutable IDs and data.
    h.scheduler
        .reload(h.packages[memory::OBJECT].clone(), StopMode::Drain)
        .await
        .unwrap();
    let raw: Result<Vec<MemoryUnit>, memory::Error> = h
        .call(memory::OBJECT, "/read", &vec![image.id.clone()])
        .await;
    assert_eq!(raw.unwrap()[0].id, image.id);
    // Chat vision changes only the API projection, not the raw message or image index.
    let mut incoming = message(20, 51, 9, "LOOK_AT_IMAGE", true);
    let Message::Segments(parts) = &mut incoming.message else {
        unreachable!()
    };
    parts.push(Segment::image("base64://iVBORw0KGgo="));
    h.reply(20, Reply::text("text model"));
    h.input(&incoming).await.unwrap();
    bounded(h.sent.recv()).await.unwrap();
    let request = h.remote.lock().unwrap().requests.last().unwrap().1.clone();
    assert!(!request["messages"].to_string().contains("image_url"));
    assert!(
        request["messages"]
            .to_string()
            .contains("[CQ:image,file=media:")
    );
    h.configure(
        provider::OBJECT,
        "/settings",
        &provider::Settings {
            endpoint: format!("{}/chat", h.endpoint),
            api_key: "test".into(),
            model: "test".into(),
            multimodal: true,
            output_limit: provider::OutputLimit::Tokens(16384),
            reasoning_effort: None,
        },
    )
    .await;
    h.scheduler
        .reload(h.packages[provider::OBJECT].clone(), StopMode::Drain)
        .await
        .unwrap();
    h.reply(
        20,
        Reply::text(&format!(
            "[CQ:image,file={}]",
            projected.images[0].reference
        )),
    );
    incoming.message_id = 52;
    h.input(&incoming).await.unwrap();
    let sent = bounded(h.sent.recv()).await.unwrap();
    let Message::Segments(segments) = sent.message else {
        panic!()
    };
    assert!(matches!(&segments[0],Segment::Image(image) if image.file.starts_with("base64://")));
    let request = h.remote.lock().unwrap().requests.last().unwrap().1.clone();
    assert!(request["messages"].to_string().contains("image_url"));
    assert!(
        request["messages"]
            .to_string()
            .contains("data:image/png;base64,")
    );
}

async fn bounded_graph(h: &Harness) {
    let settings=memory::Settings {endpoint:h.endpoint.clone(),retrieval_json:r#"{"seed_threshold":0.55,"seed_limit":1,"sequence_weight":0.55,"association_weight":0.9,"activation_min":0.1,"association_limit":2,"node_limit":8,"edge_limit":12,"result_limit":8}"#.into(),..Default::default()};
    h.configure(memory::OBJECT, "/settings", &settings).await;
    h.scheduler
        .reload(h.packages[memory::OBJECT].clone(), StopMode::Drain)
        .await
        .unwrap();
    let mut ids = vec![];
    for group in 100..121 {
        let result: Result<memory::Accepted, memory::Error> = h
            .call(
                memory::OBJECT,
                "/accept",
                &memory::Incoming::Group(Box::new(message(group, 1, 9, "graph", false))),
            )
            .await;
        ids.push(result.unwrap().unit.id);
    }
    let archived: Result<(), memory::Error> = h.call(memory::OBJECT, "/archive", &ids).await;
    archived.unwrap();
    let result: Result<(), memory::Error> = h.call(memory::OBJECT, "/index/advance", &()).await;
    result.unwrap();
    let hub = ids[0].clone();
    let mut links = vec![];
    for neighbor in &ids[1..] {
        links.push(memory::RecallLink {
            trigger: hub.clone(),
            recalled: neighbor.clone(),
        });
        links.push(memory::RecallLink {
            trigger: neighbor.clone(),
            recalled: hub.clone(),
        });
        links.push(memory::RecallLink {
            trigger: hub.clone(),
            recalled: neighbor.clone(),
        });
    }
    let result: Result<(), memory::Error> = h.call(memory::OBJECT, "/link", &links).await;
    result.unwrap();
    h.remote.lock().unwrap().seed_filter = Some((hub.group, hub.sequence));
    let result: Result<memory::Region, memory::Error> =
        h.call(memory::OBJECT, "/recall", &hub).await;
    let result = result.unwrap();
    assert_eq!(result.units.len(), 3); // One seed and two sampled links, despite 20 neighbors.
    let graph: Value = serde_json::from_str(&result.graph_json).unwrap();
    assert!(graph["edges"].as_array().unwrap().len() <= 12);
    assert!(
        graph["nodes"]
            .as_array()
            .unwrap()
            .iter()
            .filter(|v| v["id"]["group"] != hub.group)
            .all(|v| v["activation"] == 0.9)
    );
    h.remote.lock().unwrap().score = Some(0.03);
    let result: Result<memory::Region, memory::Error> =
        h.call(memory::OBJECT, "/recall", &hub).await;
    assert!(result.unwrap().units.is_empty());
    h.remote.lock().unwrap().score = None;
    // A missing archived position remains two chronological steps, not one.
    let mut gap = vec![];
    for id in 1..=3 {
        let result: Result<memory::Accepted, memory::Error> = h
            .call(
                memory::OBJECT,
                "/accept",
                &memory::Incoming::Group(Box::new(message(200, id, 9, "sequence gap", false))),
            )
            .await;
        gap.push(result.unwrap().unit.id);
    }
    let result: Result<(), memory::Error> = h
        .call(
            memory::OBJECT,
            "/archive",
            &vec![gap[0].clone(), gap[2].clone()],
        )
        .await;
    result.unwrap();
    let result: Result<(), memory::Error> = h.call(memory::OBJECT, "/index/advance", &()).await;
    result.unwrap();
    h.remote.lock().unwrap().seed_filter = Some((200, gap[0].sequence));
    let result: Result<memory::Region, memory::Error> =
        h.call(memory::OBJECT, "/recall", &gap[0]).await;
    let result = result.unwrap();
    assert_eq!(result.units.len(), 2);
    let graph: Value = serde_json::from_str(&result.graph_json).unwrap();
    let value = graph["nodes"]
        .as_array()
        .unwrap()
        .iter()
        .find(|v| v["id"]["sequence"] == gap[2].sequence)
        .unwrap()["activation"]
        .as_f64()
        .unwrap();
    assert!((value - 0.55f64.powi(2)).abs() < 1e-10);
}
