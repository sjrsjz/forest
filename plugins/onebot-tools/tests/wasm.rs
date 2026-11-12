use forest_onebot::{
    Client, Transport,
    forest::{self as onebot, OneBotError, Query, SendPrivateMessage},
    message::{Message, Segment},
};
use forest_onebot_tools::contract::{Error, Invocation};
use forest_protocol::SubjectPattern;
use forest_runtime::{RuntimeOptions, Scheduler, StopMode};
use serde_json::{Value, json};
use std::{
    path::{Path, PathBuf},
    process::Command,
    sync::{Arc, Mutex},
};

#[derive(Clone, Default)]
struct Gateway(Arc<Mutex<Vec<(String, Value)>>>);

impl Transport for Gateway {
    type Error = std::io::Error;
    async fn request(&self, action: &str, params: Vec<u8>) -> Result<Vec<u8>, Self::Error> {
        let params: Value = serde_json::from_slice(&params).unwrap();
        self.0.lock().unwrap().push((action.into(), params.clone()));
        let data = match action {
            "get_login_info" => json!({"user_id":10,"nickname":"test bot"}),
            "get_group_list" => json!([
                {"group_id":42,"group_name":"first","member_count":3,"max_member_count":100},
                {"group_id":43,"group_name":"second","member_count":1,"max_member_count":100}
            ]),
            "get_friend_list" => json!([{"user_id":20,"nickname":"friend","remark":"classmate"}]),
            "get_group_info" => {
                json!({"group_id":params["group_id"],"group_name":"first","member_count":3,"max_member_count":100})
            }
            "get_group_member_info" | "get_group_member_list" => {
                let member = |id| {
                    json!({
                        "group_id":42,"user_id":id,"nickname":format!("member {id}"),"card":"card",
                        "sex":"unknown","age":0,"join_time":1,"last_sent_time":2,
                        "level":"0","role":"admin","unfriendly":false
                    })
                };
                if action == "get_group_member_info" {
                    member(params["user_id"].as_i64().unwrap())
                } else {
                    Value::Array((20..123).map(member).collect())
                }
            }
            "get_stranger_info" if params["user_id"] == 13 => {
                return Ok(serde_json::to_vec(
                    &json!({"status":"failed","retcode":1404,"wording":"not found","data":null}),
                )
                .unwrap());
            }
            "get_stranger_info" => {
                json!({"user_id":params["user_id"],"nickname":"user","sex":"unknown","age":0})
            }
            "send_private_msg" if params["user_id"] == 13 => {
                return Ok(br#"{"status":"async","retcode":1,"data":null}"#.to_vec());
            }
            "send_group_msg" | "send_private_msg" => json!({"message_id":123}),
            _ => panic!("unexpected action {action}"),
        };
        Ok(serde_json::to_vec(&json!({"status":"ok","retcode":0,"data":data})).unwrap())
    }
}

fn package(root: &Path, destination: &Path, artifact: &str, manifest: forest_protocol::Manifest) {
    std::fs::create_dir(destination).unwrap();
    std::fs::copy(
        root.join(format!(
            "target/wasm32-unknown-unknown/debug/{artifact}.wasm"
        )),
        destination.join("module.wasm"),
    )
    .unwrap();
    forest_package::write(&destination, &manifest).unwrap();
}

async fn invoke(scheduler: &Scheduler, interface: &str, args: Value) -> Result<String, Error> {
    scheduler
        .call(
            "/module/agent-tools".parse().unwrap(),
            "/module/onebot-tools".parse().unwrap(),
            interface.parse().unwrap(),
            &Invocation {
                group_id: 42,
                bot_id: 10,
                name: format!("onebot_{}", interface.trim_start_matches('/')),
                arguments_json: args.to_string(),
            },
        )
        .await
        .unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn wasm_tools_query_native_onebot_and_send_markdown_with_real_receipts() {
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
                "-j",
                "2",
                "-p",
                "forest-onebot-tools",
                "-p",
                "forest-media",
                "-p",
                "forest-message-output",
                "-p",
                "forest-markdown",
                "--lib"
            ])
            .current_dir(&root)
            .status()
            .unwrap()
            .success()
    );
    let temp = tempfile::tempdir().unwrap();
    let media = temp.path().join("media");
    let tools = temp.path().join("tools");
    let output = temp.path().join("output");
    package(&root, &media, "forest_media", forest_media::manifest());
    package(
        &root,
        &output,
        "forest_message_output",
        forest_message_output::manifest(),
    );
    package(
        &root,
        &tools,
        "forest_onebot_tools",
        forest_onebot_tools::manifest(),
    );
    let scheduler = Scheduler::new(
        temp.path().join("config.sqlite3"),
        RuntimeOptions {
            data_directory: temp.path().join("data"),
            ..RuntimeOptions::default()
        },
    )
    .await
    .unwrap();
    let gateway = Gateway::default();
    forest_runtime::onebot::install(
        &scheduler,
        Client::new(gateway.clone()),
        forest_runtime::onebot::GroupScope::All,
        vec![SubjectPattern::Path("/module/**".parse().unwrap())],
    )
    .unwrap();
    let markdown = temp.path().join("markdown");
    package(
        &root,
        &markdown,
        "forest_markdown",
        forest_markdown::manifest(),
    );
    scheduler
        .register_native(forest_runtime::NativeInterface::new(
            "/module/typst".parse().unwrap(),
            "/typeset".parse().unwrap(),
            forest_protocol::manifest::callers(vec![SubjectPattern::Path(
                "/module/markdown".parse().unwrap(),
            )]),
            |_, _: String| async {
                Err::<Vec<String>, forest_markdown::typst::Error>(
                    forest_markdown::typst::Error::Execution("unused in messaging test".into()),
                )
            },
        ))
        .unwrap();
    scheduler
        .load_roots(
            vec![
                scheduler.package(&media).await.unwrap(),
                scheduler.package(&markdown).await.unwrap(),
                scheduler.package(&tools).await.unwrap(),
                scheduler.package(&output).await.unwrap(),
            ],
            &["/module/onebot-tools".parse().unwrap()],
        )
        .await
        .unwrap();

    let login: Value = serde_json::from_str(
        &invoke(&scheduler, "/get_login_info", json!({}))
            .await
            .unwrap(),
    )
    .unwrap();
    assert_eq!(login["user_id"], 10);
    let groups: Value = serde_json::from_str(
        &invoke(&scheduler, "/get_group_list", json!({"limit":1}))
            .await
            .unwrap(),
    )
    .unwrap();
    assert_eq!(groups["total"], 2);
    assert_eq!(groups["next_offset"], 1);
    let members: Value = serde_json::from_str(
        &invoke(&scheduler, "/get_group_member_list", json!({"offset":100}))
            .await
            .unwrap(),
    )
    .unwrap();
    assert_eq!(members["items"].as_array().unwrap().len(), 3);
    assert_eq!(members["next_offset"], Value::Null);
    for (interface, args) in [
        ("/get_friend_list", json!({})),
        ("/get_group_info", json!({"no_cache":true})),
        ("/get_group_member_info", json!({"user_id":20})),
        ("/get_stranger_info", json!({"user_id":20})),
    ] {
        invoke(&scheduler, interface, args).await.unwrap();
    }
    assert!(
        gateway
            .0
            .lock()
            .unwrap()
            .iter()
            .any(|(action, args)| action == "get_group_info"
                && args["group_id"] == 42
                && args["no_cache"] == true)
    );
    let count = gateway.0.lock().unwrap().len();
    assert!(matches!(
        invoke(&scheduler, "/get_group_member_list", json!({"limit":0})).await,
        Err(Error::Arguments(_))
    ));
    assert_eq!(gateway.0.lock().unwrap().len(), count);
    assert!(
        matches!(invoke(&scheduler,"/get_stranger_info",json!({"user_id":13})).await,Err(Error::Execution(error)) if error.contains("1404") && error.contains("not found"))
    );

    let blob = forest_media::contract::Blob {
        mime: "image/png".into(),
        bytes: b"image bytes".to_vec(),
    };
    let reference = scheduler
        .call::<_, Result<String, forest_media::contract::Error>>(
            "/user/admin".parse().unwrap(),
            "/module/media".parse().unwrap(),
            "/put".parse().unwrap(),
            &blob,
        )
        .await
        .unwrap()
        .unwrap();
    let body = format!(
        "literal [CQ:at,qq=999][user](forest:at/20)[face](forest:face/14)![image]({reference})"
    );
    let sent: Value = serde_json::from_str(
        &invoke(&scheduler, "/send_group_message", json!({"message":body}))
            .await
            .unwrap(),
    )
    .unwrap();
    assert_eq!(sent["message_ids"], json!([123]));
    assert_eq!(sent["group_id"], 42);
    assert_eq!(sent["message"], body);
    assert!(!sent.to_string().contains("base64://"));
    let call = gateway.0.lock().unwrap().last().unwrap().clone();
    assert_eq!(call.0, "send_group_msg");
    assert_eq!(call.1["auto_escape"], false);
    assert_eq!(
        call.1["message"][0]["data"]["text"],
        "literal [CQ:at,qq=999]"
    );
    assert_eq!(call.1["message"][1]["type"], "at");
    assert_eq!(
        call.1["message"][3]["data"]["file"],
        "base64://aW1hZ2UgYnl0ZXM="
    );
    invoke(
        &scheduler,
        "/send_private_message",
        json!({"user_id":20,"message":"hello"}),
    )
    .await
    .unwrap();
    let failure: Value = serde_json::from_str(
        &invoke(
            &scheduler,
            "/send_private_message",
            json!({"user_id":13,"message":"hello"}),
        )
        .await
        .unwrap(),
    )
    .unwrap();
    assert_eq!(failure["message_ids"], json!([]));
    assert!(failure["error"].as_str().unwrap().contains("Submitted"));
    let count = gateway.0.lock().unwrap().len();
    for body in ["[bad](media:invalid)", "[reply](forest:reply/43/-7)"] {
        assert!(
            invoke(&scheduler, "/send_group_message", json!({"message":body}))
                .await
                .is_err()
        );
    }
    assert_eq!(gateway.0.lock().unwrap().len(), count);
    let denied = scheduler
        .call::<_, Result<String, Error>>(
            "/module/stranger".parse().unwrap(),
            "/module/onebot-tools".parse().unwrap(),
            "/get_login_info".parse().unwrap(),
            &Invocation {
                group_id: 42,
                bot_id: 10,
                name: "onebot_get_login_info".into(),
                arguments_json: "{}".into(),
            },
        )
        .await;
    assert!(denied.is_err());
    assert_eq!(gateway.0.lock().unwrap().len(), count);
    scheduler.shutdown(StopMode::Drain).await.unwrap();

    let scoped = Scheduler::new(
        temp.path().join("scoped.sqlite3"),
        RuntimeOptions {
            data_directory: temp.path().join("scoped"),
            ..RuntimeOptions::default()
        },
    )
    .await
    .unwrap();
    forest_runtime::onebot::install(
        &scoped,
        Client::new(gateway.clone()),
        forest_runtime::onebot::GroupScope::only([42]),
        vec![SubjectPattern::Path("/module/**".parse().unwrap())],
    )
    .unwrap();
    let query = |request| {
        scoped.call::<_, Result<String, OneBotError>>(
            "/module/test".parse().unwrap(),
            onebot::OBJECT.parse().unwrap(),
            onebot::QUERY.parse().unwrap(),
            request,
        )
    };
    assert!(matches!(
        query(&Query::GroupInfo {
            group_id: 43,
            no_cache: false
        })
        .await
        .unwrap(),
        Err(OneBotError::GroupDenied)
    ));
    let denied = scoped
        .call::<_, Result<onebot::SentMessage, OneBotError>>(
            "/module/test".parse().unwrap(),
            onebot::OBJECT.parse().unwrap(),
            onebot::SEND_PRIVATE_MESSAGE.parse().unwrap(),
            &SendPrivateMessage {
                user_id: 20,
                message: Message::Segments(vec![Segment::text("not sent")]),
                auto_escape: Some(false),
            },
        )
        .await
        .unwrap();
    assert!(matches!(denied, Err(OneBotError::GroupDenied)));
    assert_eq!(gateway.0.lock().unwrap().len(), count);
    let groups: Value =
        serde_json::from_str(&query(&Query::GroupList).await.unwrap().unwrap()).unwrap();
    assert_eq!(groups.as_array().unwrap().len(), 1);
    assert_eq!(groups[0]["group_id"], 42);
    scoped.shutdown(StopMode::Drain).await.unwrap();
}
