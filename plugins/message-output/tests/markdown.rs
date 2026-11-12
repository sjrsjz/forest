#[path = "../../onebot/tests/support/native.rs"]
mod onebot_mock;
use forest_markdown::{contract as md, typst};
use forest_message_output::{Settings, contract::*, managed};
use forest_onebot::{
    Client, Transport,
    message::{Message, Node, Segment},
};
use forest_protocol::{SubjectPattern, manifest::callers};
use forest_runtime::{NativeInterface, RuntimeOptions, Scheduler, StopMode};
use serde_json::{Value, json};
use std::{
    path::PathBuf,
    process::Command,
    sync::{Arc, Mutex},
};

#[derive(Clone, Default)]
struct Gateway(Arc<Mutex<Vec<Value>>>);
impl Transport for Gateway {
    type Error = std::io::Error;
    async fn request(&self, _: &str, body: Vec<u8>) -> Result<Vec<u8>, Self::Error> {
        let body: Value = serde_json::from_slice(&body).unwrap();
        let mut calls = self.0.lock().unwrap();
        let fail = body["group_id"] == 72 && calls.iter().any(|v| v["group_id"] == 72);
        calls.push(body);
        Ok(serde_json::to_vec(&if fail {
            json!({"status":"failed","retcode":1400,"wording":"rejected","data":null})
        } else {
            json!({"status":"ok","retcode":0,"data":{"message_id":calls.len()}})
        })
        .unwrap())
    }
}
async fn send(s: &Scheduler, group: i64, source: &str) -> Result<DeliveryReport, Error> {
    s.call(
        "/module/agent".parse().unwrap(),
        OBJECT.parse().unwrap(),
        SEND_MARKDOWN.parse().unwrap(),
        &MarkdownOutput {
            group_id: group,
            bot_id: 42,
            bot_name: Some("Example".into()),
            source: source.into(),
        },
    )
    .await
    .unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn wasm_markdown_output_preserves_nodes_and_partial_delivery() {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..");
    assert!(
        Command::new("cargo")
            .args([
                "build",
                "--locked",
                "--offline",
                "-j",
                "2",
                "--profile",
                "guest",
                "--target",
                "wasm32-unknown-unknown",
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
    let s = Scheduler::new(
        temp.path().join("config.sqlite3"),
        RuntimeOptions {
            data_directory: temp.path().join("data"),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    let access = callers(vec![
        SubjectPattern::Path("/module/**".parse().unwrap()),
        SubjectPattern::Path("/user/admin".parse().unwrap()),
    ]);
    let reference = format!("media:{}", "a".repeat(64));
    let image = reference.clone();
    let sources = Arc::new(Mutex::new(Vec::<String>::new()));
    let capture = sources.clone();
    s.register_native(NativeInterface::new(
        typst::OBJECT.parse().unwrap(),
        "/typeset".parse().unwrap(),
        access.clone(),
        move |_, source: String| {
            let image = image.clone();
            let capture = capture.clone();
            async move {
                capture.lock().unwrap().push(source.clone());
                if source.contains("fail-render") {
                    Err(typst::Error::Execution("cannot typeset this node".into()))
                } else {
                    Ok(vec![image])
                }
            }
        },
    ))
    .unwrap();
    s.register_native(NativeInterface::new(
        managed::OBJECT.parse().unwrap(),
        "/get".parse().unwrap(),
        access,
        |_, _: String| async {
            Ok::<_, managed::Error>(managed::Blob {
                mime: "image/png".into(),
                bytes: vec![1, 2, 3],
            })
        },
    ))
    .unwrap();
    let gateway = Gateway::default();
    onebot_mock::install(
        &s,
        Client::new(gateway.clone()),
        onebot_mock::GroupScope::All,
        vec![SubjectPattern::Path("/module/**".parse().unwrap())],
    )
    .unwrap();
    for (manifest, artifact) in [
        (forest_markdown::manifest(), "forest_markdown"),
        (forest_message_output::manifest(), "forest_message_output"),
    ] {
        let dir = temp.path().join(artifact);
        std::fs::create_dir(&dir).unwrap();
        std::fs::copy(
            root.join(format!(
                "target/wasm32-unknown-unknown/guest/{artifact}.wasm"
            )),
            dir.join("module.wasm"),
        )
        .unwrap();
        forest_package::write(&dir, &manifest).unwrap();
        s.load(s.package(&dir).await.unwrap()).await.unwrap();
    }
    let source = "[reply](forest:reply/70/-9)[alice](forest:at/7)[smile](forest:face/14)\n\n$$\\frac{a}{b}$$\n\n|a|b|\n|-|-|\n|1|2|\n\n```rust\nlet x = 1;\n```\n\n---\n\n```text\nfail-render\n```\n";
    let report = send(&s, 70, source).await.unwrap();
    assert_eq!(report.sent.len(), 2);
    report.outcome.unwrap();
    let calls = gateway.0.lock().unwrap().clone();
    let first: Message = serde_json::from_value(calls[0]["message"].clone()).unwrap();
    let Message::Segments(segments) = first else {
        panic!()
    };
    assert!(matches!(&segments[0],Segment::Reply {id} if id=="-9"));
    assert!(
        segments
            .iter()
            .any(|s| matches!(s,Segment::At {qq} if qq=="7"))
    );
    assert_eq!(
        segments
            .iter()
            .filter(|s| matches!(s, Segment::Image(_)))
            .count(),
        3
    );
    assert!(
        calls[1]["message"]
            .to_string()
            .contains("```text\\nfail-render\\n```")
    );
    assert_eq!(sources.lock().unwrap().len(), 4);
    let before = gateway.0.lock().unwrap().len();
    assert!(
        send(&s, 70, "first\n\n---\n\n[reply](forest:reply/71/9)")
            .await
            .is_err()
    );
    assert_eq!(gateway.0.lock().unwrap().len(), before);
    let report = send(&s, 72, "first\n\n---\n\nsecond\n\n---\n\nthird")
        .await
        .unwrap();
    assert_eq!(report.sent.len(), 1);
    assert!(report.outcome.is_err());
    assert_eq!(gateway.0.lock().unwrap().len(), before + 2);

    let original = "literal [x](forest:at/888) [CQ:at,qq=999] & <tag> *bold*";
    let encoded: Result<Vec<Vec<md::Fragment>>, md::Error> = s
        .call(
            "/user/admin".parse().unwrap(),
            md::OBJECT.parse().unwrap(),
            "/encode".parse().unwrap(),
            &vec![md::Encode {
                group_id: 70,
                message: Message::Segments(vec![
                    Segment::text(original),
                    Segment::At { qq: "7".into() },
                ]),
            }],
        )
        .await
        .unwrap();
    let encoded = encoded
        .unwrap()
        .into_iter()
        .flatten()
        .filter_map(|p| match p {
            md::Fragment::Text(t) => Some(t),
            _ => None,
        })
        .collect::<String>();
    send(&s, 70, &encoded).await.unwrap().outcome.unwrap();
    let call = gateway.0.lock().unwrap().last().unwrap().clone();
    let decoded: Message = serde_json::from_value(call["message"].clone()).unwrap();
    let Message::Segments(segments) = decoded else {
        panic!()
    };
    let text = segments
        .iter()
        .filter_map(|s| match s {
            Segment::Text { text } => Some(text.as_str()),
            _ => None,
        })
        .collect::<String>();
    assert_eq!(text, original);
    assert_eq!(
        segments
            .iter()
            .filter(|s| matches!(s, Segment::At { .. }))
            .count(),
        1
    );

    s.config_writer("/user/admin".parse().unwrap())
        .set(forest_protocol::service::ConfigSet {
            target: forest_protocol::service::ConfigKey {
                object: OBJECT.parse().unwrap(),
                key: "/settings".parse().unwrap(),
            },
            value: forest_protocol::AsValue::as_value(&Settings {
                character_threshold: 5,
                ..Default::default()
            }),
        })
        .await
        .unwrap();
    send(&s, 70, "```rust\nlet x = 1;\n```")
        .await
        .unwrap()
        .outcome
        .unwrap();
    let last = gateway.0.lock().unwrap().last().unwrap().clone();
    let decoded: Message = serde_json::from_value(last["message"].clone()).unwrap();
    assert!(
        matches!(decoded,Message::Segments(parts) if matches!(&parts[0],Segment::Node(Node::Custom {..})))
    );
    s.shutdown(StopMode::Drain).await.unwrap();
}
