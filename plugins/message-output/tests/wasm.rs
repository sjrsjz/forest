use forest_message_output::{GroupRule, Settings, contract::*, managed};
use forest_onebot::{
    Client, Transport, cq,
    forest::{self as onebot, SentMessage},
    message::{Message, Node, Segment},
};
use forest_protocol::{
    AsValue, SubjectPattern,
    manifest::callers,
    service::{ConfigKey, ConfigSet},
};
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
    async fn request(&self, action: &str, params: Vec<u8>) -> Result<Vec<u8>, Self::Error> {
        assert_eq!(action, "send_group_msg");
        let params: Value = serde_json::from_slice(&params).unwrap();
        let id = {
            let mut calls = self.0.lock().unwrap();
            calls.push(params.clone());
            calls.len() as i32
        };
        let response = if params["group_id"] == 44 {
            json!({"status":"failed","retcode":1400,"wording":"forward rejected","data":null})
        } else {
            json!({"status":"ok","retcode":0,"data":{"message_id":id}})
        };
        Ok(serde_json::to_vec(&response).unwrap())
    }
}

async fn send(s: &Scheduler, group_id: i64, body: &str) -> Result<SentMessage, Error> {
    s.call(
        "/module/agent".parse().unwrap(),
        OBJECT.parse().unwrap(),
        SEND.parse().unwrap(),
        &GroupOutput {
            group_id,
            bot_id: 10,
            bot_name: Some("Test Bot".into()),
            message: Message::Cq(body.into()),
        },
    )
    .await
    .unwrap()
}
async fn configure(s: &Scheduler, settings: &Settings) {
    s.config_writer("/user/admin".parse().unwrap())
        .set(ConfigSet {
            target: ConfigKey {
                object: OBJECT.parse().unwrap(),
                key: "/settings".parse().unwrap(),
            },
            value: settings.as_value(),
        })
        .await
        .unwrap();
}
fn message(g: &Gateway) -> Message {
    serde_json::from_value(g.0.lock().unwrap().last().unwrap()["message"].clone()).unwrap()
}
fn forward_content(message: Message) -> Vec<Segment> {
    let Message::Segments(nodes) = message else {
        panic!("expected node list")
    };
    let [
        Segment::Node(Node::Custom {
            user_id,
            nickname,
            content: Message::Segments(content),
        }),
    ]: [_; 1] = nodes.try_into().expect("one intact message per forward")
    else {
        panic!("expected custom node")
    };
    assert_eq!(user_id, "10");
    assert_eq!(nickname, "Test Bot");
    content
}
fn text(segments: &[Segment]) -> String {
    segments
        .iter()
        .filter_map(|s| {
            if let Segment::Text { text } = s {
                Some(text.as_str())
            } else {
                None
            }
        })
        .collect()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn actual_wasm_output_folds_by_group_preserves_cq_and_never_falls_back_to_spam() {
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
                "-j",
                "2",
                "--target",
                "wasm32-unknown-unknown",
                "-p",
                "forest-message-output",
                "--lib"
            ])
            .current_dir(&root)
            .status()
            .unwrap()
            .success()
    );
    let temp = tempfile::tempdir().unwrap();
    let dir = temp.path().join("output");
    std::fs::create_dir(&dir).unwrap();
    std::fs::copy(
        root.join("target/wasm32-unknown-unknown/debug/forest_message_output.wasm"),
        dir.join("module.wasm"),
    )
    .unwrap();
    forest_package::write(&dir, &forest_message_output::manifest()).unwrap();
    let s = Scheduler::new(
        temp.path().join("config.sqlite3"),
        RuntimeOptions {
            data_directory: temp.path().join("data"),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    let gateway = Gateway::default();
    forest_runtime::onebot::install(
        &s,
        Client::new(gateway.clone()),
        forest_runtime::onebot::GroupScope::All,
        vec![SubjectPattern::Path("/module/**".parse().unwrap())],
    )
    .unwrap();
    s.register_native(NativeInterface::new(
        managed::OBJECT.parse().unwrap(),
        "/get".parse().unwrap(),
        true,
        callers(vec![SubjectPattern::Path(OBJECT.parse().unwrap())]),
        |_, reference: String| async move {
            if reference == format!("media:{}", "a".repeat(64)) {
                Ok(managed::Blob {
                    mime: "image/png".into(),
                    bytes: vec![0, 1, 2],
                })
            } else {
                Err(managed::Error::InvalidInput(
                    "managed references only".into(),
                ))
            }
        },
    ))
    .unwrap();
    s.register_native(NativeInterface::new(
        "/module/markdown".parse().unwrap(),
        "/format".parse().unwrap(),
        true,
        callers(vec![SubjectPattern::Path(
            "/module/message-output".parse().unwrap(),
        )]),
        |_, _: forest_message_output::markdown::Format| async {
            Err::<Vec<forest_message_output::markdown::Part>, _>(
                forest_message_output::markdown::Error::Input("not used by native send".into()),
            )
        },
    ))
    .unwrap();
    let package = s.package(&dir).await.unwrap();
    s.load(package.clone()).await.unwrap();

    let short = "[CQ:at,qq=20] Hello [CQ:face,id=14]";
    assert_eq!(send(&s, 42, short).await.unwrap().message_id, 1);
    assert_eq!(
        message(&gateway),
        Message::Segments(cq::decode(short).unwrap())
    );
    let boundary = "界".repeat(1000);
    send(&s, 42, &boundary).await.unwrap();
    assert_eq!(
        message(&gateway),
        Message::Segments(vec![Segment::text(&boundary)])
    );
    let long = format!("{}🙂", boundary);
    send(&s, 42, &long).await.unwrap();
    assert_eq!(
        forward_content(message(&gateway)),
        vec![Segment::text(&long)]
    );
    let multiline = "line\n".repeat(21);
    send(&s, 42, &multiline).await.unwrap();
    assert_eq!(
        forward_content(message(&gateway)),
        vec![Segment::text(&multiline)]
    );
    let mixed = format!(
        "[CQ:reply,id=1][CQ:at,qq=20]{}[CQ:image,file=media:{}][CQ:face,id=14]tail &#91;literal&#93;",
        long,
        "a".repeat(64)
    );
    send(&s, 42, &mixed).await.unwrap();
    let flat = forward_content(message(&gateway));
    assert!(matches!(&flat[0], Segment::Reply {id} if id=="1"));
    assert!(matches!(&flat[1], Segment::At {qq} if qq=="20"));
    assert!(
        flat.iter()
            .any(|s| matches!(s,Segment::Image(image) if image.file=="base64://AAEC"))
    );
    assert!(text(&flat).ends_with("tail [literal]"));

    configure(
        &s,
        &Settings {
            groups: vec![GroupRule {
                group_id: 42,
                enabled: false,
            }],
            ..Default::default()
        },
    )
    .await;
    send(&s, 42, &long).await.unwrap();
    assert_eq!(
        message(&gateway),
        Message::Segments(vec![Segment::text(&long)])
    );
    send(&s, 43, &long).await.unwrap();
    assert_eq!(text(&forward_content(message(&gateway))), long);
    s.reload(package, StopMode::Drain).await.unwrap();
    send(&s, 42, &long).await.unwrap();
    assert_eq!(
        message(&gateway),
        Message::Segments(vec![Segment::text(&long)])
    );
    configure(
        &s,
        &Settings {
            enabled_by_default: false,
            groups: vec![GroupRule {
                group_id: 42,
                enabled: true,
            }],
            ..Default::default()
        },
    )
    .await;
    send(&s, 42, &long).await.unwrap();
    assert_eq!(text(&forward_content(message(&gateway))), long);
    send(&s, 43, &long).await.unwrap();
    assert_eq!(
        message(&gateway),
        Message::Segments(vec![Segment::text(&long)])
    );

    configure(&s, &Settings::default()).await;
    let before = gateway.0.lock().unwrap().len();
    assert!(matches!(
        send(&s, 44, &long).await,
        Err(Error::OneBot(onebot::OneBotError::Api { .. }))
    ));
    assert_eq!(gateway.0.lock().unwrap().len(), before + 1);
    assert!(matches!(
        send(&s, 42, "[CQ:image,file:bad]").await,
        Err(Error::InvalidMessage(_))
    ));
    assert!(
        send(&s, 42, "[CQ:image,file=https://example.com/image.png]")
            .await
            .is_err()
    );
    assert_eq!(gateway.0.lock().unwrap().len(), before + 1);
    let denied = s
        .call::<_, Result<SentMessage, Error>>(
            "/module/stranger".parse().unwrap(),
            OBJECT.parse().unwrap(),
            SEND.parse().unwrap(),
            &GroupOutput {
                group_id: 42,
                bot_id: 10,
                bot_name: None,
                message: Message::Cq(long),
            },
        )
        .await;
    assert!(matches!(
        denied,
        Err(forest_protocol::DispatchError::Denied)
    ));
    s.shutdown(StopMode::Drain).await.unwrap();
}
