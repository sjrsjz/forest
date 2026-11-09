use base64::{Engine, engine::general_purpose::STANDARD};
use forest_onebot::{
    Client, Transport, cq,
    forest::{GroupMessage, GroupSender, SendGroupMessage},
    message::{Message, Segment},
};
use forest_protocol::{
    AsValue, SubjectPattern,
    service::{ConfigKey, ConfigSet},
};
use forest_runtime::{
    RuntimeOptions, Scheduler, StopMode,
    onebot::{self, GroupScope, OneBotIngress},
};
use std::{path::PathBuf, process::Command, time::Duration};
use tokio::sync::mpsc;

struct Gateway(mpsc::UnboundedSender<SendGroupMessage>);
impl Transport for Gateway {
    type Error = std::io::Error;
    async fn request(&self, action: &str, params: Vec<u8>) -> Result<Vec<u8>, Self::Error> {
        assert_eq!(action, "send_group_msg");
        self.0.send(serde_json::from_slice(&params)?).unwrap();
        Ok(br#"{"status":"ok","retcode":0,"data":{"message_id":1}}"#.to_vec())
    }
}

fn input(ingress: &OneBotIngress, user_id: i64, message: Message) {
    let event = GroupMessage {
        time: 1700000000,
        self_id: 42,
        post_type: "message".into(),
        message_type: "group".into(),
        sub_type: "normal".into(),
        message_id: 7,
        group_id: 100,
        user_id,
        anonymous: None,
        message,
        raw_message: "not used as source".into(),
        sender: GroupSender::default(),
    };
    let report = ingress
        .receive(&serde_json::to_vec(&event).unwrap())
        .unwrap();
    assert_eq!(report.accepted, 1);
    assert!(report.rejected.is_empty());
}

async fn reply(receiver: &mut mpsc::UnboundedReceiver<SendGroupMessage>) -> Vec<Segment> {
    let sent = tokio::time::timeout(Duration::from_secs(30), receiver.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(sent.group_id, 100);
    assert_eq!(sent.auto_escape, Some(false));
    let segments = cq::segments(&sent.message).unwrap().into_owned();
    assert!(matches!(&segments[0], Segment::Reply { id } if id == "7"));
    segments.into_iter().skip(1).collect()
}

fn assert_images(segments: &[Segment], pages: usize) {
    assert_eq!(segments.len(), pages);
    for segment in segments {
        let Segment::Image(image) = segment else {
            panic!("expected rendered image, got {segment:?}")
        };
        let bytes = STANDARD
            .decode(image.file.strip_prefix("base64://").unwrap())
            .unwrap();
        assert_eq!(&bytes[..8], b"\x89PNG\r\n\x1a\n");
        let width = u32::from_be_bytes(bytes[16..20].try_into().unwrap());
        let height = u32::from_be_bytes(bytes[20..24].try_into().unwrap());
        assert_eq!(width, 960);
        assert!(height > 32);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn actual_wasm_command_renders_delivers_reports_errors_and_reloads() {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .canonicalize()
        .unwrap();
    for args in [
        vec![
            "build",
            "--locked",
            "--offline",
            "-j",
            "2",
            "--target",
            "wasm32-unknown-unknown",
            "--lib",
            "-p",
            "forest-typst-command",
            "-p",
            "forest-media",
            "-p",
            "forest-message-output",
            "-p",
            "forest-markdown",
        ],
        vec![
            "build",
            "--locked",
            "--offline",
            "-j",
            "2",
            "--target",
            "wasm32-unknown-unknown",
            "--lib",
            "--profile",
            "guest",
            "-p",
            "forest-typst",
        ],
    ] {
        assert!(
            Command::new("cargo")
                .args(args)
                .current_dir(&root)
                .status()
                .unwrap()
                .success()
        );
    }
    let temp = tempfile::tempdir().unwrap();
    let scheduler = Scheduler::new(
        temp.path().join("config.sqlite3"),
        RuntimeOptions {
            data_directory: temp.path().join("data"),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    let (tx, mut sent) = mpsc::unbounded_channel();
    let ingress = onebot::install(
        &scheduler,
        Client::new(Gateway(tx)),
        GroupScope::All,
        vec![SubjectPattern::Path("/module/**".parse().unwrap())],
    )
    .unwrap();
    let mut command_package = None;
    for (manifest, artifact, profile) in [
        (forest_media::manifest(), "forest_media", "debug"),
        (forest_typst::manifest(), "forest_typst", "guest"),
        (forest_markdown::manifest(), "forest_markdown", "debug"),
        (
            forest_message_output::manifest(),
            "forest_message_output",
            "debug",
        ),
        (
            forest_typst_command::manifest(),
            "forest_typst_command",
            "debug",
        ),
    ] {
        let path = temp.path().join(artifact);
        std::fs::create_dir(&path).unwrap();
        std::fs::write(
            path.join("manifest.json"),
            serde_json::to_vec(&manifest).unwrap(),
        )
        .unwrap();
        std::fs::copy(
            root.join(format!(
                "target/wasm32-unknown-unknown/{profile}/{artifact}.wasm"
            )),
            path.join("module.wasm"),
        )
        .unwrap();
        let package = scheduler.package(&path).await.unwrap();
        scheduler.load(package.clone()).await.unwrap();
        if manifest.package.id.to_string() == forest_typst_command::OBJECT {
            command_package = Some(package);
        }
    }
    let command_package = command_package.unwrap();
    for (user, text) in [(9, "ordinary chat"), (9, "/typstfoo x"), (42, "/typst x")] {
        input(&ingress, user, Message::Segments(vec![Segment::text(text)]));
    }
    scheduler
        .reload(command_package.clone(), StopMode::Drain)
        .await
        .unwrap();
    assert!(sent.try_recv().is_err());

    // No Agent or model Provider exists in this runtime.
    input(
        &ingress,
        9,
        Message::Segments(vec![Segment::text("/typst")]),
    );
    let help = reply(&mut sent).await;
    assert!(matches!(&help[0], Segment::Text { text } if text.contains("源码")));

    let source = "= 中文 & 字符\n#table(columns: 2, [A & B], [方括号])\n$ integral_0^1 x^2 dif x = 1/3 $\n#pagebreak()\n第二页";
    input(
        &ingress,
        9,
        Message::Segments(vec![Segment::text(format!("/typst\n{source}"))]),
    );
    assert_images(&reply(&mut sent).await, 2);

    // Wire CQ decoding and the code fence must preserve brackets and ampersands.
    let fenced = Message::Segments(vec![
        Segment::Reply { id: "6".into() },
        Segment::at("42"),
        Segment::text(" /typst\n```typst\n#table(columns: 2, [A & B], [中文])\n```"),
    ]);
    input(&ingress, 9, Message::Cq(cq::encode(&fenced).unwrap()));
    assert_images(&reply(&mut sent).await, 1);

    input(
        &ingress,
        9,
        Message::Segments(vec![Segment::text("/typst #missing_function()")]),
    );
    let error = reply(&mut sent).await;
    assert!(
        matches!(&error[0], Segment::Text { text } if text.contains("第 1 行") && text.contains("unknown variable"))
    );
    assert!(sent.try_recv().is_err());

    input(
        &ingress,
        9,
        Message::Segments(vec![Segment::text(
            "/typst\n```typst\nmissing closing fence",
        )]),
    );
    let error = reply(&mut sent).await;
    assert!(matches!(&error[0], Segment::Text { text } if text.contains("结束标记")));

    scheduler
        .config_writer("/user/admin".parse().unwrap())
        .set(ConfigSet {
            target: ConfigKey {
                object: forest_typst_command::OBJECT.parse().unwrap(),
                key: "/settings".parse().unwrap(),
            },
            value: forest_typst_command::Settings {
                allowed_groups: vec![200],
            }
            .as_value(),
        })
        .await
        .unwrap();
    scheduler
        .reload(command_package.clone(), StopMode::Drain)
        .await
        .unwrap();
    input(
        &ingress,
        9,
        Message::Segments(vec![Segment::text("/typst should be ignored")]),
    );
    scheduler
        .reload(command_package, StopMode::Drain)
        .await
        .unwrap();
    assert!(sent.try_recv().is_err());
    scheduler.shutdown(StopMode::Drain).await.unwrap();
}
