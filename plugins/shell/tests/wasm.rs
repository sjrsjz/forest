use forest_config::{ConfigStore, Key};
use forest_onebot::{
    Client, Transport, cq,
    forest::{GroupMessage, GroupSender, SendGroupMessage},
    message::{Message, Segment},
};
use forest_protocol::{
    AsValue, DispatchError, SubjectPattern,
    service::{ConfigKey, ConfigSet},
};
use forest_runtime::{
    ModulePhase, RuntimeOptions, Scheduler, StopMode,
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
fn message(user: i64, id: i32, text: &str) -> GroupMessage {
    GroupMessage {
        time: 1700000000,
        self_id: 42,
        post_type: "message".into(),
        message_type: "group".into(),
        sub_type: "normal".into(),
        message_id: id,
        group_id: 100,
        user_id: user,
        anonymous: None,
        message: Message::Segments(vec![Segment::text(text)]),
        raw_message: "unused".into(),
        sender: GroupSender::default(),
    }
}
fn input(ingress: &OneBotIngress, user: i64, id: i32, text: &str) {
    let report = ingress
        .receive(&serde_json::to_vec(&message(user, id, text)).unwrap())
        .unwrap();
    assert!(report.rejected.is_empty());
}
async fn reply(sent: &mut mpsc::UnboundedReceiver<SendGroupMessage>) -> String {
    let message = tokio::time::timeout(Duration::from_secs(30), sent.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(message.group_id, 100);
    assert_eq!(message.auto_escape, Some(true));
    cq::encode(&message.message).unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn real_wasm_group_authorization_management_configuration_and_school_help() {
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
                "--lib",
                "-p",
                "forest-shell",
                "-p",
                "forest-npucraft",
                "-p",
                "forest-test",
            ])
            .current_dir(&workspace)
            .status()
            .unwrap()
            .success()
    );
    let temp = tempfile::tempdir().unwrap();
    let catalog_root = temp.path().join("packages");
    for (name, manifest, artifact) in [
        ("shell", forest_shell::manifest(), "forest_shell"),
        ("npucraft", forest_npucraft::manifest(), "forest_npucraft"),
        ("hello", forest_test::manifest(), "forest_test"),
    ] {
        let path = catalog_root.join(name);
        std::fs::create_dir_all(&path).unwrap();
        std::fs::write(
            path.join("manifest.json"),
            serde_json::to_vec(&manifest).unwrap(),
        )
        .unwrap();
        std::fs::copy(
            workspace.join(format!(
                "target/wasm32-unknown-unknown/debug/{artifact}.wasm"
            )),
            path.join("module.wasm"),
        )
        .unwrap();
    }
    let bad = catalog_root.join("invalid");
    std::fs::create_dir(&bad).unwrap();
    std::fs::write(bad.join("manifest.json"), "bad manifest").unwrap();
    let store = ConfigStore::open(temp.path().join("config.sqlite3"))
        .await
        .unwrap();
    let scheduler = Scheduler::with_store(
        store.clone(),
        RuntimeOptions {
            data_directory: temp.path().join("modules"),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    let catalog =
        forest_control::packages::Packages::new(&catalog_root, scheduler.module_manager())
            .await
            .unwrap();
    let admin = scheduler.config_writer("/user/admin".parse().unwrap());
    forest_control::install(
        &scheduler,
        catalog,
        admin.clone(),
        vec![SubjectPattern::Path("/module/shell".parse().unwrap())],
    )
    .unwrap();
    let (tx, mut sent) = mpsc::unbounded_channel();
    let ingress = onebot::install(
        &scheduler,
        Client::new(Gateway(tx)),
        GroupScope::All,
        vec![SubjectPattern::Path("/module/**".parse().unwrap())],
    )
    .unwrap();
    let shell = scheduler.package(catalog_root.join("shell")).await.unwrap();
    let school = scheduler
        .package(catalog_root.join("npucraft"))
        .await
        .unwrap();
    scheduler.load(shell.clone()).await.unwrap();
    scheduler.load(school.clone()).await.unwrap();
    // Default authorization is closed. Drain gives a completion barrier for ignored input.
    input(&ingress, 7, 1, "#forest module list");
    scheduler
        .reload(shell.clone(), StopMode::Drain)
        .await
        .unwrap();
    scheduler
        .reload(school.clone(), StopMode::Drain)
        .await
        .unwrap();
    assert!(sent.try_recv().is_err());
    let settings = forest_shell::Settings {
        administrators: vec![7],
        allowed_groups: vec![100],
    };
    admin
        .set(ConfigSet {
            target: ConfigKey {
                object: forest_shell::OBJECT.parse().unwrap(),
                key: "/settings".parse().unwrap(),
            },
            value: settings.as_value(),
        })
        .await
        .unwrap();
    input(&ingress, 8, 2, "#forest module list");
    assert!(reply(&mut sent).await.contains("没有群聊管理权限"));
    // QQ display metadata cannot grant access, and module ACL prevents forged ingress.
    let forged = scheduler
        .call::<_, Result<(), String>>(
            "/module/other".parse().unwrap(),
            forest_shell::OBJECT.parse().unwrap(),
            "/group_message".parse().unwrap(),
            &message(7, 3, "#forest module list"),
        )
        .await;
    assert!(matches!(forged, Err(DispatchError::Denied)));
    let access = scheduler
        .call::<_, Result<forest_control::contract::Snapshot, forest_control::contract::Error>>(
            "/module/other".parse().unwrap(),
            "/core/control".parse().unwrap(),
            "/snapshot".parse().unwrap(),
            &(),
        )
        .await;
    assert!(matches!(access, Err(DispatchError::Denied)));
    input(&ingress, 7, 4, "#forest module list");
    let modules = reply(&mut sent).await;
    assert!(modules.contains("/module/shell") && modules.contains("Serving"));
    input(&ingress, 7, 5, "#forest packages");
    let packages = reply(&mut sent).await;
    assert!(packages.contains("/module/forest-test") && packages.contains("无效包"));
    input(&ingress, 7, 6, "#forest module load /module/forest-test");
    assert!(
        reply(&mut sent)
            .await
            .contains("已加载 /module/forest-test")
    );
    assert_eq!(
        scheduler.phase(&"/module/forest-test".parse().unwrap()),
        Some(ModulePhase::Serving)
    );
    input(
        &ingress,
        7,
        7,
        "#forest config set /module/forest-test /state/replies 99i64",
    );
    assert!(reply(&mut sent).await.contains("permission denied"));
    assert_eq!(
        store
            .get(
                &"/module/forest-test".parse().unwrap(),
                &Key::<i64>::new("/state/replies".parse().unwrap())
            )
            .await
            .unwrap(),
        0
    );
    input(&ingress, 7, 8, "#forest module reload /module/shell");
    assert!(reply(&mut sent).await.contains("请使用 WebUI"));
    assert_eq!(
        scheduler.phase(&forest_shell::OBJECT.parse().unwrap()),
        Some(ModulePhase::Serving)
    );
    for (id, trigger) in [(9, "地图"), (10, "卫星地图"), (11, "ciallo")] {
        input(&ingress, 9, id, trigger);
        let help = reply(&mut sent).await;
        assert!(help.contains("https://map.npucraft.com/bluemap-industry"));
        assert!(help.contains("https://plan.npucraft.com/"));
    }
    let school_settings = forest_npucraft::Settings {
        allowed_groups: vec![100],
        reply: "updated [CQ:at,qq=all]".into(),
        ..Default::default()
    };
    input(
        &ingress,
        7,
        12,
        &format!(
            "#forest config set /module/npucraft /settings {}",
            school_settings.as_value()
        ),
    );
    assert!(reply(&mut sent).await.contains("已保存"));
    input(&ingress, 7, 13, "#forest module reload /module/npucraft");
    assert!(reply(&mut sent).await.contains("已重载"));
    input(&ingress, 9, 14, "ciallo");
    assert!(
        reply(&mut sent)
            .await
            .contains("updated &#91;CQ:at,qq=all&#93;")
    );
    input(
        &ingress,
        7,
        15,
        "#forest config get /module/npucraft /settings",
    );
    assert!(reply(&mut sent).await.contains("updated"));
    input(&ingress, 7, 16, "#forest module unload /module/npucraft");
    assert!(reply(&mut sent).await.contains("已卸载"));
    assert_eq!(
        scheduler.phase(&forest_npucraft::OBJECT.parse().unwrap()),
        None
    );
    input(&ingress, 7, 17, "#forest module load /module/npucraft");
    assert!(reply(&mut sent).await.contains("已加载"));
    input(&ingress, 9, 18, "地图");
    assert!(reply(&mut sent).await.contains("updated"));
    // Removing authorization through the same config port affects the next command.
    input(
        &ingress,
        7,
        19,
        "#forest config set /module/shell /settings {administrators: [], allowed_groups: [100i64]}",
    );
    assert!(reply(&mut sent).await.contains("已保存"));
    input(&ingress, 7, 20, "#forest status");
    assert!(reply(&mut sent).await.contains("没有群聊管理权限"));
    scheduler.shutdown(StopMode::Drain).await.unwrap();
    drop(ingress);
    drop(admin);
    drop(scheduler);
    store.close().await.unwrap();
}
