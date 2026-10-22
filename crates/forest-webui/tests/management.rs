use forest_protocol::{
    AccessPolicy, AsSchema, AsValue, ConfigDeclaration, InterfaceUse, Manifest, SubjectPattern,
    manifest::callers,
};
use forest_runtime::{ModulePhase, NativeInterface, RuntimeOptions, Scheduler, StopMode};
use forest_webui::{Console, Options};
use serde_json::{Value, json};
use std::{
    path::{Path, PathBuf},
    process::Command,
    time::Duration,
};

fn artifacts(destination: &Path) -> [PathBuf; 2] {
    let workspace = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .canonicalize()
        .unwrap();
    [false, true].map(|second| {
        let mut command = Command::new("cargo");
        command.args([
            "build",
            "--offline",
            "--locked",
            "--target",
            "wasm32-unknown-unknown",
            "-p",
            "forest-scheduler-probe",
            "--lib",
        ]);
        if second {
            command.args(["--features", "second"]);
        }
        assert!(
            command
                .current_dir(&workspace)
                .env("CARGO_BUILD_JOBS", "2")
                .status()
                .unwrap()
                .success()
        );
        let path = destination.join(if second { "second.wasm" } else { "first.wasm" });
        std::fs::copy(
            workspace.join("target/wasm32-unknown-unknown/debug/forest_scheduler_probe.wasm"),
            &path,
        )
        .unwrap();
        path
    })
}
fn write_package(path: &Path, manifest: &Manifest, artifact: &Path) {
    std::fs::create_dir_all(path).unwrap();
    std::fs::write(
        path.join("manifest.json"),
        serde_json::to_vec(manifest).unwrap(),
    )
    .unwrap();
    std::fs::copy(artifact, path.join("module.wasm")).unwrap();
}
async fn phase(scheduler: &Scheduler, object: &str, expected: ModulePhase) {
    let object = object.parse().unwrap();
    tokio::time::timeout(Duration::from_secs(15), async {
        while scheduler.phase(&object) != Some(expected) {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
}
async fn value(scheduler: &Scheduler, object: &str, interface: &str) -> i64 {
    scheduler
        .call::<_, Result<i64, String>>(
            "/user/test".parse().unwrap(),
            object.parse().unwrap(),
            interface.parse().unwrap(),
            &(),
        )
        .await
        .unwrap()
        .unwrap()
}
async fn post(
    client: &reqwest::Client,
    base: &str,
    cookie: &str,
    action: &str,
    input: Value,
) -> reqwest::Response {
    client
        .post(format!("{base}/api/modules/{action}"))
        .header("Cookie", cookie)
        .json(&input)
        .send()
        .await
        .unwrap()
}
async fn success(response: reqwest::Response) {
    let status = response.status();
    assert_eq!(status, 204, "{}", response.text().await.unwrap());
}

#[tokio::test]
async fn local_packages_load_dependencies_and_replace_code_without_losing_configuration() {
    let temp = tempfile::tempdir().unwrap();
    let assets = temp.path().join("assets");
    let packages = temp.path().join("packages");
    std::fs::create_dir_all(&assets).unwrap();
    std::fs::create_dir_all(&packages).unwrap();
    std::fs::write(assets.join("index.html"), "Forest").unwrap();
    let [first, second] = artifacts(temp.path());
    let mut provider = forest_scheduler_probe::manifest("/module/provider".parse().unwrap());
    provider.package.version = "1".into();
    provider.configs.push(ConfigDeclaration {
        key: "/settings/text".parse().unwrap(),
        schema: String::as_schema(),
        default: String::new().as_value(),
        access: AccessPolicy {
            readers: vec![SubjectPattern::SelfModule],
            writers: vec![SubjectPattern::Path("/user/admin".parse().unwrap())],
            callers: vec![],
        },
    });
    let mut consumer = forest_scheduler_probe::manifest("/module/consumer".parse().unwrap());
    consumer.uses.push(InterfaceUse {
        object: provider.package.id.clone(),
        interface: "/echo".parse().unwrap(),
        input: String::as_schema(),
        output: <Result<String, String>>::as_schema(),
    });
    write_package(&packages.join("provider"), &provider, &first);
    write_package(&packages.join("consumer"), &consumer, &first);
    let invalid = packages.join("invalid");
    std::fs::create_dir(&invalid).unwrap();
    std::fs::write(invalid.join("manifest.json"), "bad manifest").unwrap();
    let bad_wasm = packages.join("bad-wasm");
    write_package(
        &bad_wasm,
        &forest_scheduler_probe::manifest("/module/bad-wasm".parse().unwrap()),
        &first,
    );
    std::fs::write(bad_wasm.join("module.wasm"), "not Wasm").unwrap();
    let scheduler = Scheduler::in_memory(RuntimeOptions {
        data_directory: temp.path().join("data"),
        ..Default::default()
    })
    .await
    .unwrap();
    let (observations, mut observed) = tokio::sync::mpsc::unbounded_channel();
    scheduler
        .register_native(NativeInterface::new(
            "/core/probe".parse().unwrap(),
            "/observe".parse().unwrap(),
            true,
            callers(vec![SubjectPattern::Path("/module/**".parse().unwrap())]),
            move |_, input: i64| {
                let observations = observations.clone();
                async move {
                    observations.send(input).unwrap();
                    Ok::<_, String>(input)
                }
            },
        ))
        .unwrap();
    let console = Console::bind(
        scheduler.inspector(),
        scheduler.module_manager(),
        scheduler.config_writer("/user/admin".parse().unwrap()),
        Options {
            address: "127.0.0.1:0".parse().unwrap(),
            password: "test-admin".into(),
            assets,
            packages: packages.clone(),
        },
    )
    .await
    .unwrap();
    let base = format!("http://{}", console.address());
    let (shutdown, closed) = tokio::sync::oneshot::channel();
    let server = tokio::spawn(console.serve(async {
        let _ = closed.await;
    }));
    let client = reqwest::Client::builder().no_proxy().build().unwrap();
    assert_eq!(
        client
            .get(format!("{base}/api/modules/packages"))
            .send()
            .await
            .unwrap()
            .status(),
        401
    );
    assert_eq!(
        post(&client, &base, "", "load", json!({"directory":"consumer"}))
            .await
            .status(),
        401
    );
    let login = client
        .post(format!("{base}/api/session"))
        .json(&json!({"password":"test-admin"}))
        .send()
        .await
        .unwrap();
    let cookie = login.headers()["set-cookie"]
        .to_str()
        .unwrap()
        .split(';')
        .next()
        .unwrap()
        .to_owned();
    assert_eq!(
        client
            .post(format!("{base}/api/modules/load"))
            .header("Cookie", &cookie)
            .header("Origin", "https://untrusted.example")
            .json(&json!({"directory":"consumer"}))
            .send()
            .await
            .unwrap()
            .status(),
        403
    );
    assert_eq!(
        post(
            &client,
            &base,
            &cookie,
            "plan",
            json!({"directory":"../assets"})
        )
        .await
        .status(),
        422
    );
    let listing: Value = client
        .get(format!("{base}/api/modules/packages"))
        .header("Cookie", &cookie)
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(listing.as_array().unwrap().len(), 4);
    assert!(
        listing
            .as_array()
            .unwrap()
            .iter()
            .any(|p| p["directory"] == "invalid" && p["status"] == "invalid")
    );
    let plan: Value = post(
        &client,
        &base,
        &cookie,
        "plan",
        json!({"directory":"consumer"}),
    )
    .await
    .json()
    .await
    .unwrap();
    assert_eq!(
        plan["order"][0]["manifest"]["package"]["id"],
        "/module/provider"
    );
    assert_eq!(
        plan["order"][1]["manifest"]["package"]["id"],
        "/module/consumer"
    );
    // Declaration preview neither compiles invalid artifacts nor starts modules.
    assert_eq!(
        post(
            &client,
            &base,
            &cookie,
            "plan",
            json!({"directory":"bad-wasm"})
        )
        .await
        .status(),
        200
    );
    assert!(scheduler.inspector().snapshot().unwrap().modules.is_empty());
    assert_eq!(
        post(
            &client,
            &base,
            &cookie,
            "load",
            json!({"directory":"bad-wasm"})
        )
        .await
        .status(),
        422
    );
    success(
        post(
            &client,
            &base,
            &cookie,
            "load",
            json!({"directory":"consumer"}),
        )
        .await,
    )
    .await;
    assert_eq!(
        scheduler.phase(&consumer.package.id),
        Some(ModulePhase::Serving)
    );
    configuration_writes(&client, &base, &cookie, &scheduler).await;
    assert_eq!(
        value(&scheduler, "/module/provider", "/implementation").await,
        1
    );
    assert_eq!(value(&scheduler, "/module/provider", "/increment").await, 1);
    assert_eq!(
        post(
            &client,
            &base,
            &cookie,
            "unload",
            json!({"object":"/module/provider","mode":"drain"})
        )
        .await
        .status(),
        422
    );
    assert_eq!(
        post(
            &client,
            &base,
            &cookie,
            "reload",
            json!({"object":"/module/provider","directory":"consumer","mode":"drain"})
        )
        .await
        .status(),
        422
    );

    // Keep Drain pending, disconnect its HTTP waiter, then explicitly Force it.
    let current = scheduler.clone();
    let call = tokio::spawn(async move {
        current
            .call::<_, Result<i64, String>>(
                "/user/test".parse().unwrap(),
                "/module/provider".parse().unwrap(),
                "/serial".parse().unwrap(),
                &60_000i64,
            )
            .await
    });
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(15), observed.recv())
            .await
            .unwrap(),
        Some(1)
    );
    provider.package.version = "2".into();
    write_package(&packages.join("provider"), &provider, &second);
    let http = client.clone();
    let url = base.clone();
    let session = cookie.clone();
    let pending = tokio::spawn(async move {
        post(
            &http,
            &url,
            &session,
            "reload",
            json!({"object":"/module/provider","directory":"provider","mode":"drain"}),
        )
        .await
    });
    phase(&scheduler, "/module/provider", ModulePhase::Stopping).await;
    let snapshot: Value = client
        .get(format!("{base}/api/inspect/snapshot"))
        .header("Cookie", &cookie)
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let view = snapshot["modules"]
        .as_array()
        .unwrap()
        .iter()
        .find(|m| m["object"] == "/module/provider")
        .unwrap();
    assert_eq!(view["lifecycle"]["phase"], "stopping");
    assert_eq!(view["lifecycle"]["replacement"]["version"], "2");
    pending.abort();
    let _ = pending.await;
    success(
        post(
            &client,
            &base,
            &cookie,
            "force",
            json!({"object":"/module/provider"}),
        )
        .await,
    )
    .await;
    phase(&scheduler, "/module/provider", ModulePhase::Serving).await;
    assert!(call.await.unwrap().is_err());
    assert_eq!(
        value(&scheduler, "/module/provider", "/implementation").await,
        2
    );
    assert_eq!(value(&scheduler, "/module/provider", "/increment").await, 2);
    for mode in ["at_yield", "force"] {
        success(
            post(
                &client,
                &base,
                &cookie,
                "reload",
                json!({"object":"/module/provider","directory":"provider","mode":mode}),
            )
            .await,
        )
        .await;
        assert_eq!(
            value(&scheduler, "/module/provider", "/implementation").await,
            2
        );
    }
    success(
        post(
            &client,
            &base,
            &cookie,
            "unload",
            json!({"object":"/module/consumer","mode":"at_yield"}),
        )
        .await,
    )
    .await;
    success(
        post(
            &client,
            &base,
            &cookie,
            "unload",
            json!({"object":"/module/provider","mode":"force"}),
        )
        .await,
    )
    .await;
    assert!(scheduler.inspector().snapshot().unwrap().modules.is_empty());
    success(
        post(
            &client,
            &base,
            &cookie,
            "load",
            json!({"directory":"provider"}),
        )
        .await,
    )
    .await;
    assert_eq!(value(&scheduler, "/module/provider", "/increment").await, 3);
    scheduler.shutdown(StopMode::Drain).await.unwrap();
    shutdown.send(()).unwrap();
    server.await.unwrap().unwrap();
}

async fn configuration_writes(
    client: &reqwest::Client,
    base: &str,
    cookie: &str,
    scheduler: &Scheduler,
) {
    let endpoint = format!("{base}/api/config/set");
    // Exercise a body larger than the console's small login/management limit.
    let text = "配置提示词\n".repeat(1000);
    let input = json!({"object":"/module/provider", "key":"/settings/text", "value":serde_json::to_string(&text).unwrap()});
    assert_eq!(
        client
            .post(&endpoint)
            .json(&input)
            .send()
            .await
            .unwrap()
            .status(),
        401
    );
    assert_eq!(
        client
            .post(&endpoint)
            .header("Cookie", cookie)
            .header("Origin", "https://untrusted.example")
            .json(&input)
            .send()
            .await
            .unwrap()
            .status(),
        403
    );
    success(
        client
            .post(&endpoint)
            .header("Cookie", cookie)
            .json(&input)
            .send()
            .await
            .unwrap(),
    )
    .await;
    let saved = scheduler
        .inspector()
        .config_entry(
            "/module/provider".parse().unwrap(),
            "/settings/text".parse().unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(saved.value(), &forest_protocol::Value::String(text.clone()));
    assert_eq!(
        saved.default_value(),
        &forest_protocol::Value::String(String::new())
    );

    for (key, value, status) in [
        ("/settings/text", "true", 422),
        ("/settings/text", "{", 400),
        ("/counter", "90", 403),
        ("/absent", "90", 403),
    ] {
        assert_eq!(
            client
                .post(&endpoint)
                .header("Cookie", cookie)
                .json(&json!({"object":"/module/provider", "key":key, "value":value}))
                .send()
                .await
                .unwrap()
                .status(),
            status
        );
    }
    let mut forged = input.clone();
    forged["subject"] = json!("/module/provider");
    assert_eq!(
        client
            .post(&endpoint)
            .header("Cookie", cookie)
            .json(&forged)
            .send()
            .await
            .unwrap()
            .status(),
        422
    );
    for (key, writable) in [("/settings/text", true), ("/counter", false)] {
        let view: Value = client
            .get(format!("{base}/api/inspect/config-entry"))
            .query(&[("object", "/module/provider"), ("key", key)])
            .header("Cookie", cookie)
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        assert_eq!(view["data"]["writable"], writable);
    }
    let target = forest_protocol::service::ConfigKey {
        object: "/module/provider".parse().unwrap(),
        key: "/settings/text".parse().unwrap(),
    };
    assert_eq!(
        scheduler
            .config_writer("/user/viewer".parse().unwrap())
            .check(&target),
        Err(forest_protocol::DispatchError::Denied)
    );
    let saved = scheduler
        .inspector()
        .config_entry(target.object, target.key)
        .await
        .unwrap();
    assert_eq!(saved.value(), &forest_protocol::Value::String(text));
}
