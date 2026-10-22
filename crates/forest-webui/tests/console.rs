use forest_config::{ConfigStore, Declaration};
use forest_protocol::{SubjectPattern, Token, Value, manifest::callers};
use forest_runtime::{NativeInterface, RuntimeOptions, Scheduler};
use forest_serde::adt::{Schema, SchemaEnum};
use forest_webui::{Console, Options};
use futures::StreamExt;
use serde_json::json;
use std::{
    collections::BTreeMap,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

#[tokio::test]
async fn authenticated_inspection_is_read_only_and_stream_closes_on_shutdown() {
    let temp = tempfile::tempdir().unwrap();
    std::fs::write(temp.path().join("index.html"), "<title>Forest</title>").unwrap();
    let store = ConfigStore::open_in_memory().await.unwrap();
    let object: Token = "/module/private".parse().unwrap();
    let key: Token = "/private/value".parse().unwrap();
    let value = Value::Map(BTreeMap::from([(
        Value::I64(i64::MAX),
        Value::Bytes(vec![7; 8192]),
    )]));
    let schema = Schema::from_schemas(vec![SchemaEnum::Map(
        Box::new(SchemaEnum::I64),
        Box::new(SchemaEnum::Bytes(8192)),
    )])
    .unwrap();
    store
        .register(
            object.clone(),
            [Declaration::new(key.clone(), schema, value.clone()).unwrap()],
        )
        .await
        .unwrap();
    let scheduler = Scheduler::with_store(
        store.clone(),
        RuntimeOptions {
            data_directory: temp.path().join("data"),
            ..RuntimeOptions::default()
        },
    )
    .await
    .unwrap();
    let called = Arc::new(AtomicUsize::new(0));
    let observations = called.clone();
    scheduler
        .register_native(NativeInterface::new(
            "/core/test".parse().unwrap(),
            "/must-not-run".parse().unwrap(),
            true,
            callers(vec![SubjectPattern::SelfModule]),
            move |_, (): ()| {
                let calls = observations.clone();
                async move {
                    calls.fetch_add(1, Ordering::SeqCst);
                    Ok::<_, String>(())
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
            assets: temp.path().into(),
            packages: temp.path().into(),
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
    assert!(
        client
            .get(&base)
            .send()
            .await
            .unwrap()
            .status()
            .is_success()
    );
    for path in [
        "snapshot",
        "events",
        "logs",
        "config-entities",
        "config-keys?object=/module/private",
        "config-entry?object=/module/private&key=/private/value",
        "config-value?object=/module/private&key=/private/value",
    ] {
        assert_eq!(
            client
                .get(format!("{base}/api/inspect/{path}"))
                .send()
                .await
                .unwrap()
                .status(),
            401
        );
    }
    assert_eq!(
        client
            .get(format!(
                "{base}/api/config/form?object=/module/private&key=/private/value"
            ))
            .send()
            .await
            .unwrap()
            .status(),
        401
    );
    assert_eq!(
        client
            .post(format!("{base}/api/session"))
            .json(&json!({"password":"wrong"}))
            .send()
            .await
            .unwrap()
            .status(),
        401
    );
    assert_eq!(
        client
            .post(format!("{base}/api/session"))
            .header("Origin", "https://untrusted.example")
            .json(&json!({"password":"test-admin"}))
            .send()
            .await
            .unwrap()
            .status(),
        403
    );
    let login = client
        .post(format!("{base}/api/session"))
        .json(&json!({"password":"test-admin"}))
        .send()
        .await
        .unwrap();
    assert_eq!(login.status(), 204);
    let set_cookie = login.headers().get("set-cookie").unwrap().to_str().unwrap();
    assert!(set_cookie.contains("HttpOnly") && set_cookie.contains("SameSite=Strict"));
    let cookie = set_cookie.split(';').next().unwrap();
    let snapshot: serde_json::Value = client
        .get(format!("{base}/api/inspect/snapshot"))
        .header("Cookie", cookie)
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert!(snapshot["modules"].as_array().unwrap().is_empty());
    assert_eq!(snapshot["hosts"][0]["object"], "/core/test");
    let headers: serde_json::Value = client
        .get(format!("{base}/api/inspect/config-keys"))
        .query(&[("object", object.to_string())])
        .header("Cookie", cookie)
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(headers["data"][0]["key"], "/private/value");
    assert!(headers["data"][0].get("current").is_none());
    let result = client
        .get(format!("{base}/api/inspect/config-entry"))
        .query(&[("object", object.to_string()), ("key", key.to_string())])
        .header("Cookie", cookie)
        .send()
        .await
        .unwrap();
    assert_eq!(result.headers()["cache-control"], "no-store");
    let entry: serde_json::Value = result.json().await.unwrap();
    assert_eq!(entry["data"]["current"]["kind"], "map");
    assert_eq!(entry["data"]["writable"], false);
    assert_eq!(
        entry["data"]["current"]["entries"][0]["key"]["text"],
        i64::MAX.to_string()
    );
    assert_eq!(
        entry["data"]["current"]["entries"][0]["value"]["length"],
        8192
    );
    let full = client
        .get(format!("{base}/api/inspect/config-value"))
        .query(&[("object", object.to_string()), ("key", key.to_string())])
        .header("Cookie", cookie)
        .send()
        .await
        .unwrap()
        .text()
        .await
        .unwrap();
    assert_eq!(full, value.to_string());
    let logs = scheduler.logs();
    logs.write(
        forest_protocol::service::LogLevel::Info,
        "/core/test".parse().unwrap(),
        "before subscription",
    );
    let response = client
        .get(format!("{base}/api/inspect/logs"))
        .header("Cookie", cookie)
        .send()
        .await
        .unwrap();
    let mut log_stream = response.bytes_stream();
    let chunk = tokio::time::timeout(Duration::from_secs(5), log_stream.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    let initial = String::from_utf8_lossy(&chunk);
    assert!(initial.contains("before subscription"));
    assert!(initial.contains("\"reset\":true"));
    logs.write(
        forest_protocol::service::LogLevel::Warn,
        "/module/test".parse().unwrap(),
        "after subscription",
    );
    let chunk = tokio::time::timeout(Duration::from_secs(5), log_stream.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    let update = String::from_utf8_lossy(&chunk);
    assert!(update.contains("after subscription"));
    assert!(!update.contains("before subscription"));
    let resumed = client
        .get(format!("{base}/api/inspect/logs"))
        .header("Cookie", cookie)
        .header("Last-Event-ID", format!("{}:1", logs.reader().id()))
        .send()
        .await
        .unwrap();
    let mut resumed = resumed.bytes_stream();
    let chunk = tokio::time::timeout(Duration::from_secs(5), resumed.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    let update = String::from_utf8_lossy(&chunk);
    assert!(update.contains("after subscription"));
    assert!(!update.contains("before subscription"));
    assert!(update.contains("\"reset\":false"));
    let restarted = client
        .get(format!("{base}/api/inspect/logs"))
        .header("Cookie", cookie)
        .header("Last-Event-ID", "previous-host:1")
        .send()
        .await
        .unwrap();
    let mut restarted = restarted.bytes_stream();
    let chunk = tokio::time::timeout(Duration::from_secs(5), restarted.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    let update = String::from_utf8_lossy(&chunk);
    assert!(update.contains("\"reset\":true"));
    assert!(update.contains("before subscription") && update.contains("after subscription"));
    assert_eq!(
        client
            .get(format!(
                "{base}/api/inspect/config-keys?object=/module/unknown"
            ))
            .header("Cookie", cookie)
            .send()
            .await
            .unwrap()
            .status(),
        404
    );
    assert_eq!(
        client
            .patch(format!(
                "{base}/api/inspect/config-entry?object=/module/private&key=/private/value"
            ))
            .header("Cookie", cookie)
            .json(&json!({"current":0}))
            .send()
            .await
            .unwrap()
            .status(),
        405
    );
    let response = client
        .get(format!("{base}/api/inspect/events"))
        .header("Cookie", cookie)
        .send()
        .await
        .unwrap();
    let mut stream = response.bytes_stream();
    let chunk = tokio::time::timeout(Duration::from_secs(5), stream.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert!(String::from_utf8_lossy(&chunk).lines().any(|line| {
        line.strip_prefix("event:")
            .is_some_and(|v| v.trim() == "snapshot")
    }));
    assert_eq!(called.load(Ordering::SeqCst), 0);
    assert_eq!(store.entry(&object, &key).await.unwrap().value(), &value);
    shutdown.send(()).unwrap();
    tokio::time::timeout(Duration::from_secs(5), server)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    drop(stream);
    drop(log_stream);
    drop(resumed);
    drop(restarted);
    store.close().await.unwrap();
}
