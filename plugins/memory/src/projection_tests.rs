use crate::{content::*, embedding, managed, openai::Assistant, types::*};
use forest_protocol::{SubjectPattern, manifest::callers};
use forest_runtime::{NativeInterface, RuntimeOptions, Scheduler, StopMode};
use std::{path::PathBuf, process::Command};

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn malformed_generated_cq_can_be_archived_read_and_projected_by_real_wasm() {
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
                "forest-memory-module",
                "--lib"
            ])
            .current_dir(&root)
            .status()
            .unwrap()
            .success()
    );
    let temp = tempfile::tempdir().unwrap();
    let package = temp.path().join("memory");
    std::fs::create_dir(&package).unwrap();
    std::fs::copy(
        root.join("target/wasm32-unknown-unknown/debug/forest_memory_module.wasm"),
        package.join("module.wasm"),
    )
    .unwrap();
    std::fs::write(
        package.join("manifest.json"),
        serde_json::to_vec(&crate::manifest()).unwrap(),
    )
    .unwrap();
    let scheduler = Scheduler::new(
        temp.path().join("config.sqlite3"),
        RuntimeOptions {
            data_directory: temp.path().join("data"),
            ..RuntimeOptions::default()
        },
    )
    .await
    .unwrap();
    let access = callers(vec![SubjectPattern::Path(
        "/module/memory".parse().unwrap(),
    )]);
    scheduler
        .register_native(NativeInterface::new(
            managed::OBJECT.parse().unwrap(),
            "/register".parse().unwrap(),
            true,
            access.clone(),
            |_, _: managed::Source| async { Ok::<(), managed::Error>(()) },
        ))
        .unwrap();
    scheduler
        .register_native(NativeInterface::new(
            managed::OBJECT.parse().unwrap(),
            "/get".parse().unwrap(),
            true,
            access.clone(),
            |_, _: String| async { Err::<managed::Blob, _>(managed::Error::NotFound) },
        ))
        .unwrap();
    scheduler
        .register_native(NativeInterface::new(
            embedding::OBJECT.parse().unwrap(),
            embedding::EMBED.parse().unwrap(),
            true,
            access,
            |_, _: Vec<embedding::Input>| async {
                Err::<embedding::Embeddings, _>(embedding::Error::NotConfigured)
            },
        ))
        .unwrap();
    scheduler
        .load(scheduler.package(&package).await.unwrap())
        .await
        .unwrap();
    let body = format!("[CQ:image,file:media:{}]", "a".repeat(64));
    let accepted = scheduler
        .call::<_, Result<Accepted, Error>>(
            "/user/admin".parse().unwrap(),
            OBJECT.parse().unwrap(),
            "/accept".parse().unwrap(),
            &Incoming::Generated {
                group: 42,
                key: "bad-cq-output".into(),
                content: MemoryContent::ModelTurn(ModelTurn {
                    time: 1,
                    assistant: Assistant {
                        content: Some(body.clone()),
                        ..Default::default()
                    },
                }),
            },
        )
        .await
        .unwrap()
        .unwrap();
    let ids = vec![accepted.unit.id];
    scheduler
        .call::<_, Result<(), Error>>(
            "/user/admin".parse().unwrap(),
            OBJECT.parse().unwrap(),
            "/archive".parse().unwrap(),
            &ids,
        )
        .await
        .unwrap()
        .unwrap();
    let units = scheduler
        .call::<_, Result<Vec<MemoryUnit>, Error>>(
            "/user/admin".parse().unwrap(),
            OBJECT.parse().unwrap(),
            "/read".parse().unwrap(),
            &ids,
        )
        .await
        .unwrap()
        .unwrap();
    let MemoryContent::ModelTurn(turn) = &units[0].content else {
        panic!("source type changed")
    };
    assert_eq!(turn.assistant.content.as_deref(), Some(body.as_str()));
    let projection = scheduler
        .call::<_, Result<Projection, Error>>(
            "/user/admin".parse().unwrap(),
            OBJECT.parse().unwrap(),
            "/project".parse().unwrap(),
            &Project { ids, images: true },
        )
        .await
        .unwrap()
        .unwrap();
    assert_eq!(projection.units.len(), 1);
    assert!(projection.images.is_empty()); // Unavailable media is independent of valid text projection.
    scheduler.shutdown(StopMode::Drain).await.unwrap();
}
