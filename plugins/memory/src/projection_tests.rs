use crate::{
    content::*, embedding, image_projection, managed, openai::Assistant, reranker, types::*,
};
use forest_protocol::{SubjectPattern, manifest::callers};
use forest_runtime::{NativeInterface, RuntimeOptions, Scheduler, StopMode};
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};
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
            |_, reference: String| async move {
                if reference == format!("media:{}", "a".repeat(64)) {
                    Err(managed::Error::NotFound)
                } else {
                    Ok(managed::Blob {
                        mime: "image/png".into(),
                        bytes: vec![0x5a; 7 * 1024 * 1024],
                    })
                }
            },
        ))
        .unwrap();
    scheduler
        .register_native(NativeInterface::new(
            image_projection::OBJECT.parse().unwrap(),
            image_projection::PREPARE.parse().unwrap(),
            false,
            access.clone(),
            |_, reference: String| async move {
                if reference == format!("media:{}", "a".repeat(64)) {
                    Err(image_projection::Error::Media(managed::Error::NotFound))
                } else {
                    Ok(managed::Blob {
                        mime: "image/png".into(),
                        bytes: vec![0x5a; 7 * 1024 * 1024],
                    })
                }
            },
        ))
        .unwrap();
    scheduler
        .register_native(NativeInterface::new(
            embedding::OBJECT.parse().unwrap(),
            embedding::EMBED.parse().unwrap(),
            true,
            access.clone(),
            |_, _: Vec<embedding::Input>| async {
                Err::<embedding::Embeddings, _>(embedding::Error::NotConfigured)
            },
        ))
        .unwrap();
    scheduler
        .register_native(NativeInterface::new(
            image_projection::OBJECT.parse().unwrap(),
            image_projection::THUMBNAIL.parse().unwrap(),
            false,
            access.clone(),
            |_, _: image_projection::Thumbnail| async {
                Err::<managed::Blob, _>(image_projection::Error::Media(managed::Error::NotFound))
            },
        ))
        .unwrap();
    scheduler
        .register_native(NativeInterface::new(
            reranker::OBJECT.parse().unwrap(),
            reranker::RANK.parse().unwrap(),
            true,
            access,
            |_, _: reranker::Request| async {
                Err::<reranker::Scores, _>(reranker::Error::NotConfigured)
            },
        ))
        .unwrap();
    let projection_calls = Arc::new(AtomicUsize::new(0));
    let capture = projection_calls.clone();
    scheduler
        .register_native(NativeInterface::new(
            crate::markdown::OBJECT.parse().unwrap(),
            "/encode".parse().unwrap(),
            true,
            callers(vec![SubjectPattern::Path(
                "/module/memory".parse().unwrap(),
            )]),
            move |_, inputs: Vec<crate::markdown::Encode>| {
                let capture = capture.clone();
                async move {
                    Ok::<_, crate::markdown::Error>(
                        inputs
                            .into_iter()
                            .map(|_| {
                                vec![crate::markdown::Fragment::Text(format!(
                                    "projection recipe {}",
                                    capture.fetch_add(1, Ordering::SeqCst)
                                ))]
                            })
                            .collect::<Vec<_>>(),
                    )
                }
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
    let references = ["b", "c"].map(|c| format!("media:{}", c.repeat(64)));
    let accepted = scheduler
        .call::<_, Result<Accepted, Error>>(
            "/user/admin".parse().unwrap(),
            OBJECT.parse().unwrap(),
            "/accept".parse().unwrap(),
            &Incoming::Generated {
                group: 42,
                key: "large-image-projection".into(),
                content: MemoryContent::ModelTurn(ModelTurn {
                    time: 2,
                    assistant: Assistant {
                        content: Some(format!(
                            "[CQ:image,file={}][CQ:image,file={}]",
                            references[0], references[1]
                        )),
                        ..Default::default()
                    },
                }),
            },
        )
        .await
        .unwrap()
        .unwrap();
    let projection = scheduler
        .call::<_, Result<Projection, Error>>(
            "/user/admin".parse().unwrap(),
            OBJECT.parse().unwrap(),
            "/project".parse().unwrap(),
            &Project {
                ids: vec![accepted.unit.id],
                images: true,
            },
        )
        .await
        .unwrap()
        .unwrap();
    assert_eq!(projection.images.len(), 2);
    for image in &projection.images {
        assert!(references.contains(&image.reference));
        assert!(image.image.url.starts_with("data:image/png;base64,"));
        assert_eq!(
            image.image.url.len(),
            "data:image/png;base64,".len() + 4 * (7 * 1024 * 1024usize).div_ceil(3)
        );
    }
    assert!(
        projection
            .images
            .iter()
            .map(|image| image.image.url.len())
            .sum::<usize>()
            > 16 * 1024 * 1024
    );
    assert_eq!(
        scheduler.phase(&OBJECT.parse().unwrap()),
        Some(forest_runtime::ModulePhase::Serving)
    );
    let group:forest_onebot::forest::GroupMessage=serde_json::from_value(serde_json::json!({
        "time":1,"self_id":42,"post_type":"message","message_type":"group","sub_type":"normal",
        "message_id":-7,"group_id":43,"user_id":7,"anonymous":null,
        "message":[{"type":"text","data":{"text":"archive once"}}],"raw_message":"archive once","sender":{}
    })).unwrap();
    let accepted: Result<Accepted, Error> = scheduler
        .call(
            "/user/admin".parse().unwrap(),
            OBJECT.parse().unwrap(),
            "/accept".parse().unwrap(),
            &Incoming::Group(Box::new(group)),
        )
        .await
        .unwrap();
    let ids = vec![accepted.unwrap().unit.id];
    for _ in 0..2 {
        let archived: Result<(), Error> = scheduler
            .call(
                "/user/admin".parse().unwrap(),
                OBJECT.parse().unwrap(),
                "/archive".parse().unwrap(),
                &ids,
            )
            .await
            .unwrap();
        archived.unwrap();
    }
    assert_eq!(
        projection_calls.load(Ordering::SeqCst),
        1,
        "repeat archive must retain the original representation even if a newer recipe would produce different text"
    );
    scheduler.shutdown(StopMode::Drain).await.unwrap();
}
