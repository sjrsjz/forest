use forest_image_projection::{Settings, contract::*, media};
use forest_protocol::{
    AsValue, SubjectPattern,
    manifest::callers,
    service::{ConfigKey, ConfigSet},
};
use forest_runtime::{NativeInterface, RuntimeOptions, Scheduler, StopMode};
use image::{DynamicImage, GenericImageView, ImageFormat, RgbImage, RgbaImage};
use std::{
    io::Cursor,
    path::PathBuf,
    process::Command,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
};

fn encoded(image: DynamicImage, format: ImageFormat) -> Vec<u8> {
    let mut bytes = Cursor::new(Vec::new());
    image.write_to(&mut bytes, format).unwrap();
    bytes.into_inner()
}

async fn prepare(scheduler: &Scheduler, reference: &str) -> Result<media::Blob, Error> {
    scheduler
        .call(
            "/module/memory".parse().unwrap(),
            OBJECT.parse().unwrap(),
            PREPARE.parse().unwrap(),
            &reference.to_owned(),
        )
        .await
        .unwrap()
}

async fn configure(scheduler: &Scheduler, settings: &Settings) {
    scheduler
        .config_writer("/user/admin".parse().unwrap())
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

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn actual_wasm_scales_long_images_preserves_originals_and_reuses_persistent_derivatives() {
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
                "--profile",
                "guest",
                "--target",
                "wasm32-unknown-unknown",
                "-p",
                "forest-image-projection",
                "--lib",
            ])
            .current_dir(&root)
            .status()
            .unwrap()
            .success()
    );
    let temp = tempfile::tempdir().unwrap();
    let dir = temp.path().join("projection");
    std::fs::create_dir(&dir).unwrap();
    std::fs::copy(
        root.join("target/wasm32-unknown-unknown/guest/forest_image_projection.wasm"),
        dir.join("module.wasm"),
    )
    .unwrap();
    std::fs::write(
        dir.join("manifest.json"),
        serde_json::to_vec(&forest_image_projection::manifest()).unwrap(),
    )
    .unwrap();
    let large = Arc::new(encoded(
        DynamicImage::ImageRgb8(RgbImage::new(1320, 10281)),
        ImageFormat::Jpeg,
    ));
    let small = Arc::new(encoded(
        DynamicImage::ImageRgba8(RgbaImage::new(32, 16)),
        ImageFormat::Png,
    ));
    let calls = Arc::new(AtomicUsize::new(0));
    let observed = calls.clone();
    let original = large.clone();
    let short = small.clone();
    let scheduler = Scheduler::new(
        temp.path().join("config.sqlite3"),
        RuntimeOptions {
            data_directory: temp.path().join("data"),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    scheduler
        .register_native(NativeInterface::new(
            media::OBJECT.parse().unwrap(),
            "/get".parse().unwrap(),
            true,
            callers(vec![SubjectPattern::Path(OBJECT.parse().unwrap())]),
            move |_, reference: String| {
                let original = original.clone();
                let short = short.clone();
                let observed = observed.clone();
                async move {
                    observed.fetch_add(1, Ordering::SeqCst);
                    let bytes = match reference.as_str() {
                        "long" => original.as_ref().clone(),
                        "short" => short.as_ref().clone(),
                        "padded" => {
                            let mut bytes = short.as_ref().clone();
                            bytes.resize(20 * 1024 * 1024, 0);
                            bytes
                        }
                        _ => b"not an image".to_vec(),
                    };
                    Ok::<_, media::Error>(media::Blob {
                        mime: "application/octet-stream".into(),
                        bytes,
                    })
                }
            },
        ))
        .unwrap();
    let package = scheduler.package(&dir).await.unwrap();
    scheduler.load(package.clone()).await.unwrap();
    let projected = prepare(&scheduler, "long").await.unwrap();
    assert_eq!(projected.mime, "image/jpeg");
    let dimensions = image::load_from_memory(&projected.bytes)
        .unwrap()
        .dimensions();
    assert_eq!(dimensions.1, 4096);
    assert!((dimensions.0 as f64 / dimensions.1 as f64 - 1320.0 / 10281.0).abs() < 1.0 / 4096.0);
    assert_eq!(
        image::load_from_memory(&large).unwrap().dimensions(),
        (1320, 10281)
    );
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    scheduler
        .reload(package.clone(), StopMode::Drain)
        .await
        .unwrap();
    assert_eq!(prepare(&scheduler, "long").await.unwrap(), projected);
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    let unscaled = prepare(&scheduler, "short").await.unwrap();
    assert_eq!(unscaled.mime, "image/png");
    assert_eq!(&unscaled.bytes, small.as_ref());
    assert!(matches!(
        prepare(&scheduler, "broken").await,
        Err(Error::Image(_))
    ));

    // Small dimensions alone cannot bound the original container's byte size.
    assert_eq!(
        prepare(&scheduler, "padded").await.unwrap().bytes.len(),
        20 * 1024 * 1024
    );
    let thumbnail: Result<media::Blob, Error> = scheduler
        .call(
            "/module/memory".parse().unwrap(),
            OBJECT.parse().unwrap(),
            THUMBNAIL.parse().unwrap(),
            &Thumbnail {
                reference: "padded".into(),
                max_side: 1024,
            },
        )
        .await
        .unwrap();
    let thumbnail = thumbnail.unwrap();
    assert!(thumbnail.bytes.len() < 4096);
    assert_eq!(
        image::load_from_memory(&thumbnail.bytes)
            .unwrap()
            .dimensions(),
        (32, 16)
    );
    let before = calls.load(Ordering::SeqCst);
    scheduler
        .reload(package.clone(), StopMode::Drain)
        .await
        .unwrap();
    let cached: Result<media::Blob, Error> = scheduler
        .call(
            "/module/memory".parse().unwrap(),
            OBJECT.parse().unwrap(),
            THUMBNAIL.parse().unwrap(),
            &Thumbnail {
                reference: "padded".into(),
                max_side: 1024,
            },
        )
        .await
        .unwrap();
    assert_eq!(cached.unwrap(), thumbnail);
    assert_eq!(calls.load(Ordering::SeqCst), before);

    configure(
        &scheduler,
        &Settings {
            max_side: 2048,
            ..Default::default()
        },
    )
    .await;
    scheduler
        .reload(package.clone(), StopMode::Drain)
        .await
        .unwrap();
    let smaller = prepare(&scheduler, "long").await.unwrap();
    assert_eq!(
        image::load_from_memory(&smaller.bytes).unwrap().height(),
        2048
    );
    configure(
        &scheduler,
        &Settings {
            max_side: 1024,
            decode_memory_mib: 1,
        },
    )
    .await;
    scheduler.reload(package, StopMode::Drain).await.unwrap();
    assert!(matches!(
        prepare(&scheduler, "long").await,
        Err(Error::Image(_))
    ));
    assert_eq!(prepare(&scheduler, "short").await.unwrap().bytes, *small);
    scheduler.shutdown(StopMode::Drain).await.unwrap();
}
