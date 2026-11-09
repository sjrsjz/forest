//! Prepare one local fixture through the actual Wasm ABI, without a Bot connection.
use forest_image_projection::{contract::*, media};
use forest_protocol::{SubjectPattern, manifest::callers};
use forest_runtime::{NativeInterface, RuntimeOptions, Scheduler, StopMode};
use std::{path::PathBuf, sync::Arc};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<_> = std::env::args_os().skip(1).collect();
    if args.len() != 2 {
        return Err("usage: verify-image <source> <destination>".into());
    }
    let bytes = Arc::new(std::fs::read(&args[0])?);
    let temp = tempfile::tempdir()?;
    let scheduler = Scheduler::new(
        temp.path().join("config.sqlite3"),
        RuntimeOptions {
            data_directory: temp.path().join("data"),
            ..Default::default()
        },
    )
    .await?;
    scheduler.register_native(NativeInterface::new(
        media::OBJECT.parse()?,
        "/get".parse()?,
        true,
        callers(vec![SubjectPattern::Path(OBJECT.parse()?)]),
        move |_, _: String| {
            let bytes = bytes.clone();
            async move {
                Ok::<_, media::Error>(media::Blob {
                    mime: "application/octet-stream".into(),
                    bytes: bytes.as_ref().clone(),
                })
            }
        },
    ))?;
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..");
    scheduler
        .load(
            scheduler
                .package(root.join("packages/image-projection"))
                .await?,
        )
        .await?;
    let output: Result<media::Blob, Error> = scheduler
        .call(
            "/user/admin".parse()?,
            OBJECT.parse()?,
            PREPARE.parse()?,
            &"fixture".to_owned(),
        )
        .await?;
    let output = output.map_err(|e| format!("{e:?}"))?;
    std::fs::write(&args[1], &output.bytes)?;
    println!("{}: {} bytes", output.mime, output.bytes.len());
    scheduler.shutdown(StopMode::Drain).await?;
    Ok(())
}
