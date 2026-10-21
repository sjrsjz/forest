//! One live batch through the actual Wasm plugin. Configuration stays local.
use base64::{Engine, engine::general_purpose::STANDARD};
use forest_config::{ConfigStore, Declaration, Key};
use forest_qwen_vl_embedding::{
    api::{CONNECTION, Connection, DIMENSIONS, EMBED, Embeddings, Error, Input, OBJECT},
    manifest,
};
use forest_runtime::{RuntimeOptions, Scheduler, StopMode};
use std::{path::PathBuf, time::Instant};

fn cosine(left: &[f32], right: &[f32]) -> f64 {
    left.iter()
        .zip(right)
        .map(|(a, b)| f64::from(*a) * f64::from(*b))
        .sum()
}

#[tokio::main(worker_threads = 2)]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let configuration = std::env::args()
        .nth(1)
        .ok_or("usage: verify <connection.json>")?;
    let connection: Connection = serde_json::from_slice(&tokio::fs::read(configuration).await?)?;
    let image_url = "https://qianwen-res.oss-cn-beijing.aliyuncs.com/Qwen-VL/assets/demo.jpeg";
    let image = reqwest::Client::builder()
        .no_proxy()
        .build()?
        .get(image_url)
        .send()
        .await?
        .error_for_status()?
        .bytes()
        .await?;
    let inputs = vec![
        Input::Text("A woman plays with her dog on a sunny beach.".into()),
        Input::Text("An SQL database stores transactions and vector indexes.".into()),
        Input::Image(image_url.into()),
        Input::Image(format!("data:image/jpeg;base64,{}", STANDARD.encode(image))),
    ];
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .canonicalize()?;
    let temp = tempfile::tempdir()?;
    let store = ConfigStore::open(temp.path().join("config.sqlite3")).await?;
    let object = OBJECT.parse()?;
    let declaration = &manifest().configs[0];
    store
        .register(
            object,
            [Declaration::new(
                declaration.key.clone(),
                declaration.schema.clone(),
                declaration.default.clone(),
            )?],
        )
        .await?;
    store
        .set(
            &OBJECT.parse()?,
            &Key::new(CONNECTION.parse()?),
            &connection,
        )
        .await?;
    let scheduler = Scheduler::with_store(
        store.clone(),
        RuntimeOptions {
            data_directory: temp.path().join("modules"),
            ..RuntimeOptions::default()
        },
    )
    .await?;
    scheduler
        .load(
            scheduler
                .package(root.join("packages/qwen-vl-embedding"))
                .await?,
        )
        .await?;
    let started = Instant::now();
    let result: Result<Embeddings, Error> = scheduler
        .call(
            "/user/admin".parse()?,
            OBJECT.parse()?,
            EMBED.parse()?,
            &inputs,
        )
        .await?;
    let elapsed = started.elapsed();
    scheduler.unload(&OBJECT.parse()?, StopMode::Drain).await?;
    drop(scheduler);
    store.close().await?;
    let result = result.map_err(|error| format!("embedding failed: {error:?}"))?;
    let vectors = result.vectors()?;
    assert_eq!(vectors.len(), inputs.len());
    for vector in &vectors {
        assert_eq!(vector.len(), DIMENSIONS);
        assert!((cosine(vector, vector) - 1.0).abs() < 1e-5);
    }
    let positive = cosine(&vectors[0], &vectors[2]);
    let negative = cosine(&vectors[1], &vectors[2]);
    let same_image = cosine(&vectors[2], &vectors[3]);
    println!(
        "Actual Wasm: {} inputs, {} dimensions, {:.3}s",
        vectors.len(),
        DIMENSIONS,
        elapsed.as_secs_f64()
    );
    println!(
        "Image vs matching text: {positive:.6}; unrelated text: {negative:.6}; URL vs base64 image: {same_image:.6}"
    );
    assert!(
        positive > negative,
        "matching text must rank above unrelated text"
    );
    assert!(same_image > 0.99, "the same URL/base64 image must agree");
    Ok(())
}
