//! One real computation through Wasm, without OneBot or a model conversation.
use forest_config::{ConfigStore, Declaration, Key};
use forest_runtime::{RuntimeOptions, Scheduler, StopMode};
use forest_wolfram_alpha::{
    CONNECTION, Connection, OBJECT,
    contract::{Error, Invocation},
    manifest,
};
use std::{path::PathBuf, time::Instant};

#[tokio::main(worker_threads = 2)]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let database = std::env::args()
        .nth(1)
        .ok_or("usage: verify <config.sqlite3>")?;
    let source = ConfigStore::open(database).await?;
    let connection: Connection = source
        .read(|view| {
            view.get(
                &OBJECT.parse().unwrap(),
                &Key::new(CONNECTION.parse().unwrap()),
            )
        })
        .await?;
    source.close().await?;
    let temp = tempfile::tempdir()?;
    let store = ConfigStore::open(temp.path().join("config.sqlite3")).await?;
    let declaration = manifest().configs.remove(0);
    store
        .register(
            OBJECT.parse()?,
            [Declaration::new(
                declaration.key,
                declaration.schema,
                declaration.default,
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
        store,
        RuntimeOptions {
            data_directory: temp.path().join("modules"),
            ..Default::default()
        },
    )
    .await?;
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..");
    scheduler
        .load(
            scheduler
                .package(root.join("packages/wolfram-alpha"))
                .await?,
        )
        .await?;
    let start = Instant::now();
    let result: Result<String, Error> = scheduler.call(
        "/user/admin".parse()?, OBJECT.parse()?, "/query".parse()?,
        &Invocation {
            group_id: 0, bot_id: 0, name: "wolfram_alpha".into(),
            arguments_json: serde_json::json!({"query": "integrate x^2 sin(x) from 0 to pi", "max_chars": 1000}).to_string(),
        },
    ).await?;
    scheduler.shutdown(StopMode::Drain).await?;
    let result = result.map_err(|e| format!("{e:?}"))?;
    let answer: serde_json::Value = serde_json::from_str(&result)?;
    if answer["status"] != "complete"
        || !answer["result"].as_str().is_some_and(|s| s.contains("pi"))
    {
        return Err("Wolfram Alpha did not return the expected computation".into());
    }
    println!(
        "{}",
        serde_json::json!({
            "tool": "wolfram_alpha", "elapsed_ms": start.elapsed().as_millis(),
            "answer": answer,
        })
    );
    Ok(())
}
