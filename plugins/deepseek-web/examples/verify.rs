//! Two real API calls through Wasm, without OneBot or a model conversation.
use forest_config::{ConfigStore, Declaration, Key};
use forest_deepseek_web::{
    CONNECTION, Connection, OBJECT,
    contract::{Error, Invocation},
    manifest,
};
use forest_runtime::{RuntimeOptions, Scheduler, StopMode};
use std::{path::PathBuf, time::Instant};

type VerifyResult<T> = Result<T, Box<dyn std::error::Error>>;

#[tokio::main(worker_threads = 2)]
async fn main() -> VerifyResult<()> {
    let path = std::env::args()
        .nth(1)
        .ok_or("usage: verify <config.sqlite3>")?;
    let source = ConfigStore::open(path).await?;
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
    let declaration = &manifest().configs[0];
    store
        .register(
            OBJECT.parse()?,
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
                .package(root.join("packages/deepseek-web"))
                .await?,
        )
        .await?;
    let run = async {
        for (interface, name, args) in [
            ("/search", "web_search", serde_json::json!({"query":"DeepSeek Search Fetch API","count":3})),
            ("/fetch", "web_fetch", serde_json::json!({"url":"https://api-docs.deepseek.com/zh-cn/guides/search/"})),
        ] {
            let start = Instant::now();
            let result: Result<String, Error> = scheduler.call(
                "/user/admin".parse()?, OBJECT.parse()?, interface.parse()?,
                &Invocation { group_id:0, bot_id:0, name:name.into(), arguments_json:args.to_string() },
            ).await?;
            let body = result.map_err(|e| format!("{name}: {e:?}"))?;
            let result: serde_json::Value = serde_json::from_str(&body)?;
            println!("{}", serde_json::json!({
                "tool":name, "elapsed_ms":start.elapsed().as_millis(),
                "results":result.get("results").and_then(|v|v.as_array()).map(Vec::len),
                "content_chars":result.get("content").and_then(|v|v.as_str()).map(|s|s.chars().count()),
                "response_chars":body.chars().count(),
            }));
        }
        Ok::<_, Box<dyn std::error::Error>>(())
    }.await;
    scheduler.shutdown(StopMode::Drain).await?;
    run
}
