//! Local setup utility. It edits declarations through ConfigStore, never prints credentials.
use forest_agent_module::{memory, provider, types::Settings};
use forest_config::{ConfigStore, Declaration, Key};
use forest_protocol::{AsSchema, AsValue, Manifest};
use std::{env, path::PathBuf};
type SetupResult<T> = std::result::Result<T, Box<dyn std::error::Error>>;
fn required(name: &str) -> SetupResult<String> {
    let value = env::var(name).map_err(|_| format!("set {name}"))?;
    if value.trim().is_empty() {
        Err(format!("set {name}").into())
    } else {
        Ok(value)
    }
}
async fn set<T: AsSchema + AsValue>(
    store: &ConfigStore,
    object: &str,
    key: &str,
    value: &T,
) -> SetupResult<()> {
    store
        .set(&object.parse()?, &Key::new(key.parse()?), value)
        .await?;
    Ok(())
}
#[tokio::main(flavor = "current_thread")]
async fn main() -> SetupResult<()> {
    // Parse required settings before touching the database.
    let endpoint = required("FOREST_CHAT_ENDPOINT")?;
    let model = required("FOREST_CHAT_MODEL")?;
    let key = required("FOREST_CHAT_KEY")?;
    let embedding_key = required("FOREST_EMBED_KEY")?;
    let groups = required("FOREST_GROUPS")?
        .split(',')
        .map(|s| s.trim().parse::<i64>())
        .collect::<std::result::Result<Vec<_>, _>>()?;
    let output_tokens = env::var("FOREST_MAX_OUTPUT")
        .unwrap_or_else(|_| "16384".into())
        .parse()?;
    let database = PathBuf::from(
        env::var("FOREST_CONFIG_DB").unwrap_or_else(|_| "data/forest.sqlite3".into()),
    );
    if let Some(parent) = database.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let store = ConfigStore::open(database).await?;
    let entities = store.read(|view| view.entities()).await?;
    for name in [
        "qwen-vl-embedding",
        "openai-provider",
        "context",
        "memory",
        "deepseek-web",
        "wolfram-alpha",
        "agent-tools",
        "agent",
    ] {
        let manifest: Manifest =
            serde_json::from_slice(&std::fs::read(format!("packages/{name}/manifest.json"))?)?;
        if !entities.contains(&manifest.package.id) {
            store
                .register(
                    manifest.package.id,
                    manifest
                        .configs
                        .into_iter()
                        .map(|c| Declaration::new(c.key, c.schema, c.default))
                        .collect::<std::result::Result<Vec<_>, _>>()?,
                )
                .await?;
        }
    }
    set(
        &store,
        provider::OBJECT,
        "/settings",
        &provider::Settings {
            endpoint,
            model,
            api_key: key,
            multimodal: env::var("FOREST_CHAT_VISION").is_ok_and(|v| v == "true"),
            output_limit: provider::OutputLimit::Tokens(output_tokens),
            reasoning_effort: env::var("FOREST_REASONING_EFFORT").ok(),
        },
    )
    .await?;
    #[derive(forest_serde::DeriveSchema, forest_serde::DeriveValue)]
    struct EmbeddingConnection {
        endpoint: String,
        api_key: String,
    }
    set(
        &store,
        "/module/qwen-vl-embedding",
        "/connection",
        &EmbeddingConnection {
            endpoint: env::var("FOREST_EMBED_ENDPOINT")
                .unwrap_or_else(|_| "https://api.siliconflow.cn/v1/embeddings".into()),
            api_key: embedding_key,
        },
    )
    .await?;
    set(
        &store,
        memory::OBJECT,
        "/settings",
        &memory::Settings {
            endpoint: env::var("FOREST_QDRANT_ENDPOINT")
                .unwrap_or_else(|_| "http://127.0.0.1:6333".into()),
            api_key: env::var("FOREST_QDRANT_KEY").unwrap_or_default(),
            ..Default::default()
        },
    )
    .await?;
    set(
        &store,
        "/module/agent",
        "/settings",
        &Settings {
            allowed_groups: groups,
            bot_name: env::var("FOREST_BOT_NAME").ok(),
            ..Default::default()
        },
    )
    .await?;
    if let Ok(prompt) = env::var("FOREST_AGENT_PROMPT") {
        set(&store, "/module/context", "/prompt/agent", &prompt).await?;
    }
    if let Ok(api_key) = env::var("FOREST_DEEPSEEK_KEY") {
        #[derive(forest_serde::DeriveSchema, forest_serde::DeriveValue)]
        struct WebConnection {
            endpoint: String,
            api_key: String,
        }
        set(
            &store,
            "/module/deepseek-web",
            "/connection",
            &WebConnection {
                endpoint: env::var("FOREST_DEEPSEEK_ENDPOINT")
                    .unwrap_or_else(|_| "https://api.deepseek.com/v1".into()),
                api_key,
            },
        )
        .await?;
    }
    if let Ok(app_id) = env::var("FOREST_WOLFRAM_APP_ID") {
        #[derive(forest_serde::DeriveSchema, forest_serde::DeriveValue)]
        struct WolframConnection {
            endpoint: String,
            app_id: String,
        }
        set(
            &store,
            "/module/wolfram-alpha",
            "/connection",
            &WolframConnection {
                endpoint: env::var("FOREST_WOLFRAM_ENDPOINT")
                    .unwrap_or_else(|_| "https://www.wolframalpha.com/api/v1/llm-api".into()),
                app_id,
            },
        )
        .await?;
    }
    store.close().await?;
    println!("Agent configuration saved.");
    Ok(())
}
