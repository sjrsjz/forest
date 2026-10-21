//! Isolated two-call native tool round trip; no OneBot connection or actual tools.
use forest_config::{ConfigStore, Declaration, Key};
use forest_openai_provider::manifest;
use forest_openai_provider::types::*;
use forest_runtime::{RuntimeOptions, Scheduler, StopMode};
use std::{path::PathBuf, time::Instant};

async fn complete(
    scheduler: &Scheduler,
    request: &Request,
) -> Result<Completion, Box<dyn std::error::Error>> {
    let result: Result<Completion, Error> = scheduler
        .call(
            "/user/admin".parse()?,
            OBJECT.parse()?,
            COMPLETE.parse()?,
            request,
        )
        .await?;
    result.map_err(|error| format!("provider failed: {error:?}").into())
}

#[tokio::main(worker_threads = 2)]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let path = std::env::args()
        .nth(1)
        .ok_or("usage: verify-openai-provider <settings.json>")?;
    let settings: Settings = serde_json::from_slice(&tokio::fs::read(path).await?)?;
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
        .set(&OBJECT.parse()?, &Key::new(SETTINGS.parse()?), &settings)
        .await?;
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .canonicalize()?;
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
                .package(root.join("packages/openai-provider"))
                .await?,
        )
        .await?;
    let mut request = Request {
        messages: vec![
            Message::System {
                content: "Use the requested function. After its result, reply with exactly that result and nothing else.".into(),
            },
            Message::User {
                content: Content::Text("Call forest_echo with value FOREST_OK, then report its result.".into()),
            },
        ],
        tools: vec![Tool {
            name: "forest_echo".into(),
            description: Some("Echo a diagnostic value without side effects.".into()),
            parameters_json: r#"{"type":"object","properties":{"value":{"type":"string"}},"required":["value"],"additionalProperties":false}"#.into(),
            strict: None,
        }],
        tool_choice: Some(ToolChoice::Auto),
    };
    let started = Instant::now();
    let first = complete(&scheduler, &request).await?;
    let first_time = started.elapsed();
    let first = first.choices.into_iter().next().ok_or("missing choice")?;
    assert_eq!(first.finish_reason, FinishReason::ToolCalls);
    let calls = first
        .message
        .tool_calls
        .clone()
        .ok_or("missing tool calls")?;
    let reasoning_retained = first.message.reasoning_content.is_some();
    request.messages.push(Message::Assistant(first.message));
    for call in calls {
        let ToolCall::Function { id, function } = call;
        assert_eq!(function.name, "forest_echo");
        #[derive(serde::Deserialize)]
        struct Arguments {
            value: String,
        }
        let arguments: Arguments = serde_json::from_str(&function.arguments)?;
        assert_eq!(arguments.value, "FOREST_OK");
        request.messages.push(Message::Tool {
            tool_call_id: id,
            content: arguments.value,
        });
    }
    let started = Instant::now();
    let second = complete(&scheduler, &request).await?;
    let second_time = started.elapsed();
    let second = second
        .choices
        .into_iter()
        .next()
        .ok_or("missing final choice")?;
    assert_eq!(second.finish_reason, FinishReason::Stop);
    assert_eq!(
        second.message.content.as_deref().map(str::trim),
        Some("FOREST_OK")
    );
    scheduler.unload(&OBJECT.parse()?, StopMode::Drain).await?;
    drop(scheduler);
    store.close().await?;
    println!(
        "Actual Wasm native tool round trip passed; first {:.3}s, continuation {:.3}s; reasoning retained: {reasoning_retained}",
        first_time.as_secs_f64(),
        second_time.as_secs_f64()
    );
    Ok(())
}
