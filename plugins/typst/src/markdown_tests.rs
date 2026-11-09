//! Exercise the Markdown Wasm plan against the actual typesetter without a QQ account.
use super::*;
use forest_markdown::contract as md;
use forest_onebot::message::Segment;
use forest_runtime::{NativeInterface, RuntimeOptions, Scheduler, StopMode};
use std::{
    path::PathBuf,
    process::Command,
    sync::{Arc, Mutex},
};

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn markdown_wasm_renders_real_math_tables_code_and_styling() {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..");
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
                "forest-markdown",
                "--lib"
            ])
            .current_dir(&root)
            .status()
            .unwrap()
            .success()
    );
    let temp = tempfile::tempdir().unwrap();
    let scheduler = Scheduler::new(
        temp.path().join("config.sqlite3"),
        RuntimeOptions {
            data_directory: temp.path().join("data"),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    let fonts = Arc::new(world::Fonts::new());
    let failures = Arc::new(Mutex::new(Vec::<String>::new()));
    let captures = failures.clone();
    scheduler
        .register_native(NativeInterface::new(
            OBJECT.parse().unwrap(),
            "/typeset".parse().unwrap(),
            false,
            callers(vec![SubjectPattern::Path(md::OBJECT.parse().unwrap())]),
            move |_, source: String| {
                let fonts = fonts.clone();
                let failures = captures.clone();
                async move {
                    match world::render(
                        &fonts,
                        &Settings::default(),
                        source.clone(),
                        BTreeMap::new(),
                    ) {
                        Ok((pages, _)) => {
                            assert!(
                                pages
                                    .iter()
                                    .all(|p| p.bytes.starts_with(b"\x89PNG\r\n\x1a\n"))
                            );
                            Ok::<Vec<String>, forest_markdown::typst::Error>(
                                pages
                                    .iter()
                                    .map(|_| format!("media:{}", "a".repeat(64)))
                                    .collect(),
                            )
                        }
                        Err(error) => {
                            failures.lock().unwrap().push(format!("{error:?}"));
                            Err(forest_markdown::typst::Error::Execution(format!(
                                "{error:?}"
                            )))
                        }
                    }
                }
            },
        ))
        .unwrap();
    let dir = temp.path().join("markdown");
    std::fs::create_dir(&dir).unwrap();
    std::fs::copy(
        root.join("target/wasm32-unknown-unknown/guest/forest_markdown.wasm"),
        dir.join("module.wasm"),
    )
    .unwrap();
    std::fs::write(
        dir.join("manifest.json"),
        serde_json::to_vec(&forest_markdown::manifest()).unwrap(),
    )
    .unwrap();
    scheduler
        .load(scheduler.package(&dir).await.unwrap())
        .await
        .unwrap();
    for (source, expected) in [
        (
            "$$\\frac{a}{b}+\\sqrt{x}$$\n\n|column|value|\n|:-|--:|\n|**row**|$x^2$|\n\n```rust\nlet message = \"#panic(1)\";\n```",
            3,
        ),
        (
            "# Heading\n\n**bold** *emphasis* ~~old~~ `code`\n\n> quotation\n\n- first\n- second\n\n3. third\n4. fourth",
            0,
        ),
    ] {
        let result: Result<Vec<md::Part>, md::Error> = scheduler
            .call(
                "/user/admin".parse().unwrap(),
                md::OBJECT.parse().unwrap(),
                "/format".parse().unwrap(),
                &md::Format {
                    group_id: Some(10),
                    source: source.into(),
                },
            )
            .await
            .unwrap();
        let parts = result.unwrap();
        assert!(
            failures.lock().unwrap().is_empty(),
            "{:?}",
            *failures.lock().unwrap()
        );
        assert_eq!(
            parts
                .iter()
                .flat_map(|p| &p.segments)
                .filter(|p| matches!(p, Segment::Image(_)))
                .count(),
            expected,
            "{parts:?}"
        );
        if expected == 0 {
            let text = parts
                .iter()
                .flat_map(|p| &p.segments)
                .filter_map(|p| match p {
                    Segment::Text { text } => Some(text.as_str()),
                    _ => None,
                })
                .collect::<String>();
            assert_eq!(
                text, source,
                "headings, lists, quotations and inline formatting must remain text"
            );
        }
    }
    scheduler.shutdown(StopMode::Drain).await.unwrap();
}
