use std::{path::PathBuf, process::Command};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let workspace = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .canonicalize()?;
    if !Command::new("cargo")
        .args([
            "build",
            "--locked",
            "--offline",
            "--target",
            "wasm32-unknown-unknown",
            "-p",
            "forest-qwen-vl-reranker",
            "--lib",
        ])
        .current_dir(&workspace)
        .env("CARGO_BUILD_JOBS", "2")
        .status()?
        .success()
    {
        return Err("Wasm build failed".into());
    }
    let directory = workspace.join("packages/qwen-vl-reranker");
    std::fs::create_dir_all(&directory)?;
    std::fs::copy(
        workspace.join("target/wasm32-unknown-unknown/debug/forest_qwen_vl_reranker.wasm"),
        directory.join("module.wasm"),
    )?;
    let manifest = forest_qwen_vl_reranker::manifest();
    forest_package::write(&directory, &manifest)?;
    println!("{}", directory.display());
    Ok(())
}
