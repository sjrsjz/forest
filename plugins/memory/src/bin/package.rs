use std::{path::PathBuf, process::Command};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let workspace = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .canonicalize()?;
    let status = Command::new("cargo")
        .args([
            "build",
            "--locked",
            "--offline",
            "--target",
            "wasm32-unknown-unknown",
            "-p",
            "forest-memory-module",
            "--lib",
        ])
        .current_dir(&workspace)
        .env("CARGO_BUILD_JOBS", "2")
        .status()?;
    if !status.success() {
        return Err("Wasm build failed".into());
    }
    let package = workspace.join("packages/memory");
    std::fs::create_dir_all(&package)?;
    std::fs::copy(
        workspace.join("target/wasm32-unknown-unknown/debug/forest_memory_module.wasm"),
        package.join("module.wasm"),
    )?;
    let manifest = forest_memory_module::manifest();
    std::fs::write(
        package.join("manifest.json"),
        serde_json::to_vec_pretty(&manifest)?,
    )?;
    for interface in &manifest.provides {
        let name = interface.path.to_string();
        for (direction, schema) in [("input", &interface.input), ("output", &interface.output)] {
            let path = package.join(format!(
                "{}.{direction}.schema",
                name.trim_start_matches('/')
            ));
            if let Some(parent) = path.parent() {
                std::fs::create_dir_all(parent)?;
            }
            std::fs::write(path, schema.to_string())?;
        }
    }
    println!("{}", package.display());
    Ok(())
}
