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
            "forest-agent-tools",
            "--lib",
        ])
        .current_dir(&workspace)
        .env("CARGO_BUILD_JOBS", "2")
        .status()?;
    if !status.success() {
        return Err("Wasm build failed".into());
    }
    let package = workspace.join("packages/agent-tools");
    std::fs::create_dir_all(&package)?;
    std::fs::copy(
        workspace.join("target/wasm32-unknown-unknown/debug/forest_agent_tools.wasm"),
        package.join("module.wasm"),
    )?;
    let manifest = if let Some(path) = std::env::args_os().nth(1) {
        forest_agent_tools::manifest_with_routes(serde_json::from_slice(&std::fs::read(path)?)?)
    } else {
        forest_agent_tools::manifest()
    };
    forest_package::write(&package, &manifest)?;
    println!("{}", package.display());
    Ok(())
}
