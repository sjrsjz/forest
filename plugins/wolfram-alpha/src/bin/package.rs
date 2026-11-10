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
            "forest-wolfram-alpha",
            "--lib",
        ])
        .current_dir(&workspace)
        .env("CARGO_BUILD_JOBS", "2")
        .status()?;
    if !status.success() {
        return Err("Wasm build failed".into());
    }
    let package = workspace.join("packages/wolfram-alpha");
    std::fs::create_dir_all(&package)?;
    std::fs::copy(
        workspace.join("target/wasm32-unknown-unknown/debug/forest_wolfram_alpha.wasm"),
        package.join("module.wasm"),
    )?;
    let manifest = forest_wolfram_alpha::manifest();
    forest_package::write(&package, &manifest)?;
    println!("{}", package.display());
    Ok(())
}
