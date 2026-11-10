use std::{path::PathBuf, process::Command};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let workspace = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .canonicalize()?;
    let status = Command::new("cargo")
        .args([
            "build",
            "--profile",
            "guest",
            "--locked",
            "--offline",
            "--target",
            "wasm32-unknown-unknown",
            "-p",
            "forest-image-projection",
            "--lib",
        ])
        .current_dir(&workspace)
        .env("CARGO_BUILD_JOBS", "2")
        .status()?;
    if !status.success() {
        return Err("Wasm build failed".into());
    }
    let package = workspace.join("packages/image-projection");
    std::fs::create_dir_all(&package)?;
    std::fs::copy(
        workspace.join("target/wasm32-unknown-unknown/guest/forest_image_projection.wasm"),
        package.join("module.wasm"),
    )?;
    let manifest = forest_image_projection::manifest();
    forest_package::write(&package, &manifest)?;
    println!("{}", package.display());
    Ok(())
}
