use std::{path::PathBuf, process::Command};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let project = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let repository = project.join("../../../..").canonicalize()?;
    let target = repository.join("target");
    let status = Command::new("cargo")
        .args([
            "build",
            "--locked",
            "-j",
            "2",
            "--target",
            "wasm32-unknown-unknown",
            "--lib",
            "--manifest-path",
        ])
        .arg(project.join("Cargo.toml"))
        .arg("--target-dir")
        .arg("target")
        .current_dir(&repository)
        .status()?;
    if !status.success() {
        return Err(std::io::Error::other(format!("Wasm build failed: {status}")).into());
    }
    let output = std::env::args_os()
        .nth(1)
        .map(PathBuf::from)
        .unwrap_or_else(|| repository.join("packages/example-hello"));
    std::fs::create_dir_all(&output)?;
    std::fs::copy(
        target.join("wasm32-unknown-unknown/debug/forest_module_example_hello.wasm"),
        output.join("module.wasm"),
    )?;
    forest_package::write(&output, &forest_module_example_hello::manifest())?;
    println!("{}", output.display());
    Ok(())
}
