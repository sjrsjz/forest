//! Native package documents and payload verification. No Wasm execution or scheduling.
mod document;
mod error;
mod files;
mod write;

use cap_std::fs::Dir;
pub use error::Error;
use forest_protocol::Manifest;
use std::path::Path;
pub use write::write;

pub const MANIFEST_FILE: &str = "manifest.toml";
const MANIFEST_TEMP: &str = ".manifest.toml.tmp";

pub struct Description {
    pub manifest: Manifest,
    pub source: String,
    files: files::Files,
}

pub struct PackageData {
    pub manifest: Manifest,
    pub module: Vec<u8>,
    pub sha256: String,
}

/// Resolves and verifies declarations without reading or compiling the Wasm artifact.
pub fn describe(root: impl AsRef<Path>) -> Result<Description, Error> {
    let directory = Dir::open_ambient_dir(root.as_ref(), cap_std::ambient_authority())
        .map_err(|error| error::io(root.as_ref(), error))?;
    let source = directory
        .read_to_string(MANIFEST_FILE)
        .map_err(|error| error::io(MANIFEST_FILE, error))?;
    let mut document: document::Document = toml::from_str(&source)?;
    let mut files = files::Files::new(directory, std::mem::take(&mut document.sha256))?;
    let manifest = document.resolve(&mut files)?;
    Ok(Description {
        manifest,
        source,
        files,
    })
}

pub fn read_manifest(root: impl AsRef<Path>) -> Result<Manifest, Error> {
    Ok(describe(root)?.manifest)
}

/// Verifies every indexed payload and returns the exact bytes to compile.
pub fn read(root: impl AsRef<Path>) -> Result<PackageData, Error> {
    let Description {
        manifest, files, ..
    } = describe(root)?;
    let sha256 = files.checksums[&manifest.module].to_ascii_lowercase();
    let module = files.module(&manifest.module)?;
    Ok(PackageData {
        manifest,
        module,
        sha256,
    })
}
