use crate::{Error, MANIFEST_FILE, MANIFEST_TEMP, document::*, error::io, files};
use cap_std::fs::Dir;
use forest_protocol::{Manifest, Schema, Value};
use std::{collections::BTreeMap, path::Path};

/// Emits a directory package. This writer owns the `generated/` directory.
pub fn write(root: impl AsRef<Path>, manifest: &Manifest) -> Result<(), Error> {
    manifest.validate()?;
    for path in std::iter::once(&manifest.module).chain(
        [&manifest.package.readme, &manifest.package.license_file]
            .into_iter()
            .flatten(),
    ) {
        files::relative(path)?;
        if path == "generated" || path.starts_with("generated/") {
            return Err(Error::InvalidPath(path.clone()));
        }
    }
    std::fs::create_dir_all(root.as_ref()).map_err(|error| io(root.as_ref(), error))?;
    let directory = Dir::open_ambient_dir(root.as_ref(), cap_std::ambient_authority())
        .map_err(|error| io(root.as_ref(), error))?;
    match directory.remove_dir_all("generated") {
        Ok(()) => {}
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => return Err(io("generated", error)),
    }
    let mut generated = BTreeMap::new();
    let mut schema = |path: String, value: &Schema| {
        generated.insert(path.clone(), value.to_string().into_bytes());
        path
    };
    let lifecycle_error = schema(
        "generated/lifecycle-error.schema".into(),
        &manifest.lifecycle_error,
    );
    let configs = manifest
        .configs
        .iter()
        .enumerate()
        .map(|(i, c)| Config {
            key: c.key.clone(),
            schema: schema(format!("generated/configs/{i}.schema"), &c.schema),
            default: format!("generated/configs/{i}.value"),
            access: c.access.clone().into(),
        })
        .collect::<Vec<_>>();
    let events = manifest
        .events
        .iter()
        .enumerate()
        .map(|(i, event)| Event {
            path: event.path.clone(),
            input: schema(format!("generated/events/{i}.schema"), &event.input),
            access: event.access.clone().into(),
        })
        .collect();
    let provides = manifest
        .provides
        .iter()
        .enumerate()
        .map(|(i, p)| Provides {
            path: p.path.clone(),
            input: schema(format!("generated/provides/{i}.input.schema"), &p.input),
            output: schema(format!("generated/provides/{i}.output.schema"), &p.output),
            access: p.access.clone().into(),
        })
        .collect();
    let uses = manifest
        .uses
        .iter()
        .enumerate()
        .map(|(i, u)| Uses {
            object: u.object.clone(),
            interface: u.interface.clone(),
            input: schema(format!("generated/uses/{i}.input.schema"), &u.input),
            output: schema(format!("generated/uses/{i}.output.schema"), &u.output),
        })
        .collect();
    for (c, declaration) in configs.iter().zip(&manifest.configs) {
        let value: &Value = &declaration.default;
        generated.insert(c.default.clone(), value.to_string().into_bytes());
    }
    for (path, bytes) in generated {
        files::put(&directory, &path, &bytes)?;
    }
    let document = Document {
        package: manifest.package.clone(),
        module: Module {
            file: manifest.module.clone(),
            lifecycle_error,
            activated: manifest.activated.clone(),
        },
        configs,
        events,
        provides,
        uses,
        subscriptions: manifest.subscriptions.clone(),
        sha256: files::checksums(&directory)?,
    };
    // Also checks metadata file references before publishing the descriptor.
    let text = toml::to_string_pretty(&document)?;
    let mut checked = files::Files::new(
        directory
            .try_clone()
            .map_err(|error| io(root.as_ref(), error))?,
        document.sha256.clone(),
    )?;
    document.resolve(&mut checked)?;
    checked.read(&manifest.module)?;
    files::put(&directory, MANIFEST_TEMP, text.as_bytes())?;
    directory
        .rename(MANIFEST_TEMP, &directory, MANIFEST_FILE)
        .map_err(|error| io(MANIFEST_FILE, error))?;
    Ok(())
}
