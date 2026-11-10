use forest_package::{Error, MANIFEST_FILE};
use forest_protocol::{
    AsSchema, AsValue, ConfigDeclaration, InterfaceDeclaration, Manifest, PackageInfo,
    SubjectPattern, manifest::callers,
};
use sha2::{Digest, Sha256};
use std::path::Path;

fn manifest() -> Manifest {
    Manifest {
        package: PackageInfo {
            id: "/module/example".parse().unwrap(),
            version: "dev-test".into(),
            name: Some("Example".into()),
            authors: vec!["Example Author".into()],
            repository: Some("https://example.org/source".into()),
            license: Some("MIT".into()),
            license_file: Some("LICENSE".into()),
            readme: Some("README.md".into()),
            homepage: Some("https://example.org/".into()),
            documentation: Some("https://example.org/docs".into()),
            keywords: vec!["example".into()],
            ..Default::default()
        },
        module: "module.wasm".into(),
        lifecycle_error: <std::convert::Infallible>::as_schema(),
        configs: vec![ConfigDeclaration {
            key: "/settings".parse().unwrap(),
            schema: i64::as_schema(),
            default: 17i64.as_value(),
            access: forest_protocol::AccessPolicy {
                readers: vec![SubjectPattern::SelfModule],
                writers: vec![SubjectPattern::Path("/user/admin".parse().unwrap())],
                callers: vec![],
            },
        }],
        // Root and punctuation are legal interface paths, not filesystem names.
        provides: ["/", "/render:document"]
            .into_iter()
            .map(|path| InterfaceDeclaration {
                path: path.parse().unwrap(),
                input: String::as_schema(),
                output: <Result<String, bool>>::as_schema(),
                reentrant: true,
                access: callers(vec![SubjectPattern::Path("/module/**".parse().unwrap())]),
            })
            .collect(),
        uses: vec![],
        subscriptions: vec![],
    }
}

fn prepare(path: &Path) {
    std::fs::write(path.join("module.wasm"), b"\0asm\x01\0\0\0").unwrap();
    std::fs::write(path.join("README.md"), "# Example\n").unwrap();
    std::fs::write(path.join("LICENSE"), "Example license text\n").unwrap();
    std::fs::create_dir(path.join("assets")).unwrap();
    std::fs::write(path.join("assets/data.txt"), "payload").unwrap();
    forest_package::write(path, &manifest()).unwrap();
}

fn edit(path: &Path, change: impl FnOnce(&mut toml::Table)) {
    let mut document = std::fs::read_to_string(path.join(MANIFEST_FILE))
        .unwrap()
        .parse::<toml::Table>()
        .unwrap();
    change(&mut document);
    std::fs::write(
        path.join(MANIFEST_FILE),
        toml::to_string_pretty(&document).unwrap(),
    )
    .unwrap();
}

#[test]
fn toml_and_forest_text_roundtrip_metadata_types_acl_and_payloads_deterministically() {
    let temp = tempfile::tempdir().unwrap();
    prepare(temp.path());
    let first = forest_package::describe(temp.path()).unwrap();
    assert!(first.source.contains("lifecycle-error ="));
    assert!(first.source.contains("readers = [\"self\"]"));
    assert!(!temp.path().join("manifest.json").exists());
    let schema = std::fs::read_to_string(temp.path().join("generated/configs/0.schema")).unwrap();
    assert!(schema.contains("schema ") && schema.contains("export "));
    assert_eq!(
        std::fs::read_to_string(temp.path().join("generated/configs/0.value")).unwrap(),
        "17i64"
    );
    let expected = manifest();
    assert_eq!(first.manifest.package.authors, expected.package.authors);
    assert_eq!(
        first.manifest.package.repository,
        expected.package.repository
    );
    assert_eq!(first.manifest.package.license, expected.package.license);
    assert_eq!(first.manifest.package.readme, expected.package.readme);
    assert_eq!(
        first.manifest.configs[0].default,
        expected.configs[0].default
    );
    assert_eq!(first.manifest.configs[0].schema, expected.configs[0].schema);
    assert_eq!(first.manifest.configs[0].access, expected.configs[0].access);
    assert_eq!(first.manifest.provides[1].path, expected.provides[1].path);
    assert_eq!(
        first.manifest.provides[1].output,
        expected.provides[1].output
    );
    let loaded = forest_package::read(temp.path()).unwrap();
    assert_eq!(loaded.module, b"\0asm\x01\0\0\0");
    assert_eq!(
        loaded.sha256,
        format!("{:x}", Sha256::digest(&loaded.module))
    );
    forest_package::write(temp.path(), &expected).unwrap();
    assert_eq!(
        forest_package::describe(temp.path()).unwrap().source,
        first.source
    );
}

#[test]
fn declarations_are_verified_before_parsing_and_artifacts_before_loading() {
    for path in [
        "generated/configs/0.schema",
        "generated/configs/0.value",
        "module.wasm",
        "README.md",
        "assets/data.txt",
    ] {
        let temp = tempfile::tempdir().unwrap();
        prepare(temp.path());
        std::fs::write(temp.path().join(path), "tampered").unwrap();
        assert!(
            matches!(forest_package::read(temp.path()), Err(Error::ChecksumMismatch { path: failed, .. }) if failed == path)
        );
        if path == "module.wasm" || path.starts_with("assets/") {
            assert!(
                forest_package::describe(temp.path()).is_ok(),
                "metadata listing does not load Wasm or unused resources"
            );
        } else {
            assert!(matches!(
                forest_package::describe(temp.path()),
                Err(Error::ChecksumMismatch { .. })
            ));
        }
    }
}

#[test]
fn invalid_paths_missing_hashes_and_unknown_manifest_fields_are_rejected() {
    for path in [
        "../outside.schema",
        "/absolute.schema",
        "C:/outside.schema",
        "a\\outside.schema",
    ] {
        let temp = tempfile::tempdir().unwrap();
        prepare(temp.path());
        edit(temp.path(), |document| {
            document["configs"][0]["schema"] = path.into()
        });
        assert!(matches!(
            forest_package::describe(temp.path()),
            Err(Error::InvalidPath(_))
        ));
    }
    let temp = tempfile::tempdir().unwrap();
    prepare(temp.path());
    edit(temp.path(), |document| {
        document["sha256"]
            .as_table_mut()
            .unwrap()
            .remove("module.wasm");
    });
    assert!(matches!(
        forest_package::describe(temp.path()),
        Err(Error::MissingChecksum(_))
    ));
    forest_package::write(temp.path(), &manifest()).unwrap();
    edit(temp.path(), |document| {
        document.insert("unknown-field".into(), true.into());
    });
    assert!(matches!(
        forest_package::describe(temp.path()),
        Err(Error::Toml(_))
    ));
}

#[test]
fn valid_hashes_do_not_replace_schema_value_or_acl_validation() {
    for (path, bytes, kind) in [
        ("generated/configs/0.schema", "not a Schema", "schema"),
        ("generated/configs/0.value", "not a Value", "value"),
        ("generated/configs/0.value", "true", "default"),
    ] {
        let temp = tempfile::tempdir().unwrap();
        prepare(temp.path());
        std::fs::write(temp.path().join(path), bytes).unwrap();
        edit(temp.path(), |document| {
            document["sha256"][path] = format!("{:x}", Sha256::digest(bytes.as_bytes())).into()
        });
        let error = forest_package::describe(temp.path()).err().unwrap();
        assert!(match kind {
            "schema" => matches!(error, Error::Schema { .. }),
            "value" => matches!(error, Error::Value { .. }),
            _ => matches!(error, Error::Manifest(_)),
        });
    }
    let temp = tempfile::tempdir().unwrap();
    prepare(temp.path());
    edit(temp.path(), |document| {
        document["configs"][0]["access"]["readers"] = toml::Value::Array(vec!["SelfModule".into()])
    });
    assert!(matches!(
        forest_package::describe(temp.path()),
        Err(Error::Subject { .. })
    ));
}

#[cfg(unix)]
#[test]
fn referenced_symlinks_cannot_escape_the_package_directory() {
    let temp = tempfile::tempdir().unwrap();
    prepare(temp.path());
    let outside = tempfile::NamedTempFile::new().unwrap();
    std::fs::write(outside.path(), "schema T = i64; export T;").unwrap();
    std::os::unix::fs::symlink(outside.path(), temp.path().join("outside.schema")).unwrap();
    edit(temp.path(), |document| {
        document["configs"][0]["schema"] = "outside.schema".into();
        document["sha256"]["outside.schema"] = format!(
            "{:x}",
            Sha256::digest(std::fs::read(outside.path()).unwrap())
        )
        .into();
    });
    assert!(matches!(
        forest_package::describe(temp.path()),
        Err(Error::Io { .. })
    ));
}
