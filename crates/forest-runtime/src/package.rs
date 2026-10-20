use anyhow::{Context, Result, ensure};
use forest_protocol::{Manifest, equivalent};
use forest_util::acl::{AclEntry, AclTable};
use std::{path::Path, sync::Arc};

#[derive(Clone)]
pub struct Package {
    pub(crate) manifest: Arc<Manifest>,
    pub(crate) module: wasmtime::Module,
    pub(crate) access: Arc<AccessTables>,
}
pub(crate) struct AccessTables {
    pub config: AclTable,
    pub calls: AclTable,
}
impl AccessTables {
    pub fn compile(manifest: &Manifest) -> Result<Self> {
        let owner = &manifest.package.id;
        let config = AclTable::new(manifest.configs.iter().map(|c| AclEntry {
            entity: owner.clone(),
            key: c.key.clone(),
            owner: owner.clone(),
            access: c.access.clone(),
        }))?;
        let calls = AclTable::new(manifest.provides.iter().map(|c| AclEntry {
            entity: owner.clone(),
            key: c.path.clone(),
            owner: owner.clone(),
            access: c.access.clone(),
        }))?;
        Ok(Self { config, calls })
    }
}

impl Package {
    pub fn manifest(&self) -> &Manifest {
        &self.manifest
    }
    pub(crate) async fn read(engine: &wasmtime::Engine, root: &Path) -> Result<Self> {
        let root = tokio::fs::canonicalize(root)
            .await
            .context("package directory")?;
        let manifest: Manifest =
            serde_json::from_slice(&tokio::fs::read(root.join("manifest.json")).await?)?;
        manifest.validate()?;
        let artifact = tokio::fs::canonicalize(root.join(&manifest.module)).await?;
        ensure!(
            artifact.starts_with(&root),
            "Wasm artifact escapes package directory"
        );
        let bytes = tokio::fs::read(&artifact).await?;
        let engine = engine.clone();
        let module =
            tokio::task::spawn_blocking(move || wasmtime::Module::new(&engine, bytes)).await??;
        for (name, signature) in [
            (
                "forest_alloc",
                (vec![wasmtime::ValType::I32], vec![wasmtime::ValType::I32]),
            ),
            (
                "forest_free",
                (vec![wasmtime::ValType::I32, wasmtime::ValType::I32], vec![]),
            ),
            (
                "forest_create",
                (
                    vec![
                        wasmtime::ValType::I64,
                        wasmtime::ValType::I32,
                        wasmtime::ValType::I32,
                    ],
                    vec![],
                ),
            ),
            (
                "forest_invoke",
                (
                    vec![
                        wasmtime::ValType::I64,
                        wasmtime::ValType::I32,
                        wasmtime::ValType::I32,
                    ],
                    vec![],
                ),
            ),
            (
                "forest_poll",
                (vec![wasmtime::ValType::I64], vec![wasmtime::ValType::I64]),
            ),
            ("forest_destroy", (vec![wasmtime::ValType::I64], vec![])),
        ] {
            let Some(wasmtime::ExternType::Func(function)) = module.get_export(name) else {
                anyhow::bail!("missing ABI function {name}");
            };
            let actual = (
                function.params().collect::<Vec<_>>(),
                function.results().collect::<Vec<_>>(),
            );
            ensure!(
                actual
                    .0
                    .iter()
                    .zip(&signature.0)
                    .all(|(a, b)| wasmtime::ValType::eq(a, b))
                    && actual
                        .1
                        .iter()
                        .zip(&signature.1)
                        .all(|(a, b)| wasmtime::ValType::eq(a, b))
                    && actual.0.len() == signature.0.len()
                    && actual.1.len() == signature.1.len(),
                "invalid ABI signature {name}"
            );
        }
        let Some(wasmtime::ExternType::Memory(memory)) = module.get_export("memory") else {
            anyhow::bail!("missing memory export");
        };
        ensure!(
            !memory.is_64() && !memory.is_shared(),
            "ABI requires private wasm32 memory"
        );
        let access = Arc::new(AccessTables::compile(&manifest)?);
        Ok(Self {
            manifest: Arc::new(manifest),
            module,
            access,
        })
    }

    pub(crate) fn check_replacement(&self, current: &Manifest) -> Result<()> {
        ensure!(
            self.manifest.package.id == current.package.id,
            "replacement changes module identity"
        );
        for old in &current.provides {
            let new = self
                .manifest
                .provides
                .iter()
                .find(|new| new.path == old.path)
                .context("replacement removes interface")?;
            ensure!(
                equivalent(&old.input, &new.input) && equivalent(&old.output, &new.output),
                "replacement changes interface contract"
            );
        }
        ensure!(
            self.manifest.configs.len() == current.configs.len(),
            "configuration declaration changes require an explicit design decision"
        );
        for old in &current.configs {
            let new = self
                .manifest
                .configs
                .iter()
                .find(|new| new.key == old.key)
                .context("replacement removes configuration")?;
            ensure!(
                equivalent(&old.schema, &new.schema) && old.default == new.default,
                "configuration schema/default changes are not enabled"
            );
        }
        Ok(())
    }
}
