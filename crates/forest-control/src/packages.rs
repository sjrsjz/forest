//! Shared host package catalog for administrative frontends.
use anyhow::{Context, Result, ensure};
use forest_protocol::{Manifest, Token};
use forest_runtime::{ModuleManager, StopMode};
use serde::{Deserialize, Serialize};
use std::{
    collections::HashMap,
    path::{Component, Path, PathBuf},
};

#[derive(Clone)]
pub struct Packages {
    root: PathBuf,
    manager: ModuleManager,
}

#[derive(Serialize)]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum LocalPackage {
    Ready {
        directory: String,
        manifest: Manifest,
    },
    Invalid {
        directory: String,
        message: String,
    },
}

#[derive(Serialize)]
pub struct Plan {
    object: Token,
    order: Vec<PlannedPackage>,
}
#[derive(Serialize)]
pub struct PlannedPackage {
    directory: String,
    manifest: Manifest,
}

impl Packages {
    pub async fn new(root: &Path, manager: ModuleManager) -> Result<Self> {
        Ok(Self {
            root: tokio::fs::canonicalize(root)
                .await
                .context("local package directory")?,
            manager,
        })
    }
    async fn directory(&self, name: &str) -> Result<PathBuf> {
        let mut components = Path::new(name).components();
        ensure!(
            matches!(components.next(), Some(Component::Normal(_))) && components.next().is_none(),
            "invalid package directory name"
        );
        let path = tokio::fs::canonicalize(self.root.join(name)).await?;
        ensure!(
            path.parent() == Some(self.root.as_path()),
            "package directory escapes configured root"
        );
        Ok(path)
    }
    async fn manifest(&self, name: &str) -> Result<Manifest> {
        let path = self.directory(name).await?;
        let manifest: Manifest =
            serde_json::from_slice(&tokio::fs::read(path.join("manifest.json")).await?)?;
        manifest.validate()?;
        Ok(manifest)
    }
    pub async fn list(&self) -> Result<Vec<LocalPackage>> {
        let mut directories = tokio::fs::read_dir(&self.root).await?;
        let mut result = Vec::new();
        while let Some(entry) = directories.next_entry().await? {
            if !entry.file_type().await?.is_dir() {
                continue;
            }
            let directory = entry
                .file_name()
                .into_string()
                .map_err(|_| anyhow::anyhow!("package directory name is not UTF-8"))?;
            result.push(match self.manifest(&directory).await {
                Ok(manifest) => LocalPackage::Ready {
                    directory,
                    manifest,
                },
                Err(error) => LocalPackage::Invalid {
                    directory,
                    message: format!("{error:#}"),
                },
            });
        }
        result.sort_by(|a, b| {
            let name = |p: &LocalPackage| match p {
                LocalPackage::Ready { directory, .. } | LocalPackage::Invalid { directory, .. } => {
                    directory.clone()
                }
            };
            name(a).cmp(&name(b))
        });
        Ok(result)
    }
    pub async fn plan(&self, name: &str) -> Result<Plan> {
        let root = self.manifest(name).await?.package.id;
        let mut manifests = Vec::new();
        let mut directories = HashMap::new();
        for package in self.list().await? {
            if let LocalPackage::Ready {
                directory,
                manifest,
            } = package
            {
                directories.insert(manifest.package.id.clone(), directory);
                manifests.push(manifest);
            }
        }
        let order = self.manager.plan(&manifests, std::slice::from_ref(&root))?;
        let mut declarations: HashMap<_, _> = manifests
            .into_iter()
            .map(|m| (m.package.id.clone(), m))
            .collect();
        Ok(Plan {
            object: root,
            order: order
                .into_iter()
                .map(|id| PlannedPackage {
                    directory: directories.remove(&id).expect("planned package directory"),
                    manifest: declarations.remove(&id).expect("planned manifest"),
                })
                .collect(),
        })
    }
    pub async fn load(&self, name: &str) -> Result<()> {
        let plan = self.plan(name).await?;
        let mut available = Vec::new();
        for entry in plan.order {
            let package = self
                .manager
                .package(self.directory(&entry.directory).await?)
                .await?;
            ensure!(
                package.manifest().package.id == entry.manifest.package.id,
                "package identity changed while reading {name}"
            );
            available.push(package);
        }
        self.manager.load(available, &[plan.object]).await
    }
    pub async fn reload(&self, request: ReloadRequest) -> Result<()> {
        let package = self
            .manager
            .package(self.directory(&request.directory).await?)
            .await?;
        ensure!(
            package.manifest().package.id == request.object,
            "replacement changes module identity"
        );
        self.manager.reload(package, request.mode).await
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReloadRequest {
    pub object: Token,
    pub directory: String,
    pub mode: StopMode,
}

impl Packages {
    pub async fn directory_for(&self, object: &Token) -> Result<String> {
        let mut found = self.list().await?.into_iter().filter_map(|p| match p {
            LocalPackage::Ready {
                directory,
                manifest,
            } if &manifest.package.id == object => Some(directory),
            _ => None,
        });
        let directory = found.next().context("package object not found")?;
        ensure!(
            found.next().is_none(),
            "multiple local packages declare {object}"
        );
        Ok(directory)
    }
    pub async fn unload(&self, object: &Token, mode: StopMode) -> Result<()> {
        self.manager.unload(object, mode).await
    }
    pub fn force(&self, object: &Token) -> Result<()> {
        self.manager.force(object)
    }
}
