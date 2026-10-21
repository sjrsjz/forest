use crate::{App, auth};
use anyhow::{Context, Result, ensure};
use axum::{
    Json, Router,
    extract::State,
    http::StatusCode,
    middleware,
    response::{IntoResponse, Response},
    routing::{get, post},
};
use forest_protocol::{Manifest, Token};
use forest_runtime::{ModuleManager, StopMode};
use serde::{Deserialize, Serialize};
use std::{
    collections::HashMap,
    future::Future,
    path::{Component, Path, PathBuf},
    sync::Arc,
};

#[derive(Clone)]
pub(crate) struct Packages {
    root: PathBuf,
    manager: ModuleManager,
}

#[derive(Serialize)]
#[serde(tag = "status", rename_all = "snake_case")]
enum LocalPackage {
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
struct Plan {
    object: Token,
    order: Vec<PlannedPackage>,
}
#[derive(Serialize)]
struct PlannedPackage {
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
    async fn list(&self) -> Result<Vec<LocalPackage>> {
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
    async fn plan(&self, name: &str) -> Result<Plan> {
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
    async fn load(&self, name: &str) -> Result<()> {
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
    async fn reload(&self, request: ReloadRequest) -> Result<()> {
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

pub(crate) fn routes(auth: Arc<auth::Auth>) -> Router<App> {
    Router::new()
        .route("/packages", get(packages))
        .route("/plan", post(plan))
        .route("/load", post(load))
        .route("/reload", post(reload))
        .route("/unload", post(unload))
        .route("/force", post(force))
        .route_layer(middleware::from_fn_with_state(auth, auth::protect))
}

struct ManagementError(anyhow::Error);
impl From<anyhow::Error> for ManagementError {
    fn from(error: anyhow::Error) -> Self {
        Self(error)
    }
}
impl IntoResponse for ManagementError {
    fn into_response(self) -> Response {
        (
            StatusCode::UNPROCESSABLE_ENTITY,
            Json(serde_json::json!({"message": format!("{:#}", self.0)})),
        )
            .into_response()
    }
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct PackageRequest {
    directory: String,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ReloadRequest {
    object: Token,
    directory: String,
    mode: StopMode,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct StopRequest {
    object: Token,
    mode: StopMode,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ObjectRequest {
    object: Token,
}

async fn packages(State(app): State<App>) -> Result<Json<Vec<LocalPackage>>, ManagementError> {
    Ok(Json(app.packages.list().await?))
}
async fn plan(
    State(app): State<App>,
    Json(input): Json<PackageRequest>,
) -> Result<Json<Plan>, ManagementError> {
    Ok(Json(app.packages.plan(&input.directory).await?))
}
// The host owns each started operation even if the HTTP waiter disconnects.
async fn execute(
    operation: impl Future<Output = Result<()>> + Send + 'static,
) -> Result<StatusCode, ManagementError> {
    tokio::spawn(operation)
        .await
        .context("module management operation")??;
    Ok(StatusCode::NO_CONTENT)
}
async fn load(
    State(app): State<App>,
    Json(input): Json<PackageRequest>,
) -> Result<StatusCode, ManagementError> {
    execute(async move { app.packages.load(&input.directory).await }).await
}
async fn reload(
    State(app): State<App>,
    Json(input): Json<ReloadRequest>,
) -> Result<StatusCode, ManagementError> {
    execute(async move { app.packages.reload(input).await }).await
}
async fn unload(
    State(app): State<App>,
    Json(input): Json<StopRequest>,
) -> Result<StatusCode, ManagementError> {
    execute(async move { app.packages.manager.unload(&input.object, input.mode).await }).await
}
async fn force(
    State(app): State<App>,
    Json(input): Json<ObjectRequest>,
) -> Result<StatusCode, ManagementError> {
    app.packages.manager.force(&input.object)?;
    Ok(StatusCode::NO_CONTENT)
}
