use crate::{App, auth};
use anyhow::{Context, Result};
use axum::{
    Json, Router,
    extract::State,
    http::StatusCode,
    middleware,
    response::{IntoResponse, Response},
    routing::{get, post},
};
pub(crate) use forest_control::packages::{LocalPackage, Packages, Plan, ReloadRequest};
use forest_protocol::Token;
use forest_runtime::StopMode;
use serde::Deserialize;
use std::{future::Future, sync::Arc};

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
    execute(async move { app.packages.unload(&input.object, input.mode).await }).await
}
async fn force(
    State(app): State<App>,
    Json(input): Json<ObjectRequest>,
) -> Result<StatusCode, ManagementError> {
    app.packages.force(&input.object)?;
    Ok(StatusCode::NO_CONTENT)
}
