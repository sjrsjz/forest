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
use forest_control::packages::ReloadGroupRequest;
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
        .route("/reload-group", post(reload_group))
        .route("/unload", post(unload))
        .route("/unload-group", post(unload_group))
        .route("/cancel", post(cancel))
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
        let cancelled = matches!(
            self.0.downcast_ref::<forest_runtime::ManagementFailure>(),
            Some(forest_runtime::ManagementFailure::Cancelled)
        );
        (
            StatusCode::UNPROCESSABLE_ENTITY,
            Json(serde_json::json!({"code": if cancelled { "cancelled" } else { "rejected" }, "message": format!("{:#}", self.0)})),
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
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct GroupStopRequest {
    objects: Vec<Token>,
    mode: StopMode,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct CancelRequest {
    request: forest_runtime::RequestId,
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
async fn reload_group(
    State(app): State<App>,
    Json(input): Json<ReloadGroupRequest>,
) -> Result<StatusCode, ManagementError> {
    execute(async move { app.packages.reload_group(input).await }).await
}
async fn unload_group(
    State(app): State<App>,
    Json(input): Json<GroupStopRequest>,
) -> Result<StatusCode, ManagementError> {
    execute(async move { app.packages.unload_many(input.objects, input.mode).await }).await
}
async fn cancel(
    State(app): State<App>,
    Json(input): Json<CancelRequest>,
) -> Result<StatusCode, ManagementError> {
    app.packages.cancel(input.request)?;
    Ok(StatusCode::NO_CONTENT)
}
async fn force(
    State(app): State<App>,
    Json(input): Json<ObjectRequest>,
) -> Result<StatusCode, ManagementError> {
    app.packages.force(&input.object)?;
    Ok(StatusCode::NO_CONTENT)
}
