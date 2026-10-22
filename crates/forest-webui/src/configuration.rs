use crate::{ApiError, App, EntryQuery, auth, form};
use axum::{
    Json, Router,
    extract::{DefaultBodyLimit, Query, State},
    http::StatusCode,
    middleware,
    response::{IntoResponse, Response},
    routing::{get, post},
};
use forest_config::ConfigError;
use forest_protocol::{
    DispatchError, Token, abi,
    service::{ConfigKey, ConfigSet},
};
use forest_runtime::ConfigWriteError;
use serde::Deserialize;
use std::sync::Arc;

pub(crate) fn routes(auth: Arc<auth::Auth>) -> Router<App> {
    Router::new()
        .route("/form", get(form))
        .route("/set", post(set))
        .layer(DefaultBodyLimit::max(abi::MAX_FRAME_BYTES))
        .route_layer(middleware::from_fn_with_state(auth, auth::protect))
}

async fn form(
    State(app): State<App>,
    Query(query): Query<EntryQuery>,
) -> Result<Json<form::Form>, ApiError> {
    let entry = app.inspector.config_entry(query.object, query.key).await?;
    Ok(Json(form::project(entry.schema(), entry.value())))
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct SetValue {
    object: Token,
    key: Token,
    /// Forest's lossless value text, the same representation as value download.
    value: String,
}

struct Error(StatusCode, String);
impl IntoResponse for Error {
    fn into_response(self) -> Response {
        (self.0, Json(serde_json::json!({"message": self.1}))).into_response()
    }
}

async fn set(State(app): State<App>, Json(input): Json<SetValue>) -> Result<StatusCode, Error> {
    let value = forest_serde::parser::parse_value(&input.value)
        .map_err(|e| Error(StatusCode::BAD_REQUEST, e.to_string()))?;
    app.config
        .set(ConfigSet {
            target: ConfigKey {
                object: input.object,
                key: input.key,
            },
            value,
        })
        .await
        .map_err(write_error)?;
    Ok(StatusCode::NO_CONTENT)
}

fn write_error(error: ConfigWriteError) -> Error {
    let status = match &error {
        ConfigWriteError::Dispatch(DispatchError::Denied) => StatusCode::FORBIDDEN,
        ConfigWriteError::Dispatch(DispatchError::HostStopped) => StatusCode::SERVICE_UNAVAILABLE,
        ConfigWriteError::Configuration(ConfigError::InvalidValue { .. }) => {
            StatusCode::UNPROCESSABLE_ENTITY
        }
        ConfigWriteError::Configuration(
            ConfigError::UnknownEntity { .. } | ConfigError::UnknownKey { .. },
        ) => StatusCode::NOT_FOUND,
        _ => StatusCode::INTERNAL_SERVER_ERROR,
    };
    Error(status, error.to_string())
}
