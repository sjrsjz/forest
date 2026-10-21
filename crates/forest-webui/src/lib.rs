//! Optional host console with separate inspection and lifecycle capabilities.
mod auth;
mod management;
mod value;

use anyhow::{Context, Result};
use axum::{
    Json, Router,
    extract::{DefaultBodyLimit, Query, State},
    http::{HeaderValue, StatusCode, header},
    middleware,
    response::{
        IntoResponse, Response, Sse,
        sse::{Event, KeepAlive},
    },
    routing::get,
};
use forest_protocol::Token;
use forest_runtime::{InspectError, Inspector, ModuleManager, RuntimeSnapshot};
use futures::{Stream, StreamExt};
use serde::{Deserialize, Serialize};
use std::{
    convert::Infallible,
    future::Future,
    net::SocketAddr,
    path::PathBuf,
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use tokio::{net::TcpListener, sync::watch};
use tower_http::{services::ServeDir, set_header::SetResponseHeaderLayer};

pub struct Options {
    pub address: SocketAddr,
    pub password: String,
    pub assets: PathBuf,
    pub packages: PathBuf,
}
pub struct Console {
    listener: TcpListener,
    router: Router,
    inspector: Inspector,
    snapshots: watch::Sender<Arc<RuntimeSnapshot>>,
    closing: watch::Sender<()>,
}
#[derive(Clone)]
struct App {
    inspector: Inspector,
    packages: management::Packages,
    snapshots: Arc<watch::Receiver<Arc<RuntimeSnapshot>>>,
    closing: watch::Receiver<()>,
}

impl Console {
    pub async fn bind(
        inspector: Inspector,
        manager: ModuleManager,
        options: Options,
    ) -> Result<Self> {
        anyhow::ensure!(
            options.assets.join("index.html").is_file(),
            "WebUI assets missing; run pnpm --dir webui install && pnpm --dir webui build"
        );
        let auth = auth::Auth::new(&options.password)?;
        let (snapshots, receiver) = watch::channel(Arc::new(inspector.snapshot()?));
        let (closing, closed) = watch::channel(());
        let app = App {
            inspector: inspector.clone(),
            packages: management::Packages::new(&options.packages, manager).await?,
            snapshots: Arc::new(receiver),
            closing: closed,
        };
        let api = Router::new()
            .route("/snapshot", get(snapshot))
            .route("/events", get(events))
            .route("/config-entities", get(entities))
            .route("/config-keys", get(keys))
            .route("/config-entry", get(entry))
            .route("/config-value", get(download))
            .route_layer(middleware::from_fn_with_state(auth.clone(), auth::protect))
            .with_state(app.clone());
        let modules = management::routes(auth.clone()).with_state(app);
        let session = Router::new()
            .route(
                "/session",
                get(auth::session).post(auth::login).delete(auth::logout),
            )
            .with_state(auth);
        let router = Router::new()
            .nest("/api/inspect", api)
            .nest("/api/modules", modules)
            .nest("/api", session)
            .fallback_service(ServeDir::new(options.assets))
            .layer(DefaultBodyLimit::max(4096))
            .layer(SetResponseHeaderLayer::overriding(
                header::CACHE_CONTROL,
                HeaderValue::from_static("no-store"),
            ));
        let listener = TcpListener::bind(options.address)
            .await
            .context("bind WebUI")?;
        Ok(Self {
            listener,
            router,
            inspector,
            snapshots,
            closing,
        })
    }
    pub fn address(&self) -> SocketAddr {
        self.listener.local_addr().expect("bound listener")
    }
    pub async fn serve(self, shutdown: impl Future<Output = ()> + Send + 'static) -> Result<()> {
        let mut closed = self.closing.subscribe();
        let inspector = self.inspector;
        let snapshots = self.snapshots;
        let sampler = tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(1));
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            loop {
                tokio::select! {
                    _=closed.changed()=>break,
                    _=interval.tick()=>if snapshots.receiver_count()>1{match inspector.snapshot(){Ok(view)=>{snapshots.send_replace(Arc::new(view));},Err(_)=>break,}}
                }
            }
        });
        let closing = self.closing;
        let result = axum::serve(self.listener, self.router)
            .with_graceful_shutdown(async move {
                shutdown.await;
                drop(closing);
            })
            .await;
        sampler.await.context("WebUI sampler")?;
        result.context("WebUI server")
    }
}

struct ApiError(InspectError);
impl From<InspectError> for ApiError {
    fn from(e: InspectError) -> Self {
        Self(e)
    }
}
impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let status = match &self.0 {
            InspectError::HostStopped => StatusCode::SERVICE_UNAVAILABLE,
            InspectError::Configuration(
                forest_config::ConfigError::UnknownEntity { .. }
                | forest_config::ConfigError::UnknownKey { .. },
            ) => StatusCode::NOT_FOUND,
            _ => StatusCode::INTERNAL_SERVER_ERROR,
        };
        (
            status,
            Json(serde_json::json!({"message":self.0.to_string()})),
        )
            .into_response()
    }
}
async fn snapshot(State(app): State<App>) -> Result<Json<RuntimeSnapshot>, ApiError> {
    Ok(Json(app.inspector.snapshot()?))
}
async fn events(
    State(app): State<App>,
) -> Result<Sse<impl Stream<Item = Result<Event, Infallible>>>, ApiError> {
    let first = Arc::new(app.inspector.snapshot()?);
    let updates = futures::stream::unfold(
        (app.snapshots.as_ref().clone(), app.closing),
        |(mut snapshots, mut closing)| async move {
            tokio::select! {
                _=closing.changed()=>None,
                changed=snapshots.changed()=>{changed.ok()?;let view=snapshots.borrow_and_update().clone();Some((view,(snapshots,closing)))}
            }
        },
    );
    let stream = futures::stream::once(async move { first })
        .chain(updates)
        .map(|view| {
            Ok(Event::default()
                .event("snapshot")
                .json_data(view.as_ref())
                .expect("snapshot JSON"))
        });
    Ok(Sse::new(stream).keep_alive(KeepAlive::default()))
}
#[derive(Serialize)]
struct Timed<T> {
    read_at_ms: u64,
    data: T,
}
fn timed<T>(data: T) -> Json<Timed<T>> {
    Json(Timed {
        read_at_ms: timestamp(),
        data,
    })
}
async fn entities(State(app): State<App>) -> Result<Json<Timed<Vec<Token>>>, ApiError> {
    Ok(timed(app.inspector.config_entities().await?))
}
#[derive(Deserialize)]
struct ObjectQuery {
    object: Token,
}
#[derive(Deserialize)]
struct EntryQuery {
    object: Token,
    key: Token,
}
#[derive(Default, Deserialize)]
#[serde(rename_all = "snake_case")]
enum ValueSource {
    #[default]
    Current,
    #[serde(rename = "default")]
    DefaultValue,
}
#[derive(Deserialize)]
struct DownloadQuery {
    object: Token,
    key: Token,
    #[serde(default)]
    value: ValueSource,
}
#[derive(Serialize)]
struct KeyView {
    key: Token,
    schema: String,
}
async fn keys(
    State(app): State<App>,
    Query(query): Query<ObjectQuery>,
) -> Result<Json<Timed<Vec<KeyView>>>, ApiError> {
    Ok(timed(
        app.inspector
            .config_keys(query.object)
            .await?
            .into_iter()
            .map(|h| KeyView {
                key: h.key,
                schema: h.schema.to_string(),
            })
            .collect(),
    ))
}
#[derive(Serialize)]
struct EntryView {
    schema: String,
    current: value::ValueTree,
    default: value::ValueTree,
    is_default: bool,
}
async fn entry(
    State(app): State<App>,
    Query(query): Query<EntryQuery>,
) -> Result<Json<Timed<EntryView>>, ApiError> {
    let entry = app.inspector.config_entry(query.object, query.key).await?;
    Ok(timed(EntryView {
        schema: entry.schema().to_string(),
        current: value::tree(entry.value()),
        default: value::tree(entry.default_value()),
        is_default: entry.value() == entry.default_value(),
    }))
}
async fn download(
    State(app): State<App>,
    Query(query): Query<DownloadQuery>,
) -> Result<Response, ApiError> {
    let entry = app.inspector.config_entry(query.object, query.key).await?;
    let value = match query.value {
        ValueSource::Current => entry.value(),
        ValueSource::DefaultValue => entry.default_value(),
    };
    Ok((
        [
            (header::CONTENT_TYPE, "text/plain; charset=utf-8"),
            (
                header::CONTENT_DISPOSITION,
                "attachment; filename=forest-value.txt",
            ),
        ],
        value.to_string(),
    )
        .into_response())
}
fn timestamp() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock before Unix epoch")
        .as_millis() as u64
}
