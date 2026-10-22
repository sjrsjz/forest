use anyhow::{Context, Result, ensure};
use clap::{ArgGroup, Parser, ValueEnum};
use forest_onebot::{Client, http::HttpTransport};
use forest_protocol::{SubjectPattern, abi, service::LogLevel};
use forest_runtime::{
    LogWriter, RuntimeOptions, Scheduler, StopMode,
    onebot::{self, GroupScope, OneBotIngress},
};
use futures::{SinkExt, StreamExt};
use std::{net::SocketAddr, path::PathBuf};
use tokio_tungstenite::{
    WebSocketStream,
    tungstenite::{Message, protocol::WebSocketConfig},
};
mod connection;
mod logs;
mod websocket;

#[derive(Parser)]
#[command(name="forest",about="Forest Wasm microkernel and OneBot host",group(ArgGroup::new("ingress").required(true).args(["reverse_ws","events_ws"])))]
struct Arguments {
    #[cfg(feature = "webui")]
    #[arg(long, help = "Enable the host console on this address")]
    webui: Option<SocketAddr>,
    #[cfg(feature = "webui")]
    #[arg(
        long,
        default_value = "FOREST_WEB_PASSWORD",
        help = "Environment variable containing the console password"
    )]
    web_password_env: String,
    #[cfg(feature = "webui")]
    #[arg(long, default_value = "webui/dist", help = "Built Vue console assets")]
    webui_assets: PathBuf,
    #[arg(
        long,
        default_value = "packages",
        help = "Local package catalog for dependency loading and the console"
    )]
    packages_directory: PathBuf,
    #[arg(long, default_value = "packages/forest-test")]
    package: Vec<PathBuf>,
    #[arg(long, default_value = "data/forest.sqlite3")]
    database: PathBuf,
    #[arg(long, default_value = "data/modules")]
    data_directory: PathBuf,
    #[arg(long, help = "OneBot v11 HTTP API base URL")]
    onebot_http: Option<String>,
    #[arg(long, help = "Accept OneBot reverse WebSocket on this address")]
    reverse_ws: Option<SocketAddr>,
    #[arg(long, help = "Connect to a OneBot forward WebSocket URL")]
    events_ws: Option<String>,
    #[arg(
        long,
        value_delimiter = ',',
        help = "Optional test restriction on received groups and native replies; omit for module-controlled group selection"
    )]
    allow_group: Vec<i64>,
    #[arg(
        long,
        default_value = "FOREST_ONEBOT_TOKEN",
        help = "Environment variable containing the HTTP API Bearer token"
    )]
    token_env: String,
    #[arg(
        long,
        help = "Environment variable containing the event Bearer token; defaults to HTTP token"
    )]
    event_token_env: Option<String>,
    #[arg(long, value_enum, default_value = "drain")]
    shutdown_mode: Mode,
}
#[derive(Clone, Copy, ValueEnum)]
enum Mode {
    Drain,
    Yield,
    Force,
}
impl From<Mode> for StopMode {
    fn from(mode: Mode) -> Self {
        match mode {
            Mode::Drain => Self::Drain,
            Mode::Yield => Self::AtYield,
            Mode::Force => Self::Force,
        }
    }
}

#[tokio::main(worker_threads = 2)]
async fn main() -> Result<()> {
    let args = Arguments::parse();
    ensure!(
        args.events_ws.is_some() || args.onebot_http.is_some(),
        "--reverse-ws requires --onebot-http"
    );
    if let Some(parent) = args.database.parent() {
        tokio::fs::create_dir_all(parent).await?;
    }
    let token = optional_secret(&args.token_env)?;
    let event_token = match &args.event_token_env {
        Some(name) => Some(
            std::env::var(name).with_context(|| format!("missing environment variable {name}"))?,
        ),
        None => token.clone(),
    };
    if let Some(address) = args.reverse_ws {
        ensure!(
            address.ip().is_loopback() || event_token.is_some(),
            "a non-loopback event listener requires an event Bearer token"
        );
    }
    let scheduler = Scheduler::new(
        &args.database,
        RuntimeOptions {
            data_directory: args.data_directory,
            ..RuntimeOptions::default()
        },
    )
    .await?;
    let logs = scheduler.logs();
    let _stderr = logs::Stderr::start(logs.reader());
    tokio::fs::create_dir_all(&args.packages_directory).await?;
    let catalog = forest_control::packages::Packages::new(
        &args.packages_directory,
        scheduler.module_manager(),
    )
    .await?;
    forest_control::install(
        &scheduler,
        catalog,
        scheduler.config_writer("/user/admin".parse()?),
        vec![SubjectPattern::Path("/module/shell".parse()?)],
    )?;
    #[cfg(feature = "webui")]
    let console = if let Some(address) = args.webui {
        let password = std::env::var(&args.web_password_env)
            .with_context(|| format!("missing environment variable {}", args.web_password_env))?;
        let server = forest_webui::Console::bind(
            scheduler.inspector(),
            scheduler.module_manager(),
            scheduler.config_writer("/user/admin".parse().unwrap()),
            forest_webui::Options {
                address,
                password,
                assets: args.webui_assets,
                packages: args.packages_directory.clone(),
            },
        )
        .await?;
        logs.write(
            LogLevel::Info,
            "/core/runtime".parse().unwrap(),
            format!("Forest console at http://{}", server.address()),
        );
        Some(server)
    } else {
        None
    };
    let mut available = Vec::new();
    let mut roots = Vec::new();
    let mut declarations = Vec::new();
    for directory in args.package {
        let package = scheduler.package(&directory).await?;
        roots.push(package.manifest().package.id.clone());
        declarations.push((directory, package.manifest().clone()));
        available.push(package);
    }
    let mut actions = None;
    let transport = if let Some(endpoint) = args.onebot_http {
        let http = reqwest::Client::builder()
            .no_proxy()
            .redirect(reqwest::redirect::Policy::none())
            .retry(reqwest::retry::never())
            .build()?;
        Gateway::Http(HttpTransport::with_client(
            endpoint.parse()?,
            token.as_deref(),
            http,
        )?)
    } else {
        let (sender, incoming) = websocket::channel(1024);
        actions = Some(incoming);
        Gateway::WebSocket(sender)
    };
    let ingress = onebot::install(
        &scheduler,
        Client::new(transport),
        if args.allow_group.is_empty() {
            GroupScope::All
        } else {
            GroupScope::only(args.allow_group)
        },
        vec![SubjectPattern::Path("/module/**".parse()?)],
    )?;
    let directories = match tokio::fs::read_dir(&args.packages_directory).await {
        Ok(directory) => Some(directory),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => None,
        Err(error) => return Err(error.into()),
    };
    if let Some(mut directories) = directories {
        while let Some(entry) = directories.next_entry().await? {
            if entry.file_type().await?.is_dir()
                && entry.path().join("manifest.json").is_file()
                && entry.path().join("module.wasm").is_file()
            {
                let candidate: forest_protocol::Manifest = serde_json::from_slice(
                    &tokio::fs::read(entry.path().join("manifest.json")).await?,
                )?;
                if !roots.contains(&candidate.package.id) {
                    declarations.push((entry.path(), candidate));
                }
            }
        }
    }
    let selected = scheduler.plan_manifests(
        &declarations
            .iter()
            .map(|(_, m)| m.clone())
            .collect::<Vec<_>>(),
        &roots,
    )?;
    for (directory, manifest) in declarations {
        if !roots.contains(&manifest.package.id) && selected.contains(&manifest.package.id) {
            available.push(scheduler.package(directory).await?);
        }
    }
    scheduler.load_roots(available, &roots).await?;
    let connection = async {
        match (args.reverse_ws, args.events_ws) {
            (Some(address), _) => reverse(address, ingress, event_token, logs).await,
            (_, Some(url)) => connection::forward(url, event_token, ingress, actions, logs).await,
            _ => unreachable!("CLI validates ingress selection"),
        }
    };
    let web = async {
        #[cfg(feature = "webui")]
        if let Some(console) = console {
            return console.serve(std::future::pending()).await;
        }
        std::future::pending::<Result<()>>().await
    };
    let result = tokio::select! {
        result=connection=>result,
        result=web=>result,
        signal=tokio::signal::ctrl_c()=>{signal?;Ok(())}
    };
    scheduler.shutdown(args.shutdown_mode.into()).await?;
    result
}

enum Gateway {
    Http(HttpTransport),
    WebSocket(websocket::Actions),
}
#[derive(Debug, thiserror::Error)]
enum GatewayError {
    #[error(transparent)]
    Http(#[from] forest_onebot::http::HttpError),
    #[error(transparent)]
    WebSocket(#[from] websocket::Error),
}
impl forest_onebot::Transport for Gateway {
    type Error = GatewayError;
    async fn request(&self, action: &str, params: Vec<u8>) -> Result<Vec<u8>, GatewayError> {
        match self {
            Self::Http(http) => Ok(http.request(action, params).await?),
            Self::WebSocket(ws) => Ok(ws.request(action, params).await?),
        }
    }
}

fn optional_secret(name: &str) -> Result<Option<String>> {
    match std::env::var(name) {
        Ok(value) => Ok(Some(value)),
        Err(std::env::VarError::NotPresent) => Ok(None),
        Err(error) => Err(error.into()),
    }
}

async fn reverse(
    address: SocketAddr,
    ingress: OneBotIngress,
    token: Option<String>,
    logs: LogWriter,
) -> Result<()> {
    use tokio_tungstenite::tungstenite::handshake::server::{ErrorResponse, Request, Response};
    let listener = tokio::net::TcpListener::bind(address).await?;
    let source: forest_protocol::Token = "/core/onebot".parse().unwrap();
    logs.write(
        LogLevel::Info,
        source.clone(),
        format!(
            "OneBot reverse WebSocket listening on {}",
            listener.local_addr()?
        ),
    );
    loop {
        let (stream, peer) = listener.accept().await?;
        let expected = token.as_ref().map(|token| format!("Bearer {token}"));
        // Tungstenite's callback requires its concrete ErrorResponse type.
        #[allow(clippy::result_large_err)]
        let handshake = move |request: &Request,
                              response: Response|
              -> std::result::Result<Response, ErrorResponse> {
            if let Some(expected) = &expected
                && request
                    .headers()
                    .get("authorization")
                    .and_then(|value| value.to_str().ok())
                    != Some(expected.as_str())
            {
                return Err(tokio_tungstenite::tungstenite::http::Response::builder()
                    .status(401)
                    .body(Some("unauthorized".into()))
                    .unwrap());
            }
            Ok(response)
        };
        let config = WebSocketConfig::default().max_message_size(Some(abi::MAX_FRAME_BYTES));
        match tokio_tungstenite::accept_hdr_async_with_config(stream, handshake, Some(config)).await
        {
            Ok(socket) => {
                logs.write(
                    LogLevel::Info,
                    source.clone(),
                    format!("OneBot connected from {peer}"),
                );
                if let Err(error) = receive(socket, &ingress, &logs).await {
                    logs.write(
                        LogLevel::Warn,
                        source.clone(),
                        format!("OneBot connection ended: {error}"),
                    );
                } else {
                    logs.write(
                        LogLevel::Info,
                        source.clone(),
                        "OneBot connection closed; waiting for reconnect",
                    );
                }
            }
            Err(error) => logs.write(
                LogLevel::Warn,
                source.clone(),
                format!("OneBot handshake rejected: {error}"),
            ),
        }
    }
}

async fn receive<S>(
    mut socket: WebSocketStream<S>,
    ingress: &OneBotIngress,
    logs: &LogWriter,
) -> Result<()>
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    while let Some(message) = socket.next().await {
        match message? {
            Message::Text(text) => dispatch_event(ingress, logs, text.as_bytes()),
            Message::Binary(bytes) => dispatch_event(ingress, logs, &bytes),
            Message::Ping(bytes) => socket.send(Message::Pong(bytes)).await?,
            Message::Close(_) => break,
            Message::Pong(_) | Message::Frame(_) => {}
        }
    }
    Ok(())
}
fn report(report: onebot::IngressReport, logs: &LogWriter) {
    for (object, error) in report.rejected {
        logs.write(
            LogLevel::Warn,
            object.clone(),
            format!("host event rejected by {object}: {error}"),
        );
    }
}
fn dispatch_event(ingress: &OneBotIngress, logs: &LogWriter, bytes: &[u8]) {
    match ingress.receive(bytes) {
        Ok(result) => report(result, logs),
        Err(error) => logs.write(
            LogLevel::Warn,
            "/core/onebot".parse().unwrap(),
            format!("OneBot event rejected before admission: {error}"),
        ),
    }
}
