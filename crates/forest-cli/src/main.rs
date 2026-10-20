use anyhow::{Context, Result, ensure};
use clap::{ArgGroup, Parser, ValueEnum};
use forest_onebot::{Client, http::HttpTransport};
use forest_protocol::{SubjectPattern, abi};
use forest_runtime::{
    RuntimeOptions, Scheduler, StopMode,
    onebot::{self, OneBotIngress},
};
use futures::{SinkExt, StreamExt};
use std::{net::SocketAddr, path::PathBuf};
use tokio_tungstenite::{
    WebSocketStream,
    tungstenite::{Message, client::IntoClientRequest, protocol::WebSocketConfig},
};
mod websocket;

#[derive(Parser)]
#[command(name="forest",about="Forest Wasm microkernel and OneBot host",group(ArgGroup::new("ingress").required(true).args(["reverse_ws","events_ws"])))]
struct Arguments {
    #[arg(long, default_value = "packages/forest-test")]
    package: PathBuf,
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
        required = true,
        value_delimiter = ',',
        help = "Only these groups may reach the plugin or receive native replies"
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
    let package = scheduler.package(&args.package).await?;
    let object = package.manifest().package.id.clone();
    let mut forward = if let Some(url) = args.events_ws {
        let mut request = url.into_client_request()?;
        if let Some(token) = &event_token {
            request
                .headers_mut()
                .insert("authorization", format!("Bearer {token}").parse()?);
        }
        let config = WebSocketConfig::default().max_message_size(Some(abi::MAX_FRAME_BYTES));
        Some(
            tokio_tungstenite::connect_async_with_config(request, Some(config), false)
                .await?
                .0,
        )
    } else {
        None
    };
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
        args.allow_group,
        vec![SubjectPattern::Path(forest_protocol::TokenPattern::exact(
            object.clone(),
        ))],
    )?;
    scheduler.load(package).await?;
    eprintln!("module {object} is Serving");
    let connection = async {
        match (args.reverse_ws, forward.take()) {
            (Some(address), _) => reverse(address, ingress, event_token).await,
            (_, Some(stream)) => match actions {
                Some(actions) => websocket::serve(stream, &ingress, actions).await,
                None => receive(stream, &ingress).await,
            },
            _ => unreachable!("CLI validates ingress selection"),
        }
    };
    let result =
        tokio::select! {result=connection=>result,signal=tokio::signal::ctrl_c()=>{signal?;Ok(())}};
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

async fn reverse(address: SocketAddr, ingress: OneBotIngress, token: Option<String>) -> Result<()> {
    use tokio_tungstenite::tungstenite::handshake::server::{ErrorResponse, Request, Response};
    let listener = tokio::net::TcpListener::bind(address).await?;
    eprintln!(
        "OneBot reverse WebSocket listening on {}",
        listener.local_addr()?
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
                eprintln!("OneBot connected from {peer}");
                if let Err(error) = receive(socket, &ingress).await {
                    eprintln!("OneBot connection ended: {error}");
                }
            }
            Err(error) => eprintln!("OneBot handshake rejected: {error}"),
        }
    }
}

async fn receive<S>(mut socket: WebSocketStream<S>, ingress: &OneBotIngress) -> Result<()>
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    while let Some(message) = socket.next().await {
        match message? {
            Message::Text(text) => report(ingress.receive(text.as_bytes())?),
            Message::Binary(bytes) => report(ingress.receive(&bytes)?),
            Message::Ping(bytes) => socket.send(Message::Pong(bytes)).await?,
            Message::Close(_) => break,
            Message::Pong(_) | Message::Frame(_) => {}
        }
    }
    Ok(())
}
fn report(report: onebot::IngressReport) {
    for (object, error) in report.rejected {
        eprintln!("host event rejected by {object}: {error}");
    }
}
