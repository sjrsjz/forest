use crate::core::{Core, Directory, Instance, State};
use crate::{ConfigWriteError, IoSettings};
use anyhow::Result;
use cap_std::fs::Dir;
use forest_protocol::{service::*, *};
use forest_util::acl::Access;
use futures::{
    SinkExt, StreamExt,
    stream::{SplitSink, SplitStream},
};
use std::{
    collections::HashMap,
    path::{Component, Path},
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, Ordering},
    },
};
use tokio::net::TcpStream;
use tokio_tungstenite::{
    MaybeTlsStream, WebSocketStream,
    tungstenite::{Message, client::IntoClientRequest},
};

pub(crate) struct Services {
    pub io: IoSettings,
    pub config: forest_config::ConfigStore,
    pub http: reqwest::Client,
    root: Arc<Dir>,
    database_root: std::path::PathBuf,
}
type SocketStream = WebSocketStream<MaybeTlsStream<TcpStream>>;
struct Socket {
    send: tokio::sync::Mutex<SplitSink<SocketStream, Message>>,
    receive: tokio::sync::Mutex<SplitStream<SocketStream>>,
}
pub(crate) struct Resources {
    io: IoSettings,
    pub files: Arc<Dir>,
    pub database: Arc<crate::sqlite::Database>,
    sockets: Mutex<HashMap<u64, Arc<Socket>>>,
    next: AtomicU64,
}

impl Services {
    pub(crate) async fn config_set(&self, input: ConfigSet) -> Result<(), ConfigWriteError> {
        IoSettings::validate_set(&input)?;
        self.config
            .apply([forest_config::Change::set(
                input.target.object,
                input.target.key,
                input.value,
            )])
            .await?;
        Ok(())
    }
    pub async fn new(
        config: forest_config::ConfigStore,
        root: &Path,
        proxy: Option<&str>,
    ) -> Result<Self> {
        tokio::fs::create_dir_all(root).await?;
        let database_root = tokio::fs::canonicalize(root).await?.join(".databases");
        let root = Arc::new(Dir::open_ambient_dir(root, cap_std::ambient_authority())?);
        let mut http = reqwest::Client::builder()
            .no_proxy()
            .redirect(reqwest::redirect::Policy::none())
            .retry(reqwest::retry::never());
        if let Some(proxy) = proxy {
            http = http.proxy(reqwest::Proxy::all(proxy)?);
        }
        Ok(Self {
            io: IoSettings::initialize(config.clone()).await?,
            config,
            http: http.build()?,
            root,
            database_root,
        })
    }
    pub async fn mount(&self, manifest: &Manifest) -> Result<Arc<Resources>> {
        let object = manifest.package.id.clone();
        let target = object.clone();
        let existing = self
            .config
            .read(move |view| Ok(view.entities()?.contains(&target)))
            .await?;
        let declarations = manifest
            .configs
            .iter()
            .map(|d| {
                forest_config::Declaration::new(d.key.clone(), d.schema.clone(), d.default.clone())
            })
            .collect::<std::result::Result<Vec<_>, _>>()?;
        if existing {
            self.config.update_defaults(&object, declarations).await?;
        } else {
            self.config.register(object.clone(), declarations).await?;
        }
        let name = object
            .to_string()
            .as_bytes()
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect::<String>();
        let root = self.root.clone();
        let database =
            crate::sqlite::Database::new(self.database_root.join(&name).join("data.sqlite3"));
        let files = tokio::task::spawn_blocking(move || -> std::io::Result<Dir> {
            root.create_dir_all(&name)?;
            root.open_dir(name)
        })
        .await??;
        Ok(Arc::new(Resources {
            io: self.io.clone(),
            files: Arc::new(files),
            database,
            sockets: Mutex::new(HashMap::new()),
            next: AtomicU64::new(1),
        }))
    }
    pub async fn http(
        &self,
        request: HttpRequest,
    ) -> std::result::Result<HttpResponse, ServiceError> {
        let limit = self.io.get().await.map_err(storage)?.http_response_bytes();
        let method = reqwest::Method::from_bytes(request.method.as_bytes())
            .map_err(|e| ServiceError::InvalidInput(e.to_string()))?;
        let mut builder = self.http.request(method, &request.url).body(request.body);
        for (name, value) in request.headers {
            builder = builder.header(name, value);
        }
        let mut response = builder.send().await.map_err(transport)?;
        let status = response.status().as_u16();
        let headers = response
            .headers()
            .iter()
            .map(|(name, value)| {
                Ok((
                    name.to_string(),
                    value
                        .to_str()
                        .map_err(|e| ServiceError::Transport(e.to_string()))?
                        .to_owned(),
                ))
            })
            .collect::<std::result::Result<Vec<_>, ServiceError>>()?;
        let mut body = Vec::new();
        while let Some(chunk) = response.chunk().await.map_err(transport)? {
            if chunk.len() > limit - body.len() {
                return Err(ServiceError::Transport(format!(
                    "HTTP response exceeds configured limit of {limit} bytes"
                )));
            }
            body.extend_from_slice(&chunk);
        }
        Ok(HttpResponse {
            status,
            headers,
            body,
        })
    }
}

impl Core {
    pub(crate) fn config_access_as(
        directory: &Directory,
        subject: &Token,
        target: &ConfigKey,
        access: Access,
    ) -> DispatchResult<()> {
        let table = directory
            .slots
            .get(&target.object)
            .map(|slot| &slot.access.config)
            .unwrap_or(&directory.host_config);
        if table.check(subject, &target.object, &target.key, access) {
            Ok(())
        } else {
            Err(DispatchError::Denied)
        }
    }

    pub(crate) fn config_access_in(
        directory: &Directory,
        instance: &Instance,
        target: &ConfigKey,
        access: Access,
    ) -> DispatchResult<()> {
        let owner = &target.object;
        if owner == &instance.object
            && directory
                .slots
                .get(owner)
                .is_some_and(|slot| matches!(slot.state, State::Creating { .. }))
        {
            return instance
                .package
                .access
                .config
                .check(&instance.object, owner, &target.key, access)
                .then_some(())
                .ok_or(DispatchError::Denied);
        }
        Self::config_access_as(directory, &instance.object, target, access)
    }
}

impl Resources {
    pub fn close(&self) {
        self.sockets.lock().unwrap().clear();
        self.database.close();
    }
    pub async fn ws_connect(
        &self,
        request: WebSocketRequest,
    ) -> std::result::Result<u64, ServiceError> {
        let limit = self
            .io
            .get()
            .await
            .map_err(storage)?
            .websocket_message_bytes();
        let mut wire = request.url.into_client_request().map_err(transport)?;
        for (name, value) in request.headers {
            wire.headers_mut().append(
                name.parse::<tokio_tungstenite::tungstenite::http::HeaderName>()
                    .map_err(transport)?,
                value.parse().map_err(transport)?,
            );
        }
        let config = tokio_tungstenite::tungstenite::protocol::WebSocketConfig::default()
            .max_message_size(Some(limit))
            .max_frame_size(Some(limit));
        let (stream, _) = tokio_tungstenite::connect_async_with_config(wire, Some(config), false)
            .await
            .map_err(transport)?;
        let (send, receive) = stream.split();
        let id = self.next.fetch_add(1, Ordering::Relaxed);
        self.sockets.lock().unwrap().insert(
            id,
            Arc::new(Socket {
                send: tokio::sync::Mutex::new(send),
                receive: tokio::sync::Mutex::new(receive),
            }),
        );
        Ok(id)
    }
    fn socket(&self, id: u64) -> std::result::Result<Arc<Socket>, ServiceError> {
        self.sockets
            .lock()
            .unwrap()
            .get(&id)
            .cloned()
            .ok_or(ServiceError::NotFound)
    }
    pub async fn ws_send(&self, input: WebSocketSend) -> std::result::Result<(), ServiceError> {
        let socket = self.socket(input.handle)?;
        let message = match input.message {
            WebSocketMessage::Text(text) => Message::Text(text.into()),
            WebSocketMessage::Binary(bytes) => Message::Binary(bytes.into()),
        };
        socket
            .send
            .lock()
            .await
            .send(message)
            .await
            .map_err(transport)
    }
    pub async fn ws_receive(
        &self,
        handle: u64,
    ) -> std::result::Result<WebSocketEvent, ServiceError> {
        let socket = self.socket(handle)?;
        let mut stream = socket.receive.try_lock().map_err(|_| ServiceError::InUse)?;
        while let Some(message) = stream.next().await {
            match message.map_err(transport)? {
                Message::Text(text) => {
                    return Ok(WebSocketEvent::Message(WebSocketMessage::Text(
                        text.to_string(),
                    )));
                }
                Message::Binary(bytes) => {
                    return Ok(WebSocketEvent::Message(WebSocketMessage::Binary(
                        bytes.to_vec(),
                    )));
                }
                Message::Close(_) => return Ok(WebSocketEvent::Closed),
                Message::Ping(_) | Message::Pong(_) | Message::Frame(_) => {}
            }
        }
        Ok(WebSocketEvent::Closed)
    }
    pub async fn ws_close(&self, handle: u64) -> std::result::Result<(), ServiceError> {
        let socket = self
            .sockets
            .lock()
            .unwrap()
            .remove(&handle)
            .ok_or(ServiceError::NotFound)?;
        socket.send.lock().await.close().await.map_err(transport)
    }
    pub async fn file_read(&self, path: String) -> std::result::Result<Vec<u8>, ServiceError> {
        validate_path(&path)?;
        let limit = self.io.get().await.map_err(storage)?.file_read_bytes();
        let root = self.files.clone();
        tokio::task::spawn_blocking(move || {
            use std::io::Read;
            let mut bytes = Vec::new();
            root.open(path)?
                .take(limit as u64 + 1)
                .read_to_end(&mut bytes)?;
            if bytes.len() > limit {
                return Err(std::io::Error::other(format!(
                    "file exceeds configured read limit of {limit} bytes"
                )));
            }
            Ok(bytes)
        })
        .await
        .map_err(storage)?
        .map_err(storage)
    }
    pub async fn file_write(&self, input: FileWrite) -> std::result::Result<(), ServiceError> {
        validate_path(&input.path)?;
        let root = self.files.clone();
        tokio::task::spawn_blocking(move || {
            if let Some(parent) = Path::new(&input.path).parent() {
                root.create_dir_all(parent)?;
            }
            root.write(input.path, input.bytes)
        })
        .await
        .map_err(storage)?
        .map_err(storage)
    }
    pub async fn file_remove(&self, path: String) -> std::result::Result<(), ServiceError> {
        validate_path(&path)?;
        let root = self.files.clone();
        tokio::task::spawn_blocking(move || root.remove_file(path))
            .await
            .map_err(storage)?
            .map_err(storage)
    }
}

fn validate_path(path: &str) -> std::result::Result<(), ServiceError> {
    if path.is_empty()
        || path.contains('\\')
        || path.contains(':')
        || Path::new(path)
            .components()
            .any(|part| !matches!(part, Component::Normal(_)))
    {
        return Err(ServiceError::InvalidInput(
            "expected a managed relative file path".into(),
        ));
    }
    Ok(())
}
fn transport(error: impl std::fmt::Display) -> ServiceError {
    ServiceError::Transport(error.to_string())
}
fn storage(error: impl std::fmt::Display) -> ServiceError {
    ServiceError::Storage(error.to_string())
}
