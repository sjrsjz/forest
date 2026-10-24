//! Persisted host I/O policy, read once at the start of each operation.
use crate::{ConfigWriteError, Scheduler};
use forest_config::{ConfigError, ConfigStore, Key};
use forest_protocol::{AccessPolicy, AsValue, SubjectPattern, Token, service::ConfigSet};
use forest_serde::{DeriveSchema, DeriveValue};
use forest_util::acl::{AclEntry, AclTable};

pub const RUNTIME_OBJECT: &str = "/core/runtime";
pub const IO_KEY: &str = "/io";
const MIB: usize = 1024 * 1024;

#[derive(Clone, Debug, PartialEq, Eq, DeriveSchema, DeriveValue)]
pub struct IoLimits {
    pub http_response_mib: i64,
    pub websocket_message_mib: i64,
    pub file_read_mib: i64,
}

impl Default for IoLimits {
    fn default() -> Self {
        Self {
            http_response_mib: 64,
            websocket_message_mib: 64,
            file_read_mib: 64,
        }
    }
}

impl IoLimits {
    fn validate(&self) -> Result<(), ConfigWriteError> {
        for (name, value) in [
            ("http_response_mib", self.http_response_mib),
            ("websocket_message_mib", self.websocket_message_mib),
            ("file_read_mib", self.file_read_mib),
        ] {
            if !(1..=(u32::MAX as usize / MIB) as i64).contains(&value) {
                return Err(ConfigWriteError::InvalidSetting(format!(
                    "{name} must be between 1 and 4095 MiB"
                )));
            }
        }
        Ok(())
    }
    pub fn http_response_bytes(&self) -> usize {
        self.http_response_mib as usize * MIB
    }
    pub fn websocket_message_bytes(&self) -> usize {
        self.websocket_message_mib as usize * MIB
    }
    pub fn file_read_bytes(&self) -> usize {
        self.file_read_mib as usize * MIB
    }
}

#[derive(Clone)]
pub struct IoSettings {
    config: ConfigStore,
}

impl Scheduler {
    pub fn io_settings(&self) -> IoSettings {
        self.core.services.io.clone()
    }
}

impl IoSettings {
    pub(crate) async fn initialize(config: ConfigStore) -> anyhow::Result<Self> {
        let object: Token = RUNTIME_OBJECT.parse().unwrap();
        let key = Key::<IoLimits>::new(IO_KEY.parse().unwrap());
        let existing = {
            let object = object.clone();
            config
                .read(move |view| Ok(view.entities()?.contains(&object)))
                .await?
        };
        if existing {
            config
                .update_defaults(&object, [key.declare(&IoLimits::default())?])
                .await?;
        } else {
            config
                .register(object, [key.declare(&IoLimits::default())?])
                .await?;
        }
        let settings = Self { config };
        settings.get().await?.validate()?;
        Ok(settings)
    }

    pub async fn get(&self) -> Result<IoLimits, ConfigError> {
        self.config
            .get(
                &RUNTIME_OBJECT.parse().unwrap(),
                &Key::new(IO_KEY.parse().unwrap()),
            )
            .await
    }

    pub(crate) fn validate_set(input: &ConfigSet) -> Result<(), ConfigWriteError> {
        if input.target.object == RUNTIME_OBJECT.parse().unwrap()
            && input.target.key == IO_KEY.parse().unwrap()
        {
            IoLimits::from_value(input.value.clone())
                .map_err(ConfigError::Decode)?
                .validate()?;
        }
        Ok(())
    }

    pub(crate) fn access() -> AclTable {
        AclTable::new([AclEntry {
            entity: RUNTIME_OBJECT.parse().unwrap(),
            key: IO_KEY.parse().unwrap(),
            owner: RUNTIME_OBJECT.parse().unwrap(),
            access: AccessPolicy {
                readers: vec![SubjectPattern::Path("/user/admin".parse().unwrap())],
                writers: vec![SubjectPattern::Path("/user/admin".parse().unwrap())],
                callers: vec![],
            },
        }])
        .unwrap()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::RuntimeOptions;
    use forest_protocol::{
        DispatchError,
        service::{ConfigKey, HttpRequest, ServiceError},
    };
    use futures::SinkExt;
    use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};

    #[tokio::test]
    async fn administrator_updates_are_atomic_persistent_and_change_real_io_limits() {
        let temp = tempfile::tempdir().unwrap();
        let db = temp.path().join("config.sqlite3");
        let options = RuntimeOptions {
            data_directory: temp.path().join("data"),
            ..RuntimeOptions::default()
        };
        let store = ConfigStore::open(&db).await.unwrap();
        let scheduler = Scheduler::with_store(store.clone(), options.clone())
            .await
            .unwrap();
        let settings = scheduler.io_settings();
        assert_eq!(settings.get().await.unwrap(), IoLimits::default());
        let key = ConfigKey {
            object: RUNTIME_OBJECT.parse().unwrap(),
            key: IO_KEY.parse().unwrap(),
        };
        for subject in ["/user/viewer", "/module/media"] {
            assert_eq!(
                scheduler
                    .config_writer(subject.parse().unwrap())
                    .check(&key),
                Err(DispatchError::Denied)
            );
        }
        let writer = scheduler.config_writer("/user/admin".parse().unwrap());
        let update = |limits: &IoLimits| ConfigSet {
            target: key.clone(),
            value: limits.as_value(),
        };
        let small = IoLimits {
            http_response_mib: 1,
            file_read_mib: 1,
            websocket_message_mib: 2,
        };
        writer.set(update(&small)).await.unwrap();
        assert_eq!(settings.get().await.unwrap(), small);
        let invalid = IoLimits {
            file_read_mib: 0,
            ..small.clone()
        };
        assert!(matches!(
            writer.set(update(&invalid)).await,
            Err(ConfigWriteError::InvalidSetting(_))
        ));
        assert_eq!(settings.get().await.unwrap(), small);
        assert_eq!(
            writer.check(&ConfigKey {
                key: "/absent".parse().unwrap(),
                ..key.clone()
            }),
            Err(DispatchError::Denied)
        );

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let payload = vec![0x5a; 20 * MIB];
        let response = payload.clone();
        let server = tokio::spawn(async move {
            for _ in 0..2 {
                let (stream, _) = listener.accept().await.unwrap();
                let mut request = BufReader::new(stream);
                let mut line = String::new();
                while !line.ends_with("\r\n\r\n") {
                    assert!(request.read_line(&mut line).await.unwrap() > 0);
                }
                let mut stream = request.into_inner();
                let header = format!(
                    "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                    response.len()
                );
                // The small-limit client intentionally closes during this write.
                if stream.write_all(header.as_bytes()).await.is_ok() {
                    let _ = stream.write_all(&response).await;
                }
            }
        });
        let request = || HttpRequest {
            method: "GET".into(),
            url: format!("http://{address}"),
            headers: vec![],
            body: vec![],
        };
        assert!(matches!(
            scheduler.core.services.http(request()).await,
            Err(ServiceError::Transport(_))
        ));
        let resources = scheduler
            .core
            .services
            .mount(&forest_scheduler_probe::manifest(
                "/module/probe-a".parse().unwrap(),
            ))
            .await
            .unwrap();
        resources.files.write("large.bin", &payload).unwrap();
        assert!(matches!(
            resources.file_read("large.bin".into()).await,
            Err(ServiceError::Storage(_))
        ));

        writer.set(update(&IoLimits::default())).await.unwrap();
        assert_eq!(
            scheduler.core.services.http(request()).await.unwrap().body,
            payload
        );
        assert_eq!(
            resources.file_read("large.bin".into()).await.unwrap(),
            payload
        );
        server.await.unwrap();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let response = payload.clone();
        let server = tokio::spawn(async move {
            for _ in 0..2 {
                let (stream, _) = listener.accept().await.unwrap();
                let mut ws = tokio_tungstenite::accept_async(stream).await.unwrap();
                // The small-limit client rejects the frame and closes the socket.
                let _ = ws
                    .send(tokio_tungstenite::tungstenite::Message::Binary(
                        response.clone().into(),
                    ))
                    .await;
            }
        });
        for websocket_message_mib in [2, 64] {
            writer
                .set(update(&IoLimits {
                    websocket_message_mib,
                    ..IoLimits::default()
                }))
                .await
                .unwrap();
            let handle = resources
                .ws_connect(forest_protocol::service::WebSocketRequest {
                    url: format!("ws://{address}"),
                    headers: vec![],
                })
                .await
                .unwrap();
            let result = resources.ws_receive(handle).await;
            if websocket_message_mib == 2 {
                assert!(matches!(result, Err(ServiceError::Transport(_))));
            } else {
                let forest_protocol::service::WebSocketEvent::Message(
                    forest_protocol::service::WebSocketMessage::Binary(body),
                ) = result.unwrap()
                else {
                    panic!("expected binary payload")
                };
                assert_eq!(body, payload);
            }
            let _ = resources.ws_close(handle).await;
        }
        server.await.unwrap();
        let custom = IoLimits {
            http_response_mib: 96,
            websocket_message_mib: 80,
            file_read_mib: 96,
        };
        writer.set(update(&custom)).await.unwrap();
        drop(resources);
        drop(settings);
        drop(scheduler);
        store.close().await.unwrap();
        let reopened = Scheduler::new(&db, options).await.unwrap();
        assert_eq!(reopened.io_settings().get().await.unwrap(), custom);
        assert!(
            reopened
                .inspector()
                .snapshot()
                .unwrap()
                .hosts
                .iter()
                .any(|host| host.object == key.object)
        );
    }
}
