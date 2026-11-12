use super::*;
use forest_protocol::service::{LogLevel, WebSocketEvent, WebSocketMessage, WebSocketRequest};
use serde::Deserialize;

impl Adapter {
    pub(super) async fn activated(
        self: Rc<Self>,
        _: CallContext,
        _: (),
    ) -> Result<(), OneBotError> {
        self.connect().await
    }

    async fn connect(&self) -> Result<(), OneBotError> {
        let headers = if self.settings.token.is_empty() {
            vec![]
        } else {
            vec![(
                "authorization".into(),
                format!("Bearer {}", self.settings.token),
            )]
        };
        match host::ws_connect(WebSocketRequest {
            url: self.settings.websocket.clone(),
            headers,
            timeout_ms: Some(self.settings.connect_timeout_ms as u64),
        })
        .await
        {
            Ok(Ok(handle)) => {
                self.socket.set(Some(handle));
                self.delay.set(self.settings.reconnect_min_ms as u64);
                if let outcome @ (Err(_) | Ok(Err(_))) =
                    host::ws_watch(handle, "/io".parse().unwrap()).await
                {
                    self.socket.set(None);
                    let _ = host::ws_close(handle).await;
                    let _ = host::log(
                        LogLevel::Error,
                        format!("OneBot receive watch failed: {outcome:?}"),
                    )
                    .await;
                    return self.schedule_reconnect().await;
                }
                let _ = host::log(LogLevel::Info, "OneBot connected").await;
                Ok(())
            }
            outcome => {
                let _ = host::log(
                    LogLevel::Warn,
                    format!("OneBot connection failed: {outcome:?}"),
                )
                .await;
                self.schedule_reconnect().await
            }
        }
    }

    pub(super) async fn schedule_reconnect(&self) -> Result<(), OneBotError> {
        let _guard = host::lock("/reconnect".parse().unwrap())
            .await
            .map_err(|e| OneBotError::Transport(e.to_string()))?;
        if self.reconnect.get() {
            return Ok(());
        }
        let delay = self.delay.get();
        self.delay.set(
            delay
                .saturating_mul(2)
                .min(self.settings.reconnect_max_ms as u64),
        );
        let random = host::random_bytes(4)
            .await
            .map_err(|e| OneBotError::Transport(e.to_string()))?
            .map_err(|e| OneBotError::Transport(e.to_string()))?;
        let jitter = u32::from_le_bytes(random.try_into().expect("four requested random bytes"))
            as u64
            % (delay / 4 + 1);
        self.reconnect.set(true);
        let result = host::timer_after(delay + jitter, "/io".parse().unwrap()).await;
        match result {
            Ok(Ok(_)) => Ok(()),
            error => {
                self.reconnect.set(false);
                Err(OneBotError::Transport(format!(
                    "reconnect timer: {error:?}"
                )))
            }
        }
    }

    pub(super) async fn on_io(
        self: Rc<Self>,
        _: CallContext,
        event: IoEvent,
    ) -> Result<(), OneBotError> {
        match event {
            IoEvent::Timer(_) => {
                self.reconnect.set(false);
                self.connect().await
            }
            IoEvent::WebSocketReady(handle) => {
                // Closing a resource does not revoke an already accepted callback.
                if self.socket.get() != Some(handle as u64) {
                    return Ok(());
                }
                let _guard = host::lock("/incoming".parse().unwrap())
                    .await
                    .map_err(|e| OneBotError::Transport(e.to_string()))?;
                match host::ws_receive(handle as u64).await {
                    Ok(Ok(WebSocketEvent::Message(message))) => {
                        let bytes = match message {
                            WebSocketMessage::Text(text) => text.into_bytes(),
                            WebSocketMessage::Binary(bytes) => bytes,
                        };
                        let result = self.frame(bytes).await;
                        if let Err(error) = &result {
                            let _ =
                                host::log(LogLevel::Warn, format!("OneBot input: {error:?}")).await;
                        }
                        result
                    }
                    _ => {
                        self.socket.set(None);
                        for (_, reply) in self.pending.take() {
                            let _ = reply.send(Err(OneBotError::ReceiptLost));
                        }
                        let _ = host::ws_close(handle as u64).await;
                        let _ = host::log(
                            LogLevel::Warn,
                            "OneBot disconnected; submitted actions are not replayed",
                        )
                        .await;
                        self.schedule_reconnect().await
                    }
                }
            }
        }
    }

    async fn frame(&self, bytes: Vec<u8>) -> Result<(), OneBotError> {
        #[derive(Deserialize)]
        struct Header {
            echo: Option<String>,
            post_type: Option<String>,
            message_type: Option<String>,
        }
        let header: Header =
            serde_json::from_slice(&bytes).map_err(|e| OneBotError::Decode(e.to_string()))?;
        if let Some(echo) = header.echo {
            if let Some(reply) = self.pending.borrow_mut().remove(&echo) {
                let _ = reply.send(Ok(bytes));
            }
            return Ok(());
        }
        if header.post_type.as_deref() != Some("message")
            || header.message_type.as_deref() != Some("group")
        {
            return Ok(());
        }
        let message: GroupMessage =
            serde_json::from_slice(&bytes).map_err(|e| OneBotError::Decode(e.to_string()))?;
        if !self.group_allowed(message.group_id) {
            return Ok(());
        }
        let subject = format!("/user/{}", message.user_id).parse().unwrap();
        let deliveries = host::emit(
            contract::GROUP_MESSAGE.parse().unwrap(),
            &message,
            Some(subject),
        )
        .await
        .map_err(|e| OneBotError::Transport(e.to_string()))?;
        for delivery in deliveries {
            if let Err(error) = delivery.result {
                let _ = host::log(
                    LogLevel::Warn,
                    format!(
                        "event delivery {} {}: {error}",
                        delivery.object, delivery.interface
                    ),
                )
                .await;
            }
        }
        Ok(())
    }
}
