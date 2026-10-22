//! Reconnect only the transport. Module instances and submitted actions are not replayed.
use crate::{receive, websocket};
use anyhow::Result;
use forest_protocol::{abi, service::LogLevel};
use forest_runtime::{LogWriter, onebot::OneBotIngress};
use std::time::Duration;
use tokio_tungstenite::tungstenite::{client::IntoClientRequest, protocol::WebSocketConfig};

pub async fn forward(
    url: String,
    token: Option<String>,
    ingress: OneBotIngress,
    mut actions: Option<websocket::Connections>,
    logs: LogWriter,
) -> Result<()> {
    let source: forest_protocol::Token = "/core/onebot".parse().unwrap();
    let mut delay = Duration::from_secs(1);
    loop {
        // A fresh handshake request also gives each connection its own WebSocket nonce.
        let mut request = url.as_str().into_client_request()?;
        if let Some(token) = &token {
            request
                .headers_mut()
                .insert("authorization", format!("Bearer {token}").parse()?);
        }
        let config = WebSocketConfig::default().max_message_size(Some(abi::MAX_FRAME_BYTES));
        match tokio_tungstenite::connect_async_with_config(request, Some(config), false).await {
            Ok((socket, _)) => {
                delay = Duration::from_secs(1);
                logs.write(LogLevel::Info, source.clone(), "OneBot connected");
                let result = match &mut actions {
                    Some(actions) => {
                        websocket::serve(socket, &ingress, actions.connected(), &logs).await
                    }
                    None => receive(socket, &ingress, &logs).await,
                };
                let message = match result {
                    Ok(()) => "OneBot connection closed".to_string(),
                    Err(error) => format!("OneBot connection ended: {error}"),
                };
                logs.write(LogLevel::Warn, source.clone(), message);
            }
            Err(error) => logs.write(
                LogLevel::Warn,
                source.clone(),
                format!("OneBot connection failed: {error}"),
            ),
        }
        logs.write(
            LogLevel::Info,
            source.clone(),
            format!("OneBot reconnecting in {}s", delay.as_secs()),
        );
        tokio::time::sleep(delay).await;
        delay = (delay * 2).min(Duration::from_secs(30));
    }
}
