use super::*;

impl Probe {
    pub(super) async fn activated(self: Rc<Self>, _: CallContext, _: ()) -> Result<(), String> {
        Self::observe(600).await?;
        Self::observe(601).await
    }

    pub(super) async fn entropy_clock(
        self: Rc<Self>,
        _: CallContext,
        _: (),
    ) -> Result<(Vec<u8>, i64, i64), String> {
        let before = host::monotonic_now().await.map_err(|e| e.to_string())?;
        let bytes = flatten(host::random_bytes(32).await)?;
        host::sleep(3).await.map_err(|e| e.to_string())?;
        let after = host::monotonic_now().await.map_err(|e| e.to_string())?;
        Ok((bytes, before as i64, after as i64))
    }

    pub(super) async fn http_deadline(
        self: Rc<Self>,
        _: CallContext,
        (url, ms): (String, i64),
    ) -> Result<bool, String> {
        let response = host::http_request(HttpRequest {
            method: "GET".into(),
            url,
            headers: vec![],
            body: vec![],
            timeout_ms: Some(ms as u64),
        })
        .await
        .map_err(|e| e.to_string())?;
        Ok(matches!(response, Err(ServiceError::Timeout)))
    }

    pub(super) async fn watch(self: Rc<Self>, _: CallContext, url: String) -> Result<i64, String> {
        let socket = flatten(
            host::ws_connect(WebSocketRequest {
                url,
                headers: vec![],
                timeout_ms: Some(1000),
            })
            .await,
        )?;
        flatten(host::ws_watch(socket, "/io".parse().unwrap()).await)?;
        Ok(socket as i64)
    }

    pub(super) async fn timer(self: Rc<Self>, _: CallContext, ms: i64) -> Result<i64, String> {
        flatten(host::timer_after(ms as u64, "/io".parse().unwrap()).await).map(|id| id as i64)
    }

    pub(super) async fn io(self: Rc<Self>, _: CallContext, event: IoEvent) -> Result<(), String> {
        match event {
            IoEvent::Timer(_) => Self::observe(800).await,
            IoEvent::WebSocketReady(handle) => {
                let event = flatten(host::ws_receive(handle as u64).await)?;
                match event {
                    WebSocketEvent::Message(WebSocketMessage::Text(text)) => {
                        Self::observe(text.parse().map_err(|e| format!("{e}"))?).await
                    }
                    WebSocketEvent::Message(WebSocketMessage::Binary(bytes)) => {
                        Self::observe(bytes.len() as i64).await
                    }
                    WebSocketEvent::Closed { .. } => Self::observe(801).await,
                }
            }
        }
    }

    pub(super) async fn publish(
        self: Rc<Self>,
        _: CallContext,
        (text, subject): (String, Option<Token>),
    ) -> Result<Vec<bool>, String> {
        host::emit("/sample".parse().unwrap(), &text, subject)
            .await
            .map(|r| r.into_iter().map(|d| d.result.is_ok()).collect())
            .map_err(|e| e.to_string())
    }

    pub(super) async fn on_event(
        self: Rc<Self>,
        context: CallContext,
        text: String,
    ) -> Result<(), String> {
        flatten(
            host::config_set(
                self.object.clone(),
                "/last-event".parse().unwrap(),
                &(context.origin, context.caller, text),
            )
            .await,
        )?;
        Self::observe(900).await
    }
}
