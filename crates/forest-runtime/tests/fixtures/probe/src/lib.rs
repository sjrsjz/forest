//! Test-only Wasm fixture exercising SDK and lifecycle contracts.
use forest_protocol::{manifest::callers, service::*, *};
use forest_sdk::{AsSchema, AsValue, CallContext, CreateContext, Handlers, Module, Token, host};
use futures::{StreamExt, stream::FuturesUnordered};
use std::{cell::Cell, rc::Rc};

pub struct Probe {
    object: Token,
    counter: Cell<i64>,
    serial: Cell<bool>,
    sequence: Cell<i64>,
}
impl Module for Probe {
    async fn create(context: CreateContext) -> Result<Self, String> {
        let denied: DispatchResult<Result<i64, String>> = host::call(
            "/core/probe".parse().unwrap(),
            "/observe".parse().unwrap(),
            &-999i64,
        )
        .await;
        assert!(matches!(denied, Err(DispatchError::LifecycleRestricted)));
        assert!(matches!(
            host::send(
                "/core/probe".parse().unwrap(),
                "/observe".parse().unwrap(),
                &-999i64
            )
            .await,
            Err(DispatchError::LifecycleRestricted)
        ));
        let counter =
            flatten(host::config_get(context.object.clone(), "/counter".parse().unwrap()).await)?;
        Ok(Self {
            object: context.object,
            counter: Cell::new(counter),
            serial: Cell::new(false),
            sequence: Cell::new(0),
        })
    }
    fn handlers() -> Handlers<Self> {
        fn policy() -> AccessPolicy {
            callers(vec![SubjectPattern::Path("/user/test".parse().unwrap())])
        }
        Handlers::new()
            .with("/echo".parse().unwrap(), true, policy(), Self::echo)
            .with("/serial".parse().unwrap(), false, policy(), Self::serial)
            .with("/parallel".parse().unwrap(), true, policy(), Self::parallel)
            .with("/nested".parse().unwrap(), true, policy(), Self::nested)
            .with(
                "/increment".parse().unwrap(),
                false,
                policy(),
                Self::increment,
            )
            .with(
                "/implementation".parse().unwrap(),
                true,
                policy(),
                Self::implementation,
            )
            .with(
                "/destroyed".parse().unwrap(),
                true,
                policy(),
                Self::destroyed,
            )
            .with("/trap".parse().unwrap(), true, policy(), Self::trap)
            .with("/spin".parse().unwrap(), true, policy(), Self::spin)
            .with("/file".parse().unwrap(), true, policy(), Self::file)
            .with("/http".parse().unwrap(), true, policy(), Self::http)
            .with("/ws".parse().unwrap(), true, policy(), Self::ws)
            .with(
                "/atomic-write".parse().unwrap(),
                false,
                policy(),
                Self::atomic_write,
            )
    }
    async fn destroy(self: Rc<Self>) -> Result<(), String> {
        let denied: DispatchResult<Result<i64, String>> = host::call(
            "/core/probe".parse().unwrap(),
            "/observe".parse().unwrap(),
            &-999i64,
        )
        .await;
        assert!(matches!(denied, Err(DispatchError::LifecycleRestricted)));
        let count: i64 =
            flatten(host::config_get(self.object.clone(), "/destroyed".parse().unwrap()).await)?;
        flatten(
            host::config_set(
                self.object.clone(),
                "/destroyed".parse().unwrap(),
                &(count + 1),
            )
            .await,
        )
    }
}
forest_sdk::export_module!(super::Probe);

impl Probe {
    async fn echo(self: Rc<Self>, _: CallContext, input: String) -> Result<String, String> {
        Ok(input)
    }
    async fn observe(value: i64) -> Result<(), String> {
        let response: Result<i64, String> = host::call(
            "/core/probe".parse().unwrap(),
            "/observe".parse().unwrap(),
            &value,
        )
        .await
        .map_err(|e| e.to_string())?;
        response?;
        Ok(())
    }
    async fn serial(self: Rc<Self>, _: CallContext, delay: i64) -> Result<i64, String> {
        assert!(
            !self.serial.replace(true),
            "non-reentrant interface overlapped"
        );
        let value = self.sequence.get() + 1;
        self.sequence.set(value);
        Self::observe(value).await?;
        host::sleep(delay).await.map_err(|e| e.to_string())?;
        Self::observe(-value).await?;
        self.serial.set(false);
        Ok(value)
    }
    async fn parallel(self: Rc<Self>, _: CallContext, delay: i64) -> Result<i64, String> {
        let value = self.sequence.get() + 100;
        self.sequence.set(value);
        Self::observe(value).await?;
        host::sleep(delay).await.map_err(|e| e.to_string())?;
        Self::observe(-value).await?;
        Ok(value)
    }
    async fn nested(self: Rc<Self>, _: CallContext, _: ()) -> Result<i64, String> {
        let mut first = true;
        std::future::poll_fn(move |cx| {
            if first {
                first = false;
                cx.waker().wake_by_ref();
                std::task::Poll::Pending
            } else {
                std::task::Poll::Ready(())
            }
        })
        .await;
        let mut futures = FuturesUnordered::new();
        for delay in [3i64, 7, 1] {
            futures.push(async move {
                host::sleep(delay).await.unwrap();
                delay
            });
        }
        let mut total = 0;
        while let Some(value) = futures.next().await {
            total += value;
        }
        Ok(total)
    }
    async fn increment(self: Rc<Self>, _: CallContext, _: ()) -> Result<i64, String> {
        let value = self.counter.get() + 1;
        flatten(host::config_set(self.object.clone(), "/counter".parse().unwrap(), &value).await)?;
        self.counter.set(value);
        Ok(value)
    }
    async fn implementation(self: Rc<Self>, _: CallContext, _: ()) -> Result<i64, String> {
        Ok(if cfg!(feature = "second") { 2 } else { 1 })
    }
    async fn destroyed(self: Rc<Self>, _: CallContext, _: ()) -> Result<i64, String> {
        flatten(host::config_get(self.object.clone(), "/destroyed".parse().unwrap()).await)
    }
    async fn trap(self: Rc<Self>, _: CallContext, _: ()) -> Result<(), String> {
        panic!("intentional Wasm trap")
    }
    async fn spin(self: Rc<Self>, _: CallContext, _: ()) -> Result<(), String> {
        Self::observe(999).await?;
        loop {
            std::hint::black_box(1);
        }
    }
    async fn file(self: Rc<Self>, _: CallContext, path: String) -> Result<String, String> {
        flatten(host::file_write(path.clone(), b"managed".to_vec()).await)?;
        let bytes = flatten(host::file_read(path.clone()).await)?;
        flatten(host::file_remove(path).await)?;
        String::from_utf8(bytes).map_err(|e| e.to_string())
    }
    async fn http(self: Rc<Self>, _: CallContext, url: String) -> Result<String, String> {
        let response = flatten(
            host::http_request(HttpRequest {
                method: "GET".into(),
                url,
                headers: vec![],
                body: vec![],
            })
            .await,
        )?;
        Ok(format!(
            "{}:{}",
            response.status,
            String::from_utf8(response.body).map_err(|e| e.to_string())?
        ))
    }
    async fn ws(self: Rc<Self>, _: CallContext, url: String) -> Result<String, String> {
        let socket = flatten(
            host::ws_connect(WebSocketRequest {
                url,
                headers: vec![],
            })
            .await,
        )?;
        flatten(host::ws_send(socket, WebSocketMessage::Text("probe".into())).await)?;
        let event = flatten(host::ws_receive(socket).await)?;
        flatten(host::ws_close(socket).await)?;
        match event {
            WebSocketEvent::Message(WebSocketMessage::Text(text)) => Ok(text),
            _ => Err("unexpected WS response".into()),
        }
    }
    async fn atomic_write(self: Rc<Self>, _: CallContext, value: i64) -> Result<(), String> {
        let mut write = Box::pin(host::config_set(
            self.object.clone(),
            "/counter".parse().unwrap(),
            &value,
        ));
        std::future::poll_fn(|cx| {
            let result = std::future::Future::poll(write.as_mut(), cx);
            assert!(result.is_pending(), "test must hold SQLite worker");
            std::task::Poll::Ready(())
        })
        .await;
        Self::observe(700).await?;
        flatten(write.await)
    }
}
fn flatten<T>(result: HostResult<T>) -> Result<T, String> {
    result
        .map_err(|e| e.to_string())?
        .map_err(|e| e.to_string())
}

pub fn manifest(object: Token) -> Manifest {
    let configs = ["/counter", "/destroyed"]
        .map(|key| ConfigDeclaration {
            key: key.parse().unwrap(),
            schema: i64::as_schema(),
            default: 0i64.as_value(),
            access: AccessPolicy {
                readers: vec![SubjectPattern::SelfModule],
                writers: vec![SubjectPattern::SelfModule],
                callers: vec![],
            },
        })
        .to_vec();
    Manifest {
        package: PackageInfo {
            id: object,
            version: if cfg!(feature = "second") {
                "2".into()
            } else {
                "1".into()
            },
            name: Some("Scheduler test fixture".into()),
            description: None,
        },
        module: "module.wasm".into(),
        configs,
        provides: Probe::handlers().declarations(),
        subscriptions: vec![],
        uses: vec![InterfaceUse {
            object: "/core/probe".parse().unwrap(),
            interface: "/observe".parse().unwrap(),
            input: i64::as_schema(),
            output: <Result<i64, String>>::as_schema(),
        }],
    }
}
