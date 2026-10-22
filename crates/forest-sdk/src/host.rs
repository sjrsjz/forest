use crate::Operation;
use forest_protocol::{AsValue, DispatchResult, Token, Value, service::*};

pub(crate) mod imports {
    macro_rules! operations {
        ($($name:ident),* $(,)?) => {
            #[cfg(target_arch = "wasm32")]
            #[link(wasm_import_module = "forest_host")]
            unsafe extern "C" { $(pub fn $name(ptr: u32, len: u32) -> u64;)* }
            $(#[cfg(not(target_arch = "wasm32"))]
              pub unsafe extern "C" fn $name(_: u32, _: u32) -> u64 { panic!("guest SDK requires Wasm") })*
        };
    }
    operations!(
        call,
        send,
        config_get,
        config_set,
        config_reset,
        http_request,
        ws_connect,
        ws_send,
        ws_receive,
        ws_close,
        file_read,
        file_write,
        file_remove,
        sqlite,
        sleep,
        log,
        now
    );

    #[cfg(target_arch = "wasm32")]
    #[link(wasm_import_module = "forest_host")]
    unsafe extern "C" {
        pub fn operation_status(id: u64) -> i64;
        pub fn operation_take(id: u64, ptr: u32, len: u32);
        pub fn operation_release(id: u64);
        pub fn task_wake(id: u64);
    }
    #[cfg(not(target_arch = "wasm32"))]
    pub unsafe fn operation_status(_: u64) -> i64 {
        panic!("guest SDK requires Wasm")
    }
    #[cfg(not(target_arch = "wasm32"))]
    pub unsafe fn operation_take(_: u64, _: u32, _: u32) {
        panic!("guest SDK requires Wasm")
    }
    #[cfg(not(target_arch = "wasm32"))]
    pub unsafe fn operation_release(_: u64) {
        panic!("guest SDK requires Wasm")
    }
    #[cfg(not(target_arch = "wasm32"))]
    pub unsafe fn task_wake(_: u64) {
        panic!("guest SDK requires Wasm")
    }
}

pub async fn call<I: AsValue, O: AsValue>(
    object: Token,
    interface: Token,
    input: &I,
) -> DispatchResult<O> {
    let request = CallRequest {
        object,
        interface,
        input: input.as_value(),
    };
    let value: DispatchResult<Value> = Operation::new(imports::call, &request).await;
    O::from_value(value?).map_err(|_| forest_protocol::DispatchError::OutputContract)
}

pub fn send<I: AsValue>(
    object: Token,
    interface: Token,
    input: &I,
) -> Operation<DispatchResult<()>> {
    Operation::new(
        imports::send,
        &CallRequest {
            object,
            interface,
            input: input.as_value(),
        },
    )
}

pub async fn config_get<T: AsValue>(object: Token, key: Token) -> HostResult<T> {
    let result: HostResult<Value> =
        Operation::new(imports::config_get, &ConfigKey { object, key }).await;
    result.map(|value| {
        value.and_then(|value| {
            T::from_value(value).map_err(|e| ServiceError::InvalidInput(e.to_string()))
        })
    })
}

pub fn config_set<T: AsValue>(object: Token, key: Token, value: &T) -> Operation<HostResult<()>> {
    Operation::new(
        imports::config_set,
        &ConfigSet {
            target: ConfigKey { object, key },
            value: value.as_value(),
        },
    )
}
pub fn config_reset(object: Token, key: Token) -> Operation<HostResult<()>> {
    Operation::new(imports::config_reset, &ConfigKey { object, key })
}
pub fn http_request(request: HttpRequest) -> Operation<HostResult<HttpResponse>> {
    Operation::new(imports::http_request, &request)
}
pub fn ws_connect(request: WebSocketRequest) -> Operation<HostResult<u64>> {
    Operation::new(imports::ws_connect, &request)
}
pub fn ws_send(handle: u64, message: WebSocketMessage) -> Operation<HostResult<()>> {
    Operation::new(imports::ws_send, &WebSocketSend { handle, message })
}
pub fn ws_receive(handle: u64) -> Operation<HostResult<WebSocketEvent>> {
    Operation::new(imports::ws_receive, &handle)
}
pub fn ws_close(handle: u64) -> Operation<HostResult<()>> {
    Operation::new(imports::ws_close, &handle)
}
pub fn file_read(path: String) -> Operation<HostResult<Vec<u8>>> {
    Operation::new(imports::file_read, &path)
}
pub fn file_write(path: String, bytes: Vec<u8>) -> Operation<HostResult<()>> {
    Operation::new(imports::file_write, &FileWrite { path, bytes })
}
pub fn file_remove(path: String) -> Operation<HostResult<()>> {
    Operation::new(imports::file_remove, &path)
}
/// Executes the complete list atomically against the executing module's private DB.
/// Each item is exactly one parameterized statement. Transactions never span awaits.
pub fn sqlite(statements: Vec<SqlStatement>) -> Operation<HostResult<Vec<SqlRows>>> {
    Operation::new(imports::sqlite, &statements)
}
pub fn sleep(milliseconds: i64) -> Operation<DispatchResult<()>> {
    Operation::new(imports::sleep, &milliseconds)
}
pub fn now() -> Operation<DispatchResult<i64>> {
    Operation::new(imports::now, &())
}
pub fn log(level: LogLevel, message: impl Into<String>) -> Operation<DispatchResult<()>> {
    Operation::new(
        imports::log,
        &LogRecord {
            level,
            message: message.into(),
        },
    )
}
