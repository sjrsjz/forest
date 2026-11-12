# 宿主 API

[返回目录](README.md)

## 1. Rust SDK 入口

函数来自 `forest_sdk::host`。请求和结果结构来自 `forest_protocol::service`。下表省略返回 Operation 与 async fn 的语法差别，均表示 await 后的结果：

| 函数 | 参数 | await 后结果 |
| --- | --- | --- |
| `call<I, O>` | object、interface、`&I` | `DispatchResult<O>` |
| `send<I>` | object、interface、`&I` | `DispatchResult<()>` |
| `emit<I>` | event、&I、可选 subject | DispatchResult<Vec<EventDelivery>> |
| `ws_watch` | handle、接口 Token | HostResult<()> |
| `timer_after` | 毫秒 u64、接口 Token | HostResult<u64> |
| `random_bytes` | 长度 u32 | HostResult<Vec<u8>> |
| `monotonic_now` | 无 | DispatchResult<u64>，单调毫秒 |
| `config_get<T>` | object、key | `HostResult<T>` |
| `config_set<T>` | object、key、`&T` | `HostResult<()>` |
| `config_reset` | object、key | `HostResult<()>` |
| `http_request` | HttpRequest | `HostResult<HttpResponse>` |
| `ws_connect` | WebSocketRequest | `HostResult<u64>` |
| `ws_send` | WebSocketSend | `HostResult<()>` |
| `ws_receive` | handle | `HostResult<WebSocketEvent>` |
| `ws_close` | handle | `HostResult<()>` |
| `file_read` | 相对路径 String | `HostResult<Vec<u8>>` |
| `file_write` | 相对路径 String、bytes | `HostResult<()>` |
| `file_remove` | 相对路径 String | `HostResult<()>` |
| `sqlite` | `Vec<SqlStatement>` | `HostResult<Vec<SqlRows>>` |
| `sleep` | 毫秒 i64 | `DispatchResult<()>` |
| `now` | 无 | `DispatchResult<i64>`，Unix 毫秒 |
| `log` | LogLevel、文本 | `DispatchResult<()>` |

`DispatchResult<T> = Result<T, DispatchError>`；`HostResult<T> = DispatchResult<Result<T, ServiceError>>`。HostError 可接收这两层错误，但每个模块仍可定义自己的错误枚举。

这些专用宿主操作不需要写成 Manifest 的 uses。uses 用于按对象/接口寻址的 send/call，包括调用 `/module/onebot`。生命周期限制和资源所有权依然适用，见[能力表](03-lifecycle-and-scheduling.md#3-普通接口与-destroy)。

## 2. HTTP

```rust
use forest_protocol::service::HttpRequest;

let response = host::http_request(HttpRequest {
    method: "GET".into(),
    url: "https://example.org/data".into(),
    headers: vec![("Accept".into(), "application/json".into())],
    body: vec![],
    timeout_ms: Some(30_000),
}).await??;
```

HttpResponse 包含 `status: u16`、`headers: Vec<(String, String)>` 和 `body: Vec<u8>`。HTTP 400/500 是响应状态，模块应自行解释；网络失败等通过 ServiceError 返回。宿主不替模块理解模型、搜索或数据库 API，也不自动重试请求。

当前宿主不自动跟随 HTTP 重定向，不隐式继承系统代理；代理由宿主的显式运行选项决定。需要重定向或其他协议行为时，由使用该接口的模块按自己的协议处理响应。

endpoint、凭据、模型选择等运行参数适合通过所属模块配置读取。其他模块调用一个 Provider 时，应使用该 Provider 的接口，而不是被迫逐次传递连接凭据。

## 3. WebSocket

```rust
use forest_protocol::service::{WebSocketRequest, WebSocketSend, WebSocketMessage, WebSocketEvent};

let handle = host::ws_connect(WebSocketRequest {
    url: "wss://example.org/events".into(),
    headers: vec![],
    timeout_ms: Some(10_000),
}).await??;
host::ws_send(WebSocketSend {
    handle, message: WebSocketMessage::Text("hello".into()), timeout_ms: Some(10_000),
}).await??;
let event: WebSocketEvent = host::ws_receive(handle).await??;
host::ws_close(handle).await??;
```

消息支持 Text 和 Binary；接收结果为 Message 或 Closed。同一个连接已有接收者时，再次并发 receive 返回 InUse。收发可以由模块安排成不同的异步路径，但句柄属于当前实例，不能转交其他模块或在重载后复用。

模块可以主动 close；实例释放时宿主回收其连接。自动重连、认证续期和重复消息处理属于使用该连接的模块协议。

## 4. 文件、SQLite 与上限

配置、私有数据库与文件用法见[存储章节](05-configuration-and-storage.md)。文件写入会创建所需父目录，但只接受模块私有目录内的相对路径。

宿主 I/O 上限由配置对象 `/core/runtime` 的 `/io` 管理：

| 字段 | 默认值 | 生效时机 |
| --- | --- | --- |
| `http_response_mib` | 64 MiB | 新 HTTP 操作开始时 |
| `websocket_message_mib` | 64 MiB | 建连时固定，约束消息及帧 |
| `file_read_mib` | 64 MiB | 新读取开始时 |

范围为 1–4095 MiB；修改需要管理员写权限。已有 HTTP/文件操作继续使用开始时的上限，已有 WS 连接继续使用建连时的上限。模块不能通过请求参数自行提高宿主上限。

这些限制、Wasm 实例内存配额、模块请求队列额度和 ABI 帧长度是不同边界。传输能表示大载荷，不代表实例有足够内存同时保留输入、临时副本和输出。媒体引用可以避免将同一份大二进制反复嵌入多个接口结果。

## 5. 日志与时间

```rust
use forest_protocol::service::LogLevel;

let started = host::now().await?;
host::log(LogLevel::Info, "index batch accepted").await?;
host::sleep(100).await?;
```

日志来源由宿主附加实际模块 Token，不接受模块伪造来源。日志用于诊断和观察，不作为通信回执或状态权威。凭据、完整请求头和大块媒体不适合写入日志。

sleep 参数不能为负数。它提供可等待的计时操作，不引入自动取消整个 Agent 或模块的超时策略。

## 6. OneBot 接入

群消息通过 Wasm 包事件 `(/module/onebot, /group_message)` 接收。平台操作通过同一对象的普通接口调用：

| 接口 | 输入 | 模块输出 |
| --- | --- | --- |
| `/send_group_message` | SendGroupMessage | `Result<SentMessage, OneBotError>` |
| `/send_private_message` | SendPrivateMessage | `Result<SentMessage, OneBotError>` |
| `/query` | Query | `Result<String, OneBotError>`，String 为查询数据 JSON |

Query 当前包含账号、好友、群、群成员及陌生人查询的固定分支。完整类型位于 [forest-onebot 的 Forest 契约](../../plugins/onebot/protocol/src/forest.rs)。Rust v11 客户端库具有的动作不等于全部已经暴露为宿主接口；Wasm 只能调用已注册且满足 uses/ACL 的入口。

原生消息可以使用 OneBot 的类型化 Segment。需要 Forest Markdown、受管媒体或长消息转发策略时，应使用相应输出模块公开的契约，见[Markdown 协议](../33-markdown-messages.md)和[消息输出](../30-message-output.md)。这些能力不由普通 OneBot 文本发送自动执行。

## 7. 作为 Agent 工具提供能力

普通模块接口不会自动出现在模型 tools 列表。现有工具网关通过自己的 Invocation、返回值契约和路由配置接入工具包；工具模块声明 callers，网关也要声明目标 uses。修改运行配置不能凭空增加 Manifest 未声明的依赖。

需要让 Agent 使用新能力时，可按[Agent 工具配置](../22-agent-guide.md)增加网关路由，并用相同契约重建相关包。模块的独立接口仍可保持类型明确；Agent 适配只负责将模型参数转换成该能力所需的数据。

宿主错误和资源协议的完整定义见 [service.rs](../../crates/forest-protocol/src/service.rs)，SDK 签名见 [host.rs](../../crates/forest-sdk/src/host.rs)。

## 7. 期限、资源通知与随机

HTTP 和 WS connect/send 的 timeout_ms 为可选相对期限，None 不设期限。HTTP 从执行到完整读取 body 共用一个期限，超时返回 ServiceError::Timeout，不承诺撤销对端副作用。重试由调用方决定。

ws_watch 绑定当前实例的 IoEvent 接口，其 callers 允许 /system/io；每连接最多缓冲一条结果，处理器收到 WebSocketReady 后调用 ws_receive 取走才继续收。timer_after 同样投递一次实例通知。create/destroy 禁止这两项操作和 emit，发布后激活可以使用。

random_bytes 使用操作系统安全随机源。monotonic_now 只比较当前进程内间隔，不能跨重启作为持久时刻。
