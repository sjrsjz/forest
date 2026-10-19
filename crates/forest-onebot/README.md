# forest-onebot

独立的 OneBot v11 API 库，提供全部 38 个公开 action 的请求类型、关联响应类型和异步客户端。默认不包含网络运行时；启用 `http` 使用原生 HTTP(S) 传输。

```toml
forest-onebot = { path = "crates/forest-onebot", features = ["http"] }
```

```rust
use forest_onebot::{Client, Outcome, action::GetLoginInfo, http::HttpTransport};

# async fn example() -> Result<(), Box<dyn std::error::Error>> {
let endpoint = "http://127.0.0.1:5700/".parse()?;
let client = Client::new(HttpTransport::new(endpoint, None)?);
match client.call(&GetLoginInfo {}).await? {
    Outcome::Completed(info) => println!("{}", info.nickname),
    Outcome::Submitted => println!("已提交，最终结果未知"),
}
# Ok(())
# }
```

发送消息时构造 `SendPrivateMsg`、`SendGroupMsg` 或 `SendMsg`。`Message` 接受 CQ 字符串、类型化消息段数组、单个消息段。纯文本可用 `Segment::text`；图片文件引用用 `Segment::image`，SDK 不自动读取、上传或下载它。字符串的 `auto_escape` 语义沿用标准。

`Client::call` 使用正常 action；`call_with_mode` 可显式选择 `CallMode::Async` 或 `RateLimited`。Rust 的 `.await` 与 OneBot 的 `_async` 是不同概念：前者等待响应，后者通常只取得提交确认。`Outcome::Submitted` 不证明动作执行成功。

`CallError` 区分传输、编码、解码、协议和远端 API 错误。HTTP 非 200 保留状态码与响应体；远端失败保留 retcode 和可选诊断字段。库不自动重试、不创建隐式后台任务、不设置固定调用超时。

同一客户端可以并发调用；连接池由 `HttpTransport` 持有。默认协议层可编译到 Wasm，自定义 `Transport` 后续可接入 Forest 的宿主调用。`http` 是原生传输，不代表已经具备 Wasm 网络导入接口。

`HttpTransport::new` 沿用环境代理配置。需要直连本地服务、设置代理或自定义 TLS 根证书时，用 `with_client` 注入自行构造的 Reqwest 客户端；此时重试、重定向与超时策略由调用方明确配置。本地模拟服务测试显式使用 `.no_proxy()`，不受机器全局代理影响。

只读示例（需要自己运行的 OneBot HTTP 服务）：设置 `ONEBOT_HTTP_URL` 和可选 `ONEBOT_ACCESS_TOKEN`，运行 `cargo run -p forest-onebot --features http --example inspect`。它只查询登录信息和群列表，不发送消息或执行群管理。

本次不包含 WS/反向 WS、事件上报服务器、CQ 解析器、实现扩展或 Forest 授权逻辑。评估记录与标准链接见[设计文档](../../doc/04-onebot-v11.md)。
