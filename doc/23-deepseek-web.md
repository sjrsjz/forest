# DeepSeek 搜索与网页抓取插件

`plugins/deepseek-web` 是独立 Wasm 包，对象为 `/module/deepseek-web`。它经宿主 HTTP 调用 DeepSeek，持有自己的连接配置。Agent、Context、Provider 和内核均不解释搜索或抓取协议。

## 工具路由

| 工具名 | 对象与接口 | 用途 |
|---|---|---|
| `web_search` | `/module/deepseek-web`、`/search` | 默认联网搜索，返回标题、URL、摘要及来源元数据 |
| `web_fetch` | `/module/deepseek-web`、`/fetch` | 默认读取网页，返回原 URL、标题和 Markdown 正文 |
| `http_request` | `/module/agent-tools`、`/http` | 保留通用 HTTP/API 操作，返回原始响应 |

网关通过工具说明引导模型优先搜索，再抓取选定页面。HTTP 工具说明明确指出网页研究与阅读应使用前两者；不将网页读取静默降级为原始 HTTP。

两个接口沿用网关的 `Invocation` 和 `Result<String, Error>` Schema，类型映射由插件自己持有。`arguments_json` 只解析对应接口的参数，不参与选择宿主能力或目标模块。接口 ACL 允许网关和受信管理员。

## API 与输出

按 [Search API](https://api-docs.deepseek.com/zh-cn/api/search/)发送 `POST /v1/tools/search`。参数包括 query、count、ori_question、localization、need_content。默认不请求整页正文，直接使用供应商返回的搜索摘要；需要时显式开启 need_content。

按 [Fetch API](https://api-docs.deepseek.com/zh-cn/api/fetch/)发送 `POST /v1/tools/fetch`，参数为 url。将返回的 Markdown、标题与请求的原始 URL 一起交给模型，避免丢失来源；不再返回网页 HTML、脚本和样式。

参数在 JSON 边界按官方约束检查，响应解码为明确的数据结构。失败返回工具错误，保留 HTTP 状态及可用 request ID；不输出鉴权信息或原始错误响应。每次调用只请求一次，不自动重试、换站或转交原始 HTTP。目标站点限制仍可能使抓取失败，见[官方指南](https://api-docs.deepseek.com/zh-cn/guides/search/)。

## 配置与部署

插件声明 `/connection`：

```json
{"endpoint":"https://api.deepseek.com/v1","api_key":"<本地设置>"}
```

endpoint 是包含 `/v1` 的 API 前缀。配置仅插件自身可读，管理员可写；空凭据允许装载，但调用返回未配置错误。修改后按普通模块热重载生效，不复用 Provider 的运行时状态或内部对象。

```powershell
cargo run --locked --offline -j 2 -p forest-deepseek-web --bin package-deepseek-web
cargo run --locked --offline -j 2 -p forest-agent-tools --bin package-agent-tools
```

Agent 构建脚本现包含此包，默认工具网关 Manifest 声明对它的静态依赖。初次设置可用 Agent 配置程序的 `FOREST_DEEPSEEK_KEY` 与可选 `FOREST_DEEPSEEK_ENDPOINT`，独立于聊天模型凭据。

## 验证

Agent 的实际 Wasm 联调通过网关调用本插件，核对鉴权、请求路径、搜索默认正文开关、Markdown 和来源返回、Unicode 参数边界、HTTP 错误、无重试及原始 HTTP 工具保留。

在线验证读取已有配置库，将连接复制进临时调度器，分别执行一次真实搜索和抓取，不连接 OneBot、不输出正文或凭据：

```powershell
cargo run --locked --offline -j 2 -p forest-deepseek-web --example verify -- data/agent/config.sqlite3
```

2026-10-22 的真实 API 验证：搜索返回 3 条结果，耗时 332 ms；抓取返回 6405 字符的 Markdown，耗时 94 ms。耗时仅代表本次样本，不作为服务延迟保证。

运行实例已启用四条工具路由：`web_search`、`web_fetch`、`http_request`、`memory_read`。本次同时验证了宿主装载同 Schema 的新默认值时保留当前配置，未归档会话经正常 destroy 保存后恢复为静默。
