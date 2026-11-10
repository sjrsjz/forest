# OpenAI 兼容 Chat Completions Wasm 模块

模块源代码为 `plugins/openai-provider`，包身份 `/module/openai-provider`。接口通过 Manifest 的 Schema 和 Forest Value 描述；本插件的 Rust 类型映射位于 `src/types.rs`。调用方可复制所需类型源码或使用其他语言实现相同 Schema，不依赖 Provider 的 Rust crate。endpoint、凭据、模型和能力声明属于模块配置；调用者不在每次请求中指定这些设置。

## 1. 边界

这是普通 Wasm 插件，使用现有配置与完整 HTTP 请求能力，没有新增宿主接口。Provider 负责协议转换、HTTP 调用及响应解码；Context 负责提示词、内容格式化及多模态投影；Agent 负责工作循环、会话窗口协调及工具执行。

模块使用 Chat Completions 的完整响应模式，固定 `stream=false`。宿主 HTTP 返回完整 body；接口支持文本、图片输入及原生 function 工具，不包含 Responses、音频输入输出或自定义类型工具。

## 2. 调用契约

| 接口 | 输入 | 返回 |
|---|---|---|
| `/capabilities` | `()` | `Result<Capabilities, Error>` |
| `/complete` | `Request` | `Result<Completion, Error>` |

```rust
struct Request {
    messages: Vec<Message>,
    tools: Vec<Tool>,
    tool_choice: Option<ToolChoice>,
}
```

Message 是明确的 System、Developer、User、Assistant、Tool 枚举，发送时序列化为原生 `role` 结构。User 与 Tool 的内容都可以是文字或有序的 text/image_url 内容块，图片地址支持服务可读 URL 或临时 data URL。Provider 不获取受管媒体文件；调用者完成资源解析后提供合法图片输入。工具图片由 Context 投影，Provider 仅按原内容编码，不更改角色或插入 User 消息。

Assistant 同时保留正文、多个原生 function tool_calls、refusal 与兼容服务的 reasoning_content。工具调用伴随正文时，两者均返回，不因为有工具调用就丢弃正文。Provider 不发送群消息、不运行工具，也不决定 reasoning 是否展示。

ToolCall 保存原始调用 ID、函数名与 arguments JSON 字符串。Provider 不把错误 arguments 改写成成功参数；工具执行器负责解释与验证，并由 Agent 决定后续反馈。工具返回使用 `role=tool` 和相应 `tool_call_id`；调用者可直接把上轮 Assistant 与工具结果追加到下一次 Request。

Tool 声明含 name、description、parameters_json 和 strict。parameters_json 在协议边界解析为 JSON Schema 内容并嵌入请求对象，不作为双重编码的字符串传给 API。Provider 不实现 JSON Schema 验证器。ToolChoice 支持 auto、none、required 和指定函数。

Completion 返回服务给出的 ID、实际模型名、创建时间、choices 和 usage。每个 choice 保留 Assistant 及 stop/length/tool_calls/content_filter 结束原因；length 或 refusal 不被改写成完整成功回答。模型别名解析成不同的实际名称是有效响应，不要求返回的模型字符串与配置别名逐字相等。

Usage 返回输入、输出和总 token，及服务提供的 cached/reasoning token 明细；没有相应明细时保留缺失。计数只作返回数据，不参与上下文预算判断。

## 3. 配置与能力

模块在 create 时读取自身 `/settings`。本地管理配置的 JSON 表示如下：

```json
{
  "endpoint": "https://api.openai.com/v1/chat/completions",
  "api_key": "<本地设置>",
  "model": "<选择的模型>",
  "multimodal": false,
  "output_limit": "ProviderDefault",
  "reasoning_effort": null
}
```

endpoint 是完整 API 地址，Provider 不猜测或拼接供应商路径。默认模型与凭据为空；模块可以加载并公布能力，实际生成调用返回 NotConfigured，直到管理员配置并重载。

output_limit 是显式枚举：ProviderDefault 不发输出限制；CompletionTokens(n) 发 max_completion_tokens；Tokens(n) 发 max_tokens。两个字段不会同时产生，不在 API 拒绝后自动换字段重试。它控制模型输出，与调用方的输入预算、估算或裁剪无关。reasoning_effort 在管理员设置时原样发送，不根据模型名猜测支持情况。

配置仅模块自身可读、`/user/admin` 可写；已授权的宿主调试台仍可查看全配置。接口允许 `/module/**` 和 `/user/admin` 调用。Manifest 默认值不含凭据。

当前 multimodal 明确表示文本加图片输入能力，默认关闭。`/capabilities` 返回配置模型名和该开关，Context 据此选择投影。开关不让 Provider 偷删图片、插入提示词或重新组织历史；未按能力投影的请求仍按原消息转交 API。该能力不表示已经实现音频或图像输出。

Settings 是实例内不可变值。更改配置后通过已有热重载生效，旧调用保留旧连接与模型设置，新实例读取新配置。运行时继续沿用普通 Drain/AtYield/Force，没有新的任务目录或 Provider 专属生命周期。

管理端选择本模块 → 配置表 → `/settings` → 编辑，即可通过 Schema 表单修改上述字段。`output_limit` 先选择 ProviderDefault、CompletionTokens 或 Tokens；后两项填写整数上限。`reasoning_effort` 勾选自定义时填写供应商支持的值，取消勾选则不发送该参数。保存后显式重载本模块生效，不需要编辑配置文件。

## 4. 错误

外层仍是调度器的 DispatchResult，内层是 Provider 的 Result。模块 Error 区分 NotConfigured、Host、Http、ToolSchema 与 Response。错误字符串仅作诊断，不用于分发或判断恢复分支。

HTTP 失败保留状态码、可用的 x-request-id 及服务端错误响应正文 `body`，使调用者能区分具体拒绝原因；正文作为第三方协议诊断文本保留，不参与错误分发。请求正文和 Authorization 不加入错误对象。不自动重试、不缩减输入后重发、不为 API 报错生成虚构正文。成功响应只在边界解码一次，之后使用类型化角色、工具和结果对象。

## 5. 构建与验证

```powershell
cargo run --offline --locked -p forest-openai-provider --bin package-openai-provider
cargo test --offline --locked -p forest-openai-provider --test wasm
```

生成包位于 `packages/openai-provider`。`manifest.toml` 从相同处理器注册表生成，引用 `generated/provides/` 下的输入、输出 Schema 和 `generated/configs/` 下的配置 Schema/Value，供独立读取 Forest 文本契约；引用文件均纳入 SHA-256 索引。Wasm 工件忽略提交。管理台可以发现并加载该包；配置表中的连接设置可经 `/user/admin` 来源和 writers 鉴权后编辑，保存后显式重载 Provider 使用新连接，见[宿主调试台](14-inspection-webui.md)。

隔离在线验证程序读取上述 Settings JSON，加载实际 Wasm 包，以两次模型调用完成一个本地 echo 工具往返：

```powershell
cargo run --offline --locked -p forest-openai-provider --example verify-openai-provider -- <settings.json>
```

它不接入 OneBot、不执行框架工具，只检验原生 function 调用、arguments、工具结果与 Assistant 历史回传。配置文件放在忽略提交的位置，不把密钥作为命令行参数。

Wasm 集成测试覆盖角色与图片块映射、Schema 对象嵌入、工具选择、正文和多个工具调用并存、reasoning 保留、usage、模型别名、ACL、错误 arguments、HTTP 错误，以及配置修改后 Drain 重载的请求交接。多模态投影由 Context 的测试单独验证。

## 6. 协议参考

接口对应 [Chat Completions](https://platform.openai.com/docs/api-reference/chat/create) 的消息和工具字段。`reasoning_content` 及 Tool 图片内容块等兼容扩展取决于实际服务能力，参见 [DeepSeek Chat Completions](https://api-docs.deepseek.com/zh-cn/api/create-chat-completion/)。Provider 保留这些字段，由调用者维护完整的 Assistant/Tool 历史和有效顺序。
