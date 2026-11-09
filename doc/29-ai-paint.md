# AI 绘画

`/module/ai-paint` 是独立、可热重载的 Wasm 包，提供 `/generate`。Agent 通过工具网关的 `image_generate` 调用它，支持 SwarmUI、Nano-Banana 与 SiliconFlow 绘图后端。

调用参数：

| 参数 | 含义 |
|---|---|
| `prompt` | 绘画描述，1–8000 字符 |
| `style` | `anime`、`photo`、`none`，默认 `anime` |
| `size` | `portrait`、`landscape`、`square`，默认 `square` |
| `seed` | 可选随机种子；SiliconFlow 和 SwarmUI 支持，OpenAI Images 路径明确拒绝此参数 |

一次调用生成一张图片。模型、endpoint、凭据、画质及步数由管理员配置，Agent 不能在工具参数中更换。每次只调用选定后端，不自动切换供应商或重试生成。失败或中断不表示供应商没有计费，工具说明明确要求模型不要自动重试。

## 配置

在 WebUI 配置表选择 `/module/ai-paint`，修改后热重载该包：

| 路径 | 内容 |
|---|---|
| `/backend` | `SiliconFlow`、`OpenAiImages` 或 `SwarmUi`，默认 `SiliconFlow` |
| `/siliconflow` | 完整生成 endpoint、api_key、model、steps，以及三种版式的尺寸字符串 |
| `/openai-images` | 完整生成 endpoint、api_key、model、quality，以及三种版式的 size 字符串 |
| `/swarm-ui` | 服务根地址、可选 auth_token、各风格模型与负面提示词、steps、三种版式的尺寸 |

三个后端分别保留配置；切换 `/backend` 不覆盖另一个后端的凭据和模型。配置沿用静态 ACL：仅自身读取，受信 `/user/admin` 写入。

SiliconFlow 默认 `Kwai-Kolors/Kolors`、20 步，像素尺寸为 `720x1440`、`1440x720` 和 `1024x1024`。Nano-Banana 路径默认 `nano-banana-2`，使用 `9x16`、`16x9` 和 `1x1` 比例以及 `high` 画质。切换其他模型时，管理员应同时按该模型协议调整尺寸、画质和步数。

SwarmUI 为每次生成获取 session，发送 `images=1`、`donotsave=true`。结果支持 data URI 或相对/绝对图片 URL。可选 auth_token 只作为 SwarmUI API 的 `swarm_token` Cookie 使用。

## 媒体与发送

生成模块接收供应商结果，下载或解码为图片字节，通过 Media 的 `/put` 持久化。媒体写入 ACL 明确允许 `/module/ai-paint`，不扩大为任意模块写入。下载图片时不携带云端 API 的 Authorization；图片重定向与供应商调用分别处理。

返回值只含 `reference`、`mime` 与 `markdown`：

```json
{
  "reference": "media:<sha256>",
  "mime": "image/png",
  "markdown": "![generated image](media:<sha256>)"
}
```

模型可原样复制 `markdown` 到普通 assistant 回复，或交给 OneBot 独立发送工具。绘图工具本身不发送群消息。供应商 URL、Base64、凭据和 SwarmUI session 不进入工具结果；图片引用可跨模块热重载继续读取，也能用于 Typst 的受管图片输入。文档、公式和表格排版仍使用 Typst 工具。

## 构建与安装

```powershell
cargo run --locked --offline -j 2 -p forest-ai-paint --bin package-ai-paint
cargo run --locked --offline -j 2 -p forest-media --bin package-media
cargo run --locked --offline -j 2 -p forest-agent-tools --bin package-agent-tools
```

已有实例需更新并重载 Media，加载 AI 绘画包，通过管理端 `set` 将新路由追加到网关 `/routes`，然后重载网关。默认值更新保留已有当前值，不覆盖管理员的其他工具。宿主与 SDK 无需新增接口。

协议依据为 [Nano-Banana 接口说明](https://www.openai-hk.com/docs/en/openai/nano-banana.html)、[SiliconFlow 绘图 API](https://api-docs.siliconflow.cn/docs/api/images-generations-post)和 [SwarmUI API](https://github.com/mcmonkeyprojects/SwarmUI/blob/master/docs/API.md)。三种适配、受管媒体持久化、无凭据下载、错误不重试及切换后端由实际 Wasm 集成验证覆盖。
