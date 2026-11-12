# 文档渲染与受管媒体

Typst 通过独立 Wasm 包渲染，Media 包持久保存图片。Agent 工具返回受管媒体引用及 Markdown 图片链接，发送时才解析图片字节。[Markdown 模块](33-markdown-messages.md)也使用 Typst 渲染表格、行间公式与代码块，其余内容保留为文本。

## 调用与所有权

```text
Agent → 工具网关 → /module/typst /render
                         → /module/media /put → media:<sha256>
Agent → 正文中的 Markdown 图片链接 → Message Output → /module/media /get → OneBot
Message Output → Markdown /format → Typst /typeset → Media /put
Memory → /module/media /register → 缓存接收到的聊天附件
群消息 → /module/typst-command → Typst /render → Message Output /send → OneBot
```

排版属于插件。宿主提供既有的原子 SQLite、HTTP、调度与 ACL；Typst 不链接调度器、Agent、Memory 或 Media 的实现。包通过 Manifest 的 uses/provides 与公开 Schema 通信，各自保留本地 Rust 类型映射。

- Typst 持有编译器、字体及本次调用的不可变虚拟资源，生成 PNG。它没有群消息订阅，也不发送 OneBot 消息。
- Media 持有私有 SQLite `media` 表：引用、原始来源、MIME、图片 BLOB 和获取错误。发布成功后字节已持久化，不依赖渲染器生命周期。
- Memory 持有原始消息、消息与附件的关联、向量和图关系。入站附件由 Memory 声明稳定身份和原始来源，Media 负责获取、缓存。
- Message Output 将已知引用解析成图片字节，只在 OneBot 传输边界生成 `base64://`。工具文本、Assistant 正文和向量化文本只保存引用；模型图片输入使用调用期生成的 data URL。

## Media 接口

| 接口 | 输入 | 返回 | 调用方 |
|---|---|---|---|
| `/put` | `{mime: string, bytes: bytes}` | `Result<string, Error>` | 已授权的媒体生产模块、管理员 |
| `/register` | `{reference: string, source: string}` | `Result<(), Error>` | Memory、管理员 |
| `/get` | `string` 引用 | `Result<Blob, Error>` | 模块、管理员 |
| `/acquire` | `()` | `Result<(), Error>` | 模块自身 |

`/put` 根据带域分隔符的 MIME 与字节摘要生成 `media:<sha256>`，重复内容复用同一引用。声明、缓存与发布均是参数化单事务操作。`/register` 接纳附件身份并唤醒本模块的 `/acquire`，消息接收链不等待下载。获取失败写入错误，后台不反复重试；显式 `/get` 可以再次尝试。同一引用的并发获取共用互斥锁，其他引用仍可推进。

`bytes` 是 Forest Schema 中的变长不透明字节类型，对应 `Value::Bytes` 和 Rust `Vec<u8>`；`bytes N` 表示定长字节，是 `bytes` 的子类型。图片不会展开成逐字节的 ADT 数组。

Media 保留所有已发布对象，不提供删除或自动清理接口。Typst 的热重载、卸载不删除媒体。原始入站消息由 Memory 保存，附件可在原始来源仍有效时重新获取。

## Typst 工具

对象 `/module/typst`，接口 `/render`，调用许可为工具网关、Typst 命令插件和管理员。输入、返回及错误沿用网关的 Invocation / `Result<string, Error>` Schema，排版源码及可选资源在 arguments_json 内声明：

```json
{
  "source": "= 积分结果\n$ integral_0^pi x^2 sin(x) dif x = pi^2 - 4 $",
  "images": {"plot.png": "media:<sha256>"}
}
```

`images` 可省略；提供时，源码可用 `#image("plot.png")`。这些引用先经 Media 读取成字节，再构造同步 World，编译回调没有宿主 I/O。虚拟资源只包含主源码和显式图片，不访问任意宿主路径、下载 Typst 包或隐式提供当前日期。

默认版式为宽 480pt、高度自适应、边距 16pt、12pt 字号、每点 2 像素。三行前置 set 规则给出默认版式，调用者可在源码中覆盖。源码上限 100000 UTF-8 字节。管理员可修改 `/settings` 并重载生效：

| 配置 | 默认 | 允许范围 |
|---|---:|---|
| page_width_pt | 480 | 120–1440 |
| font_size_pt | 12 | 6–48 |
| pixels_per_point | 2 | 1–3 |
| max_pages | 8 | 1–32 |
| max_page_pixels | 8000000 | 1–16000000 |

栅格化、发布之前整体检查页面数量及像素分配，防止文档意外生成超大画布。页面按顺序输出 PNG：

```json
{
  "pages": [{"page":1,"reference":"media:<sha256>","width":960,"height":300,"markdown":"![page 1](media:<sha256>)"}],
  "warnings": []
}
```

Agent 将返回的 `markdown` 放进正常 assistant 正文即可发送。编译失败作为工具错误返回 diagnostics，包含原始源码中的行、列、消息和建议；警告随成功结果返回。框架不猜测、改写或自动重新编译。调用完成后清空编译缓存；渲染和排版入口共同获取 `/document` 宿主锁，同一实例依次处理文档，计算时间片仍由既有调度器控制。另提供 `/typeset: string -> Result<Vec<string>, Error>` 给独立 Markdown 包，沿用相同编译、像素限制和媒体发布流程，不经过工具网关或模型。

使用 Typst/typst-render 0.15.1，内置 typst-assets 字体和 OFL 授权的 Noto Sans CJK SC 2.004，无需宿主安装字体或浏览器。字体来源、摘要及许可证见插件 assets 目录。字体与引擎均编入包，通过 Wasm 热重载替换。

## 群内快速渲染

独立命令包 `/module/typst-command` 订阅群消息。直接发送以下文本即可，不必 @ Bot，也不调用 Agent 或模型 API：

````text
/typst
= 公式示例
$ integral_0^1 x^2 dif x = 1/3 $
````

命令后的源码也可以使用完整的 `typst`、`typ` 或无语言标记的三反引号代码块包裹。单独发送 `/typst` 返回使用示例；`/typstfoo` 等其他词不会触发。命令读取解码后的文本消息段，保留源码中的方括号、`&` 和换行；允许命令前的回复和 @ Bot 段，不把其他消息段当作 Typst 源码。

渲染成功后，命令插件将每页受管图片引用按顺序交给 Message Output，在一条回复中发送。编译失败直接返回诊断，不让 Agent 修改或重试源码。Typst 仍只负责渲染，不订阅群消息；命令插件不链接渲染器、Media 或输出插件的实现，依赖由 Manifest 静态声明。

命令包的 `/settings.allowed_groups` 默认为空，表示所有群均可使用；填入群列表可限制范围，保存后重载生效。宿主显式启用的测试白名单仍然有效。

`cargo run --locked --offline -p forest-typst-command --bin package-typst-command` 构建命令包。通过 WebUI 加载 `typst-command`，或将 `--package packages/typst-command` 添加为启动根包；依赖闭包会加载 Typst、Media 和 Message Output。systemd 服务模板已包含此启动根包。

## 构建和验证

`scripts/build-agent.ps1` 构建全部插件包。Typst 使用 `guest` 优化 profile，其他包沿用现有构建方式；首次构建须获取锁文件中的依赖，此后可离线构建。

集成验证使用实际 Wasm，覆盖中文、公式、表格、多页、编译诊断、输出尺寸限制、显式图片资源、Typst/Media 重载后的持久引用，以及 Agent 工具回合到 OneBot 图片发送。模型 API 和 OneBot 发送端替换为本地模拟端，图片生成和引用读取不模拟。设置 `FOREST_TYPST_PREVIEW` 可将首张 PNG 保留用于目视检查。

## Markdown 排版边界

`typst_render` 接受 Typst 源码。普通 Assistant 正文由 Markdown 模块解析成语法节点，仅将表格、独立行间公式及代码块转换为 Typst。公式使用 MiTeX 转换；标题、列表、引用、正文和行内样式不单独生成图片。无法排版的节点保留原始文本，渲染不要求引入浏览器。

首次加载包含字体与编译器的 Wasm 包需要编译，后续启动可复用宿主的持久化缓存。性能测量应分别统计加载、排版、图片发布与发送耗时。

实现依据：[Typst World](https://docs.rs/typst/0.15.1/typst/trait.World.html)、[typst-render](https://docs.rs/typst-render/0.15.1/typst_render/)。
