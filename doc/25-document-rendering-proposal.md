# 文档渲染与受管媒体

Typst 通过独立 Wasm 包渲染，独立 Media 包持久保存图片。Agent 工具只返回媒体引用。Markdown/HTML 浏览器渲染尚未引入。

## 调用与所有权

```text
Agent → 工具网关 → /module/typst /render
                         → /module/media /put → media:<sha256>
Agent → 正文中的 CQ 图片引用 → Delivery → /module/media /get → OneBot
Memory → /module/media /register → 缓存接收到的聊天附件
```

排版属于插件。宿主提供既有的原子 SQLite、HTTP、调度与 ACL；Typst 不链接调度器、Agent、Memory 或 Media 的实现。包通过 Manifest 的 uses/provides 与公开 Schema 通信，各自保留本地 Rust 类型映射。

- Typst 持有编译器、字体及本次调用的不可变虚拟资源，生成 PNG。它没有群消息订阅，也不发送 OneBot 消息。
- Media 持有私有 SQLite `media` 表：引用、原始来源、MIME、图片 BLOB 和获取错误。发布成功后字节已持久化，不依赖渲染器生命周期。
- Memory 持有原始消息、消息与附件的关联、向量和图关系。入站附件由 Memory 声明稳定身份和原始来源，Media 负责获取、缓存。
- Delivery 将已知引用解析成图片字节，只在 OneBot 传输边界生成 `base64://`。工具文本、原始 assistant 文本和 CQ 向量化文本只保存引用。对支持视觉的模型/嵌入接口，Memory 的 API 投影才生成临时 data URL。

## Media 接口

| 接口 | 输入 | 返回 | 调用方 |
|---|---|---|---|
| `/put` | `{mime: string, bytes: bytes}` | `Result<string, Error>` | Typst、管理员 |
| `/register` | `{reference: string, source: string}` | `Result<(), Error>` | Memory、管理员 |
| `/get` | `string` 引用 | `Result<Blob, Error>` | 模块、管理员 |
| `/acquire` | `()` | `Result<(), Error>` | 模块自身 |

`/put` 根据带域分隔符的 MIME 与字节摘要生成 `media:<sha256>`，重复内容复用同一引用。声明、缓存与发布均是参数化单事务操作。`/register` 接纳附件身份并唤醒本模块的 `/acquire`，消息接收链不等待下载。获取失败写入错误，后台不反复重试；显式 `/get` 可以再次尝试。同一引用的并发获取共用互斥锁，其他引用仍可推进。

`bytes` 是 Forest Schema 中的变长不透明字节类型，对应 `Value::Bytes` 和 Rust `Vec<u8>`；原来的 `bytes N` 表示定长字节，是 `bytes` 的子类型。图片不会展开成逐字节的 ADT 数组，也不必以字符串中转。

Media 当前保留所有已发布对象，尚无删除或自动清理接口。Typst 的热重载、卸载不删除媒体。原始入站消息由 Memory 保存，附件缓存可以从其原始来源重新建立，不增加旧缓存文件的迁移机制。

## Typst 工具

对象 `/module/typst`，接口 `/render`，调用许可为工具网关和管理员。输入、返回及错误沿用网关的 Invocation / `Result<string, Error>` Schema，排版源码及可选资源在 arguments_json 内声明：

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
  "pages": [{"page":1,"reference":"media:<sha256>","width":960,"height":300,"cq":"[CQ:image,file=media:<sha256>]"}],
  "warnings": []
}
```

Agent 将返回的 `cq` 放进正常 assistant 正文即可发送。编译失败作为工具错误返回 diagnostics，包含原始源码中的行、列、消息和建议；警告随成功结果返回。框架不猜测、改写或自动重新编译。调用完成后清空编译缓存；渲染接口不可重入，同一实例依次处理文档，计算时间片仍由既有调度器控制。

使用 Typst/typst-render 0.15.1，内置 typst-assets 字体和 OFL 授权的 Noto Sans CJK SC 2.004，无需宿主安装字体或浏览器。字体来源、摘要及许可证见插件 assets 目录。字体与引擎均编入包，通过 Wasm 热重载替换。

## 构建和验证

`scripts/build-agent.ps1` 构建全部十个包。Typst 使用 `guest` 优化 profile，其他包沿用现有构建方式；首次构建须获取锁文件中的依赖，此后可离线构建。

集成验证使用实际 Wasm，覆盖中文、公式、表格、多页、编译诊断、输出尺寸限制、显式图片资源、Typst/Media 重载后的持久引用，以及 Agent 工具回合到 OneBot 图片发送。模型 API 和 OneBot 发送端替换为本地模拟端，图片生成和引用读取不模拟。设置 `FOREST_TYPST_PREVIEW` 可将首张 PNG 保留用于目视检查。

本机示例两页渲染及持久发布约 92 ms，首张 PNG 为 960×317、21576 字节。当前 Wasm 包约 60 MiB；调试版宿主首次编译整个包明显慢于渲染，十包集成验证的总耗时约 364 秒。渲染延迟不包含首次包编译，应分别衡量。

## 后续引擎

Markdown、LaTeX、Typst 是不同语法；当前工具要求 Typst，不进行字符串式转换。未来可用独立 Wasm 转换器，或由独立插件请求浏览器 HTTP 服务完成 HTML/CSS 栅格化，然后复用 Media `/put`。这不要求内核加载 Chromium，也不要求新的任务数据库、轮询协议或调度系统。

实现依据：[Typst World](https://docs.rs/typst/0.15.1/typst/trait.World.html)、[typst-render](https://docs.rs/typst-render/0.15.1/typst_render/)。
