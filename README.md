# Forest

Rust + Wasm Bot 微内核。当前已实现调度器核心、基础 SDK、配置 ACL、专用 I/O 和 OneBot 接入。模块用外部 Manifest 声明单例身份、配置、接口、依赖及宿主事件订阅；独立 Wasm 内存中的异步任务由宿主调度，支持完整代码替换。

当前设计面向单个 Bot 账号，可处理多个群聊；不预设多 Bot 实体、账号路由或按 Bot 挂载配置。

已提供 Vue + TypeScript [宿主调试台](doc/14-inspection-webui.md)：实时模块状态、声明依赖图、配置查看与 ACL 鉴权编辑、滚动日志，以及本地包加载、热重载和卸载。OneBot 连接中断会自动重连，模块与会话保持原实例。构建与启动方法见该文档。

```text
crates/
  forest-serde/          Schema、Value、类型转换与文本语法
  forest-serde-macros/   从 Rust 类型生成 Schema 与 Value 转换
  forest-util/           通用值类型与纯操作，包含 token 和 acl 模块
  forest-onebot/         类型化 OneBot v11 API，可选异步 HTTP(S) 传输
  forest-config/         宿主 SQLite 配置表，必填默认值、异步原子更新与可读 JSON
  forest-protocol/       Manifest、调度错误、ABI 和宿主操作数据
  forest-sdk/            类型化处理器、来宾 Future 与固定导出
  forest-runtime/        Wasmtime、调度、生命周期、ACL、I/O、notifier
  forest-webui/          可选 Inspector HTTP/SSE、受限模块管理与管理员会话
  forest-control/        WebUI 共用包目录与显式授权的宿主管理端口
  forest-cli/            可执行宿主与 OneBot HTTP/WebSocket 接入
webui/                  Vue、TypeScript、Mermaid 前端，构建产物为 dist
plugins/forest-test/     #forest-test 示例模块与打包程序
plugins/qwen-vl-embedding/ 2048 维多模态向量供应 Wasm 模块
plugins/openai-provider/  OpenAI 兼容 Chat Completions Wasm 模块
plugins/context/          可配置 Agent 提示词的上下文格式化 Wasm 模块
plugins/memory/           原文、媒体、跨群联想与派生向量索引 Wasm 模块
plugins/agent/            每群工作循环、持续点与短期窗口 Wasm 模块
plugins/agent-tools/      原生工具声明与显式模块调用网关 Wasm 模块
plugins/deepseek-web/     DeepSeek 搜索与 Markdown 网页抓取 Wasm 插件
plugins/wolfram-alpha/    Wolfram Alpha LLM API 精确计算 Wasm 插件
plugins/typst/            中文、公式与文档排版 Wasm 插件
plugins/media/            独立受管图片存储 Wasm 插件
plugins/shell/            按用户与群授权的 Forest 管理命令 Wasm 插件
plugins/npucraft/         学校 MC 服务器地图与帮助回复 Wasm 插件
packages/forest-test/    生成的外部 Manifest 与 module.wasm
data/                   默认运行数据，不进入 Git
```

插件的实现、提示词和 Rust 类型映射均留在各自的 `plugins/` 包中。模块间通过固定 Wasm ABI 传递 Forest Value，以 Manifest 的 Schema 为契约；不依赖对方的 Rust crate。打包同时输出可独立读取的 Schema 语言文本，其他语言可按相同契约实现模块。

```sh
rustup target add wasm32-unknown-unknown
cargo run -p forest-test --bin package --locked
cargo run -p forest-cli --locked -- --events-ws ws://127.0.0.1:8080 --allow-group 123456789
cargo test --workspace --all-features --locked
```

序列化语义、修正范围和当前限制见 [序列化说明](doc/serialization.md)。令牌类型、身份边界与通配符规则见 [来源令牌设计](doc/03-token.md)。插件包、模块通信、状态表、ACL 与热重载的设计见 [模块运行时架构](doc/01-module-runtime.md)和[状态与访问详细设计](doc/02-state-access-and-reload.md)。

地址与群号均为示例。网关需要令牌时，启动前设置 `FOREST_ONEBOT_TOKEN` 环境变量；凭据不放入命令行。模块 Serving 后，在允许群发送 `#forest-test`，回复 `hello world`。默认包和数据都位于仓库根目录下，不使用 `.local`。更多启动方式、SDK 和热重载 API 见[运行与 ABI](doc/13-runtime-and-sdk.md)。

上述 `--allow-group` 是可选的宿主测试限制。正常 Agent 启动省略此参数，在 WebUI 的 Agent 配置表中管理 `allowed_groups`，保存并重载 Agent 即可生效。

OneBot 使用方式见 [API 库说明](crates/forest-onebot/README.md)，选型和职责边界见 [OneBot v11 设计](doc/04-onebot-v11.md)。事件沿用 v11 原始结构，经 notifier 转发给模块；HTTP 与双向 WS 调用均可使用。

配置表的声明、类型化访问、原子更新、SQLite 存储和序列化边界见 [配置表实现](doc/05-config-table.md)。

配置与接口在声明上挂静态 AccessPolicy，宿主绑定真实来源并检查；配置使用 readers/writers，接口 send/call 使用 callers，不建立动态 SQL ACL 表。内存表与只读 `check` 已接入宿主边界，不存在的对象/资源键对一律返回 `false`，见[访问声明](doc/06-config-access.md)。

包的外部静态声明见[模块 Manifest 详细设计](doc/07-module-manifest.md)：单例对象由 Manifest 声明稳定 Token，调用按 `(对象 Token, 接口路径)` 定位，`uses` 直接声明两端路径，热重载保持调用地址和契约；本阶段不包含 UI 配置和 ABI 版本，自动生成工具另行设计。

模块可用的最小操作见[宿主接口设计](doc/08-host-interfaces.md)：通信、单项配置 `get/set/reset`、HTTP、WebSocket、受管文件、日志与时间。上述接口已实现；模块侧不引入事务回调或自动冲突处理。

[调度设计](doc/09-scheduling.md)明确同一实例独占推进、时间片被动让出保留执行权、模块主动让出释放执行权，以及接口 reentrant 的排队与恢复规则；已由运行时和 SDK 实现，本阶段不设计死锁处理或接口数值并发上限。

[生命周期设计](doc/10-module-lifecycle.md)明确 RAII、create 禁止 send/call、Drain/AtYield/Force 三种卸载方式、destroy 宿主操作白名单、静态 notifier 注册和 trap 后不可调度的处理。所有方式保持对象地址，非排空方式允许中断旧请求，不迁移连接或执行状态。

[宿主事件 notifier 与静态依赖设计](doc/11-notifier-and-dependencies.md)明确宿主产生事件，Manifest 只用 subscriptions 声明接收入口，调度器按注册表投递；模块没有事件发布接口。模块加载依赖与宿主事件支持分开检查，不提供运行时订阅增删接口。

[调度器状态机与所有权](doc/12-scheduler-state-machine.md)细化六个模块阶段、Idle/Advancing 执行权、接口许可、请求额度、一次性结果出口及唤醒规则。不变量依靠状态构造和所有权转移保持；集成测试使用真实 Wasm 验证并发、抢占、热重载与 SQLite 写入交接。Agent、Memory 与工具网关均为独立 Wasm 包，构建与使用见[Agent 实现与启动](doc/22-agent-implementation.md)。

旧 Python 实现及旧设计文档位于 `archive/python-2026-10-19`，最后快照为 `2d560a69`。其他旧分支及 worktree 快照分别保存在 `archive/master-2026-10-19`、`archive/control-payloads-2026-10-19`、`archive/shell-contracts-2026-10-19`。旧文档不再指导新主线实现。

文档与公式可通过 `typst_render` 工具生成 PNG；工具只返回受管媒体引用，Agent 在正文中使用返回的 CQ 图片码发送。见 [文档渲染与受管媒体](doc/25-document-rendering-proposal.md)。

群聊管理命令以 `#forest` 开头，授权与命令见[群聊 Shell 与学校帮助](doc/26-group-shell-and-school-help.md)。Shell 与 NPUCraft 为独立可热重载的 Wasm 包，可在 WebUI 本地包页面加载。
