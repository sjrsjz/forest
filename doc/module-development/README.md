# 模块开发手册

本手册面向插件作者，说明如何从接口设计到构建、装载和热重载完成一个 Forest 模块。内容以当前 Rust SDK、宿主接口和 TOML 包实现为准；底层设计的推导过程见[架构文档](../README.md)。

## 阅读路径

| 文档 | 解决的问题 |
| --- | --- |
| [快速开始](01-quickstart.md) | 构建一个能响应群聊命令的完整模块，修改配置并重载 |
| [类型与接口契约](02-contracts.md) | Schema/Value、Rust 类型映射、类型化错误、跨语言契约 |
| [生命周期与并发](03-lifecycle-and-scheduling.md) | create/destroy、共享内存、重入、主动与被动让出 |
| [通信、事件与权限](04-communication-and-permissions.md) | send/call、uses、宿主事件、可信来源和 ACL |
| [配置与数据存储](05-configuration-and-storage.md) | 默认值、原子性、私有 SQLite、受管文件和状态恢复 |
| [宿主 API](06-host-api.md) | SDK 函数、HTTP、WebSocket、日志、时间和 OneBot |
| [打包与热重载](07-packaging-and-reload.md) | TOML、文本引用、SHA-256、兼容性、三种停止方式 |
| [调试与其他语言](08-debugging-and-other-languages.md) | 定位失败、检查实际状态、实现非 Rust SDK 的入口 |

第一次编写模块时，先完成快速开始，再阅读生命周期、通信和存储。实现公共服务时，重点阅读类型契约及热重载兼容性。实现其他语言的模块时，先明确相同的 Schema/Value 和 ABI，再选择语言绑定。

## 基本对象

| 名称 | 含义 |
| --- | --- |
| 包 | `manifest.toml`、Wasm、引用的 Schema/Value 文本和静态资源 |
| 模块对象 | 由 `package.id` 标识的单例，拥有稳定地址、配置和私有数据 |
| 执行实例 | 当前代码对应的 Wasm 内存与任务；重载时被替换 |
| 接口 | 模块内的一条路径，声明输入和完整 `Result<T, E>` 输出 |
| 调用任务 | 一次已开始的接口执行，可以挂起并在之后恢复 |
| 宿主操作 | 任务提交的一次通信、配置、I/O 或计时操作 |

完整调用地址由两个独立 Token 组成，例如 `(/module/example-hello, /greet)`。接口路径不需要包含对象前缀，文件名和 Rust 函数名也不构成接口地址。

一个逻辑模块只有一个对外活动实现。热重载保持模块对象和数据归属，替换其执行实例；不需要调用方重新寻找实例 ID。

## 最小职责边界

- **宿主**负责接纳、调度、来源绑定、生命周期、Schema 校验、ACL 和受管资源。
- **模块**负责自己的功能、状态、数据结构、错误以及跨异步边界的数据协调。
- **SDK**负责 Rust 类型到公开契约的映射、来宾 Future 和固定 ABI 导出。
- **包工具**负责将已声明的契约写成外部文件并计算摘要，不参与在线调度。

模块不链接 `forest-runtime` 或其他插件的实现。公共 `.schema` 是语言无关的接口契约；Rust 类型可以在各模块内独立映射。`forest-onebot` 等协议类型库可以辅助实现，但不赋予宿主访问权。

## 完整示例

[examples/hello](examples/hello/Cargo.toml) 是独立 Cargo 工程，包含：

- [模块实现](examples/hello/src/lib.rs)：群事件入口、公开 `/greet` 接口、类型化错误和配置声明。
- [打包程序](examples/hello/src/bin/package.rs)：编译 Wasm，生成 TOML、Schema/Value 及文件摘要。
- 独立的 `Cargo.lock`，用于固定该示例的依赖。

示例与当前仓库 SDK 配套，运行不需要模型服务、Agent 或向量数据库。所有群号、地址和凭据示例均为占位信息。
