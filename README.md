# Forest

Rust 新主线。目前实现序列化库、derive 宏、来源令牌路径与通配符匹配、内存 ACL 查询、SQLite 配置存储，以及独立的异步 OneBot v11 API 库。模块运行时的设计讨论记录于 [设计文档](doc/README.md)，尚未进入实现。

当前设计面向单个 Bot 账号，可处理多个群聊；不预设多 Bot 实体、账号路由或按 Bot 挂载配置。

```text
crates/
  forest-serde/          Schema、Value、类型转换与文本语法
  forest-serde-macros/   从 Rust 类型生成 Schema 与 Value 转换
  forest-util/           通用值类型与纯操作，包含 token 和 acl 模块
  forest-onebot/         类型化 OneBot v11 API，可选异步 HTTP(S) 传输
  forest-config/         宿主 SQLite 配置表，必填默认值、异步原子更新与可读 JSON
```

```sh
cargo test --workspace --locked
cargo test -p forest-onebot --features http --locked
cargo check -p forest-serde --target wasm32-unknown-unknown --locked
```

序列化语义、修正范围和当前限制见 [序列化说明](doc/serialization.md)。令牌类型、身份边界与通配符规则见 [来源令牌设计](doc/03-token.md)。插件包、模块通信、状态表、ACL 与热重载的设计见 [模块运行时架构](doc/01-module-runtime.md)和[状态与访问详细设计](doc/02-state-access-and-reload.md)。

OneBot 使用方式见 [API 库说明](crates/forest-onebot/README.md)，选型和职责边界见 [OneBot v11 设计](doc/04-onebot-v11.md)。宿主接入及事件传输尚未实现；事件设计沿用 v11 原始结构，经 notifier 转发给模块。

配置表的声明、类型化访问、原子更新、SQLite 存储和序列化边界见 [配置表实现](doc/05-config-table.md)。

配置与接口在声明上挂静态 AccessPolicy，宿主绑定真实来源并检查；配置使用 readers/writers，接口 send/call 使用 callers，不建立动态 SQL ACL 表。内存表与只读 `check` 已实现 Read/Write/Call，不存在的对象/资源键对一律返回 `false`；声明接入及宿主鉴权尚未实现，见[访问声明](doc/06-config-access.md)。

包的外部静态声明见[模块 Manifest 详细设计](doc/07-module-manifest.md)：单例对象由 Manifest 声明稳定 Token，调用按 `(对象 Token, 接口路径)` 定位，`uses` 直接声明两端路径，热重载保持调用地址和契约；本阶段不包含 UI 配置和 ABI 版本，自动生成工具另行设计。

模块可用的最小操作见[宿主接口设计](doc/08-host-interfaces.md)：通信、单项配置 `get/set/reset`、HTTP、WebSocket、受管文件、日志与时间。模块侧不引入事务回调或自动冲突处理；底层 Wasm ABI 和异步执行模型尚未实现。

[调度设计](doc/09-scheduling.md)明确同一实例独占推进、时间片被动让出保留执行权、模块主动让出释放执行权，以及接口 reentrant 的排队与恢复规则；运行时和 SDK 尚未实现，本阶段不设计死锁处理或数值并发上限。

[生命周期设计](doc/10-module-lifecycle.md)明确 RAII、create 禁止 send/call、Drain/AtYield/Force 三种卸载方式、destroy 宿主操作白名单、静态 notifier 注册和 trap 后不可调度的处理。所有方式保持对象地址，非排空方式允许中断旧请求，不迁移连接或执行状态。

[宿主事件 notifier 与静态依赖设计](doc/11-notifier-and-dependencies.md)明确宿主产生事件，Manifest 只用 subscriptions 声明接收入口，调度器按注册表投递；模块没有事件发布接口。模块加载依赖与宿主事件支持分开检查，不提供运行时订阅增删接口。

[调度器状态机与所有权](doc/12-scheduler-state-machine.md)细化六个模块阶段、Idle/Advancing 执行权、接口许可、请求额度、一次性结果出口及唤醒规则，并逐项审查热重载与异步完成的交错。不变量依靠状态构造和所有权转移保持；运行时仍待实现。

旧 Python 实现及旧设计文档位于 `archive/python-2026-10-19`，最后快照为 `2d560a69`。其他旧分支及 worktree 快照分别保存在 `archive/master-2026-10-19`、`archive/control-payloads-2026-10-19`、`archive/shell-contracts-2026-10-19`。旧文档不再指导新主线实现。
