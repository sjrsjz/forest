# Forest

Rust 新主线。目前实现序列化库、derive 宏、来源令牌路径与通配符匹配、内存 ACL 查询、SQLite 配置存储，以及独立的异步 OneBot v11 API 库。模块运行时的设计讨论记录于 [设计文档](doc/README.md)，尚未进入实现。

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

OneBot 使用方式见 [API 库说明](crates/forest-onebot/README.md)，选型和职责边界见 [OneBot v11 设计](doc/04-onebot-v11.md)。该库尚未包装成 Forest 的 Wasm 插件。

配置表的声明、类型化访问、原子更新、SQLite 存储和序列化边界见 [配置表实现](doc/05-config-table.md)。

配置 ACL 由插件声明谁可以访问自身配置，宿主绑定真实来源并检查；规则作为插件元数据装载，不建立动态 SQL ACL 表。内存表与只读 `check` 已实现，不存在的实体/配置键对一律返回 `false`；配置声明接入及宿主鉴权尚未实现，见[配置访问声明](doc/06-config-access.md)。

旧 Python 实现及旧设计文档位于 `archive/python-2026-10-19`，最后快照为 `2d560a69`。其他旧分支及 worktree 快照分别保存在 `archive/master-2026-10-19`、`archive/control-payloads-2026-10-19`、`archive/shell-contracts-2026-10-19`。旧文档不再指导新主线实现。
