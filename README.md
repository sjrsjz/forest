# Forest

Rust 新主线。目前仅包含从 Synthase 移植的序列化库与 derive 宏；模块运行时和框架架构尚未实现，将另行讨论。

```text
crates/
  synthase-serde/          Schema、Value、类型转换与文本语法
  synthase-serde-macros/   从 Rust 类型生成 Schema 与 Value 转换
```

```sh
cargo test --workspace --locked
cargo check -p synthase-serde --target wasm32-unknown-unknown --locked
```

序列化语义、修正范围和当前限制见 [序列化说明](doc/serialization.md)。

旧 Python 实现及旧设计文档位于 `archive/python-2026-10-19`，最后快照为 `2d560a69`。其他旧分支及 worktree 快照分别保存在 `archive/master-2026-10-19`、`archive/control-payloads-2026-10-19`、`archive/shell-contracts-2026-10-19`。旧文档不再指导新主线实现。
