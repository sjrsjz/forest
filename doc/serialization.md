# Forest 序列化移植

由已有序列化实现移植并修正。本仓库使用 `forest-serde` 与 `forest-serde-macros`，公共模块拼写统一为 `prelude`。包名、宏生成路径、示例和依赖均使用 Forest 命名，不提供旧名称兼容层。

本文只说明类型表达、转换和序列化，不定义模块 ABI、调度器、调用权限、数据库或框架生命周期。运行时设计方向另见[模块运行时架构](01-module-runtime.md)和[状态与访问详细设计](02-state-access-and-reload.md)。

## 类型与值

`Schema` 表达结构类型；`Value` 表达独立值树。Rust `TypeId` 只参与本地类型图构造，导出的 Schema 使用索引引用。Record、Variant 和命名载荷使用有序映射，使同一 Rust 类型重复生成的 Schema 具有稳定的字段遍历和引用编号。不同但结构等价的类型图不承诺编码字节相同。

`DeriveSchema` 和 `DeriveValue` 从同一 Rust 类型生成对应契约与转换，包括泛型和递归类型。

```rust
use forest_serde::{DeriveSchema, DeriveValue};
use forest_serde::adt::{AsSchema, AsValue};

#[derive(DeriveSchema, DeriveValue)]
struct State<T> {
    revision: i64,
    data: T,
}

let state = State { revision: 1, data: "hello".to_owned() };
let value = state.as_value();
assert!(value.check(&State::<String>::as_schema(), false));
let restored = State::<String>::from_value(value)?;
```

## 转换规则

保留原有的结构子类型和前缀投影语义：

- `I32` 可以提升为 `i64`；不进行反向缩窄或其他猜测转换。
- Record 按目标字段读取，额外字段忽略；缺少字段是错误。`Option<T>` 字段缺失不等于该字段的值是 `None`。
- Tuple 和未命名 Variant 载荷读取目标所需前缀，长度不足是错误，额外尾部忽略。
- 空 Record 和空 Tuple 维持原有互操作；空结构的转换忽略源结构中的额外字段或元素。空 Variant 载荷同理。
- 固定数组使用 const generics，不再限制为宏生成的 0–12 长度。
- Map 键转换产生冲突时返回错误，不静默覆盖记录。Map Schema 的键类型仍保持不变性。
- `Value::check(schema, true)` 是显式宽松校验，允许部分跨形状匹配；`AsValue::from_value` 不启用宽松模式，也不猜测输入形状。

投影是有损的：读取旧类型后重新保存，只保留目标类型中的字段，不提供未知字段透传。自定义 `AsValue` 与 `AsSchema` 实现必须自行遵守其声明的转换契约。

## 类型化错误

`AsValue::from_value` 返回 `Result<T, DecodeError>`。错误通过枚举携带类型、字段、索引、长度、未知 Variant 标签或 Map 键冲突等数据；`Display` 只用于展示，调用方不解析错误字符串。错误本身支持 Serde 编解码。

## 编码与当前限制

- Schema 的 Serde 编码统一为根类型在索引 0 的类型表，序列化与反序列化对称。拒绝空类型表和越界引用。
- Value 与 Schema 都支持 Serde。Map 编码为有序键值对序列，支持 JSON 中的任意结构键并拒绝重复编码键。文本 DSL 的字符串遵循 JSON 转义，格式化保留 Variant 的外层载荷结构，避免单个 Tuple/Record 字段被展开。
- 继承原来的纯 Ref 环与递归结构类型规则；本次没有重定义类型代数。
- 原生目标保留 `stacksafe` 调用栈增长；Wasm 目标不依赖原生栈切换，递归受其执行栈限制。当前不承诺任意深度的输入可成功处理。
- 尚未扩展浮点数和无符号整数。
- JSON 和 bincode 用于检查往返一致性；本阶段尚未冻结跨版本的持久化二进制协议，也没有加入状态迁移机制。模块身份、存档版本及持久状态格式在后续设计中确定。
