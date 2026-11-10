# 类型与接口契约

[返回目录](README.md)

## 1. 契约位于包外

模块间共享 Schema 与 Value，不共享 Rust 对象布局、TypeId 或具体处理函数。外部 Manifest 引用的 `.schema` 文件可以独立读取，接口调用依赖 `(对象, 接口)` 和结构类型。

例如，一个输入契约可表示为：

```text
schema Greeting = { name: string };
export Greeting;
```

对应 Value 文本为：

```text
{ name: "Example" }
```

Schema 文件定义类型图，Value 文件定义具体值。文本可通过 `forest_serde::parser::parse_schema`、`parse_value` 读取；`to_string()` 产生规范文本。实际打包文件的类型名和引用编号由格式化器生成，不依赖以上示例命名。

## 2. Rust 映射

```rust
use forest_sdk::{DeriveSchema, DeriveValue};
use forest_protocol::{AsSchema, AsValue};

#[derive(DeriveSchema, DeriveValue)]
struct Greeting {
    name: String,
}

fn encode(request: &Greeting) -> forest_protocol::Value {
    request.as_value()
}
```

`AsSchema` 声明类型，`AsValue` 提供值转换。Derive 是语言绑定，不是宿主通过反射寻找处理器的机制。普通接口通过 `Handlers::with` 绑定具体函数；注册时生成输入和完整返回类型的 Schema。

| 数据 | 常用 Rust 表示 | 要点 |
| --- | --- | --- |
| 有符号整数 | `i32`、`i64` | 位宽参与契约；i32 可提升为 i64，反向不成立 |
| 文本、布尔 | `String`、`bool` | 空字符串也是合法文本值 |
| Record | 具名 struct | 按字段名匹配，不依赖 Rust 类型名称 |
| Tuple | 元组、元组 struct | 位置和元素类型参与契约 |
| Variant | enum、`Result<T, E>`、`Option<T>` | 分支名称及载荷均参与契约 |
| 序列 | `Vec<T>`、固定数组 | 固定长度与变长序列有各自的类型关系 |
| 不透明字节 | `Vec<u8>` | 特殊映射为 `bytes`，适合媒体和二进制载荷 |
| Map | 受支持键和值组成的映射 | 键类型保持不变性；解码键冲突是错误 |
| 无错误载荷 | `Infallible` | 可用于不产生模块逻辑错误的 Result 分支 |

Forest Value 当前没有原生浮点数或一般的无符号整数映射。`Vec<u8>` 是字节载荷的特例。向量、模型响应或其他含浮点的协议可使用约定好的 JSON 文本或二进制 bytes；内容格式由该接口明确声明，不能把 `Vec<f32>` 当成已支持的 Forest 类型。

Serde 的外部 JSON 字段命名不自动改变 Forest Record/Variant 契约。公共类型发生变化时，以生成的 `.schema` 为准。

## 3. 完整 Result 与两层错误

所有 provides 的输出必须是 `Result<T, E>`。以下调用：

```rust
let returned: Result<Reply, RemoteError> =
    host::call(target, interface, &request).await?;
let reply = returned.map_err(LocalError::Remote)?;
```

实际层次为：

```text
Result<Result<Reply, RemoteError>, DispatchError>
       └── 目标模块的正常输出       └── 宿主派发或执行失败
```

外层 Denied、QueueFull、Interrupted 等不转换成目标模块的 E。内层错误由模块定义，宿主校验其结构但不解释含义。对数据没有结果需求时可以使用 send，接纳后不会向发送者返回这个内层结果。

宿主资源操作常用：

```rust
type HostResult<T> = Result<Result<T, ServiceError>, DispatchError>;
```

SDK `HostError` 保留两类错误，可直接用作生命周期错误。自定义枚举也可以包含 `HostError`、`DispatchError` 或具体远端错误。Rust 的 `?` 只应用当前需要的 `From`，不会自动沿多层枚举包装进行传递。

错误枚举可带字符串作为诊断字段，但流程应依赖分支和字段。例如 `InvalidArgument { field: String }` 可以表达输入错误；调用方不应通过搜索某个错误字符串决定是否重试。

## 4. 解码和结构子类型

- Record 解码读取目标需要的字段，忽略源中的额外字段；缺少字段会失败。
- `Option<T>` 字段存在且值为 None，与整个字段缺失是两种情况。
- Tuple 和未命名载荷读取目标所需前缀，源元素不足会失败。
- 读取成较窄的 Rust 类型再保存会丢掉额外字段；类型转换不提供未知字段透传。
- 不同类型图的结构等价，不代表序列化字节必然相同。

因此，给旧请求添加一个必需字段通常会缩窄输入；扩大可能返回的错误分支通常会扩大输出。两者都可能破坏已有调用方。不要只凭“新增字段”“新增错误”判断变更兼容。

依赖检查使用：

```text
uses.input  <: provides.input
provides.output <: uses.output
```

其中输出包含完整 Result。更多具体方向见[热重载](07-packaging-and-reload.md)。

## 5. 公共契约的设计方式

公共输入优先使用语义明确的具名结构，错误使用可识别的分支。一个接口对应一种明确操作；不同模块各自保留本地类型映射，通过公开 Schema 对齐。

标准协议的 JSON 或 bytes 可以作为明确的载荷格式使用，例如 OneBot 查询返回的 JSON。它们内部的协议仍应由模块解析，不交由调度器猜测。跨模块传入 Rust 函数、引用或内部数据库连接不属于当前 ABI。

完整的类型规则与 DSL 说明见[序列化文档](../serialization.md)。
