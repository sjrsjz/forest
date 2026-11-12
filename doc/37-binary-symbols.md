# 符号表二进制编码

本文定义 Forest Value 与完整 Schema 在 [Wasm ABI](13-runtime-and-sdk.md#4-固定-abi) 中的二进制布局。控制结构仍使用固定小端、定长整数 bincode；这里规定其内部 Value/Schema 字段的编码。Forest 文本与可读 JSON 使用名称，不使用符号索引。

## 1. 文档与符号

每次序列化一个 Value 或完整 Schema，编码一份独立文档：

```text
document = dynamic_symbols, body
dynamic_symbols = count: u64, [byte_length: u64, utf8_bytes] * count
```

编码器收集不同名称，排除保留名字后按 UTF-8 字典序排列动态表，并通过二分查找得到索引。枚举标签、Record 字段名、命名载荷字段名共用这张表；它们在 Map 的键中出现时也属于同一文档。Record 与命名载荷按字段名称排序遍历，普通 Map 按逻辑 Value 的顺序遍历。普通 `Value::String`、字符串 Map 键的文本内容及 bytes 不进入符号表。解码允许名称表采用其他排列，相等性不依赖表项顺序。

全局声明位于 [`forest_serde::symbol`](../crates/forest-serde/src/symbol.rs)：

| 常量 | 当前值 | 含义 |
| --- | --- | --- |
| `NONE_SYMBOL` | 0 | 名字 `None` |
| `SOME_SYMBOL` | 1 | 名字 `Some` |
| `FIRST_DYNAMIC_SYMBOL` | 2 | 动态表第 0 项的编号 |
| `RESERVED_SYMBOLS` | `(NONE_SYMBOL, None)`, `(SOME_SYMBOL, Some)` | 保留编号与名字的唯一映射 |

正文中的名称使用 `SymbolId`，其编码为 `u32`。动态表第 `n` 项的编号是 `FIRST_DYNAMIC_SYMBOL + n`。保留名字不在动态表重复发送。保留索引只决定名字，不隐含枚举载荷形状；例如命名载荷 `Some { item: bool }` 仍是合法普通枚举分支，不会被改写为 Optional。

表不跨帧共享，不由调度器登记，不属于模块实例。不同文档中的同一个名字可以有不同索引；逻辑相等、哈希、排序和 Map 键的身份均按恢复后的名字判断。Schema 的类型引用 `Ref` 与 `SymbolId` 是不同的索引空间。

解码拒绝重复动态名字、在动态表再次声明保留名字、越界符号、未知节点标签、截断输入和重复键。完整 Schema 还检查非空类型表与所有类型引用的有效性。ABI 拒绝尾随字节。没有字节布局版本协商或旧格式识别。

## 2. 基础布局

所有整数均为小端。`i32`、`i64` 使用对应宽度的二进制补码；bool 是一个字节的 0/1。字符串写 `u64` 字节数及 UTF-8 原文，bytes 写 `u64` 长度及原始字节。序列和映射写 `u64` 元素/键值对数后紧接各项；元组和字段组合只连接各字段，不额外写长度。Schema 固定字节数和类型引用索引按 `u64` 编码。

节点的 kind 是 `u32`，紧接该 kind 的载荷。原生 Rust 表示与以下协议独立，不能拷贝 Rust 对象内存代替编码。

## 3. Value 正文

Value 文档的 body 是单个 Value 节点：

| kind | 类型 | 载荷 |
| --- | --- | --- |
| 0 | I32 | i32 |
| 1 | I64 | i64 |
| 2 | Bool | bool |
| 3 | String | 字符串 |
| 4 | Bytes | 字节缓冲 |
| 5 | Tuple | Value 节点序列 |
| 6 | Array | Value 节点序列 |
| 7 | Map | `(Value 键, Value 值)` 映射 |
| 8 | Variant | `SymbolId 标签, 载荷节点` |
| 9 | Record | `(SymbolId 字段名, Value 值)` 映射 |

载荷节点使用以下 kind，编号与 Value kind 属于不同位置：

| kind | 类型 | 载荷 |
| --- | --- | --- |
| 0 | Unnamed | Value 节点序列 |
| 1 | Named | `(SymbolId 字段名, Value 值)` 映射 |

`optional T` 与 `variant { None, Some(T) }` 是同一种结构类型。`null` 的值是空 Unnamed 载荷的 `None`；`some x` 的值是只有一个元素的 Unnamed 载荷的 `Some`，不另写一个 Option 判别项。嵌套 Optional 按嵌套 Variant 编码。普通枚举的结构子类型及前缀投影规则同样适用。

原生 ABI 控制结构中直接交给 Serde 的 Rust `Option<T>` 仍采用 bincode 的单字节 0/1 标记。本节的 Variant 布局适用于 Forest Value 中的 Optional。

例如 `null` 的完整文档为 28 字节：

```text
00 00 00 00 00 00 00 00   # 动态符号数量 0: u64
08 00 00 00               # Variant: u32
00 00 00 00               # NONE_SYMBOL: u32
00 00 00 00               # Unnamed: u32
00 00 00 00 00 00 00 00   # 载荷元素数量 0: u64
```

`some 42` 将符号替换为 `SOME_SYMBOL`、载荷长度替换为 1，再追加 I32 节点 `00 00 00 00 2a 00 00 00`。

## 4. Schema 正文

完整 Schema 的 body 是 Schema 节点序列，索引 0 是根类型。所有节点及其嵌套载荷共用文档的符号表。

| kind | 类型 | 载荷 |
| --- | --- | --- |
| 0 | I32 | 无 |
| 1 | I64 | 无 |
| 2 | Bool | 无 |
| 3 | String | 无 |
| 4 | Bytes | 固定长度 u64 |
| 5 | Tuple | Schema 节点序列 |
| 6 | Array | 一个 Schema 节点 |
| 7 | Map | 键 Schema 节点、值 Schema 节点 |
| 8 | Variant | `(SymbolId 标签, Schema 载荷节点)` 映射 |
| 9 | Record | `(SymbolId 字段名, Schema 节点)` 映射 |
| 10 | Ref | 类型表索引 u64 |
| 11 | ByteBuffer | 无 |

Schema 载荷节点的 kind 同样是 0/1：Unnamed 保存 Schema 节点序列，Named 保存 `(SymbolId 字段名, Schema 节点)` 映射。Optional 在类型构造或文本解析时即展开，不在此处增加特殊 Schema kind。

## 5. 实现与测量

编码先借用原树收集名称，再通过携带符号表引用的序列化器直接输出正文，不生成索引版的第二棵树。ABI 的 Writer 限制实际输出长度，避免 bincode 的长度预估遍历再次构建符号表。字节缓冲走连续读写；字符串内容保持原值。

Rust 层的结构名称使用不可变 `Symbol`：宏生成的名字借用静态文本，解码的动态名字在一份文档中只分配一次 `Arc<str>`，各节点克隆引用。保留名字直接使用静态文本。名字不依赖输入缓冲或解码器的生命周期，也不进入全局驻留池；最后一个值释放名字时即回收对应分配。`Symbol` 按文本实现相等、哈希、排序及 `Borrow<str>`；局部传输编号使用独立的 `SymbolId`，不泄漏到逻辑 Value 的比较语义。

大量重复字段减少输出体积、复制量和跨 Wasm 内存的搬运量；符号表也有构建和查找成本。可复现的编码/解码基准位于 [`examples/symbols.rs`](../crates/forest-protocol/examples/symbols.rs)，比较单条记录、一万条重复字段记录及 4 MiB 字节缓冲的旧布局与符号编码，输出体积、名称缓冲数量和七组计时的中位数：

```sh
cargo run -p forest-protocol --example symbols --release --locked --offline
```

2026-11-13 本机一次 Release 测量如下；耗时单位为毫秒，左右分别是旧布局和当前实现：

| 样本 | 编码字节数 | 编码耗时 | 解码耗时 |
| --- | --- | --- | --- |
| 一万条记录，每条 8 个重复字段 | 3,170,012 → 1,400,229 | 4.19 → 6.80 | 14.85 → 9.38 |
| 4 MiB 字节缓冲 | 4,194,316 → 4,194,324 | 3.63 → 0.92 | 6.71 → 0.94 |

记录样本的体积减少 55.8%，解码后独立的名称缓冲从 80,000 个变为 8 个。编码阶段仍有收集和查询符号的额外成本；字节缓冲通过连续读写减少逐字节编解码开销。单条 8 字段记录的体积从 329 增至 369 字节，编码约从 0.13 增至 1.12 微秒，反映出小对象的符号表开销。基准包含原值构造后的编码、解码和结果释放，不包含网络或 Wasm 内存复制；这些耗时用于复现该样本，不代表整个调用链的延迟。

更改这一布局后，宿主、SDK、插件和其他语言绑定必须配套重新构建。Forest Schema/Value 的可读 JSON 中，Optional 也使用普通 Variant；部署时需按新格式导入旧配置中的该表示。直接使用 Serde 的普通 Rust 数据结构仍沿用其原有 JSON 格式。
