# 配置与数据存储

[返回目录](README.md)

## 1. 选择存放位置

| 位置 | 适合内容 | 所有权和恢复 |
| --- | --- | --- |
| Wasm 内存 | 当前请求、缓存、短期窗口、临时计算 | 属于执行实例；重载后不保留 |
| 配置表 | 运行参数、小型可恢复状态 | 由实体和键寻址，按 Schema 与 ACL 访问 |
| 私有 SQLite | 大量记录、索引关系、需要 SQL 事务的数据 | 按稳定模块 Token 分配，表结构由模块管理 |
| 受管文件 | 二进制、媒体、较大静态载荷 | 按模块隔离目录，使用相对路径 |
| 外部服务 | 向量索引、远程模型或专用数据库 | 由模块通过网络协议访问并管理连接配置 |

模块的实体 Token 决定私有数据归属。更换 package.id 等同于换一个模块身份，不能当作普通代码升级。包目录用于发布工件，运行状态应通过宿主数据能力保存。

## 2. 配置声明与默认值

示例模块声明 `/settings/reply`：

```rust
ConfigDeclaration {
    key: "/settings/reply".parse().unwrap(),
    schema: String::as_schema(),
    default: "hello world".to_owned().as_value(),
    access: AccessPolicy {
        readers: vec![SubjectPattern::SelfModule],
        writers: vec![SubjectPattern::Path("/user/admin".parse().unwrap())],
        callers: vec![],
    },
}
```

每项必须提供符合 Schema 的默认值。宿主准备声明后，模块读取该项的当前值；配置 API 不用 `Option<Entry>` 表达一个正常声明项。Schema 本身可以是 `Option<T>`，这表示该项允许 None，与条目不存在无关。

第一次注册使用默认值。已存在的配置保留当前值；兼容重载允许更新默认值，后续 reset 才恢复到新的默认值。

## 3. 读取与写入

```rust
let reply: String =
    host::config_get(object.clone(), key.clone()).await??;
host::config_set(object.clone(), key.clone(), &new_reply).await??;
host::config_reset(object, key).await??;
```

此处函数错误类型需能接收 DispatchError 和 ServiceError，例如 SDK HostError。读取返回副本；写入和重置成功代表本地 SQLite 提交完成。操作不改变 Schema、默认值或 ACL。

读取时机决定配置生效方式：create 时读取形成实例内快照，需重载后更新；普通处理器每次读取则使用该次读取的当前值。写入本身不自动重建模块。

配置目标不必是自身，但对其他对象仍须得到目标 readers/writers 授权。常规宿主配置操作使用实际执行模块身份，不能任意把它改成用户或宿主身份。

## 4. 原子操作的范围

单个 set/reset 是原子的。下列两次调用不是原子读改写：

```rust
let old: i64 = host::config_get(object.clone(), key.clone()).await??;
host::config_set(object, key, &(old + 1)).await??;
```

两个调用者可能读到同一个 old，再互相覆盖。宿主不自动合并、不比较版本，也不重试这些写入。模块可以约束单一写入口、使用来宾异步协调，或将需要共同提交的数据放进私有 SQLite 的一个事务。

配置 ABI 当前没有 CAS、锁句柄、批量事务或事务内回调。宿主侧 `forest-config` 的管理接口不等于开放给 Wasm 的能力。

## 5. 私有 SQLite

入口为 `host::sqlite(Vec<SqlStatement>)`。每次完整列表在模块专用数据库中的一个事务内执行，任意语句失败则本次事务不提交；结果也必须能够编码返回后才提交。

```rust
use forest_protocol::service::{SqlStatement, SqlValue};

let results = host::sqlite(vec![
    SqlStatement {
        sql: "CREATE TABLE IF NOT EXISTS counters (name TEXT PRIMARY KEY, value INTEGER NOT NULL)".into(),
        parameters: vec![],
    },
    SqlStatement {
        sql: "INSERT INTO counters(name, value) VALUES (?1, 1) ON CONFLICT(name) DO UPDATE SET value=value+1 RETURNING value".into(),
        parameters: vec![SqlValue::Text("accepted".into())],
    },
]).await??;
```

与先 SELECT 再另发一次 UPDATE 相比，以上计数在数据库内完成原子更新。一次操作中可同时维护多张自己的表；模块调用和网络操作不能插入这个事务中。

`SqlValue` 的分支为 Null、Integer(i64)、Real(f64)、Text、Blob。这里的浮点属于宿主 I/O 协议，不表示 Forest Value 增加了浮点类型。结果按语句顺序返回 `SqlRows { columns, rows, changed }`。

每个 SqlStatement 只能包含一条参数化语句；值使用 parameters 绑定。宿主拒绝事务控制、SAVEPOINT、ATTACH/DETACH、PRAGMA 和扩展加载。事务边界由本次 sqlite 操作建立，不接收跨 await 的连接或事务句柄。

同一模块数据库内的操作由宿主串行执行；不同模块拥有不同数据库。当前路径位于数据目录下 `.databases/<模块Token的UTF-8十六进制>/data.sqlite3`。应用代码无需自行计算路径，也不能通过 SQLite 接口选择其他模块数据库。

## 6. 受管文件

```rust
host::file_write("cache/result.bin".into(), bytes).await??;
let bytes = host::file_read("cache/result.bin".into()).await??;
host::file_remove("cache/result.bin".into()).await??;
```

路径相对模块数据目录；父目录逃逸、绝对路径和越界符号链接不构成文件访问能力。当前宿主以模块 Token 的 UTF-8 十六进制作为目录名。这与发布包中的 `assets/` 是不同命名空间，`file_read("assets/x")` 不代表读取包资源。

文件操作不提供与配置或 SQLite 共同提交的事务。跨多种存储的一致性、重复调用和恢复策略，需要模块按实际协议设计。

## 7. 重载时的数据

替换 Wasm 会丢掉内存中的临时状态。普通 destroy 可以把短期状态写入已声明的配置项，新 create 再恢复；大型记录可在处理期间持续提交到私有数据库。

Force/trap 会跳过 destroy。宿主仍等待已提交的本地文件和数据库操作结算，再允许替换实例使用相同资源，但这不把未提交的内存内容变成持久状态，也不回滚远端已收到的请求。

数据库表结构由模块管理；当前框架不生成迁移脚本，不把所有模块数据塞进配置表，也不协调跨模块 SQL 事务。需要访问另一模块的数据时，应调用其公开接口。
