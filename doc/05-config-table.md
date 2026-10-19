# 配置表与 SQLite 持久化

`forest-config` 是宿主配置存储。逻辑结构为 `实体 Token → 配置项 Token → Entry`，不包含 Bot 或 Agent 特定含义。SQLite 是数据的唯一权威，不另设一份可独立修改的内存表。

## 声明与默认值

模块使用 `Key<T>` 声明类型化键，通过 `declare(&default)` 生成条目声明；底层也支持显式提供路径、`Schema` 和默认 `Value`。默认值必须符合 Schema。

每个 `Entry` 始终拥有 Schema、默认值和当前值。注册实体时根据全部声明一次建立配置表，当前值初始化为默认值；没有未初始化条目或 `Option<Entry>`。没有声明的路径返回错误，不在读取时补项。

读取只读，写入只改变当前值，不替换 Schema 或默认值。`reset` 将当前值恢复成声明默认值，不删除条目。数据类型本身可以声明为 `Option<T>`，这与条目是否存在不同。

类型化键访问时检查其 Schema 与已声明 Schema 相互为子类型，避免错误的键类型静默读取投影。原始值使用序列化库的非宽松校验，符合声明后原样保存，不通过解码再编码丢弃额外字段。

## 存储格式与查看

宿主显式指定数据库路径，库不选择工作目录或创建隐藏目录。使用 SQLite 无需启动数据库服务；构建时通过 `bundled` 特性编入 SQLite。

数据库只有两张表，定义见 [`schema.sql`](../crates/forest-config/src/schema.sql)：

| 表 | 列 | 含义 |
|---|---|---|
| `config_entities` | `entity` | 实体 Token 的规范文本，主键，保留空配置表 |
| `config_entries` | `entity`, `path` | 所属实体和配置项路径，组成主键 |
| `config_entries` | `schema_json` | Forest Schema 的 JSON 表示 |
| `config_entries` | `default_json` | 声明默认 Value 的 JSON 表示 |
| `config_entries` | `value_json` | 当前 Value 的 JSON 表示 |

JSON 复用 Forest 类型的 Serde 编解码，保留类型标签，例如 `{"I64":19}`，不是另建一套动态值语义。复杂值同样可读；Map 按键值对数组编码以保留非字符串键。列全部非空，SQL NULL 不代表缺失默认值。

用户可以用 SQLite 命令行或普通 SQLite 查看器直接查看：

```sql
SELECT entity, path, schema_json, default_json, value_json
FROM config_entries
ORDER BY entity, path;
```

数据库文件访问权高于框架 ACL；直接用外部工具修改数据库不会经过框架授权，也不会通知运行模块应用配置。正常修改经宿主配置接口或之后的管理 UI；人工修复宜在停止宿主后进行。读取磁盘数据时验证 JSON、Schema 和值，不隐式修补无效条目。

本库只存储当前配置及可恢复状态项。Bot 消息归档、记忆图与向量检索的存储接口尚未实现，不因选择 SQLite 就全部塞入配置表。

## 异步接口与原子性

使用 `tokio-rusqlite`，每个 Store 连接由一个后台线程持有；异步调用提交操作并等待结果，不在 Tokio 执行线程运行 SQLite。`ConfigStore` 的克隆共享同一个连接和后台线程，不为每个模块打开连接。原生宿主示例：

```rust
let store = ConfigStore::open("forest.sqlite3").await?;
let counter = Key::<i64>::new(counter_path);
// 只在实体首次建立时注册；宿主重启或模块热重载后直接读取已有数据。
store.register(module_token.clone(), [counter.declare(&0)?]).await?;
let count = store.get(&module_token, &counter).await?;
```

- `register` 在一个事务内建立实体和全部默认条目；实体已存在则报错，不重置旧数据。
- `remove_entity` 显式删除实体及其条目。重置单项不删除声明。
- `read` 提供一个数据库读事务中的只读视图，可一致读取多个实体或条目；`entities`、`entries` 可列举地址及值。
- `apply` 校验全部目标和新值后，在同一个写事务中提交。同一批次重复修改某项时按输入顺序执行，最后一次生效。
- `update` 在一个 `BEGIN IMMEDIATE` 写事务中读取、计算、校验及提交，避免并发读改写丢失更新。计算仅面对只读视图，返回结果与数据形式的修改列表。计算失败、校验失败或数据库执行失败均回滚，不提交部分修改。

独立的 `get` 和 `set` 仍是两次操作，不能自动组成一次原子更新。不引入条目版本、CAS 或自动重试。写事务决定并发操作的提交顺序，不会把本来不满足交换律的修改变成可交换操作。

`read`、`update` 的回调是同步宿主代码，在数据库线程执行，捕获数据须满足 `Send + 'static`。回调不能重新进入同一个 Store、等待其他模块或执行无关 I/O；它不暴露连接或可变 Entry。这里的回调不是已经确定的 Wasm ABI。

磁盘数据库启用 WAL、`synchronous=FULL` 和外键。写入成功返回表示事务已经提交，不只是排入保存队列；持久性仍以操作系统与存储设备正确执行同步为前提。SQLite 写事务串行执行，本库不设置自动锁重试或等待期限，外部写者占用数据库时返回 SQLite 错误。

丢弃已经提交到后台线程的操作 Future 不会撤回该操作，调用方不能把取消等待当成事务回滚；未知结果时也不应盲目重放非幂等更新。`close` 按连接队列关闭数据库，其余克隆之后返回连接关闭错误。执行实例销毁不关闭 Store，Store 属于宿主。

WAL 运行期间可能存在 `-wal`、`-shm` 文件。查看器可正常读取，但在线备份不能只复制主文件；应使用 SQLite 备份能力或停止宿主、关闭连接后复制数据库。

## 显式导出与导入

`snapshot().await` 在一致读事务中生成序列化副本，包含实体、路径、Schema、默认值和当前值，也保留空实体。按规范路径文本排序输出。

`import(snapshot).await` 验证地址、重复实体/条目、默认值和当前值类型后，在一个写事务内导入。遇到已有实体报错，整个导入回滚，不覆盖、不自动合并或改声明。默认值和当前值均为必填字段。

导出用于检查或显式转移数据，不是写入后的后台保存任务，也不是 Wasm 执行快照。普通重启直接打开数据库，无需反复导出、重建整张表。

## 运行时边界

这是原生宿主库，SQLite 不装进 Wasm 模块。模块在 `create` 收到稳定 Token 后可以将其作为目标地址；真实来源仍由调度器附加。目标 Token 不授予权限。

配置访问采用插件声明自身条目可读、可写主体的方式，访问规则属于装载元数据，不新增 SQL ACL 表，也不放进可普通写入的配置项。具体规则见[配置访问声明](06-config-access.md)。`forest-util::acl` 已提供独立内存表和 `check`；当前 `Declaration` 尚未包含这些访问规则，SQLite 中的 Schema 和默认值不是授权依据。

宿主鉴权、ACL 检查、Wasm 导入、生命周期限制和配置生效通知尚未接入。后续运行时必须在授权边界调用 Store；只读入口不能通过间接调用获得写入能力。原生 Store 的整表访问、导出、导入与实体管理不能原样开放给模块或 UI。热重载保留 Store 和已有数据，不重新注册或覆盖实体；规则随所属声明交接，具体排空与声明变更流程另行设计。

异步驱动行为参见 [tokio-rusqlite](https://docs.rs/tokio-rusqlite/latest/tokio_rusqlite/)，读写并发和 WAL 说明参见 [SQLite WAL](https://www.sqlite.org/wal.html)。
