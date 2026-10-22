# 运行时、SDK 与 Wasm ABI

本文记录第一版实际实现，语义约束以[调度状态机](12-scheduler-state-machine.md)为准。没有旧 Python 调用层、包格式迁移或 ABI 版本协商。

| crate | 职责 |
|---|---|
| forest-protocol | 外部 Manifest、调度异常、操作数据和固定字节编码 |
| forest-sdk | 类型化处理器、来宾 Future、固定导出和唤醒 |
| forest-runtime | 唯一装载目录、请求接纳、实例执行权、生命周期、鉴权和资源 |
| forest-cli | 启动参数、网关连接和 v11 API 回执关联 |

## 1. 构建并运行

从仓库根目录执行：

```sh
rustup target add wasm32-unknown-unknown
cargo run -p forest-test --bin package --locked
```

打包程序编译 `plugins/forest-test`，生成 `packages/forest-test/module.wasm` 和外部 `manifest.json`。接口声明从同一份类型化处理器注册表产生，仍可在不执行 Wasm 的情况下读取。二进制构建产物不进入 Git。

正向 WebSocket 可同时收事件与调用 API：

```sh
cargo run -p forest-cli --locked -- \
  --events-ws ws://127.0.0.1:8080 \
  --allow-group 123456789
```

地址和群号均为示例。网关要求令牌时，启动前设置 `FOREST_ONEBOT_TOKEN` 环境变量；凭据不放入命令行或 Manifest。`--token-env` 可指定其他变量名，`--event-token-env` 可为事件连接另选令牌。

独立 HTTP API 配合反向 WS：

```sh
cargo run -p forest-cli --locked -- \
  --onebot-http http://127.0.0.1:3000 \
  --reverse-ws 127.0.0.1:3001 \
  --allow-group 123456789
```

由网关连接宿主的反向 WS 地址；也可将 `--reverse-ws` 换成 `--events-ws`。反向 WS 必须同时配置 HTTP API。非回环监听必须配置事件令牌。HTTP 默认直连，不隐式继承系统代理。

模块 Serving 后，在允许群发送 `#forest-test`，模块回复一次 `hello world`。支持文本段及消息数组中对当前账号的 @。示例接口不可重入；发送成功后才持久化 `/state/replies`。其余命令不触发回复，未允许群不会进入模块或收到原生发送。

以上示例显式传入 `--allow-group` 以限制测试范围；该参数现在可选。省略时 OneBot 宿主不按群过滤，是否处理由接收模块决定，接口 ACL 始终检查。正常 Agent 实例只使用其配置表的 `allowed_groups`，见[Agent 启动](22-agent-implementation.md)。需要宿主测试限制时才传此参数；`GroupScope::Only` 的空集合拒绝全部群，不将空集合解释为全放行。

支持多客户端的 OneBot 网关可以同时连接现有 Bot 和测试宿主。测试期间保持现有 Bot 正常服务，通过允许群和精确测试命令限制测试模块的响应范围；启动测试宿主不要求停止现有 Bot。

默认路径均从启动工作目录解析：包 `packages/forest-test`，SQLite `data/forest.sqlite3`，受管文件 `data/modules`。分别可用 `--package`、`--database`、`--data-directory` 修改；不使用隐式 `.local` 工作区。

Ctrl+C 默认 Drain，可用 `--shutdown-mode yield` 或 `force` 显式选择。OneBot 正向 WS 在初次连接失败或运行中断开后自动重连，间隔按 1、2、4、8、16、30 秒退避；成功连接后恢复为 1 秒。连接恢复不重建调度器、模块或 Agent 会话，WebUI 继续运行。反向 WS 保持原监听器，连接结束后等待网关再次接入。

双向 WS 的动作队列和回执表属于单次连接。未连接时请求立即返回 Disconnected，连接关闭时尚未收到回执的请求返回 ReceiptLost，已排队动作随该连接释放；重连后的队列为空。未收到回执并不证明平台未执行，因此不自动重发消息或重试动作。HTTP action 传输不因事件 WS 重连而自动重试。连接、回执及模块诊断可在[调试台运行日志](14-inspection-webui.md)查看。

## 2. Rust 模块 SDK

Rust SDK 是可选的语言绑定。模块不链接调度器，也不链接被调用模块；其他语言只需提供规定的 Wasm 导出、使用固定宿主导入，并按 Schema/Value 编码数据。当前现成 SDK 与主要验证使用 Rust，尚未提供其他语言的 SDK。

实际 Rust 示例见 [forest-test](../plugins/forest-test/src/lib.rs)。实现 `forest_sdk::Module`：

- `create(CreateContext)` 返回模块对象，可以恢复配置表中的状态。
- `handlers()` 用 `Handlers::with` 注册路径、重入属性、访问规则和具体 Rust 异步函数；输入和完整 `Result<T, E>` 的 Schema 从函数类型生成。
- `destroy(Rc<Self>)` 可选，默认无操作。
- `export_module!` 生成固定 ABI 导出。

处理器取得 `Rc<Self>`、可信 `CallContext` 和具体输入类型。共享状态采用普通 Rust 作用域借用或内部可变性，不应持有与其他处理器冲突的借用跨 await。只有主动 Pending 才开放实例重入，调用宿主函数本身不开放。

`host::call` 返回 `Result<Result<T, E>, DispatchError>`：外层属于调度器，内层完全属于被调用方。宿主只检查内层 Schema，不根据 E 决定重试或补偿。`host::send` 只返回接纳结果，没有完成回执。

宿主操作使用独立函数和结构体：配置 `get/set/reset`、HTTP、WS、受管文件、时间和日志。配置必须在 Manifest 声明 Schema、默认值及 ACL；未知或未授权的对象/键默认拒绝。真实来源由宿主附加，参数不能伪造。

## 3. 嵌入与热重载

宿主通过 `Scheduler::new` 或 `with_store` 创建调度器，先注册原生接口及事件定义，再读取包。`NativeInterface::new` 从具体 Rust 函数生成输入及 Result 输出契约。

| 操作 | 行为 |
|---|---|
| package(directory) | 读取 Manifest、编译 Wasm、校验固定导出与私有内存 |
| plan_manifests(manifests, roots) | 仅从声明给出依赖优先顺序，不编译 Wasm |
| plan_load(packages, roots) | 复用相同规划，返回编译包闭包及依赖优先顺序 |
| load_roots(packages, roots) | 明确执行计划，依赖先初始化 |
| load(package) | 单包装载，依赖必须已在目录中 |
| reload(package, mode) | 以稳定对象身份交接，新请求留给候选 |
| unload(object, mode) | 仍有静态使用者时拒绝，先移除使用者 |
| dependents(object) | 查询直接静态使用者，无执行副作用 |
| force(object) | 中断 create/destroy，或升级已有停止过程 |
| shutdown(mode) | 使用者先于提供者卸载 |

当前显式拒绝依赖环，不引入环启动和运行时死锁处理。计划不更改外部启用意图；某个 create 失败后已加载的依赖保留，不伪装成原子整批加载。

Drain 完成截止前的旧集合；AtYield 等当前推进交回后中断其他旧请求；Force 中断原推进并跳过所有来宾析构。三者都等待已提交的本地 SQL/文件操作实际结束，之后才创建新实例，避免旧写入追上新状态。已接纳的下游调用不递归撤销，远端效果不回滚。

替换保留已发布接口，按 `old.input <: new.input`、`new.output <: old.output` 检查函数子类型；完整 Result 输出包含错误分支。加载依赖采用相同方向：`uses.input <: provides.input`、`provides.output <: uses.output`。不兼容候选在停止旧实例前拒绝，适用于全部停止方式。交接请求执行时绑定新实例的输出 Schema，正常调用也使用同一子类型关系检查 uses。配置键集合和 Schema 仍要求等价；允许原子更新默认值并保留全部当前值，后续 reset 使用新默认值。ACL 随新声明切换；create 使用自己的候选规则，外部仍看到旧规则，直到 Serving 时整体发布。已接纳请求不追溯重新鉴权。

trap 后保留 Failed 槽位，回收实例，不自动重启；外部可 load 恢复或 unload 移除。CLI 启动时加载指定包；[宿主调试台](14-inspection-webui.md)通过独立 ModuleManager 能力提供本地包装载、三种模式的重载与卸载，以及显式 Force；ConfigWriter 使用宿主绑定的 `/user/admin` 来源按 ACL 执行配置 set，不授予可写 ConfigStore 或任意模块调用。尚无文件监视。

## 4. 固定 ABI

| 导出 | 参数与返回 |
|---|---|
| memory | 私有、非共享 wasm32 内存 |
| forest_alloc | (len: u32) → ptr: u32 |
| forest_free | (ptr: u32, len: u32) → () |
| forest_create | (task: u64, ptr: u32, len: u32) → () |
| forest_invoke | (task: u64, ptr: u32, len: u32) → () |
| forest_destroy | (task: u64) → () |
| forest_poll | (task: u64) → buffer: u64 |

buffer 低 32 位为指针，高 32 位为长度。输入由宿主经 guest alloc 分配、复制、传入入口、guest free 归还；输出由 SDK 分配，宿主复制解码后 guest free。Force/trap 后不得再执行 free 等任何来宾辅助函数。

create 输入为 CreateContext，invoke 为 Invocation，两者只建立任务。poll 推进 Future，返回 `Pending`、`Output(Value)` 或 `Lifecycle(Result<(), String>)`。参数和结果收尾均属于同一次独占推进。

ABI 结构与操作结构采用 `forest_protocol::abi` 固定小端、定长整数 bincode 编码；接口值和配置值仍是 Forest Schema/Value。单帧最多 16 MiB，拒绝尾随字节。此边界没有版本协商。

这里描述的是字节协议，不是 Rust 对象内存布局。非 Rust 模块无须导入 Rust trait、crate 或 TypeId；需要实现相同的字节编码、任务推进和资源所有权约定。模块间调用按对象与接口地址派发，双方分别以自己的类型表示相同 Schema，在线性内存之间传递编码后的 Value。Manifest 的类型图和打包附带的 `.schema` 文本均可在不执行 Wasm、不读取 Rust 源码的情况下取得。

导入模块名为 `forest_host`。每个宿主操作有独立函数：

```text
call send config_get config_set config_reset
http_request ws_connect ws_send ws_receive ws_close
file_read file_write file_remove sqlite sleep log now
```

操作函数接受 `(ptr: u32, len: u32)`，返回实例内 operation id。提交时检查阶段、来源及权限；完成只保存结果并唤醒，不重入 Wasm。

| 控制导入 | 语义 |
|---|---|
| operation_status(id) → i64 | -1 为等待，非负为结果长度，只读 |
| operation_take(id, ptr, len) | 复制结果并消费句柄 |
| operation_release(id) | 放弃本地结果，不声称撤销外部效果 |
| task_wake(task) | 合并本实例任务的就绪通知 |

句柄不授予身份权限，也不跨实例有效。无效控制句柄、越界内存、错误 ABI 调用属于协议破坏并 trap；正常拒绝、队列满及模块错误以 Result 返回。

## 5. 执行与资源

目录转换使用短临界区，不持锁执行 Wasm 或等 I/O。每个实例由一个拥有 Store 的异步驱动器推进；实例内 Start/Resume 共用去重 FIFO，实例间另有去重就绪队列。默认并行推进数为 2，可通过 RuntimeOptions 配置。

独立线程每 5 ms 推进 epoch，用于可恢复让出和显式 Force 检查，不是请求超时。Wasmtime Pending 保留原推进，SDK Pending 才释放实例执行权。不可重入接口许可直到整个请求结算才归还。

默认每个对象保留最多 1024 个请求、64 MiB 输入，Wasm 内存上限 256 MiB。名额随请求、任务和必要收尾移动，满额原子拒绝，不经隐藏的无界入口排队。原生接口也受容量和重入约束。

Wasm 使用的 HTTP/WS 原语不自动重试或重连；OneBot 宿主适配器的连接恢复与这些原语分开。受管文件由 capability directory 限制在实例目录内。卸载释放实例网络资源。配置与文件操作提交后必须等本地实际结束，丢弃 Future 不等于操作系统已撤销操作。

## 6. 验证与范围

```sh
cargo test --workspace --all-features --locked
```

集成测试编译真实 Wasm，使用两个代码不同、契约相同的候选。覆盖类型化调用、ACL、非重入与重入交错、自唤醒和组合 Future、容量、三种卸载方式、代码替换、trap、单线程执行器下的抢占、配置恢复和延迟 SQLite 写入交接、文件隔离、HTTP/WS，以及实际 CLI 的群消息到 OneBot 回执。自动测试流量只进入本地模拟服务。

宿主当前注册 OneBot 群消息事件和 `/core/onebot` 的 `/send_group_message`；独立 API 库中的其他 action 尚未逐一映射为原生接口。CLI 的 OneBot callers 授权 `/module/**`，因此独立加载的回复插件可使用原生发送；可选 `--allow-group` 继续限制实际接收与发送的群。CLI 另安装 `/core/control` 管理适配器，仅授权 `/module/shell`，见[群聊 Shell](26-group-shell-and-school-help.md)。可选 WebUI 已提供观测和模块装卸。Agent、Memory 与工具网关已由独立 Wasm 包实现，见[实现与启动](22-agent-implementation.md)。热重载可更新同 Schema 的声明默认值并保留当前值；配置项集合或 Schema 变更仍拒绝。包分发与配置变更通知尚未实现。
