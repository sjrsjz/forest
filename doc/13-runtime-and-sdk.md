# 运行时、SDK 与 Wasm ABI

本文说明运行时 API、Rust SDK、固定 Wasm ABI 与资源限制。调度语义以[调度状态机](12-scheduler-state-machine.md)为准。

| crate | 职责 |
|---|---|
| forest-protocol | 已解析的 Manifest、调度异常、操作数据和固定字节编码 |
| forest-package | 宿主侧 TOML 读写、Forest 文本引用解析及包文件 SHA-256 校验 |
| forest-sdk | 类型化处理器、来宾 Future、固定导出和唤醒 |
| forest-runtime | 唯一装载目录、请求接纳、实例执行权、生命周期、鉴权和资源 |
| forest-cli | 启动参数、网关连接和 v11 API 回执关联 |

## 1. 构建并运行

从仓库根目录执行：

```sh
rustup target add wasm32-unknown-unknown
cargo run -p forest-test --bin package --locked
```

打包程序编译 `plugins/forest-test`，通过 `forest-package` 生成 `packages/forest-test/module.wasm`、外部 `manifest.toml` 和 `generated/` 下的 Schema/Value 文本。接口声明从同一份类型化处理器注册表产生，可在不执行 Wasm 的情况下读取。CLI、包目录和运行时共用包读取器；实际装载前校验全部索引文件，编译使用已校验的同一份 Wasm 字节。格式与可选作者、仓库、许可证等元信息见[模块 Manifest](07-module-manifest.md)。二进制构建产物不进入 Git。

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

模块 Serving 后，在允许群发送 `#forest-test`，模块回复一次 `hello world`。支持文本段及消息数组中对当前账号的 @。示例处理器显式持有 `/replies` 锁；发送成功后才持久化 `/state/replies`。其余命令不触发回复，未允许群不会进入模块或收到原生发送。

以上示例显式传入 `--allow-group` 以限制测试范围；该参数现在可选。省略时 OneBot 宿主不按群过滤，是否处理由接收模块决定，接口 ACL 始终检查。正常 Agent 实例只使用其配置表的 `allowed_groups`，见[Agent 启动](22-agent-guide.md)。需要宿主测试限制时才传此参数；`GroupScope::Only` 的空集合拒绝全部群，不将空集合解释为全放行。

默认路径均从启动工作目录解析：包 `packages/forest-test`，SQLite `data/forest.sqlite3`，受管文件 `data/modules`。分别可用 `--package`、`--database`、`--data-directory` 修改。

Ctrl+C 默认 Drain，可用 `--shutdown-mode yield` 或 `force` 显式选择。OneBot 正向 WS 在初次连接失败或运行中断开后自动重连，间隔按 1、2、4、8、16、30 秒退避；成功连接后恢复为 1 秒。连接恢复不重建调度器、模块或 Agent 会话，WebUI 继续运行。反向 WS 保持原监听器，连接结束后等待网关再次接入。

双向 WS 的动作队列和回执表属于单次连接。未连接时请求立即返回 Disconnected，连接关闭时尚未收到回执的请求返回 ReceiptLost，已排队动作随该连接释放；重连后的队列为空。未收到回执并不证明平台未执行，因此不自动重发消息或重试动作。HTTP action 传输不因事件 WS 重连而自动重试。连接、回执及模块诊断可在[调试台运行日志](14-inspection-webui.md)查看。

## 2. Rust 模块 SDK

Rust SDK 是可选的语言绑定。模块不链接调度器，也不链接被调用模块；其他语言只需提供规定的 Wasm 导出、使用固定宿主导入，并按 Schema/Value 编码数据。当前现成 SDK 与主要验证使用 Rust，尚未提供其他语言的 SDK。

实际 Rust 示例见 [forest-test](../plugins/forest-test/src/lib.rs)。实现 `forest_sdk::Module`：

- `create(CreateContext)` 返回模块对象，可以恢复配置表中的状态。
- `type LifecycleError: AsSchema + AsValue` 声明 create/destroy 的模块错误类型，其 Schema 写入 Manifest 的 `lifecycle_error`。无失败路径可用 `Infallible`。
- `handlers()` 用 `Handlers::with` 注册路径、访问规则和具体 Rust 异步函数；输入和完整 `Result<T, E>` 的 Schema 从函数类型生成。
- `destroy(Rc<Self>)` 可选，默认无操作。
- `export_module!` 生成固定 ABI 导出。

处理器取得 `Rc<Self>`、可信 `CallContext` 和具体输入类型。共享状态采用普通 Rust 作用域借用或内部可变性，不应持有与其他处理器冲突的借用跨 await。只有主动 Pending 才开放实例重入，调用宿主函数本身不开放。

`host::call` 返回 `Result<Result<T, E>, DispatchError>`：外层属于调度器，内层完全属于被调用方。宿主只检查内层 Schema，不根据 E 决定重试或补偿。`host::send` 只返回接纳结果，没有完成回执。

宿主操作使用独立函数和结构体：配置 `get/set/reset`、HTTP、WS、受管文件、时间和日志。配置必须在 Manifest 声明 Schema、默认值及 ACL；未知或未授权的对象/键默认拒绝。真实来源由宿主附加，参数不能伪造。

## 3. 嵌入与热重载

宿主通过 `Scheduler::new` 或 `with_store` 创建调度器，先注册原生接口及事件定义，再读取包。`NativeInterface::new` 从具体 Rust 函数生成输入及 Result 输出契约。

运行时默认启用 Wasmtime 的持久化编译缓存，位置为 `RuntimeOptions.data_directory/.wasmtime-cache`，CLI 中跟随 `--data-directory`。缓存只保存编译代码，不保存模块实例、配置或执行状态；普通加载与热重载使用相同路径。Wasmtime 按 Wasm 内容、编译设置、宿主目标与引擎构建信息区分缓存，更新代码或编译环境后自动重新编译。首次加载仍需编译，后续进程启动复用缓存；删除缓存目录只会使下次加载重新编译。目录由宿主维护，不向来宾开放。包准备完成后，运行日志记录模块 Token 和耗时。

部署时可先准备缓存，不创建来宾实例、不连接 OneBot 或调用模型 API：

```sh
cargo run --release --locked -p forest-runtime --example prepare_packages -- data/modules packages/typst packages/agent
```

最后的包目录可继续追加；该命令只准备明确列出的包，不自动遍历依赖。应在目标机器上使用与正式宿主一致的 Wasmtime 构建与编译设置准备缓存。Debug 构建还以可执行文件修改时间区分缓存，独立示例程序不能替 Debug 宿主预热；同一个 Debug 宿主重启可以复用。缓存是宿主管理的本机数据，不作为跨 CPU 或跨平台插件工件分发。

启动耗时应区分包读取、Wasm 编译与实例初始化。跨平台部署需在目标机器生成编译缓存，具体步骤见[Linux ARM64 部署](27-linux-deployment.md)。

| 操作 | 行为 |
|---|---|
| package(directory) | 读取 Manifest、编译 Wasm、校验固定导出与私有内存 |
| plan_manifests(manifests, roots) | 仅从声明求依赖闭包并作确定性排列，不编译 Wasm |
| plan_load(packages, roots) | 复用相同规划，返回编译包闭包 |
| load_roots(packages, roots) | 联合检查并创建依赖闭包，全部成功后统一开放入口 |
| load(package) | 单包装载，依赖必须已在目录中 |
| load_many(packages) | 联合检查并装载整个集合，包括循环依赖 |
| reload(package, mode) | 单模块管理请求；Drain 等自然静止再提交 |
| request_reload(packages, mode) | 请求批量重载已编译候选，返回可观察、可取消的 ManagementRequest |
| request_reload_from(targets, mode) | 先登记请求，再读取和编译候选目录；准备期间也可取消 |
| cancel(request_id) | 仅取消 Preparing / Waiting 请求，不回滚普通工作 |
| unload_many(objects, mode) | 检查集合之外没有使用者，再整组停止、卸载 |
| unload(object, mode) | 仍有静态使用者时拒绝，先移除使用者 |
| dependents(object) | 查询直接静态使用者，无执行副作用 |
| force(object) | 明确将该对象所属管理集合改为 Force，或中断已提交集合的当前实例 |
| shutdown(mode) | 取消待提交管理请求，等待或明确中断已提交交接，再整组卸载 |

静态依赖允许环，不提供普通调用死锁自动处理。计划不更改外部启用意图；装载集合的候选全部初始化成功后才统一发布，成员失败则整组不发布并清理。已存在的组外依赖保留，持久化和外部副作用不回滚。活动管理请求通过 Inspector 的 requests 展示，拥有 Preparing / Waiting / Committed 阶段和目标集合；请求句柄提供 snapshot、cancel 和 wait。

Drain 保持旧实例正常服务，等目标集合中已开始任务全部完成收尾后原子关闭入口；重载时尚未首次取得执行权的请求连同原返回通道与额度交给新实例，卸载则须执行完全部已接纳请求。AtYield 提交后等当前推进交回再中断其他旧请求；Force 中断原推进并跳过所有来宾析构。三者都等待已提交的本地 SQL/文件操作实际结束，之后才创建新实例，避免旧写入追上新状态。已接纳的下游调用不递归撤销，远端效果不回滚。

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

create 输入为 CreateContext，invoke 为 Invocation，两者只建立任务。poll 推进 Future，返回 `Pending`、`Output(Value)`、`Lifecycle(Result<(), Value>)` 或 `FrameTooLarge`。生命周期错误值必须符合当前实例 Manifest 的 `lifecycle_error`；不匹配返回 `OutputContract`。`FrameTooLarge` 表示结果无法用 wasm32 帧长度表达，调度器只结束该调用，不把它视为模块 trap。参数和结果收尾均属于同一次独占推进。

模块主动返回的生命周期错误由 `InstanceFailure::Lifecycle` 保留发生阶段、Schema 与原始 Value；运行时异常由 `InstanceFailure::Dispatch` 保留 DispatchError。`Scheduler::failure`、管理操作及 Inspector 均保留这一结构，日志和页面中的文字只是显示投影。初始化失败时，等待新实例的普通调用得到 `Unavailable`，不会把生命周期错误冒充为接口返回的 E。正常卸载完成资源释放后，仍会向管理调用者报告 destroy 的主动失败。热重载可采用不同的生命周期错误 Schema，错误始终携带所属实例的契约，不按模块名称重新解释。

SDK 的 `HostError` 分别保存 `DispatchError` 与 `ServiceError`，模块可以直接将它作为生命周期错误，或嵌入自己的错误枚举。配置和数据库错误不需要在 create/destroy 中转换为字符串。

ABI 结构与操作结构采用 `forest_protocol::abi` 固定小端、定长整数 bincode 编码；接口值和配置值仍是 Forest Schema/Value。帧长度按 wasm32 的 `u32` 表达，不另设 16 MiB 的固定传输上限。解码受实际输入字节长度约束并拒绝尾随字节，指针与长度必须对应实例线性内存中的有效范围；实例内存与待处理请求仍分别受运行时内存配额、队列字节配额约束。编码长度无法表达时返回调度错误，不能由 SDK `expect` 引发模块 panic。此边界没有版本协商。

宿主 I/O 上限保存在配置对象 `/core/runtime` 的 `/io` 中，HTTP 响应、WebSocket 单条消息与帧、受管文件读取分别使用 `http_response_mib`、`websocket_message_mib`、`file_read_mib`，默认各 64 MiB。每项以 MiB 为单位，允许 1 至 4095；最大值来自 wasm32 帧长度表示能力。新 HTTP 请求和文件读取在操作开始时读取设置，已有操作保留开始时的上限；WebSocket 在建连时固定消息与帧上限，现有连接不被设置写入打断。配置持久化，管理端写入沿用配置 set 和 `/user/admin` ACL；Wasm 模块不能修改宿主上限。管理端配置写请求本身保持独立的 16 MiB 请求体限制。I/O 设置不用于判断模型上下文预算。

这里描述的是字节协议，不是 Rust 对象内存布局。非 Rust 模块无须导入 Rust trait、crate 或 TypeId；需要实现相同的字节编码、任务推进和资源所有权约定。模块间调用按对象与接口地址派发，双方分别以自己的类型表示相同 Schema，在线性内存之间传递编码后的 Value。Manifest 的类型图和打包附带的 `.schema` 文本均可在不执行 Wasm、不读取 Rust 源码的情况下取得。

导入模块名为 `forest_host`。每个宿主操作有独立函数：

```text
call send lock unlock config_get config_set config_reset
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

lock/unlock 输入是编码后的具体 Token，结果为 DispatchResult<()>。lock 等待时返回 Pending；授予先由完成槽持有，take 成功后才转移到当前任务。unlock 提交时立即完成，非持有者返回 Denied。SDK 的 LockGuard 在所属任务内离开作用域时同步解锁；create/destroy 禁止这两项操作。任务回收在不可中断本地工作结束后释放残留锁。

句柄不授予身份权限，也不跨实例有效。无效控制句柄、越界内存、错误 ABI 调用属于协议破坏并 trap；正常拒绝、队列满及模块错误以 Result 返回。

## 5. 执行与资源

目录转换使用短临界区，不持锁执行 Wasm 或等 I/O。每个实例由一个拥有 Store 的异步驱动器推进；实例内 Start/Resume 共用去重 FIFO，实例间另有去重就绪队列。默认并行推进数为 2，可通过 RuntimeOptions 配置。

独立线程每 5 ms 推进 epoch，用于可恢复让出和显式 Force 检查，不是请求超时。Wasmtime Pending 保留原推进，SDK Pending 才释放实例执行权。已持有的宿主锁跨主动与被动暂停保留，按模块显式作用域释放。

默认每个对象保留最多 1024 个请求、64 MiB 输入，Wasm 内存上限 256 MiB。名额随请求、任务和必要收尾移动，满额原子拒绝，不经隐藏的无界入口排队。原生接口也受接纳容量约束，宿主实现用自己的类型化同步结构协调数据。

Wasm 使用的 HTTP/WS 原语不自动重试或重连；OneBot 宿主适配器的连接恢复与这些原语分开。受管文件由 capability directory 限制在实例目录内。卸载释放实例网络资源。配置与文件操作提交后必须等本地实际结束，丢弃 Future 不等于操作系统已撤销操作。

## 6. 验证与范围

```sh
cargo test --workspace --all-features --locked
```

集成测试编译真实 Wasm，使用两个代码不同、契约相同的候选。覆盖类型化调用、ACL、宿主锁互斥与等待交错、自唤醒和组合 Future、容量、三种卸载方式、代码替换、trap、单线程执行器下的抢占、配置恢复和延迟 SQLite 写入交接、文件隔离、HTTP/WS，以及实际 CLI 的群消息到 OneBot 回执。自动测试流量只进入本地模拟服务。

宿主注册 OneBot 群消息事件和 `/core/onebot` 的 `/send_group_message`、`/send_private_message`、`/query`；查询入口接受固定的只读 action 枚举，Agent 工具封装位于独立 [OneBot 工具包](28-onebot-agent-tools.md)。CLI 的 OneBot callers 授权 `/module/**`，因此独立加载的插件可使用这些原生接口；可选 `--allow-group` 额外限制群接收、查询与发送，并禁止私聊发送。CLI 另安装 `/core/control` 管理适配器，仅授权 `/module/shell`，见[群聊 Shell](26-group-shell.md)。可选 WebUI 已提供观测和模块装卸。Agent、Memory 与工具网关已由独立 Wasm 包实现，见[实现与启动](22-agent-guide.md)。热重载可更新同 Schema 的声明默认值并保留当前值；配置项集合或 Schema 变更仍拒绝。包分发与配置变更通知尚未实现。
