# 运行时、SDK 与 Wasm ABI

本文说明运行时 API、Rust SDK、固定 Wasm ABI 与资源限制。调度语义以[调度状态机](12-scheduler-state-machine.md)为准。

| crate | 职责 |
|---|---|
| forest-protocol | 已解析的 Manifest、调度异常、操作数据和固定字节编码 |
| forest-package | 宿主侧 TOML 读写、Forest 文本引用解析及包文件 SHA-256 校验 |
| forest-sdk | 类型化处理器、来宾 Future、固定导出和唤醒 |
| forest-runtime | 唯一装载目录、请求接纳、实例执行权、生命周期、鉴权和资源 |
| forest-cli | 包装载、宿主配置、关闭与可选 WebUI |

## 1. 构建并运行

从仓库根目录执行：

```sh
rustup target add wasm32-unknown-unknown
cargo run -p forest-test --bin package --locked
```

打包程序编译 `plugins/forest-test`，通过 `forest-package` 生成 `packages/forest-test/module.wasm`、外部 `manifest.toml` 和 `generated/` 下的 Schema/Value 文本。接口声明从同一份类型化处理器注册表产生，可在不执行 Wasm 的情况下读取。CLI、包目录和运行时共用包读取器；实际装载前校验全部索引文件，编译使用已校验的同一份 Wasm 字节。格式与可选作者、仓库、许可证等元信息见[模块 Manifest](07-module-manifest.md)。二进制构建产物不进入 Git。

宿主无需平台参数即可运行，未指定根包时不加载模块：

```sh
cargo run -p forest-cli --locked -- --webui 127.0.0.1:8081
```

启用 WebUI 前设置非空 FOREST_WEB_PASSWORD，不需要用户名。CLI 不自动读取 .env。

Bot 示例另外构建普通 OneBot 包，根包的订阅会把它纳入依赖闭包：

```sh
cargo run -p forest-onebot-module --bin package-onebot --locked
cargo run -p forest-cli --locked -- --package packages/forest-test --webui 127.0.0.1:8081
```

在 /module/onebot 的 /settings 设置 websocket、token、connect_timeout_ms、action_timeout_ms、reconnect_min_ms、reconnect_max_ms 和 allowed_groups，然后重载此包。默认连接 ws://127.0.0.1:8080；allowed_groups 为空表示不额外限制平台范围。正常 Agent 群启用仍由 Agent 配置决定。首版支持正向 WS，反向监听需要未来的通用监听资源。

在群中发送 #forest-test，示例包回复 hello world。它持有实例内 /replies 锁，发送成功后才更新 /state/replies。

路径相对启动目录解析：包目录 packages，SQLite data/forest.sqlite3，资源 data/modules。--package 可指定多个根包，--packages-directory 修改可用包目录。--controller 显式授权 TokenPattern 调用 /core/control；例如 Shell 需要 --controller /module/shell，默认不授权模块。

Ctrl+C 默认 Drain，--shutdown-mode yield/force 可显式中断。等待 Drain 时管理台继续服务。OneBot 自行维护重连与抖动退避，断线以 ReceiptLost 结算未返回动作，不自动重放可能已执行的请求。宿主不解释这些语义。

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

每个二进制 Value 和完整 Schema 都是自包含的符号文档：先传动态名称表，再传引用该表的结构正文。`NONE_SYMBOL = 0`、`SOME_SYMBOL = 1` 为保留名字，动态符号从 `FIRST_DYNAMIC_SYMBOL` 开始；这些声明集中于 `forest_serde::symbol`，编码和解码不在分支内硬编码保留编号。Optional 与普通枚举共用类型语义及 Variant 编码，保留名字不改变载荷规则。名称表、节点编号、字段顺序及独立编码例子见[符号表二进制编码](37-binary-symbols.md)，非 Rust 绑定必须实现同一布局。

编码器以受限 Writer 检查实际写入字节数，不先运行一次序列化估算长度；符号表只构建一次，正文借用现有值树并直接写入输出缓冲。解码器在恢复逻辑值时解析符号索引，拒绝表中重复名称、重复声明保留名称、越界索引及重复键。没有跨帧字典协商，也没有旧字节布局自动识别；宿主、SDK 与 Wasm 工件须配套构建。

宿主 I/O 上限保存在配置对象 `/core/runtime` 的 `/io` 中，HTTP 响应、WebSocket 单条消息与帧、受管文件读取分别使用 `http_response_mib`、`websocket_message_mib`、`file_read_mib`，默认各 64 MiB。每项以 MiB 为单位，允许 1 至 4095；最大值来自 wasm32 帧长度表示能力。新 HTTP 请求和文件读取在操作开始时读取设置，已有操作保留开始时的上限；WebSocket 在建连时固定消息与帧上限，现有连接不被设置写入打断。配置持久化，管理端写入沿用配置 set 和 `/user/admin` ACL；Wasm 模块不能修改宿主上限。管理端配置写请求本身保持独立的 16 MiB 请求体限制。I/O 设置不用于判断模型上下文预算。

这里描述的是字节协议，不是 Rust 对象内存布局。非 Rust 模块无须导入 Rust trait、crate 或 TypeId；需要实现相同的字节编码、任务推进和资源所有权约定。模块间调用按对象与接口地址派发，双方分别以自己的类型表示相同 Schema，在线性内存之间传递编码后的 Value。Manifest 的类型图和打包附带的 `.schema` 文本均可在不执行 Wasm、不读取 Rust 源码的情况下取得。

导入模块名为 `forest_host`。每个宿主操作有独立函数：

```text
call send emit lock unlock config_get config_set config_reset
http_request ws_connect ws_send ws_receive ws_close ws_watch timer_after
file_read file_write file_remove sqlite sleep log now monotonic_now random_bytes
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

默认每个对象普通请求和 I/O 通知分别保留最多 1024 个名额、64 MiB 输入，Wasm 内存上限 256 MiB。名额随请求、任务和必要收尾移动，满额原子拒绝，不经隐藏的无界入口排队。原生接口也受接纳容量约束，宿主实现用自己的类型化同步结构协调数据。

Wasm 使用的 HTTP/WS 原语不自动重试或重连；OneBot Wasm 接入包的连接恢复与这些原语分开。受管文件由 capability directory 限制在实例目录内。卸载释放实例网络资源。配置与文件操作提交后必须等本地实际结束，丢弃 Future 不等于操作系统已撤销操作。

## 6. 验证与范围

```sh
cargo test --workspace --all-features --locked
```

集成测试编译真实 Wasm，使用两个代码不同、契约相同的候选。覆盖类型化调用、ACL、宿主锁互斥与等待交错、自唤醒和组合 Future、容量、三种卸载方式、代码替换、trap、单线程执行器下的抢占、配置恢复和延迟 SQLite 写入交接、文件隔离、HTTP/WS，以及实际 CLI 的群消息到 OneBot 回执。自动测试流量只进入本地模拟服务。

OneBot 群消息与发送/查询接口由 /module/onebot 提供，与 Agent、Memory、工具一样是独立包。CLI 仅安装通用 /core/control 并按显式 controller 参数授权。配置默认值可在同 Schema 重载时更新并保留当前值；配置集合或 Schema 变更仍拒绝。

## 7. 激活、通知与 I/O 期限

Manifest 的 module.activated 指向 Unit 输入的普通接口，callers 允许 /system/lifecycle。整组发布后，调度器用独立控制名额投递一次激活；它可以使用普通能力并等待，不阻止其他处理器开始。

模块事件和实例通知的权限与重载区别见[事件与依赖](11-notifier-and-dependencies.md)。空闲监听不占任务；已接受的实例通知不能转交下一代。HTTP、WS connect/send 的 timeout_ms 由调用方决定，超时返回 ServiceError::Timeout；HTTP 期限包括完整 body。monotonic_now 返回进程内单调毫秒，random_bytes 使用操作系统安全随机源。完整接口见[宿主 API](08-host-interfaces.md)。
