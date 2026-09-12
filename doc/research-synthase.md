# synthase 源码调研记录

调研日期：2026-09-12。源目录：`../synthase`；读取时 HEAD 为 `0b19f77de23ab274dec21fd06c04d2db787a7a21`。本文依据当前工作树静态阅读，不代表源库已经构建通过，也没有运行 WASM、加载插件或修改源项目。文中行号对应调研时工作树；链接相对于本文件。

## 1. 定位与实现边界

synthase 已具备 **WASM 调用执行器、按包缓存、可重新读取包、宿主 Effect 调用入口、结构化参数校验，以及独立的层级权限抽象**。它可以提供 Forest 的执行与能力边界设计素材，但目前不能视为具备完整在线升级协议的插件内核，也不是已经接通权限执行链路的沙箱平台。

工作区包含 `synthase-core`、`synthase-serde`、`synthase-serde-marco`、`synthase-principal` 和根 crate。`synthase-core` 的 Cargo package 名实际为 `synthase-unit`，根 crate 主要重导出依赖，不能仅按目录名假定公共 API 已整合。证据：[根 Cargo.toml](../../synthase/Cargo.toml)，[core Cargo.toml](../../synthase/synthase-core/Cargo.toml)，[根 lib.rs](../../synthase/src/lib.rs) 第 1–14 行。

## 2. 已实现行为及证据

| 主题 | 实际实现 | 源码证据 |
| --- | --- | --- |
| WASM 引擎 | 启用 Wasmtime async；依据调度器配置选择 Wasm32/Wasm64；注册单一 `host::perform_effect` 宿主接口 | [scheduler/mod.rs](../../synthase/synthase-core/src/scheduler/mod.rs) 第 37–58、184–395 行 |
| 缓存与标识 | `HashSet<Arc<ScheduleUnit>>` 保存已加载包，`VecDeque<PathBuf>` 管理淘汰顺序；包相等与哈希只依赖路径 | [scheduler/mod.rs](../../synthase/synthase-core/src/scheduler/mod.rs) 第 26–34、165–180 行；[unit/mod.rs](../../synthase/synthase-core/src/unit/mod.rs) 第 39–57 行 |
| 包加载 | 规范化包目录路径；已加载时刷新队列后直接返回；新包读取 metadata、编译 WASM、检查声明的内存模型，再加入缓存 | [scheduler/mod.rs](../../synthase/synthase-core/src/scheduler/mod.rs) 第 122–150 行 |
| 包重新加载 | `reload_package(&mut self)` 读取 `metadata.json`，写入 metadata 后调用 WASM 重新编译；`offload_wasm_module` 清空 metadata 与 module | [unit/mod.rs](../../synthase/synthase-core/src/unit/mod.rs) 第 74–108 行 |
| 每次执行 | 校验输入；序列化；创建新的 `Store`；克隆 Linker 并创建新的 Instance；调用结束后释放本次实例上下文 | [unit/mod.rs](../../synthase/synthase-core/src/unit/mod.rs) 第 132–182 行 |
| 入口 ABI | 导出 `memory`、`synthase_alloc`、`synthase_dealloc`、`synthase_entry`；入口使用指针、长度及调用者提供的结果缓冲区；默认结果缓冲区 256 KiB | [unit/mod.rs](../../synthase/synthase-core/src/unit/mod.rs) 第 13–17、191–255、306–374 行 |
| 序列化 | 主入口输入/结果使用 bincode；Effect 的请求/结果使用 JSON；二者是不同协议 | [unit/mod.rs](../../synthase/synthase-core/src/unit/mod.rs) 第 150–151、279–284 行；[scheduler/mod.rs](../../synthase/synthase-core/src/scheduler/mod.rs) 第 234–258 行 |
| 宿主交互 | Guest 提供能力名、参数和要求的结果 schema；宿主得到当前 `ScheduleUnit`，调用泛型 `Handler::handle`，将结果写回 guest memory | [effect.rs](../../synthase/synthase-core/src/unit/effect.rs) 第 6–27 行；[scheduler/mod.rs](../../synthase/synthase-core/src/scheduler/mod.rs) 第 17–25、234–282 行 |
| Schema | Metadata 声明参数与结果 schema；入口实际校验输入/结果；`Ability::check` 定义名称、参数及结果子类型检查 | [metadata.rs](../../synthase/synthase-core/src/unit/metadata.rs) 第 36–55、109–133 行；[ability.rs](../../synthase/synthase-core/src/handler/ability.rs) 第 5–13 行 |
| 类型表达 | serde 层支持基本值、可选值、元组、数组、Map、Variant、Record；有结构子类型和 value/schema 检查 | [adt/mod.rs](../../synthase/synthase-serde/src/adt/mod.rs) 第 546–560、613–639、859–860 行 |
| 权限主体 | `Principal` 是路径；支持生成子路径、求父主体及严格祖先判断；Capability 通过 `covers` 表达覆盖关系 | [principal/lib.rs](../../synthase/synthase-principal/src/lib.rs) 第 3–47 行 |
| 权限委派 | Reader/Writer 由外部提供；`derive_child` 要求父权限覆盖子权限；修改后代检查祖先关系以及目标直接父节点的权限覆盖 | [principal/lib.rs](../../synthase/synthase-principal/src/lib.rs) 第 150–209 行 |

`Store`/`Instance` 每次调用创建，意味着当前代码没有持久化 guest 线性内存的会话 actor。`Arc<U>` 则在多次调用间共享宿主 Handler；其内部共享状态、并发隔离及外部 I/O 语义由实现者承担。证据：[unit/mod.rs](../../synthase/synthase-core/src/unit/mod.rs) 第 153–169 行；[scheduler/mod.rs](../../synthase/synthase-core/src/scheduler/mod.rs) 第 17–31、95–106 行。

## 3. 不能直接继承的行为与缺口

### 3.1 重新读包尚未形成原子的在线热重载

1. **更新已加载路径不会自动重新加载。** `Scheduler::load_unit` 在路径已存在时直接返回；调度器层没有文件监控、版本切换、依赖图更新、健康探测、排空或回滚协议。证据：[scheduler/mod.rs](../../synthase/synthase-core/src/scheduler/mod.rs) 第 122–150 行。
2. **metadata/module 可能混代。** `reload_package` 在编译新 WASM 前覆盖 metadata；读文件或编译失败时，旧的 `wasm_module` 可能仍存在，但 metadata 已经是新的。需要先构造完整候选代，再原子发布。证据：[unit/mod.rs](../../synthase/synthase-core/src/unit/mod.rs) 第 75–86、99–107 行。
3. **调用身份可能混代。** `call` 使用 `self.wasm_module`，但传入宿主 `ExecutionState.unit` 的值是按路径从调度器重新查询得到的。可复现的静态路径是：保留旧 `Arc<ScheduleUnit>` → 从调度器卸载该路径 → 加载同路径新包 → 在旧 Arc 上调用 `call`。执行的是旧 module，而 Effect 的 `original` 来自新包。若后续将权限绑定到 `original.metadata`，这会形成错误的授权身份。证据：[unit/mod.rs](../../synthase/synthase-core/src/unit/mod.rs) 第 145–168 行；[scheduler/mod.rs](../../synthase/synthase-core/src/scheduler/mod.rs) 第 69–86 行。此处是源码推导，未运行攻击或集成复现。
4. **缓存移除不是任务终止。** `offload_unit` 仅从集合/队列删除；已有 Arc 保留对象，接口没有撤销 lease、停止接单、撤销 effect 或等待在途任务的定义。证据：[scheduler/mod.rs](../../synthase/synthase-core/src/scheduler/mod.rs) 第 68–92 行。
5. **LRU 没有按实际执行更新。** 队列只在 `load_unit` 内刷新；`get_unit` 和 `call` 不刷新。故当前更接近按加载/显式触达顺序淘汰的包缓存，不能据此承诺高频执行模块不会被淘汰。证据：[scheduler/mod.rs](../../synthase/synthase-core/src/scheduler/mod.rs) 第 82–86、129–149、170–179 行；[unit/mod.rs](../../synthase/synthase-core/src/unit/mod.rs) 第 132–182 行。

### 3.2 Ability 检查不是权限执行

`Ability::check` 表达接口兼容性；`minimal_abilities` 表达依赖声明。二者都不能证明某个 Bot、某个用户或某次 Agent Run 有权执行某项副作用。

在调研到的调用链中，`host::perform_effect` 直接调用 `Handler::handle`，没有先调用 `Ability::check`，也没有调用 `Metadata::validate_abilities`。后者仅存在于定义处，未在调度器加载/执行入口接通。`synthase-core` 的依赖没有包含 `synthase-principal`，`ExecutionState` 也不携带 principal、授权上下文或 Bot 身份。证据：[ability.rs](../../synthase/synthase-core/src/handler/ability.rs) 第 5–13 行；[metadata.rs](../../synthase/synthase-core/src/unit/metadata.rs) 第 118–133 行；[scheduler/mod.rs](../../synthase/synthase-core/src/scheduler/mod.rs) 第 22–25、234–251 行；[core Cargo.toml](../../synthase/synthase-core/Cargo.toml)。

principal 库也仍是抽象草稿：

- `PrincipalPath::resolve` 直接为 `todo!()`，因此相对路径解析不能用于生产授权。[principal/lib.rs](../../synthase/synthase-principal/src/lib.rs) 第 85–87 行。
- `Principal.path` 私有，当前公开 API 中未见根 principal 的构造函数或默认构造实现；调用方需要明确可信根主体如何产生。第 9–48 行。
- Reader 返回 `&C`，接口未定义主体不存在、撤销、过期、策略版本、deny 优先级、鉴权审计、跨 Bot 边界与并发更新语义。第 150–165 行。
- `PrincipalPath::child` 对尾部 Parent 的处理、`parent` 对尾部 Child 的处理并非直接追加路径片段，且 `resolve` 尚未实现，不能假定与文件系统相对路径规则一致。第 95–130 行。
- `modify_descendant` 检查目标父节点的当前权限，但没有定义缩减父权限后既有子孙授权如何失效。若新框架仅复用此方法、继续信任缓存中的旧子孙授权，会缺少撤销传播。第 188–207 行。

### 3.3 WASM 沙箱还缺资源、错误和副作用协议

| 缺口 | 证据与影响 | Forest 应补充的契约 |
| --- | --- | --- |
| 无显式执行预算 | Engine 配置仅见 async 和 memory64；Store 创建未配置 fuel、epoch、资源限制或 deadline。无法从现有实现推断恶意死循环可被可靠中断 | 每次调用 CPU/时间/内存/并发预算；取消传播；超时后 fencing；隔离失效时终止进程 |
| 宿主分配未先限制请求大小 | Effect 直接按 guest 的 `effect_len` 创建 Vec，然后才读 guest memory：[scheduler/mod.rs](../../synthase/synthase-core/src/scheduler/mod.rs) 第 220–239、325–344 行 | 在分配和解码前检查最大包长、地址区间、嵌套深度和请求配额 |
| 副作用完成与结果返回脱节 | 先执行 Handler，再检查结果缓冲容量并写回内存；缓冲不足/目标地址错误可使 guest 收到错误，但副作用已完成：[scheduler/mod.rs](../../synthase/synthase-core/src/scheduler/mod.rs) 第 245–274 行 | `effect_id` 幂等键；执行记录；状态查询；把 transport/serialization failure 与业务执行结果区分 |
| ABI 无版本协商 | 入口与 Effect 各用一套编码，当前 metadata 没有显式 wire protocol 版本：[metadata.rs](../../synthase/synthase-core/src/unit/metadata.rs) 第 36–55 行 | 统一协议版本与 schema 标识；兼容性检查；codec limits；错误码；升级兼容矩阵 |
| 包路径边界未定义 | 包目录 canonicalize，但 WASM 文件直接读取 metadata 中的 `wasm_path`，未见与包目录 join/归属校验：[unit/mod.rs](../../synthase/synthase-core/src/unit/mod.rs) 第 75–85、99–107 行 | 将路径解析到受管 artifact，校验路径留在包根内；以内容哈希标识不可变制品 |
| 同步编译在 async 路径 | `reload_wasm_module` 内直接 `Module::new`：[unit/mod.rs](../../synthase/synthase-core/src/unit/mod.rs) 第 81–85 行 | 独立且有界的编译工作池；上线前预编译；编译故障不影响当前代 |

`wasmtime::Memory::read/write` 提供越界错误处理，入口也检查结果长度和 schema，这是已有防护；但这些检查不能替代进程资源配额、宿主参数限制或副作用幂等。

### 3.4 测试只能说明作者意图，当前不能当作生产验证

调度器测试覆盖创建、空容量、Wasm64 配置、空集合卸载和 Arc 共享等基础接口；名为 `test_lru_eviction` 的测试实际上仅断言初始缓存为空，注释明确没有加载真实包。证据：[scheduler/mod.rs](../../synthase/synthase-core/src/scheduler/mod.rs) 第 418–485 行，特别是第 468–474 行。

unit 测试集中于输入拒绝、缺少 WASM、相等/哈希与卸载；未见实际加载 guest 并跨越热切换验证在途 Effect 的集成用例。部分测试仍使用 `Value::from_values`、重复导入 `Value` 等与当前 `Value` 定义不一致的调用痕迹。证据：[unit/mod.rs](../../synthase/synthase-core/src/unit/mod.rs) 第 432、475–510 行；[effect.rs](../../synthase/synthase-core/src/unit/effect.rs) 第 83–88 行；[adt/mod.rs](../../synthase/synthase-serde/src/adt/mod.rs) 第 613–639 行。本文没有运行 cargo，因此不报告编译通过或精确编译失败结论。

## 4. Forest 可借鉴的设计思想

以下是新框架建议，不是 synthase 已实现功能。

1. **保留统一宿主 Effect 网关。** 模块只提交有 schema 的能力请求；内核负责将真实调用身份、Bot、run、generation、deadline、幂等键和审计上下文注入调用。guest 传入的能力名是请求，不是授权证明。
2. **保留模块清单和结构化边界。** 将 metadata 扩展为带接口版本、制品哈希、必选/可选依赖、权限需求、状态 schema 与迁移能力的 manifest。依赖满足、类型兼容和授权允许分别校验。
3. **保留短生命周期 WASM 调用作为一种执行模型。** 适合无状态变换、策略和工具；长期 Agent 状态交给独立服务持久化。无需为每个长期对话保存 WASM Store。
4. **继承权限只能收窄的原则。** 使用可信主体树和结构化资源约束；Agent Run、Tool Call 权限取 Bot 授权、模块授权、用户/任务上下文及委派范围的交集。权限按当前策略版本重新求值，不能由字符串路径祖先关系直接放行。
5. **把卸载动作提升为可观测的生命周期协议。** 引入 generation、注册表路由、调用 lease、资源托管、排空、健康检查、状态迁移和回滚；缓存淘汰与服务卸载是两个操作。

## 5. 对新框架生命周期与通信的具体约束

### 5.1 代际一致性

- 一个调用入口一次性取得 `ModuleInstanceRef(module_id, generation, artifact_hash)`；此对象同时决定实际代码、manifest、权限上限和宿主 original 身份。
- 新代先完成校验、初始化和健康检查，再通过注册表原子更新路由。旧代停止获得新租约，已持有租约的请求按规定继续、取消或检查点恢复。
- 每个宿主 effect 校验调用 lease 与 generation；被撤销、超时或排空终止的旧代不能继续提交新副作用。
- 数据库提交与外部发消息还需各自检查 fencing/幂等状态；仅在宿主入口检查一次不足以覆盖检查后到实际提交前发生的代际变更。
- 回滚定义为把后续流量切到兼容代；已经发送的消息、数据库提交和外部工具动作不能通过指针切换撤回。

### 5.2 状态与多 Bot

- `bot_id` 是身份、记忆所有权、授权和资源配额的边界；模块包路径不承担这些身份职责。
- 模块运行时状态、每个 Bot 的长期记忆、每条 Conversation 的短期队列、每个 Agent Run 的检查点分别建模；同一 Bot 的长期记忆不能随群聊拆库。
- 同一 Bot 可在多个群聊并发运行；对话 lane 维持本会话顺序，共享记忆提交通过短事务、版本检查和幂等保证一致性，不使用覆盖全 Bot 的长时间推理锁。
- 记忆读取可以形成一致性快照；LLM 推理期间不持锁；提交时检查所依据的版本与冲突策略。全局记忆并不意味着所有来源的原始信息均可无条件向任意群聊输出，来源和传播范围应保留在记录中。

### 5.3 有界且可恢复的通信

- 请求/响应定义 deadline、取消、错误分类和幂等键；事件定义持久化边界、顺序键、消费确认、重试与死信。
- Effect 提交后响应丢失时，可以按同一 `effect_id` 查询/重取结果；调用者不得凭超时推断副作用尚未执行。
- 长时间外部 I/O 不阻塞模块控制面；依赖调用避免长期持有注册表/状态锁；嵌套能力调用有深度、预算和循环保护。
- WASM、独立进程模块和受信任内置模块共用同一通信与授权契约，只有执行适配器不同。

## 6. 应转化为 Forest 验收用例的场景

| 场景 | 应验证的结果 |
| --- | --- |
| 新 WASM 编译失败 | 当前 generation 的代码、manifest、授权与路由均不变 |
| 保留旧模块引用后上线新代 | 旧代码始终携带旧代身份，不取得新代权限 |
| 旧代被撤销时有外部 I/O 在途 | 后续提交受 fencing 控制；已完成副作用可以查询结果 |
| guest 请求超大 Effect payload | 分配/解码之前拒绝，不耗尽宿主内存 |
| Effect 已完成但响应缓冲无效 | 按幂等键取回既有结果，不执行第二次 |
| 父授权缩减或 Bot 停用 | 子 Run 的下一次能力调用受新策略约束 |
| 同 Bot 两群聊同时推理并提交记忆 | 推理并发，短事务提交不丢更新，对话路由不串群 |
| 不同 Bot 使用相同外部群号和用户号 | 会话队列、授权和记忆均按 Bot 隔离 |
| 长任务期间模块排空超时 | 检查点/取消/强制终止结果明确，资源和租约最终回收 |
