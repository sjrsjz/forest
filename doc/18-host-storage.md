# Host 物理存储所有权与回收协议

`host.storage` 是 Host scope 的可热重载 builtin 包，负责 Bot 等资源在本机的**物理所有权账本**。它不是 deployment 的存储配置页，也不能从数据库命名规则推断自己拥有某个旧资源。生产 Host Graph 会启动此包；生产 Bot 供给器现只消费已经提交的 profile，不再自行建库。预发布旧路径产生的资源仍需准确登记或重建；在 Host 清单完整之前，不允许开启自动回收。

## 权威与边界

| 参与者 | 持有的权威 |
| --- | --- |
| Host 内核 | 稳定 `host_id`、租约、Graph/Generation、路由与授权；不持有各模块的物理资源清单。 |
| deployment 内核 | Bot 基础身份、期望运行状态、逻辑 storage profile/head、供给 Job、备份/恢复 Job，以及各引用的冻结事实；builtin 只能通过受限端口提供管理操作。 |
| `host.storage` | 本 Host 的 `resource_id`、预留操作账本、物理名称、owner marker、物理状态与后续回收计划。账本位于 Host 独立数据库内该包的私有 schema，随 Worker 热替换保留。 |
| Host 原生 Effect | 根据 `host.storage` 的准确资源记录执行受限 PostgreSQL 操作。包 Worker 不接触维护凭据、超级用户 DSN 或其他作用域数据库连接。 |

包公开 `host.storage.reserve`、`host.storage.materialization.enqueue`、`host.storage.reference.pin` 幂等服务、`host.storage.materialization.status` 与 `host.storage.resource` 精确查询，以及 `generations` 键集分页查询；`host.storage.postgres.materialize` 是原生 Effect。两个精确查询都要求 `resource_id`、`deployment_id`、`bot_id`、`storage_generation` 同时匹配；资源查询只返回数据库名、role、状态与修订，不返回密码或 owner marker。预留服务也只返回资源引用及状态，不返回随机 owner marker。物理 Effect 输入仅有 `resource_id` 与受保护的 `credential_ref`；它从 Host 私有账本读取数据库名、专属 role、Host 身份和 marker，不能由调用者指定 SQL 对象名。包内持久任务独占私有 `realize-record` 命令：它接受 Effect receipt，在 Host 私有事务中重算 marker 摘要并核对资源、Host、数据库和 role 后，才把 `RESERVED` 推进为 `ACTIVE`；该命令不发布为跨组件服务。操作账本保留同键重放的原回执。

每个资源记录冻结 `host_id`、`deployment_id`、`bot_id`、`storage_generation`、种类、数据库名、role 名、随机 marker、修订和状态。`resource_id` 只生成一次；同一 `(operation_id, input_digest)` 重放返回原结果，同一操作 ID 异输入拒绝，同一逻辑 storage generation 的第二次预留拒绝。不同请求竞争同一资源由数据库唯一约束和事务锁合流。包 schema 的改变必须按包版本协议预发布；Worker 启动不得暗改 schema。

## PostgreSQL 物理创建

物理 Effect 在执行前校验准确 Host 租约与包 Generation，按数据库名取得 PostgreSQL session advisory lock，再按如下顺序处理：

1. 在一个事务内创建无超级用户、无建库/建角色/复制/BYPASSRLS 权限的专属 login role，并把包含 `resource_id`、`host_id`、随机 marker 的归属证据写入 role 注释。已有 role 必须同时匹配标记与权限；合法重试可更新密码。
2. 在事务外创建以该 role 为 owner 的数据库。PostgreSQL 不允许在普通事务内执行 `CREATE DATABASE`，因此不能假装建库和标记是原子动作。
3. 核对数据库 owner 与注释。若数据库存在而注释缺失，只有在资源仍处于 `RESERVED`、专属 role 已有准确 marker 且 owner 匹配时，才补写数据库注释；任何其他标记或 owner 都拒绝接管。
4. 再次验证 Host 租约与 Generation，返回不含凭据和原始 marker 的 receipt。数据库错误报告为需要按准确标记核对的 `UNKNOWN`，不能据此盲目分配另一个库。

物理动作与 Host 账本跨两个 PostgreSQL 数据库，不能共享事务。角色先写归属标记，使“角色已创建但数据库未创建”和“数据库已创建但注释未写入”两个崩溃窗口都能按同一资源核对。session advisory lock 约束并发重放；租约/Generation 校验约束新请求与热替换，但外部动作可能在校验后失去租约，所以它必须始终按准确名字和 marker 幂等核对。操作 receipt 不能代替 Host 账本中的 `ACTIVE` 结算。

## 跨作用域供给与回收

当前协议是：deployment 内核在冻结请求的同一事务中建立投递账本；可热重载的 `deployment.storage-orchestrator` 包通过声明式 owned-work 领取该请求，调用受限原生 Effect 按准确请求引用访问 Host；Host 持久 owned-work 领取物理 Effect 并记录意图和观察；Host `realize` 在账本事务中把已核实的 receipt 结算为 `ACTIVE`；deployment 验证并 pin 引用，再原子发布 DSN Secret 与逻辑 profile。投递账本的 `PENDING`、`SETTLED`、`BLOCKED` 不是 Host 物理状态：仅在准确 profile 已提交后才能标 `SETTLED`。崩溃后以原请求 ID 和准确资源引用查询并重放，绝不重新生成 marker 或另一个库；role 密码通过受控 Secret ref 解析；同一 storage generation 的凭据版本冻结，轮换须走新 generation 的物理与 DSN 切换协议。Host 的物理状态与 deployment 的逻辑引用状态是两种权威，WebUI 必须分别显示，不能用一方状态推断另一方已提交。

### Bot storage profile 切换顺序

旧 `storage.create-local` 曾在 deployment 事务中先生成 DSN Secret 和 immutable storage profile，随后由 `OfficialBotProvisioner` 调用 `LocalPostgresDatabaseAllocator.ensure_database`。该入口已从生产包 UI/端口与生产组合根移除。直接把 allocator 换成 Host 服务调用会让 profile 在 Host 接纳或物理结算前就可供 bootstrap/Job 引用，也会把跨数据库网络调用放进 deployment 事务，因此新路径采用下述分阶段协议。

目标切换分成以下顺序步骤，所有重试使用最初冻结的请求身份；已实现范围见下文状态：

1. deployment 事务只冻结 `request_id`、目标 `host_id`、`deployment_id`、`bot_id`、`storage_generation`、预期 `profile_ref`、专属凭据引用、版本与摘要，不创建可用的 storage profile。请求 ID 与同一逻辑 storage generation 的唯一约束要在本库合流并发写；不同输入的同键请求拒绝。
2. 跨 scope 桥以冻结的 deployment 身份投递准确请求。Host `reserve` 返回的资源 ID 必须在 deployment 持久化后才能继续；`materialization.enqueue`、`materialization.status` 和 `resource` 均按同一资源和 owner 元组重放。桥不能相信调用者自行填写的 `deployment_id`，必须从 deployment 权威行绑定。Host 状态未知时保留原资源引用等待核对，不重新预留。
3. Host 包的 owned-work 将准确资源推进到 `ACTIVE`。deployment 读取 Host 受限端口核对数据库名、role、Host 身份、owner 元组和修订，但这一次读不能单独充当持久引用围栏。`host.storage.reference.pin` 只接受同一归属且完成 owned-work 结算的 `ACTIVE` 资源，在 Host 私有账本内不可变地绑定预期 `profile_ref` 和资源修订，返回无凭据的签收；同一资源或同一 deployment/profile 的不同请求冲突。Host 回收协议以后必须把该 pin 当成尚未解除的引用，直到 deployment 证明相应 profile 已撤销并完成解绑定协议。
4. deployment 在一个本库事务中核对冻结请求和 Host 引用证据，写入最终 DSN Secret、immutable storage profile 与请求结算回执。只有这笔事务提交后，bootstrap profile 和 Bot 供给 Job 才能引用该 storage generation；供给器不再执行物理建库。

当前生产装配由 deployment SecretAuthority 持有 role 密码，生成端须提供强随机值，Host 原生 Effect 按准确 Secret ref 和用途解析；最终 settlement 也必须从同一冻结版本读取并构造 DSN。明文密码不得写入 Host 包表、Graph 计划或 Web 回执。以上跨库步骤没有 ACID，恢复逻辑只能从两端已提交的准确事实继续，不能凭超时推断物理动作失败。

回收须另有协议：deployment 先对目标 storage generation 施加禁止新引用的 fence，证明 Bot head、备份/恢复 Job 和下载租约均已结束或转移；Host 持久化回收计划，并在每次物理删除前重新验证 fence、引用水位、租约、资源修订、精确数据库名、role 和 marker。目标存在而归属不符时拒绝删除；外部结果未知时保留账本供核对。包卸载或热替换只排空 Worker、owned-work 与 Effect，不能删除账本或物理资源。其他资源种类（artifact、Milvus collection）必须各有原生标记和 Effect，不能复用 PostgreSQL 删除逻辑。

## 预发布实现状态（2026-10-07）

本节后续段落保留各阶段事实；当前装配以此段为准。deployment 内核持有 Bot 基础身份、期望运行意图、冻结的 Host 请求及投递租约；`deployment.storage-orchestrator` 1.0.0 是独立的可热重载 builtin，持有 Worker 工作处理入口、模块自带 WebUI 与受限管理端口，但不复制 Bot 目录。管理动作只冻结请求并由内核注入目标 Host；请求和 `storage_request_dispatch` 在同一事务创建。Worker 从内核账本排他领取、续租，调用幂等原生 Effect，经 `DeploymentHostStorageBridge` 预留/入队/查询/pin，再由 `DeploymentStorageSettlementAuthority` 原子发布 DSN Secret 与 profile。等待 Host 时按原请求延后重领；异常结果记录错误，明确阻断进入 `BLOCKED`；成功结算必须核对准确已提交的 profile。生产 `OfficialBotProvisioner` 已不再调用旧本地分配器，供应包旧 `storage.create-local` UI/端口已移除。独立 PostgreSQL 已验证请求并发领取与 fencing、真实双库桥结算，以及包 Worker 热替换后从同一账本继续处理；这些分别验证，不等于完整生产 Supervisor 的端到端 Host 建库与 Bot 供应验收。当前仍需完成该验收、旧预发布物理资源对账/重建、安全回收，以及删除只在测试使用的旧 allocator/本地管理命令。

role 密码现为每个请求的专属 deployment Secret：请求创建必须先核对 Secret 的 `storage.postgresql.password` 用途、口令长度与格式；生成端负责提供强随机值，在同一事务中冻结版本与摘要；请求与 Secret 写入共享凭据引用围栏，胜出的版本成为不可变事实，之后的新轮换写入拒绝。Host pin 的凭据摘要必须等于该请求摘要，最终结算再次核对冻结版本、Host pin 和实际口令。完整轮换要另建 storage generation，不能直接修改正在被物理 role 和 immutable DSN 引用的 Secret。预发布 deployment 基线变动，需重建测试库。

deployment 现有隔离的 `DeploymentStorageSettlementAuthority`：它只接受已冻结请求及 pin，在有效 Supervisor 租约下核对 Host Effect 所写 role 密码的 SHA-256 证据，以同一 deployment 事务创建加密 DSN Secret 和 immutable storage profile。已提交的准确 DSN/profile 是结算事实，同请求并发重放合流，无须再建第二张状态表；密码、租约或重放 endpoint 不匹配时拒绝。预留 DSN ref 的普通 Secret/profile 写端口不能抢占，包管理输入也不能提供内部 `pinned_request_id`。隔离双数据库真实登录验证通过。当前生产装配让 Host 原生 Effect 通过受控 deployment SecretAuthority 查询 role 密码，维护 endpoint 与旧本地 allocator 共用；settlement 尚未在生产调度路径中调用，也未固化 endpoint 的跨阶段身份。

deployment 基础 schema 现可持久化不可变的 Host 存储请求：`request_id` 从 deployment、Bot 与 storage generation 派生，同一逻辑键的相同输入合流，不同 Host/profile/凭据引用或版本冲突。请求在同一事务中验证强随机 role 密码 Secret，并冻结其引用、版本与摘要；不写 DSN Secret 或可用 profile。请求创建、旧 profile 创建及 DSN Secret 写入共享逻辑键围栏；已预留的 DSN ref 不能被旧写端口抢占。该 authority 目前只供内部调用；隔离装配已有 Host 桥，但管理动作、生产调度与最终结算尚未接线，不能把请求视为已供应的数据库。

deployment 另有不可变的请求到 Host `resource_id` 绑定事实。受信任的跨 scope 桥必须先按请求身份调用 Host `reserve`，再凭有效 deployment Supervisor 租约写入准确资源；绑定只核对并保存资源与归属，不把 Host 回执中的可变 `state`/`revision` 固定为资源身份。同一资源的 `RESERVED` 与后续 `ACTIVE` 回执可重放合流；同请求换资源、跨请求复用资源或归属不符均拒绝。另一个不可变 pin 事实以复合外键绑定同一请求与资源，记录 Host、资源修订、物理数据库和 role 名称；只有 Host pin 回执与准确资源查询都匹配冻结请求、且资源查询仍为 ACTIVE 时才能写入。生产桥尚未接线，这些事实本身也不发布可供 Bot 使用的 profile。

隔离装配现有受限 `DeploymentHostStorageBridge`：它只从已冻结的 deployment 请求构造 Host `reserve`、`materialization.enqueue`、`materialization.status`、`reference.pin` 与 `resource` 输入，不接受包 Worker 自填 Host 归属；每项调用有固定的 `request_id` 派生操作身份。Host 预留成功而 deployment 租约失效后热替换 Host 包，原请求重放仍绑定唯一 `resource_id`；随后对同一资源重复入队合流，查询准确归属直到 Host owned-work 将其物化为 `ACTIVE`。Host pin 成功而 deployment 写入前再次失租时，热替换后按同一操作身份重放，Host 只保留一个 pin，deployment 将 pin 与准确资源查询一起持久化。真实双数据库回归 `1 passed`，deployment catalog 全套 `30 passed`。生产组合根已启动 Host 包 Graph，但尚未调度此桥；profile 结算与安全回收仍待接线；预发布 deployment 基线变更后需重建对应测试库。

`host.storage` 包版本 `0.7.0`、私有 SQL `1.5.0`，现设置 `auto_install=true`。包私有表保存每个物理资源的一条物化请求、凭据引用、领取租约、尝试次数、结算结果和幂等回执；Worker 从包内持久队列领取后调用受 Generation 围栏约束的 PostgreSQL 原生 Effect，再由仅供包内任务声明的私有 `realize-record` 命令核对物理标记并提交 `ACTIVE`。Effect 对实际写入 role 的密码计算 SHA-256 摘要，摘要随实现回执、持久工作结算和 Host pin 冻结；deployment pin 账本也保存同一摘要，最终 DSN 结算必须以强随机密码核对，不能把摘要当作保密口令或允许弱口令。租约过期仅重领同一个 `resource_id`；Effect 按准确 role、数据库和 owner marker 复核，不能借重试分配新名字。包内私有命令由持久任务声明并经编译器核对，不开放旧 `host.storage.realize` 服务。精确资源与工作状态查询供跨 scope 桥按冻结归属查询；桥本身仍须把调用方 deployment 身份绑定到输入。独立 PostgreSQL 的 Host 包及双库桥回归 `3 passed`，包目录与 Host 编译器回归 `100 passed, 2 skipped`；该包已进入生产 Host Graph，但跨 scope 供给与最终结算仍只在隔离装配中验证。

生产 Host Graph 已绑定原生 Effect、deployment Secret 查询和维护 DSN；目前使用与旧本地 allocator 相同的 PostgreSQL endpoint。deployment 跨 scope 持久投递、冻结逻辑引用与安全回收仍缺失，现有 deployment 直接建库路径也还在。因而不能把 `host.storage` 账本视为全量物理库存，也不能据此执行删除。下一步须接通跨 scope 供给，再切断旧分配器，重建或准确登记预发布资源。
