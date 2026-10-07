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
2. 在事务外创建以该 role 为 owner、连接数限制为零的数据库。PostgreSQL 不允许在普通事务内执行 `CREATE DATABASE`，因此不能假装建库和标记是原子动作。
3. 核对数据库 owner、注释、连接限制和 ACL。新库先撤销 `PUBLIC` 的全部数据库权限，再写入归属注释，最后开放连接。崩溃重放只接续准确标记且权限已收紧的中间态；存在外部授权、角色成员关系、错误连接限制或已有标记下的 `PUBLIC` 授权时拒绝接管。无标记数据库只有在资源仍处于 `RESERVED`、专属 role 已有准确 marker、owner 匹配且仍禁止连接时才可接续。
4. 再次验证 Host 租约与 Generation，返回不含凭据和原始 marker 的 receipt。数据库错误报告为需要按准确标记核对的 `UNKNOWN`，不能据此盲目分配另一个库。

物理动作与 Host 账本跨两个 PostgreSQL 数据库，不能共享事务。角色先写归属标记，使“角色已创建但数据库未创建”和“数据库已创建但注释未写入”两个崩溃窗口都能按同一资源核对。session advisory lock 约束并发重放；租约/Generation 校验约束新请求与热替换，但外部动作可能在校验后失去租约，所以它必须始终按准确名字和 marker 幂等核对。操作 receipt 不能代替 Host 账本中的 `ACTIVE` 结算。

## 跨作用域供给与回收

当前协议是：deployment 内核在冻结请求的同一事务中建立投递账本；可热重载的 `deployment.storage-orchestrator` 包通过声明式 owned-work 领取该请求，调用受限原生 Effect 按准确请求引用访问 Host；Host 持久 owned-work 领取物理 Effect 并记录意图和观察；Host `realize` 在账本事务中把已核实的 receipt 结算为 `ACTIVE`；deployment 验证并 pin 引用，再原子发布 DSN Secret 与逻辑 profile。投递账本的 `PENDING`、`SETTLED`、`BLOCKED` 不是 Host 物理状态：仅在准确 profile 已提交后才能标 `SETTLED`。崩溃后以原请求 ID 和准确资源引用查询并重放，绝不重新生成 marker 或另一个库；role 密码通过受控 Secret ref 解析；同一 storage generation 的凭据版本冻结，轮换须走新 generation 的物理与 DSN 切换协议。Host 的物理状态与 deployment 的逻辑引用状态是两种权威，WebUI 必须分别显示，不能用一方状态推断另一方已提交。

### Bot storage profile 切换顺序

供应包的公开 `storage.create` 管理动作已移除。它允许管理员绕过 Host 资源账本提交任意 DSN/profile；现在 WebUI 只能通过存储编排包提交 Host 请求。profile 的受信任 authority 保留给 Host 结算与隔离测试，不能作为普通包管理端口暴露。

旧 `storage.create-local` 曾在 deployment 事务中先生成 DSN Secret 和 immutable storage profile，随后由 `OfficialBotProvisioner` 调用 `LocalPostgresDatabaseAllocator.ensure_database`。该入口已从生产包 UI/端口与生产组合根移除。直接把 allocator 换成 Host 服务调用会让 profile 在 Host 接纳或物理结算前就可供 bootstrap/Job 引用，也会把跨数据库网络调用放进 deployment 事务，因此新路径采用下述分阶段协议。

目标切换分成以下顺序步骤，所有重试使用最初冻结的请求身份；已实现范围见下文状态：

1. deployment 事务只冻结 `request_id`、目标 `host_id`、`deployment_id`、`bot_id`、`storage_generation`、预期 `profile_ref`、专属凭据引用、版本与摘要，不创建可用的 storage profile。请求 ID 与同一逻辑 storage generation 的唯一约束要在本库合流并发写；不同输入的同键请求拒绝。
2. 跨 scope 桥以冻结的 deployment 身份投递准确请求。Host `reserve` 返回的资源 ID 必须在 deployment 持久化后才能继续；`materialization.enqueue`、`materialization.status` 和 `resource` 均按同一资源和 owner 元组重放。桥不能相信调用者自行填写的 `deployment_id`，必须从 deployment 权威行绑定。Host 状态未知时保留原资源引用等待核对，不重新预留。
3. Host 包的 owned-work 将准确资源推进到 `ACTIVE`。deployment 读取 Host 受限端口核对数据库名、role、Host 身份、owner 元组和修订，但这一次读不能单独充当持久引用围栏。`host.storage.reference.pin` 只接受同一归属且完成 owned-work 结算的 `ACTIVE` 资源，在 Host 私有账本内不可变地绑定预期 `profile_ref` 和资源修订，返回无凭据的签收；同一资源或同一 deployment/profile 的不同请求冲突。Host 回收协议以后必须把该 pin 当成尚未解除的引用，直到 deployment 证明相应 profile 已撤销并完成解绑定协议。
4. deployment 在一个本库事务中核对冻结请求和 Host 引用证据，写入最终 DSN Secret、immutable storage profile 与请求结算回执。只有这笔事务提交后，bootstrap profile 和 Bot 供给 Job 才能引用该 storage generation；供给器不再执行物理建库。

当前生产装配由 deployment SecretAuthority 持有 role 密码，生成端须提供强随机值，Host 原生 Effect 按准确 Secret ref 和用途解析；最终 settlement 也必须从同一冻结版本读取并构造 DSN。明文密码不得写入 Host 包表、Graph 计划或 Web 回执。以上跨库步骤没有 ACID，恢复逻辑只能从两端已提交的准确事实继续，不能凭超时推断物理动作失败。

回收须另有协议：deployment 先对目标 storage generation 施加禁止新引用的 fence，证明 Bot head、备份/恢复 Job 和下载租约均已结束或转移；Host 持久化回收计划，并在每次物理删除前重新验证 fence、引用水位、租约、资源修订、精确数据库名、role 和 marker。目标存在而归属不符时拒绝删除；外部结果未知时保留账本供核对。包卸载或热替换只排空 Worker、owned-work 与 Effect，不能删除账本或物理资源。其他资源种类（artifact、Milvus collection）必须各有原生标记和 Effect，不能复用 PostgreSQL 删除逻辑。

Bot 退役的第一道 deployment 内核围栏现已实现：`begin_retirement` 在锁住 Bot 身份行和运行意图后，把 `READY` 变为 `DELETING`、把期望状态强制写成 `OFFLINE` 并递增意图修订；同一操作重放合流，另一操作不能再次开启退役。普通运行意图命令在非 `READY` 状态拒绝；Supervisor 的 `STARTING/ONLINE` 观测与退役并发时按 Bot 行锁排序，失效的启动被拒绝，`STOPPING/OFFLINE/FAILED` 仍可记录。此状态只表示禁止重新启动，不证明旧运行图已关闭，也不授权解除 Host pin 或删除物理库。退役确认、其他引用的撤销和物理回收仍须后续协议完成。

## 预发布实现状态（2026-10-07）

生产 Supervisor 已把 Host 作为独立 Web 管理作用域发布。`host.storage` 是 Host Graph 内可热重载的 builtin 包，自带资源清单、配置入口和私有 owned-work；Host Web 票据、回执与 Graph 校验均绑定完整 `host_id`。隔离 PostgreSQL 的实际 HTTP、包换代和 Supervisor 端到端验收已覆盖 Host 建库、pin/结算、Bot READY 与 OFFLINE 初始意图。

deployment 内核独占 Bot 基础身份、期望运行意图、冻结的 Host 存储请求与投递租约。`deployment.storage-orchestrator` 作为可热重载 builtin 领取请求，经受限 Effect 与 `DeploymentHostStorageBridge` 完成 Host 预留、物化、精确查询与 pin；`DeploymentStorageSettlementAuthority` 在 deployment 事务内核对冻结凭据、pin 和资源，再发布 DSN Secret 与不可变 storage profile。供给器只消费已提交 profile，不再本地建库。请求、Host 资源绑定和 pin 使用稳定身份与围栏合流，重试不能另建数据库。role 口令以强随机 Secret 冻结，轮换须建立新的 storage generation。

Host PostgreSQL Effect 创建库时先限制连接数为零，撤销 `PUBLIC` 数据库权限并写入准确归属 marker，之后才开放连接；已有库或 role 的外部授权与错误归属均拒绝接管。隔离 PostgreSQL 验收已确认 Bot role 可进入专属库，而另一有效登录角色无法连接该库。

本机旧预发布资源仍未纳入 Host 账本，其中四组旧 Bot 库和 role 仍被运行中的 deployment 引用。安全退役、pin 释放和物理回收协议尚未实现；在完成引用围栏及旧资源重建前，不能把 Host 账本当作全量物理库存，也不能逐库删除旧资源。真实 Compose 长时在线验收仍待完成。
