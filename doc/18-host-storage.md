# Host 物理存储所有权与回收协议

`host.storage` 是 Host scope 的可热重载 builtin 包，负责 Bot 等资源在本机的**物理所有权账本**。它不是 deployment 的存储配置页，也不能从数据库命名规则推断自己拥有某个旧资源。当前 deployment 中的 `LocalPostgresDatabaseAllocator` 仍能直接建库；在这条旧路径移除且旧资源经过准确登记或重建之前，Host 清单不完整，不允许开启自动回收。

## 权威与边界

| 参与者 | 持有的权威 |
| --- | --- |
| Host 内核 | 稳定 `host_id`、租约、Graph/Generation、路由与授权；不持有各模块的物理资源清单。 |
| deployment | Bot 身份、逻辑 storage profile/head、供给 Job、备份/恢复 Job，以及各引用的冻结事实。 |
| `host.storage` | 本 Host 的 `resource_id`、预留操作账本、物理名称、owner marker、物理状态与后续回收计划。账本位于 Host 独立数据库内该包的私有 schema，随 Worker 热替换保留。 |
| Host 原生 Effect | 根据 `host.storage` 的准确资源记录执行受限 PostgreSQL 操作。包 Worker 不接触维护凭据、超级用户 DSN 或其他作用域数据库连接。 |

包提供 `host.storage.reserve`、`host.storage.realize` 幂等服务和 `generations` 键集分页查询，声明 `host.storage.postgres.materialize` 原生 Effect。预留服务只返回资源引用及状态，不返回随机 owner marker。物理 Effect 输入仅有 `resource_id` 与受保护的 `credential_ref`；它从 Host 私有账本读取数据库名、专属 role、Host 身份和 marker，不能由调用者指定 SQL 对象名。`realize` 接受 Effect 的完整 receipt，在 Host 私有事务中重算 marker 摘要并核对资源、Host、数据库和 role 后，才把 `RESERVED` 推进为 `ACTIVE`；操作账本保留同键重放的原回执。

每个资源记录冻结 `host_id`、`deployment_id`、`bot_id`、`storage_generation`、种类、数据库名、role 名、随机 marker、修订和状态。`resource_id` 只生成一次；同一 `(operation_id, input_digest)` 重放返回原结果，同一操作 ID 异输入拒绝，同一逻辑 storage generation 的第二次预留拒绝。不同请求竞争同一资源由数据库唯一约束和事务锁合流。包 schema 的改变必须按包版本协议预发布；Worker 启动不得暗改 schema。

## PostgreSQL 物理创建

物理 Effect 在执行前校验准确 Host 租约与包 Generation，按数据库名取得 PostgreSQL session advisory lock，再按如下顺序处理：

1. 在一个事务内创建无超级用户、无建库/建角色/复制/BYPASSRLS 权限的专属 login role，并把包含 `resource_id`、`host_id`、随机 marker 的归属证据写入 role 注释。已有 role 必须同时匹配标记与权限；合法重试可更新密码。
2. 在事务外创建以该 role 为 owner 的数据库。PostgreSQL 不允许在普通事务内执行 `CREATE DATABASE`，因此不能假装建库和标记是原子动作。
3. 核对数据库 owner 与注释。若数据库存在而注释缺失，只有在资源仍处于 `RESERVED`、专属 role 已有准确 marker 且 owner 匹配时，才补写数据库注释；任何其他标记或 owner 都拒绝接管。
4. 再次验证 Host 租约与 Generation，返回不含凭据和原始 marker 的 receipt。数据库错误报告为需要按准确标记核对的 `UNKNOWN`，不能据此盲目分配另一个库。

物理动作与 Host 账本跨两个 PostgreSQL 数据库，不能共享事务。角色先写归属标记，使“角色已创建但数据库未创建”和“数据库已创建但注释未写入”两个崩溃窗口都能按同一资源核对。session advisory lock 约束并发重放；租约/Generation 校验约束新请求与热替换，但外部动作可能在校验后失去租约，所以它必须始终按准确名字和 marker 幂等核对。操作 receipt 不能代替 Host 账本中的 `ACTIVE` 结算。

## 跨作用域供给与回收

目标协议是：deployment 在自身事务中持久化稳定的 Host 请求引用；受限跨 scope 路由调用 `reserve`；Host 持久 owned-work 领取物理 Effect 并记录意图和观察；Host `realize` 在账本事务中把已核实的 receipt 结算为 `ACTIVE`；deployment 再冻结逻辑 profile/head 并供给 Bot。崩溃后以原请求 ID 和准确资源引用查询并重放，绝不重新生成 marker 或另一个库；role 密码通过受控 secret ref 解析并可受控轮换。Host 的物理状态与 deployment 的逻辑引用状态是两种权威，WebUI 必须分别显示，不能用一方状态推断另一方已提交。

回收须另有协议：deployment 先对目标 storage generation 施加禁止新引用的 fence，证明 Bot head、备份/恢复 Job 和下载租约均已结束或转移；Host 持久化回收计划，并在每次物理删除前重新验证 fence、引用水位、租约、资源修订、精确数据库名、role 和 marker。目标存在而归属不符时拒绝删除；外部结果未知时保留账本供核对。包卸载或热替换只排空 Worker、owned-work 与 Effect，不能删除账本或物理资源。其他资源种类（artifact、Milvus collection）必须各有原生标记和 Effect，不能复用 PostgreSQL 删除逻辑。

## 预发布实现状态（2026-10-07）

`host.storage` 包版本 `0.3.0`，故意设置 `auto_install=false`。已有私有预留/结算账本、`generations` 查询、自带管理 UI、声明式内部幂等服务和 PostgreSQL 原生物理 Effect。隔离 PostgreSQL 的既有 Worker 验证涵盖预留重放/冲突、真正建库、角色与数据库标记、缺失数据库标记的恢复、错误归属标记拒绝，以及 Worker 热替换后继续核对；新增的结算 SQL 已在独立 PostgreSQL 中验证伪造 receipt 拒绝、`ACTIVE` 转移和重放。新增 `realize` 的完整 Worker 路径仍待数据库端口通路恢复后验证。

目前原生 Effect 只在隔离测试中绑定维护 DSN 与凭据解析器；生产 Host Graph 尚未装配它，也没有 Host 持久 owned-work、deployment 跨 scope 投递、冻结引用与安全回收。现有 deployment 直接建库路径仍在。因而不能启用生产 `host.storage`、把它的 UI 清单视为全量物理库存，或据此执行删除。下一步应确定 role 凭据权威，接通 Host 持久工作与跨 scope 供给，再切断旧分配器并重建/准确登记预发布资源。
