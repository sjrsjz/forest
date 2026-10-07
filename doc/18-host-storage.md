# Host 物理存储所有权与回收协议

本文规定 `host.storage` 的实现边界。它是可热重载的 Host scope builtin 包，不是部署后台的存储页面。现有 `LocalPostgresDatabaseAllocator` 直接从 deployment 进程创建 Bot 数据库，且没有 Host 物理资源账本；在这条路径被替换、历史预发布资源被重建或明确登记之前，不能把 `host.storage` 的空清单当成真实库存，也不能开放回收。

## 所有权和接口

| 项目 | 契约 |
| --- | --- |
| owner | `host.storage` 持有本 Host 的物理资源清单、预留/兑现/回收 Job 和操作账本；Host 内核只持有稳定 `host_id`、租约、Graph/Generation 与授权。deployment 持有 Bot 身份、逻辑 storage profile/head、备份/恢复 Job 和引用冻结事实。 |
| namespace / repository | Host 独立 PostgreSQL 库内的 `forest.host.storage` 模块 schema，repository contract `forest.host-storage.repository/1`。历史资源记录不随 Worker Generation 卸载；包 schema 变更须经预发布重建或显式版本协议，不准由 Worker 启动时暗改。 |
| provides | 管理查询 `generations`、`cleanup.plan`、`cleanups`、`cleanup`；管理动作 `cleanup.apply`、`cleanup.retry`。内部契约提供 `reserve`、`realize`、`detach` 与 `hold.release`，每个动作有独立输入 schema 和操作 ID，不设带 `mode` 的总入口。 |
| requires | Host 内核身份/租约和 Generation 校验；仅被授予准确 Host 包的原生 PostgreSQL、artifact、向量集合维护 Effect；deployment 通过有界跨 scope 请求提供逻辑引用与冻结证明。Worker 不取得超级用户 DSN、文件路径或其他作用域数据库连接。 |
| UnitOfWork | 单条 Host 请求的操作账本、资源修订、Effect 意图和 Web 回执在 Host 库同一事务提交。物理动作由持久 owned-work 领取，完成后凭外部事实结算；deployment/Host 两库之间没有分布式事务。 |
| management | 页面、筛选、来源/所有者、容量、计划与 Job 状态随包 Generation 发布。只读页按 `(kind, state, owner, resource_id)` 的服务端索引和稳定键集分页，不全量物化资源。所有入口经过 scope provider 与准确 Generation。 |
| concurrency | 每个资源用 `resource_id`、资源修订、Host lease epoch、Effect operation ID、外部 owner marker 与物理名称围栏。同键同输入重放返回原结果；同键异输入拒绝；不同请求争用同一资源按修订 CAS 合流。 |
| deletion | 卸载包只排空其 Worker、owned-work 和原生 Effect，不能删除资源账本或实体。资源删除必须经过独立 `cleanup.plan`、`cleanup.apply`、`cleanup.retry`；任一步 UNKNOWN 保留原记录供核对。 |

`resource_id` 是只生成一次的随机资源引用；Host 账本将它与稳定 Host 身份、资源种类、逻辑 owner、storage generation 和预留操作绑定，不含进程 incarnation。记录至少冻结 `host_id`、`deployment_id`、`bot_id`（可空）、`storage_generation`、`kind`、物理名称、角色/容器标识、随机 owner marker、修订、生命周期状态和创建操作 ID。预留服务只返回资源引用与状态，不返回 owner marker；后者只供 Host 原生物理 Effect 核验。首批资源种类是 `POSTGRES_DATABASE`；artifact 和 Milvus collection 只有在相应原生 Effect 与引用冻结协议实现后才可加入同一清单，不能仅凭扫描或命名规则宣称拥有。

## PostgreSQL 分配与跨库合流

deployment 供应 Job 在本库持久化一个稳定的 Host 分配请求引用，然后经受限跨 scope Contract Router 送达 `host.storage.reserve`。Host 在自己的事务内预留准确数据库名和 owner marker，并持久化物理创建 Effect；部署方不能再持有可直接执行 `CREATE DATABASE` 的分配器。Host 原生 Effect 使用单独受保护的维护凭据，在准确名称的 advisory lock 下创建无特权 Bot role/数据库，并将包含 `resource_id`、`host_id` 和 marker 的标记写到物理数据库；重复执行先复核角色权限、数据库 owner 与标记。若目标已存在但标记不同，拒绝接管。Effect 的结果为持久 receipt，Host 的 `realize` 在事务内推进资源状态；deployment 只凭该 receipt 继续冻结 storage profile 和供应 Bot。

崩溃可能发生在任意两个库的提交之间。Host 请求由 `(deployment_id, operation_id, input_digest)` 幂等合流；deployment Job 保留请求 ID 并在恢复时查询 Host receipt，不能重新生成密码、marker 或另一个数据库。物理创建结果 UNKNOWN 时先查询准确名称、角色和 marker，再结算或进入人工核对；不得盲目再次创建。Host 资源状态和 deployment profile/head 是不同权威，WebUI 必须分别标明，不能用其中一个状态推断另一边已提交。

## 冻结引用与回收

deployment 在准备回收时，先以资源 fence 把目标 storage generation 标为不可再附着，禁止新的 Bot head、备份/恢复 Job 或下载租约引用；已有引用须结束或显式转移。该冻结操作产生绑定 `resource_id`、逻辑 owner、fence、引用水位和有效期的证明。Host 接受证明时持久化回收计划并复核自身资源状态；实际删除前再次通过同一受限端口核对冻结仍有效、没有活动引用，且物理名称、角色、owner marker、Host lease epoch 与计划完全一致。只读“当前无引用”不足以允许删除，因为检查后可能出现新引用。

回收 Job 对 PostgreSQL 数据库先停止新的连接并等待既有连接排空，再删除准确数据库及其专属 role；失败或连接未排空保持可重试状态。每个外部步骤前后写入持久 Effect 意图/观察，UNKNOWN 以实际数据库和 marker 核对，不用 `DROP ... IF EXISTS` 的成功返回冒充原动作完成。artifact 与 Milvus 的回收各自使用种类专属标记和原生 Effect，不共享数据库删除实现。

## 启用门槛

生产 Host Graph 可以在具备真实 Host builtin 后启动，但 `host.storage` 的 `generations` 不能宣称完整，直到现有 deployment 直建库路径被移除、所有新资源先预留再创建、旧预发布资源被重建或按准确 marker 登记。回收动作还须等待冻结引用端口、物理标记核验和 UNKNOWN 对账均通过真实 PostgreSQL 验证。部署 Web provider 必须以 Host 签名票据和本地回执打开包的管理契约；壳层不得直接实现这些动作。

当前预发布实现（2026-10-07）：`host.storage` 已有包私有预留账本、`generations` 键集查询、自带管理 UI 和声明式内部幂等服务；隔离 PostgreSQL 验证了同键重放、异键冲突、查询和 Worker 热替换后的原键重放。包故意不自动安装，资源仍只可能处于 `RESERVED`：物理创建 Effect、deployment 跨 scope 投递、ACTIVE 结算、引用冻结及回收尚未实现，不能启用生产 Host Graph 或把页面用作完整库存。
