# 干净数据库基线、模块数据所有权与载荷来源详细设计

> **权威性：目标设计。** 本文取代所有按开发期 SQL 编号组织的 payload/migration 文档。Forest 尚未发布，不升级开发期旧库，不保留旧列 fallback、双写、数据搬运器或 legacy marker。旧实现只能从 Git 历史追查，不能成为新代码的兼容要求。

## 1. 目标

持久化必须服务微内核边界，而不是把当前官方模块固化成一个产品数据库。内核只拥有运行、路由、授权和效果的通用事实；Agent、Memory、Adapter、Conversation Scheduler、Shell、Job 与 Delivery 分别拥有自己的状态和载荷。模块卸载、替换或热重载时，内核不需要理解其业务表。

正文和二进制内容必须从首次写入起携带完整来源闭包。删除、授权收缩、热重载、异步索引完成和外部结果晚到时，任何路径都不能通过缓存、派生正文或重放回执复活已撤销来源。

## 2. 数据库组成

每个 Bot 仍拥有独立事实库和凭据。一个事实库可以承载多个模块命名空间，但访问边界由数据库角色与宿主 Repository 同时执行。

| 所有者 | 可以持久化的内容 | 禁止持久化的内容 |
|---|---|---|
| Kernel | Bot 身份、schema bundle 账本、RuntimeGraph、Generation、RouteGraph、能力授予、Call/Effect/Operation、lease/fence | Agent prompt、记忆节点、OneBot wire body、Job 定义、模块配置正文 |
| `adapter.onebot` | Binding、连接事实、规范化入站、平台媒体引用、发送 attempt/receipt | Agent Run、记忆向量、其他 Adapter 的连接状态 |
| `conversation.scheduler` | Conversation lane、ingress 顺序、分类水位、trigger 与 InputBatch 切口 | 模型 Provider 私有状态、长期记忆图 |
| `agent.runtime` | Session/Run/ModelTurn/ToolCall/checkpoint、模型请求与结果载荷引用 | 平台连接、Memory 内部索引、内核路由 head |
| `memory.graph` | source/trunk、payload、media capture、graph、embedding、retrieval evidence、deletion epoch | Agent 执行所有权、平台发送状态 |
| `shell.command` | 规范命令请求、类型化参数引用、回执引用和当前命令 registry | 任意管理 API 地址或管理员会话 |
| `scheduler.jobs` | Job 定义、occurrence、lease 与触发结果引用 | 第二条 Agent 执行链或独立模型配置 |
| `delivery` | SendIntent、part、attempt、resolution 与因果引用 | Adapter 私有 socket、Agent 历史副本 |

模块表放入由 bundle 分配的独占 PostgreSQL schema。模块运行角色只拥有该 schema 的 DML 权限和明确批准的内核函数；第三方 Worker 不获得数据库连接。官方受信模块也必须经自己的 Repository 访问，不能借同进程身份跨模块 join。跨模块原子流程使用内核主持的类型化 transaction participant 或 outbox/effect 协议；不能靠某模块直接更新另一模块的表来“方便地”完成事务。

## 3. Schema bundle 生命周期

模块 manifest 声明：

```toml
[components.storage]
schema_id = "forest.memory.graph"
schema_version = "1.0.0"
baseline = "schema/baseline.sql"
baseline_sha256 = "..."
scope = "bot"
repository_contract = "forest.memory.repository/1"
```

`package_digest` 不写入 manifest，因为包不能稳定地自引用自己的摘要；宿主在完整包快照通过校验后派生该值，并把它与上述字段、Runtime Generation、Repository contract 和管理 UI 摘要一起写入描述符事实及安装账本。缺少包摘要或任一字段不一致都不能按幂等安装处理。

Schema bundle 使用两个不可混淆的执行档位。可安装包固定为 `declarative`，只允许宿主审计器接受的表、类型、索引、序列和注释，不能声明函数、触发器、`ALTER`、跨 namespace 引用或数据语句。随 Forest 官方 distribution 摘要发布且描述符 `trust_level=official` 的模块可以使用 `official` 档位承载函数、触发器和完整约束；该档位是对已经能够执行宿主 Python 的官方代码做完整性校验，不是 SQL 沙箱。Runtime registry 必须复核 artifact trust level，安装器把 `execution_profile` 写入不可变 bundle 账本；第三方描述符选择 `official` 时在执行任何 SQL 前拒绝。

首发前只允许以下安装状态：

```text
ABSENT -> VERIFYING -> INSTALLING -> READY
                    -> FAILED
```

安装事务先锁定 `(bot_id, schema_id)`，要求目标命名空间为空且没有未知对象；随后验证包摘要、SQL 静态限制和数据库能力，在受限 owner 下执行 baseline，写入精确 bundle 摘要，再把状态置为 READY。候选 Generation 只有引用 READY 的准确 bundle 才能 PREPARE/ACTIVE。安装失败回滚整个命名空间，不发布半成品路由。

`VERIFYING/INSTALLING/FAILED` 是 Lifecycle Coordinator 的操作状态；bundle 账本只保存事务提交后的 `READY`（以及卸载后的 `DETACHED`）。安装事务失败时 namespace 和账本行共同回滚，失败详情写控制操作审计，不能为了记录 `FAILED` 而留下一个可被误认作已安装的半成品 schema。

重复安装相同摘要为幂等；相同 `schema_id + version` 对应不同摘要是供应链冲突。开发期 schema 变化直接修改 baseline 并重建测试库，不增加 `ALTER old_table`、兼容 view 或数据 handler。首次正式发布后才设计 `from_version -> to_version` 的模块升级边，升级也属于模块生命周期操作，不成为内核硬编码的 001、002、003 全产品链。

模块卸载默认只撤下路由和运行代际，数据进入 `DETACHED`，不自动删除。显式 purge 需要模块声明的有界计划、当前授权和独立审计；内核只监督操作与资源，不理解业务行。

## 4. 统一载荷与来源闭包

所有自由正文和二进制数据使用不可变 `PayloadRef`：

```text
PayloadRef {
  payload_id, owner_component_id, owner_kind, owner_id,
  media_type, byte_length, content_digest,
  source_manifest_digest, deletion_epoch, state
}
```

`SourceManifest` 是有序且内容寻址的来源集合。每项至少包含 Bot、稳定 source unit/revision/part、原始群主干、具体 Conversation/Binding 观察、作者主体、发生时间和传播限制。派生载荷必须联合所有实际输入的来源；不能只记录最后调用者、当前群或模型自报的引用。跨模块传递正文时，发送方同时传递由宿主签发的 payload capability；接收方创建派生载荷时由宿主追加父引用和来源闭包，Worker 不能自行缩小闭包。

正文状态只有 `ACTIVE -> REVOKED -> SCRUBBED`。永久事实保留稳定 ID、状态、摘要、因果和外部效果结算信息；正文不可用时返回类型化 `PAYLOAD_REVOKED/PAYLOAD_SCRUBBED`，不得返回空字符串、空 JSON 或从审计/outbox/checkpoint 复制一份旧内容。

下列内容从第一次写入就必须使用该协议：群入站、Context 单元、Agent snapshot/request/result/tool/final、Memory source/media/evidence、Kernel call 参数与结果、模块状态、配置修订、Job 定义、Shell 参数和结果、Delivery wire body/receipt、管理操作结果和审计详情。不存在“以后再迁”的裸 JSONB 正文列。

## 5. 删除与异步完成的线性化

每个 Bot 的内容域持有单调 `deletion_epoch`。公开遗忘采用 `plan -> apply -> scrub`：

1. plan 固定请求目标、精确来源闭包、影响集合摘要、当前 epoch 和期限；
2. apply 在短事务内重算集合并 CAS epoch，写 suppression，撤销所有已知派生载荷和索引资格；集合变化时要求重新 plan；
3. scrub 有界删除 body、媒体对象、向量与缓存，直到可证明的在线副本归零；WAL、备份和已发外部副本单列状态。

异步写只有两种合法排列：新结果先提交，则 apply 必须在重算闭包时包含它；apply 先提交，则新结果在最终发布时观察更高 epoch 并被拒绝。下载、embedding、LLM、工具、图构建和平台发送均在外部 I/O 后、写入业务事实前复验来源、epoch、Generation、Host lease 和自身 operation 身份。已发生费用或外部结果可以结算元数据，不能重新发布正文。

相同幂等 operation 重放返回原永久回执和“正文已撤销”状态，不重新调用 Worker、模型或平台。不同输入复用 operation ID 必须拒绝。UNKNOWN 外部效果保持 UNKNOWN，遗忘不能把它改写成确定未发送。

## 6. 多模块事务与通信

模块之间只交换版本化 DTO、`PayloadRef`、`SourceManifestRef` 和明确因果引用。需要原子提交时，由内核的 UnitOfWork 创建一次 operation，并按已注册 participant contract 调用模块 Repository；所有 participant 使用同一 Bot 数据库事务，但每个 participant 只能写自己的 schema。典型流程包括：

- 入站：Adapter 写平台事实，Conversation Scheduler 分配 ingress，Memory 登记 source observation；
- 最终回复：Agent 关闭 Run，Conversation Scheduler 提交回答状态，Delivery 写 SendIntent；
- Shell：Shell 写类型化请求，Control operation 写操作事实，Delivery 写 CONTROL 回执；
- Job 触发：Job 关闭 occurrence claim，Conversation Scheduler 写内部 ingress/trigger。

participant 集合、顺序、输入摘要和结果摘要写入 operation；重试复用同一身份。模块不能在一个数据库事务中等待网络、LLM、Worker 或用户确认。跨独立数据库没有伪造的原子事务，使用 outbox、幂等接纳和显式补偿/对账。

## 7. 配置、UI 与敏感字段

模块配置属于模块 schema。内核只保存可路由的配置 revision 身份、desired/effective 指针和配置载荷引用；字段 schema、默认值、验证、计划差异与生效动作由准确 Generation 的管理契约提供。WebUI 通过模块描述符加载 schema 或沙箱 UI，不建立 Agent/Memory/OneBot 专用表单。

密钥正文直接写入 Secret Resolver，配置载荷只保存 opaque `secret_ref`。读取配置不回显密钥；修改计划只记录“是否变化”和不可逆摘要。VL embedding 的模型种类、输出维度、归一化和融合语义属于 Bot 创建时冻结的 Memory schema identity；创建 Bot 后只能更换协议兼容 endpoint/凭据。改变模型种类必须创建新的 Bot 记忆域或执行未来明确设计的全量重建操作，不能通过普通配置 apply 偷换向量空间。

## 8. 干净基线验收

- 空部署只安装最小内核 baseline；其中不存在 Agent、Memory、OneBot、Shell、Job 或 Delivery 业务表。
- 启用模块才安装其准确 bundle；删除任一官方模块包后，内核仍能启动并明确报告缺失契约。
- 目标命名空间非空、摘要不符、未知对象、越权 DDL 和跨 owner 引用均拒绝。
- 源码、SQL 和测试中不存在开发期 `migration_v*`、001…087 回放、legacy marker、旧列 fallback、双写或非空旧库升级夹具。
- 从群入站到 Agent、Memory、Tool、Delivery、audit 的 canary 内容只有受管 payload body；遗忘后所有在线正文与向量不可读且晚到结果不能复活。
- 两个群并发写同一 Bot 记忆时，来源与主干不混淆；事实合流按稳定 ID/版本完成，不按 Worker 完成先后覆盖。
- 模块 schema、Repository、management contract 与 UI 工件均绑定同一包摘要和 Generation。
