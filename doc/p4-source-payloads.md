# P4 来源与载荷存储切片

本文描述来源账本与载荷存储的实际实现。图/向量发布、Agent 图文召回、Web 记忆配置和公开来源遗忘已接通，见[记忆运行链路](p4-memory-runtime.md)与[记忆来源管理](p4-memory-administration.md)。029–035 曾将输入、上下文、Agent、历史 Child 和发送正文迁入可删除载荷，036–038 接通 Kernel/状态/代际暴露，039–049 接通管理回执及 Agent、Memory、Binding、组件和 Watch 配置正文，060–061 接通会话 Job settings/delegation 及 occurrence 撤权，062–063 接通 Shell 参数、结果与 CONTROL 回复链。这里的 Child 描述只记录旧实现曾出现的敏感副本；最终未发布基线直接删除 Child 表、迁移与数据搬运器，不保留兼容遗留层，详见[删除设计](p3-children.md)。部署库和备份/WAL 仍需完成；公开流程只能对已经具备完整来源闭包的来源承诺立即抑制和托管 body 清理。

## 来源进入实际消息流水

`MessageStore` 在正常 Inbox 接纳事务中登记来源、分配来源序号、保存作者与有序内容，并记录该次 Conversation/Binding 观察。内部计划任务也登记来源，但使用独立 `SYSTEM` 身份和目标会话，不伪装为群成员发言。正式最终回复在正常提交的同一个事务内取得来源，`FINAL_READY` 私有候选没有这个资格。

同一个 Bot 的同一 QQ 群使用一条来源主干身份。连接重建、不同接入账号和 thread 不生成第二个群身份；Context 与 Actor 的输入顺序仍由各 Conversation 管理。OneBot 消息 ID 只能在其已验证的账号范围去重，相同裸 message ID、文本或发生时间不能证明两个账号看到了同一个事件。存在可证明为同源的观察时保留多个 occurrence，不覆盖首次来源位置。

来源序号在短事务中固定，是接纳顺序的有序仲裁。给定这些位置后，异步图归约不得按 Worker 完成先后重新串主干。群名称与用户昵称属于事件载荷，稳定 principal 与 source 身份不以名称合并。

源注册不等于可以召回。来源 observation 只有在持久 InputBatch 或静默排空中获得接纳证据后才具备输入资格。候选及已登记依赖必须有完整来源标记；旧回复和不能证明来源闭包的派生内容不进入候选结果。召回读取同时检查冻结 cut；直接来源及载荷依赖中，任何当前 Conversation 的超切口或未接纳观察都会使对应候选不可用。其他合法群/会话的来源按 Bot 全局域使用。普通 GROUP 默认 `bot_shared`，DIRECT 和内部 SYSTEM 默认 `source_restricted`，这与[04 的可见性规则](04-bot-agent-memory.md#73-可见性与跨群使用)一致。

管理命令只保存允许进入模型的安全历史投影，不因记忆归档重新公开原始参数。图片 part 取得稳定来源单元/part 身份，关联实际 Inbox 的独立媒体引用；原图捕获仍由既有媒体服务管理。相同图片的不同投稿保留不同群与作者来源。

029 升级在同一迁移事务中分页登记旧 Inbox 来源，再移除旧正文列，保证启动后不需要读取已退役字段。后台补登记仍使用持久游标处理其他历史缺口；已登记输入直接跳过，已删除的 Reply 只推进游标，不重新发布正文。旧位置明确标记 `RECONSTRUCTED`，不伪称当年已经存在的接纳时间线。后台每批最多 32 项、运行超时 5 秒，空闲后等待，错误退避；关闭 Bot 时任务随宿主回收。新消息接纳不等待嵌入或 LLM。

## 永久事实头与可清理正文

新增 `021_payloads.sql` 和 `022_memory_sources.sql`，不改写已经发布的 001–020 迁移。

| 实体 | 当前职责 |
|---|---|
| `bot_memory_clock` | Bot 内来源事实修订和删除 epoch，串行化内容接纳与删除决议 |
| `bot_payload_headers` | 永久 owner 身份、原内容摘要/字节数、固定来源清单、状态和删除请求引用 |
| `bot_payload_bodies` | 实际 JSON bytes 或二进制正文，受控删除此行 |
| `bot_payload_sources` | 原始 source unit/revision/part 的结构化依赖，包含父载荷传递下来的来源 |
| `bot_payload_derivations` | 精确父 payload 引用，保留派生路径 |
| `bot_payload_forget_requests` | 幂等删除决议、固定目标清单、提交 epoch |
| `bot_payload_suppression` | 跨修订持续生效的 source/part 抑制事实 |
| `bot_memory_sources` | 群/私聊/系统身份、可见性与该来源的接纳序号 |
| `bot_memory_source_units` | 稳定来源单元、载荷引用、作者身份、位置及重建/依赖完整性标记 |
| `bot_memory_source_occurrences` / `bot_memory_source_eligibility` | 每次 Conversation 观察及实际 InputBatch 资格 |

`PayloadStore` 是受信任宿主 Repository，不接受 Worker 选择 Bot 或任意 owner 表。公开 Python 方法验证当前 HostLease；`*_in` 供已有业务事务调用，要求调用方已取得其业务权限和锁。其源码中列出的 owner kinds 是封闭集合。载荷层只检查内容依赖与删除屏障，Conversation 读取资格由 `MemorySourceStore` 等实际业务所有者判断。

锁序为既有 Binding/路由、runtime/host、Conversation 所有权，再到 memory clock、来源/载荷头。事务内没有图片下载、HTTP 或模型调用。最终写入重新检查数据库时间中的宿主许可，锁等待期间过期会使整个事务回滚。

每个 `(bot_id, owner_kind, owner_id)` 只能接纳一个内容和 manifest。重试同内容返回原 PayloadRef，不重复写正文；同 owner 异内容报 `PAYLOAD_CONFLICT`。删除后的相同操作仍返回 `REVOKED/SCRUBBED` 身份回执，不回到“未执行”，也不重新写正文。读取旧正文明确返回 `PAYLOAD_ERASED`。

`SourceRef(source_unit_id, source_revision, part_id)` 的 `part_id="*"` 表示依赖整个来源。删除一个 part 会使依赖整个来源的混合请求不可用，但精确依赖另一未删 part 的独立内容可以保留。删除抑制跨所有修订持续有效；修改任务 ID、来源修订或携带最新 epoch，均不能绕过已经抑制的稳定 source/part。

父载荷的来源依赖在接纳时规范排序并并入子 manifest。删除一个源可以找到多层派生正文，包括没有逐字引用原文的回答。原始摘要与操作头不随正文删除；不能把删改后的请求继续声称为原来的精确 wire bytes。

当前 PayloadStore 的每个载荷最多 32 MiB、256 个直接父载荷和 4096 项来源依赖。向载荷层提交的来源集合超限会明确拒绝；长期回复链会持续增加依赖，因此源 DAG/分片及其删除闭包是后续容量实现的必要工作。此限额不构成长时间高流量验收已完成的证据。

新 Agent Reply 继承最终模型输出载荷；其快照、私有工具组和 BOUND 证据继续传递父引用。只有全部来源都对应完整登记的 SourceUnit 时才标记依赖完整。旧 Reply 和未提供完整父载荷的模板路径继续标记 `lineage_complete=false`；旧有界上下文扫描不能充当完整来源证明。Kernel 状态读及工具返回尚未补齐的额外数据来源，仍是全链路遗忘必须完成的范围，见[剩余迁移清单](p4-payload-migration-notes.md)。

## 删除基础的实际范围

内部 `PayloadStore.forget_sources` 在当前宿主、预期 deletion epoch 和稳定操作 ID 下提交固定目标，增加 epoch，并使相应新式载荷成为 `REVOKED`。读取立即拒绝；每次 `scrub` 以短事务删除最多 256 个 body 行，再标记 `SCRUBBED`，重复执行安全。SQL 触发器拒绝直接替换正文、未登记删除决议的 body 删除，以及向已抑制/已清理身份重新附加正文。

同图在两个群中独立投稿时，甲的来源被抑制不会删除乙的独立载荷。这里没有实现跨来源物理 blob 去重；未来共享 blob 必须继续保持来源授权与物理回收分离。

本切片**只清理已纳入 PayloadStore 的 body**。029–049 已将 Inbox 名称/内容、媒体定位与捕获、Context/Reply、Agent 快照/请求/工具、Delivery、Kernel/状态、管理回执和主要配置正文改为载荷引用，原正文列禁止再次写入。已迁移内容的身份、摘要、费用及外部效果状态保留。旧 Child 表及其 payload handler 是待删除代码，不作为审计、结算或遗忘兼容层保留；真正通用的费用和外部效果事实应进入对应通用账本。034 对旧实现中已发生删除却仍留有不完整来源副本的数据库处理只保留为历史取证；当前未发布基线以干净数据库验收。剩余自由文本族、部署库、数据库旧页、WAL、备份和已发送外部内容不在在线 body scrub 的能力边界内。

删除新式 body 行也不代表历史数据库页、WAL、备份或已许可外发的内容已经清除。`scrub` 回执明确限定 `scope=managed_payload_bodies`，并保留 `storage_reclaim_required=true`。当前公开遗忘页区分立即撤销与后台在线正文删除，并明确提示外部内容不能撤回；存储页和备份回收仍由独立运维流程负责。

## 当前验证

`tests/test_payloads.py` 使用真实 PostgreSQL 验证：传递删除闭包、二进制与文本的 part 区分、同内容不同群来源保留、删除早于来源接纳、新修订防复活、重复 owner 幂等/冲突、两个独立 Python 进程竞争、删除 CAS、锁等待过期回滚、数据库约束及事务失败清理。

来源与运行时测试检查群主干身份、多人/多图来源、不同 Conversation 切口、静默资格、正式回复、受限控制历史与旧库补登记。纯图归约和嵌入协议分别见[图归约器](p4-graph-reducer.md)与[图文嵌入驱动](p4-embedding-driver.md)。这些测试不等于完整 RAG、真实 QQ 或商业嵌入语义质量验收。
