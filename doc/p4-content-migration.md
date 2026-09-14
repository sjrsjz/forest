# 正文迁移的事务与版本边界

029 起的正文迁移在原 Bot 专属数据库内执行。升级前先停止并排空宿主；仍持有有效 HostLease 时返回 `MIGRATION_HOST_ACTIVE`。升级不调用模型、嵌入服务或媒体下载，也不使用第二条数据库连接。

`BotStore.migrate()` 先检查数据库身份、未知迁移和全部历史 checksum，再按 runtime → host 的锁序阻止旧宿主提交。事务内使旧 owner epoch 失效，然后依次执行 SQL prepare、`storage.data_migrations.<family>_v1.migrate_in(conn, store)`、handler 内的最终约束和迁移回执。任何一步失败都会撤销本次全部 DDL、数据搬运、host 失效和回执。数据库锁等待上限为 3 秒；发生锁竞争时整次升级失败，可在停止占用连接后重试。

001–028 保留原 SQL SHA-256。带数据 handler 的 029–035、037–038 使用 UTF-8 JSON 数组 `['forest.bot.migration.v1', SQL, handler 源码, migration_v1.py 源码]` 的 SHA-256，数组使用无额外空格的 JSON 编码；纯 SQL 的 036 仍使用 SQL SHA-256。handler 和 `migration_v1.py` 发布后不得修改；后续算法使用新的版本文件。handler 应包含迁移所需的历史算法，不调用会随运行时变化的业务存储实现。

新连接通过 PostgreSQL 启动参数声明当前 `forest.writer_generation`。029 的基础正文切换使用 generation 1，042 的配置正文切换提升为 generation 2，060 的 Job 正文切换提升为 generation 3，062 的 Shell 正文切换提升为 generation 4，064 的 Shell v7 上下文控制协议提升为 generation 5，066 的模块 Shell 提升为 generation 6，067 的固定 Core form 提升为 generation 7，068 的 Shell P0 资格依据提升为 generation 8。数据库的 `bot_schema_writer_gate` 和 HostLease INSERT/UPDATE/DELETE trigger 拒绝缺少版本或版本过旧的连接，所以旧进程即使更换 incarnation，也无法重新取得或续签宿主租约。这是版本兼容协议，数据库所有者权限仍只属于受信任宿主。无待执行迁移的重复启动不会改变当前宿主租约。

`PayloadStore.import_legacy_json_in()` / `import_legacy_bytes_in()` 只在 dispatcher 绑定的同连接、同任务、同事务内有效，普通业务调用及继承 ContextVar 的子任务会被拒绝。参数为 `owner_kind`、`owner_id`、`value`、`sources`、`parents`，返回 `PayloadRef`。它与正常写入使用一致的稳定身份和摘要：

- `payload_id = 'payload:' + digest_json([bot_id, owner_kind, owner_id])`。
- JSON 使用现有 Python canonical JSON；原精确 HTTP 请求使用原 bytes，不从 PostgreSQL JSON 文本重建。
- 父载荷的来源清单展开后参与不可变 input digest。
- 相同身份和摘要返回已有回执；已有 `REVOKED` / `SCRUBBED` 回执永不恢复正文。
- 新载荷命中既有来源抑制时，仅写摘要、来源、父引用和 `REVOKED` header，并引用真正的删除操作；不插入正文。

旧派生物不能证明完整来源时，handler 显式包含 `legacy_source_ref(bot_id)`。首次删除决策若发现该标记，或发现不可变旧 SourcePayload 的 `lineage_complete=false`，会把保守标记及相应旧 source unit 明确写入持久删除 targets。原请求幂等摘要仍只基于用户请求的 targets 和期望 deletion epoch；重放使用已经固定的决策，不随之后新增数据扩展。结果的 `legacy_conservative` 表明是否扩大了这次旧数据清理范围。超过 4096 个来源明确拒绝，不截断。其他完整、独立输入不因此加入删除范围。

若首次删除早于迁移、当时尚不存在保守标记，则未知来源的旧派生物在导入时额外继承最早的实际 suppressed source，直接按原删除回执登记为 `REVOKED`，不写正文。选择顺序固定为 deletion epoch、operation ID、source unit、part；原删除决策仍不可变。

032 是升级前产品 Child 实现的历史正文迁移：它把 Child snapshot、冻结 spec、候选结果及模型 request/manifest/result 改为载荷指针，原正文列强制为空。该版本中，Child 的 snapshot/spec 继承父 step 的 snapshot、output、BOUND EvidenceBundle 和工具参数；模型请求和结果继续传递这些父引用。050 已禁止创建或调度新 Child，032 只继续承担旧数据审计、遗忘和费用结算，详见[产品子 Agent 退役](p3-children.md)。

在退役迁移前，子 Agent 还复验父 Run 已绑定记忆的当前配置、来源可见性、资格、切口和运行时权限。退役后这些校验仅保护遗留记录的终结与结算；任何新 Child HTTP、候选正文或父 Agent 接纳都被禁止。数据库原有完整性约束继续保护历史 snapshot、request wire digest 和载荷 header，不能因产品入口删除而削弱遗忘闭包。

| 迁移 | 搬运对象 |
| --- | --- |
| 029 | Inbox 作者/消息正文、旧媒体来源、capture bytes 和来源 manifest |
| 030 | Context 非输入单元、回复正文和相应载荷指针 |
| 031 | Agent snapshot/output、精确模型请求/manifest/result、工具参数/结果/观察 |
| 032 | 升级前遗留 Child snapshot/spec/result、精确模型请求/manifest/result；仅作清理与审计 |
| 033 | Delivery 发送内容、精确请求和回执正文 |
| 034 | 升级前已发生遗忘，但旧不完整 SourcePayload 仍为 ACTIVE 的补偿删除决策 |
| 035 | 为平台成功回执精确确认的自身回声登记不可变来源依赖，协调先回声、后确认或先遗忘的顺序 |
| 036 | 固定 Worker 代际的暴露集合、不可变交付回执、容量与来源失效状态 |
| 037 | Call 输入/结果、状态当前值/历史/步骤回执、迁移源与候选正文 |
| 038 | generation 配置与状态 schema 正文，以及独立 Host 输入来源域 |

034 仅在 Bot 已有 suppression 时执行。它每批选择最多 256 个仍 ACTIVE 且 `lineage_complete=false` 的 SourceUnit，通过新的 `migration:034:legacy-reconcile:<batch>` 删除操作撤销其自身及派生闭包，同时登记原删除操作引用、删除 epoch、来源和载荷数量。旧请求的 targets、摘要与回执保持原样；完整的独立输入不受影响。所有批次和其他迁移同属一个事务，可整体回滚。没有先前删除的旧库不产生任何补偿决策。正文立即不可读，可由正常 scrub 清除；重复 migrate 不重复作出决策。普通删除也只扩展尚未撤销的不完整 SourceUnit，避免已清除的旧历史持续消耗请求来源上限。

035 从既有的精确回声 alias 迁入来源依赖。运行时也在确认 alias 的同一事务追加依赖，数据库复核 Bot、绑定账号、群/线程、平台消息 ID、正式回复或控制命令与 SENT 发送尝试。关联不依赖文本相似度；其他用户独立提交的相同文字不因此成为派生副本。原载荷 manifest、原输入摘要与既有删除 targets 保持不可变，读取与后续写入检查关联后的传递依赖。真实证明不会因为读取闭包超过 4096 项而丢弃；递归查询使用 UNION 去重，有限图中的环不导致无限展开。超限内容明确不可用，Agent 能终结该 Run，晚到结果只结算原费用与效果事实，不再保存正文。

父载荷已经撤销时，关联接纳事务为当前回声追加单来源删除决策，撤销含该来源的平面副本，并登记原因、身份与计数。`scrub` 在清除 body 前每批最多推进 256 条此类关联；父 header 的撤销成为下游下一批的依据。其 remaining 包含待传播关联，调用方应迭代至零后才宣称受管正文已经清理完毕。升级 handler 在记录版本前也按批推进至无待处理关联。正常显式删除若一次目标闭包超过 4096 项，仍整体拒绝且不产生成功回执；这一限额不能被用于回滚并丢弃已经确认的回声来源。

`tests/test_payload_migration_infrastructure.py` 使用真实 PostgreSQL 验证：有效/过期宿主、旧连接重新取权、DDL 等锁回滚、handler 失败回滚、稳定 JSON/bytes、已删除来源导入、作用域逃逸、旧 checksum、代码篡改检测、保守来源的固定删除决策，以及非空 022 旧库升级的补偿与无删除时不操作。另有 257 个旧不完整来源的跨批次验证，检查两个正式删除决策、独立完整来源保留、scrub 和后续请求范围不累积。`tests/test_child_payloads.py` 验证实际 Child 重试、晚到 HTTP 用量、父子闭包、下一 Run 不复活、旧 028 精确字节迁移，以及真实 BOUND query 在配置撤销后的派发/结果守卫。各正文族另行验证实际字段迁移与运行时读取。

## Job 定义正文补充（060–061）

060 使用独立的 `migration_v3.py` 把 writer gate 提升到 3；其 checksum 包含 v1、v2、v3 三代冻结 helper。061 的 checksum 还包含数据 handler 与配置依赖导入 helper，后续不得修改已经发布的 SQL、handler 或 helper 字节。

061 搬运 `bot_job_plans.values`、`bot_job_revisions.values` 和 `bot_job_revisions.delegation`。settings 使用 `family=job`，delegation 使用 `family=job.delegation`；两者按 `job_id` 与 plan/revision 身份独立寻址。迁移完成后原三列只能为 SQL `NULL`，运行元数据不含 prompt、name 或管理员主体。新 apply 的 settings/delegation 同时继承真实计划值和 apply 输入，结果回执同时继承两个 payload；旧行继承确切 041 来源或可证明的 apply 结果，已删除父项不会恢复正文。

occurrence 数据库授权使用不获取 memory clock 行锁的不可变可用性快照，业务 hydrate 则在 Job head 之后锁定 deletion epoch。该分工保持 `Job head → deletion clock` 的写入锁序，也允许遗忘与领取按先后提交线性化。具体字段、失效行为和专项验证见 [Job 定义载荷](p4-job-payloads.md)。

## Shell 正文补充（062–063）

062 使用独立的 `migration_v4.py` 把 writer gate 提升到 4；其 checksum 包含 v1–v4 四代冻结 helper。063 的 checksum 还包含 Shell 数据 handler、`shell_payloads.py` 和冻结 registry，发布后不得修改这些历史字节。

063 搬运 `bot_shell_requests.arguments/outcome/delivery`。参数使用 `owner_kind=shell.arguments`，Shell 结果使用 `owner_kind=control.result`，内部 delivery receipt 使用 `owner_kind=control.outbox`；迁移完成后原三列只能为 SQL `NULL`。参数继承原群消息 payload；Shell 结果继承原消息、参数、Binding 配置及可证明的类型化管理结果；CONTROL send part 继承 Shell 结果，receipt 再继承结果和各个实际发送 part。无法证明完整旧结果链的终端记录保留 `legacy_result_untracked`，不会伪造一次发送或控制效果。

Shell claim 先锁请求 head，再在同一事务锁定 deletion clock 并 hydrate 参数。业务控制方法缓存冻结参数身份，取得自身业务锁后才在最终 `_time` 安全点重新 hydrate，从而统一为“业务 head → deletion clock”的锁序。遗忘先提交时待执行命令原子终结为 `CANCELLED/CONTROL_CONTENT_REVOKED`；命令完成先提交时永久事实保留，正文随后不可读且不能复活。完整字段、父链和验证见 [Shell 参数与结果载荷](p4-shell-payloads.md)。

## Shell 上下文控制补充（064–065）

064 使用独立的 `migration_v5.py` 把 writer gate 提升到 5；其 checksum 包含 v1–v5 冻结 helper、`shell_payloads_v7.py` 与 `shell_registry_v7.py`。v7 作为新模块继承并解析 v2–v6 的持久命令，不修改 063 已绑定的 `shell_payloads.py` 和 `shell_registry.py` 字节。generation-4 进程在 064 提交后不能重新取得 HostLease，避免它领取无法解释的 v7 请求。

065 增加 `bot_context_management_requests`。请求身份、operation、来源会话、种类、预期视图和登记时间不可变；状态只允许从 `PENDING` 一次终结为 `APPLIED` 或 `CONFLICT`。写操作与 `bot_web_operations` 使用延迟外键同事务提交，Shell 不能留下无来源的上下文请求。Actor 按发令会话加锁，在现有模型/工具安全边界消费请求；裁剪只更新 Context 可见性、视图修订与不可变 manifest，不搬运或生成正文，也不调用模型。
