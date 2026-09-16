# P4 准备：正文分离、物理清理与不可复活的删除闭包

状态：**历史副本调查，不是当前目标 schema 或兼容计划。** 第 1–11 节保留旧实现曾产生正文副本的位置，供提取通用遗忘不变量；其中的 Child 入口、表、迁移、handler 和测试不得被后续实现恢复或保留。Forest 尚未发布，最终基线直接删除整条 Child 分支并重建开发数据库，不承担 019/032/050 的升级兼容。当前协议以[单 Agent 协议](p3-agent.md)、[产品子 Agent 删除](p3-children.md)和[实现状态](implementation-status.md)为准。其余通用 Payload、Kernel、配置、Job 与 Shell 载荷边界仍需按各自现行文档复核；本页不把 `visible=false`、`context_disposition` 或任务取消占位解释成删除。

“永久事实头 + 可清理载荷体 + 来源依赖”契约已用于新记忆和上述正文族。新 Agent Reply 从最终步骤的显式父载荷登记；旧回复和不能证明来源闭包的内容继续保持 `lineage_complete=false`。Kernel 状态读取和 Worker 跨调用缓存仍需要补齐真实依赖，不能把局部来源传递称为全链路保证。迁移后的记录一旦撤销或清理，禁止回退旧正文列。

## 1. 已确认的载荷清单

下表中的 JSONB 不一定每行都含聊天正文；它们都是可以保存正文、作者展示信息、图片 URL 或衍生产物的位置。迁移时应按字段登记载荷类型，不能仅搜索 JSON 中的 `text` 键。

| 保存处 | 实际内容与副本关系 | 迁移后必须保留的事实头 |
| --- | --- | --- |
| `bot_message_inbox.segments`、`author` | 文本原文；图片 typed ref；作者昵称、群名片、匿名对象；被确认 self echo 的消息仍有独立原文 | event/source_event/conversation/ingress_seq、原 input_digest、来源类别、受保护的稳定 principal_ref、时间和分类结果 |
| `bot_inbound_media_refs.source` | 原图片 URI 与适配器源对象，可能带短期下载签名；不是图片字节 | media_ref、event_id、part_index、源摘要、捕获 epoch/状态 |
| `bot_media_captures.content`、`source_manifest` | 图片原始 bytea；source_manifest 再复制作者/来源展示信息 | media_ref/revision、blob_digest、MIME/尺寸/帧数、不可变捕获 ID、来源 ID；图片字节必须真删 |
| `bot_conversation_replies.parts`、`delivery` | 最终答复正文；delivery 当前主要是发送 receipt 引用，应改为受约束 metadata | reply_id、run_id、input_revision、response_seq、原 input_digest、intent_id |
| `bot_context_units.control_parts`、`step_parts` | CONTROL 输出及已闭合模型/工具组正文的副本；INPUT 单元本身仅引用 Inbox | unit_id、kind、event/reply/step/command 引用、位置、水位、视图可见性 |
| `bot_agent_runs.spec` | 冻结配置可能包含 system_prompt、工具 schema 描述和其他自由文本；不是单纯 ID | run/session/spec_digest、配置 revision、预算/截止时间、状态及 checkpoint |
| `bot_agent_steps.snapshot` | 包含输入单位正文、作者、图片 ref、历史答复和工具组；即使 Provider 后来裁掉部分单位，快照仍保存它们 | step_id/seq、输入 revision/cut、snapshot_digest、依赖 manifest ID |
| `bot_agent_steps.output`、`closed_parts` | 模型文本/工具调用与结果；closed_parts 又会复制到 Context 和以后步骤 snapshot | output_digest、闭合/发布状态、对应模型 request/attempt IDs |
| `bot_model_request_manifests.manifest` | **`projected_units` 保存完整投影文本**；`source_manifest` 也复制展示信息。名称叫 manifest 不代表它只含引用 | request_id、原 manifest_digest、input/projected_content digests、能力/序列化版本、输入/媒体/证据引用 |
| `bot_model_transport_attempts.request_body` | 完整精确 API JSON bytes；vision 输入包含 `data:image/…;base64,…`，是图片的另一份完整字节表示 | request/attempt IDs、wire_digest、许可/开始/结果状态、预算、宿主与 owner fence |
| 同表 `result`、`prepared_metadata` | result 含模型文本/工具参数与 usage；prepared_metadata 当前是 token/stream/serializer 等小型恢复数据，应拆出允许保留的字段 | result_digest、usage、provider receipt ID、错误码和 token 结算；正文另存 |
| `bot_agent_tools.arguments`、`result` | 可任意嵌套文本、图片引用、工具响应与子 Agent 结果 | tool ID、operation ID、arguments/result digests、实际触发和 principal_ref、状态/尝试次数 |
| `bot_agent_tool_observations.observation` | **013 的 `forest_agent_tool_observe` 每次相关状态变化写 `to_jsonb(NEW)` 整行**，重复复制 arguments/result | observation ID、operation ID、attempt_count、状态、原观察摘要和载荷引用 |
| `bot_agent_children.snapshot`、`spec`、`result` | 私有分支冻结源内容、委托任务与候选结果；结果采纳后还会进父工具 result | child/parent/tool IDs、source_unit_ids、原 snapshot/result digests、lease/状态/父 checkpoint |
| `bot_child_model_attempts.request_body`、`manifest`、`result`、`metadata` | 子模型同样保存完整请求（可含 base64 图片）、投影文本与输出 | request/attempt IDs、wire/result digests、结果状态、日/树预算及费用事实 |
| `bot_send_parts.payload` | 发送消息段原文或媒体引用 | intent/part/operation IDs、payload_digest、顺序、state/owner/attempt |
| `bot_send_attempts.wire_body` | 精确 OneBot API bytes；以后允许图片发送时也可能含完整编码图片 | attempt/intent/part IDs、wire_digest、固定目标、许可时间、发送结果与 UNKNOWN 状态 |
| send parts/attempts 的 `receipt`；`bot_delivery_resolutions.reason/result` | receipt 目前主要是平台 ID/错误码，但接口接收有界任意 JSON；人工 reason 是自由文本 | 经白名单验证的效果状态/平台消息 ID、人工决定主体/时间、原输入摘要 |
| `bot_calls.result` | **Kernel 缓存完整 Worker 返回值**。OneBot normalize 回执含整个消息，prepare 回执含 `wire_body_b64`；普通工具/响应服务也可返正文 | call/operation/root/parent IDs、input_digest、独立 result_digest（需补）、终态、生成代/权限信息 |
| `bot_component_state.value`、`bot_state_revisions.value` | 组件可写入用户资料或工具结果；当前状态清空不会删除历史版本 | 组件/key/revision、原值摘要（需补）、call/operation、deleted 事实 |
| `bot_state_steps.receipt` | 当前主要是写入版本回执；应按固定 schema 保留，不能允许回执重新内嵌敏感值 | 操作输入/载荷摘要、各 key/revision 和成功状态 |
| `bot_state_migration_sources.source_rows`、`bot_state_migration_results.result_values` | 热重载迁移又保存一套组件状态完整副本 | operation、源/目标 schema/revision、source/result digests、迁移结果状态 |
| `bot_web_operations.result`、`bot_audit_records.details`、`bot_outbox_events.payload` | plan/apply/read 操作可能返回配置/提示词；`BotControl._record` 会把 result 写管理回执，又通过 `BotStore._event` 复制到 audit/outbox | 动作/operation/主体、摘要、状态、引用；敏感 details 移出 metadata |
| `bot_shell_requests.arguments`、`outcome` | typed 参数中的 reason、查询/诊断输出可能包含个人信息；原 raw_digest 不等于保存 raw 正文 | command/event IDs、name、priority、gate_version、参数/结果摘要、权限主体及状态 |
| `bot_job_revisions.values`、`bot_job_plans.values` | 计划 prompt/name 的原始版本，随后成为 `origin=job` 的消息副本 | job/revision、固定目标、schedule、delegation ID/epoch、原定义摘要和 operation |
| 配置 plans/revisions、`bot_generations.config`、`bot_dev_watch_heads.config`、Binding 当前 config | 管理员可能主动复制记忆内容到 system_prompt、response_template 或插件自由配置；未来 schema 应支持来源标注 | 配置/包/版本/权限头；只有明确包含目标来源或用户指定范围时才删除其内容 |

`bot_agent_checkpoints.payload` 当前由 `_checkpoint` 的白名单构建，主要包含 ID、水位、phase、spec_digest；它可保持 metadata，不应因“JSONB”整体丢弃。InputBatch、context_compactions、审批决定、echo_alias、job expansion/occurrence 与模型预算表也主要是事实索引，应保留稳定身份和计数。

Binding/Agent 的 `secret_ciphertext` 属于凭据生命周期，不是默认记忆删除对象。若删除整个 Bot 或凭据中明确混有目标数据，需进入另外的凭据/密钥清理闭包。不能因遗忘一句聊天而破坏当前连接凭据。

## 2. 既有不可变触发器与 FK 的实际限制

### 2.1 不能原位置空后保持旧约束

- `005.forest_inbox_source_immutable` 明确拒绝 DELETE，并固定 `author/segments`；`bot_conversation_replies_immutable` 拒绝 UPDATE/DELETE。
- `013.forest_agent_fixed_fields` 固定 Agent spec、step snapshot、模型 request_body、工具 arguments、Context step/control parts；`forest_agent_observation_once` 又固定已经观察到的模型 result/output。`bot_model_manifests_immutable`、`bot_agent_tool_observations_immutable` 和 checkpoint 触发器拒绝整行变更。
- `016.bot_media_captures_immutable` 拒绝 UPDATE/DELETE；content 为非空、长度至少 1，并要求 `blob_digest=sha256(content)`。`forest_media_source_immutable` 固定 URI/source 且禁止删除媒体源行。用空 bytes 替换既不符合约束，也伪造了原 blob 的内容。
- `019` 固定 child snapshot/spec、child request_body；wire_digest 有 bytea 计算 CHECK，已存在 result 也不能替换。
- `006.bot_delivery_immutable` 固定 intent、payload、精确 wire_body 和人工 resolution。parts/attempts 的触发器仅拦 UPDATE，并不代表它们可安全整行删除：FK、echo reconciliation、UNKNOWN 处理和去重都依赖这些行。
- `001` 的 state steps/revisions、audit，`003` 的 Web 操作/config，`004` 的 Binding 修订，`010` 的状态迁移副本，`018` 的 Job definitions/plans 均有拒绝修改的触发器。`020` 的 Shell arguments 也固定。

### 2.2 保留事实行，删除载荷行

Inbox → Trigger/Context/媒体/echo/Job；Run → Reply/Agent/step；step → model manifest/attempt/tool/child；Intent/part → attempt/echo/resolution；Call → state step/revision/子调用，都有 FK 或等价恢复依赖。简单 `DELETE CASCADE` 会删掉证明“这个 operation 已经做过”的唯一依据，之后同 ID 重试可能再次执行。

推荐保留各表原主键、UNIQUE、FK、原摘要与状态；可清理内容通过独立 payload ID 引用。ID 指向永久载荷头，载荷头可处于 SCRUBBED，而物理载荷体被删除。FK 不指向会被删除的 body 行。

## 3. 推荐一轮发布内完成的迁移

迁移需要停止该 Bot 的所有写入、取得合法 Host/maintenance/storage fence，并终止或隔离旧 Worker/Provider callbacks。备份供回滚可以保留，但必须登记为受限的待清理旧明文副本；备份不是遗忘已经完成的证明。以下建议整体放在一个应用版本升级中，不长期运行双写明文旧列与新载荷表。

### 3.1 新表与载荷接口

建议最小实体：

| 新表 | 字段要点 |
| --- | --- |
| `bot_memory_clock` | bot_id、deletion_epoch、来源/事实提交水位；与既有 runtime 锁顺序协调 |
| `bot_memory_source_units` | source_unit_id、source_id、source_event_id、source_order_seq、kind、source_revision、稳定作者引用、发生/接纳时间、visibility、payload_id；正文不内嵌 |
| `bot_memory_source_occurrences` | 同一源在各 Binding/Conversation/Inbox 的观察关联、ingress_seq、输入资格；不合并各群/各账号的输入切口 |
| `bot_source_manifests` / `bot_source_manifest_members` | manifest ID/digest；精确 source_unit/part/revision、媒体版本、依赖类型；成员为结构化可索引行 |
| `bot_payload_headers` | payload_id、kind、encoding/schema_version、原 digest/长度、source_manifest_id、accepted_deletion_epoch、state=ACTIVE/SCRUBBED、scrub request ID/epoch |
| `bot_payload_bodies` | bot/payload ID、JSON 或 bytes 二选一；仅这里存受管理明文/图片/向量/精确请求体，物理删除此行 |
| `bot_payload_owners` | 受控 owner kind、原表主键、字段用途 → payload_id；封闭枚举映射，不接收调用者提供的 SQL 表名/路径 |
| `bot_payload_derivations` | derived_payload_id → input payload/source manifest；反向索引用于删除闭包 |
| `bot_forget_requests` / `bot_forget_targets` / `bot_scrub_tasks` | plan/apply CAS、固定目标 manifest/digest、operation 幂等、deletion epoch、阶段/租约/清理回执 |
| `bot_suppressed_sources` | 稳定源身份 + 被禁止的 revision/part/范围；重放接纳必须检查，不能仅查调用者携带的 epoch |

所有表按 Bot 隔离。同一 Bot 有多个群主干；`source_id` 指向群/私聊的稳定来源，而不是每个主干一套数据库。迁移已有 OneBot 源时沿用目前 account-scoped `source_event_id`，不能按相同文本或相同 message_id 把不同 self_id 的观察认作同一平台事件。对旧数据只作可以证明的合并，其余保留独立原始来源。

每个 raw unit 保存有序原生文本/图片 part 引用。图片保存的是受管原图或声明的变体，不能把 caption 当作唯一可恢复内容。`source_order_seq` 在每个 source head 下有序分配，图 root/head/顺序边的发布仍经事务 CAS；此序列和删除 epoch 是有序仲裁，不是可无条件交换的 CRDT。

### 3.2 转换步骤

1. 先创建来源头、载荷头/body、依赖/owner 表和删除协调表；对旧数据生成稳定、可重跑的 payload IDs。
2. 将上节列出的正文列逐列搬到 body 表，原行增加 `<purpose>_payload_id`。原 SHA-256/input/result/wire 摘要原样保留；缺少独立结果摘要的 `bot_calls.result`、state value、audit/details 等补摘要。
3. 从 Inbox/Context/step snapshot/model manifest/child source_unit_ids/Reply → Delivery/Call → Agent tool 的已有关联回填 lineage。OneBot normalize/prepare 的 Kernel 回执应在宿主调用时显式登记来源，不能只依赖 operation ID 字符串格式猜关系。
4. 对无法证明来源的旧插件状态/自由 JSON，标记 `LEGACY_UNTRACKED`，在数据删除计划中列明。若用户要求覆盖该部分，应暂停并保守清理整个关联组件/Run 载荷；不能把“不知道来源”当作“与删除无关”。
5. 重建触发器：不可变的是事实头、payload ID 和原摘要；body 表只允许首次插入或受控 scrub。旧代码直接读正文列将不能运行，必须在同一发布更新所有 Repository 的 hydrate/replay/写入路径。
6. 移除旧明文列，或把旧表重建为仅有事实头的新表后切换；不能保留一个无人读取但仍有正文的 legacy 表。`DROP COLUMN` 本身也不证明历史物理页已清理，见第 8 节。
7. 对迁移源表、审计、outbox、逻辑导出、临时转储进行副本清点。完成后才恢复写入；旧版本宿主不得绕过 schema generation 重新连接。

媒体表的 hash/长度检查移到“ACTIVE payload 首次写入”事务或约束触发器；SCRUBBED header 仍保存原 blob_digest，但没有 content 字段可被误验为另一张图片。模型/child/Delivery 的原精确 bytes 同理；删除后不可构建 `PreparedModelRequest` 或 `PreparedSend`，应返回明确的 `PAYLOAD_ERASED`。

OneBot normalize 的 Kernel 调用完成时间可能早于 Inbox 接纳，二者之间崩溃会留下未关联源的完整 result。该入口需要宿主生成的 ingress-envelope/原始调用关联和接纳时的绑定事务，或对尚不能归属的副本采用有界 `UNBOUND` 保留与保守清理策略。不能只在成功写 Inbox 后登记 lineage，却把失败/半完成解析调用遗留在闭包之外。晚到的旧 epoch normalize 返回同样受载荷接纳 fence 约束。

## 4. 来源闭包与完整多模态

用结构化依赖登记内容的复制和派生，而不是等遗忘时扫描整个 Bot 的 JSON 猜引用。依赖至少区分内容依据、作者展示信息、身份/审计引用；只有永久安全字段可留在 metadata。

原始消息的删除闭包包括媒体源 URI、所有捕获/变体、raw-unit payload、涉及它的模型 snapshot、manifest.projected_units、精确请求体、结果/工具组、子 Agent、最终回复、发送 payload/wirebody，以及相应 Kernel result、tool observation 和管理/audit/outbox 副本。模型输出默认依赖实际输入的所有 included parts；不能通过“结果没有逐字引用原文”证明不存在依赖。保存了完整 canonical snapshot 的载荷则依赖该快照内全部内容，包括后来没有发给模型的部分。

一份精确请求同时混有多个群/多个用户时，不能原位删几段后继续宣称是原 wire_digest。删除整个精确 body，保留原摘要和 metadata；如业务仍允许继续，重新选择未删除来源、形成新逻辑步骤/新请求，而不能对 UNKNOWN 原 attempt 自动重发。

未来图节点、断言值、关系描述、显式语义产物、检索 query/query image、检索候选正文、EvidenceBundle、embedding 输入/向量、索引 document/snippet 都必须使用同一载荷与 lineage 契约。图 raw unit 的 metadata 原位保留为 suppressed 源，不把删掉正文的节点当合法证据。召回必须在最终返回时重验每个 source/part 的当前可见性与删除版本，输出明确群来源；文本检索和图片检索遵守完全相同的删除闭包。

非多模态 LLM 只在 Provider 投影中省略图片内容并记录 omitted part；canonical raw unit、图片来源与支持图文的 embedding 输入不因此变成纯文本。删除图片时必须处理原图、变体、图像向量及包含 base64 的历史请求；caption 不能替代这些对象，也不能成为逃离同一来源删除闭包的副本。

相同图片字节可在同 Bot 内共享物理 blob，但 source grant、media_ref 和读取能力不能共享。忘记群甲来源时清理甲的引用和派生物，群乙仍有合法独立来源则可保留同一 blob；不存在合法引用后才物理删除。若用户要忘记该 Bot 中所有相同图片内容，应把所有合法匹配来源列入更大、明确的计划范围。不能隐式扩大到其他 Bot，也不能用乙的合法引用复活甲的出处。

## 5. 受控 scrub 与晚到结果

建议接口（均为提案）：

```python
ForgetStore.plan(ticket, operation_id, selectors, expected_deletion_epoch)
ForgetStore.apply(ticket, operation_id, plan_id, plan_digest, expected_deletion_epoch)
ForgetStore.claim_scrub(lease, owner_id, limit=64)
ForgetStore.scrub_batch(lease, claim)  # idempotent actual DELETE of body rows
ForgetStore.complete(lease, request_id, expected_revision)
PayloadStore.accept_in(conn, owner, frozen_manifest, expected_deletion_epoch, body)
PayloadStore.read_in(conn, payload_id, access_scope, expected_source_revisions)
PayloadStore.observe_terminal_in(conn, original_permit, result_metadata, body=None)
```

`apply` 的同一事务先锁定当前 runtime/host 与 memory clock，增加 deletion_epoch、写 suppression/固定删除目标、标记受影响载荷不可读、登记清理任务及 metadata-only outbox。旧候选立即失去发布权；受影响的未开始发送取消，已在途/UNKNOWN 保留效果事实。控制请求重放返回原决定，不能再次增加 epoch。

异步 scrub 按固定 owner/payload 排序取锁，验证当前宿主、scrub lease/epoch、目标 manifest、payload 原摘要、合法剩余引用，并真正 `DELETE bot_payload_bodies`、删除对象/向量/索引文档。header 一次性变成 SCRUBBED，永不再回 ACTIVE；清理重试只有“已清理”回执，不会重建内容。共享 blob 的最终 GC 与新增合法引用必须争用同一个 blob/head 锁和 revision CAS。

普通插入与更新 payload 必须同时验证：源未 suppressed、对应 revision/part 仍合法、当前删除屏障、调用/Actor/工具能力及业务期望版本。只比较 `expected_deletion_epoch == current` 不够：旧 Worker 可以把 epoch 改成新值重放已删源。既有 SCRUBBED owner/payload ID 再插入 body 也必须被数据库拒绝。

晚到 HTTP/模型/工具结果可以保留受限 metadata：原 attempt 身份、原响应摘要、usage/费用、平台消息 ID、SENT/NOT_SENT/UNKNOWN 等事实。但受影响结果的正文不再写入任何 payload、工具观察、audit/outbox，也不能产生新工具提案、FINAL_READY、EvidenceBundle 或发送许可。应将“外部效果观察”与“业务内容发布”分成两层：原操作可能已 RESPONDED/SENT，同时结果内容为 SUPPRESSED；不能为了遗忘把已经发生的外部效果谎记为失败或未发送。

数据库 scrub 入口使用封闭字段/owner 枚举和已提交删除任务作为依据。不要用任意 `SET forest.allow_delete=true`、禁用所有 immutable trigger、`session_replication_role` 或调用者传表名来绕过约束。生产迁移角色和运行角色应分开：普通运行/Worker 路径无任意 body DELETE 或触发器管理权，专用 SECURITY DEFINER 函数只接受固定请求/claim 并验证所有事实。当前宿主数据库所有者权限仍是受信任边界，不能宣称数据库能阻止被完全攻陷的宿主/DB 管理员。

## 6. 必须修改的具体守卫入口

| 入口 | P4 必需动作 |
| --- | --- |
| `MessageStore.ingest`、`enqueue_job_trigger_in` | 同事务接纳 raw source/payload 与来源观察；先检查 suppression，重放已忘 source 只回 metadata receipt，不重新分配 ingress 或复活正文 |
| `MessageStore._context_in`、`_select_input_in`、`_finalize_reply_in` | hydrate 受当前删除权限约束；失效内容保留来源占位，不回退序号；最终回答的 source manifest 必须复验 |
| `MediaStore._source/_claim/_finish/materialize` | claim 固定 source revision/deletion epoch；cache 命中也复验；晚到下载不得保存字节/签发新句柄；引用计数和 quota 在物理清理事务调整 |
| `AgentStore.snapshot/prepare_model/check_model_permit` | snapshot/payload/manifest 登记依赖；最终网络前许可重验 included source parts 与媒体 grant；准备好的 bytes 不构成授权 |
| `AgentStore.complete_model/observe_tool/close_tools/_flush/commit_final` | 接收晚到 metadata，拒绝被删内容的新 payload/派生发布；tool observation trigger 只复制 metadata 与载荷引用 |
| `AgentStore.replay_prepared/recover_tool/load` | 已删载荷明确终止内容恢复，不从 Kernel 回执或新 request UUID 绕过；UNKNOWN 不转可重试 |
| `ChildStore.spawn/prepare/check_permit/complete/adopt/media` | 私有 snapshot/模型/结果使用父来源 manifest；删除后不采纳子结果，不从父缓存恢复图片 |
| `DeliveryStore.enqueue_in/claim/permit/mark_started` | intent 绑定回答 source manifest/deletion epoch；精确发送许可最后复验，未开始工作因删除失效 |
| `DeliveryStore.complete/reconcile/recover` | 原 attempt 的结果仍可对账，但 receipt 内容须白名单；scrub 后不得把 body 再附回、不得让 NOT_SENT 人工决定自动复活消息 |
| `BotStore.admit/_guard/commit_state/finish_call/authorized_call` | 跨 Worker 调用传播由宿主签发的来源依赖；参数/结果/写状态登记 lineage；被删 result 重放只给已完成且内容不可用的 receipt |
| `BotStore.capture_state_migration/stage_state_migration/_publish_state` | 迁移源/候选都受同一屏障；旧状态迁移结果不能恢复已删值，所有中间副本可清理 |
| `BotControl._record`、`BotStore._event`、Web/Shell inspect/read | 分离 metadata 与自由正文载荷；幂等历史读取也走 hydrate，审计不能成为旁路全文导出 |
| P4 事实 reducer、图发布、embedding/index upsert、EvidenceBundle 构建 | 从可信 source manifest 验证当前内容；删除前候选晚完成不能发布；检索最终读取再次过滤，随后物理删除外部索引副本 |

**锁序特别注意 Delivery.complete。** 它目前为了接纳旧宿主的效果观察只锁 part → attempt，不锁当前 runtime/host。如果删除协议先锁 memory clock 再锁 part，而 complete 在 part 后追加 memory clock，会形成反向锁序。实现时把 deletion/内容资格锁放在共同规定的前序（观察接口可以不要求现宿主，但应先取得该锁），或使观察只能写固定 metadata、不创建/复活任何 payload。不要在现有函数尾部简单添加通用 `_host`/memory guard。

## 7. 操作幂等与来源审计不能一起被擦掉

重复的同一 operation 仍应返回“已接纳/已完成，载荷已清理”，而不是 NOT_FOUND。对不同输入仍比较原 input/payload/result digest，拒绝冲突；不重新运行已完成 Call、不再次分配 Job occurrence、不再次发消息。

保留事实头中的摘要不意味着它可以公开：短文本或常见图片的无盐哈希可被猜测验证。原摘要用于内部幂等，默认不提供跨 Bot 全局查哈希接口；如果要求连身份展示信息也遗忘，昵称/群名片放 payload，审计仅保留受保护的不透明 principal/source IDs。应在计划中区分“忘记内容”和“删除身份资料”，不能把未清理的作者完整 JSON 都称作无敏感性的 metadata。

对于原 `bot_calls.result` 等没有独立 result_digest 的位置，先补并冻结摘要，再允许 body 擦除。模型的原 manifest_digest、wire_digest 与结果摘要同理。一个 SCRUBBED 请求仍证明发过何种不可复写身份的操作，但不能再恢复其全文；这是明确的恢复能力变化，需要 typed `PAYLOAD_ERASED` 分支，不能用空 JSON 冒充合法原结果。

## 8. 物理清理的完成标准

删除协议分阶段报告：

| 阶段 | 能保证什么 |
| --- | --- |
| `REVOKED` | suppression/epoch 已提交，新读取和发布不可见；**尚不能声称明文已物理清理** |
| `ACTIVE_PAYLOADS_REMOVED` | 正文/图片/请求 body 行与当前对象/索引载荷已实际删除，缓存句柄撤销，晚到写入被拒绝 |
| `STORAGE_RECLAIM_PENDING` | 历史数据库页、快照、WAL/备份及受控导出仍待保留策略回收，有明确清单和期限 |
| `PURGED` | 约定范围内的在线副本、持久派生、临时载荷和备份/恢复约束已核验完成；已发到外部服务的内容仍按既有许可边界单列 |

PostgreSQL 的 UPDATE/DELETE 不立即移除旧行版本，普通 VACUUM 主要让旧空间可回收，不能据此宣称磁盘上的历史 bytes 已逐字擦除。旧表重建、DROP COLUMN 或删除 payload 行都必须结合存储回收与备份政策，不能仅靠 API 看不到来验收。[PostgreSQL 17 常规清理文档](https://www.postgresql.org/docs/17/routine-vacuuming.html)

WAL 与基础备份可以恢复旧时间点的数据，因此旧明文备份必须有独立生命周期；从早于遗忘的备份恢复后，应在开放任何读取、连接或执行之前重放独立持久的 suppression/deletion ledger，并完成相应清理，不能恢复到“删之前”直接上线。[PostgreSQL 17 持续归档与时间点恢复](https://www.postgresql.org/docs/17/continuous-archiving.html)

可对新载荷采用按删除域的 envelope encryption，降低历史物理副本残留风险，但它不代替 body/object 实际删除；也不能用当前全局 SecretVault 密钥模拟单条内容删除，否则要么旧密钥仍可恢复，要么误伤其他内容。密钥备份与销毁必须在同一完成标准内。旧版本已经生成的明文 WAL/备份不会因迁移后加密而自动消失。

Python bytes、HTTP 客户端缓冲和运行中的 Worker 也可能暂时持有明文。取消并有界回收相关执行、清理缓存/临时文件、拒绝所有新内容派发；无法证明释放的进程应隔离或终止，不把引用计数减为零当作安全擦除证明。已经获得许可并在途的模型/QQ 请求不能承诺撤回，保留必要的 UNKNOWN/计费/平台 receipt 元数据。

## 9. 实施切分与必须先有的验收

建议 root 先安排“载荷/删除基础契约”与“raw source 接纳”两条协调工作线，冻结 `SourceManifest`、PayloadRef、删除锁序、typed unavailable/replay 结果，再并行接 Agent/Media/Delivery/Kernel 副本。图与 embedding 可依赖这份契约开发，但不能在任一老明文路径未迁移时宣称完整遗忘完成。

验收至少包括：

1. 使用唯一 canary 文本和唯一测试图片生成 Inbox → 模型请求 → tool/child → Reply/Delivery → Kernel/audit 副本，执行遗忘后检查所有 body 表、原列/旧表、base64 精确请求及可读索引均无载荷；单验查询过滤不足。
2. 延迟图片下载、模型、子模型、工具、embedding 与图发布，先提交删除后放回结果：允许 metadata 结算，不得新增正文、字节、向量、可发送回复或工具提案。
3. scrub 中途崩溃后接管，已删 body 不重新插入；相同 operation 仍幂等完成，UNKNOWN 仍 UNKNOWN。
4. 共享图片的甲源删除与乙源引用接纳/GC 交错，证明合法乙引用保留且甲来源不能恢复；Bot B 完全隔离。
5. 同一请求中混合两个群来源，删其中一个后原精确请求不可重放；新上下文仍有正确未删群来源和不可用占位。
6. 老 Inbox、Kernel result、状态迁移候选、Job prompt 和旧备份带“新 epoch”重放，仍被稳定 suppressed source 拦截。
7. 有明确证据的在线载荷删除与备份未到期状态分别显示；从旧备份恢复的 Bot 在 suppression 重放前拒绝所有读/写/外发。

上述是完整迁移的验收边界；最初 `001–020` 版本未提供这些表、scrub 权限、载荷分离或删除屏障，当前完成范围以正文迁移协议和第 10 节为准。

## 10. 029–035 之后的剩余族与下一实现切片

本节保留 029–035 完成时的后续设计依据。其中 Kernel、状态、迁移正文、代际配置及长期 Worker 暴露账本已由 036–038 接入，会话 Job settings/delegation 已由 060–061 接入，Shell 参数/结果由 062–063 接入；实际协议见 [Kernel 正文与代际来源](p4-kernel-payloads.md)、[Job 定义载荷](p4-job-payloads.md)和[Shell 参数与结果载荷](p4-shell-payloads.md)。部署管理正文和备份路径仍不能视为完成。

### 10.1 Kernel 返回值、状态及迁移副本应一起处理

| 仍含正文的位置 | 实际写入/读取路径 | 最小迁移契约 |
| --- | --- | --- |
| `bot_calls.result` | `Kernel._invoke` 读取 `authorized_call/authorized_child_call` 的旧回执；Worker 返回后 `BotStore.finish_call` 缓存结果。`AgentStore.recover_tool` 也直接读该表 | 增加并冻结独立 `result_digest`、`result_payload_id`；原列严格 NULL。COMPLETED 回执不会因 body 消失变成 NOT_FOUND，不再调用 Worker |
| `bot_component_state.value`、`bot_state_revisions.value` | `Kernel._state_rpc` → `BotStore.read_state/commit_state`；`snapshot_state/_state_source` 又读全量状态 | 值按固定 `(component,key,revision)` 引用载荷；当前头保留 revision/deleted，历史行保留原值摘要。删除正文不能回退 revision，也不能用业务值 `None` 冒充已擦除值 |
| `bot_state_steps.receipt` | `commit_state` 的回执实际含完整 `value`，相同步骤重放直接返回旧 receipt | 回执 metadata 保留 operation/revision/原 input 和 payload digest；值只通过同一状态载荷 hydrate。结果内容不可用仍证明该写入已经执行 |
| `bot_state_migration_sources.source_rows`、`bot_state_migration_results.result_values` | `capture_state_migration` → 两次 Worker `migrate` → `stage_state_migration` → `_publish_state` | 源和候选分离载荷，候选继承全部实际迁移源；最终发布同时复验 route、state revision、runtime fence 与载荷依赖，禁止旧候选恢复被删状态 |

代码入口以当前函数为准：`kernel.py` 的 `_state_rpc/_service_rpc/_invoke/_install_one`；`storage/bot.py` 的 `admit/admit_child/_guard_time/read_state/commit_state/finish_call/authorized_call/authorized_child_call/capture_state_migration/stage_state_migration/_publish_state`。状态、Kernel 回执和 Agent 工具结果之间必须传递宿主掌握的载荷引用；仅迁列、不连接读取依赖，仍会把旧值重新写进新回复。

建议新增内部 `CallDependencyStore`，固定 Call 的输入依赖 manifest 和摘要，并持久记录返回给 Worker 的状态/子调用结果载荷。`read_state` 必须在同一事务内完成授权、hydrate、登记实际 `(key,revision,payload_id)`，然后才能把值返回 Worker；不能等 Worker 自报“使用了哪些 source”。子服务返回、包括命中已有 COMPLETED 回执的路径，也要在返回父 Worker 前登记其结果依赖。`commit_state`、`finish_call` 以这些真实依赖创建结果载荷，重放只比较原摘要与固定依赖身份。

非 Agent 的入口也必须由 Host 绑定来源。OneBot normalize 的返回值含消息原文，prepare 的返回值含 `wire_body_b64`；前者需要在调用 codec 前建立稳定入口来源，后者应继承已经存在的 Delivery 请求载荷。不能从 operation ID 的字符串格式猜来源，也不能接纳 Worker 或模型提供的任意父载荷 ID。Agent 接收 Kernel 结果时，由 Host 将实际 Kernel `result_payload_id` 传入 `AgentPayloadStore.tool_result_in(..., parents=...)`。

### 10.2 单次 Call 的读取集合不足以覆盖长期 Worker

当前 Python Worker 长期复用 Plugin 对象。`Kernel._install_one` 把配置和恢复状态交给 Worker，后续 Call 还可将读取值缓存到对象属性。即使某次 `state.read` 已正确登记在 Call A，Call B 仍可能从缓存返回同一内容。因此不能把单次 Call 读取闭包称为完整跨调用防复活。

下一切片采用保守的 **generation exposure 依赖头**：

1. 初始配置、restore 状态及随后交付给该 Worker 的每次输入、状态读取和子服务返回，都先在数据库中登记对应载荷与 exposure revision，再交付正文。
2. 后续结果和状态写入继承该 generation 当前已暴露的依赖集合；并发 Call 共享该头，按数据库顺序合并集合，不按模型完成时刻覆盖。实现可采用不可变分块 manifest 加 CAS head；超过容量明确拒绝并轮换 Worker，不能丢弃历史暴露记录后继续使用同一进程。
3. 遗忘后该 generation 的新业务 admission、结果/状态提交均因来源屏障被拒绝；监督器回收实际 Worker，再从仍可读的配置和状态重建。已经在途的外部效果只允许原 metadata 对账，不能假称撤回。

这是对框架控制的 Worker 输入、状态和回执的保守追踪。可信 Python 插件仍不是 OS 沙箱：其自行访问的文件、网络或外部持久副本必须另列边界，不能承诺宿主能够擦除任意第三方程序保存的内容。

锁序保持组件资源/路由 → Bot runtime/Host → Call 树和状态头 → exposure head → memory clock → payload；跨 Call 的共享 exposure 更新在返回数据前完成。遗忘后的监督回收不在数据库事务中等待进程退出。提交与关闭仍需当前 generation/Host/maintenance/storage fence，来源摘要相同不替代执行授权。

### 10.3 配置与控制面回执不能遗漏

| 待迁移正文 | 来源与行为要求 |
| --- | --- |
| `bot_agent_runs.spec` | `AgentRuntime.resolve_spec` 同时把 values 展开在顶层并保留 values 子对象，`AgentStore.ensure_run` 冻结整个 spec；system_prompt 实际存在重复。整体迁为 spec 载荷，另保留白名单的配置/权限/预算 metadata，使正文失效后仍能终止和结算 |
| `bot_config_plans/revisions.values`、`bot_agent_config_plans/revisions.values`、Binding plans/revisions/current config、`bot_generations.config`、`bot_dev_watch_heads.config` | 配置预览、发布、热重载与重启都应 hydrate 同一配置载荷链。schema/template/自由 JSON 也可能含正文，不能只处理名为 system_prompt 的键。凭据 ciphertext 保持独立生命周期 |
| `bot_web_operations.result`、`bot_audit_records.details`、`bot_outbox_events.payload` | `BotControl._record` 将预览 result 写 Web 回执，又经 `BotStore._event` 复制到 audit/outbox；result 含 values/before_values 时形成至少三份正文。改为白名单 metadata 与同一结果载荷引用；`_replay`、history、inspect 均经授权 hydrate |
| `bot_shell_requests.arguments/outcome` | 已由 062–063 迁移为参数、结果、delivery receipt 三个载荷；管理输入和 CONTROL 回复继承真实父链，删除后旧命令不能凭新 operation 或当前 epoch 重建正文 |

配置是独立的管理员输入，不应凭文本相似就擅自推断其来自某条聊天。新的自动复制路径必须显式继承来源；手工输入以配置/命令自身的可管理删除域登记。旧无法证明上游的派生体继续使用明确的 legacy conservative 策略；管理计划应展示影响范围，不能悄悄把保守清理包装成精确单条遗忘。

### 10.4 031/032 的当前闭包与局部守卫要求

031 的 snapshot 继承实际 Context 和私有 closed_parts；manifest 继承 snapshot 与 BOUND evidence；request → model result → tool arguments/results → closed_parts → Reply 继续传递父载荷。032 的 child snapshot 还继承父工具参数和父模型输出，child candidate → 父工具结果也传递实际结果载荷；因此 child task 中转述的跨群证据不会只因未列入显式选中 unit IDs 而丢失删除依赖。

**可读载荷与当前执行授权必须同时复验。** Child 的发送许可、结果发布和父采纳需要使用完整 `AgentMemoryStore.guard_run_in`，不能只调用检查 payload 的内部 guard，否则父 BOUND evidence 的当前 MemoryConfig/来源资格撤销可能被漏过。结算入口保留 metadata 分支，不能把来源 guard 放在费用结算前的无条件身份入口。

另外，旧快照在 Actor 接管时可能已被擦除：`AgentStore.load` 的 hydrate 会在 `AgentRunner.advance` 获得 head 之前抛错。恢复必须先凭当前 Actor、准确 Conversation/active Session 和 Run metadata，在同一事务重新核实真实撤销，再终止该已存在 Agent；不能误走“没有 Agent checkpoint”的 legacy 分支，也不能为了取得新 owner 而恢复旧正文。来源未撤销、其他群、旧 Actor 和无 Agent 的 legacy Run 均不得被这一分支结束。

### 10.5 下一族必须具备的真实验收

1. **跨调用缓存与重启**：真实 Worker 的 Call A 读取带 canary 的状态并缓存，Call B 不再调用 state.read；遗忘 A 后，B 不得写状态/返回可发布结果。已暴露 generation 被回收；新 Worker 只能读取合法剩余状态，旧状态迁移候选也不能恢复该 canary。
2. **回执与物理副本**：同一状态操作生成 current/history/receipt、Kernel result、Agent tool result，控制预览另生成 Web/audit/outbox 副本。删除后核查原列 NULL 和实际 body 删除；同 operation 保留原完成状态及摘要，不重复执行；旧 UNKNOWN 保持明确不确定。
3. **权限与接管**：在最终 Kernel Gateway 写入前、Child 已获 permit 后以及重新 claim 一个已擦除快照前交错撤销。完整权限复验阻止新发送/发布；已知费用仍结算，结束后按既有 silent drain 完成 Session，其他群及无 Agent 的 legacy Run 不受影响。
4. **非空旧库升级**：真实旧状态、迁移候选、长 operation ID、已完成 Call、配置预览/历史与 Job/Shell 数据，从发布过的 SQL 起步迁移；中断/重启后固定摘要、状态和删除 marker 不变，执行期没有旧正文列 fallback。

## 11. 036–038 完成时记录的后续实施边界

本节是 036–038 完成时固定的后续清单。039–041 随后完成 Bot 控制回执，042–049 完成配置定义与实际运行父链；对应行保留用于说明依赖为何必须一起迁移。现存自由正文不能只按列名判断：管理员输入的取消 reason 会被复制到 `bot_conversation_triggers.terminal_reason`、`bot_agent_runs.error_code` 和不可变 checkpoint 的 `payload.error_code`，这些位置仍属于待处理正文。

| 剩余族 | 必须一起完成的读写路径 |
| --- | --- |
| 控制结果、审计与 outbox | `BotControl._record` 当前复制同一预览结果到三处；改为共享结果载荷和封闭 metadata。旧 operation 的完成状态、原摘要保留，正文不可用时不得重新执行 |
| 取消及分类原因 | 自由 reason 与固定状态码分离。旧 checkpoint 的原 checksum 不改写，迁移原正文并保留其引用；终止和结算不能依赖已删原因正文 |
| Bot 配置与冻结 spec | 组件/Agent/Memory/Binding/Watch 的 plan、revision、current 和 `AgentRun.spec`；真实配置父载荷须继续传到 generation、模型请求及子 Agent。SQL 发布、调度字段和表达式索引同时更新 |
| Shell | 已接受命令输入 → arguments → 管理结果/CONTROL 已由 062–063 接通；最终写事务复验实际父载荷，不能换新 operation 恢复旧参数或结果；Job definition → occurrence 输入已由 060–061 接通 |

Web/CLI 的手工配置属于独立管理员输入域，不按文本相似猜测聊天来源。Shell 命令来自真实群消息，其派生内容须继承该消息；框架自动复制配置或记忆时同样继承实际父载荷。旧 generation/Reply 的不可变 manifest 不能原地补写；无法证明旧上游时采用明确的保守迁移策略。

部署库的 Bot 名称、配置及请求回执使用独立管理域，不能引用私有 Bot 数据库的外键，也不能声称由 Bot 的聊天删除覆盖。密钥密文、WAL、备份及外部副本仍有独立生命周期。下一族验收须包括真实预览/历史/幂等回执的正文清理、Job 最终入队前撤销、重启后不复活，以及保留原 checksum 的非空旧库升级。
