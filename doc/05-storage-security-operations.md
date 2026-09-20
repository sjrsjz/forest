# 持久化、鉴权与运维详细设计

## 1. 事实源与存储配置

PostgreSQL 作为首版持久事实与协调账本。每个 Bot 的共享图包含多个群聊主干，图逻辑事实、来源与发布决定属于同一 Bot 事实域；向量索引、图查询投影、对象存储和消息通知是可替换适配器。记忆原生保存文本与图片，使用统一有序 `ContentPart`；图片的受管字节、来源与引用不会因为当前对话 LLM 仅支持文本而删除或改写。媒体原始载荷存受管对象存储，事实库持有其不可变字节修订、内容摘要、权限和生命周期引用；完整契约见[11 多模态记忆](11-multimodal-memory-and-model-projection.md)。

| 存储域 | 保存内容 | 权威边界 |
|---|---|---|
| 部署目录库 | 包、Bot 目录、平台账号唯一归属、部署配置、Web 身份/会话/初始化状态、管理请求关联、批量操作进度 | 部署资源的权威；Bot 配置列表仅作带版本的目录投影，不代替目标 Bot 的提交 fencing |
| 每 Bot 事实库 | Bot/组件配置修订及生效头、路由 epoch、授权版本、Inbox、Context、Run、工具、记忆、outbox、Job | 该 Bot 的配置、授权、事务和写入权威 |
| 每 Bot 向量命名空间 | 来源/投影版本对应的向量及最少候选元数据 | 召回加速，最终读取回事实库验证 |
| 对象存储 | 媒体、不可变包、候选派生产物、snapshot | 按 Bot/包命名空间和授权句柄读写 |
| 易失缓存/通知 | ready 唤醒、路由快照、热点查询、stream | 可全部丢弃并从权威账本恢复 |

`MemoryStoreProfile` 指向 `fact_store_ref, vector_store_ref, vector_namespace, object_namespace, embedding_profile, storage_generation`。凭据使用 secret_ref，不出现在普通 Bot 配置、manifest、消息载荷或日志中。

默认每 Bot 独立数据库/凭据，服务器可共享。允许显式的共享数据库模式时，所有表仍包含 bot_id，强制 scoped repository/RLS，数据库角色不能绕过隔离；超级用户不用于业务连接。Worker 不接收任意 DSN，也不能由参数选择另一个 profile。

## 2. 数据模型

下表是跨模块协议需要保存的**逻辑事实词汇**，不是一份共享 SQL schema，也不是 `BotStore.migrate()` 的建表清单。实际表由拥有该事实的模块以内容寻址 schema bundle 在独占 namespace 中创建，并只能经该模块 Repository 访问；模块之间保存带摘要的类型化引用，禁止跨 namespace 外键、宿主直查和跨模块 join。首次发布前每个模块只有从空 namespace 安装的 baseline，不生成或保留开发期编号 SQL migration。所有 `*_at` 使用 UTC，外部 ID 用字符串；独立 Bot 数据库仍保留 `bot_id` 和检查约束。下述主键均为逻辑简写，共享物理部署时必须将 `bot_id` 纳入本模块的主键、唯一键与外键。

事实所有权按职责拆分：deployment 目录、包仓库、账号和管理入口分别由对应 deployment 模块持有；`kernel.runtime` 只持有 lease、Generation、Route、Lifecycle、通用 Call、状态引用和 RuntimeGraph；Payload、Authorization、Effect 与 Audit 分别属于自己的 kernel 模块；Shell、配置、Adapter、Conversation、Agent、Routing、Delivery、Scheduler 与 Memory 各自由准确模块持有自己的表。后续各节把相关事实放在一起是为了描述跨模块因果关系，不授予任何组件把它们建进同一个 `public` schema 的权力。准确模块表、namespace、bundle 身份和 Repository contract 以[干净数据库基线](14-clean-schema-and-payload-provenance.md)为准。

### 2.1 控制与所有权

| 表 | 主要字段 | 关键约束 |
|---|---|---|
| `bot_catalog` | bot_id、status、profile_ref、storage_generation、config_revision、observed_at | 部署库；bot_id 不复用；config_revision 是带观测版本的目标配置投影，不能作为 Bot 配置 CAS；profile_ref/storage_generation 是部署目录的存储定位/迁移切换权威，按 2.5 与 Bot 库 ACTIVE/SEALED 及写入 fence 协调 |
| `binding_catalog` | binding_id、bot_id、platform、external_account、status | 部署库；活动 `(platform, external_account)` 唯一 |
| `module_packages` | package_id、version、digest、trust、artifact_ref | 三元组不可变；摘要校验 |
| `kernel.runtime/routes` | bot_id、component_id、service_name、service_major、generation_id、route_epoch、admission | 路由键唯一；epoch 递增；不携带 Agent/Adapter 业务字段 |
| `kernel.runtime/runtime_state` | bot_id、runtime_revision、storage_generation、status、data_admission、control_admission、maintenance_epoch | runtime_revision 只仲裁运行封印；维护意图提升 maintenance_epoch，旧恢复流程不得覆盖新决定 |
| `kernel.authorization/policy_clock` | bot_id、policy_revision | 授权撤销事实与 revision 同事务单调推进；授予集、票据和消费事实在 Authorization 自有表中，不塞入 Runtime |
| `lifecycle_operations` | operation_id、affected_resources、phase、owner_id、op_epoch、lease_until、old/new_plan、error | 相交资源独占；阶段迁移 CAS |
| `kernel.runtime/module_resource_leases` | resource_key、operation_id、op_epoch、owner_id、generation_id、lease_until | 相交生命周期资源唯一归属；按规范 resource_key 顺序获取 |
| `binding_sessions` | binding_id、connection_epoch、owner_id、lease_until、capabilities | 每绑定最多一个获准发送的所有者 |
| `command_requests` | command_id、source_event_id、raw_ref、origin_bot_id、origin_binding_id、origin_conversation_id、origin_principal_ref、origin_auth_ref、target_bot_id、target_resource、target_principal_ref、target_auth_ref、command_key、command_version、schema_digest、registry_snapshot_id、argv_ref、argv_digest、input_digest、service_class、state、operation_ref、approval_ref、reply_route、created_at、deadline_at | 单Shell入口`(origin_bot_id, source_event_id)`唯一；RECEIVED冻结来源/原文引用/路由/快照，PARSED再冻结规范命令/参数/目标；operation_ref为`{kind,id}`，approval_ref为`{bot_id,approval_id}` |
| `command_registry_snapshots` | snapshot_id、scope、revision、digest、entries_ref、created_at | 不可变的名称/别名/契约/实现代际解析快照；核心前缀不可由插件覆盖 |
| `shell_sessions` | shell_session_id、sender_principal_id、origin_bot_id、conversation_id、default_target_bot_id、output_preferences、expires_at、revision | `(sender_principal_id, origin_bot_id, conversation_id)` 唯一；默认目标/偏好/TTL 不是授权，实际请求仍逐次重验 |
| `management_requests` | management_request_id、entrypoint、web_subject_ref、session_ref、target_scope、action_key/version、input_ref/digest、plan_ref/digest、expected_revisions、idempotency_key、admission_state、targets_manifest_ref、operation_ref、approval_ref、created_at | 部署库；Web 接纳与关联；同主体/目标/动作/幂等键唯一，同键异摘要拒绝；单目标可直接关联 operation/approval，批量 targets_manifest_ref 固定不可变目标清单及稳定目标请求 ID；执行阶段从目标账本读取 |
| `management_request_targets` | management_request_id、target_key、child_request_id、input_digest、operation_ref、approval_ref、observed_state、observed_at、last_error_ref | 部署库；父请求/目标唯一、child_request_id 稳定；逐目标接纳关联及可重建进度缓存，不是跨 Bot 执行账本，不把一个目标成功覆盖为整批成功 |
| `config_drafts` | draft_id、owner_subject_ref、target_scope、draft_revision、base_revisions、schema_digest、values_ref、secret_refs、updated_at、expires_at | 部署库；主体/目标隔离、草稿 revision CAS、受限保留期；只保存 secret refs，保存草稿不改变目标 desired/effective，当前授权仍决定能否重开或提交 |
| `config_revisions` | config_revision_ref、resource_key、revision、previous_revision_ref、inherited_revision_refs、schema_digest、values_ref、values_digest、secret_refs、origin_ref、plan_digest、created_by、created_at | 资源权威库；不可变且资源内 revision 唯一；previous_revision_ref 是同资源上一修订，inherited_revision_refs 固定物化快照的所有父配置，来源管理请求/目标资源幂等唯一，密钥仅保存版本化引用 |
| `config_heads` | resource_key、desired_revision_ref、effective_revision_ref、head_revision、apply_policy、operation_ref、last_error_ref | 与配置资源同库；desired 接纳不代表 effective 已切换；head_revision 做 CAS，不用目录缓存替代；各资源独立显示状态 |
| `web_accounts` / `web_sessions` | subject_id、issuer/subject、credential_record_ref、account_revision；session_id_hash、subject_id、session_revision、authn_strength、expires_at、revoked_at | 部署库；显示名不参与身份等价；认证凭据受限存储；服务端会话撤销后停止发放新操作票据 |
| `web_setup_state` | deployment_id、state、setup_token_digest、expires_at、consumed_at、initial_admin_ref、revision | 部署库；未初始化状态 CAS 创建首管理员并单次消费初始化令牌；无公开默认管理员或可重复远程注册窗口 |

生命周期资源互斥以逐资源的 `lifecycle_resource_leases(resource_key PK, operation_id, op_epoch, lease_until)` 实现，按排序后 resource_key 获取全部资源；不使用“检查无重载→稍后写入”的竞态流程。跨 Bot 批量操作在部署库记录父操作，每个 Bot 使用自己的子操作事务。

`ChangePlan/plan_ref` 复用受管不可变工件和 manifest 存储：固定目标、规范输入/差异、schema、所有 expected_revisions、影响集合、生效步骤、审批需求与摘要，由入口请求引用并在目标接纳时复制或固定其验证依据。计划是提交对象，不是执行状态机；完成进度从所关联的原有 operation/job/effect 读取。草稿和目录计划可以在部署库保存受限候选内容，但不具有目标 Bot 的配置或许可权威。

`command_requests` 是入口请求与可查询关联，不是另一套效果或生命周期状态机。重载/启停引用 `lifecycle_operations`，后台检查/重建引用 `jobs`，外部执行引用 `effect_operations`，`run.cancel` 等类型化资源操作引用已有目标资源的持久操作事实；`operation_ref={kind,id}` 明确其可查询种类及稳定 ID，尚未创建子操作时可空，不新增 `control_operations`。执行器沿用原账本的阶段、lease、幂等和 UNKNOWN 规则。详见[09 群聊 Shell](09-group-shell-command-system.md)。

命令字段按状态形成 CHECK：`RECEIVED` 已持久固定可信来源、`raw_ref`、原 ReplyRoute 与注册表快照；命令键/版本、schema、规范 argv、目标及摘要在事务外有界解析成功后，以 `PARSED` 短事务一次冻结。未解析或遭拒请求可没有这些派生字段，不能填入伪值。当前授权检查通过后进入 `AUTHORIZED`，此时才按宿主确定的 service_class 取得 P0/P1 执行额度；接入识别/解析另有有界预算及主体限额，不允许任意带核心前缀的无权请求先挤占救援执行容量。

首版只有一个核心 Shell 消息入口，`command_requests` 以 `(origin_bot_id, source_event_id)` 去重，不以解析后的 command_key/schema/argv 组成入口唯一键；未知命令、语法失败和重载后重放均返回原请求。WebUI 是独立的 HTTP 管理入口，使用 `management_requests`，不虚构 platform event 或塞入 Shell 的去重命名空间。两个入口在类型化管理服务汇合，复用同一目标执行账本；`management_requests` 仅记录接纳、拒绝、目标操作/批准的关联与可重建展示缓存，不能自行决定目标已经成功、取消或回滚。完整接口见[12 WebUI](12-webui-administration.md)。

### 2.2 接入、会话与执行

| 表 | 主要字段 | 关键约束 |
|---|---|---|
| `inbox_messages` | message_id、binding_id、platform_event_key、conversation_id、ingress_seq、author_snapshot、payload_ref、context_disposition、context_role、safe_projection_ref、command_route、command_id?、registry_snapshot_id?、trigger_decision、classification_job_id | 平台去重键唯一；会话 ingress_seq 唯一；command_route 为 CONTROL/BUSINESS/NONE；CONTROL_COMMAND 不生成 Agent trigger，但仍进入上下文流水 |
| `conversations` | conversation_id、canonical_key、next_ingress_seq、classified_through_seq、context_applied_seq、context_revision、context_view_revision、next_commit_seq、next_response_seq、active_session_id、mailbox_revision、owner_id、actor_epoch、lease_until | canonical_key 唯一；游标只前进；actor_epoch 递增 |
| `conversation_sessions` | session_id、conversation_id、status、active_run_id、session_epoch、created_at、closed_at | 同会话最多一个未终结 Session；WAIT/RELOAD 不能释放此唯一性 |
| `conversation_triggers` | trigger_id、event_id、ingress_seq、policy_revision、principal_ref、status、claimed_session_id、claimed_run_id、input_batch_id、answered_reply_id、error | 事件/触发类型幂等；pending/claimed/answered/failed/cancelled 有显式转换 |
| `context_input_batches` | input_batch_id、conversation_id、session_id?、run_id?、start_seq?、end_seq?、kind、trigger_ids、source_manifest_ref、input_revision、context_revision、status、input_digest | 固定切口；批次幂等；TRIGGER_INPUT/SILENT_DRAIN/RECOVERY_CLAIM，恢复claim空区间引用已有消息而不追加 |
| `runs` | run_id、trigger_operation_id、conversation_id、session_id、input_revision、state、holds_actor、owner_epoch、lease_until、deadline_at、config_snapshot、reply_route | 属于会话当前Session；holds_actor=true最多一个，仅此约束不足以代替Session独占 |
| `run_inputs` | run_id、input_batch_id、event_id、input_order、first_visible_input_revision | 输入按批追加不重复；模型调用固定所见 input_revision |
| `run_checkpoints` | run_id、checkpoint_seq、session_id、session_epoch、input_revision、input_batch_ids、claimed_trigger_ids、flushed_step_ids、context_view_revision、schema、phase、payload_ref、digest、reload_operation_id | 唯一且追加；能恢复原切口及尚未回应触发 |
| `context_units` | unit_id、conversation_id、commit_seq、unit_index、run_id?、input_batch_id?、source_event_id?、source_ingress_seq?、author_snapshot、kind、payload_ref、parts_manifest_ref、digest | payload 内为统一有序 ContentPart；GROUP_MESSAGE 来源唯一；静默输入允许无Run；输入顺序与模型步骤因果顺序分别保存 |
| `context_compactions` | compaction_id、conversation_id、source_manifest、input_digest、base_context_revision、base_view_revision、context_view_profile_ref、token_budget、tokenizer_version、trim_policy_version、pin_set_ref、deletion_epoch、result_ref、status | result_ref 为确定性 canonical 活动视图 manifest；预算/模态成本策略由 context_view_profile_ref 固定，独立于实际 LLM 的单次请求投影；先归档后移出视图，不生成模型摘要；旧候选不得覆盖新来源/身份，输入游标不回退 |
| `run_steps` | run_id、logical_step_id、input_revision、visible_trigger_ids、contract、input_digest、result_ref、closed、context_unit_ids、status、model_request_id?、transport_attempt_id?、model_call_epoch、provider_receipt?、usage? | 完整步骤可幂等flush；模型调用/传输阶段与不确定状态沿用该步骤及checkpoint，不另设执行账本；同step至多一个当前有效request/attempt，接替时CAS推进model_call_epoch；未闭合流/工具不与新群输入交错 |
| `model_request_manifests` | request_id、run_id、logical_step_id、context_revision、context_view_revision、input_revision、input_batch_refs、source_cut_digest、source_manifest_ref、actual_deployment_id、model_revision、capability_snapshot_ref、capability_revision、projection_version、evidence_renderer_revision、included_part_refs、omitted_part_refs、omission_reasons、projected_units_ref、media_transform_refs、input_digest、projected_content_digest、budget_snapshot、deletion_epoch、policy_revision | ModelRequestManifest 为不可变语义请求；source manifest 覆盖实际纳入内容及必要派生依据，omitted 清单不表示已见；不放 wire hash/可变派发状态，不回写 canonical Context 或充当裁剪结果 |
| `model_transport_attempts` | transport_attempt_id、request_id、payload_digest、serializer_revision、permit_ref、permit_expires_at、budget_receipt_ref、created_at | ModelTransportAttempt 固定已prepare精确API body的wire摘要；短效URL不普通持久化；刷新URL产生新attempt，不改旧hash；派发state/receipt/usage沿用RunStep/checkpoint并按attempt关联 |
| `approval_requests` | approval_id、operation_id?、command_id?、origin_ref、principal、scope、input_digest、plan_digest、argv_digest?、command_version?、schema_digest、expected_revisions、generation_digest、origin_bot_id?、target_bot_id?、conversation_id?、token_digest、expires_at、decision、consumed_at | 工具、Shell 与 WebUI 共用目标域批准权威；origin_ref 按 shell/web/tool 分支约束，Web 不填伪造群会话；对象不可变，决定绑定主体/计划/目标/版本；令牌短效且原子单次消费 |

`holds_actor` 表示当前是否持有执行/提交 lease；active_session_id 表示同群响应链的逻辑所有权，两者不能合并。长工具/人工确认可以释放 lease，但新消息只排队，不能建立第二个活跃 Session；恢复必须校验原 session_epoch 和 input_revision。无 Agent 的静默输入作业持有短期 Actor 写权，同样与启动 Session 事务互斥。

context_applied_seq 只证明消息前缀已有 Context/有效占位记录，不能代替 trigger 完成状态；触发在 InputBatch 中领取，只有最终回复或明确失败/取消决议才终结。作者快照含可信 platform/user ID、subject_id、当时显示名、消息类型与引用来源，不能仅有全批一个 sender 字段。08 是这些新表的交互状态与恢复语义来源。

核心最小识别为 CONTROL 消息同事务生成保守的 `context_role` 和 `safe_projection_ref`，使完整参数解析或权限检查尚未结束时也不会将原始管理载荷暴露给 Context。后续安全投影可按声明的字段/输出策略完成，但不能在失败重试中退回未脱敏正文；可读性与历史连续水位仍是独立事实。

批准前尚未创建目标操作时，Shell 的 `approval_requests` 绑定来源 `bot_id + command_id` 和冻结计划，Web 绑定 `management_request_id` 及可信主体，既有工具批准绑定其 `operation_id`。适用字段随 `origin_ref` 分支形成 schema 约束，至少有一个明确的请求/操作对象。确认事务同时校验令牌摘要、确认主体、该分支的来源绑定、期限、计划和当前授权，再以 CAS 写入决定及 `consumed_at`；随后执行仍校验目标 revisions/generation，改变参数或计划必须重新取得所需批准。无须批准的例行可逆命令按部署策略直接执行，不为每个命令另造确认步骤；不新增 `command_confirmations`。

有副作用的批准权威与目标操作同驻目标 Bot 事实库。跨 Bot 来源 `command_requests` 只保存 `approval_ref={bot_id,approval_id}`；对端命令关联是受验证的来源身份引用，不伪造跨独立数据库外键。目标库中的批准与目标执行许可可在本地统一锁序事务内重验，来源库的批准显示缓存不具有执行授权。

Web 来源使用 `origin_ref={kind:web,management_request_id,web_subject_ref,session_ref}`，Shell 分支保留原 Bot、命令和会话绑定，工具分支保留 Run/operation 绑定。Bot 批准引用继续使用 `{bot_id,approval_id}`；部署资源批准由部署库权威管理，使用显式 `{authority_scope:{kind:deployment,id},approval_id}` 分支，不能猜测某个 Bot 来存放。Web 账户与平台主体的等价必须来自已验证、可撤销的身份映射；跨入口查看同一批准不等于能代替原主体批准。跨入口消费须是该固定计划允许的渠道、满足原绑定并重验身份映射修订，否则在新入口按当前主体重新形成批准；不能仅凭操作 ID、QQ 昵称或复制一次性码转移权力。

### 2.3 效果、发送与工作队列

| 表 | 主要字段 | 关键约束 |
|---|---|---|
| `kernel.effect/effect_operations` | effect_operation_id、input_digest、effect_kind、target_kind/id、producer generation、root operation ref、input payload ref、state、current_attempt_id、attempt_count、revision | 相同语义效果唯一；不知道 Run/命令/工具/发送业务分支；正文只作 PayloadRef |
| `kernel.effect/effect_attempts` | attempt_id/no、permit_operation_id、wire payload ref、authorization basis、executor generation、fence identity/epoch、deadline、handoff_operation_id、state/timestamps | 最长 30 秒许可；`PREPARED` 交接前可放弃，`DISPATCHING` 结果丢失只能进入 UNKNOWN |
| `kernel.effect/effect_observations` / `attempt_abandonments` | 幂等 operation、attempt、evidence payload ref、observed/resulting state、accepted/conflict；reason、cancel flag | 观察与放弃事实不可变；UNKNOWN 只由显式 reconciliation 解析，相反晚到证据留账但不覆盖 |
| `send_intents` | intent_id、operation_id、send_lane、run_id?、session_id?、conversation_id、response_seq?、command_id?、control_seq?、addressed_trigger_ids、ReplyRoute、contract/schema、content_ref、state、policy_dependencies | CHAT 与 CONTROL 分支 CHECK；分别约束会话 response_seq、命令 control_seq 唯一；意图可恢复，产生代际仅为审计信息 |
| `send_parts` | intent_id、part_index、content_digest、state、platform_message_id、delivery_owner_epoch | 子项唯一；部分成功单独对账 |
| `domain_outbox` | event_id、topic、aggregate_id、aggregate_revision、payload_ref、created_at | 与源事实同事务；事件内容不可变 |
| `subscription_receipts` | subscriber_id、event_id、status、processed_at | 消费幂等；代际切换保留稳定 subscriber_id |
| `jobs` | job_id、job_key、kind、status、next_attempt_at、attempt_count、owner_id、lease_epoch、lease_until、payload_ref | job_key 唯一；过期 lease 的提交失败 |
| `dead_letters` | event/job_id、consumer、error、attempts、replay_audit | 重放复用原逻辑操作 ID |
| `budget_accounts` | bot_id、budget_kind、period_key、limit、reserved、consumed、revision | 不超额约束；短事务预留与结算 |
| `budget_operations` | operation_id、account_id、reserved、settled、status | 重试不重复扣减，释放未用额度幂等 |

`send_lane=CHAT` 使用非空 `response_seq`，遵循会话回复顺序；由 Agent 产生时保留其 Run/Session 关联。`send_lane=CONTROL` 要求 `command_id` 与每命令单调 `control_seq`，`response_seq` 为空，不要求 Run/Session。分支 CHECK 防止一条意图兼占两种序列；唯一键分别为 `(bot_id, conversation_id, response_seq)` 的 CHAT 部分索引，以及 `(bot_id, command_id, control_seq)` 的 CONTROL 部分索引。各意图的拆片仍使用 `(intent_id, part_index)` 幂等。CONTROL 不受 CHAT 的 DISPATCHING/UNKNOWN 队首阻塞，但必须使用相同平台/账号限流、绑定发送租约、出站授权与 UNKNOWN 对账；同一命令的控制回执保持自身顺序。

### 2.4 记忆与派生

事实归约字段以[06 并发合流设计](06-concurrent-convergence.md#32-最小逻辑表)为准，包括 `memory_clock`、`memory_operations`、`memory_assertions`、`memory_relations`、`memory_tombstones`、`suppressed_sources`、`memory_scope_heads`、`memory_projections`、`projection_jobs`、`llm_artifact_slots/candidates` 和 `index_documents`。多主干图及召回契约以[10 多主干记忆图](10-multi-trunk-memory-graph.md)为准。

Episode/Archive 使用 `archive_units(archive_id, source_unit_ids, source_context_ids, payload_ref, digest, visibility, source_revision)`。Archive 是可重新打包的持久载荷容器，稳定图节点身份来自 `source_unit_id`，不能把一次裁剪批次当作新节点或依据完成顺序续接主干。向量与图节点必须引用明确的 source/archive/assertion/projection 版本，不能引用活动 Prompt 缓冲。

| 来源与图记录 | 权威约束 |
|---|---|
| `memory_sources` | 注册 Bot 域内稳定 `source_id` 与 `source_kind`；GROUP 类型的 `group_source_id = source_id`，外部身份按 `(bot_id, platform, realm_id, external_group_id, identity_incarnation)` 唯一，群名只作显示快照 |
| `memory_source_clocks` | `(bot_id, source_id)` 唯一，分配源内 `source_order_seq`；与 Bot 的 `memory_clock.source_revision`、Conversation 的 ingress/Context 游标分别计数 |
| `memory_source_units` | 固定`source_unit_id, source_id, source_identity, source_order_seq, source_event_id/context_unit_id, source_conversation_id?, thread_id?, source_ingress_seq?, author_snapshot, occurred_at, accepted_at, source_revision, visibility`；同源位置及稳定source identity唯一，多观察的会话快捷字段不替代occurrences，保留内容版本与删除依据 |
| `memory_source_occurrences` | occurrence_id、source_unit_id、binding_id、inbox_message_id、source_conversation_id、source_ingress_seq、input_batch_refs、eligibility_state；源与Inbox观察关联唯一，可验证的同一跨Binding事件共用源身份，保留每个会话的独立输入边界 |
| `memory_trunks` | 每个 `(bot_id, group_source_id)` 恰有一条逻辑主干；根节点与主干身份稳定，Binding 重建、会话重开、线程变化或群改名不产生第二条同群主干 |
| `graph_generations` | `bot_id, generation_id, schema_version, reducer_version, build_state, manifest_ref`；候选重建使用独立物理命名空间，稳定来源身份不随 generation 改变 |
| `graph_revisions` | `bot_id, graph_revision, generation_id, source_revision, deletion_epoch, manifest_ref, coverage_ref, status`；不可变可读快照及明确覆盖范围 |
| `graph_nodes / graph_edges` | 同 Bot 规范节点/边 ID、类型和源/断言/工件引用；使用 generation 下的版本可见区间或不可变分片 manifest 固定修订读取 |
| `graph_heads` | `bot_id, active_generation_id, active_graph_revision`；同 Bot 短事务 CAS 是唯一图发布点，指向全部主干与跨主干关系的兼容快照 |

普通平台消息在权威 Inbox 接纳事务中固定来源单元和源内顺序；模型/工具闭合记录在自身事实提交中固定。Inbox 登记只确定身份与位置，群消息必须经过合法 InputBatch/静默输入且源载荷可靠归档后，才能发布为图成员/可检索索引。后台归档、分词、向量化和图投影只复用这些身份及位置，不再分配“最新节点”。`source_order_seq` 表示该群来源账本的确定位置，不代表消息已进入 Agent 的当前输入；同群未过本次模型 InputBatch 切口的排队尾部必须在检索和派生来源传递时继续过滤。

各主干的 `TRUNK_NEXT` 只连接同主干节点，跨主干关联使用显式 `ASSOCIATES_WITH`、`EVIDENCE` 等边，端点必须同 Bot 且保留完整证据来源。图读者固定一个已发布 `graph_revision`，在该修订下读取 root/head、节点和邻接，发布时原子切换完整图 manifest；不能逐条覆盖邻接后再更新 head。驱动可使用 MVCC 或不可变分片引用，不要求每次复制全图。索引数据库物理写完不代表可读，未发布修订只能作为候选，详见 10 的发布与恢复协议。

图候选可以明确冻结事实修订s0并报告滞后；覆盖范围外的新接纳不要求整个候选失效，发布仍检查原head CAS、来源/删除/授权依赖及覆盖不回退。若声明最新完整图，才必须覆盖最新对应scope head。持续写入不应造成永远无法发布；并发主干更新在CAS失败后复核分片并合成兼容manifest，不丢掉已发布分支。FINAL_READY私有候选不得冒充正式ModelTurn源；步骤flush/echo复用原源身份。

同Bot跨Binding观察的规范事件去重以Adapter声明的稳定平台事件键为依据，不能按相同正文猜合并。规范源单元只分配一次source_order_seq，各观察记录独立进入对应InputBatch；查询必须检查当前Conversation的观察关系，即使其他Binding已消费同源事件，也不能把当前仍排队的观察经RAG回流。无法验证平台事件同一性时声明较弱dedup_scope，保留可追踪的独立观察，详见10。

召回的每个可公开依据都携带稳定群来源、群名快照、线程/会话和消息定位；多群共同支持的事实返回全部获准依据。结构化出处是必填返回字段，模型渲染也必须明确注明来源群，不能只放在不展示的 metadata。查询命中、遍历每个端点与最终物化均执行当前 Bot、可见性、删除和输入切口校验；可见关联边不能成为读取隐藏端点或隐藏证据数量的旁路。

`context_compactions` 的 `result_ref` 是确定性的活动视图 manifest，保存预算/分词版本、保留单元、移出范围和来源引用。默认裁剪仅进行有界分块归档、精确来源保留、预算计算和视图替换；没有“记忆摘要”生成，不调用 LLM，也不等待向量化或语义抽取。未回答触发原文及未闭合工具组按 08 的规则 pin；模型可通过受权历史读取工具取得移出内容。显式请求的语义抽取或联想任务单独创建 Job 并受预算控制，不能作为每条消息或裁剪完成的隐含依赖。

### 2.5 存储迁移与账号转移

fact_store_ref 指向该 Bot 的完整事实与协调库，更换时执行独立 durable cutover 操作：

1. 在部署目录记录迁移 operation_id、旧/新 profile 和预期 storage_generation；互斥该 Bot 的其他配置、生命周期和迁移操作。
2. 冻结该 Bot 的全部 admission、平台接入、发送领取、Job、Run、记忆与控制写；排空在途外部调用或将不确定效果入账。协议无回放时明确记录接入维护窗口。
3. 在旧库将 Bot 写状态封闭为 SEALED，所有 CommitGuard 都检查此状态；只允许本次迁移恢复器操作控制标记。封闭后导出一致快照，包括租约、路由、权限、幂等、删除、效果与未完成作业，以及群来源注册/身份时钟、稳定单元、主干关系操作、图发布指针和重建水位，不能只复制聊天文本。
4. 导入新库，校验记录数、摘要、外键、删除日志与来源链；新库保持 NOT_ACTIVE。重新分配宿主/Worker incarnation，提升 storage_generation 和接管相关 epoch，旧进程令牌不能用于新库。
5. 以部署目录的 profile/storage_generation CAS 为切换线性化点，然后新库确认该目录决定并开放 ACTIVE，恢复组件、Job 与接入。旧库保持 SEALED，旧凭据撤销。
6. 崩溃恢复只认 durable cutover 阶段：目录切换前可由原操作所有者恢复旧库；目录切换后只恢复新库，不因连接失败静默退回旧库。切换后的回迁是新操作，必须携带新事实。

这里不使用跨库事务，而用“旧库先封闭、新库后开放”和持久目录决定避免双写。外部在途效果不能由快照复制消除其 UNKNOWN 状态。

整库迁移的 SEALED 阶段同时关闭 `data_admission` 与 `control_admission`，属于普通暂停保持群聊控制可用的例外。此时原 Bot 群聊入口不能承诺继续写入命令或发送回执；进度和恢复由部署管理 API/CLI 或另一个仍在线、获准查询迁移的 Bot 提供，恢复器遵循原迁移操作所有权，不因群聊命令优先级绕过封闭。

账号转移也采用分阶段工作流：冻结旧绑定，在旧 Bot 库提升/撤销 binding epoch，停止旧入站和新发送许可，排空实际连接并处理 UNKNOWN，取消或封存未发送旧意图，关闭旧连接；随后释放部署目录的账号归属，为新 Bot 创建全新 binding_id。旧 ReplyRoute 永远不能解析到新绑定。已获准的外部在途效果保留原归属，不能保证转移撤回了它们，也不能让新 Bot 自动重放。

### 2.6 多模态来源、受管媒体与嵌入

`ContentPart(part_id, part_index, kind, text?, media_ref?)` 是消息、归档、工具结果与召回 Evidence 共用的有序内容契约，首版 `kind=text|image`；`kind` 决定互斥字段，图片使用 `ManagedMediaRef=(bot_id, media_id, media_revision)`。消息身份、群/作者出处及 part 顺序独立于载荷是否已下载，不将整条图文消息提前扁平化为一个字符串。后续扩展模态需声明新的 schema 与能力，不能把未知二进制嵌在 text 中绕过投影。

| 表 / 记录 | 主要字段 | 权威约束 |
|---|---|---|
| `media_assets` / MediaAsset | bot_id、media_id、media_revision、blob_digest?、object_ref?、mime?、byte_size?、width?、height?、state、retention_policy_ref、deletion_epoch、created_at | PENDING/STORED/READY/UNAVAILABLE/REJECTED/TOMBSTONED/PURGED为资产状态；STORED起必须有已验证digest及可恢复对象，PURGED后对象已回收；已物化字节不可变，仅同Bot内容去重，不合并来源和权限 |
| `media_captures` | capture_id、source_part_ref、locator_ref、expected_digest?、media_ref、capture_revision、materialization_ref?、state、attempt_count、lease_epoch、error_code | 捕获状态固定为PENDING_FETCH/FETCHING/STORED/READY/RETRYABLE_FAILURE/UNAVAILABLE/REJECTED/CANCELLED，转换见[11](11-multimodal-memory-and-model-projection.md#4-图片接纳归档和可恢复性)；重试不重分配源顺序，READY不能被异内容覆盖；资产删除不冒充捕获成功/失败 |
| `media_source_links` / MediaSourceLink | link_id、source_unit_id、source_revision、part_id、part_index、media_ref、lineage_ref、visibility、status | 来源修订/part 与媒体修订关联幂等；同一图片在不同群/消息的每次合法来源均保留；关联不放宽任一源的可见性 |
| `media_variants` | variant_id、parent_media_ref、transform_version、transform_params_digest、output_media_ref、lineage_ref | 缩放/裁切/转码产生新不可变修订和完整父来源；原图保留，变体不伪装成新的群发言 |
| `embedding_profiles` / EmbeddingProfile | profile_id、profile_revision、space_id、provider_ref、actual_deployment_id、model_revision、preprocess_revision、chunker_revision、normalization_revision、input_modalities、query_modalities、dimensions、distance_metric、cross_modal_alignment_revision、supports_joint_input、config_digest、batch_limits、budget_ref | 文本和图片均有原生 document/query 编码；声明跨模态对齐空间及预处理，维度相同不表示兼容；LLM 能力不参与该配置 |
| `embedding_artifacts` / EmbeddingArtifact | embedding_id、artifact_slot_id、profile_id/revision、space_id、model_revision、preprocess_revision、chunker_revision、normalization_revision、input_modalities、input_mode、source_manifest_ref、encoded_part_refs、encoded_media_manifest_ref、ordered_parts_digest、embedding_input_digest、vector_ref、vector_digest、admitted_deletion_epoch、source_revision、state、coverage_ref | TEXT/IMAGE/MIXED 明确区分；缓存key含实际物化版本/原图摘要/变换的encoded_media_manifest_digest；图片向量不能以caption/OCR文本向量顶替；同输入同版本复用已冻结工件 |
| `embedding_index_heads` | bot_id、index_scope、active_generation_id、space_id、profile_revision、coverage_ref、revision | 完整空间版本和覆盖范围原子发布；新旧空间分离重建，读取固定已发布 head，不能把同维度向量直接混搜 |

权威接纳先固定 `source_unit_id/source_order_seq` 与有序 part 身份，再异步捕获图片；预先分配的 media revision 可处于待物化状态，READY 后字节不变。原 URL、平台下载令牌仅作受限捕获线索，不是永久记忆。下载失败保留原图片 part、状态与来源；文本部分可先归档/索引，图文整体标记 `PARTIAL` 并报告已覆盖模态，不假报“完整记忆已索引”。图片后来就绪生成可验证的物化/索引事件，提升相关 source/scope 依赖水位，沿用原图节点、part 与源位置。若相同 locator 返回不同字节，先校验预期摘要；确需接纳变更时建立新的明确内容修订及 lineage，不能覆盖原图或让先完成的下载决定原始内容。稳定原图最终不可获得时保留永久缺失状态，不能通过模型描述捏造原图。

`index_documents` 的细字段以[06](06-concurrent-convergence.md#32-最小逻辑表)为准：每个文档绑定来源修订、part refs、`ordered_parts_digest`、嵌入工件与空间/预处理版本。至少支持 text→text、image→image、text→image、image→text；图文混合检索可采用已声明的原生联合编码，或明确的多路查询融合，不随意拼接/平均不同空间的向量。召回仍逐个原始证据物化为 ContentPart，图、向量、缓存均不成为访问权威。来自图片的相似度仅是候选/联想证据，不凭相似性生成没有原文依据的权威描述。

捕获、变体和嵌入均是独立有界 Job，按 Bot 计字节、图片数、像素、并发与费用，支持批处理、复用和修复；流式入 Context 与无模型裁剪不等待全部索引完成。保留策略可将旧完整单元归档后移出 canonical 活动视图，但 `context_view_profile_ref` 固定该视图的预算算法，不从当前 Provider 的能力或请求投影反推删除哪些原图。媒体长期保留/遗忘由明确保留策略及 tombstone 管理，不能因为裁剪窗口已无图片就直接清空 blob。

### 2.7 每次模型请求的能力投影与记录

对话 LLM 的 `ModelCapabilities` 是单独版本化的能力快照，与 EmbeddingProfile 无依赖。Provider/模型组合未声明支持图片时，按文本能力投影：先在固定 InputBatch 切口及当前授权下取得 canonical Context、召回和完整工具结果，再递归去掉所有非文本 ContentPart，最后按目标 API 序列化。不得发送 image 部分、图像字节/base64、可下载的原图 URL 或媒体句柄供文本模型绕过限制，也不自动补发 OCR/caption/视觉模型请求。保留获准的群/作者标签及无图像语义推测的缺失标记；只有图片的消息仍有来源和遗漏事实，不能伪造文本解释。结构化工具输出按 schema 渲染安全文本投影，必要的 tool-call/result 配对保留，不能仅删除顶层 image 字段却把深层二进制序列化成 JSON 字符串。

`ModelRequestManifest` 冻结本次 `included_part_refs` 与 `omitted_part_refs`，后者逐项附 UNSUPPORTED_MODALITY/UNAVAILABLE/REQUEST_BUDGET/POLICY 等原因；不可公开的原来源仅留受限审计引用，不能把敏感来源名称或数量写给模型。manifest 同时保存来源切口、实际模型能力版本、投影版本、语义 `input_digest/projected_content_digest` 与本次预算依据，使 ModelTurn 能追溯模型实际看到什么。其 source manifest 只将实际纳入内容及必要派生依据视为模型输入，omitted 清单另行审计。它既不是 `context_compactions.result_ref`，也不能更新 context_applied_seq、删减 canonical parts 或把未读图片标为已理解。

每次实际调用包括工具后的续调和已获准的 Provider fallback 都执行投影；fallback 固定原 `input_revision/source_cut_digest`，按新能力及预算重新生成 manifest，不重用视觉请求的 payload，不趁重试纳入新群队列。能力快照在本次 dispatch 固定，热重载后续调用可使用新快照；旧在途结果按原请求 manifest 审计，不把新模型能力追写进去。请求预算基于实际投影后的文本、图片数/像素或目标模型成本规则，并预留输出/工具结构开销；超限仅改变本次获准请求视图或进入显式预算处理，不回写 canonical 裁剪计划，不调用摘要模型缩短内容。

模型出站按“事务外 prepare → 最终短事务许可 → 实际发送”执行：Provider Gateway 按冻结语义请求及当前模型能力、Bot/来源授权获取字节或短效签名 URL，构建精确 API body；随后在最终事务重验来源、删除、Session/Run及预算，冻结 `ModelTransportAttempt.payload_digest`、serializer版本和短效permit，提交后才将该精确body交给目标API。body在prepare时尚未发给模型，失权、能力不符或permit过期须丢弃/重新准备，不能以缓存payload为授权。请求在许可后已有在途外部效果时，仍遵循撤权/遗忘的既有线性化边界。

语义 `projected_content_digest` 与真正 wire `payload_digest` 分开；prepare 之前不能声称已经知道包含签名 URL 的实际请求hash。短效URL不放普通日志、canonical记忆或长期manifest；签名刷新得到新 `transport_attempt_id/payload_digest`，可复用未变的语义 request manifest，不能改写旧attempt。Provider fallback改变实际模型/能力时则创建新 `request_id` 和传输attempt。派发状态、provider receipt、usage和UNKNOWN沿用RunStep/checkpoint的调用执行状态，并保留按attempt关联的历史；刷新URL或更换模块不能成为对UNKNOWN请求盲目重发的理由。对象引用及请求载荷的受限临时保留与清理都纳入同Bot删除闭包。

同一 logical_step 只允许一个当前有效 `model_request_id/transport_attempt_id/model_call_epoch`。接替调用必须先确定旧attempt终态，或按明确策略放弃旧结果并持久化其fence失效；随后CAS提升call epoch并绑定新attempt。最终模型结果提交核验该三元组及Run/Session fence，新旧至多一个生成ModelTurn或工具步骤。迟到旧响应只追加对账/费用记录，不能覆盖新调用结果；取消本地等待不代表外部请求已撤回，原UNKNOWN费用状态继续保留。

## 3. 事务与锁顺序

### 3.1 统一提交守卫

所有可写 repository 入口使用 `CommitGuard`，包含：受信 bot、调用者/执行器所需的 route fences、会话session/actor及job/effect owner epoch、policy revision、deletion epoch（适用时）、预期业务 revision、operation_id 与 input_digest。模型/工具步骤提交还需固定的 input_revision 和当前Session身份。

不是每个操作都检查同一组 epoch：记忆后台任务不需要某个群 Actor lease；会话轮次必须检查自己的 Actor；Delivery 执行旧代已提交意图使用当前 Delivery/绑定 lease，不要求产生意图的旧 Agent 继续在线。

Shell 控制执行使用当前命令主体、目标授权、资源 revision、实际执行器 fence 与关联操作所有权；不为获得控制资格伪造 Actor/Run。涉及暂停、恢复及业务准入的写入还需校验最新 `maintenance_epoch`，旧 resume 或旧重载收尾不能重新开放已被后续维护意图关闭的业务准入。

统一锁顺序为：生命周期资源所有权（若适用）→ 相关路由 fence（按组件/服务排序）→ Bot policy/storage 状态 → Conversation/Session/Run/Job 所有者行（按表序与规范键排序）→ 输入批次/触发领取记录 → memory_clock → memory_source_clocks（按 source_id）→ memory scope heads（按 scope_id）→ 投影/图发布/效果/预算等业务行（按表序与主键）→ 去重/事实/outbox 插入。无需某层的事务可跳过，不能反向获取；实际 Repository 必须共享锁序枚举。源注册与主干创建纳入来源业务的固定表序；涉及源分配的 Inbox/Context 事务必须先锁所需 Conversation，后台图投影不得持源或图锁反向获取 Conversation。

最终 fence 读取采用会阻止对应 epoch 更新的行锁，持有到事务结束。PostgreSQL 的 `FOR SHARE` 会阻止行更新，`FOR KEY SHARE` 不阻止某些非键更新，所以不能用后者保护普通 epoch 字段。锁模式及冲突关系依据[PostgreSQL 官方行锁文档](https://www.postgresql.org/docs/18/explicit-locking.html#LOCKING-ROWS)。

不持数据库事务等待模型、用户确认、HTTP、平台消息、文件渲染或其他模块 RPC。冲突与死锁重试整个短事务，沿用 operation_id；不得用更改 payload 的方式伪装同一次重试。

命令、批准与 `config_heads` 行归入上述“投影/效果/预算等业务行”层，仍按共享锁序枚举及主键排序；不在全局锁序前面另加一把 command/config 锁。入口接纳短事务先完成请求/批准事实，提交后再执行目标操作；禁止持有 `command_requests`、`management_requests` 或草稿行锁等待生命周期资源、目标 Bot RPC、Agent 安全点或用户确认。服务等级 P0 只给予提交取消/维护意图及状态检查的保留容量，不能抢走在途 `lifecycle_resource_leases`；生命周期资源持有者完成可取消阶段或交给合法恢复器后，新的相交操作才可取得所有权。

### 3.2 核心事务边界

| 操作 | 同一事务内必须完成 |
|---|---|
| 接收平台消息 | 验证绑定/接收身份、平台去重、分配 ingress_seq、固定 source_unit_id/source_order_seq 与真实群来源、保存逐条作者/Inbox及有界ShellGate路由结果、推进mailbox_revision、登记相应分类/控制唤醒工作；命中CONTROL时同事务创建RECEIVED请求 |
| 完成消息分类 | 保存上下文处置与触发决策/trigger记录、推进连续classified_through_seq、mailbox_revision与持久唤醒 |
| ShellGate 接纳命令 | 与原Inbox接收同事务幂等确定CONTROL/CONTROL_COMMAND；只冻结来源/raw_ref/ReplyRoute/注册表快照，保存RECEIVED command_requests与有界解析唤醒；不要求前序普通消息分类完毕 |
| 完成Shell解析/授权 | 完整解析在事务外；PARSED短事务冻结command_key/version/schema、argv_digest/input_digest及显式目标；经当前授权确认才转AUTHORIZED并入P0/P1执行调度 |
| 确认 Shell/工具计划 | 校验 approval_requests 的精确对象/参数/目标/主体/版本/期限与 token_digest，原子记录 decision/consumed_at 及持久执行唤醒；无外部等待 |
| 更新维护意图 | 校验目标权限与预期 maintenance_epoch，提升 epoch、修改 data/control admission 的允许字段、登记取消/维护动作及 outbox；不转移生命周期资源所有权 |
| 领取前台 Run | 检查 admission、会话唯一Session、Actor/Run lease、holds_actor唯一性、提升owner_epoch；不得仅因旧Run等待就另开Session |
| 提交输入批次 | 固定切口/原游标CAS、先flush前序闭合步骤、复用源单元追加所有来源消息/占位、trigger claim、推进context_applied_seq和input_revision、确定性裁剪/归档Job及outbox；无模型摘要 |
| 最终回复提交 | CommitGuard、Session/input/Context CAS、完整ModelTurn及其稳定源单元/群顺序、实际覆盖trigger终态、Run终态、CHAT response_seq与发送意图、确定性归档/图投影Job、下一调度决策 |
| 提交控制回执 | 当前来源群输出可见性与固定 ReplyRoute、command_id/单调 control_seq、CONTROL 发送意图及 outbox；不修改 Agent 触发完成状态 |
| 静默排空/结束 | 复核无活跃模型、无待触发前缀，选择有限尾部批次并追加Context/裁剪Job；最终锁定会话复核mailbox/分类/输入水位后才关闭Session，入站/分类晚到必须保留唤醒 |
| 记忆操作接纳 | 权限/删除屏障、幂等、不可变操作与关系、memory_clock、scope 脏标记、outbox |
| 发布派生产物 | 当前源集/版本/屏障复核、适用时 slot 选择、projection CAS、索引 Job 与 outbox |
| 发布图修订 | 固定源单元/端点/证据 manifest、当前权限和删除屏障、预期图修订及源头复核、登记 graph_revisions 并 CAS graph_heads 至完整 root/head/边快照、processed 水位与 outbox |
| 路由切换 | operation owner、旧 epoch、排空证据、state pointer、路由新 epoch 与生命周期日志 |
| 授权撤回 | policy revision/授予记录变化、待执行效果失效标记或重新验证标记、outbox |
| 遗忘 | deletion_epoch、tombstone/来源 suppression、相关/全域重验证任务、outbox |
| 图片物化接纳 | Job/route fence、固定 source part/capture 版本、媒体摘要校验、不可变 media revision、来源关联及 lineage、受影响 scope 水位、索引 Job/outbox；不重分配源位置 |
| 发布多模态索引 | 已冻结 EmbeddingProfile/space 与 ordered_parts_digest、当前源/part/materialization 修订和删除/权限验证、index document/coverage 记录及 head CAS；不等待外部编码调用 |
| 模型请求许可 | Gateway先在事务外合法prepare精确body；短事务校验Session/Run/input、来源切口、能力/投影版本、实际included/omitted语义manifest、预算与当前授权/删除，冻结transport attempt的wire hash/permit并关联RunStep；提交后发送同一body |
| 配置接纳 | 资源权威库内重验 Web/Shell 主体、当前 policy/fences、schema/plan 摘要、expected_revisions 和源请求幂等；创建不可变 ConfigRevision、CAS desired head、需要时创建既有 lifecycle/job 关联、审计与 outbox；不等待浏览器或外部验证 |
| 配置生效 | 资源权威库内校验原计划和当前 desired/maintenance/storage 版本；dynamic 发布可供后续消费者获取的完整快照，generation 将 effective head 与 route/state 一起 CAS，restart 在受控重启的已验证切换点发布；失败不改旧 effective。首发前 schema 变化直接重建开发库，不存在 migration 生效策略 |

最终轮次默认仅入列确定性归档/图投影工作，不自动创建 LLM 抽取、摘要或记忆整合调用。业务显式请求的语义任务才独立入列并预留预算；若要求同时接纳确定性记忆事实，可由同 Bot 的事务协调入口在本地 repository 内完成，不能跨 RPC 开启所谓联合数据库事务。

群输入提交独立于最终回复提交，保证无触发、取消或失败都不会丢掉已接纳的历史。大前缀可在短事务中追加不可变 Inbox 范围引用、逐条来源 manifest 与 retention pin，逻辑上完整覆盖该段后推进游标；随后有界分块物化和裁剪，模型调用必须等合法视图准备完成。范围引用不能遗漏作者，不能在归档/物化前释放原载荷，也不能临时缩短到只含触发消息。静默排空不创建回复意图，不把任何 pending trigger 自动标成 answered。

ShellGate 属于稳定核心入口；只做有界、确定性的直接命令前缀/入口识别，将路由结果与Inbox一并持久化，不等插件分类或 Agent 排队。复杂参数/目标解析和权限判断在后续有界阶段完成；被核心命令识别的请求即使未知、无权限或参数错误，也维持 CONTROL 终结路由，不回落模型。命令自身的分类结果可先落库，但 `classified_through_seq` 与 Context 仍只推进连续 `ingress_seq`，不得因为控制执行已完成跳过前面的群消息。相关契约见[09](09-group-shell-command-system.md)。

### 3.3 跨 Bot 命令协调

跨 Bot 请求使用“来源库持久请求 → 目标库幂等子操作 → 来源库 CONTROL outbox”的工作流。来源 Bot 库冻结真实发起者、原会话/ReplyRoute、显式目标和请求摘要；调用目标前不持来源事务或请求锁。目标 Bot 在自己的当前授权与资源 fence 下，以由来源 command_id 和目标范围稳定派生的 operation ID 接纳原有类型的子操作，返回 `operation_ref={kind,id}`。来源请求记录该引用并持续查询/恢复，重复 RPC 不能重复重载或执行外部效果。

目标需要批准时，在目标库创建 `approval_requests` 并返回 `approval_ref`。来源收到的 `confirm/deny` 是新的受权、可幂等控制请求，携带可信当前发送者与原绑定/会话事实进入目标批准事务；目标独立解析该主体在目标 Bot 的权限，校验批准绑定的 origin/group/plan/期限和令牌，再单次写入决定。不能把来源 Bot 签发的本地能力句柄直接当目标授权。目标执行许可与对应 approval/policy/resource 行在同一目标库事务内检查，不持目标事务调用来源库“再查一次”；各 Bot 的授权撤销分别以自己的权威提交生效。

目标已提交而来源尚未记录回执时，重启后查询同一目标操作恢复结果，不能把超时推断为目标未执行。目标不可达返回可查询的等待/失败状态；多目标逐项保存结果，不宣称跨库原子成功、回滚或同时撤权。最终回群在来源 Bot 的当前策略下独立检查目标结果的来源可见性，必要时仅发送阶段和受限结果引用；目标读取权限不自动包含向来源群公开的权限。

### 3.4 lease 领取与续期

Worker 在短事务中使用 `FOR UPDATE SKIP LOCKED` 选择到期任务，递增 lease_epoch 并保存 owner_id、lease_until，返回工作 DTO；事务完成后才执行 I/O。owner_id 不是 fencing，必须同时校验 lease_epoch 与有效期。

lease_until 由数据库时间计算；续期失败或节点失联时，Worker 停止新副作用与提交，已有外部请求进入对账流程。接管者只能在租约失效后提升 epoch。仅使用内存 asyncio.Lock 不能保证进程重启/多 Worker 的单所有者。

### 3.5 Web 请求与目标操作的恢复

HTTP 接入先在部署库固定 `management_request_id`、可信 Web 主体、目标、规范参数摘要、预期版本及幂等键，再通过相同管理服务向目标提交。目标操作标识由该请求与目标范围稳定派生，目标在自己的事务内接纳配置修订或既有 lifecycle/job/effect。部署入口和目标库间不持跨库锁、不承诺原子完成；目标成功但 HTTP/部署回执丢失时，以原幂等键和原 `operation_ref` 恢复。相同键改参数返回 `IDEMPOTENCY_CONFLICT`；新一次有意修改才使用新键。

配置竞争返回 `REVISION_CONFLICT`，HTTP 表达为 409，附获准读取的当前版本和差异依据；保留浏览器草稿，由用户显式重基后重新 preview/apply。不得自动修改 expected_revision 后静默覆盖。多 Bot/多资源批量计划逐项显示接纳、生效、失败、待恢复状态，不能把部署父请求的成功当全部目标生效。目标存储处于迁移 SEALED 等拒写阶段时返回其真实维护状态，不向目录缓存写入“成功配置”。

## 4. 鉴权模型

### 4.1 主体、委派与能力

主体类型包括部署管理员、Bot 管理员、平台用户、Bot 服务任务、模块实例、Agent Run 与 Tool Call。用户权限与运行代码权限分别存在，最终效果使用二者交集。

```text
Allowed = PackageRequested
        ∩ DeploymentGranted
        ∩ BotPolicy
        ∩ UserOrJobAuthority
        ∩ DelegationRestrictions
        ∩ CurrentResourceState
```

能力例：`message.read`、`message.send`、`memory.recall`、`memory.assert`、`memory.forget`、`media.read`、`media.capture`、`tool.invoke`、`job.schedule`、`module.reload`、`bot.manage`、`host.exec`。每项附 Bot、目标频道、数据范围、动作类型、有效期、预算与可委派限制。权限路径只表达资源结构，不能仅凭路径前缀推出允许。图片查询输入本身、召回图片、变体及跨群可见性分别校验，获准计算向量不自动意味着获准向模型服务或当前群公开原图。

群聊 InputBatch 中每条消息与 trigger 保留独立主体。Context 可读性以 Bot 的会话访问权限和来源策略为准；副作用授权必须绑定具体 trigger/request 主体。不得把同批成员权限取并集、继承最后一个管理员发言者的权限，或将引用/转发原作者视为当前授权者。最终群输出可见性也不能由某个触发用户的私人读取权替代。

平台群主/管理员身份只映射部署明确授予的群作用域动作，不自动取得 Bot 运维、能力授予、跨 Bot 管理或宿主执行权限。Shell 请求来自可信 sender 的当前直接命令；引用/转发、普通正文、模型工具提议均不能成为 shell 控制主体。`host.exec` 是独立能力，类型化管理命令无须获得它；启用时由专用受限执行器约束可执行程序/参数、规范工作目录、环境与 secret 引用、网络、时间和输出量，不将原始群消息直接交给宿主 shell。

`shell_sessions` 仅缓存显示与补全偏好。无 `--bot` 的命令绑定接收 Bot；跨 Bot 操作仍须在每条命令显式指定目标，并分别校验来源接入、目标资源操作与回群结果可见性，不能凭上一次会话默认目标或缓存权限扩大范围。

有效能力由宿主发放短寿命不透明 handle，绑定 Worker incarnation、Generation、Run、主体、policy revision 和目标范围。插件不能构造同名字符串提权；Kernel 嵌套组件调用只能继承或衰减根工具能力，不能扩大权限。嵌套调用不是另一个 Agent。

### 4.2 校验位置

1. 平台接入：认证连接、确认绑定和 sender 标识，拒绝 payload 假冒 bot_id。
2. Router：检查发起者、目标服务和能力，快速拒绝不合法请求。
3. 业务存储事务：重验权限、所有权、revision、遗忘状态，形成持久决定。
4. 外部执行许可：准备 DISPATCHING 时重验当前权限、参数摘要和目标；已经许可/在途的请求按回执或 UNKNOWN 处理。
5. 检索物化与最终输出：复核来源依赖及可见性，避免索引与缓存滞后泄露内容。
6. 媒体物化与模型 egress：以具体来源 part 验证引用、query-image 的读取/委派和出域策略，递归执行能力投影后才由 Gateway 获取字节或签 URL prepare请求；最终permit事务再验授权/删除和wire摘要，提交后发送，blob digest 命中不代替来源权限。

撤权不是从缓存中删一个 token。Bot 授权权威在该 Bot 事实库；部署级批量撤权需要逐 Bot 提交并返回完成/部分失败结果，不能在控制库写一行就宣称所有独立库立即完成。首版单节点可同时暂停 admission 加速生效，但持久权限仍需落库。

### 4.3 Web 认证、短效操作票据与撤销边界

Web 管理主体来自服务端验证的本地账户或显式接入的身份提供方，不从请求参数中的 user/role/bot_id 接受自报权限。浏览器使用 HTTPS 下的 HttpOnly/Secure 会话 cookie；写接口执行 CSRF token 与 Origin 检查，WebSocket/SSE 连接也验证会话与来源，跨域策略仅接受配置的管理站点。会话、登录尝试、长连接和查询均有独立额度，未认证流量不能耗尽控制执行池。具体登录、页面与 API 契约见[12](12-webui-administration.md)。

部署库 Web 会话与独立 Bot 的授权不具有跨库原子性。首版采用用途限定的 `WebAuthorityTicket`：可信管理网关在部署库验证未撤销会话后签发，绑定 `ticket_id, issuer/key_revision, web_subject_ref, session_ref/session_revision, target_scope, target_operation_ref, action_key, input_digest, plan_digest, issued_at, expires_at`。票据只在后端传递，初始最大有效期 30 秒；目标在自己的最终许可事务中验证签名、期限、精确目标/输入/计划和当前 Bot policy/fences，再把票据依据记入原操作或配置修订。`target_operation_ref` 预分配且由请求稳定派生，同票重放只能恢复原操作，不得创建新效果；票据不能替代 Bot 授权，也不让普通 Worker 持有长效 Web 登录权限。

目标库不能先单独插入一条“验票成功”记录再调用业务 repository。每个管理写 authority 必须提供接收调用方连接的 `*_in(conn)` 入口，由目标 UoW 在同一事务内依次锁定当前 policy revision、当前 RuntimeGraph/Generation fence 和业务资源，验证票据，写入 `web_authority_receipt`，再提交配置修订、取消意图、Binding 变更或持久 Job 接纳。收据至少保存 ticket digest（不保存原始签名）、主体与 Session revision、精确 scope/component/Generation、policy revision、operation/action、输入/plan 摘要和接纳时间；业务事实保存该收据引用。任一参与者失败时收据与业务写入一起回滚，因此“有收据”准确表示目标事务已经接纳，而不是网关曾经检查过。

`target_operation_ref` 是业务幂等身份，`ticket_id` 是一次短票尝试。票据过期后允许为同一 operation 签发新 ticket attempt，但目标事务只允许它恢复或完成摘要完全相同的操作；不同主体、目标、action、input 或 plan 使用相同 operation 时返回冲突。并发 ticket attempts 由 operation advisory lock 与业务唯一约束收敛，只能有一个权威业务结果。读取类请求不预写接纳收据，但每次分页、流式增量和媒体物化都重新验签并执行当前可见性查询。

HTTP 返回丢失不改变目标事实：客户端以 `target_operation_ref` 查询原结果。若目标事务尚未提交，网关超时只能报告 `UNKNOWN`，不能写“成功”审计，也不能换 operation 重试；若业务已提交，查询恢复同一收据和结果。部署库可以另存请求尝试与观察结果用于关联，但它不是 Bot 操作是否生效的权威，且不得通过部署侧状态覆盖目标库结果。

退出、锁定账户或撤销会话以部署库提交为边界，立即停止签发新票据和建立新订阅。已签发但尚未提交的票据存在上述至多 30 秒许可窗口，界面和审计如实记录；尚未取得目标许可的排队请求在到期后取得新票据，失效会话不能续票。目标已持久许可并接纳的操作按原固定委派、当前 Bot policy 和生命周期/效果协议继续或恢复，不要求浏览器持续在线，也不能沿旧委派扩大计划；后续副作用仍重验当前目标权限。已进入派发的请求继续沿用原账本的撤销与 UNKNOWN 边界，关闭网页或退出登录不等于取消它。需要停止目标 Bot 权限时，管理服务逐 Bot 提交当前主体授权撤销/暂停屏障，并逐项报告完成；目标事务提交之后旧票据也因当前 policy 失效而不能通过。不把全局退出登录宣称为所有独立库的同时撤权。

只读列表、事件订阅、图片下载和导出同样绑定主体/目标与当前可见性；每次翻页、重连、增量发送前验证会话，目标读取按当前 Bot policy 过滤。旧游标、签名地址或客户端缓存不获得新的读取权。已发出的受限短效地址有显式有效期和在途边界；受管代理下载可逐请求校验。身份映射修订属于目标授权依赖，撤销后不能靠历史 Web↔QQ 映射继续批准或扩大数据范围。

## 5. Python 插件与信任边界

主开发方式是[原生 Python 插件](07-python-plugin-development.md)，不要求 Wasm、Rust 或自定义 ABI。普通插件可用异步函数/类和 SDK 调用能力；复杂模块按相同底层协议声明服务和状态迁移。

默认 Worker 进程负责完整依赖代际隔离和故障回收。可信 Python 代码仍可能直接使用操作系统 API，因此“只经网关产生副作用”在此模式下是 SDK、代码审查和部署信任契约，不能当成对恶意代码的强制沙箱保证。

协议 Adapter、Provider、存储网关、Secret Resolver 是持必要特权的基础设施模块，需逐项审计其网络、文件与凭据边界。第三方不可信代码只有在 OS 沙箱/Wasm Driver 的限制真实可验证时才允许运行；不支持隔离的环境返回明确错误。

以下限制在正常 SDK 路径强制执行：文件使用受限 namespace 与规范路径；HTTP 工具限定目标和响应大小；浏览器/渲染进程有超时与资源额度；模型输出不得调用任意控制面命令；日志不写密钥、令牌、完整私聊或隐藏推理内容。

## 6. 配置与管理接口

**WebUI 是首版必需的后台管理入口。**日常创建和编辑 Bot、绑定账号、配置模型/原生图文嵌入、记忆存储、模块、触发与调度策略、权限、成本额度，以及观察/取消操作，均通过表单和管理 API 完成，不要求管理员手改配置文件。CLI 与群聊 Shell 提供同一控制能力的其他入口；部署启动所必需的数据库定位由独立初始化页面收集，主密钥来源由安装/启动引导管理，不依赖尚未启动的 Bot、插件或 Agent。初始化页面、配置流程和操作中心详见[12 WebUI](12-webui-administration.md)。

配置按 deployment → Bot → component → Run 的明示覆盖顺序合成不可变快照。Bot 物化快照固定所有父配置修订引用，读取时不能把部署新默认值静默合入旧快照。更新部署默认/共享模板时，选择目标并以各 Bot 受权、幂等的子计划推进其 desired/effective，展示覆盖字段、待同步、冲突与部分成功；部署库提交不原子修改所有 Bot。安全 scope 只能收窄，不允许普通配置覆盖扩大能力。当前阶段字段只声明 `reload_policy = dynamic / generation / restart`：

- dynamic：经校验后生成新快照，后续 Run 使用；在途安全策略仍实时重验。
- generation：新建候选并走模块替换，例如 Provider 客户端设置或插件代码。
- restart：内核/Driver 基础参数，执行受控宿主重启。

首发前不提供 `migration` 策略。存储归属或 schema 变化直接修改内容寻址 baseline、删除旧实现并重建开发数据库；不得从配置保存路径生成数据搬运、双写或兼容读取。

### 6.1 配置权威、接纳与生效

`ConfigRevision` 位于对应资源的权威库：部署/宿主资源在部署库，Bot 及其组件资源在目标 Bot 事实库。配置文件导入、导出和运行目录文件是有 schema/摘要的工件或已提交修订的投影，不能成为第二个动态配置权威；文件 watcher 主要监视插件源代码，不能用无版本文件覆盖 WebUI 已保存配置。密钥内容不进入普通导出。

WebUI 编辑器展示配置 schema、默认值/继承来源、当前 desired/effective、字段生效方式和校验错误。preview 在事务外完成类型校验、依赖检查、影响/成本预估并生成固定 `plan_digest`，不改变运行态；实际 apply 必须携带 `expected_revisions`、原计划摘要、同一次请求的幂等键。远端连通性探测单独作为可取消的现有 Job，返回观测时间，不能把一次探测成功永久等同生效保证；涉及付费 API 的探测先明示模型、预计调用和预算。

配置接纳在短事务中记录不可变修订和 desired head；结果同时显示“已保存、等待生效/生效失败/已生效”，不能只返回绿色成功。`dynamic` 发布完整快照后供新 Run 使用，现有 Run 保持其固定快照；明确支持按模型安全点更新的字段也必须在合法边界形成新版本记录。授权撤销、预算/资源限制和维护暂停依其最终 guard 即时校验，不等待现有 Run 自然结束。`generation` 只有在同目标库的路由/状态 CAS 与候选健康检查通过时才同时发布 effective；`restart` 在受控重启的验证切换点发布。失败保留旧 effective 和可诊断 operation，不能把保存 desired 冒充真实生效。

同组件一次修订含不同生效方式时，按足够严格且可执行的整体计划发布完整快照，例如同时改 dynamic 与 generation 字段时都随新代切换，避免拼接半新半旧客户端。不同资源无法联合切换时，计划显式分步并展示每项 effective；跨 Bot 不承诺 all-or-nothing。新 desired 取代未生效候选时必须使旧候选提交 CAS 失效，不能在晚到完成时覆盖新选择。回滚是以当前 head 为 expected revision、引用旧值创建**新修订**并走相同生效流程；迁移等不可直接逆转状态须展示其恢复计划，不倒退 revision 或伪造已撤回外部效果。

### 6.2 多入口管理与操作跟踪

管理命令示意：

```text
forest module validate ./plugins/weather
forest module reload weather --bot alice
forest module status --bot alice
forest bot pause alice
forest run inspect <run_id>
forest effect reconcile <operation_id>
forest memory rebuild --bot alice --scope <scope_id>
```

以上是拟定 CLI，不是目前可执行命令。WebUI、CLI、Shell 都提交同一类型化控制命令，返回入口 request/command ID、`operation_ref` 和可查询阶段；读取与展示也执行当前目标授权。热重载前展示解析后的影响集合、状态版本与资源预留；普通可逆保存和开发重载按既有授权/策略直接执行，少量高影响操作才复用 `approval_requests`，不能把每次编辑或文件保存变成手工批准流程。

群聊 Shell 通过稳定 `ShellGate` 提交相同控制命令，语法、匹配顺序和忙时处理以[09 群聊 Shell](09-group-shell-command-system.md)为准。群聊入口先返回 `command_id`，目标操作创建后关联可查询的 `operation_ref`；命令扩展在不可变注册表快照中发布，不允许插件 `priority` 覆盖内建前缀。命令状态/进度通过 CONTROL 回执发送，后台工作交给现有持久 Job/生命周期/效果执行器并保留发起者、来源、目标和代际。

正常 `bot pause` 关闭 `data_admission`，保留 `control_admission`、群聊命令接入与控制回执；是否结束当前 Run 是显式参数/维护计划，不能靠暂停隐式丢弃触发。重载仅冻结受影响路由，状态查询、取消、批准与核心救援入口不受业务插件队列或 Agent 会话锁阻塞。`resume` 使用最新维护期望和 `maintenance_epoch` CAS；较旧的重载收尾不得顺手恢复新暂停关闭的准入。主动停止 Adapter/宿主或整库迁移会影响原群控制通道，计划中必须标明影响和现有恢复入口。

服务等级 P0–P4 由宿主注册表确定，数值越小越优先；普通插件不能自行提升为救援等级。控制准入、轻量命令执行和回执保留有限容量，按主体/Bot 限额，避免业务队列占满或命令洪峰无限占用资源。P0 只做有界控制意图/状态操作，耗时重载、导出和重建移交受监督后台操作；优先级不绕过授权、事务锁序或平台限制。

全群消息保存规则同样适用于 shell：保留原 ingress_seq、真实作者及命令/操作来源，允许公开的正文进入 Context，敏感参数按字段 schema 生成安全投影。`argv_ref`、详细输出和原始载荷分配受限读取策略，不把令牌、密码、配置 secret 或完整诊断日志送给模型、摘要、向量索引或群聊回执。仅 `Consumed/CONTROL_COMMAND` 本身不能成为删除历史的理由；不可读字段保留适当的来源占位。命令日志默认仅记录安全摘要和引用，异常堆栈/大输出返回受限结果引用并单独鉴权。

凭据通过 Secret Resolver 按精确用途交给特权边界，轮换产生新版本并触发相关客户端重建。管理界面不通过任意 WebSocket 代理或直接暴露 Bot 对象实现控制。

### 6.3 密钥设置与首次初始化

管理表单中的凭据为 write-only：保存时由受信任后端写入 Secret Resolver，普通配置仅保存版本化 `secret_ref`，再次读取只显示“已设置”、用途、版本和轮换状态。空白保留、替换和撤销是不同动作，不能用星号占位覆盖原密钥。请求日志、错误响应、diff、审计、配置导出与事件流不回显密钥；长期凭据不放浏览器 localStorage，也不进入群消息或 Agent Context。轮换创建新版本及受影响资源计划，切换失败保留可用旧客户端；旧版本仅在引用/在途依赖清理后按策略撤销。

尚未配置部署数据库时，由安装器/独立 bootstrap 底座自动维护受限本地初始化状态和主密钥来源，启动仅提供初始化能力的页面；管理员在浏览器填写并测试数据库连接，不先手改管理模块配置。通过受限本机通道一次性展示有期限的初始化凭据，手工粘贴到 HTTPS 表单；凭据不放 URL 查询参数、浏览器可分享链接或普通日志，服务端仅保存摘要。数据库就绪后，以稳定 setup ID 幂等迁移初始化意图至部署库；首管理员创建、`web_setup_state` 完成和令牌消费同部署库事务 CAS，失败重试查询同一结果，不能创建第二个管理员或重开已完成的本地窗口。多人同时初始化仅一方成功。完成后关闭注册窗口；恢复管理能力走明确的本机运维恢复流程，不公开永久后门。WebUI、管理网关和认证恢复不依赖业务插件、群 Adapter 或 LLM 正常运行。

初始化跨本地状态与部署库仍不承诺联合事务。本地 bootstrap 使用安装器创建的受限 SQLite 状态库等具备进程间原子持久事务的机制，在首次目标写入前 CAS 冻结 deployment/setup ID、唯一目标存储标识、计划摘要及 owner lease/fence，不能仅依赖内存锁或两个独立 JSON 文件。旧 owner 不能修改本地决定，目标写入沿用同一固定 ID/计划幂等；超时/UNKNOWN 必须对账原目标，不得换一个数据库重新注册并制造两个首管理员权威。首版一旦冻结目标，向导只能恢复该固定计划，不提供解除后重选另一个库的快捷路径；一次查询未发现记录不能证明旧在途请求不会稍后提交。不能确认时保留待恢复状态，已初始化后的存储变更走独立迁移；完成后封闭本地初始化写入口。

## 7. 观测与故障响应

所有调用、Run、Job、操作与重载关联 trace/operation ID。结构化日志记录 bot_id、conversation_id 的受控标识、组件代际、状态、耗时和错误码；默认不记录内容正文。

| 指标 | 诊断意义 |
|---|---|
| 入站/出站延迟、队列长度与字节、最老任务年龄 | 背压和真实积压 |
| Bot/Conversation 调度等待、Provider 槽占用 | 公平性、热点 Bot 和慢模型 |
| 活跃 Worker/task/句柄/订阅与回收耗时 | 重载后资源泄漏 |
| reload 各阶段耗时、失败点、旧代迟到拒绝 | 热切换完整性 |
| Context CAS 冲突、STALE_EPOCH、幂等冲突 | 所有权或重复处理异常 |
| projection desired/processed watermark、重建延迟 | 记忆索引新鲜度 |
| conflict 数、旧派生产物发布拒绝、删除重验证积压 | 合流与遗忘正确性 |
| 图发布修订/源水位差、跨主干边拒绝、缺失召回出处数 | 检测图新鲜度、来源混淆和越权遍历；group/source 明细放追踪，不作无界指标标签 |
| 裁剪耗时/移出字节/来源 pin、裁剪路径 LLM 调用计数 | 高流量视图处理保持有界，LLM 调用计数必须为零 |
| 媒体捕获失败/缺失率、待物化年龄、字节/像素额度、变体与 blob 清理积压 | 区分已接纳来源与未就绪图片，发现原 URL 失效和保留策略问题 |
| 各模态嵌入覆盖/费用、索引空间版本、image query 失败及修复积压 | 文本成功不能掩盖图片编码缺失；同维度异空间拒绝和费用单独可见 |
| 每请求 included/omitted parts 计数、投影版本、能力不符拒绝数 | 发现 text-only API 泄漏图片或 fallback 复用旧 payload；来源明细仅记受限 manifest |
| UNKNOWN 效果数与最老年龄、死信数 | 外部不确定操作和恢复债务 |
| Bot 预算预留/消耗、限流拒绝 | 自主 Agent 成本与失控检测 |
| 命令准入/首回执延迟、按服务等级排队年龄、等待批准/目标操作数 | 群聊控制在业务忙碌时是否仍可用；明细关联 command_id 与 operation_ref |
| data/control admission、maintenance_epoch、旧恢复决定拒绝数 | 区分业务暂停、控制不可用和重载/恢复竞态 |
| Web 请求接纳/409冲突、desired/effective 差距与滞留年龄、订阅丢帧/重连水位 | 定位配置已保存但未生效、并发覆盖尝试和页面投影滞后；主体/配置明细只放受限审计 |
| Web 会话/票据拒绝、初始化竞争、跨 Bot 撤权进度 | 区分登录状态、目标权限和在途许可边界，不把 UI 隐藏按钮当授权保障 |

指标标签避免无界 run_id/message_id；明细放日志与追踪。健康分为宿主 alive、组件 ready、绑定 connected、存储 writable 和索引 freshness；不能用一个 `/health=ok` 掩盖功能故障。

## 8. 初始容量与验证目标

首轮基准固定两个 Bot、每 Bot 至少 20 个会话、至少一个慢 Provider 和一个慢工具。把平台、模型、网络耗时与框架调度耗时分别测量。

设计目标：框架本地接入/派发路径在无外部 I/O 堵塞时 p95 小于 100 ms；已提交普通记忆在无故障负载下 30 秒内进入派生索引；长期 Worker/句柄数在 100 次重载后回到配置基线附近且无持续增长。它们是验收目标，实际数据和硬件基线需随实现记录。

正确性为硬门槛：零串 Bot 存储访问、零错误群路由、零旧代非法提交、已持久接收项目全部可追踪。外部平台不确定消息必须显示 UNKNOWN，不能为追求成功率指标将其记作成功。

## 9. 验收矩阵

| 编号 | 场景 | 必须观察到的事实 |
|---|---|---|
| A-01 | 两 Bot 使用相同群号/用户号并发 | Context、记忆、缓存、媒体、权限均正确隔离 |
| A-02 | 同 Bot 两群积累获准共享事实 | 使用同一全局记忆域、两条独立群主干；跨群召回逐条明确来源群，多源事实不丢依据 |
| A-03 | 同群乱序完成预处理 | ingress_seq 顺序保持，失败项有终结记录 |
| A-04 | 慢群、慢工具与消息洪峰 | 其他 Bot/群获得配额，队列和内存有界 |
| A-05 | 两群对同 key 并发写入 | 无丢失更新；结果符合确定 reducer，不依赖完成先后 |
| A-06 | 事实事件随机乱序/重复/分批重放 | 同有效操作集产生相同事实投影语义，见 06 的性质测试 |
| A-07 | 显式语义派生候选在新事实/遗忘后完成 | 旧源集或旧 epoch 发布被拒绝，相关投影最终重建；默认裁剪不创建该模型任务 |
| A-08 | 各 reload 阶段注入异常/杀进程 | 权威路由可恢复，最多一个有效写代际，旧版本数据不被覆盖 |
| A-09 | 两次相交 reload 并发 | operation lease 互斥，失败方不能解冻另一方 |
| A-10 | 同时取消、撤权和提交 | 出现有定义的事务顺序，失败侧不能迟到写入 |
| A-11 | effect 已发出但回执丢失 | UNKNOWN 可查询，不因重试/热重载制造第二次执行 |
| A-12 | 发送拆片部分成功后重启 | 已确认成功部分不重发，未知部分先对账 |
| A-13 | 模块不释放任务/连接 | Supervisor 有限时间回收进程，记录清理失败和残余资源 |
| A-14 | 向量候选跨 Bot/已删除/无权限 | 事实物化拦截，不能经缓存旁路输出 |
| A-15 | Job 到期、重连、Worker 接管重叠 | 稳定 job_key、lease_epoch 和 effect_id 防重复 |
| A-16 | Python 文件语法错误/依赖错误/钩子失败 | 新候选失败，旧插件继续服务且诊断可定位 |
| A-17 | Context 提交后 outbox dispatcher 崩溃 | 恢复后意图仍可投递；不出现历史成功而意图丢失 |
| A-18 | 存储配置/embedding 版本迁移 | 旧凭据与句柄失效，事实数量/摘要和访问隔离校验通过 |
| A-19 | 忙时普通A、触发B、普通C、触发D、普通E | 下一模型安全点追加A–D，E继续排队；没有新触发且工作结束后E静默入Context |
| A-20 | 长时间仅有普通群消息，无触发 | 全量来源逐条入Context并持续确定性裁剪/归档，无对话模型调用、裁剪模型调用或回复意图 |
| A-21 | 静默排空/关闭Session时新触发到达或分类完成 | 触发不丢失、不被silent claim为已回答、不产生两个Session |
| A-22 | WAIT_TOOL/WAIT_APPROVAL/RELOAD期间收到多个触发 | 继续接收和分类；原Session恢复后到最后触发切口，零同群并发响应链 |
| A-23 | 同名/改名用户、转发与第一人称混杂 | 原作者ID/显示名快照/引用作者及群来源分别保留，裁剪引用、召回和工具授权不串人 |
| A-24 | InputBatch提交后模型失败或重载、后续CHAT回复先ready | 原输入不重复/不丢，触发未误终结；CHAT Delivery按response_seq串行而非完成速度发送 |
| A-25 | Agent等待批准且前序普通消息分类阻塞，此时收到受权取消/确认命令 | ShellGate独立接纳和执行控制；无第二Agent，Context仍按原ingress_seq连续推进 |
| A-26 | CHAT发送UNKNOWN，此时管理员查询状态 | CONTROL可使用保留发送容量；同平台限流/绑定租约有效，CHAT自身仍维持顺序和对账 |
| A-27 | 新暂停与旧reload/resume收尾交错，另有P0取消请求 | 最新maintenance_epoch决定准入；旧流程不能重新开闸，P0不夺生命周期资源租约 |
| A-28 | 群管理员发宿主命令，或获准目标Bot读取却无权公开到来源群 | 按独立能力/输出可见性拒绝；不回落Agent执行，不泄露目标结果 |
| A-29 | 跨Bot目标操作已提交但来源请求更新前崩溃 | 按稳定子操作ID恢复同一operation_ref；不重复执行，来源回执可恢复，不声称跨库原子 |
| A-30 | 同一批准令牌重复/异群提交，或批准后参数/代际/权限变化 | approval_requests单次消费；错误来源和旧计划拒绝，当前授权仍复核 |
| A-31 | shell命令包含敏感字段，处理失败后进入静默归档/裁剪 | 作者/序号/安全命令事实保留，参数和受限输出不进入Prompt、普通日志或跨群记忆 |
| A-32 | 同群换 Binding/重建 Conversation/改名，两群使用相同显示名 | 同群仍映射唯一主干，不同群稳定来源不同；召回同时展示来源标识和安全显示名 |
| A-33 | 多群归档/图投影乱序完成、重复投递并改变批次 | 节点身份和源内顺序不变；TRUNK_NEXT 不跨群，跨主干联想为显式证据边 |
| A-34 | 图物理写入一半崩溃，或发布前发生遗忘/端点变化 | 读者只见完整旧修订或完整合法新修订；候选不能混入当前邻接，旧源候选不得发布 |
| A-35 | 关联边可见但端点/证据不可公开，或端点来自另一 Bot | 跨 Bot 接纳被拒绝；权限不完整的路径不返回内容、来源名称、隐藏数量或推断结论 |
| A-36 | 连续高流消息与多次裁剪，显式语义任务未开启 | 每条稳定来源保留，活动视图有界、未答触发原文受保护；裁剪路径 LLM 请求/费用计数均为零 |
| A-37 | 图文与纯图片消息入群，两个群发送相同图片字节 | ContentPart 顺序、每群来源/作者分别保留；Bot 内可去重 blob，不合并发言或授权，不建重复主干节点 |
| A-38 | 仅文本 LLM 搭配原生图文 EmbeddingProfile，分别做 text/image 查询 | 图片归档和 text→image、image→text、image→image 召回正常；结果逐项标群/作者/part，未把图片编码成 caption 文本 |
| A-39 | 图片下载/嵌入超时后重试或永久失败 | 有界等待及 PARTIAL/缺失记录；文本可独立索引；迟到图片复用原位置并有版本事件，裁剪不等待嵌入或自动调用视觉模型 |
| A-40 | 含图片的 Context、跨群召回、嵌套工具结果调用 text-only API | 最终 payload 无非文本 parts/原图 URL/base64/隐含媒体字段；安全来源与遗漏记录保留，canonical Context/记忆及图像索引不变 |
| A-41 | 视觉模型失败后获准 fallback 到文本模型，同时新消息或图片到达 | 同一 input/source cut 用新能力生成新 manifest；不复用旧视觉 payload，不纳入新尾部，不改冻结的旧请求所见 |
| A-42 | 同维度的不同 embedding space/preprocess 版本交错发布 | 独立索引代际及 head CAS；查询使用匹配空间或声明的结果融合，绝不直接混算距离 |
| A-43 | 图片遗忘/撤权与下载、变体、嵌入、签 URL、模型 dispatch 交错 | 原来源和派生闭包失效；旧任务不能复活或重新签发；在途外部请求仍按已许可边界留账 |
| A-44 | 文本模型请求缩减预算后切回视觉模型，期间 canonical 裁剪按策略发生 | 请求裁剪不删除 canonical 图片或更改其 view profile；合法归档与引用可恢复图片，模型摘要/OCR/caption 自动调用计数为零 |
| A-45 | 视觉请求超时后按策略fence旧attempt并fallback，旧响应随后返回 | RunStep当前request/attempt/call epoch只接纳一个结果；旧响应仅对账费用，无第二ModelTurn/工具执行；wirehash与语义摘要分别可追踪 |
| A-46 | 全新部署经 Web 初始化后配置两 Bot、模型/嵌入、绑定、插件、记忆与预算 | 首管理员单次创建，日常操作无须手改文件；配置和数据按目标库授权隔离，部署目录只展示观测版本 |
| A-47 | Web 两标签页与 Shell 同 revision 改相同配置，HTTP 超时后重试 | 仅一个合法 head CAS；其余 409/REVISION_CONFLICT 保留草稿；同键同摘要恢复原操作，异摘要拒绝 |
| A-48 | generation 配置已保存后候选失败、旧候选晚到、新 desired 与回滚交错 | desired/effective 分开展示，失败保留旧 effective；发布随 route/state CAS，回滚新修订，旧完成不能覆盖新选择 |
| A-49 | Web 退出/账户撤销、票据到期与目标许可竞争，随后跨 Bot 批量撤权 | 禁止新票/续票；已签票窗口明确；各目标 policy 提交阻止旧票，无全局原子撤权承诺，已许可操作不伪报回滚 |
| A-50 | 密钥表单保存/轮换、参数报错、导出配置和查看事件流 | 普通读取仅 secret_ref/已设置状态；无明文回显/日志泄漏，空白不误覆盖，切换按客户端生效计划进行 |
| A-51 | 群命令在 Web 查看/批准、Web 操作期间断开群 Adapter 或页面刷新 | 当前权限下可查同 operation；跨入口身份需验证映射/原审批渠道约束或重新批准，Web 不伪造群消息；断连不重复或取消操作 |

实现测试分三层：纯 reducer 的性质/置换测试；真实 PostgreSQL 的事务与多进程竞争测试；假平台/假 Provider 的故障注入端到端测试。关键并发用 barrier 控制交错，不依赖 sleep 猜竞态。本节只定义验收方法，不声明当前完成度；已经运行的测试、失败和缺口统一记录在[实现状态](implementation-status.md)。

Shell 与 WebUI 的完整行为验收矩阵见[09](09-group-shell-command-system.md)及[12](12-webui-administration.md)，上述条目侧重持久事务、鉴权与恢复边界。

## 10. 建议代码结构

```text
src/forest/
  kernel/       bootstrap, router, lifecycle, supervisor, grants, recovery
  contracts/    envelope, bot, conversation, agent, memory, effect
  sdk/          plugin decorators, context, service handles, resource scope
  drivers/      python_worker, ipc
  storage/      kernel bootstrap, bundle installer, scoped repositories, commit guards
  management/   typed actions, configuration plans, request admission, query projections
  web/          HTTP API, authentication, setup, bounded event subscriptions
  modules/      official component packages; each owns descriptor, repository, schema and UI
  cli/
plugins/        普通 Python 用户插件与示例
webui/          后台页面、schema 表单、配置差异与操作中心
tests/
  properties/   合流、规范化与幂等性质
  integration/  数据库、IPC、生命周期
  scenarios/    多 Bot、多群、恢复与外部不确定性
```

这是目标目录，不在本次文档任务中创建空实现。Wasm Driver 为后续可选包，不影响 Python 路径的完整发布。

## 11. 开发阶段与出口

本节使用 M0–M5 表示里程碑，避免与群聊 Shell 的 P0–P4 服务优先级或已经删除的旧 P0–P5 切片混淆。

| 阶段 | 交付范围 | 出口 |
|---|---|---|
| M0 契约与可靠内核 | Python SDK/Worker、消息 schema、Scoped Store、CommitGuard、生命周期骨架、日志；独立 Web bootstrap/登录、配置修订与类型化管理纵向切片 | 普通 Python 插件可加载/失败清理，伪造身份和旧 epoch 写入被拒绝；浏览器首次初始化不依赖 Bot/LLM |
| M1 多 Bot 消息闭环 | OneBot Adapter、全量Inbox/作者信息、分类水位、会话Session/Actor、公平调度、命令插件、Delivery顺序；模块贡献式 Binding/基础配置与操作查询 | 两Bot多群并行，同群有序且全部正常消息可追溯，断线与UNKNOWN可查询；创建/设置无需手改文件 |
| M2 原生插件热重载 | watcher、不可变包候选、依赖计划、排空、路由提交、状态 handoff、操作恢复；模块 schema 表单、diff、desired/effective 与回滚 | 文件保存触发受控替换，语法/钩子失败保留旧代，100 次替换无资源增长；Web/Shell 并发配置无丢失更新 |
| M3 Agent 与工具 | CorePrompt/PresetContext、原子ModelTurn、Provider、InputBatch/触发切口、Run/checkpoint、静默裁剪、单工具账本、修复、确认/取消与定时触发 | 08交互用例通过；等待不阻塞接收和其他群、不另开同群Agent，恢复不重做已知效果 |
| M4 全局记忆与合流 | 原生图文来源/受管媒体、多主干图、跨主干证据、图文查询与出处召回、操作集/冲突投影、scope 水位、显式可选语义派生、遗忘/索引修复；LLM 能力投影与 canonical 记忆分离 | A-02/A-05/A-06/A-07/A-14/A-32–A-45、11 多模态验收及 06 全部性质场景通过；默认裁剪模型调用为零 |
| M5 运维与容量 | 模块贡献式 Web 管理、权限/密钥/成本、实时操作/日志视图、备份恢复、压力与故障测试 | A-46–A-51 及 12 WebUI 场景通过；首版 WebUI 可完成日常后台管理，单节点运行基线和已知限制可复现 |
| 可选扩展 | 远程/集群 Worker、额外平台、不可信插件隔离、Wasm Driver | 不弱化已建立协议，单独验收 |

各阶段累积实现同一套契约，不在早期采用未来无法兼容的无界队列、按群长期库或原地共享内存热替换。实现开始时优先交付可运行纵向切片，避免只搭建大量空抽象。
