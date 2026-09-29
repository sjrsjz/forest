# 干净数据库基线、模块数据所有权与载荷来源详细设计

> **权威性：存储详细设计。** 本文展开[00 架构总契约](00-architecture-contract.md)，并取代所有按开发期 SQL 编号组织的 payload/migration 文档。Forest 尚未发布，不升级开发期旧库，不保留旧列 fallback、双写、数据搬运器或 legacy marker。旧实现只能从 Git 历史追查，不能成为新代码的兼容要求。

## 1. 目标

持久化必须服务微内核边界，而不是把当前官方模块固化成一个产品数据库。不可绕过的 bootstrap 只拥有 Bot 身份与 schema bundle 账本；运行、统一载荷、授权、外部效果和审计分别由不可卸载但仍遵守模块描述符、bundle、Repository 与管理贡献协议的 kernel 模块持有。Agent、Memory、Adapter、Conversation Scheduler、Shell、Job 与 Delivery 分别拥有自己的业务事实和载荷引用。模块卸载、替换或热重载时，bootstrap 和其他模块都不需要理解其业务表。

正文和二进制内容必须从首次写入起携带完整来源闭包。删除、授权收缩、热重载、异步索引完成和外部结果晚到时，任何路径都不能通过缓存、派生正文或重放回执复活已撤销来源。

## 2. 数据库组成

每个 Bot 仍拥有独立事实库和凭据。一个事实库可以承载多个模块命名空间，但访问边界由数据库角色与宿主 Repository 同时执行。

| 所有者 | 可以持久化的内容 | 禁止持久化的内容 |
|---|---|---|
| Kernel bootstrap (`forest_kernel`) | Bot 身份、schema bundle 账本 | RuntimeGraph、业务配置、调用、授权、效果、审计正文 |
| `kernel.runtime` | Bot 运行封印、host/module lease、Generation、Route、Lifecycle、服务操作/attempt、状态引用、RuntimeGraph | Agent/Adapter/Memory/Shell/Job 业务字段、业务 payload 正文 |
| `kernel.payload` | Payload header/body、SourceManifest、派生父边、payload capability、删除 epoch 与 revoke/scrub 状态 | Memory 图、Context 顺序、业务可检索索引、模块私有配置 head |
| `kernel.authorization` | 精确能力授予集、短寿命票据、一次性消费、撤销事实、通用批准与授权 revision | 模块私有策略正文、Web 会话、平台账号、Shell 管理员名单 |
| `kernel.effect` | 通用 Effect identity、dispatch fence、attempt/outcome 与 UNKNOWN 对账 | Delivery SendIntent、OneBot wire body、Agent 工具上下文副本 |
| `kernel.audit` | 通用审计 envelope、来源 operation/call 引用、脱敏查询索引 | 模块业务正文、密钥、跨模块复制的完整 payload |
| `adapter.onebot` | Binding、配置引用、连接代际、规范化平台入站观察与媒体引用 | Inbox/Context、管理员名单与 Shell 策略/命令、SendIntent/发送 attempt/receipt、Agent Run、记忆向量 |
| `conversation.scheduler` | Conversation lane、Inbox、ingress 顺序、分类水位、trigger、InputBatch 成员、Context 顺序与确定性裁剪事实 | Agent Session/Run/ModelTurn/ToolCall、模型 Provider 私有状态、长期记忆图、消息正文 |
| `agent.runtime` | Session/Run/ModelTurn/ToolCall/checkpoint、模型请求与结果载荷引用 | 平台连接、Memory 内部索引、内核路由 head |
| `memory.graph` | source/trunk、payload/media 引用、capture、graph、embedding、retrieval evidence、记忆资格与 suppression 引用 | Payload 正文与 Bot deletion clock、Agent 执行所有权、平台发送状态 |
| `shell.command` | 按 Binding/群/主体生效的管理员策略、规范命令请求、资格/接纳事实、claim、类型化参数与回执引用、当前命令 registry | 平台连接、Inbox/Context、任意管理 API 地址或 Web 管理员会话 |
| `scheduler.jobs` | Job 定义、occurrence、lease 与触发结果引用 | 第二条 Agent 执行链或独立模型配置 |
| `process.executor` | 仅有标准模块配置账本；执行结果引用由 Job 持有，stdout/stderr 正文由 Payload 持有 | Job lease/状态、任意 shell 文本、请求方覆盖的路径或环境、输出正文副本 |
| `delivery` | SendIntent、part、attempt、resolution 与因果引用 | Adapter 私有 socket、Agent 历史副本 |

模块表放入由 bundle 分配的独占 PostgreSQL schema。模块运行角色只拥有该 schema 的 DML 权限和明确批准的内核函数；第三方 Worker 不获得数据库连接。官方受信模块也必须经自己的 Repository 访问，不能借同进程身份跨模块 join。跨模块原子流程使用内核主持的类型化 transaction participant 或 outbox/effect 协议；不能靠某模块直接更新另一模块的表来“方便地”完成事务。

`kernel.runtime` 的干净 bundle 只包含 Bot 运行封印、Host lease/incarnation、模块资源租约、Generation、接纳路由、生命周期操作、通用服务操作/attempt、组件状态 payload 引用和 RuntimeGraph。`service_operations` 固定稳定业务操作、服务、输入摘要、因果根和唯一终态；`service_call_attempts` 固定每次 call 的 Generation、Worker、RuntimeGraph、父调用、deadline 与执行/重放观察。结果和错误正文仍由 `kernel.payload` 持有。该 bundle 不因“都是内核功能”而吞并 payload、authorization、effect 或 audit 的表。任何 kernel 模块表都不得出现 `agent_tool_id`、写死 `adapter.onebot` 的控制服务约束或 Agent、Memory、Binding、Job 外键。业务模块通过注册端口证明自己的业务前置条件，再让对应 kernel 模块提交通用事实。运行图中的 storage 身份必须冻结 schema ID、version、namespace、baseline digest、Repository contract 和 execution profile，并与该 Generation 的 package digest 同时保存。

`kernel.authorization` 只回答“某个稳定主体凭哪一组当前能力，能否对精确资源执行精确动作”。授予集按主体、可选目标组件代际、能力键、资源种类和资源 ID 规范化并以摘要寻址；模块自身仍解释管理员名单、群 Shell 规则、工具策略、可见性和业务审批含义。模块先通过自己的策略端口形成能力请求，授权模块再签发最长 30 秒且绑定 `ticket_id + operation_id + subject + audience + action + resource + input_digest + plan_digest + grant_set_digest + policy_revision` 的票据。消费记录是不可变的一次性事实；相同 operation 的精确重放返回原回执，不同输入或目标复用身份必须拒绝。

没有 audience 的 bootstrap grant 不能默认扩张到任意运行代际。只有具体能力项显式声明 `delegable` 时，签发端才能把这一项委托给一个准确的 `component_id + generation_id`；已有 audience 的 grant 只能沿用原 audience。短票区分唯一的 `issuance_operation_id` 与最终 `operation_id`：发送前过期可以建立新的签发 attempt，但同一最终 operation 通过 advisory lock 与唯一消费事实只接受其中一张票，竞争消费确定地一成一败。通用批准只冻结请求对象、动作、输入/计划摘要、请求主体、批准范围、policy revision 与期限。批准决定本身必须消费绑定 `approval.decide + scope + approval_id/decision digest + plan digest + decision principal + kernel.authorization audience Generation` 的短票；获批对象在签发业务票据的同一事务中单次消费，拒绝、过期、撤权或异摘要重放都不能产生效果许可。

授权撤销与票据消费在同一 Bot 的 `policy_clock` 上线性化。普通授予、签发与首次消费取得共享事务锁，撤销取得排他事务锁并把 revision 只推进一次。若消费先提交，随后撤销不能抹掉已经发生的效果，但精确重放仍返回原消费事实；若撤销先提交，旧票据在最终消费边界观察到新 revision 并拒绝。撤销事实、旧/新 revision、选择器和幂等 operation 必须同事务提交，不能只在缓存中递增数字。授予集、票据和消费事实不可更新或删除，批准只允许 `PENDING -> APPROVED|REJECTED` 以及 `APPROVED -> CONSUMED` 的单向转换。

Web 登录、部署账号与 issuer 密钥属于 deployment authority；OneBot 群管理员和平台身份解析属于 Adapter/Shell；Agent 工具定义、参数验证和工具审批的业务上下文属于 Agent。它们可把稳定主体引用、策略结果或批准依据交给授权端口，但不能把这些业务表搬进 `kernel.authorization`，也不能让通用票据携带密码、平台令牌或任意策略正文。授权模块不跨 schema 建外键或 join，涉及正文的限制条件必须使用经 `kernel.payload` 端口复核的载荷引用。

`kernel.effect` 只负责不可撤回外部副作用的通用派发边界。Delivery 仍拥有 SendIntent、群聊顺序、Binding 和平台回执语义；Agent 拥有 ToolCall、工具 schema、Run 续调、请求投影与 fallback 决策；模型 Provider 拥有传输和用量回执。它们以稳定 `effect_operation_id`、精确目标、语义输入摘要和受管 payload 引用请求 Effect Gateway，不得把自己的业务状态机搬入内核效果表。效果记录不得出现 `tool_id`、`intent_id`、`binding_id`、群号、Provider 名称或 OneBot action 等业务字段。

`adapter.onebot` 的包 namespace 只物理拥有规范平台事件和 participant 幂等事实；Binding 配置使用每个包都具备的标准 `module_configuration` 账本，不在 Adapter 私表复制一套配置计划系统。配置 plan 固定期望 revision、完整 schema 校验后的配置 Payload、操作者和期限，apply 只引用该 plan；并发 apply 只能有一个推进 desired revision。凭据始终是宿主 Secret Resolver 的 opaque ref，明文不进入 Worker、配置 Payload、Adapter 表或日志。

WebSocket client、echo waiter 与连接缓冲是准确 Adapter Generation 的进程资源，不伪装成可恢复数据库实体。候选 Generation 以冻结配置、`binding_epoch`、RuntimeGraph 身份确定性派生 `connection_epoch`、`client_incarnation` 和 transport digest；候选 start 只连接并缓冲，RuntimeGraph 提交后的本地 activation 才打开事件流。退役先冻结 frame ingress 并等待已接纳调用排空，再 quiesce send effect、关闭 socket 和 Worker。旧代即使仍持有进程对象，也会被 Generation/graph/transport/effect 四重 fence 拒绝。Adapter 先把原始帧、规范化事件和作者保存为受管 Payload，再通过一次 UnitOfWork 把稳定平台事件交给 Conversation/Memory/Media/Shell participant；重放同一 `(binding, platform_message_id)` 返回原事实，不能重新分配 Inbox 顺序。Adapter 不持有 Conversation lane、Shell 请求、Agent Run、Memory source、Delivery intent/attempt 或平台发送回执账本。

OneBot action 是 `adapter.onebot` 暴露的传输端口，不是 Adapter 自己的持久发送队列。`delivery` 在自己的事务中冻结目标、wire payload、当前 Binding/connection fence 与 effect operation，取得通用 Effect 许可后才调用该端口。Adapter 对一次调用至多写 socket 一次并返回 `SUCCEEDED`、`NOT_APPLIED` 或 `UNKNOWN` 的类型化执行观察；Delivery 和 `kernel.effect` 各自在自己的 schema 记录业务结算与通用效果结算。socket、echo waiter、Bearer token 和 webhook secret 永不进入 Delivery schema。

`delivery` 的干净 bundle 物理拥有 `delivery_lanes`、`send_intents`、`send_parts`、`claim_operations`、`transport_attempts`、`delivery_observations`、`claim_releases` 与 `intent_cancellations`。CHAT lane 以 Conversation 为 ordering key 并使用单调 `response_seq`；CONTROL 使用独立 ordering key/sequence，可越过 CHAT 等待，但两者仍在实际传输端口共享 Adapter 容量。一个 lane 只有最早的非终态 intent 可领取；多 part intent 内也只允许当前 part 派发。`UNKNOWN` 会阻塞同 lane 的后续 intent，不能通过换 intent/attempt ID 绕过。

SendIntent 只保存 cause、目标、顺序和受管 part PayloadRef；最终回复事务由协调器在同一 UnitOfWork 中调用 Conversation 的 `append_context_in` 与 Delivery 的 `create_intent_in`，因此不存在已回答 trigger 却没有发送意图，或有发送意图却没有正式 REPLY Context 的半提交。领取使用短 lease 与单调 owner epoch；claim 只授予准备权，不允许网络 I/O。每个 transport attempt 冻结同一 part operation、wire PayloadRef、Binding/connection/client fence、Adapter Generation 与 `kernel.effect` operation/attempt。Delivery 的 prepare、handoff 和 observation 与 `kernel.effect` 对应端口在同一事务提交；只有 handoff 已提交后才调用 Adapter。

执行观察分为 `SENT`、`RETRYABLE_NOT_APPLIED`、`REJECTED_NOT_APPLIED` 与 `UNKNOWN`：前两种通用效果分别映射为 `SUCCEEDED` 与 `NOT_APPLIED`，拒绝也映射为确定 `NOT_APPLIED`，但 Delivery 将该业务 part 终结为失败而不是自动重试。`UNKNOWN` 只能由 `RECONCILIATION` 证据解析为确定结果；每个晚到或冲突观察都保留，只有一个被接受的状态推进 lane。receipt 正文是 `kernel.payload` 引用，不得以内联 JSONB、异常文本或 Adapter 私有响应对象保存。

`conversation.scheduler` 的干净 bundle 物理拥有 `conversation_lanes`、`inbox_messages`、`conversation_triggers`、`selection_operations`、`input_batches`、`input_batch_events`、`input_batch_triggers`、`context_units`、`context_append_operations`、`context_compactions`、`context_compaction_units`、`active_context_units`、`trigger_terminal_operations`、`conversation_silent_drain_settlements`、`conversation_context_maintenance_settlements` 与 `conversation_agent_work_settlements`。它只对 `forest_kernel.bot_identity` 建立 bootstrap 外键；对 Adapter event、Payload、Agent Session/Run、Delivery reply 和业务路由结果一律保存稳定引用及必要摘要，并通过对应端口复核。正文、作者快照和结构化分类依据只存在于 `kernel.payload`，调度器表不得出现任意 JSONB 正文列。裁剪成员表只记录稳定单元身份与摘要，归档引用正文仍由 `kernel.payload` 拥有。

非输入 Context 单元只能通过幂等追加 operation 写入。最终 `REPLY` 单元与其回答的 trigger 状态必须在同一调度器事务提交；不存在绕过 Context 单独把 trigger 标为 `ANSWERED` 的 Repository 方法。Delivery 仍只接收 reply/send-intent 引用并独立记录外部送达，不能反向改写 Context 或 trigger。

`agent.runtime` 的干净 bundle 物理拥有 `agent_sessions`、`agent_runs`、`session_close_operations`、`run_inputs`、`run_input_triggers`、`run_checkpoints`、`model_requests`、`model_transport_attempts`、`model_observations`、`model_turns`、`tool_calls` 与 `tool_observations`。`COMPLETED` Run 只结束该 Run，不自行关闭 Session；Conversation 的排空裁决随后显式关闭 Session，或以唯一后继 Run 推进同一 Session。它不拥有 Conversation Inbox/trigger/InputBatch/Context 顺序，不拥有 Provider 客户端、Delivery SendIntent、Memory 图、Kernel Call/Effect 或审批 authority。来自这些模块的对象只保存稳定引用、摘要、冻结 revision 和当时验证结果；不得跨 schema 外键或 join。

`agent_sessions` 表示一个 Conversation 的单 Agent 逻辑响应链，活动状态对 `conversation_ref` 具有唯一约束；它不是“子 Agent registry”。一个 Session 内的 Run 和一个 Run 内的 ModelTurn/ToolCall 都按顺序推进，不能让同群两个 Run 同时取得执行 lease。调度器先产生稳定 `session_ref/run_ref` 并提交 trigger cut，Agent runtime 再以幂等 start operation 接受 InputBatch 引用；两边通过类型化事件/回执收敛，不双写对方表。

每个 Run 冻结 `agent_spec_revision`、配置/Preset PayloadRef、`ForestCorePrompt protocol_version + digest`、parser/serializer revision、预算和绝对 deadline。CorePrompt 正文仍是发布制品中的只读常量，不复制到数据库、不允许 WebUI 更新；每个模型请求必须再次固定它的版本、digest 与实际 Provider 指令通道。`run_inputs` 只追加 InputBatch 引用与规范化 trigger/principal 成员，不能覆盖早期 input revision。

`model_requests` 是一次语义模型调用的不可变 manifest；`model_transport_attempts` 是其实际 wire/effect 尝试。每个派发或对账结果先追加不可变 `model_observations`，再按单向状态机更新 attempt/request/Run；互相冲突的晚到证据保留为 rejected observation，不能覆盖已接受结果。`UNKNOWN` 只能由明确对账解决，不能重发来猜测。`model_turns` 只接受 [04 的 Agent 协议](04-bot-agent-memory.md#31-forestcoreprompt-v1)解析出的一个完整原子 ModelTurn；原始候选、解析后 turn、Assistant 候选与 FunctionCall 参数均是 `kernel.payload` 引用。`tool_calls` 保存 Agent 工具业务语义和明确 `authority_trigger_ref`，通用派发状态由 `kernel.effect` 管理；Kernel 表不得重新出现 Agent tool ID。

`run_checkpoints` 是不可变、带 checksum 的恢复事实。可变 Run head 只指向最后 checkpoint、当前 input/step 和 phase；owner epoch 与短 lease 保护物理执行权。进程内 Python 对象、流回调或异常对象都不是恢复权威。最终 Assistant 仍必须交给 `conversation.scheduler` 的原子 Context/reply 端口，Agent runtime 自己不能把 trigger 标为 `ANSWERED` 或直接写 Delivery。

该 bundle 不拥有 `conversation_sessions`、`runs`、`model_turns`、`tool_calls`、`business_command_requests`、`message_handler_chains` 或平台 Binding。一个 Conversation 的 `active_session_ref/active_run_ref` 只是串行调度 fence 的外部身份，不是对 Agent schema 的 SQL 外键。输入批次成员必须使用规范化关系表，不能把 event/trigger ID 数组藏进 JSON。所有已接纳且允许读取的消息都先占用不可变 `ingress_seq`；分类可以乱序完成，但 `classified_through_seq` 只跨越连续终态前缀。

输入选择仅接受 `IDLE`、`MODEL_SAFE_POINT` 和 `SESSION_END` 三种边界。事务锁定单个 lane 后，以 `classified_through_seq` 为上界寻找最后一个待领取 trigger：存在 trigger 时提交从 `context_applied_seq + 1` 到该 trigger 的连续前缀；其后的普通尾部留队。模型安全点没有 trigger 时不消费普通尾部；Session 结束或空闲且只有普通消息时提交无 Run 的 `SILENT_DRAIN`。`WAIT_INPUT/CONTINUE/IDLE` 等无批次结果也写不可变 selection 回执；相同 operation 与相同输入摘要精确重放第一次结果，冲突重用拒绝；同群 actor epoch 串行，不同群不共享锁。

外部派发严格分为四个边界：

1. 业务模块在事务外准备实际 wire 内容，并把正文交给 `kernel.payload`；此时没有外部效果许可；
2. 短事务锁定 effect operation，经端口消费精确授权票据、复核 wire payload 和执行器 fence，创建最长 30 秒的 `PREPARED` attempt；
3. 实际 I/O 前的最后一个短事务再次复核 attempt、payload、执行器 fence 与期限，把 attempt 持久变为 `DISPATCHING` 并写入 `started_at`；事务提交后才把 bytes 交给网络、进程或设备；
4. 执行器只追加 `SUCCEEDED`、`NOT_APPLIED` 或 `UNKNOWN` 观察。结果正文和供应商回执仍是 payload 引用，通用表只保留摘要、状态和因果身份。

`PREPARED` 尚未交接，可在过期或明确取消后变为 `ABANDONED`；`DISPATCHING` 丢失执行结果必须收敛为 `UNKNOWN`，不能回退 `PENDING`。`UNKNOWN` 不得自动重发，只能由显式对账 operation 和不可变 evidence 将同一 attempt 解析为 `SUCCEEDED` 或 `NOT_APPLIED`；只有确定 `NOT_APPLIED` 后，业务模块才能以同一 effect operation 创建下一 attempt。`SUCCEEDED` 为终态。取消只阻止尚未交接的 attempt，不能把已进入 `DISPATCHING` 的事实改写成未执行。

所有路径按 effect operation 后 attempt 的固定顺序取锁。相同许可、交接、观察或对账 operation 的精确重放返回原回执；同一 ID 改输入必须冲突。并发观察与对账无论谁先提交都只能形成一个当前结算结果；冲突证据必须保留为可审计冲突，不能以后到写覆盖。热重载、Host 失租、Bot 停止、授权撤销和遗忘可以阻止新的 `PREPARED`/交接，但已交接效果仍按原 attempt 留账并对账，不能因正文被 scrub 就抹成 `NOT_APPLIED`。

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

`kernel.payload` 是这套协议的唯一写入权威。它只理解稳定来源引用、内容摘要、派生边、授权/删除 fence 和正文生命周期，不理解某个 payload 是 Prompt、记忆节点、OneBot 消息还是配置字段。业务模块在自己的 schema 保存 `payload_id + digest + owner_kind/owner_id`，并通过 payload participant 在同一 UnitOfWork 中创建或复核正文；模块间不得对 `kernel.payload` 表建立 SQL 外键或直接 join。Memory 仍拥有 source/trunk/graph 与检索资格，Conversation 仍拥有 Context 顺序，Payload 模块不会因集中保存正文而取得这些业务语义。

正文状态只有 `ACTIVE -> REVOKED -> SCRUBBED`。永久事实保留稳定 ID、状态、摘要、因果和外部效果结算信息；正文不可用时返回类型化 `PAYLOAD_REVOKED/PAYLOAD_SCRUBBED`，不得返回空字符串、空 JSON 或从审计/outbox/checkpoint 复制一份旧内容。

下列内容从第一次写入就必须使用该协议：群入站、Context 单元、Agent snapshot/request/result/tool/final、Memory source/media/evidence、Kernel call 参数与结果、模块状态、配置修订、Job 定义、Shell 参数和结果、Delivery wire body/receipt、管理操作结果和审计详情。不存在“以后再迁”的裸 JSONB 正文列。

## 5. 删除与异步完成的线性化

每个 Bot 的内容域持有单调 `deletion_epoch`。公开遗忘采用 `plan -> apply -> scrub`：

1. plan 固定请求目标、精确来源闭包、影响集合摘要、当前 epoch 和期限；
2. apply 在短事务内重算集合并 CAS epoch，写 suppression，撤销所有已知派生载荷和索引资格；集合变化时要求重新 plan；
3. scrub 有界删除 body、媒体对象、向量与缓存，直到可证明的在线副本归零；WAL、备份和已发外部副本单列状态。

异步写只有两种合法排列：新结果先提交，则 apply 必须在重算闭包时包含它；apply 先提交，则新结果在最终发布时观察更高 epoch 并被拒绝。下载、embedding、LLM、工具、图构建和平台发送均在外部 I/O 后、写入业务事实前复验来源、epoch、Generation、Host lease 和自身 operation 身份。已发生费用或外部结果可以结算元数据，不能重新发布正文。

`kernel.payload` 的普通 admission 对 Bot deletion clock 取得共享事务锁，多个群和多个模块可以并行写不同 payload；遗忘 apply 取得同一行的排他锁并只把 epoch 推进一次。这样 apply 若后获得锁，会看见并撤销此前提交的正文及其派生闭包；apply 若先获得锁，晚到 admission 在锁释放后必须观察新 epoch 和 suppression 后失败。正文最终接纳触发器必须复用同一种共享锁，不能在已经持有共享锁后升级成排他锁；否则两个相同 payload 的幂等并发写会在唯一键等待与锁升级之间形成死锁。不得为了简化实现把每个 Bot 的全部 payload admission 都串行化到排他 clock 锁，也不得只在事务外比较 epoch。

相同幂等 operation 重放返回原永久回执和“正文已撤销”状态，不重新调用 Worker、模型或平台。不同输入复用 operation ID 必须拒绝。UNKNOWN 外部效果保持 UNKNOWN，遗忘不能把它改写成确定未发送。

单来源遗忘的包间事务原语为 `Memory prepare -> Payload revoke -> Memory apply`。Memory prepare 锁定精确来源单元，输出通用 `forest.payload-forget-plan/1` 和作者载荷锚点。Kernel Payload 不解释群、Memory 单元或图；它要求锚点的不可变来源清单只有一个完整根来源，撤销该根的载荷派生闭包，并返回版本化回执及精确 Payload ID 选择集。Memory apply 确认锚点仍属于请求的来源单元，再在自己的 namespace 中失效所有引用命中载荷的单元、occurrence、facet、关系端点与关系证据，为全部潜在索引 ref 登记删除 outbox。任何 participant 拒绝都会回滚整次撤销。多根锚点拒绝而不是猜测；更大范围选择器由显式管理计划固定，不能通过该原语隐式扩大。

遗忘不撤销已领取工作的结算身份。已领取 facet 准备任务保持 claim，返回或过期恢复时根据 suppression 收敛为失效；新领取与派生发布从权威来源状态拒绝。历史载荷的 `accepted_deletion_epoch` 是审计值，读取应在当前删除屏障下检查 ACTIVE 状态；不能拿创建时 epoch 拒掉未受影响的配置、组件状态或服务回执。需要冻结模型/副作用快照的调用仍显式传入自己的期望 epoch。此事务原语不替代管理面的影响集合计划、审批与物理 scrub 进度。

## 6. 多模块事务与通信

模块之间只交换版本化 DTO、`PayloadRef`、`SourceManifestRef` 和明确因果引用。需要原子提交时，由内核的 UnitOfWork 创建一次 operation，并按已注册 participant contract 调用模块 Repository；所有 participant 使用同一 Bot 数据库事务，但每个 participant 只能写自己的 schema。典型流程包括：

- 入站：Adapter 写平台事实，Conversation Scheduler 分配 ingress，Memory 登记 source observation；
- 最终回复：Agent 关闭 Run，Conversation Scheduler 提交回答状态，Delivery 写 SendIntent；
- Shell：Shell 写类型化请求，Control operation 写操作事实，Delivery 写 CONTROL 回执；
- Job 触发：Job 关闭 occurrence claim，Conversation Scheduler 写内部 ingress/trigger。

participant 集合、顺序、输入摘要和结果摘要写入 operation；重试复用同一身份。模块不能在一个数据库事务中等待网络、LLM、Worker 或用户确认。跨独立数据库没有伪造的原子事务，使用 outbox、幂等接纳和显式补偿/对账。

## 7. 配置、UI 与敏感字段

模块配置属于模块 schema。拥有该组件配置的模块 Repository 保存 revision、desired/effective 指针和配置载荷引用；`kernel.runtime` 只在 Generation/RuntimeGraph 中冻结本次实际采用的 revision、payload identity 与 digest，不维护另一套可写配置 head。字段 schema、默认值、验证、计划差异与生效动作由准确 Generation 的管理契约提供。WebUI 通过模块描述符加载 schema 或沙箱 UI，不建立 Agent/Memory/OneBot 专用表单。

`kernel.runtime.generations` 必须保存明确的 `config_revision`，并与 `config_payload_id/config_payload_digest`、package digest、状态契约摘要、能力授予集摘要、实例/Worker/Host incarnation 一起构成不可变代际身份。`runtime_graph_modules` 必须保存 `generation_id`，以 `(bot_id, generation_id)` 外键引用 `generations`，并约束该 Generation 的 `component_id` 等于图成员；图中重复保存的 package/storage identity 只是可独立核验的封印，不能替代 Generation 引用。图摘要必须覆盖该引用以及依赖和契约摘要。没有配置的模块也必须由其所属 Repository 发布一个内容寻址的空配置修订，禁止由内核填写空字符串、全零摘要或隐式默认值。这样管理 UI 显示的 effective 配置、调用准入使用的 Generation 和消息冻结的 RuntimeGraph 才指向同一份事实。

官方 bundle 通过同一 `module_configuration` Repository contract 在各自私有 namespace 中拥有独立的 head、plan、revision 和 activation 表；共享的是协议代码，不是表或配置所有权。配置正文由 `kernel.payload` 保存，模块验证后形成 plan；apply 只推进 desired revision，当前 Generation 继续引用 effective revision。候选模块 PREPARE 必须读取并冻结当前 desired 的 payload identity。RuntimeGraph 发布事务把每个模块的配置 activation 作为必需 participant：先复验载荷仍 ACTIVE、revision/payload 与候选 Generation 完全一致，再与 Generation ACTIVE、Route epoch 和 graph head 一起提交 effective 指针。缺少 participant、配置已遗忘、身份不一致或任一 participant 失败都回滚整张图；不允许在图发布后用第二个事务“补写 effective”。

密钥正文直接写入 Secret Resolver，配置载荷只保存 opaque `secret_ref`。读取配置不回显密钥；修改计划只记录“是否变化”和不可逆摘要。VL embedding 的模型种类、输出维度、归一化和融合语义属于 Bot 创建时冻结的 Memory schema identity；创建 Bot 后只能更换协议兼容 endpoint/凭据。改变模型种类必须创建新的 Bot 记忆域或执行未来明确设计的全量重建操作，不能通过普通配置 apply 偷换向量空间。

## 8. 干净基线验收

- 新 Bot 数据库先只安装 `forest_kernel` bootstrap，再按冻结的首发 RuntimeGraph 安装所需 kernel 模块与所选业务模块 bundle；bootstrap 中不存在 RuntimeGraph、Payload、授权、效果、审计或任何业务表，任何 kernel 模块 bundle 中也不得混入 Agent、Memory、OneBot、Shell、Job 或 Delivery 业务表。
- 启用模块才安装其准确 bundle；删除任一官方模块包后，内核仍能启动并明确报告缺失契约。
- 目标命名空间非空、摘要不符、未知对象、越权 DDL 和跨 owner 引用均拒绝。
- 源码、SQL 和测试中不存在开发期 `migration_v*`、001…087 回放、legacy marker、旧列 fallback、双写或非空旧库升级夹具。
- 从群入站到 Agent、Memory、Tool、Delivery、audit 的 canary 内容只有受管 payload body；遗忘后所有在线正文与向量不可读且晚到结果不能复活。
- 两个群并发写同一 Bot 记忆时，来源与主干不混淆；事实合流按稳定 ID/版本完成，不按 Worker 完成先后覆盖。
- 模块 schema、Repository、management contract 与 UI 工件均绑定同一包摘要和 Generation。
