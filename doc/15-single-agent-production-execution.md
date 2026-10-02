# 单 Agent 生产执行与会话协调

> **权威性：生产执行边界详细设计。** 本文展开 [00 架构总契约](00-architecture-contract.md)、[08 群聊上下文](08-group-context-interaction.md)与[13 微内核运行时](13-microkernel-control-plane-redesign.md)。Agent 始终是一条 QiYu Agentlib 风格的 Session/Run 状态机；“下一轮”是同一 Run 中的下一次模型调用，不创建产品子 Agent。

## 1. 所有权与调用边界

生产链由现有模块和微内核机制协作完成，不增加拥有第二套聊天状态的“Agent 编排模块”。

| 主体 | 拥有 | 提供/调用 | 明确不拥有 |
|---|---|---|---|
| `conversation.scheduler` | Inbox、trigger、InputBatch、Context、群 actor lease、ConversationWorker 生命周期任务 | 冻结输入切口；经内核 Contract Router 调用 `agent.execute/2`；向 manifest UnitOfWork 提交回复或 Session 交接计划 | ModelTurn、工具状态、Provider 客户端、Delivery 状态、其他模块 participant 对象 |
| `agent.runtime` | Session、Run、CorePrompt 协议、Preset 引用、ModelTurn、ToolCall、模型 attempt 与 checkpoint | `agent.execute/2`；在每次模型调用前执行自身 manifest 声明的 `agent.acquire-model-input/1` UnitOfWork；产出 `FinalCandidate` | Inbox 游标、群 actor、正式 REPLY、SendIntent |
| `provider.chat-completions` | Provider profile、HTTP 传输、`model.result` 与 `model.usage` Payload | `provider.profile.bind/1` 返回模型能力；`provider.model.dispatch/2` 校验 Agent 冻结的投影及 wire 精确引用、授权票据并发送请求，只返回结果引用 | Context 裁剪、输入预算估算/拒绝、多模态内容选择、Memory 证据选择、Agent Run、Effect 账本 |
| `tool.*` | 各工具能力快照、attempt/effect 观察 | 被 Agent 通过版本化 contract 调用 | Conversation 与 Agent 状态机 |
| `delivery` | SendIntent、part、attempt、发送回执与 UNKNOWN | `delivery.intent/1` participant；异步发送已提交意图 | 回复内容决策、Agent Run |
| 微内核 Contract Router | RuntimeGraph 中的 contract → component Generation 解析、版本/代际/deadline/admission 校验 | 调用冻结的公开端口 | 聊天策略、Agent 选择、Prompt、Context 或执行循环 |
| 微内核 UnitOfWork 协调器 | 一次事务的 participant 集与提交 guard | 调用 participant 的同连接事务端口 | participant 的领域事实与私有 Repository |

旧实现中名为 `contract.router`、同时选择 Agent、调用模板 responder 并驱动聊天的业务模块属于错误抽象，应直接删除。内核 Contract Router 不能被实现成 Bot 业务模块，也不能因历史类名相同而接管 Agent 循环。

## 2. 组件设计门

### 2.1 `conversation.scheduler`

| 项目 | 契约 |
|---|---|
| owner / namespace | 仅拥有会话连续序、分类水位、trigger、InputBatch、Context、actor lease 和 Worker 任务；写 `conversation_scheduler` 私有 namespace |
| provides | `conversation.context.snapshot/2`；`conversation.admit-message/1`、`conversation.complete-classification/1`、`conversation.select-input/1`、`conversation.append-context/1`、`conversation.terminal-run/1` 等 participant |
| requires | 内核 lifecycle/Contract Router/UnitOfWork，冻结分类结果，`agent.execute/2`；Scheduler 不直接依赖或持有 `delivery` 运行实例 |
| Generation fence | 本 Generation 控制 Worker admission；每次领取、选批次、追加 Context 和最终提交都校验 actor epoch 与 member generation |
| concurrency | 数据库 actor lease 保证同群单链；有界 semaphore 只控制跨群并发，进程锁不构成权威 |
| recovery | 从 lane 的 active Session/Run 引用恢复；完成 Run 只继续交接；过期 actor 不能提交 |

### 2.2 `agent.runtime`

| 项目 | 契约 |
|---|---|
| owner / namespace | 仅拥有单 Agent Session/Run、模型轮次、工具步骤、attempt、checkpoint 与 FinalCandidate；写 `agent_runtime` 私有 namespace |
| provides | `agent.execute/2`、`agent.run-query/1`、`agent.finalize-participant/1` |
| requires | Provider、工具、Authorization、Effect Gateway、Payload Authority、`conversation.context.snapshot/2`；模型安全点通过 `agent.acquire-model-input/1` UnitOfWork 调用 Conversation 与 Memory 的精确 Generation participant |
| Generation fence | Run 冻结 Agent spec、Preset、Provider/tool contract 引用；每次外部 handoff 和事务 participant 调用复核 member generation 与 Run owner epoch |
| concurrency | 同一 Run 只有一个 owner epoch；模型调用和工具因果组顺序闭合，不产生分支 Agent |
| recovery | 从持久 checkpoint 继续；不可查询的已派发模型/工具进入 `UNKNOWN/BLOCKED_UNKNOWN`，不能换 operation 重发 |

两个模块只交换类型化 DTO、稳定引用和受管 PayloadRef。Scheduler 不持有 Agent Repository 或运行实例；Agent 不持有 Scheduler Repository 或运行实例。Contract Router 句柄不是 Python bound method，必须冻结目标 Generation、契约版本和 admission fence。跨模块原子提交由内核 UnitOfWork 根据计划解析已注册 participant；不能把 participant 对象注入 Scheduler，也不能由任一业务模块直接写其他 namespace。

`agent.execute/2` 的输入是可序列化 DTO，只包含 `conversation_id`、`session_ref`、`run_ref`、恢复标记、绝对期限以及 Scheduler 签发的 actor lease fence。禁止传递 Python callback、Repository、数据库连接、participant 或宿主对象。Agent 到达下一次模型调用安全点时，用确定性的 step identity 调用自身包声明的 `agent.acquire-model-input/1`；该计划在一个 Bot 数据库事务内依次调用 Conversation 的输入选择 participant 与 Memory 的批次资格 participant。Conversation participant 复核 actor owner/epoch、Session/Run、当前 Scheduler Generation 和连续分类水位，只发布截至最后一个新 trigger 的连续前缀。Agent 随后通过 `conversation.context.snapshot/2` 取得不可变 ContextView 的精确 PayloadRef。运行时调用可以形成 `conversation.scheduler -> agent.runtime -> conversation.scheduler` 服务环，但两个方向都必须经过 Contract Router；只有启动依赖边参与无环拓扑，Agent 对 Context 服务的要求声明 `startup_dependency=false`。

初始批次也不能由 Conversation Worker 先选择后作为普通参数交给 Agent，否则两次提交之间崩溃会留下“trigger 已 CLAIMED、Agent Run 尚不存在”的裂缝。`conversation.scheduler` 的 `conversation.agent-run.pending` owned-work 只领取群 actor lease，并以稳定 work operation 调用 `agent.execute/2`；调用 DTO 只含 actor fence、`session_ref`、`run_ref`、恢复标记和绝对期限。Package Adapter 在 Worker 的只读 Generation identity 中固定当前 configuration 的完整 PayloadRef；Agent Worker 将它与自身配置中的 Preset 引用组合，不允许 Conversation 传入或覆盖 Agent 私有配置身份。随后 Agent 调用包声明的 `agent.start-from-conversation/1`：同一事务先执行 `conversation.select-input`，再由 `agent.start-selected-run-record` 从前一步结果建立 Run 和第一版输入。没有 trigger 时整项返回 `IDLE`，不得创建空 Run；任一步失败则 trigger claim、lane 活动引用和 Agent Run 一起回滚。服务响应丢失时，owned-work 以相同 operation ID 重放，UoW 只能返回原选择与原 Run；不得换 `session_ref`/`run_ref` 绕过冲突。

`agent.execute/2` 不接收 `context_snapshot`、`next_model_input`、`before_external_step` 等 callback。后续安全点由 Agent Worker 直接调用 `agent.acquire-model-input/1`，Context 通过 `conversation.context.snapshot/2` 服务读取；Generation 停止准入或进入 quiesce 时，Worker 只在模型/工具边界把 Run 写成可恢复状态并返回类型化 reload 结果。Conversation owned-work 的 settle 仅在 Agent 返回终态、可恢复停驻态或明确失败后释放 actor lease；调用进程消失时 lease 到期后重新领取同一 active Session/Run，不创建并发 Agent。

Context 快照正文不穿过 Worker 控制帧。Conversation Worker 以最多 32 个活动单元为一页读取自己的权威，逐页写入 `context.snapshot.page`，并把每个正文与作者的完整 PayloadRef 固定为父边；超过 128 页时继续写入 `context.snapshot.index` 中间节点，最终 `context.snapshot` manifest 只引用根节点。每页查询都复核同一 actor lease，Worker 还必须比较所有页的 Context revision、View revision、lane 身份与归档头；分页期间任一 fence 变化就放弃整个调用。这样数月历史经裁剪后仍可有界传输，来源、作者和 archive 关系也不会因序列化大对象而丢失。

模型大载荷不穿过 Worker 控制帧。Agent 将规范输入及来源父链写为 `agent.runtime/model.input` Payload，按已绑定的能力生成 `agent.runtime/model.projection` 与 `agent.runtime/model.wire`；投影决定消息、来源标签和多模态内容，wire 是精确的 HTTP 请求正文。Provider 不接收原始 Context、Preset 或媒体列表，也没有请求 materialize 服务。内核随后把只含精确引用、Provider Generation fence 与授权票据的 dispatch 命令冻结为 `kernel.effect/effect.wire`。Agent model attempt 与 Effect permit 必须在同一 UnitOfWork 中绑定这个 `effect.wire`；Provider driver 复核 Effect permit 和两个 Agent PayloadRef 后才读取 `model.wire` 并进行 HTTP handoff。响应成功时，同一个 Provider Generation 在一个事务内保存不可变 `model.result`，可选 usage 另存为以结果为父边的 `model.usage`；`provider.model.dispatch/2` 和 Effect evidence 只携带完整 owner/digest 引用，Agent 不得复制响应正文或冒充 Provider 写响应载荷。`model.wire` 是 Agent 冻结的 Provider HTTP 正文，`effect.wire` 是内核冻结的副作用命令，两者不可混用。

Agent 的恢复读取和模型写入都属于 `agent.runtime` 包内端口，不能由宿主 Repository 暴露表行。`agent.run-recovery/1` 只返回状态机继续执行所需的稳定事实。建立一次模型调用先以 `agent.prepare-model-request/1` 冻结 Provider Generation、投影和输入 fence，再以 `agent.prepare-model-effect/1` 在同一事务内签发准确 audience 的授权票据、建立 Kernel Effect permit 并写入 Agent Attempt；不存在可由 Worker 单独调用的 Attempt 准备端口。外部 HTTP 前必须提交 `agent.handoff-model-effect/1`，它在同一事务内把 Effect、Attempt、Request 和 Run 推进到 `DISPATCHING/DISPATCHING/DISPATCHING/MODEL_PENDING`；任一 participant 拒绝时全部回滚。Provider 返回后只允许提交 `agent.observe-model-effect/1`，Kernel Effect 与 Agent observation 必须具有相同 attempt、operation、状态和证据类别并原子收敛为 `PARSING`、`PREPARING_MODEL` 或 `BLOCKED_UNKNOWN`。一旦 handoff 提交，重启或热重载只能查询原 Effect，不能更换 operation 重发；`UNKNOWN` 只有带查询证据的 `RECONCILIATION` 可以收敛成成功或明确未发送。相同稳定身份与相同内容可重放原事实，任何字段变化都冲突；PayloadRef 由 manifest 声明的 provenance 规则在进入包 SQL 前验证。Provider 只验证冻结引用并执行网络效果，不选择上下文内容、不读取或裁决上下文预算。

Request 已写为 `READY` 但尚无 Effect 时，以及既有 Attempt 全部明确 `NOT_APPLIED` 时，Agent 都通过 `agent.frozen-model-resume` 只读端口取得该 Request 首次 `model.wire` 和 `model.projection` 的不可变 PayloadRef。`READY` 要求零 Attempt，并直接复用首次 wire；`NOT_APPLIED` 要求所有既有 Attempt 均已确定未发送，将原 wire 字节复制为新 Attempt 的 `model.wire`，并建立到初始 wire 和投影的父链。两种恢复都不重新执行记忆召回、Context 读取或模型投影。`model_requests` 固定首次 wire 引用；Attempt 表里的 `kernel.effect/effect.wire` 只表示内核副作用命令，不能充当 Provider 请求正文。此路径仍要求旧 Provider Generation 和 profile 可用，跨 Provider Generation 的继承另按下述协议处理。

`effect.prove-not-applied` 是内核 Effect 的只读 UnitOfWork participant：按稳定 ID 顺序锁定每个 Effect/Attempt，要求当前状态均为 `NOT_APPLIED`，并返回已接受的未发送 observation 身份与证据集合摘要。Agent 的三次尝试耗尽结算现先执行该 participant，再由 Agent 私有 participant 在同一事务内核对全部 Attempt/Observation 与证明回执后写入失败 checkpoint；任一侧不一致则整笔事务回滚。这个跨命名空间的证据端口也供后续请求继承使用，不允许 Agent SQL 直接读取 Kernel Effect 表。

模型请求已冻结旧 Provider Generation 且尚未终结时，新 Agent 可以在保留该 Provider Generation 的定向替换后继续。同一次 Graph 替换移除旧 Provider 时，候选插件在预热后通过通用 `replacement_facts` 生命周期钩子声明不可变兼容性事实；声明非空事实的包必须在 manifest 的 `lifecycle.replacement_facts_schema` 给出 JSON Schema，候选发布前按冻结 schema 验证，不声明时只允许空对象。内核只携带代次与事实，不解释 Provider 配置。Provider 包以 `forest.provider-profile/1` 声明 profile 引用、模型与能力摘要；Agent 包的 `agent.replacement-guard` 仅在这些值与冻结 Request 完全一致，且请求处于零 Attempt 的 `READY` 或全部 Attempt 明确未发送的 `NOT_APPLIED` 时放行。新 Agent 在首次 dispatch 前用下述继承协议创建新 Request。其余情形发布前拒绝并恢复旧图；不能用新 Provider 身份重建旧 request ID，或把请求 ID 冲突当作暂时失败反复重试。

### 已确定未发送请求的跨 Provider Generation 继承

继承的单位是同一 Run 的同一逻辑模型步骤，不是复用旧 request ID。旧 `model_requests` 行和每个 Effect/Attempt/Observation 永久保留；新请求有自己的 request ID、Provider Generation、投影引用和首个 Attempt，旧请求只能单向进入 `ABANDONED`。同一 `(Bot,Run,step_seq,request_kind)` 至多有一个非 `ABANDONED` 请求；请求修订号单调递增，后继事实唯一指向前驱。Run 的 `current_model_request_id` 只在同一事务内从前驱切到后继，不增加 `next_step_seq`、不改变 InputBatch、Context cut、trigger claim、Session 或群 actor。

私有 schema 以 `(Bot,Run,step_seq,request_kind,request_revision)` 唯一键保存历史 Request，并用部分唯一索引约束同一步骤只有一个非 `ABANDONED` 头；不可变 `model_request_inheritances` 事实固定前驱/后继、两个 Provider Generation、旧 Attempt 证据集合摘要、投影语义摘要和操作 ID。`agent_run_recovery` 只返回当前请求头；在 `current_model_request_id` 暂时为空时只可选该步骤的 `READY` 头。`guard_model_request` 仅在同事务继承事实已写入时允许 `READY/NOT_APPLIED → ABANDONED`，延迟外键要求提交时后继存在；它禁止直接编辑请求的 Provider Generation 或抹掉旧 Attempt。

新 Agent 首先通过包内只读端口取前驱请求的冻结 Provider profile、`model.projection` 完整 PayloadRef、首次 `model.wire` 完整 PayloadRef、源码摘要和所有 Attempt 的收敛状态；学习候选留在私有 Request 行中随继承复制。候选 Provider 的 profile 来自当前 Graph 的 `provider.profile.bind`，其 `profile_ref`、模型、能力摘要、指令通道、输出预算与前驱完全一致；此首版不跨能力变更重新投影。Worker 从前驱不可变 Payload 读取字节；新投影只将 manifest 的 request ID 换成后继 ID，其他语义字段及其摘要保持相同，并以前驱投影为父边；新 wire 以新 Attempt ID 为 owner 保存相同字节与 digest，并以前驱 wire 和新投影为父边。不得重新读取流动的群 Context 或重新执行记忆召回来构造该逻辑步骤。Provider Generation 和 Effect audience 则必须是当前 Graph 的新代。

`agent.inherit-model-request` 是跨 `kernel.effect` 证明 participant 与 Agent 私有 participant 的 UnitOfWork。Effect participant 锁定并证明每个旧 Effect 已确定 `NOT_APPLIED`；Agent participant 在同一 Bot 数据库事务内锁 Run 与前驱 Request，复核 Run owner/epoch、当前步骤和请求头、旧 Generation 与新 Generation 不同、旧请求状态为 `READY`（零 Attempt）或 `NOT_APPLIED`、每个旧 Attempt 的 Agent observation 与 Effect participant 回执逐项一致、无 `DISPATCHING/UNKNOWN/SUCCEEDED`。Worker 从旧投影确定性生成新投影；Agent participant 再核对 wire 摘要不变、能力及 profile 相同、新 Payload provenance 正确，写不可变继承事实、前驱 `ABANDONED`、后继 `READY`，并推进 Run 请求头。稳定 operation ID 与输入摘要负责幂等合流；同 ID 不同输入冲突，事务失败不能留下已废弃前驱或孤立后继。若旧请求已经是 `UNKNOWN`，必须按原 Effect 查询/对账，不能继承；若已成功，继续消费成功结果，不能再调用 Provider。

发布前的 `agent.replacement-guard` 核对候选 Provider 预热后声明的 profile、模型与能力摘要，并检查 Agent 已收敛的未发送观察；两侧观察在原 `agent.observe-model-effect` UnitOfWork 中原子提交，发布后继承又由 Effect participant 在同一事务内复核。事实缺失或变化、证据缺失、`UNKNOWN` 或代次不变都拒绝发布并恢复旧图。发布后若冻结 Provider profile 与实际绑定不一致，或冻结投影/wire 已损坏，Agent 在仍持有 Run lease 时通过 `agent.block-frozen-model/1` 固定当前请求头、错误码和不可变 `BLOCKED_UNKNOWN` checkpoint；下次领取只返回诊断，不重建请求或派发 Effect。该阻断只处理确定性不兼容与载荷损坏；暂时性 I/O、数据库或继承事务失败仍由原 operation 重试或对账。同一 operation 并发继承合流、不同 operation 竞争排他，以及后继再次被继承后重放前笔操作均已在真实 PostgreSQL 中验证；提交后杀死 Worker、等待旧 HostLease/Run owner 自然到期再冷启动的隔离完整图用例也通过。首个新 Effect 失败及在线连续 Provider 热换代仍待验收；这些不能由单次整图热重载通过来替代。

若 Worker 在继承事务提交后、回执送达前崩溃，新 Host 必须先等旧 HostLease 到期；新 Agent 领取同一 Run 时，旧 Run owner 尚有效则返回 `WAIT_OWNER`，Conversation 以 30 秒间隔延期同一工作，不将其结算为失败或抢夺租约。旧 owner 自然到期后，Agent 从持久恢复查询读取已推进的后继请求头，只能用后继的冻结投影与 wire 继续；前驱 `ABANDONED` 不能再次派发。若冷启动形成又一个 Provider Generation，还需以当前后继为前驱再继承一代，请求 ID 变化而 wire 字节及摘要不变。`WAIT_OWNER` 是跨包执行回执，不是新的 Agent Run 状态。

活动 `agent.execute` 持有 300 秒 Run owner lease；Agent 包在同一次调用的任务所有权下每 60 秒调用 `agent.renew-run-lease`，逐次使用不同的稳定 operation ID，并始终复核原 owner ID/epoch。续租的服务端暂时性故障和 Worker UOW 网关的 `OVERLOADED`（429）在保守的 60 秒安全余量前用同一 operation ID 重试；明确失去 owner、其他输入/权限拒绝或无法在余量前续租时取消主执行，后续由持久 Effect 状态恢复，不能带过期 owner 继续交接。调用结束先取消并等待续租任务，再释放 Run lease。热重载停车开始主动释放 owner 时，续租任务停止；即使并发续租的迟到回执是 `STALE_AGENT_RUN_OWNER`，也不能取消正常的 `RELOAD_WAIT` 停车结果。

冻结投影或 wire 的精确 PayloadRef 无效、身份不符、正文缺失或已撤销、字节摘要不符，以及继承或 `NOT_APPLIED` 重试复制 wire 时同一 owner 内容冲突或父载荷不可用，均属于确定性载荷失败；Worker 只在调用 Payload 读写端口时将这些错误归入 `AGENT_FROZEN_PAYLOAD_INVALID` 的持久阻断，不把失效的冻结请求继续发给 Provider。`agent.inherit-model-request` 的 Effect 证明和 Agent 事务回执保持各自原始错误码，即使错误码恰好带有 `PAYLOAD_` 前缀，也不能由 Worker 推断为冻结载荷损坏。Payload authority 暂时不可用、分块协议暂时异常和继承事务失败仍向外传播以保留重试与对账语义。

解析失败只能提交 `agent.record-protocol-failure/1`。该端口在同一事务中验证当前成功 Attempt、写入不可变 failure 与 checkpoint、清除当前 Model 请求，并把 Run 推进到 `REPAIR_READY` 或 `FAILED`；第一次进入 `REPAIR_READY` 即持久消耗唯一 repair 配额并固定下一步号，后续候选不能再次申请修复。修复输入仅从 failure 引用的有界 feedback Payload 恢复，禁止容错解析、从错误 XML 猜字段或在 Provider 层重试协议。

解析成功只能提交 `agent.record-model-turn/1`。它接收包内解析器生成的规范块和受管 PayloadRef，在单个 Agent 事务中复核 `PARSING` 头、repair 来源、块顺序及 FunctionCall 的 trigger authority；随后一次性写入 ModelTurn，并在存在 FunctionCall 时同时写入 ToolCall 与真实 principal。Run 只能据此进入 `PREPARING_MODEL`、`WAIT_TOOL` 或 `SAFE_BOUNDARY`。宿主不能先保存“差不多合法”的回复，再补写工具或修改终止语义。

ToolCall 固定目标 component、Generation、service、契约摘要、完整 binding 摘要、审批要求和查询能力。审批等待只能经过 `agent.request-tool-approval/1` 与 `agent.inspect-tool-approval/1`，Effect handoff 与结果收敛只能经过对应的 manifest UnitOfWork；成功结果的 Payload owner 必须等于冻结目标组件，`UNKNOWN` 只有声明 `supports_query=true` 的 binding 才能通过 `RECONCILIATION` 收敛。终态 observation 在同一事务内清除当前 ToolCall 并让 Run 回到 `PREPARING_MODEL`，宿主和工具模块都不能直接改 Agent 表。

工具目录由各执行模块通过 `RuntimeModuleDescriptor.agent_tools` 贡献，不写死在 Agent 工厂或 Bot 组合根。贡献冻结工具名、模型声明、服务 ID/版本、审批要求和查询能力，并作为模块 descriptor digest 的一部分进入 RuntimeGraph；同一图中的工具名与服务键都必须唯一。`agent.runtime` 只能通过受限 factory context 枚举贡献并请求对应服务句柄，随后把贡献者的准确 component/Generation、service 和 contract digest 冻结为 `AgentToolBinding`。Contract Router sealing 必须确认该服务确实由声明贡献的模块提供；所有权不符时整张候选图拒绝发布。工具结果仍由执行模块拥有，Agent 只持有 PayloadRef，并统一经 Authorization 与 Effect Gateway 完成 handoff 和恢复。

工具执行不是 contribution directory 上的普通 `execute` 便捷调用。Agent Worker 只能提交可序列化的 `forest.agent-tool-command/1`，命令包含冻结 ToolCall、binding、authority trigger/principal、参数摘要和稳定 operation identity。`agent.prepare-tool-effect/1` 在同一事务内依次完成 Authorization 决策、`kernel.effect` command/wire 冻结和 Agent ToolCall 状态推进；三方输出必须指向同一个 executor component/Generation、service contract、binding digest、tool call 和 operation。需要审批时，`agent.request-tool-approval/1` 只写持久 approval intent 与不可变 Authorization observation 并进入 `WAIT_APPROVAL`，不会建立可交接 Effect。决定由 `agent.inspect-tool-approval/1` 在恢复轮次读取；批准后 `agent.prepare-approved-tool-effect/1` 原子消费 approval、签发 ticket、准备 Effect 和冻结 attempt，拒绝或过期则原子取消 ToolCall 并回到模型循环。

授权完成后，`agent.handoff-tool-effect/1` 在同一事务内把 Kernel Effect 与 Agent ToolCall 推进到 `DISPATCHING`。提交成功后，受限工具网关才可调用 RuntimeGraph 中已冻结的目标服务；网关必须复核 Effect permit、wire digest、目标 Generation、service/version、binding digest 和当前调用因果根，不能按工具名重新发现路由。工具服务接收 `forest.agent-tool-execute/1` DTO，返回自身拥有的 `tool.result` 精确 PayloadRef 或类型化失败；它不能取得 Agent Repository、授权对象或回调。

执行回执只能进入 `agent.observe-tool-effect/1`：`kernel.effect` 先冻结 executor evidence，Agent participant 在同一事务内复核 evidence 与当前 ToolCall 后收敛状态。handoff 后超时、断连或 Worker/Generation 切换一律进入 `UNKNOWN`；只有 manifest 声明 `supports_query=true` 时，才允许通过同一冻结服务的 `forest.agent-tool-query/1` 查询原 operation，并以 `RECONCILIATION` evidence 收敛。查询不是重发。目标 Generation 必须在已 handoff Effect 得出可证明终态之前保留恢复端口和资源租约；无法证明时 Run 保持 `BLOCKED_UNKNOWN`。

Agent 的工具目录 SDK 因此只有 `list`、`dispatch_prepared` 和 `query_handed_off` 三种能力。后两者都要求内核签发的精确 permit，不能接受裸工具名、临时 service handle 或 Worker 提供的“已授权”布尔值。旧 `KernelToolExecutionPort` 的进程内 callback、直接 Python `ToolDispatchResult` 和宿主工具映射已删除。`kernel.runtime` 自有的 `runtime_status` 工具也接受同一 `forest.agent-tool-execute/1` 命令，并返回 `forest.agent-tool-receipt/1` 与归属明确的 PayloadRef。

## 3. 单 Agent 执行循环

每个群的执行顺序固定如下：

1. Scheduler 的生命周期 Worker 领取群 actor，并在 `IDLE` 边界冻结首个 `InputBatch`；没有 trigger 时只做静默排空。
2. Worker 经内核 Contract Router 调用当前 RuntimeGraph 中冻结的 `agent.execute/2`。请求只携带稳定 Session/Run 引用、初始 InputBatch、恢复标记和 actor lease fence，不携带进程内回调。
3. Agent 建立或恢复同一个 Session/Run 并取得 Run owner。每次模型调用前以确定性的 step identity 执行 `agent.acquire-model-input/1`；若出现新 trigger，Conversation participant 只返回截至最后一个 trigger 的连续前缀，Memory participant 在同一事务内登记该批次的查询资格，Agent 再把结果追加到同一 Run。
4. Agent 用 Scheduler 返回的冻结 Context view 构造模型 snapshot。每个单元保留 `conversation_id`、平台、`channel_kind/channel_ref`、Binding、事件、作者快照和受管 payload 来源；不能从显示名或消息正文推断身份。
5. Agent 调用冻结的 Provider contract。FunctionCall 只进入同一 Run 的顺序工具步骤；工具通过当前 RuntimeGraph 的模块贡献目录解析准确 executor Generation，不创建另一 Agent。
6. 工具完成或明确失败后，状态机回到同一 Run 的下一次模型调用；`DISPATCHING` 结果丢失时只查询原 effect，无法查询则进入 `BLOCKED_UNKNOWN`。
7. 管理取消先写入绑定 `run_id`、期望 checkpoint、主体和原因的不可变 intent。模型/工具的最终 handoff 事务与 intent 检查共享 Run 锁，因此并发只可能确定地落为“取消阻止本次交接”或“交接已发生、先观察再取消”。取消 checkpoint 关闭 Agent Session；Conversation 侧再以幂等终态事实取消准确 trigger 并释放 lane，崩溃恢复不得将其误报为失败或重新回答。
8. 终止 ModelTurn 产生 `FinalCandidate`。Scheduler 生成 `ReplyCommitPlan` 并调用内核 UnitOfWork；内核解析三 participant，原子提交 Agent Run `COMPLETED`、正式 `REPLY`、trigger `ANSWERED` 与 CHAT SendIntent。
9. Scheduler 在 `SESSION_END` 边界选择后继：有新 trigger 就在同一 Session 建立唯一后继 Run；没有 trigger 就静默排空普通尾部并关闭 Session。

“Agent 正在工作时有新触发”只改变下一次模型调用的输入版本，不启动并发响应；模型流和未闭合工具组中途不修改请求。Agent 完成且没有新 trigger 时，静默排空剩余普通消息，不额外调用模型。

管理取消的竞态边界覆盖三种外部效果交接和最终回复提交：管理动作必须在锁定 Run 后校验期望 `checkpoint_seq`，用调用 operation ID 幂等写入唯一不可变意图，并由管理入口注入可信管理员主体。模型与工具 handoff、旧版 `agent.complete-run-record` 及正式 `agent.commit-final-run` 均在同一 Run 锁下检查意图；若完成事实先提交，取消动作只能拒绝，不能撤销已经发布的回复。意图先提交时，尚未交接的效果不得再 dispatch，最终回复不得再提交。已交接效果必须先走原 Effect 观察或查询端口收敛；`DISPATCHING/UNKNOWN` 没有权威结论时，取消保持待处理，不能把 Run 和 Session 伪装成 `CANCELLED`。尚未交接的 `PREPARED` Effect 应通过 `kernel.effect` 的受限 abandon participant 与 Agent 状态在同一 UnitOfWork 收敛。只有消除所有未决外部效果后，Agent 包才能持 owner fence 写入取消 checkpoint、原子关闭 Run/Session；Conversation 再按准确领取组取消 trigger 并释放 lane。重启后的同一意图继续这一状态机，不创建新的 Agent 或外部效果。

## 4. 冻结 lane 身份与回复路由

Conversation lane 必须把以下身份作为创建后不可变事实保存：

- `source_component_id`
- `binding_ref`
- `platform`
- `channel_kind`
- `channel_ref`
- `thread_ref`

Context snapshot 与回复计划都从 Scheduler 端口读取这些字段。不得用 `conversation_id` 前缀猜测群聊/私聊，也不得查询 Adapter 私有 Binding 表拼接路由。`FinalReplyPlan.binding_ref/channel_kind/channel_ref` 必须与当前 actor 的冻结 lane 身份一致，Delivery Generation 来自该调用冻结的 RuntimeGraph。

## 5. CorePrompt、Preset 与运行配置

CorePrompt 是 `agent.runtime` 包内版本化协议，随 Agent spec 和 parser revision 冻结。PresetContext 是独立受管 payload，由 Agent 配置引用；它不能覆盖 CorePrompt 的协议规则，也不是 CorePrompt 的可编辑副本。每个 Run 固定：

- Agent spec revision 与配置 payload；
- PresetContext revision 与 payload；
- Provider Generation 与单次 capability snapshot；
- 工具声明及准确 component/Generation/service/contract digest；
- deadline、步数、输出 token 与费用上限。

配置热重载只影响新 Agent Generation 接纳的新 Session。活动 Session 按已冻结引用完成，或在安全边界进入明确的 reload wait；不能在 Run 中途混用新旧 preset、Provider 或工具目录。

## 6. UnitOfWork 与恢复

语义召回和证据投影的步骤身份、等待及恢复边界见 [16 持久语义召回与 Agent 接入](16-semantic-recall-execution.md)。Agent 拥有按模型能力选择输入内容与冻结请求正文的投影，Provider 只验证并传输，不决定是否删除图片或裁剪文本。

正式回复 participant 固定为 `agent.runtime + conversation.scheduler + delivery`；Session 交接 participant 固定为 `agent.runtime + conversation.scheduler`。内核协调器只持有注册表解析出的公开 participant 端口和 Bot transaction handle，不得取得任一模块 Repository。Scheduler 只提交计划，不保存 participant、其他模块实例或共享模块字典；同一计划的恢复由稳定 operation ID 重新进入内核协调器。

| 持久状态 | 恢复动作 |
|---|---|
| `PREPARING_MODEL` | 在下一次模型调用前重新检查安全点，使用确定性 step identity 继续 |
| `REPAIR_READY` | 读取不可变协议失败事实和有界原候选，以 `request_kind=REPAIR` 发起唯一协议重写调用；只保留原输入 fence 用于审计，不读取或投影 InputBatch、ContextView、Preset、Memory、媒体，不接入新群消息，也不重建第二条 Agent 链 |
| 模型 `DISPATCHING` | Provider 没有可靠查询时记 `UNKNOWN` 并阻断，不能重发 |
| 工具 `PROPOSED/WAIT_APPROVAL` | 用原 tool call、authority trigger、principal 与 approval ref 继续授权流程 |
| 工具 `DISPATCHING` | 只通过工具 contract 的 query 端口查询原 effect；不支持查询则阻断 |
| `FINAL_READY` | 用稳定计划 ID 重试三 participant UnitOfWork |
| `COMPLETED` 且 Session 活跃 | 跳过模型和回复提交，只执行幂等 Session 交接 |
| `FAILED/AGENT_PROTOCOL_FAILED` | Conversation 以准确 Session/Run fence 终结该 Run 已领取的 trigger，保存失败成员与原因并释放 lane；重启后重放同一失败 operation |

Scheduler quiesce 先关闭新 actor admission，再等待已领取群离开模型/工具 handoff 临界区并到达 checkpoint。不能通过取消协程把已 handoff 的外部调用伪装成未执行；结果必须由 Provider/Effect/Delivery 账本决定。

群聊 `core reload` 不在 Shell Worker 内同步调用图替换。`kernel.runtime` 先以 operation、请求主体和 expected graph revision 写入持久请求；宿主监督循环另行领取并把稳定 activation ID 交给候选图。这样旧 Shell 可以先提交回执，再进入正常的消费者优先排空。宿主在发布后、完成请求前崩溃时，恢复器只依据同一 activation ID 的 RuntimeGraph 事实收敛成功；只有 PREPARED 的旧宿主候选不会跨 HostLease 冒充可继续发布的图。

### 6.1 Agent 管理读模型

会话、Run、模型请求/回合和工具调用均归 `agent.runtime` 的私有 Repository；WebUI 只能通过该包当前 Generation 的管理查询读取，不能跨 schema 拼接 Conversation 或 Provider 表。Run 将 `conversation_ref` 作为不可变身份存储，并用 `(bot_id,session_id,conversation_ref)` 外键锁定所属 Session，避免跨群管理视图依赖运行中的 lane 或从字符串前缀猜测归属。全 Bot 与单会话列表分别有按 `(created_at DESC,run_id DESC)` 的索引，状态筛选另有对应索引；单页最多 100 条。游标绑定会话、状态与首次查询时间，后续页只扫描该创建时间界限内更早的 Run。Run 的状态会继续变化，这个列表是活动诊断视图而不是跨 HTTP 请求冻结的 MVCC 历史快照；详情查询始终读取当前持久状态。列表只返回执行元数据，不返回 Prompt、模型正文、工具参数或 Payload 内容。

模块自带紧凑管理 UI。`agent.sessions.page` 按 Bot、群聊及 Session 状态有界分页，点击 Session 后通过准确 `session_id` 筛选 `agent.runs.page`，`agent.run` 返回当前 Run 状态。Session 与 Run 分别以创建时间及身份 ID 为键集游标，游标绑定筛选和首次查询时间，单页最多 100 条。`agent.steps.page` 在单个 Run 内按 step 序号分别分页读取 ModelTurn 或 ToolCall，可按工具状态筛选；游标绑定 Run、记录种类、筛选条件和首次查询时间。ModelTurn 通过 `(bot_id,run_id,request_id,step_seq)` 外键指向同一 Run 的 ModelRequest，ToolCall 通过 `(bot_id,run_id,turn_id,step_seq)` 外键指向同一 Run 的 ModelTurn，因此归因由数据库约束而不是管理层推断。列表只暴露时序和状态元数据，不暴露模型正文或工具参数；可变状态仍是当前诊断值，不承诺跨页冻结。取消及 UNKNOWN 对账必须继续在同一包管理契约内实现，不能在壳层新增 Agent 专用路由或读取此列表的内部表。

## 7. 实现拒绝项

- 新增聊天业务 `contract.router`，或让内核 Contract Router 理解 Prompt、模板、群策略和 Agent 选择；
- 在 Supervisor、Bot 聚合对象或 Web 层构造 ConversationWorker、Agent、Provider 或 Delivery；
- Scheduler/Agent 直接持有对方 Repository，或跨 schema 查询对方表；
- 用 `conversation_id` 字符串推导 Binding、群类型或发送目标；
- 把下一次模型调用、工具调用、恢复任务或并行群执行称为或实现成子 Agent；
- 在回复、Session 交接、工具 handoff 中绕过 Generation、actor/run owner 或 UnitOfWork guard。
