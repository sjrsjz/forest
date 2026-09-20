# 单 Agent 生产执行与会话协调

> **权威性：生产执行边界详细设计。** 本文展开 [00 架构总契约](00-architecture-contract.md)、[08 群聊上下文](08-group-context-interaction.md)与[13 微内核运行时](13-microkernel-control-plane-redesign.md)。Agent 始终是一条 QiYu Agentlib 风格的 Session/Run 状态机；“下一轮”是同一 Run 中的下一次模型调用，不创建产品子 Agent。

## 1. 所有权与调用边界

生产链由现有模块和微内核机制协作完成，不增加拥有第二套聊天状态的“Agent 编排模块”。

| 主体 | 拥有 | 提供/调用 | 明确不拥有 |
|---|---|---|---|
| `conversation.scheduler` | Inbox、trigger、InputBatch、Context、群 actor lease、ConversationWorker 生命周期任务 | 冻结输入切口；经内核 Contract Router 调用 `agent.execute/1`；向内核 UnitOfWork 提交回复或 Session 交接计划 | ModelTurn、工具状态、Provider 客户端、Delivery 状态、其他模块 participant 对象 |
| `agent.runtime` | Session、Run、CorePrompt 协议、Preset 引用、ModelTurn、ToolCall、模型 attempt 与 checkpoint | `agent.execute/1`；在每次模型调用前调用 Scheduler 提供的安全点回调；产出 `FinalCandidate` | Inbox 游标、群 actor、正式 REPLY、SendIntent |
| `provider.*` / `tool.*` | 各自客户端、能力快照、attempt/effect 观察 | 被 Agent 通过版本化 contract 调用 | Conversation 与 Agent 状态机 |
| `delivery` | SendIntent、part、attempt、发送回执与 UNKNOWN | `delivery.intent/1` participant；异步发送已提交意图 | 回复内容决策、Agent Run |
| 微内核 Contract Router | RuntimeGraph 中的 contract → component Generation 解析、版本/代际/deadline/admission 校验 | 调用冻结的公开端口 | 聊天策略、Agent 选择、Prompt、Context 或执行循环 |
| 微内核 UnitOfWork 协调器 | 一次事务的 participant 集与提交 guard | 调用 participant 的同连接事务端口 | participant 的领域事实与私有 Repository |

旧实现中名为 `contract.router`、同时选择 Agent、调用模板 responder 并驱动聊天的业务模块属于错误抽象，应直接删除。内核 Contract Router 不能被实现成 Bot 业务模块，也不能因历史类名相同而接管 Agent 循环。

## 2. 组件设计门

### 2.1 `conversation.scheduler`

| 项目 | 契约 |
|---|---|
| owner / namespace | 仅拥有会话连续序、分类水位、trigger、InputBatch、Context、actor lease 和 Worker 任务；写 `conversation_scheduler` 私有 namespace |
| provides | `conversation.ingress/1`、`conversation.input/1`、`conversation.context/1`、`conversation.reply-participant/1`、`conversation.session-participant/1` |
| requires | 内核 lifecycle/Contract Router/UnitOfWork，`message.router` 的冻结分类结果，`agent.execute/1`；Scheduler 不直接依赖或持有 `delivery` 运行实例 |
| Generation fence | 本 Generation 控制 Worker admission；每次领取、选批次、追加 Context 和最终提交都校验 actor epoch 与 member generation |
| concurrency | 数据库 actor lease 保证同群单链；有界 semaphore 只控制跨群并发，进程锁不构成权威 |
| recovery | 从 lane 的 active Session/Run 引用恢复；完成 Run 只继续交接；过期 actor 不能提交 |

### 2.2 `agent.runtime`

| 项目 | 契约 |
|---|---|
| owner / namespace | 仅拥有单 Agent Session/Run、模型轮次、工具步骤、attempt、checkpoint 与 FinalCandidate；写 `agent_runtime` 私有 namespace |
| provides | `agent.execute/1`、`agent.run-query/1`、`agent.finalize-participant/1` |
| requires | Provider、工具、Authorization、Effect Gateway、Payload Authority；安全点和 Context snapshot 由调用请求提供的受限回调取得 |
| Generation fence | Run 冻结 Agent spec、Preset、Provider/tool contract 引用；每次外部 handoff 和事务 participant 调用复核 member generation 与 Run owner epoch |
| concurrency | 同一 Run 只有一个 owner epoch；模型调用和工具因果组顺序闭合，不产生分支 Agent |
| recovery | 从持久 checkpoint 继续；不可查询的已派发模型/工具进入 `UNKNOWN/BLOCKED_UNKNOWN`，不能换 operation 重发 |

两个模块只交换类型化 DTO、稳定引用和受管 PayloadRef。Scheduler 不持有 Agent Repository 或运行实例；Agent 不持有 Scheduler Repository 或运行实例。Contract Router 句柄不是 Python bound method，必须冻结目标 Generation、契约版本和 admission fence。跨模块原子提交由内核 UnitOfWork 根据计划解析已注册 participant；不能把 participant 对象注入 Scheduler，也不能由任一业务模块直接写其他 namespace。

工具目录由各执行模块通过 `RuntimeModuleDescriptor.agent_tools` 贡献，不写死在 Agent 工厂或 Bot 组合根。贡献冻结工具名、模型声明、服务 ID/版本、审批要求和查询能力，并作为模块 descriptor digest 的一部分进入 RuntimeGraph；同一图中的工具名与服务键都必须唯一。`agent.runtime` 只能通过受限 factory context 枚举贡献并请求对应服务句柄，随后把贡献者的准确 component/Generation、service 和 contract digest 冻结为 `AgentToolBinding`。Contract Router sealing 必须确认该服务确实由声明贡献的模块提供；所有权不符时整张候选图拒绝发布。工具结果仍由执行模块拥有，Agent 只持有 PayloadRef，并统一经 Authorization 与 Effect Gateway 完成 handoff 和恢复。

## 3. 单 Agent 执行循环

每个群的执行顺序固定如下：

1. Scheduler 的生命周期 Worker 领取群 actor，并在 `IDLE` 边界冻结首个 `InputBatch`；没有 trigger 时只做静默排空。
2. Worker 经内核 Contract Router 调用当前 RuntimeGraph 中冻结的 `agent.execute/1`。请求携带稳定 Session/Run 引用、初始 InputBatch 引用、Context snapshot 回调和下一模型安全点回调。
3. Agent 建立或恢复同一个 Session/Run 并取得 Run owner。每次模型调用前调用安全点回调；若出现新 trigger，Scheduler 只返回截至最后一个 trigger 的连续前缀，Agent 把该 InputBatch 追加到同一 Run。
4. Agent 用 Scheduler 返回的冻结 Context view 构造模型 snapshot。每个单元保留 `conversation_id`、平台、`channel_kind/channel_ref`、Binding、事件、作者快照和受管 payload 来源；不能从显示名或消息正文推断身份。
5. Agent 调用冻结的 Provider contract。FunctionCall 只进入同一 Run 的顺序工具步骤；工具通过当前 RuntimeGraph 的模块贡献目录解析准确 executor Generation，不创建另一 Agent。
6. 工具完成或明确失败后，状态机回到同一 Run 的下一次模型调用；`DISPATCHING` 结果丢失时只查询原 effect，无法查询则进入 `BLOCKED_UNKNOWN`。
7. 管理取消先写入绑定 `run_id`、期望 checkpoint、主体和原因的不可变 intent。模型/工具的最终 handoff 事务与 intent 检查共享 Run 锁，因此并发只可能确定地落为“取消阻止本次交接”或“交接已发生、先观察再取消”。取消 checkpoint 关闭 Agent Session；Conversation 侧再以幂等终态事实取消准确 trigger 并释放 lane，崩溃恢复不得将其误报为失败或重新回答。
8. 终止 ModelTurn 产生 `FinalCandidate`。Scheduler 生成 `ReplyCommitPlan` 并调用内核 UnitOfWork；内核解析三 participant，原子提交 Agent Run `COMPLETED`、正式 `REPLY`、trigger `ANSWERED` 与 CHAT SendIntent。
9. Scheduler 在 `SESSION_END` 边界选择后继：有新 trigger 就在同一 Session 建立唯一后继 Run；没有 trigger 就静默排空普通尾部并关闭 Session。

“Agent 正在工作时有新触发”只改变下一次模型调用的输入版本，不启动并发响应；模型流和未闭合工具组中途不修改请求。Agent 完成且没有新 trigger 时，静默排空剩余普通消息，不额外调用模型。

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

正式回复 participant 固定为 `agent.runtime + conversation.scheduler + delivery`；Session 交接 participant 固定为 `agent.runtime + conversation.scheduler`。内核协调器只持有注册表解析出的公开 participant 端口和 Bot transaction handle，不得取得任一模块 Repository。Scheduler 只提交计划，不保存 participant、其他模块实例或共享模块字典；同一计划的恢复由稳定 operation ID 重新进入内核协调器。

| 持久状态 | 恢复动作 |
|---|---|
| `PREPARING_MODEL` | 在下一次模型调用前重新检查安全点，使用确定性 step identity 继续 |
| `REPAIR_READY` | 读取不可变协议失败事实和有界反馈，保持原 InputBatch/ContextView fence，以 `request_kind=REPAIR` 发起唯一修复调用；不得接入新群消息或重建第二条 Agent 链 |
| 模型 `DISPATCHING` | Provider 没有可靠查询时记 `UNKNOWN` 并阻断，不能重发 |
| 工具 `PROPOSED/WAIT_APPROVAL` | 用原 tool call、authority trigger、principal 与 approval ref 继续授权流程 |
| 工具 `DISPATCHING` | 只通过工具 contract 的 query 端口查询原 effect；不支持查询则阻断 |
| `FINAL_READY` | 用稳定计划 ID 重试三 participant UnitOfWork |
| `COMPLETED` 且 Session 活跃 | 跳过模型和回复提交，只执行幂等 Session 交接 |
| `FAILED/AGENT_PROTOCOL_FAILED` | Conversation 以准确 Session/Run fence 终结该 Run 已领取的 trigger，保存失败成员与原因并释放 lane；重启后重放同一失败 operation |

Scheduler quiesce 先关闭新 actor admission，再等待已领取群离开模型/工具 handoff 临界区并到达 checkpoint。不能通过取消协程把已 handoff 的外部调用伪装成未执行；结果必须由 Provider/Effect/Delivery 账本决定。

## 7. 实现拒绝项

- 新增聊天业务 `contract.router`，或让内核 Contract Router 理解 Prompt、模板、群策略和 Agent 选择；
- 在 Supervisor、Bot 聚合对象或 Web 层构造 ConversationWorker、Agent、Provider 或 Delivery；
- Scheduler/Agent 直接持有对方 Repository，或跨 schema 查询对方表；
- 用 `conversation_id` 字符串推导 Binding、群类型或发送目标；
- 把下一次模型调用、工具调用、恢复任务或并行群执行称为或实现成子 Agent；
- 在回复、Session 交接、工具 handoff 中绕过 Generation、actor/run owner 或 UnitOfWork guard。
