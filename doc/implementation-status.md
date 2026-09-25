# Forest 实现状态

> 本页只记录当前工作树事实，不定义目标架构。架构约束以 [00 架构总契约](00-architecture-contract.md)、[13 微内核控制面重构](13-microkernel-control-plane-redesign.md)和[15 单 Agent 生产执行](15-single-agent-production-execution.md)为准。当前生产装配已切为微内核包图；Memory 完整主链和长期运行验收尚未完成，因此 Forest 仍不可发布。

## 1. 判定标准

只有同时满足以下条件的组件才记为“已包化”：

- 组件由 `src/forest/builtin/<package>/module.toml` 描述，并从不可变 `PackageSnapshot` 构建；
- 业务代码只在隔离 `python-worker` 中执行；
- 对外调用只经过 manifest 声明的版本化 Service、Domain、UnitOfWork、Effect 或贡献目录端口；
- 私有状态只由包声明的 storage baseline 和 participant/query/command 访问；
- 配置、Worker、Effect Driver、管理贡献和资源属于同一个 Generation 生命周期；
- 生产组合根只调用 `bootstrap_builtin_package()`，不存在同组件的宿主 Runtime 或 Repository 旁路。

目录名位于 `builtin`、Python import 路径中包含 `builtin`、或者有独立测试，均不能单独证明组件已经包化。

## 2. 已工作的微内核与包运行时

以下机制已经进入通用运行时，而不是某个业务模块的特例：

- 不可变包构建、manifest/schema 校验和包摘要；
- 隔离 Python Worker 与有界 RPC、任务、资源和队列；
- RuntimeGraph 中精确到 component/Generation/contract 的路由；
- 候选代预热、原子发布、旧代冻结、排空、失败回滚和资源回收；
- 包私有 Domain Query/Command、manifest UnitOfWork、durable owned work；
- 包快照只在 descriptor 编译时完整捕获和校验一次，所有契约 schema 从该次不可变捕获批量编译；schema 数量不会造成重复遍历包内容的二次方启动开销；
- Generation 专属 Effect Driver，Secret、socket、子进程和远端客户端不进入 Worker；
- 包自带管理 UI 与配置贡献；
- Job、Shell 和 Routing 三类冻结贡献目录。

这些能力构成 builtin 模块系统本身。业务组件只有经过这些端口运行，才算使用了该系统。

包 manifest 现在同时支持消费 `agent-tools` 冻结目录和通过 `components.agent_tools` 发布工具。发布项必须静态绑定本包声明的 service major 与不可变工具声明，随后进入 descriptor digest、RuntimeGraph 唯一路由和 Generation 动态依赖；宿主不能再替 builtin 模块注入工具贡献。冻结目录提供 `dispatch-prepared` 与 `query_handed_off` 两个不同端口：前者逐项复核 binding、目标 component/Generation/service、Kernel Effect `DISPATCHING` attempt、permit 和规范 command digest 后才首次调用包 service；后者只允许 `supports_query=true` 的同一冻结 binding，在 Effect 已经 `DISPATCHING/UNKNOWN` 后查询原 operation，并以 `RECONCILIATION` evidence 收敛，不能重发首次命令。Agent 侧已有工具 prepare/handoff/observe UoW，Memory 精确召回是首个完整实现执行与查询协议的 builtin 工具。审批工具通过 Kernel Authorization participant 创建、检查和消费持久 approval；请求者、Bot 审批域和策略版本均由内核从调用 Generation 推导。

## 3. 当前生产装配

`CleanBotHost._assemble_registry()` 当前直接以真实 builtin 包装入下列业务组件：

| 组件 | builtin 包 | 当前边界 |
|---|---|---|
| Provider | `provider_chat_completions` | Worker 拥有 profile；同代 materialize/dispatch driver 通过 PayloadRef 生成模型投影、读取媒体并执行 HTTP；响应正文和 usage 由 Provider Generation 保存，dispatch/Effect/Agent observation 只传精确引用；Secret 不进入 Worker |
| 多模态 Embedding | `embedding_multimodal` | Worker 拥有 Bot 生命周期冻结的 VL profile 和图文请求物化；同代 Effect Driver 校验固定请求与 payload 引用后执行网络派发 |
| 向量索引 | `vector_index_milvus` | Milvus 只保存派生向量；Bot 库保存 ref 与权威来源 |
| Media | `media_capture` | Worker/owned work 管理抓取状态；Effect Driver 管理外部下载 |
| Routing | `routing_graph` | 路由事实和投递工作由包私有 SQL 与 Worker 管理 |
| Trigger policy | `trigger_policy` | 群/私聊触发判定由包服务提供 |
| OneBot codec | `onebot_codec` | 纯规范化与发送投影服务 |
| Job scheduler | `job_scheduler` | 持久任务与 Job 贡献目录 |
| Process executor | `process_executor` | Worker 投影受限 profile；Effect Driver 执行最小 native capability |
| Delivery | `delivery` | SendIntent、Effect handoff、结果收敛与管理 UI |
| Group shell | `shell_command` | P0/P1/P2 授权、审计、持久执行与 Shell 贡献目录 |
| OneBot adapter | `adapter_onebot` | Binding、入站投影与发送语义在 Worker；WebSocket 在 Generation Driver |
| Agent | `agent_runtime` | CorePrompt、模型/工具循环、审批与 reload 恢复均在隔离 Worker 和包私有 SQL 内 |
| Conversation | `conversation_scheduler` | 同群串行、跨群并发、安全边界合流、静默归档和上下文裁剪 |
| Memory | `memory_graph` | 多主干来源、OneBot 多模态捕获和精确来源召回；图与语义召回仍待补齐 |

内核的 Runtime、Payload、Authorization、Effect 与 Audit Authority 仍是可信微内核组件。它们不是可卸载业务插件，也不应迁入 `builtin`。

## 4. 已切换的生产 builtin

生产组合根已统一通过 `bootstrap_builtin_package()` 装载以下组件。Host 只装配配置、能力授权、效果驱动和冻结契约句柄，不再构造这些组件的 Repository 或业务 Runtime：

| 组件 | 生产包 | 当前状态 | 剩余工作 |
|---|---|---|---|
| Agent | `builtin/agent_runtime` | 隔离 Worker、私有 SQL、manifest UoW 与 owned-work 已接入生产图 | 删除未引用的旧宿主实现；继续补齐可观测管理面 |
| Conversation | `builtin/conversation_scheduler` | 入站、分类、静默归档、Agent 调度与安全边界合流已接入生产图 | 扩充管理查询与长时间压力验证 |
| Memory | `builtin/memory_graph` | 多模态来源、归档资格、精确召回、持久 facet 准备与域绑定已运行；排他批次 claim SQL 与预算事务已验证但尚无派发 Worker 调用 | facet 派发与向量 ref、图扩散、跨主干联想、遗忘和完整管理面 |

OneBot Adapter 现在按消息形态调用 manifest 声明的四条入站 UnitOfWork 计划。Conversation、Memory/Media、Shell 与分类 participant 在同一事务计划内提交；Host 不再提供 `kernel.uow.onebot.ingress` 业务服务。

`clean_*` 是重构期间遗留的宿主实现名称，不是新的模块层。对应 builtin 完成生产切换后，应直接删除这些文件和旧 baseline，不保留兼容转发。

## 5. Agent 生产包已经拥有的部分

`builtin/agent_runtime` 已拥有唯一 CorePrompt 与 XML 协议解析器，并以私有 SQL 和 manifest UoW 封闭以下状态转换：

- Session/Run 创建、输入追加、owner lease、释放和热重载停车/恢复；
- `agent.acquire-model-input/1` 由 manifest 固定 Conversation、Kernel Payload、Memory 与 Agent 四个 participant，在同一 PostgreSQL 事务内完成模型安全点消息切口、删除 epoch 冻结、Memory 资格确认和 Agent Input revision 推进；任一步拒绝都会整体回滚；
- `agent.start-from-conversation/1` 使用相同四方 participant，在初始 `IDLE` 边界把 trigger 选择、删除 epoch、Memory 资格确认与首个 Agent Run 建立合并为一个事务；Conversation 不再需要先提交 InputBatch 再用 callback 启动宿主 Agent，选择与建 Run 之间不存在可见裂缝；
- Provider Generation 与模型投影先冻结；授权、Kernel Effect permit、wire Payload 和 Agent Attempt 由 `agent.prepare-model-effect/1` 原子建立；其中 `kernel.effect` 的 `effect.prepare-command` participant 在协调事务内校验授权票据、父 Payload 与 Generation fence，并以自身身份写入 `effect.input`/`effect.wire`，Agent Worker 只提交声明式 command，不能伪造内核 Payload owner；该 UoW 的最终结果保留完整 authorization ticket、input/wire PayloadRef 和 permit，后续 handoff 不从宿主取旁路凭据，也不重新猜测 Effect 事实；
- `agent.handoff-model-effect/1` 原子推进 Effect 与 Agent handoff；`agent.observe-model-effect/1` 通过 `effect.observe-command-result` 让内核校验 Provider 结果引用并以自身身份冻结 `effect.evidence`，Agent participant 同时核对 evidence 摘要、父引用集合与观察状态，随后原子收敛两本账及 `UNKNOWN` 对账；
- 错误 XML 的一次协议修复、合法 ModelTurn 提交；
- ToolCall 的准确 component/Generation/service/contract/binding 冻结；非审批工具由 `agent.prepare-tool-effect/1` 在同一事务内建立 Authorization ticket、Kernel Effect permit 和 Agent 侧 wire/attempt 事实，再由 `agent.handoff-tool-effect/1` 原子进入 `DISPATCHING`；目标只能通过冻结工具目录的 `dispatch-prepared` 端口调用；
- 审批工具由 `agent.request-tool-approval/1` 原子创建 Kernel approval 并把 ToolCall/Run 推进到 `WAIT_APPROVAL`；后续执行通过 `agent.inspect-tool-approval/1` 读取持久决定。拒绝或过期写入不可变 Authorization observation 并回到模型循环；批准则由 `agent.prepare-approved-tool-effect/1` 在同一事务内消费 approval、签发准确 ticket、准备 Kernel Effect 并冻结 Agent attempt，任何一步失败都会整体回滚；
- `agent.observe-tool-effect/1` 让 Kernel Effect 冻结 executor receipt 后，由 Agent participant 在同一事务内复核 evidence、父结果引用和冻结 attempt，再收敛 ToolCall；旧的裸 `agent.observe-tool-result` 计划已撤出 Worker capability，不能绕过 Effect 账本提交派发结果；
- terminal ModelTurn 先停在 `SAFE_BOUNDARY`；确认没有新触发批次后，Worker 以最新 terminal turn 的准确 `assistant.candidate` 为父边生成独立 `final.reply`，`agent.finalize-candidate/1` 同时核对来源与正式候选并原子推进到 `FINAL_READY`；内部解析候选不会直接交给 Delivery；
- 最终完成、同 Session 后继 Run 和 Session 关闭的私有 participant。

包内已加入 `model_execution.py`，模型 attempt 的 profile bind、`model.input` 写入、Provider materialize、授权与 Effect prepare、原子 handoff、Provider dispatch、Effect/Agent 原子 observe 及 Provider 结果读取均只经过 `ctx.services`、`ctx.uow`、`ctx.payloads` 与冻结的 `ctx.identity`。它没有 Repository、数据库连接或宿主 Agent adapter；Provider 异常或回执 fence 不一致在 handoff 后统一收敛为 `UNKNOWN`，不会盲目重发。

Worker 已公开 `agent.execute/2`：输入只含 Conversation/Session/Run 引用、恢复标记、绝对期限和 actor fence；首批选择、Run lease、每次模型调用前的输入合流、Context Payload 树读取、Provider Effect、严格 ModelTurn 解析、唯一一次协议修复、非审批 ToolCall 的 Effect 执行以及 `final.reply` 生成均在隔离包内完成。恢复查询现在返回完整 Payload digest/owner fence，Worker 不再按裸 Payload ID 读取宿主存储。

Agent 已进入生产组合根。支持查询的 ToolCall 能在 handoff 后通过冻结目录的 `query_handed_off` 查询原 operation，并用 `RECONCILIATION` 收敛；不支持查询的工具保持 `BLOCKED_UNKNOWN`，不会盲目重发。审批决定不触发宿主 callback；Conversation 的持久 defer deadline 负责重新调度，Agent 在恢复执行时检查审批事实。`agent.reload-resume.pending` 负责热重载后的持久恢复，Host 不解释 Agent 状态。

包运行时现已具备能力受控的分块 Payload 端口：`payload.read` 只接受完整 owner/digest 引用并在 Worker 端复核最终摘要，`payload.write` 强制当前组件所有权、连续 offset、声明长度/摘要与精确父引用；提交结果由 SDK 强制收敛为五字段 PayloadRef，传输元数据不能混入跨模块引用。单代暂存最多 32 MiB，并随调用和 Generation 回收。该端口解决了长期 Context、模型输入和图片不能穿过 256 KiB 控制帧的问题，是把 Agent、Memory、Conversation 执行逻辑移入 Worker 的基础设施。它本身不代表三个模块已完成切换。

Provider 的 `provider.model.dispatch/2` 已移除内联模型正文：效果驱动在所属 Generation 下原子写入 `model.result`/`model.usage`，服务回执与 Kernel Effect evidence 只持有精确引用；旧宿主 Agent 过渡执行器和新的包内 model attempt 都只能把这些引用交给 Agent observation，不能再以 Provider 身份复制载荷。Effect/Agent 私有 schema 已用独立 PostgreSQL 重建验证；完整 Agent 执行循环仍待接入 Worker 公共服务。

## 6. Conversation 生产包已经拥有的部分

`builtin/conversation_scheduler` 已封闭消息接纳、连续分类水位、截至最后 trigger 的输入选择、带作者来源的 Context、无 LLM 摘要的窗口裁剪、静默排空和 `conversation.context.snapshot/2`。快照不再把完整历史塞进 Worker 帧：权威查询最多返回 32 个活动单元，Worker 写入带正文/作者父边的 `context.snapshot.page`，超过 128 页时构造 `context.snapshot.index` 树，服务只返回最终 manifest 的精确 PayloadRef。同群串行和跨群并行依靠数据库 actor lease，而不是进程锁。

数据库租约驱动的 `conversation.agent-run.pending` 已进入生产组合根：同一群聊只允许一个 actor claim，跨群可由多个槽并行；Worker 通过 `agent.execute/2` 驱动 Run，在模型安全边界合流，随后用事务计划原子提交 Agent 完成态、Conversation 回复单元与 Delivery Intent，再在 Session 边界选择后继输入并联动 Memory 资格确认。Agent 的 `FAILED/CANCELLED` 终态由独立事务计划核对精确 trigger 集、收敛 trigger 状态并释放 Session；`WAIT_APPROVAL` 依靠持久 defer deadline 轮询准确 approval，不需要宿主监听器。

生产组合根显式给 `agent.runtime` 的 grant set 授予 Bot 级 `provider.invoke`，给 `memory.graph` 授予 Bot 级 `embedding.invoke`；两者的 Kernel Effect 授权票据仍须逐次绑定准确输入摘要、目标 Generation 与期限，服务调用能力本身不代替外部副作用授权。整图启动/重启测试核验 grant 落库。

## 7. Memory 生产包已经拥有的部分

`builtin/memory_graph` 已经是可由 `PackageCatalog` 构建的独立包，声明自己的隔离 Worker、私有 storage baseline、Generation 配置、管理 UI、来源捕获与输入资格 UnitOfWork participant，以及 `memory_recall_exact` Agent 工具。嵌入配置将 `TEXT_IMAGE` 模型种类、部署标识、模型修订、维数与归一化方式绑定到 Bot 模块生命周期；端点轮换仍应由后续受限 Effect 配置承担，不能在已创建 Bot 上改换模型种类。

消息来源捕获已由 `memory.capture-source` manifest UnitOfWork 封闭：Kernel Payload 删除 epoch、Conversation 已接纳的 event/ingress sequence、群来源身份、作者引用、多模态 part 与 occurrence 在同一事务校验和提交；来源、unit 与 occurrence 都有独立并发围栏和幂等账本。Media Worker 可经 `memory.source.capture` 使用同一 participant，抓取失败则经 `memory.source.unavailable` 留下持久不可用事实，避免 Agent 选择先于异步抓图完成时永久等待。输入资格 participant 会在捕获先到时原子提升 occurrence/unit，在选择先到时登记 `PENDING_CAPTURE` 并由后续捕获收敛；两种顺序均不需要宿主 Repository 回调。该路径已用真实 PostgreSQL 安装包 baseline 并验证捕获、资格转换、精确召回与 Registry 资源回收。

资格边界已进一步拆分：`memory.qualify-input-batch` 现在只将 occurrence 标为 `ADMITTED`，不会让仍在活动 Context 的 source unit 进入长期召回。静默裁剪的 `conversation.commit-silent-input` 在同一 UnitOfWork 提交 Context 归档、捕获 Payload 删除 epoch，再调用 `memory.qualify-archive`；只有已提交的 compaction 成员才能把 occurrence/unit 提升为 `ELIGIBLE`。归档先于异步来源到达时留下 `pending_archive_events`，捕获与确定不可用会按同一事件围栏解析它。新 Session 首个 Run 在 Agent 选择输入前先由 Conversation 包执行 `conversation.compact-before-run`，归档及 Memory 资格仍在同一事务中提交；Run 中途不更换已经冻结的视图。后续 Run 的交接裁剪、有界分块和长尾维护唤醒仍待完成。

精确召回不再调用宿主 `builtin_memory_tool.py`：工具声明由 Memory manifest 发布并静态绑定本包 service，隔离 Worker 经只读 Domain Query 获取最多八个归档稳定 source unit，只有 ACTIVE 来源、`ELIGIBLE` unit 和完整 part 集可见。Worker 按精确 PayloadRef 读取文本、保留图片引用，结果明确携带 source/trunk/group/author provenance，并以原始 part 为父边写入 Memory 所有的 `tool.result`。真实 PostgreSQL 测试已覆盖群主干来源、缺失 ID、Payload 读取/写入、Generation 路由与资源回收。

OneBot 入站已经调用 Memory 的 manifest UnitOfWork，文本与图片 part 会以明确来源和群主干进入同一 source unit；Media 异步抓取通过稳定 ref 收敛。Memory 包现提供只读主干、来源、关系查询端口；来源查询按群主干、作者、模态、单元类型和发生时间筛选，使用快照加稳定游标分页；关系查询返回节点两端和有界证据，逐条标明来源群、主干及作者。WebUI 与群聊 Shell 都经包声明访问这些端口，Host 不再拥有 Memory 浏览逻辑。真实 PostgreSQL 测试已覆盖包安装、来源筛选及关系证据来源。

Memory 包已注册 `memory.embedding.dispatch` 和 `memory.embedding.recover` 两类 owned-work：Provider 成功绑定并完成 domain.bind 后才自动领取最多 32 个图文 facet；Memory 持有预算、Kernel Effect 与 facet 的 prepare/handoff/observe 事务，Provider 包只物化请求并执行 HTTP。原 claim 在 Bot 级事务锁下合流，避免并发重复领取；失败和明确未发送有冷却时间，预算耗尽延至下一 UTC 日。过期 PREPARED attempt 只放弃并释放预算，DISPATCHED attempt 只观察 UNKNOWN，恢复任务绝不重发网络请求。PostgreSQL 域状态测试和宿主启动/重启测试已通过；真正从归档消息到向量落库的端到端验收尚未完成。

旧宿主 Memory 执行栈已移除，图区域扩散算法位于 `builtin/memory_graph/python/graph_region.py`。嵌入派发和崩溃恢复已在 Memory 包中接线，但尚未通过真实跨包 Effect 与重启恢复端到端验收；独立向量索引 ref 发布、图扩散与有界随机采样的生产接入、跨主干联想写入、语义召回、遗忘及真实召回试验仍未完成。在接入并验收之前，管理 UI 不能宣称这些能力可用。

Memory 的 handoff 事务现由 `memory.embedding.dispatch` owned-work Worker 调用；prepare/handoff/observe 之间仍须通过真实跨包集成和故障注入验收。

过期 PREPARED attempt 由 `memory.embedding.recover` 放弃并释放预算；已 handoff 的 attempt 只转 UNKNOWN，不重发 HTTP。真实跨包事务及故障注入仍待验收。

Memory owned-work Worker 已接入结果观察；成功回执依 claim 顺序验证向量和实际 token 用量，再由单一 UnitOfWork 同时结算 Effect、预算与 facet。独立向量索引发布和语义召回仍未完成；跨包真实请求及崩溃恢复的端到端测试仍待完成。

## 8. 下一切换顺序

1. 在 `builtin/memory_graph` 内建立文档/查询 facet 准备、生命周期绑定的多模态 Embedding Effect、每日成本配额和包所有的持久任务；只把索引 ref 发布到向量库。
2. 接入图区域扩散、跨主干联想和语义召回；恢复及重放需与删除 epoch、Generation 围栏收敛。
3. 补齐遗忘 UnitOfWork、证据浏览的进一步分页以及图文真实召回试验。
4. 验证热重载失败回滚、长任务 drain、群并发、审批、Provider/Tool 不确定结果和 WebUI 包管理贡献。
5. 在真实 OneBot 实例上只进行准许群的单次低频交互验收。

生产图已经是微内核包系统；目标重型 Bot 框架仍需完成 Memory 主链和长期运行验证。
