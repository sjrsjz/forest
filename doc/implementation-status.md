# Forest 实现状态

> 本页只记录当前工作树事实，不定义目标架构。架构约束以 [00 架构总契约](00-architecture-contract.md)、[13 微内核控制面重构](13-microkernel-control-plane-redesign.md)和[15 单 Agent 生产执行](15-single-agent-production-execution.md)为准。当前 **Bot 运行图** 已切为微内核包图；部署 WebUI 仍以宿主 `deployment_management.py` 对象和固定字符串 Generation 提供管理操作，尚未达到 deployment/host 包图目标。图文记忆主链已有隔离 Bot、真实 VL 嵌入端点、PostgreSQL 与 Milvus 的跨群召回和管理试验验证。获准群的真实 QQ 入站到回复、真实浏览器结合供应商的端到端链路、百万级混合负载及长期运行仍未验收，因此 Forest 仍不可发布。

本页保留按实施先后写下的阶段记录；早期段落中的“尚未”只描述当时状态，当前结论以第 8 节的切换清单和末尾最新验收记录为准。

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

`BotHost._assemble_candidate()` 当前 provision 下列真实 builtin 包，并按 Worker Host 选择快照决定最终 RuntimeGraph 成员；已启用的外部包按精确摘要装配：

| 组件 | builtin 包 | 当前边界 |
|---|---|---|
| Provider | `provider_chat_completions` | Worker 拥有 profile；同代 HTTP driver 验证 Agent 冻结的投影和 wire PayloadRef 后执行传输；响应正文和 usage 由 Provider Generation 保存，dispatch/Effect/Agent observation 只传精确引用；Secret 不进入 Worker |
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
| Memory | `memory_graph` | 多主干来源、图文 DOCUMENT/QUERY 嵌入、独立 ref 向量索引、精确/语义召回、有界图扩散、检索区域冻结及图文证据已接入 Agent；剩余实网与长期验收见下表 |

向量索引的 `upsert/search` 契约现为 major 2：Milvus 只保存 `ref_id`、Bot/空间/用途路由字段和向量，ANN 只返回 ref 与分数；群来源、修订、作者和模态均由 Memory 按 ref 在 PostgreSQL 权威表复核。collection 名称按新布局分代，避免旧列布局在热重载或重建时被误用。管理试验已改用独立临时输入，经同一 QUERY facet 嵌入、索引、召回、冻结和证据链；真实 VL 供应商加 Milvus 的隔离验证已通过，浏览器授权与长期负载仍需验收。

内核的 Runtime、Payload、Authorization、Effect 与 Audit Authority 仍是可信微内核组件。它们不是可卸载业务插件，也不应迁入 `builtin`。

内核向包提供的 RuntimeGraph 状态、候选图预检、包工件目录和带准确来源的 `runtime_status` Agent 工具，属于受限的 Kernel Service 桥接，不拥有可热重载业务状态。它们以 `kernel_*` 源文件命名并由 `core_runtime` 注册；真正的 builtin 组件仍必须从不可变包快照装配。`builtin_packages.py` 是包装载器，不是业务插件。

## 4. 已切换的生产 builtin

生产组合根已统一通过 `bootstrap_builtin_package()` 装载以下组件。Host 只装配配置、能力授权、效果驱动和冻结契约句柄，不再构造这些组件的 Repository 或业务 Runtime：

| 组件 | 生产包 | 当前状态 | 剩余工作 |
|---|---|---|---|
| Agent | `builtin/agent_runtime` | 隔离 Worker、私有 SQL、manifest UoW 与 owned-work 已接入生产图 | 继续审计剩余宿主边界并补齐可观测管理面 |
| Conversation | `builtin/conversation_scheduler` | 入站、分类、静默归档、Agent 调度与安全边界合流已接入生产图 | 扩充管理查询与长时间压力验证 |
| Memory | `builtin/memory_graph` | 多模态来源、归档资格、精确/语义召回、DOCUMENT/QUERY 嵌入发布、按需图扩散、检索区域冻结、规范证据物化/发布、成功回复联想日志及归档提升、单来源遗忘固定计划/管理入队和撤销正文后台清理，以及大范围遗忘的冻结选择、逐批 P0 授权入队、批次事务与进度查询已运行 | 真实跨群学习验收、大范围遗忘的压力/崩溃恢复与浏览器授权验收、管理试验的真实浏览器及供应商图文验收、完整管理面 |

OneBot Adapter 现在按消息形态调用 manifest 声明的四条入站 UnitOfWork 计划。Conversation、Memory/Media、Shell 与分类 participant 在同一事务计划内提交；Host 不再提供 `kernel.uow.onebot.ingress` 业务服务。

旧 `clean_*` 宿主文件已改为职责名称：`bot_host`、`multi_bot_supervisor`、`supervisor_host`、`management_gateway`、`module_management_contract` 和 Bot/Deployment Store；旧 Python import 路径及 `Clean*` 类型名不保留兼容转发。这些现行文件承载组合根、部署入口、管理路由和最小数据库句柄，不能误判为可热重载的业务模块。内核服务列表中五个没有实现及调用方的旧 Conversation/Agent UoW 服务声明已删除。Provider、Embedding、Vector Index、Process Executor 的默认配置已归入各自 builtin 包，根目录配置 sidecar 已删除；启动目录读取包内声明，实际 Generation 继续以冻结包 schema 校验。Process Executor 的执行安全约束由宿主原生 effect driver 独立执行，不能交给 Worker 决定。业务状态和执行循环必须留在可热替换 builtin 包内，不能靠宿主 Python import 重新固定扩展点。

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

包内已加入 `model_execution.py`，模型 attempt 的 profile bind、Agent-owned `model.input`/`model.projection`/`model.wire` 写入、授权与 Effect prepare、原子 handoff、Provider dispatch、Effect/Agent 原子 observe 及 Provider 结果读取均只经过 `ctx.services`、`ctx.uow`、`ctx.payloads` 与冻结的 `ctx.identity`。它没有 Repository、数据库连接或宿主 Agent adapter；Provider 异常或回执 fence 不一致在 handoff 后统一收敛为 `UNKNOWN`，不会盲目重发。

Worker 已公开 `agent.execute/2`：输入只含 Conversation/Session/Run 引用、恢复标记、绝对期限和 actor fence；首批选择、Run lease、每次模型调用前的输入合流、Context Payload 树读取、Provider Effect、严格 ModelTurn 解析、唯一一次协议修复、非审批 ToolCall 的 Effect 执行以及 `final.reply` 生成均在隔离包内完成。恢复查询现在返回完整 Payload digest/owner fence，Worker 不再按裸 Payload ID 读取宿主存储。

Agent 已进入生产组合根。支持查询的 ToolCall 能在 handoff 后通过冻结目录的 `query_handed_off` 查询原 operation，并用 `RECONCILIATION` 收敛；不支持查询的工具保持 `BLOCKED_UNKNOWN`，不会盲目重发。审批决定不触发宿主 callback；Conversation 的持久 defer deadline 负责重新调度，Agent 在恢复执行时检查审批事实。`agent.reload-resume.pending` 负责热重载后的持久恢复，Host 不解释 Agent 状态。

包运行时现已具备能力受控的分块 Payload 端口：`payload.read` 只接受完整 owner/digest 引用并在 Worker 端复核最终摘要，`payload.write` 强制当前组件所有权、连续 offset、声明长度/摘要与精确父引用；提交结果由 SDK 强制收敛为五字段 PayloadRef，传输元数据不能混入跨模块引用。单代暂存最多 32 MiB，并随调用和 Generation 回收。该端口解决了长期 Context、模型输入和图片不能穿过 256 KiB 控制帧的问题，是把 Agent、Memory、Conversation 执行逻辑移入 Worker 的基础设施。它本身不代表三个模块已完成切换。

Provider 的 `provider.model.dispatch/2` 已移除内联模型正文：效果驱动在所属 Generation 下原子写入 `model.result`/`model.usage`，服务回执与 Kernel Effect evidence 只持有精确引用；包内 model attempt 只能把这些引用交给 Agent observation，不能再以 Provider 身份复制载荷。Effect/Agent 私有 schema 已用独立 PostgreSQL 重建验证；当前完整 Agent 执行循环已位于隔离 Worker，旧宿主执行器已删除。

## 6. Conversation 生产包已经拥有的部分

`builtin/conversation_scheduler` 已封闭消息接纳、连续分类水位、截至最后 trigger 的输入选择、带作者来源的 Context、无 LLM 摘要的窗口裁剪、静默排空和 `conversation.context.snapshot/2`。快照不再把完整历史塞进 Worker 帧：权威查询最多返回 32 个活动单元，Worker 写入带正文/作者父边的 `context.snapshot.page`，超过 128 页时构造 `context.snapshot.index` 树，服务只返回最终 manifest 的精确 PayloadRef。同群串行和跨群并行依靠数据库 actor lease，而不是进程锁。

数据库租约驱动的 `conversation.agent-run.pending` 已进入生产组合根：同一群聊只允许一个 actor claim，跨群可由多个槽并行；Worker 通过 `agent.execute/2` 驱动 Run，在模型安全边界合流，随后用事务计划原子提交 Agent 完成态、Conversation 回复单元与 Delivery Intent，再在 Session 边界选择后继输入并联动 Memory 资格确认。Agent 的 `FAILED/CANCELLED` 终态由独立事务计划核对精确 trigger 集、收敛 trigger 状态并释放 Session；`WAIT_APPROVAL` 依靠持久 defer deadline 轮询准确 approval，不需要宿主监听器。

生产组合根显式给 `agent.runtime` 的 grant set 授予 Bot 级 `provider.invoke`，给 `memory.graph` 授予 Bot 级 `embedding.invoke`；两者的 Kernel Effect 授权票据仍须逐次绑定准确输入摘要、目标 Generation 与期限，服务调用能力本身不代替外部副作用授权。整图启动/重启测试核验 grant 落库。

## 7. Memory 生产包已经拥有的部分

`builtin/memory_graph` 已经是可由 `PackageCatalog` 构建的独立包，声明自己的隔离 Worker、私有 storage baseline、Generation 配置、管理 UI、来源捕获与输入资格 UnitOfWork participant，以及 `memory_recall_exact` Agent 工具。嵌入配置将 `TEXT_IMAGE` 模型种类、部署标识、模型修订、维数与归一化方式绑定到 Bot 模块生命周期；端点轮换仍应由后续受限 Effect 配置承担，不能在已创建 Bot 上改换模型种类。

消息来源捕获已由 `memory.capture-source` manifest UnitOfWork 封闭：Kernel Payload 删除 epoch、Conversation 已接纳的 event/ingress sequence、群来源身份、作者引用、多模态 part 与 occurrence 在同一事务校验和提交；来源、unit 与 occurrence 都有独立并发围栏和幂等账本。Media Worker 可经 `memory.source.capture` 使用同一 participant，抓取失败则经 `memory.source.unavailable` 留下持久不可用事实，避免 Agent 选择先于异步抓图完成时永久等待。输入资格 participant 会在捕获先到时原子提升 occurrence/unit，在选择先到时登记 `PENDING_CAPTURE` 并由后续捕获收敛；两种顺序均不需要宿主 Repository 回调。该路径已用真实 PostgreSQL 安装包 baseline 并验证捕获、资格转换、精确召回与 Registry 资源回收。

资格边界已进一步拆分：`memory.qualify-input-batch` 现在只将 occurrence 标为 `ADMITTED`，不会让仍在活动 Context 的 source unit 进入长期召回。静默裁剪的 `conversation.commit-silent-input` 在同一 UnitOfWork 提交 Context 归档、捕获 Payload 删除 epoch，再调用 `memory.qualify-archive`；只有已提交的 compaction 成员才能把 occurrence/unit 提升为 `ELIGIBLE`。归档先于异步来源到达时留下 `pending_archive_events`，捕获与确定不可用会按同一事件围栏解析它。新 Session 首个 Run 在 Agent 选择输入前先由 Conversation 包执行 `conversation.compact-before-run`，归档及 Memory 资格仍在同一事务中提交；Run 中途不更换已经冻结的视图。后续 Run 的交接裁剪、有界分块和长尾维护唤醒仍待完成。

精确召回不再调用宿主 `builtin_memory_tool.py`：工具声明由 Memory manifest 发布并静态绑定本包 service，隔离 Worker 经只读 Domain Query 获取最多八个归档稳定 source unit，只有 ACTIVE 来源、`ELIGIBLE` unit 和完整 part 集可见。Worker 按精确 PayloadRef 读取文本、保留图片引用，结果明确携带 source/trunk/group/author provenance，并以原始 part 为父边写入 Memory 所有的 `tool.result`。真实 PostgreSQL 测试已覆盖群主干来源、缺失 ID、Payload 读取/写入、Generation 路由与资源回收。

OneBot 入站已经调用 Memory 的 manifest UnitOfWork，文本与图片 part 会以明确来源和群主干进入同一 source unit；Media 异步抓取通过稳定 ref 收敛。Memory 包现提供只读主干、来源、关系查询端口；来源查询支持多群来源 ID、精确来源 ID、主干、平台/Realm/群号、Binding、作者、模态、单元类型、事件时间、单来源序号和当前状态的组合筛选，使用快照加绑定筛选摘要的稳定游标分页；关系查询返回节点两端和有界证据，逐条标明来源群、主干及作者。WebUI 与群聊 Shell 都经包声明访问这些端口，Host 不再拥有 Memory 浏览逻辑。真实 PostgreSQL 测试已覆盖包安装、来源筛选及关系证据来源。

Memory 包已注册 `memory.embedding.dispatch` 和 `memory.embedding.recover` 两类 owned-work：Provider 成功绑定并完成 domain.bind 后才自动领取最多 32 个图文 facet；Memory 持有预算、Kernel Effect 与 facet 的 prepare/handoff/observe 事务，Provider 包只物化请求并执行 HTTP。原 claim 在 Bot 级事务锁下合流，避免并发重复领取；失败和明确未发送有冷却时间，预算耗尽延至下一 UTC 日。过期 PREPARED attempt 只放弃并释放预算，DISPATCHED attempt 只观察 UNKNOWN，恢复任务绝不重发网络请求。PostgreSQL 域状态测试和宿主启动/重启测试已通过；真实宿主图已用假 HTTP 客户端验证一条归档群消息的文本与图片 facet 经 `EMBEDDED` 发布为 `READY`，并写入独立向量索引。

旧宿主 Memory 执行栈已移除，图区域扩散算法位于 `builtin/memory_graph/python/graph_region.py`。嵌入派发已通过 Kernel Effect、Memory 和 Embedding 包的联合测试；崩溃后恢复与真实供应商响应尚未端到端验收。独立向量索引包已有 PostgreSQL 持久 ref 围栏：同 ref 写删串行、删除建立不可逆 tombstone，删除后的 upsert 被拒绝；后台周期清扫会重放 tombstone 删除，以收敛 Milvus 超时后的迟到写入。Memory 已用包内 owned-work 按 Embedding batch 读取结果、验证每个 facet 的向量摘要并发布稳定 ref；PostgreSQL 复核来源后才把 facet 置为 `READY`，失效候选写删除 outbox，另一包内任务执行删除。隔离 PostgreSQL 加真实 Milvus 的索引测试，以及假 Embedding HTTP、假 Milvus 的宿主图文发布、outbox 删除、热替换和重启测试已通过。单来源遗忘事务、固定影响计划、管理入队和 Payload 正文清理已接入包公开服务；跨主干联想写入与语义召回主链已有后续实现，真实召回试验和大范围遗忘选择器仍待完成。管理 UI 不应将待验收能力标为可用。

`memory.source.forget/1` 是单来源的事务原语，已通过固定影响计划、WebUI P0 授权入队和包自有 Worker 开放。Memory prepare participant 只读不可变作者载荷锚点，不提前锁源；`kernel.payload` 的通用 `payload.forget-anchor-from-prior` 仅接受具有唯一完整根来源的锚点，在删除屏障下撤销该来源及所有已知派生 Payload。Memory apply participant 消费版本化回执和选中 Payload ID 集合，在自己的 namespace 内同步失效受影响 source unit/occurrence/facet、停用关系端点与证据贡献，并为全部 facet ref 写不可逆删除 outbox。任一步拒绝整体回滚；重放返回原回执，同 operation 并发只推进一次 epoch。跨群派生载荷如果命中来源闭包也会失效，不按当前查询群覆盖来源。Kernel Payload 由 Generation 自有的受监督任务分批删除 `REVOKED` 正文，宿主 PostgreSQL 遗忘/热替换测试已验证 `SCRUBBED` 且 `payload_bodies` 无对应行；更大范围选择器仍待完成。

历史 Payload 的 `accepted_deletion_epoch` 仅用于接纳审计，不能作为永久读取许可。配置、组件状态、服务回执和通用 JSON 读取已改为在当前删除屏障下验证 ACTIVE 状态；未受影响载荷不会因另一来源遗忘而不可读，撤销载荷仍立即拒绝读取。宿主端到端测试验证图文来源遗忘、幂等重放、向量删除失败重试，以及遗忘后的热替换和重启。

Memory 的 handoff 事务现由 `memory.embedding.dispatch` owned-work Worker 调用；prepare/handoff/observe 已通过假 HTTP 客户端的真实跨包事务验证，故障注入仍待验收。

过期 PREPARED attempt 由 `memory.embedding.recover` 放弃并释放预算；已 handoff 的 attempt 只转 UNKNOWN，不重发 HTTP。恢复路径已有隔离测试，真实进程崩溃与重启故障注入仍待验收。

Memory owned-work Worker 已接入结果观察；成功回执依 claim 顺序验证向量和实际 token 用量，再由单一 UnitOfWork 同时结算 Effect、预算与 facet。独立向量索引发布已接入并用假 Milvus 验证；语义召回、真实供应商请求及崩溃恢复的端到端测试仍待完成。

Embedding 与向量发布主链同时支持 `DOCUMENT`/`QUERY`，用途来自持久 claim。每批最多 32 个 facet，同批只能属于一种用途及同一个 query；QUERY 必须匹配固定 InputBatch/context revision 和 source revision，且已完成全部 part 准备。QUERY 发布不提升源单元的长期归档资格。宿主测试通过公开注册/准备服务完成图文查询，不再直接写入 QUERY facet；DOC/QUERY 发布、同时遗忘、删除重试、热替换和重启已验证。

`memory.query.begin/1` 由隔离 Worker 调用 manifest `memory.begin-query` 事务计划，Kernel Payload 先捕获删除 epoch，Memory 依据持久接纳记录登记不可变查询成员集。尚有来源捕获未完成或已接纳成员被撤销时拒绝整份查询；超过 256 个 part 明确拒绝。只读 `memory.read-query-parts` 每页最多 32 项，翻页前检查整份成员集的源修订、接纳资格和来源状态，返回准确 PayloadRef。接纳和待捕获记录均有 InputBatch/revision 索引。`memory.query.prepare/1` 已通过固定 part 回执和不可变完成事实闭合查询准备；部分准备不触发嵌入，空白文本以零 facet 回执闭合。

查询准备的宿主验证覆盖两个不同调用并发准备同一个 query：part 操作和完成事实只提交一份，两个结果相同；缺少 part 回执时完成事务明确拒绝。纯分块回归、8 项 Memory 包测试和宿主图集成通过。完整语义召回仍需完成最终证据冻结与 Agent 接入，不能以查询向量 READY 代替召回验收。

`memory.query.match/1` 已独立承担 ANN 准备：通过 manifest UnitOfWork 固定查询的检索策略及 facet 数量，逐 facet 查询索引，由 Memory SQL 重验并持久化不可变匹配回执。每个查询 facet 对每个源单元只登记首个合法命中，零命中也登记回执；多个调用合流到已提交的同一回执，不覆盖先前 ANN 结果。匹配行保存准确 query/document facet、源修订、候选 rank 和分数。重试只补缺少的 facet；完整性由固定 facet 数量与唯一合法回执集合闭合。旧的 Python ANN/RRF 种子实现已删除，`memory.query.seeds/1` 与 `memory.query.region/1` 只读取完整持久匹配集合，RRF 在数据库计算，区域使用查询固定策略。两种读取均不调用 ANN，不物化正文、不冻结最终 EvidenceBundle；来源群、主干、作者及当前删除过滤仍由 Memory 事实库负责。

种子宿主集成使用假 Embedding HTTP、假 Milvus 和真实 PostgreSQL：后续图文查询召回已归档消息，返回其原群及作者，不存在的索引引用被过滤，来源遗忘后种子立即消失，热替换与重启通过。查询注册操作 ID 由 query_id 决定，不依赖外层调用；不同调用可合流到同一注册事实。8 项 Memory 包测试及最终宿主测试通过；真实供应商端到端召回和最终 Provider 证据投影仍待验收。

查询登记现保存 PostgreSQL `pg_snapshot`，归档和向量发布保存顶层 `xid8`；种子资格按事务可见性判定，不再将时间戳当成提交顺序。关系贡献及证据也保存顶层事务 ID，供后续图邻接读取使用。真实 PostgreSQL 并发回归验证：时间戳早于查询边界但之后才提交的事实，对旧快照不可见、对新快照可见。宿主图文种子、来源遗忘、热替换和重启验证通过；该变化没有实现尚缺的生产图扩散及最终 EvidenceBundle 冻结。

`memory.query.region/1` 已把种子读取接入包内按需最大乘积扩散，旧的全图加载 helper 已删除。种子、邻接及关系证据复用相同的 SQL 来源可见性规则；数据库分别寻找两条时序邻接，并对合格联想贡献先做查询派生限量采样，再向 Worker 返回。节点读取预算在展开前执行，失效节点也消耗预算；最大 64 次节点读取、每点 128 条联想边及扩散阶段 20 秒上限与包消息帧约束一致。配置及模块自带 UI 均收敛为 `(0,1]` 扩散权重，不再接受会放大路径分数的权重。

真实 PostgreSQL 邻接测试覆盖高出度限量采样、相同查询稳定子样本、不同查询不同子样本、双向跨主干关系、来源群/作者、队列尾部、迟到归档/关系、证据集合创建事务封闭、part/修订外键、抑制与退役。4 项算法回归覆盖最强路径、环、读取预算、失效节点及权重为 1 时保留直接种子。假 Embedding HTTP/假 Milvus 加真实 PostgreSQL 的宿主验证覆盖区域公共服务、遗忘后区域为空、热替换与重启。区域只返回引用和元数据；区域经独立服务物化和发布规范证据；联想学习写入及 Agent 模型输入接入仍未完成。

匹配回执的宿主验证覆盖 ANN 第二个 facet 故障后的部分准备拒绝、保留已提交回执并补齐、两个调用合流到同一完整集合、完整重放与种子/区域读取不再增加 ANN 调用。图文匹配保留 IMAGE/TEXT 查询模态，来源遗忘后种子及区域立即为空，热替换和重启通过。单来源遗忘 prepare 已改为只读不可变 anchor，实际源锁在 Payload 删除屏障之后由 apply 获取，修正匹配外键锁与遗忘提前源锁的反向等待风险。15 项 PostgreSQL/算法/宿主验证通过；规范证据物化及发布已在后续切片接入，Agent 接入仍未完成。

`memory.query.freeze/1` 已由包内 Worker、私有查询和 manifest UnitOfWork 实现。提交事务按固定匹配策略重验种子集合、源修订及父路径，把检索项目与实际路径边一次性写入不可变记录并推进查询为 FROZEN；失败发生在任何记录写入之前。同一查询的并发冻结收敛到同一回执，空结果可以冻结，读取和重放不再调用 ANN。删除后原回执保留历史元数据，只读召回立即过滤已失效来源；回执不携带原文或读取权限。

此次真实 PostgreSQL、算法及宿主验证共 15 项通过，覆盖并发冻结、空区域、固定回执、遗忘过滤、热替换及重启。邻接验证另外覆盖 32 节点/31 条父路径的冻结，以及伪造种子排名、非种子激活值、边、未结算父节点和源修订的拒绝与零部分写入。嵌入预算验证首次在 Windows 临时包目录 rename 处遭遇访问错误；使用独立短临时路径复验通过，未修改生产代码增加重试。规范证据正文/媒体物化已在后续切片接入，模型输入及 Agent 语义接入仍未完成。

冻结结果的包私有 `memory.read-retrieval-parts/1` 已提供按 item_index 和 part_index 的有界只读分页，每页最多 32 个原始 parts 引用。返回来源群/主干、作者及身份 PayloadRef、时间/源顺序、当前来源标志、冻结排名/激活/前驱和准确命中 part ID。读取沿最长 64 个节点的原父链复核来源修订、当前关系证据和合法根种子；失效链返回空内容引用，不重新扩散。数据库验证覆盖跨主干出处、空分页、越界项目和根失效阻断自身仍有效的时序邻居。后续物化服务已基于该端口生成规范图文证据及完整父路径/关系证据来源 lineage，并在发布前复核；Agent 模型派发边界的接入和复核仍未完成。

本次两项真实 PostgreSQL/宿主验证通过：完整宿主覆盖 TEXT/IMAGE/TEXT 的两页原序引用、作者和群来源、遗忘后无引用、包热替换及重启；图邻接验证覆盖跨群与时序路径、已命中 part、末页/不存在项目、来源退役，以及根被抑制后仍有效子节点的内容阻断。验证未发送真实 QQ 消息。

`memory.query.materialize/1` 已通过独立包 Worker、私有分页及 manifest 发布事务生成 `forest.memory-evidence/1` 规范图文载荷。保留事件时作者身份、群/主干、时间/顺序、命中 parts 和固定路径；正文按固定字符预算截取，图片保留原始 Managed PayloadRef。内容截断、图预算截断、采样和失效项目分别标记，不调用 ANN、Embedding 或 LLM。查询输入、实际 parts、父路径和关系必要证据的全部来源锚点均建立 Payload 父链；发布锁住依赖并复核全部合格项目后，一次性写不可变 `retrieval_materializations`。重放重验路径和 Payload，失效证据不重新选取正文。

18 项 Linux 整包/数据库/算法/宿主验证通过；新增作者身份 64 KiB 上限后，6 项宿主及物化约束复验通过。覆盖并发合流、空证据的查询 lineage、TEXT/IMAGE/TEXT 原序、历史作者/群来源、无需再次 ANN、遗忘撤销规范载荷、热替换/重启、关系必要证据来源、非法项目与有效项目遗漏的零部分写入、Unicode 截取、父引用分组和无效作者/文本拒绝。Windows 原生验证的部分包目录 rename 仍出现访问错误，原因未确认；没有加入生产重试或兼容分支。验证未发送真实 QQ 消息，测试容器采用 1 CPU/768 MiB 上限。

此时 Agent 尚未消费该规范证据；后续已接入公开召回端口和原生证据投影，见本节末的最新记录。跨主干联想学习、完整遗忘管理/物理清理和管理召回试验仍待完成。

`memory.query.progress/1` 已加入 Memory 包公开只读契约，私有 SQL 在一个语句快照内从已有准备、facet、匹配、冻结及证据发布事实推导阶段，不新增重复的阶段表。完整准备为零 facets 与尚未完整准备分别返回 NEED_MATCHING 与 NEED_PREPARATION；部分匹配仍为 NEED_MATCHING，UNKNOWN 明确阻断，失败/抑制不冒充空结果。FROZEN 查询不会回退准备，READY 在私有路径复核之后还经 Worker 读取准确 PayloadRef，删除后的证据不会重报可用。该端口不登记工作、不触发付费请求，也不承诺后台召回已经运行。

两项 Linux 真实 PostgreSQL 验证分别通过：宿主集成涵盖公开阶段端口、部分 ANN 回执、空证据、规范证据引用、遗忘、热替换和重启；图数据库验证涵盖零 facet/部分准备、合法嵌入等待/UNKNOWN/抑制转换及严格输出 schema。4 项图算法及 5 项证据物化快速回归通过。新增阶段 fixture 最初使用非法 PENDING → UNKNOWN 转换，被现有约束拒绝；修正为合法路径后单项复验通过。此次未发送真实 QQ 消息。

[16 持久语义召回](16-semantic-recall-execution.md) 规定 Memory owned-work、Agent 步骤输入版本、等待后接纳新 trigger 和总截止时间。模型能力投影位于可热替换的 Agent 包；Provider 的请求 materialize 服务和宿主投影函数已删除。Agent 冻结 `model.input`、`model.projection` 与 `model.wire`，Provider 仅核对引用并传输。Context 根、Preset 或协议修复反馈、规范 Memory 证据和已解析图片进入 `model.input` 父链。Agent 现已接入 Memory 公开 begin/request/work-status/cancel 端口；伪 OneBot 群触发到伪模型请求的隔离端到端验收已通过，真实 QQ 实例验收仍待执行。

Memory 包现有 `memory.query.request/1`、`memory.query.work-status/1` 与 `memory.query.advance` owned-work，按已登记 query_id 和不可延长的绝对截止时间工作。任务表只保存 claim、lease、attempt、退避及终态；实际阶段仍来自 `memory.query.progress/1`。Worker 每次只执行一个现有幂等阶段，WAIT_EMBEDDING 交还租约，BLOCKED_UNKNOWN 不触发付费请求；过期 claim 的结算被拒绝，失败最多三次。READY 状态读取再次检查 Memory 路径和受管证据载荷。Agent 现按持久 Run 输入推导 query_id，等待未完成的来源捕获与召回；已在隔离 PostgreSQL 和 Worker 路径验证一次伪群消息到实际 Provider 请求体的链路，尚不能据此宣称已上线。

`memory.query.cancel/1` 已在 Memory 包中加入 SUPERSEDED 取消。待领取工作立刻进入 CANCELLED；已领取工作先显示 CANCELLING，Worker 起步前可跳过，已开始的幂等阶段由原 claim 结算，再落为 CANCELLED。取消与领取/结算锁同一工作行，READY/FAILED 先提交时返回 TERMINAL 而不删除历史；外部嵌入 Effect 不被假定可撤销。真实 PostgreSQL 覆盖 PENDING/CLAIMED/TERMINAL、重复取消和结算竞争。Agent 现于同一步骤收到更新的 InputBatch 后取消旧 query；已登记但尚未请求工作的 query 无须取消。

Agent 包的模型安全边界选择每次观察使用新的 operation identity，不再把一个 `CONTINUE` 或 `WAIT_INPUT` 回执当作同一步骤永久有效；已提交 BATCH 的真实去重靠同事务推进的 Run input revision 和 InputBatch 唯一约束。Agent 包状态、同群最后触发切口、宿主热替换/重启及投影所有权的真实 PostgreSQL 测试通过。旧宿主 `forest.models` 已拆成 Worker 可分发的纯协议值与宿主传输代码，旧 Provider materialize 驱动及服务声明已移除。Agent 现可返回 `WAIT_MEMORY`，原生解析 `forest.memory-evidence/1` 并保留群、主干、作者与路径归因；图文模型通过 `media.context.resolve` 取当前上下文图片，文本模型省略图片字节。伪群图文请求已通过整链验收；真实 QQ 与真实供应商响应仍待验证。

固定包内容后，Linux 隔离 PostgreSQL 与宿主 Worker 的两项集成验证通过，用时约 163 秒。数据库验证覆盖请求合流与截止冲突、UNKNOWN 停驻、重复结算、token 重用拒绝、lease 过期后的旧 claim 拒绝、故障/截止终态；宿主验证覆盖新查询由 owned-work 自动完成准备、嵌入等待、固定 ANN、冻结和证据发布，READY 读取、遗忘后读取拒绝、热替换与重启。未发真实 QQ 消息，测试容器受 1 CPU/768 MiB 限额约束。中途一次测试因运行时改写了同版本包内容而被包仓库正确拒绝，最终在包内容固定后重新完整通过。

本轮修复后，本地全套测试 436 项通过、89 项跳过，隔离 PostgreSQL 宿主与调度回归 16 项通过。Agent 通过持久 Run 输入推导查询 ID 和共同截止时间，在 Memory 捕获或召回等待时返回 `WAIT_MEMORY`；召回完成后再次检查群聊排队输入，旧版本查询在被新输入取代时取消。当前上下文图片经 `media.context.resolve` 读取，历史证据图文按模型能力投影；文本模型不读取图片字节。新增隔离验收以伪 OneBot 群 @ 触发真实宿主、Scheduler、Agent、Memory 与 Provider 请求准备，核对群、主干、作者来源和纯文本 wire 不含图片；伪模型返回 UNKNOWN，因此没有真实 QQ 发送。此链路暴露并修复了 Agent 缺少 Memory/Media 服务绑定、同一 Run 不同 claim 复用 WAIT_MEMORY 服务回执，以及 Scheduler 包 `max_state_inflight=1` 阻塞 owned-work 读状态的问题。后续独立验收通过图文群消息到模型 wire，以及第一群模型调用挂起时同群 trigger 排队、第二群模型调用继续进入 Provider；图片召回命中、真实供应商响应及跨群成功回复仍待验证。

成功回复链路的隔离验收进一步使用合规的模型 XML 回执，真实 Agent 包完成 `FINAL_READY` 后经 Scheduler 的 FinalReply UnitOfWork 提交 Run 终态、Conversation 回复单元、Memory 联想日志和 Delivery Intent，伪 OneBot 收到一条投递；未发送真实 QQ 消息。此前 `agent.commit-final-run-record` 的 SQL 同名变量错误已修正。新增学习路径冻结实际投影的直接种子和 part，排除扩散节点、未投影图片及失败 Run；Memory 在同事务复核触发事件、查询 part 和冻结检索种子。隔离 PostgreSQL 验证了活动消息只留待归档日志、归档后跨主干提升、重放与重复召回不累加边权。真实 QQ 跨群成功回复和遗忘竞争长时间验收仍待执行。

单来源遗忘新增 `memory.source.forget.preview/1` 包服务和三步 UnitOfWork：Memory 先取得作者 anchor，Kernel Payload 在删除时钟锁下选定包含派生节点的闭包，Memory 对来源、向量删除引用、关系贡献与待归档联想日志计算计数及完整集合摘要。携带计划执行时，Payload 复核删除纪元和选中摘要，Memory 复核影响计划；任一变化都会使整个遗忘事务回滚。FinalReply UnitOfWork 也先捕获删除屏障，再提交 Agent、Conversation 和 Memory 联想，避免关系提升绕过遗忘的固定影响计划。隔离 PostgreSQL 验证了新增派生 Payload 后旧计划被拒绝且纪元未前进，以及关系证据变化后旧计划被拒绝、重新预览成功；完整宿主测试经过热替换和重启，伪 OneBot 成功回复链也通过。最终本地回归 438 项通过、94 项跳过，未发真实 QQ 消息。Memory 管理 UI 已接入预览、管理员确认入队和请求状态查询；较大范围选择器仍待实现。

Memory 管理面现以包 manifest 映射 `memory.forget.preview` 只读服务、`memory.forget.enqueue` P0 Domain Command 和 `memory.forget.status` Domain Query。管理员确认的完整计划通过 JSON Pointer 进入 Web authority ticket 摘要；票据、Bot 策略和审计在同一入队事务内核验，动作只写 Memory 自有队列。Memory owned-work 随 Generation 领取请求，沿稳定 operation ID 调用跨模块遗忘 UnitOfWork，旧计划失败保留错误码；已提交遗忘在租约过期后从不可变 `source_forget_operations` 收敛为成功。隔离 PostgreSQL 宿主验证了旧计划 `FAILED` 且删除纪元不前进、有效计划在真实 Web 票据保护下成功、热替换与重启后的状态查询一致。模块 UI 使用现有隔离 MessageChannel 完成预览、显式确认、入队和终态查询，并加入群主干、作者、模态、类型与时间过滤；没有发送真实 QQ 消息。

旧宿主 Agent 模型/工具执行器、上下文投影及工具授权/效果桥接代码已从生产树删除，其旧测试也已清理。内核自有 `runtime_status` 工具改为直接接受冻结的 `forest.agent-tool-execute/1` 命令并返回包工具使用的 `forest.agent-tool-receipt/1`，不再依赖旧 Python `ToolDispatchResult`。这只完成旧执行路径清理；管理召回试验、大范围遗忘选择器、真实供应商与 QQ 验收仍未完成。

Memory 来源管理查询新增群来源集合、精确来源、平台/Realm/群号、Binding、来源状态和单来源序号区间，均在包 SQL 内服务端筛选，并进入游标的筛选摘要；新增 Binding 浏览索引。模块 UI 将较少使用的条件收进折叠区，避免拉高工作区。隔离 PostgreSQL 主链验证五组通过，覆盖组合条件、错误群来源、状态变化后的游标拒绝。关系审核筛选和连续批次自动编排仍未完成，不能用来源查询替代固定批量影响计划。

来源浏览现以首次查询的 `pg_snapshot` 绑定游标，并以 `source_units.accepted_xid` 和归档提交的 `eligible_xid` 判断首读可见性；`accepted_at` 仅作时间边界与显示元数据，不再冒充提交快照。隔离 PostgreSQL 验证：来源事务在首屏读时尚未提交、提交后其旧时间戳不会插入后续页，新查询仍可看到它；当前删除/资格过滤继续生效。模块 UI 与查询契约已传递 `snapshot_xid`，但大范围选择器及完整管理召回试验仍未实现。

关系管理查询已停止用 `created_at` 判定分页快照中的正向事实：首读捕获 PostgreSQL `pg_snapshot`，游标携带并绑定该快照，关系贡献、源端点与必要证据都以事务 ID 检查快照可见性，同时重验当前资格。结果明确返回相对 seed 的方向、创建来源、跨主干状态、关系键以及证据的来源 ID、平台、群/主干和作者；包 UI 可按关系类型、来源、方向与跨主干状态筛选。当前只有一跳权威事实浏览，审核与发布图投影尚无权威记录，因此回执不包含审核或投影状态，也不能把它展示成已审核或已发布图版本。真实 PostgreSQL 的迟提交关系测试证明旧快照不会混入该关系；图文主链的 5 组用例中一次旧测试的后台 Worker 与手动 claim 竞速，已把测试冻结点移到手动仲裁之前并复验通过。

Kernel Payload 的遗忘基础路径已将多来源根选择改为一次 `unnest` 集合连接，将来源目标与闭包 Payload 记录改为集合插入；删除屏障、完整闭包摘要、100000 Payload 上限及重放围栏不变。真实 PostgreSQL 的多来源共享派生载荷验证表明，同一派生 Payload 只进入闭包一次，未选来源仍保留；原有单来源、预览变更和双锁顺序并发测试均通过。这是大范围遗忘选择器需要的下层基础，上层选择目录、分批计划及管理 UI 的交付状态见下文。

管理召回试验采用“启动 action + 状态/结果 query + 独立取消 action”。试验使用独立查询身份和私有临时 part 表，不借群消息表的外键伪造来源；它与正式查询共用嵌入、索引、图扩散和证据物化链路。通用管理资产传输已接入；结果 query 已提供有界、带来源归因的证据分页，真实图文全链验收尚未完成。

正式召回的 `memory.read-query-parts` 显式返回 `SOURCE_UNIT` 输入身份，查询 facet 的稳定 ID 将输入种类并入摘要，且校验来源身份与已接纳 part 一致。相同的纯 facet 切分函数接受 `ADMIN_TRIAL`，后者还有独立私有存储与 SQL 准备分支；试验的通用管理资产入口和结果 UI 尚未接通，不能表述为召回试验可用。

Memory 管理关系新增 `memory.relations.neighbourhood` 只读端口：从指定权威节点按固定 PostgreSQL 语句快照展开 1 至 3 跳，节点与边各至多 32 个，最多读取 64 页并有 8 秒截止；返回前驱、跳数、关系类型、方向、创建来源及逐项真实来源证据，遇预算给出明确截断原因。模块 UI 可设置预算并浏览节点与边。它复用一跳关系的当前资格和删除过滤，仍不代表可重建发布图投影、人工审核或图版本状态；管理召回试验和大范围遗忘选择器仍未完成。

查询 part 的读取与 facet 准备契约按 `SOURCE_UNIT` / `ADMIN_TRIAL` 输入身份分支：试验 part 不允许携带群来源单元、修订或 occurrence 字段，正式来源 part 必须携带；Worker 将身份传到准备事务。`embedding_facets` 以身份判别式分别约束真实来源 part 和试验临时 part，试验准备事务校验 Bot、Generation、有效期、修订、有序 part 与 PayloadRef，查询准备完结按相应 part 表计数；PostgreSQL 已验证图文试验 facet 的登记和完结。

试验临时图片 part 现在必须保存并约束真实宽高，文本 part 禁止伪造宽高；Embedding 物化服务支持独立的 `ADMIN_TRIAL` 查询身份和图片快照，冻结请求验证不再需要伪造群来源单元。图文物化在两种 VL 模型请求格式下通过回放验证，原有群来源物化回归通过。

后续已把试验 facet 接入 Memory 的嵌入 claim、handoff 资格复核与向量发布：认领时要求 Bot/Generation、未过期的试验输入、固定 part 和 PayloadRef；handoff 前重新锁定并核验，旧 Generation 不可继续派发。发布事务仍按当前 Generation 与到期时间判定，失效向量写 ref-only 删除 outbox 并抑制 facet；向量发布 Worker 的任务契约不再暴露群来源字段。隔离 PostgreSQL 已验证图文试验认领、旧 Generation 拒绝、正常发布为 READY、旧 Generation 发布时墓碑删除，以及删除 outbox 的认领与结算；该阶段完整 Memory 包 15 项、静态检查和本地 441 项回归通过。

Memory 的常驻向量删除 Worker 每次认领前有界扫描已过期试验：未曾派发的 facet 直接抑制；曾有任何嵌入尝试或可能产生迟到写入的 facet 先登记 ref-only 删除屏障，等待原任务收敛；EMBEDDED/READY 同事务登记删除并抑制。单次最多处理 32 个 facet，扫描索引只收录待处理状态，不包含已抑制的历史行；索引删除继续由原 owned-work 租约与结算完成。真实 PostgreSQL 验证了已发布试验在静默到期后无法读取、被抑制、两个并发认领合流为唯一删除任务、结算及二次轮询不重复领取。后续版本加入完整试验元数据回收；Kernel Payload 另由自身的 TTL 撤销和清扫路径处理。

管理试验复用正式查询的召回 owned-work 状态机时，状态读取现按 Bot、Generation 和有效期拒绝失效试验；任务认领及旧 claim token 重放会把到期或换代的试验收敛为 `FAILED/MEMORY_TRIAL_INPUT_UNAVAILABLE`，避免继续调度图扩散。SQL 定向验证覆盖有效试验状态、换代拒读、旧 claim 重放和待领取工作终止。

Memory 包现已声明 `memory.trial.start`、`memory.trial.get`、`memory.trial.result`、`memory.trial.image` 和独立 `memory.trial.cancel` 管理端口。启动 action 在同一权威事务内冻结 Bot/管理员/Generation、图文 part、十分钟有效期及八分钟召回截止；Memory 的 Domain Command Authority 在提交前向 Kernel Payload 核验每个精确引用的身份、媒体类型和字节数。PostgreSQL 验证了图文登记幂等、伪造摘要和字节数拒绝、独立取消以及 Agent 公开端口拒绝试验 ID。试验证据物化使用单独协议，不伪造“当前群来源”；既有群来源证据协议保持原样。结果端口分页读取证据正文和图片引用并标明群、主干、来源与作者；图片端口按需读取单个归属 part 的 96 KiB 原图分块，隔离 UI 校验完整摘要后显示。真实图文全链验收尚未完成。

取消 action 现同时给试验输入写入单向 `cancelled_at`，阻断后续 part 读取、facet 准备、嵌入领取与派发资格；向量发布不能确认取消后的 READY，向量删除 Worker 将取消与过期试验一起清扫，包括已准备但尚未派发的 `PREPARED` facet。PostgreSQL 覆盖了已领取召回工作的取消后结算、待嵌入和已准备 facet 不再领取并被抑制。已派发的外部请求仍按原 at-most-once 效果结算，不能许诺即时撤销网络调用。

Memory 查询根有显式 `input_kind`，以非空约束区分群来源查询和管理试验查询；正式查询仍必须持有真实来源、InputBatch 与 Context 修订，试验查询的这些字段必须为空。试验元数据及有序图文 part 位于独立表（有效期内不可改删，过期后按回收事务的固定顺序删除），外键按输入种类防止试验查询写入正式来源 part 表；包内 part 读取按 Bot、Generation 和到期时间拒绝旧输入。结果 query 已读取发布证据并在读取前后复核管理员与查询状态。

试验输入的多个 part 现使用由 `query_id` 与 `part_id` 派生的独立 Kernel Payload owner_id；Memory 的启动命令与表约束同时复核该身份，避免图文 part 争用同一唯一 owner。隔离 PostgreSQL 已验证两个真实 Payload 能在同一管理试验登记，错误摘要和字节数仍被拒绝。

管理资产通道已新增于 `kernel.payload`：包清单声明可上传资产，部署管理路由对 256 KiB 原始块设硬上限，Bot Web ticket/策略/审计和分块写入在同一事务内完成。上传会话绑定 Bot、管理员、组件 Generation、目标 query/part、owner 与总体摘要；5 分钟未完成超时、30 分钟最终 Payload TTL，数据库约束块身份，限流窗口及每 Bot 保留量设上界，常驻 Kernel 清理撤销过期 Payload。完成时从真实图片字节解析尺寸；Memory 试验 action 对每个 part 复核同一管理员、Generation、query/part、元数据及剩余有效期。试验状态、结果和图片查询由 Web 网关注入管理员主体，Memory 对查询与取消复核主体/Generation；不同管理员不能凭试验 ID 读取或取消。结果、图片和遗忘预览服务现声明为管理专用，只能经包管理入口调用，不出现在公共依赖图和 RuntimeGraph。隔离包 UI 已加入图文上传、启动、状态、证据分页、按需原图分块显示和取消入口；定向 PostgreSQL 已验证分块幂等、绑定拒绝和到期撤销，HTTP 与前端还需完整真实会话验收。

## 8. 下一切换顺序

逐批遗忘的管理授权已完成同一路径联合验收：隔离 PostgreSQL 中冻结来源选择，真实 Edge 经部署 ASGI HTTP、隔离 iframe 和 Web authority ticket 完成预览、确认、提交与轮询；Memory 包自有 Worker 结算为 `SUCCEEDED`，页面及包内持久查询均显示选择 `COMPLETE`、一个来源单元被抑制（`1 passed`，106.85 秒）。该浏览器测试使用真实包契约与数据库，不连接 QQ；此前的分层网关/浏览器测试仍保留。大规模批次压力、发布崩溃重放及长期运行尚未验收。

1. 补齐大范围遗忘的批量压力、发布崩溃重放和部分完成后的恢复验收；逐批 P0 票据、部署管理网关、包自有 Worker 与真实浏览器到 PostgreSQL 的联合场景已验收。
2. 验证 Agent 语义召回的真实 QQ 群聊链、图片召回命中与跨群成功回复及联想提升；恢复及重放需与删除 epoch、Generation 围栏收敛。
3. 补齐证据浏览的进一步分页和长期混合图文负载验收。
4. 继续完成 [17](17-component-worker-host.md) 的包选择、显式启停和未变化 Generation 保留；对 Worker Host 配置控制事务验证热重载失败回滚、长任务 drain、群并发、审批、Provider/Tool 不确定结果和 WebUI 包管理贡献。
5. 在真实 OneBot 实例上只进行准许群的单次低频交互验收。
6. 将部署控制面从宿主 `deployment_management.py`、`deployment_provisioning.py` 和固定静态 UI 迁入独立 deployment/host 包及各自 Generation；切换通用 scope provider 后删除旧对象与伪 Generation，不能通过搬运文件或包装旧对象声称完成。

Bot 运行图已经是微内核包系统；部署/宿主控制面仍是缺口，目标重型 Bot 框架还需完成 Memory 主链与长期运行验证。

现行 Memory 包 UI 增加独立真实浏览器冒烟脚本 `scripts/smoke_memory_module.py`：本机 Edge 验证了隔离 MessageChannel 初始化、管理试验结果中的群号/作者归因、超过两个 96 KiB 块的原图显示，以及分块被篡改时摘要拒绝显示。旧管理静态目录已经移除，依赖它的旧面板脚本及仍引用旧页面 DOM 的旧整站冒烟脚本同步删除；此浏览器 fixture 不替代真实 HTTP 会话、数据库和供应商图文召回验收。

试验元数据回收现位于 `memory_graph` 包的向量删除 owned-work 认领事务：先有界抑制到期 facet 并为任何曾尝试派发的 facet 登记删除屏障；没有待领删除任务时，每轮最多回收一份到期试验、检查八份候选。Bot 级持久游标跨轮轮转和回绕，使前八份因有效租约而阻塞时，第九份仍能被回收。事务锁定试验输入，要求召回工作终结、所有 facet 已抑制、相关向量删除均结算、嵌入 claim/attempt/发布/恢复工作没有在途状态、预算预留已结算且没有成功回复联想日志。过期但没有外部尝试的旧 claim 与召回工作由该事务收敛，再按外键顺序删除试验 part、查询、facet、证据区域和包内结算行；向量删除 tombstone 与费用预算保留。仅对应 Bot、试验查询、bundle 和 claim 的不可变事实触发器在该事务内放行，正常群查询不受影响。真实 PostgreSQL 冻结 bundle/证据、过期 claim、有效租约阻断、向量屏障、游标轮转及并发认领用例通过，Memory 包 19 项数据库回归通过；Kernel Payload 的物理清扫仍按其独立 TTL 路径运行。真实供应商图文召回和长时间运行验收尚未完成。

部署管理网关现经真实本机 HTTP 与 Edge 浏览器验证登录、Bot 作用域选择、Memory 包 UI 的摘要读取与隔离 iframe 初始化、管理 query 的 MessageChannel 往返、卸载和登出。验收发现原 Blob iframe 继承控制台 CSP 后无法执行包内脚本，已改为带摘要的认证路由加载并独立施加 sandbox CSP；作用域路径改写曾把全局登出误送往 Bot 路由，现只改写 Runtime 与模块接口。切换工作区时，旧 UI 的异步挂载和 MessageChannel 会失效。该浏览器验收使用内存管理后端，证明 HTTP 会话和包 UI 接线，不证明真实 PostgreSQL 管理试验、供应商图文召回或长期运行。

旧 `FOREST_MANAGEMENT_TOKEN` 单 Bot HTTP 入口已删除；该入口使用另一套令牌 API，无法与现行部署会话控制台及包 UI 协议正确配合。`CleanManagementGateway` 现必须持有部署 Web authority ticket signer，Bot scope 由部署控制台提供；单 Bot 启动函数只管理 Bot 生命周期和状态输出。真实 Edge 会话下的 5 项部署管理验收通过，单 Bot 主机本地 5 项测试通过、5 项需数据库环境而跳过；此清理不替代真实数据库管理试验。

包管理动作的最后入口也已收紧：`PackageGenerationAdapter.invoke_management_action` 必须同时收到 Web authority ticket 与事务内授权 guard，缺失即拒绝；声明 `actor_field` 的动作只从非空 `web_subject_ref` 注入审计主体，不再使用 `management:shared-token`。隔离 PostgreSQL 下的 Memory 管理试验、Job Scheduler 动作和完整 Bot 宿主遗忘/重启链路已通过；这些测试使用显式测试票据和 guard，生产路径仍由部署网关签发、消费真实票据。大范围遗忘的长期压力验收尚未完成。

Memory 单次遗忘的执行阶段已将来源锁定、抑制记录、资格/观察更新、向量删除屏障、facet 和准备任务收敛改为有界集合写入，保留 Payload 删除纪元、固定影响计划、幂等操作回执和关系撤销。私有 PostgreSQL 上 19 项 Memory 包回归通过；修改后真实宿主遗忘、向量删除故障、热替换及重启链路通过。此改动是大范围遗忘的写入基础，批次计划与管理 UI 已另行接入，长期压力验收仍待完成。

Memory 已新增私有的 `forget_selections` 和 `forget_selection_members`：选择动作与来源浏览使用同一个 SQL 过滤函数，在 PostgreSQL 快照和接受时间下冻结最多一百万个来源的修订、作者载荷锚点、筛选条件、有序成员摘要和管理员主体；成员目录按序号分页，已冻结事实不可修改。同一创建操作幂等，新消息不会追加到旧选择，其他管理员不能读取成员。隔离 PostgreSQL 下新增选择、分页、主体隔离和来源浏览快照回归通过。选择目录与批次预览仍不能被标为完成了大范围遗忘。

批次预览现已接通 `memory.preview-forget-batch` UnitOfWork：Memory 从冻结目录按序号取最多 64 个当前有效成员，Kernel Payload 在删除时钟锁下对去重作者锚点形成闭包，Memory 再计算跨来源影响并固定成员、闭包、删除纪元和影响摘要。管理查询注入管理员主体，Worker 只携带选择引用与批次范围。真实 PostgreSQL 验证了两个根来源、共享派生 Payload、后续新来源隔离，以及包运行时管理查询的完整入口。预览本身只读；每批独立的 P0 授权、持久执行与批量 UI 已接入。

单来源 Memory 遗忘的事务写入效果现收敛为包内私有的 `memory_apply_forget_effects`：它按稳定顺序锁定所有受 Payload 闭包影响的来源，并统一写来源抑制、向量删除屏障、facet/准备任务失效和关系贡献撤销。单来源操作继续校验原有精确计划、保存原有幂等回执；这个私有函数是供批次事务复用的写入基础，不提供批次授权或批次执行入口。

Memory 包内现有私有 `memory.forget-batch` 三步 UnitOfWork：从冻结成员目录取根锚点，Kernel Payload 按预览的删除纪元及闭包摘要撤销，Memory 在同一事务内重算完整批次影响计划、复核管理员所属选择及成员修订，再复用遗忘效果函数并写入独立的不可变批次回执。错误计划使整笔事务回滚，重放返回原回执；PostgreSQL 用例覆盖两个根、共享派生 Payload、后续新来源隔离、错误计划回滚及幂等重放。该计划现由管理 P0 动作按每批固定影响计划入队，模块 owned-work 领取并执行，成功结果以不可变回执收敛；选择查询汇总完成批数、成员数、首个未处理序号与 FROZEN/PARTIAL/COMPLETE 状态。隔离 PostgreSQL 验证了入队幂等、主体隔离、批次领取、计划执行和结算。仍未验证大规模压力、崩溃恢复及真实浏览器的完整批次授权链。

管理图文召回试验现经完整 Bot 宿主、真实 PostgreSQL、包自有 owned-work、假 Embedding HTTP 和假 Milvus 贯通：两份经过管理资产权威绑定的文本/PNG 输入启动独立试验，自动准备/匹配/冻结/物化后达到 READY；结果分页返回原群号与作者，图片按归属 part 读取，另一管理员无法读取。发现并修正试验没有当前来源时证据节点把 `is_current_source` 投影成 SQL NULL 的协议错误，现明确为 `false`。宿主测试随后继续通过遗忘、向量删除故障重试、热替换和重启。冷启动时 30 秒 HostLease 在慢装配中可能过期；宿主现以十秒续期频率使用两分钟 TTL，测试验证本轮完整启动。

图文试验宿主验收现进一步走部署管理 App 的真实 ASGI HTTP 路由：登录会话经 CSRF 与 Web authority ticket，按管理资产声明分块上传文本和 PNG，管理 action 入队后由包自有 Worker 完成召回；状态、结果与归属图片均经 HTTP 管理 query 读取。此路径与真实 PostgreSQL、假 Embedding HTTP 和假 Milvus 的完整 Bot 宿主集成用例通过。当前证明 HTTP 协议及事务授权链路可用；真实浏览器对实际宿主、真实 Embedding/向量供应商请求和长期运行仍待验收。

Memory 包自带管理 UI 现接入已存在的冻结选择和有界批次预览端口：管理员从来源页当前筛选生成不可变成员目录，按序号分页核对根来源及作者锚点，并按起点/大小查看 Payload、来源、向量、关系及联想日志影响。界面现支持逐批确认、P0 授权入队、状态轮询与成功后进度刷新；每批单独授权，不自动执行整个冻结选择。真实浏览器下的不可恢复操作仍待隔离环境验证。

QiYu 配置中的 SiliconFlow `Qwen/Qwen3-VL-Embedding-8B` 已用于一次低频真实供应商请求：Forest 的 `EmbeddingClient` 按自身冻结请求格式同时发送一段文本和一张 64×64 PNG 的 data URI，收到 HTTP 200、两条 4096 维向量以及供应商报告的 41 个总 token（含 4 个图片 token）。该结果证明当前端点对 Forest 图文请求格式兼容；它没有经过 Bot 宿主的完整索引与召回链，也不证明远端模型修订身份或长期稳定性。

跨主干学习的隔离 PostgreSQL 验收现把“回复联想日志 → 归档提升”接到后续真实查询快照：后续群消息使旧触发来源不再是当前输入，包内邻接查询从另一群种子读到提升后的 `ASSOCIATED` 边；`graph_region.expand` 沿该边扩散，节点分别保留原群号、主干和作者。这证明包内关系与扩散路径可合流，仍未证明真实 QQ 的跨群成功回复或完整 Agent 证据物化。

真实 Milvus 与隔离 PostgreSQL 的向量索引测试现覆盖外部写入成功而确认丢失：测试专用 collection 中第一笔 upsert 已写入 Milvus，随后故意抛出传输错误；PostgreSQL ref 权威记录仍为空。关闭该 Generation 的驱动并重新创建后，相同 ref/向量重放成功、ref 变为 ACTIVE，后续检索与删除仍通过。该验证覆盖向量索引包的写后失联/重启窗口；Memory 包 Worker 的实际进程崩溃验证见下文。

完整 Bot 宿主的隔离 PostgreSQL + 假 Milvus 用例又在文档 facet 首次向量发布时注入“一条 ref 已写入、确认丢失”：Memory 包自有 Worker 将该批工作重试至少一次，所有文档 facet 最终 READY，后续召回、遗忘、热替换和重启仍通过。这证明 Memory 整批发布在运行中可从部分外部成功收敛；实际进程崩溃的后续验证见下文。

Memory 包的 PostgreSQL 用例还覆盖了另一个崩溃边界：批次发布事务已把全部 facet 置为 READY，但 Worker 尚未写任务结算就失去租约。租约到期后，新 claim 对同一批次重新计算并提交幂等发布回执；旧 claim 的结算被拒绝，新 claim 可结算为 SETTLED。此验证没有实际杀死进程；外部 Milvus 写入时的真实进程崩溃另见下文。

冻结遗忘选择的成员页现在按当前权威来源与抑制记录标记“有效 / 本选择已遗忘 / 外部已抑制”，并返回对应遗忘操作 ID；UI 在同一分页表中显示状态。真实 PostgreSQL 用例核对了批次事务提交前后的状态变化和管理员主体隔离。它展示已发生的成员事实，并与逐批 P0 授权、入队和状态查询配合；不自动继续执行剩余批次。

来源浏览现返回当前 DOCUMENT facet 总数、READY 数和四态覆盖情况，并允许服务端按覆盖状态筛选；冻结选择目录可沿用这一条件固定成员。PostgreSQL 用例覆盖无 facet、部分就绪和全部就绪及原来源游标。覆盖状态随 facet 发布实时变化，游标仅固定来源接纳事实，不保证跨页的历史覆盖快照，也不代表 Milvus 单次查询可用。

一次本地合成规模探针在受限 PostgreSQL 容器中插入 5 万条无 facet 来源：普通第一页、无 facet 第一页和无匹配的完整覆盖筛选分别耗时约 0.168、0.154、0.105 秒。它只检查该数据分布和查询成本，不能代替数月真实消息、混合图文 facet 与并发写入的长期压力验收。

长时在线观测脚本已按当前候选基线修正：检查 20 个模块、实际 `embedding_batch_attempts` 与预算预留的 deadline，并移除旧版 OneBot Binding/Client 表查询，因为当前模块不持久化连接状态。用现有在线 Bot 库做一次只读采样时，20 个 bundle 虽在，但缺少候选基线的 `embedding_batch_attempts`；探针明确返回 `BOT_SCHEMA_INCOMPATIBLE`，因此该在线库不能用作当前候选版本的长时验收。在线 Supervisor 和 Bot 未重启或写入，仍需在隔离的当前基线 Bot 库上跑完整探针与真实连接验收。

当前工作树重新构建候选测试镜像后，在独立无持久卷 PostgreSQL 容器中执行完整 Bot 宿主的文本、图片、跨群并发及成功回复四个场景，结果 `4 passed`（288.03 秒）。同一候选镜像与独立 PostgreSQL 再接入现有真实 Milvus，向量 ref 的写后失联重试、查询、删除围栏场景 `1 passed`（4.68 秒）；该测试自行删除临时 collection，测试 PostgreSQL 容器已移除。宿主四场景仍使用假 Embedding、假 Milvus、假 OneBot；Milvus 场景是索引包单独验收，不能合并宣称真实 Embedding＋Milvus 的完整宿主图文召回已通过。

宿主图片场景现在有凭据文件显式启用的真实传输入口：从 QiYu 的只读 `.env` 只提取 VL 嵌入配置，将 OneBot/模型响应保留为假客户端，Memory 使用真实 SiliconFlow `Qwen/Qwen3-VL-Embedding-8B` 和真实 Milvus。隔离 Bot 库运行时核对了 TEXT/IMAGE 查询 facet 均为 READY、其 ref 在索引包 PostgreSQL 权威表为 ACTIVE、两个查询匹配回执已提交。中途一次运行在嵌入前报 `MEMORY_QUERY_INPUT_UNAVAILABLE`；将图片捕获确定性延迟 2 秒后复现：pending input 为 RESOLVED、occurrence 为 ADMITTED，但未写 `eligibility_operations`，查询成员集因而为空。Memory 捕获事务现按与即时接纳相同的身份和摘要补写资格事实，schema 基线升级至 3.51.0。修复后的延迟捕获宿主场景 `1 passed`（77.75 秒）、Memory 包资格事务的五个参数分支 `5 passed`（202.06 秒），真实 VL＋Milvus 图片查询场景 `1 passed`（71.12 秒）；测试后删除临时 collection 与 Bot 库。当前证据只证明图文**查询侧**的真实嵌入与 Milvus 搜索，不证明已有文档图片命中后的跨群证据物化，也不等于长期实网稳定验收。

完整跨群图文链已有隔离验收：假 OneBot 在群 `933179609` 注入一条文字＋有效 PNG，真实 SiliconFlow VL 嵌入和 Milvus 把 TEXT/IMAGE DOCUMENT facet 发布为 READY；另一群 `933179610` 用相同图片触发 Agent，模型请求的证据清单出现原群 `933179609`、原作者 `qq:20001` 和准确来源单元。数据库中的 `query_seed_matches` 还确认 IMAGE 查询 facet 命中该 IMAGE 文档 facet，而非仅由文字相似度带出来源。最初手工调用 Memory 归档事务的用例 `1 passed`（77.43 秒）。随后将同一用例改为通过模块配置计划和热替换把 Conversation 窗口设为 2/1，连续注入四条无触发的群 A 消息；生产 `conversation.silent-drain` 自行提交裁剪和 Memory 归档，首条图文来源达到 ELIGIBLE，两个 DOCUMENT facet 均达到 READY，群 B 的图片查询与来源归因继续通过（`1 passed`，135.14 秒）。测试没有再直接调用归档 participant；临时 PostgreSQL 和 Milvus collection 均已清理。OneBot 与模型回复仍为假客户端，尚未验收真实 QQ 发送或数月运行稳定性。

旧的顶层审批等待、最终回复、会话交接、投递协调器及 Conversation/Memory 事务协调器现已删除；旧 `storage.conversation_scheduler` Repository、其独立 SQL 基线和专测旧基线的用例也已删除。生产 Conversation 仍由 `builtin/conversation_scheduler` 的包声明、私有 schema 和 UnitOfWork 运行，没有增加兼容层。随后 Agent 包状态用例改用当前包的租约 UnitOfWork 和公开 Run 查询端口，旧 `storage.agent_runtime_authority` Repository 也已删除。该用例为单独检验终态 UnitOfWork，经当前 Agent schema 的合法状态边设置 FINAL_READY 前置条件；真正的模型输出到回复提交仍由宿主成功回复用例验证。移除旧 Agent 文件后重建的 Python 3.11 镜像中，无数据库回归为 `452 passed, 85 skipped`，隔离 PostgreSQL 的 Agent 包用例 `1 passed`；临时数据库已移除。

管理图文召回试验现也经过真实 VL 与向量索引验收：在上述隔离宿主自动归档来源之后，管理员资产权威绑定一段文本和一张 64×64 PNG，Memory 包的管理 action 启动独立试验；包自有 Worker 使用 QiYu 的 SiliconFlow `Qwen/Qwen3-VL-Embedding-8B` 和真实 Milvus 完成准备、匹配、冻结与证据物化，状态达到 READY。试验结果包含原群 `933179609`、作者 `qq:20001` 和图像证据，`query_seed_matches` 确认本次管理试验的 IMAGE 查询 facet 命中该来源的 IMAGE 文档 facet（隔离测试 `1 passed`，143.01 秒）。此项真实供应商用例直接调用管理资产与模块 action，先前的假供应商用例另行验证了部署管理 App 的 ASGI 上传、会话、CSRF 与 Web ticket；尚未将真实供应商和真实浏览器合成一项验收。临时 PostgreSQL 容器与 Milvus collection 已移除，OneBot 和聊天模型仍为假客户端。

`component.worker-host` 的首条真实只读控制链已经进入 Bot RuntimeGraph：官方 Python Worker 包通过声明的 `service.call:runtime.graph.snapshot` capability 调用 `kernel.runtime` 的受限 Graph 投影，包管理 `module` query 从该投影读取准确组件 Generation；`module.list` Shell 命令贡献已从 `kernel.runtime` 移到此包。包清单构建、Python Ruff 和无数据库回归通过（`452 passed, 85 skipped`），隔离 PostgreSQL 的完整 Bot 宿主查询、热替换后新代继续查询及旧代拒绝调用、重启用例通过（`1 passed`，196.70 秒）。它尚未拥有可选工件目录、配置写入、启停意图、配置/启停管理 UI 或局部 Generation 重建；现有 Web gateway 的组件配置直接路径仍需按 [17](17-component-worker-host.md) 切换。

Worker Host 的 `module` 查询现从 `kernel.runtime` 持久 Graph 成员事实取得包 ID、版本、摘要及 DATA/CONTROL 接纳状态，包自带只读组件检查 UI 随 Generation 发布。包工件清单校验与页面脚本语法检查通过；最终 `1.1.0` 工件在隔离 PostgreSQL 的完整宿主用例中核对了 Agent 工件摘要等于实际加载快照，并再次覆盖热替换和重启（`1 passed`，204.12 秒）；无数据库回归 `452 passed, 85 skipped`，临时 PostgreSQL 已清理。本机 Edge 的隔离 iframe/MessageChannel 冒烟发现表单提交被 sandbox 拦截、HTML pattern 在新版正则下失效；页面改用受控按钮与回车事件后，两次组件查询及包身份展示通过。此页只读当前已发布组件，不提供包选择、配置管理或启停；跨模块配置提交与 Graph 重建申请的原子事务仍是下一项关键缺口。

Memory 整批向量发布又通过一次真实进程崩溃注入：独立宿主在真实 Milvus 确认第一条 DOCUMENT upsert 后立即退出，此时隔离 PostgreSQL 中该批 facet 仍为 `EMBEDDED`、向量 ref 尚无权威确认、发布工作仍为第一次 `CLAIMED`。等待原宿主和发布 claim 的真实租约自然到期后，以同一 Bot 库和包目录启动新宿主；同一批工作重领至少一次并达到 `SETTLED`，facet 全部转为 `READY`，对应 PostgreSQL ref 均为 `ACTIVE`（`1 passed`，359.36 秒）。这验证了 Memory Worker 与 VectorIndex 跨进程的写后崩溃重放；用例采用假 OneBot 与假 Embedding、真实 Milvus 和一次性 PostgreSQL，未发送 QQ 消息，也不替代真实 VL 或长期运行验收。RuntimeGraph 只读投影同时按 Bot 全局 DATA/CONTROL 准入与成员准入的交集报告有效状态，避免普通暂停后组件界面仍显示开放。

Conversation 的无触发 `SILENT_DRAIN` 现在每次只选择最多 64 条连续已分类消息，结算后由持久扫描领取后续批次，避免断线积压形成一次巨型输入事务。隔离 PostgreSQL 在停住包自有 Worker 后注入 65 条普通消息，再恢复 Worker，实际提交了序号 `4..67` 与 `68..68` 两批，分类/Context 水位最终均为 68（`1 passed`，42.66 秒）。这只封闭了静默输入选择的大小；带触发的超长前缀、已入 Context 后仍需继续裁剪的长尾维护，以及单次归档候选的硬上限仍待实现，不能据此宣称全部积压路径已受界。

Conversation 包现另有 `conversation.context-maintenance.pending` 自有任务：仅在会话空闲、输入已排空且没有未决触发时领取 actor；配置 Generation 或 Context/视图修订变化后重新检查，成功裁剪后逐批追赶，失败延时重试，旧 actor 不能结算新租约。每次归档候选硬限 64 个。`active_context_units` 是与不可变 Context/归档成员同事务更新的活动视图索引，Context 分页和裁剪旧前缀不再每次扫描完整历史。隔离 PostgreSQL 在输入水位 68 已排空时额外建立 67 个活动单元，维护任务无新消息自行完成 `64+4` 两次归档，活动索引与视图计数都收敛为 1；Conversation 和 Agent 包级用例 `2 passed`（122.47 秒），完整 Bot 宿主的记忆、热替换与重启用例 `1 passed`（198.96 秒）。带触发的超长输入前缀仍可能进入一个大事务，长期压力与实际运行验收也未完成。

RuntimeGraph 的启动服务边现由冻结包描述符的 `provides`、`effect_services` 和 `requires.startup_dependency=true` 编译，宿主已删除五处组件名手写启动顺序；非启动服务和 UnitOfWork participant 保持晚绑定。完整 Bot 启动、退役、重启用例在隔离 PostgreSQL 通过（`1 passed`，195.46 秒）。`clean_bot_host` 仍逐个安装官方包并硬编码初始配置，动态包选择尚未完成。

`component.worker-host` 增加 `modules` 包内管理查询，分页读取当前 RuntimeGraph 的已运行组件，返回稳定组件 ID、包版本与状态；后续页绑定 Graph ID，热替换后旧游标被拒绝。包内 UI 可从列表选择组件再查看 Generation 详情；Edge 隔离 iframe/MessageChannel 冒烟覆盖列表加载、选择与详情呈现。这是运行图浏览，不是可选择工件目录或配置/启停控制。包清单与局部回归 `108 passed, 1 skipped`，完整 Bot 宿主查询和热替换用例在隔离 PostgreSQL 通过（`1 passed`，196.37 秒）。

Worker Host 包现贡献 `module.reload` 群 Shell 命令，并经受能力声明约束的 `runtime.graph-replacement.request/2` 内核服务提交 Host lease 围栏内的持久重建请求。Worker Host 先核对目标当前在 Graph 内；内核仍原子检查期望 Graph revision，随后由原有宿主监督循环认领并结算。回执明确列出本版会换代的整张图，而不声称局部热重载。隔离 PostgreSQL 验证了缺失组件与过时 revision 均无请求、合法请求结算为 `SUCCEEDED`、所有成员 Generation 换代，以及同一操作经新代重放只返回旧请求（`1 passed`，112.80 秒）；Shell 文本 `#forest module reload --component-id agent.runtime --expect-graph 7` 也通过真实解析器检查。数据库测试调用 Shell 的类型化服务句柄，未向真实 QQ 发消息。Web 配置与启停仍由旧 gateway 直接操作，真正的 Worker Host 配置控制账本及局部 Generation 保留尚未实现。

Worker Host now owns a private durable reload-intent ledger. Web `module.reload` and Shell `module.reload` submit to the same package domain command; a package-owned worker claims the intent and requests the fenced kernel replacement with a stable operation ID. Expired claims are reclaimed, and kernel replay prevents duplicate graph replacements. The package `reload` query projects both the intent and the kernel's terminal outcome, and the package UI exposes queue and status actions. The isolated PostgreSQL full-host case covers a failed Web intent, successful Shell replacement of all Generations, and replay after replacement (`1 passed`, 96.37 seconds). Package build and Ruff pass. Web configuration apply and enable/disable still bypass this package, and unchanged Generations are not yet retained.

The obsolete package management-action flag `replace_runtime_graph` and its gateway branch have been removed. Management actions now return their package result directly; they cannot ask the Web gateway to rebuild the graph as a side effect. Configuration apply remains the one direct Web configuration write and synchronous Host replacement path. [17](17-component-worker-host.md) requires atomic commitment of the target configuration desired revision and the Host's `PENDING_REBUILD` fact through typed UnitOfWork participants, followed by an idempotent kernel replacement request. The standard configuration and Worker Host control participants are not yet installed, so this cutover is still incomplete. Local management/package checks (`91 passed, 3 skipped`) and isolated PostgreSQL Job Scheduler plus full Worker Host replacement (`2 passed`, 102.05 seconds) passed after the action contract removal.

The configuration cutover contract has been corrected to a static two-party UnitOfWork declared by each target package, with its own standard configuration participant and the fixed Worker Host control participant. This avoids a dynamic target slot and its ambiguous cross-target replay semantics. `ModuleConfigurationRepository.apply_in` now performs the existing authorized desired-CAS operation inside a caller-owned transaction; `apply` delegates to it. An isolated PostgreSQL test forces the following participant to fail and confirms that the desired revision and plan consumption both roll back, then the original apply succeeds (`1 passed`). The standard participant, Worker Host control participant, and Web route cutover remain to be implemented.

Worker Host now provides `component.configuration-rebuild.record/1` as a versioned UnitOfWork participant and owns the corresponding private intent ledger. A provisional standard configuration coordinator commits the target desired-CAS and the Host `PENDING_REBUILD` fact in one transaction; a rejected Host participant rolls both back. A package-owned task claims the fact, requests a fenced full-Graph replacement, checks the kernel outcome, and marks the intent `EFFECTIVE` only after the active Graph reports the target desired revision. The management query exposes this state without reading another package's tables. The isolated PostgreSQL full-host case verifies rollback, operation replay, Shell reload, and configuration convergence (`1 passed`, 145.24 seconds). This coordinator is not yet a descriptor-frozen target package participant, and Web configuration apply still uses its old direct path; no production cutover is claimed. Enable/disable and retaining unchanged Generations also remain open.

All 18 builtin package manifests now declare the standard `configuration_apply = "worker-host/1"` ABI and have new immutable package versions. The target Generation's package management contract binds its own authorized configuration repository step and the sealed Worker Host participant handle; its Web apply route commits desired and `PENDING_REBUILD` atomically and returns the rebuild request, without synchronously replacing the Graph. The generic configuration UI reports the queued request, and Worker Host UI can read the durable rebuild status. The isolated PostgreSQL full-host test uses a real Web authority ticket, observes `EFFECTIVE`, then replays the same operation with a new ticket and Generation without another Graph replacement (`1 passed`, 143.34 seconds); local package/runtime/management checks passed (`91 passed, 2 skipped`). Replay verifies the current Web policy inside the transaction but does not consume a second write ticket. The older direct replacement branch remains only for non-package kernel authority configuration, which must be migrated or removed before the legacy host stack can be deleted. Component enable/disable, package selection, and retaining unchanged Generations are still unfinished.

Builtin and generic package storage identities are now derived from the storage schema and execution profile rather than the full runtime package digest. A Python/UI-only package revision therefore keeps the installed storage identity while publishing a new immutable package Generation; a focused package build test confirms two runtime digests with one storage digest. This is a pre-release identity change: existing development databases with the earlier storage ledger need to be rebuilt, while unchanged-schema hot reloads after that rebuild no longer require a database reset.

Generic package UnitOfWork plans now reject the Worker Host configuration-rebuild record participant at manifest validation, and the participant checks the management-configuration causal root. This reserves the control intent for the authorized standard configuration transaction; the malicious-plan package validation case passes. Worker Host package version and baseline hash were refreshed for this pre-release guard change.

内核 `kernel.audit` 与 `kernel.authorization` 的管理配置现与 builtin 包共用同一标准组件配置事务：目标 authority Generation 内绑定自身配置仓库和 Worker Host 控制 participant，Web gateway 只调用该代管理契约，不再持有全 Bot 配置锁、直接写 desired 或同步执行 `replace_runtime_graph()`。配置提交返回持久重建请求，管理页面区分 desired 提交与 effective 生效。隔离 PostgreSQL 完整宿主测试覆盖 Shell 重载、包配置拒绝回滚与重放，以及内核授权策略配置从真实 Web ticket 提交至 `EFFECTIVE`（`1 passed`，191.92 秒）；本地管理/包回归 `92 passed, 2 skipped`，JS 与 Ruff 检查通过。测试使用独立临时库和假 OneBot/模型客户端，未向真实群发消息。显式启停、包选择、局部 Generation 保留及长期真实链路验收仍未完成。

Conversation 的 `CONTEXT_PREFILL` 已在此前实现：带触发前缀分 64 条提交，最终 `TRIGGER_INPUT` 保留最后一个触发，130 条消息及两处触发的隔离 PostgreSQL 用例验证了两次预填、原作者与触发领取。本轮将每次预填时的全量待触发 `count(*)/max()` 改为沿 `(bot_id, conversation_id, status, ingress_seq, trigger_id)` 索引读取最后一个待触发序号；原有切口和状态转移不变，包构建及该数据库用例再次通过（`1 passed`，41.24 秒）。极端情况下，最终批次仍会一次领取所有积压触发并返回完整触发列表；该集合尚无独立分页/分段认领协议，不能宣称触发数量本身已受界。

标准配置事务的调用作用域现从包专用 `PackageCallScope` 分离为 `ComponentConfigurationApplyScope`；内核 authority 不再构造包 Worker 身份，包 Generation 只在普通服务和通用 UnitOfWork 使用包作用域。新作用域保留精确组件/Generation/调用/operation 身份，`management.configuration` 因果根由协调器固定生成。包与 Web 管理本地回归 `92 passed, 2 skipped`；隔离 PostgreSQL 完整宿主测试再次覆盖包配置拒绝回滚、跨代重放及内核授权配置到 `EFFECTIVE`（`1 passed`，189.87 秒）。临时库和容器已清理。

Memory 图的高出度 `ASSOCIATES_WITH` 邻接读取现使用贡献 ID 的持久伪随机键和查询/源节点派生的游标起点，从双向局部索引分别读取起点后及回绕前的有界候选，再合并选取扩散边。可见性、证据及快照资格在各段达到上限前验证；同查询结果稳定，换查询通常改变窗口。隔离 PostgreSQL 用例覆盖双向索引可用性、跨主干采样、隐藏证据、快照与来源归因（`1 passed`），包构建及 Ruff 检查通过。大量不可见边仍可能增加索引扫描长度；该方案是伪随机窗口，不宣称独立均匀抽样。长期真实群聊及完整遗忘管理仍待验收。

Conversation 最终回复现在在持有会话 lane 锁的事务中校验 Agent 提交的触发 ID 无重复，且与该 Session/Run 仍处于 `CLAIMED` 的完整集合相等。此前只验证提交集合属于当前 Run，漏列触发会在 Run 完成后遗留孤儿领取；现在整个回复提交拒绝此情况。隔离 PostgreSQL 的长前缀预填场景以两个不同作者的触发验证漏列和重复均不改状态、完整集合可以回答（`1 passed`）；完整 Bot 宿主的正常成功回复链也通过（`1 passed`）。包构建与 Ruff 通过。最后一次领取仍会返回无界触发数组，而最终回复输入最多允许 4096 个 ID；超过此数尚无分段归属协议，本修正只封闭集合正确性，不宣称规模上界已解决。

Conversation 失败/取消终结现按 `(Session,Run)` 精确查验和更新触发，不能以 Session 为范围波及其他 Run。隔离 PostgreSQL 用例在同一 Session 中注入另一个 Run 的领取并回滚探针事务，验证当前 Run 只终结自己的触发（`1 passed`）；包构建与 Ruff 通过。对超长触发积压的目标协议已在 [08](08-group-context-interaction.md#24-超长触发积压的传递与结算) 写明：按最多 64 个消息/触发的领取分段维护组引用、累计摘要，Agent 和最终回复使用引用而非整组数组，终态再按有界批次结算。当前代码仍传递无界数组，分段领取、精确资格端口和组级终态都尚未实现。

最终回复与失败终结的跨模块回执现由 Conversation 自己确定当前 `(Bot,Conversation,Session,Run)` 的完整已领取集合；Agent 不再输出整组触发 ID，调用方也不再回传整组 ID。最终回复事务记录已回答数量并更新该 Run 的全部领取，只向 Memory 返回最多 128 个学习提案实际引用的触发及其事件来源；无学习提案时返回空来源列表。失败终结同样只返回终结数量；旧的未使用 requeue 表和逐条终结成员表已删除。隔离 PostgreSQL 验证长前缀双触发回复、提案来源限界、跨 Run 终结隔离和事务重放；Memory 包回归及完整 Bot 宿主成功回复链通过，包构建和 Ruff 通过。初始选择、Agent Run 输入快照及相关模型投影仍含整组触发数组，单次最终领取仍可能超过 Agent 输入协议的 4096 条上限；当前事务也仍批量更新整组触发，尚未实现 [08](08-group-context-interaction.md#24-超长触发积压的传递与结算) 的分段组引用与有界终态结算。

工具调用的触发资格现由 Conversation 的 `conversation.authorize-run-trigger` UnitOfWork participant 按精确 `(Bot,Run,trigger)` 查询，并返回该触发原有的 Session、群聊与 principal；Agent 在同一 `agent.record-model-turn` 事务内核对这些身份与自己的 Run/Session，再写 ToolCall。普通无工具 ModelTurn 使用同一计划的空资格回执。Agent 不再靠私有 `run_input_triggers` 副本判定工具资格，这使后续删除逐触发 Run 副本有了权威端口。隔离 PostgreSQL 分别验证了合法与跨 Run 触发、Agent ModelTurn 状态事务，完整 Bot 宿主的普通成功回复链通过；尚未用真实群聊和真实工具执行验证该链。`run_input_triggers` 本身和启动/模型快照里的整组数组仍在，分段领取与组引用仍未完成。

Agent 的私有 `run_input_triggers` 表现已删除。每次 `run_inputs` 仅保留触发数量、按来源序号选出的末触发身份与序号、来源 manifest 摘要；Run 查询和模型投影不再按触发数量聚合成员列表。最终学习仍以冻结模型请求的 `input_revision` 找到该输入的末触发，工具资格继续由 Conversation 的精确端口提供。Agent 包级 PostgreSQL 用例覆盖新建、追加与后继 Run，完整 Bot 宿主成功回复链、热替换与重启用例通过；模型投影本地用例也通过。此时 Conversation 最终选择仍把所有待领取触发聚成数组并一次更新，Agent 初始/追加输入计划仍接收该数组；该缺口由下一轮改动关闭。

Conversation 选择此前把逐触发成员留在自身规范化表中，跨模块只传 `trigger_claim`：以 `input_batch_id` 为领取引用，带计数、末触发身份/序号和按领取顺序滚动的 SHA-256 摘要。Agent 启动、追加、后继 Run 以及模型输入快照均只保存有界摘要，不再接收或重聚逐触发数组；工具授权继续向 Conversation 按单个触发精确核验。Conversation 与 Agent 的隔离 PostgreSQL 用例、宿主成功回复链和本地模型投影用例通过。此时引用仍只代表**单个 InputBatch**，选择事务一次更新所有待触发；该局限由下一轮分段改动缩小。

Conversation 预填现以目标 Session/Run 领取当前最多 64 条连续消息内的触发，最终输入只领取剩余窗口，并将每段成员保存在 `input_batch_triggers` 与不可变 `trigger_claim_segments`；可更新的 `trigger_claim_groups` 头保存该 Run 累计计数、末触发和滚动摘要。Agent 收到的是稳定组引用及该次选择时的累计快照。预填不提前更改活动 Run；`SESSION_END` 预填使用后继 Run 引用。隔离 PostgreSQL 用例让 130 条消息均触发，验证预填两段各 64 项、最终段 2 项、同一组引用、幂等重放与最终回复归属；同一用例还核对 `SESSION_END` 预填把 64 项领给后继 Run，而活动 Run 在交接前仍指向旧 Run。Agent 包级状态和完整宿主成功回复链也通过。最终回复和失败终结现核对组计数与实际已领取数。不过回复/失败仍在单事务批量更新全部成员；组级权威终态、可恢复的 64 项物理结算及失效重排仍待实现。因此超长积压的**领取**已分段，**终态**尚未完成，不能称为 [08](08-group-context-interaction.md#24-超长触发积压的传递与结算) 全部达标。

预填失租约恢复现保持原 Run 身份。`trigger_claim_groups` 增加单向 `ACTIVE→TERMINAL` 状态：最终回复或失败终结必须有对应的不可变操作事实，才能关闭该组；`IDLE` 选择若试图用不同 Session/Run 越过未终结预填组会被拒绝。新 actor claim 在没有活动 Session 时复用唯一活动预填组的 Session/Run，并仍以 `resume=false` 创建尚未提交的 Agent Run；`SESSION_END` 后继 Run ID 改为仅由群聊和已完成 Run 派生，actor epoch 变化不再改变目标。隔离 PostgreSQL 用例核对了 64 项预填后的租约失效重领、异 Run 选择拒绝、最终回复后组终结，以及后继预填不提前替换当前活动 Run。此路径解决 IDLE 预填失租约造成的孤儿组；当时仍未提供显式撤销/重排或有界终态结算。

Conversation 终态现在先提交权威组事实：成功回复使用不可变 `context_append_operations`，失败/取消使用不可变 `trigger_terminal_operations`，组头在同一事务中记录终态和操作引用，并建立独立结算游标；跨模块回执只含计数和有界学习来源。物理触发行随后由 `conversation.trigger-settlement.pending` owned-work 每次按不可变领取分段最多更新 64 项。结算 UOW 核对租约 token、epoch、分段序号、原 Run 归属和成员数，提交回执后推进游标；迟到旧租约被拒绝，新租约可重领未完成分段。物理收敛前，内部有效状态查询、工具资格和归档资格均读取组终态，避免把已终结触发误当成活动输入。隔离 PostgreSQL 用例覆盖 130 条触发的 64/64/2 结算、失败组的有效状态与物理状态、过期租约的迟到写拒绝；完整宿主成功回复用例核对 owned-work 自动把触发物理收敛为 `ANSWERED`。仍缺少失败/取消后的显式受控重排、异常孤儿组处置和数月规模的长期验收。

终态提交不再扫描整组触发行计数，直接使用每次领取时以最多 64 项事务更新的组头 `trigger_count`；回复、失败和取消均要求组仍为 `ACTIVE`。逐触发行进入终态时，数据库触发器还核对它在不可变领取分段内的成员身份、组级权威终态及对应回复/失败操作事实。没有显式重排事实的 `CLAIMED→PENDING` 转换已关闭，避免失租约或误操作绕过组事实复活触发。这让终态提交的读取量不随组长增长；组成员物理写入仍由分段结算完成。受控重排尚需单独设计和实现，不能直接恢复旧的无条件状态回退。

Memory 的冻结遗忘选择查询现从不可变 `batch_forget_operations` 按选择计算已完成批数、成员数、首个未处理序号及 `FROZEN/PARTIAL/COMPLETE` 状态；该读取有 `(Bot,selection,ordinal)` 索引，模块 UI 同页展示进度和逐成员的实际抑制归属。隔离 PostgreSQL 的真实批次事务用例核对执行前后进度由 `FROZEN` 收敛为 `COMPLETE`。这只是已提交事实的持久只读投影，不代表 P0 授权、自动继续执行、失败重预览或跨批任务控制已经开放；这些仍待补齐。

通用 Web 配置栏现在把 apply 回执严格表述为 desired 已提交，保留 Worker Host 的持久重建请求 ID，并在当前 Bot 工作区内轮询其 `EFFECTIVE/REBUILD_FAILED` 状态。只有 `EFFECTIVE` 回执才报告“已生效”；重建失败会保留已提交 desired 与错误码。apply 成功后的页面刷新若失败，也只报告刷新失败，不重新开放提交按钮或把已提交事务报成失败。Memory 与 Process Executor 的包内 UI 同步改用 `desired-committed` 事件，不再把提交当生效。三处 JavaScript 语法检查和实际包清单验证通过；不再由任何贡献引用的旧 `clean-agent`、`clean-onebot` 静态页面已删除。此修正不等于实现了显式 enable/disable 或动态候选图；它们仍须按 [17](17-component-worker-host.md) 完成。

逐批遗忘补齐了“事务已提交、Worker 未结算”故障窗口的隔离 PostgreSQL 验证：测试使持有领取的租约过期，下一次认领先读不可变 `batch_forget_operations` 回执，将队列直接收敛为 `SUCCEEDED`、保留原尝试次数和效果，且旧领取不得迟到结算，也未再次提高 Payload 删除纪元。真实 Edge 在隔离 MessageChannel fixture 中走通批次页展开、选择读取、预览、确认、携稳定幂等键的 action、状态轮询和进度刷新；该浏览器 fixture 模拟管理回执，不证明真实 Web 票据签发、PostgreSQL 入队或大规模批次压力。

局部 Generation 保留的内核边界已核对：当前 Registry 的候选图构造、`kernel.runtime.publish_graph` 对全部成员的 READY/退役处理，以及 Contract Router 对准确 Graph ID/revision 的封口，共同决定了 `module.reload` 只能换整图。目标协议和需要保持的成员、路由、租约不变量已写入 [17](17-component-worker-host.md#局部换代的内核边界)；显式启停及真正的局部换代仍未实现，不得把现有整图重建标为模块级热重载完成。

Worker Host 包新增 `packages` 只读管理查询。它通过声明的 `deployment.package-registry.packages` 路由访问部署共用的 `PackageCatalog`，按目录修订分页读取、逐页校验不可变包字节，只返回包/组件身份、manifest 与工件摘要和能力申请，不暴露 ZIP/路径；包内 WebUI 折叠展示候选。包目录并发新增后旧页返回 `PACKAGE_CATALOG_CHANGED`。本地目录/包运行时回归 `8 passed`，隔离 PostgreSQL 完整 Bot 宿主在激活的 Worker Host Generation 上查询官方包候选，并完成原有热替换/重启链（`1 passed`，230.85 秒）。这一步只建立可审计的候选浏览端口，尚无部署安装权威、Bot 级选择/启停事实、兼容性验证或未变化 Generation 保留。

Worker Host 的 `runtime.graph-replacement.request/2` 现携带目标组件 ID，内核在期望 Graph revision 上验证成员身份，并把目标纳入幂等摘要。Registry 将启动 DAG 与保留依赖分开：普通服务、UnitOfWork participant 和配置控制引用可成环，但在局部换代规划中参与反向闭包；冻结 Generation 绑定及内核图事实记录保留依赖。独立 PostgreSQL 验证了缺失目标不入队、同操作不同目标冲突、保留者不能依赖被换代者，以及图状态显示目标；Worker Host 的完整宿主重载用例通过（`1 passed`，288.01 秒，未向 QQ 发消息）。宿主仍执行整图重建，尚未消费目标闭包或转移未变化 Worker 实例；这项工作仍未完成。

`kernel.runtime` 新增成员级 admission 围栏：在当前 Graph/Host lease 事务内核对成员集合及启动、保留依赖的反向闭包，不闭合时拒绝；冻结与失败恢复只递增选中成员的 route epoch。隔离 PostgreSQL 验证了遗漏依赖消费者会被拒绝、合法叶子成员冻结/恢复后未选中成员 Generation 和 route epoch 均保持不变，原整图路径仍通过（`2 passed`）。宿主尚未调用成员级端口，局部实例转移与真实局部热重载仍未完成。

`RuntimeModuleRegistry.drain_owned_work_for_replacement()` 已支持选中成员：它调用内核依赖闭包围栏，只冻结/排空选中组件的 owned work，保留成员的 Router admission 持续开放；失败恢复按同一成员集合回滚。轻量生命周期验证通过（`10 passed`，其中新增用例核对未变化组件不被冻结、同操作改目标冲突和精确恢复）。宿主替换循环仍使用整图模式，实例所有权与旧 Router 寿命转移尚未接通，因此 `module.reload` 仍是整图重建。

Worker Host 的目标组件重载现执行局部换代。宿主比较候选与当前冻结描述符，把目标、变化组件和依赖消费者纳入换代闭包；候选只为闭包预热新实例，其他组件沿用原 Generation、Python 实例和 Worker，且不重复激活配置。发布成功后新 Registry 接管保留实例的监督及最终关闭；旧 Registry 只关闭换代成员，原 Router 保留至其最后一个原 Generation 被关闭。隔离 PostgreSQL 的五成员图验证了单目标换代、四成员身份与实例保留，以及候选预热失败不关闭借用实例（`1 passed`）；完整官方包宿主在 Fake OneBot 下经 Shell 请求、跨图旧句柄重放和后续配置重建走通（`1 passed`，301.62 秒，未向 QQ 发消息）。动态新增/移除成员、部署工件选择与发布后的激活故障恢复仍未完成，不能把全部组件控制面标记为生产验收通过。

RuntimeGraph 热替换的发布后故障现按故障边界处理：候选图提交后若本地 Generation 激活失败，Registry 撤下候选路由，Host 不再尝试恢复已退役的旧图，也不把替换请求结算为成功；宿主监督器使该 Bot 失败并关闭两代资源。发布前失败仍可恢复旧图；若排空、恢复或清理本身失败，则宿主同样失败。针对性本地用例 7 passed，隔离 PostgreSQL 发布后激活故障用例 1 passed，Ruff 通过。新宿主自动恢复策略及动态成员增删、部署包启停仍未完成。

RuntimeGraph 成员集合变化已贯通 Registry 规划与宿主替换入口：候选与来源成员取对称差，叠加描述符变化和启动/保留依赖消费者闭包；纯新增跳过旧图排空，纯移除允许零个新建 Generation 并保留其余实例、路由和身份。隔离 PostgreSQL 用例经宿主入口依次加入、移除真实 Counter builtin 包，核对五个内核 Generation 和实例始终不变、移除后旧包路由失效（1 passed）。该阶段只有图执行能力，尚未读取 Worker Host 的持久选择事实；后续进展见本文件末尾。管理端安装/校验/选择仍未完成，不能宣称用户可经 WebUI 动态安装插件。

已安装外部包的独立装配入口现从 PackageCatalog 按不可变摘要取快照，要求 Bot scope、Python Worker、静态配置 schema 与精确批准的能力集合；框架标准配置账本和外部包声明式 SQL 分别安装到独立 namespace，后者以稳定存储契约摘要绑定，绝不借用官方 SQL 执行权限。真实 PostgreSQL 用例从 ZIP 导入带声明式表的测试插件，并以 1.0.0 -> 1.0.1 两个不同工件摘要热替换，确认未变的 SQL schema 可复用且内核 Generation 保留。外部包 SQL 函数、UnitOfWork participant、owned-work SQL 与 host effect driver 仍被明确拒绝，需后续独立受审查 authority；部署 Web 安装、Worker Host 选择事实和宿主自动装配尚未接通。
本轮隔离 PostgreSQL schema/运行时套件 44 passed；包、Worker 与宿主非 PostgreSQL 套件 98 passed、1 skipped；Ruff 和 diff 检查通过。测试使用独立数据库与假 Bot 链路，未向 QQ 发消息。

部署包目录现有精确工件摘要读取：`PackageCatalog.artifact` 对每次查询重新验证内容寻址工件，返回与分页目录一致的身份及 manifest 摘要，并附配置 schema 摘要、能力申请、服务依赖/提供声明和存储执行权限。受限 `deployment.package-registry.artifact` 只读服务与 Worker Host 包内 `artifact` 管理 query 已接通，包内 UI 可展开检查声明。18 个 builtin 工件投影均通过输出 schema 校验；包目录本地回归 `84 passed, 1 skipped`，完整官方宿主的真实 Worker Host 查询、目标热重载和配置链在隔离 PostgreSQL 通过（`1 passed`，264.65 秒），临时容器已清理。它仍只是安装与选择前的可信候选声明读取；部署 Web 安装、完整 `module.validate`、Bot 私有选择事实、显式启停及宿主按选择装配尚未完成。

接入组件选择前清除了未被生产代码调用的旧 `module_validation.py`：它将所有服务要求当作同一启动 DAG，和现行 RuntimeGraph 的启动依赖/保留依赖分离规则冲突。两处仅针对旧规划器的测试随之删除；其余包声明与运行时本地回归 `34 passed`，Ruff 通过。后续 `module.validate` 必须复用实际 Registry 候选图规则；选择修订、授权写入和宿主冻结读取仍待实现。

Worker Host 私有 schema 已加入 Bot 级选择修订头、每组件选择条目、幂等操作账本以及独立启用/停用 SQL 端口；选择快照只读查询按组件排序返回事实。宿主侧 `ComponentSelectionReader` 可在共享 PostgreSQL 选择围栏下读取规范化快照和摘要，并在发布期间持有该围栏；选择写入使用相同键的独占围栏。隔离 PostgreSQL 用例验证两笔同修订并发写只有一笔成功、操作重放和输入冲突、快照投影、发布围栏阻塞新写及旧摘要拒绝（`1 passed`）。这只是内部持久/读取契约：尚未通过目录工件校验后开放 Web/Shell 管理动作，内核请求尚未携带冻结选择凭据，宿主尚未按选择装配候选图。

宿主候选装配现消费 Worker Host 的选择快照：builtin 条目控制候选图成员，外部包条目按目录中精确工件与 manifest 摘要核对后装配；与官方组件 ID 冲突、非法内核目标和停用 Worker Host 均拒绝。发布前在数据库共享选择围栏下复核修订与摘要，写入不能在旧候选发布期间抢先完成。外部包热重载可从目标私有配置账本读取已初始化的 desired；不存在的配置明确拒绝，schema 摘要或实际值不符合当前工件则拒绝。隔离 PostgreSQL 已验证首次未初始化拒绝及同一配置账本跨外部包代码版本换代，完整官方宿主的空选择热重载回归通过（`1 passed`，278.97 秒）。同一完整宿主用例随后经内部 SQL 选择事实热加入、停用已安装 Counter 包，核对所有原有 Generation 保留、新包获得 Generation、停用后旧代停止（`1 passed`，346.28 秒），未向 QQ 发送消息。Web/Shell 授权选择、冻结内核请求和自动执行仍未接通。

内核图替换请求现支持独立的 `/3` 冻结选择契约，持久保存选择修订和规范化快照摘要，并把它们纳入幂等输入摘要；仅携带该凭据的请求可指向旧图中尚不存在的组件。宿主消费请求时先比对已装配候选的选择身份，发布时仍持有选择共享围栏并再次核对；不匹配则失败，不能用更新后的选择冒充旧操作。普通 `/2` 重载维持原目标成员检查。隔离 PostgreSQL 请求、操作重放和摘要冲突用例 `1 passed`，宿主本地回归 `7 passed`。Worker Host 尚未自动把内部选择操作提交给内核，也没有 Web/Shell 授权选择入口；前段历史段落描述的是各阶段当时的差距。

Worker Host 现于选择写入事务中保存不可改写的操作后快照，并在独立的 `selection_applications` 表排入持久任务；包内 owned-work 领取后比较当前快照和 Graph revision，按规范化快照摘要提交内核 `/3` 请求。过时任务结算 `SUPERSEDED`，执行状态、回执与错误留在包私有账本。隔离 PostgreSQL 已验证并发选择 CAS、快照冻结、任务领取/结算与操作不可变（`1 passed`）；完整官方宿主在假 Bot 链路下由任务自动热加入并停用已安装 Counter 包，原有 Generation 保留（`1 passed`，368.35 秒）。这仍是内部端口：部署安装、完整候选验证及 Web/Shell 授权选择尚未接通，不能宣称用户已可从管理界面安装插件。

Worker Host 包内管理页现提供显式选择快照与选择请求状态查询；后者将包私有任务状态和内核 Graph 替换结算分别显示，避免 `SUBMITTED` 被误读成成功发布。包声明与 UI 脚本语法已验证，本地服务状态用例 `1 passed`。对外启停仍保持关闭：现行通用 management action 只允许直接映射 Domain Command，无法在同一次授权写入前完成工件、配置和真实候选图预验证。设计文档明确要求先扩展预验证加事务复核契约，再发布两个独立授权动作。

在重新启动 Docker、确认宿主机端口可连接并把 pytest 临时目录移到 D 盘后，完整 Worker Host PostgreSQL 用例通过（`1 passed`，392.39 秒）。它按顺序验证组件重载、两次配置重建、选择任务驱动的外部 Counter 热加入与停用，以及内核结算后的旧 Generation 停止；均使用假 Bot 客户端，未向 QQ 发消息。上次在 `QUIESCENT` 上失败是测试先于旧 Registry 排空读取状态的时序问题，现于内核选择请求 `SUCCEEDED` 后断言。此结果只覆盖内部选择执行链；部署安装、无副作用的完整 `module.validate`、Web/Shell 授权选择和长期运行仍未验收。

Worker Host 选择任务的崩溃重领现保留原 Graph 围栏直接请求内核，不再先以当前 Graph revision 将旧请求判为失败；内核先按稳定 operation ID 重放已有请求，只有新请求才执行 Graph revision CAS。包版本升至 `1.6.3`，本地 Worker Host 用例 `8 passed`，隔离 PostgreSQL 的内核请求跨 Graph 发布重放用例 `1 passed`，新包清单构建通过。选择快照被后续操作覆盖时仍按原规则结算 `SUPERSEDED`；这项修正不代表 Web/Shell 选择入口已开放。

正式 `RuntimeModuleRegistry` 的换代规划现拆为只读 `preview_graph_transition()` 与应用计划的 `plan_graph_transition()`：前者复用现有描述符摘要、启动依赖和保留依赖闭包，返回替换、保留、移除、新增以及排空/预热集合，不写入候选实例或绑定保留所有权；后者消费同一结果执行原有局部换代。非数据库 Registry/宿主回归 `18 passed`，隔离 PostgreSQL 的真实局部替换和纯增删成员两项用例 `2 passed`。这为 `module.validate` 提供了唯一的影响集合计算入口；精确工件/配置的无副作用描述符编译随后已接通；计划摘要随后已接通；授权提交复核仍未接通。

已安装包的静态约束现由装配与 `inspect_installed_package()` 共用：精确工件/manifest 摘要、Bot scope、Python Worker、能力批准、外部 SQL/Effect 权限和配置 schema 均使用同一判定。目标配置 desired 值、修订和不可变字段在一个 `REPEATABLE READ` 快照中读取；为保留 Payload 删除围栏仍获取短暂共享锁，但无持久写入。生产装配在引用已初始化配置时先预检，缺失配置不会先安装 schema；隔离 PostgreSQL 的外部包热重载用例 `1 passed`，并核对预检及失败装配前后 schema bundle 数量不变、过期配置修订被拒绝。本地包目录/装配回归 `84 passed, 1 skipped`。检查结果随后已接入候选描述符编译与正式 Graph 预览；计划事实摘要随后已接通；完整 `module.validate` 仍缺授权提交事务复核。
已安装包现在可经 `compile_installed_candidate_descriptor()` 用生产包编译器生成无 factory 的只读候选描述符，再由正式 Registry 预览 Graph 影响集合。含规划描述符的 Registry 在 Graph 准备前被拒绝。外部包两版本热重载的隔离 PostgreSQL 用例验证：预览影响集合与实际生产候选一致、预览不能启动且 Graph revision 不变（`1 passed`）；本地包与依赖规则回归 `101 passed, 1 skipped`。Bot 选择集合编译与事实摘要随后已接通；只读 `module.validate` Web 管理查询随后已接入；资源预算、授权提交复核及 Shell 入口仍未完成。
生产宿主与候选预览现共用 `select_package_members()`，不再分别维护 builtin 启停、外部包身份及精确工件选择规则。只读 `project_component_selection()` 的修订、排序与摘要经 PostgreSQL 选择账本对照；`preview_selected_graph()` 用冻结快照编译全部启用外部包的无 factory 描述符，要求目标外部包的精确配置修订，并复用正式 Registry 计算影响集合。宿主 `preview_selection_transition()` 在 Graph 替换锁内调用它。选择账本及外部包热重载 PostgreSQL 用例 `2 passed`；宿主本地回归 `8 passed`，相关包与宿主非数据库回归 `99 passed, 1 skipped`。官方描述符当前取自最近一次真实装配；预览随后已生成事实摘要；只读 `module.validate` 随后已接入；资源预算、计划摘要的授权提交复核和 Web/Shell 启停入口仍未完成。
候选 Graph 预览现把官方与外部包的当前 desired 配置修订、Payload 身份、schema/存储摘要一并冻结为只读事实，并使用正式 Registry 的描述符投影计算 `plan_digest`；摘要还包含 Bot、来源 Graph ID/revision、选择快照 revision/digest、目标组件和完整影响集合。`ComponentSelectionSnapshot` 集中校验排序、条目修订及规范化摘要，读取器不再重复检查。隔离 PostgreSQL 的外部包换代用例验证重复预览摘要稳定、工件换代摘要变化且真实热替换影响一致（连同选择账本用例 `2 passed`）；相关本地回归 `99 passed, 1 skipped`。资源预算、事务内计划复核、授权管理动作与真实长时运行仍未完成，因此摘要尚不是可提交的启停授权。

Worker Host 已声明 `component.host.validate/1` 只读管理查询，通过受限内核服务调用宿主的单笔选择投影和正式 Graph 预览，返回影响集合、冻结事实及计划摘要；已启用外部包的目标配置修订必须精确指定。官方包检查在只读路径中有界并发，目录工件校验在线程执行，以免同步文件校验阻塞 Worker 的服务期限。查询不写选择、配置或 Graph，也不构成启停授权。资源预算、授权提交事务的重新核对和 Web/Shell 启停入口仍未完成。

Graph 描述符构造已改为读取 Generation 预编译的管理 schema 副本，避免每次预览为同一 query/action 重复扫描不可变工件。完整 PostgreSQL 宿主用例经真实 Worker Host 管理查询取得计划摘要与影响集合，核对选择和 Graph 未变化，然后完成后续重载、配置重建及外部包热加入/停用（`1 passed`，170.67 秒）；相关本地回归 `99 passed, 1 skipped`。这是只读预览的验收，尚未覆盖已安装包的 Web 授权启停提交。

Worker Host 包内管理页现有折叠式候选图预检，按实时选择修订与当前 Graph revision 调用只读 `validate`，展示计划摘要和六类完整影响成员；外部包的工件、manifest、能力批准与目标配置修订由管理员显式填写。页面不提供启停提交。包版本升至 `1.6.5`，内嵌脚本通过 Node 语法检查，完整包工件构建通过；系统 Edge 的真实 MessageChannel 冒烟验证了请求修订、目标及影响集合渲染。Web 授权提交、资源预算及长时运行验收仍待完成。

只读候选预检的并发事实边界已收紧：宿主先取 Graph 替换锁和选择共享围栏，再以另一个 `REPEATABLE READ` 连接读取候选中全部包的配置与 Payload；配置读取复用调用方事务，避免每个包各取独立快照后拼出不存在的事实组合。若未读到的配置在该快照建立后被并发改写，预检以序列化冲突失败。隔离 PostgreSQL 的双配置并发改写、外部包 Graph 预览、选择 CAS 和完整 Worker Host 管理查询/热重载用例各 `1 passed`；后者耗时 173.95 秒。本地宿主和 schema 非数据库用例分别 `8 passed`、`11 passed`，Ruff 通过。宿主已移除无调用者的旧预检包装方法。此边界仍是只读预检，启停提交所需的同事务授权、资源预算与计划复核尚未实现。

通用管理 action 现可在 manifest 声明 `transaction_preflight` 宿主能力；Generation 装配和调用均在缺失绑定时拒绝，action ID 与 Domain Command 端口强制对应。前置检查作用域先于 Domain Command 事务进入，Web 票据和检查回调在写入前于同一事务执行，等待锁也计入 action 超时。Domain Command 返回值现在于事务内通过包的输出 schema，错误输出回滚包内写入。包运行时本地用例 `8 passed`；隔离 PostgreSQL 的 Job Scheduler 管理 action 用例证明故意制造输出 schema 错误时 enqueue 账本未提交，正常提交和热重载仍通过（`1 passed`）。Worker Host 的启停 action 尚未声明，组件选择具体检查能力、资源预算和 Web/Shell 入口仍未实现。

组件选择 SQL 已限定只接受内核标记的 `management_action` 端口种类，普通 Domain Command 不能绕过管理事务调用选择写入。Worker Host 包版本升至 `1.6.6`、私有 SQL schema 版本升至 `1.4.0`；隔离 PostgreSQL 的选择 CAS/幂等重放用例验证普通命令被拒绝，管理端口的选择操作与发布围栏仍通过（`1 passed`）。这是对未来启停提交的必要封口，启停 action 当前仍未开放。

包清单与运行时非数据库回归 `92 passed, 1 skipped`，Job Scheduler 管理 action 的 PostgreSQL 回滚与正常热重载用例最终复跑 `1 passed`；Ruff 和差异检查通过。

事务内输出契约扩展到普通 Domain Command、Generation 启动目录同步和清理目录释放，不再只有管理 action 享有回滚保证。Job Scheduler 的隔离 PostgreSQL 用例对普通命令强制无效输出，确认 `DOMAIN_OUTPUT_INVALID` 且 enqueue 操作账本没有提交；同用例正常管理动作与热重载继续通过（`1 passed`）。

选择预检的下一修订投影现由 Worker Host 包内只读 `component.selection.project` SQL 端口生成；宿主在 Graph 锁、选择共享围栏与配置 `REPEATABLE READ` 快照内读取包返回的规范快照，不再保留平行的 Python 投影实现。包版本升至 `1.6.7`、私有 SQL schema 升至 `1.5.0`。隔离 PostgreSQL 验证 builtin 与已安装包的预检快照均等于相同输入的实际选择写入后快照（`1 passed`），外部包 Graph 预览仍通过（`1 passed`）。完整宿主管理查询和热重载复验见后续记录；具体启停 action 与资源预算仍未接通。

完整 Worker Host 宿主 PostgreSQL 用例最终通过（`1 passed`，176.15 秒）：包内 `validate` 查询取得只读计划，后续 shell 重载、两次配置重建、选择任务驱动的已安装包热加入和停用均完成。此前一次运行在等待 Graph 替换时超时，复验确认该替换实际可成功；另一次复验暴露测试夹具仍把直调选择 SQL 标为普通 `command`，新围栏正确拒绝，夹具改用 `management_action` 后全链通过。此用例使用假 Bot 客户端，未向 QQ 发消息；Web 授权启停与资源预算仍未验收。

Worker Host 包内新增只读 `component.selection.operation-probe`：在选择独占围栏下可先判定操作不存在、相同输入的重放或同 ID 不同输入冲突，返回旧操作的准确回执而不依赖已变化的当前选择。包版本升至 `1.6.8`，私有 SQL schema 升至 `1.6.0`；隔离 PostgreSQL 的选择账本用例验证三种结果并继续覆盖 CAS、快照和发布围栏（`1 passed`）。它尚未接入授权启停 action，不能单独构成提交复核。

Embedding 和 Milvus 的 Bot 生命周期身份现由各自包清单的 `config_immutable_paths` 声明并交给通用配置账本执行，分别冻结 `/profile` 与 `/space_id`、`/dimensions`、`/metric`；endpoint、凭据和传输限制仍可计划换代。已删除宿主导入的两份专用配置身份校验器、Embedding 私有身份表/仓库和 Milvus 私有身份表/仓库；Milvus 私有 schema 只保留 ref tombstone。官方包初始配置现在统一在写入前按工件 schema 校验。包构建与本地包/模型测试 `123 passed, 1 skipped`；隔离 PostgreSQL 的配置可变/不可变行为、并发配置快照、外部包热重载各 `1 passed`，完整假 Bot Worker Host 重载链 `1 passed`（177.06 秒），Ruff 通过。包持久布局发生破坏性变化，未实现旧身份表迁移；当前未上线部署按清库重建处理。组件选择授权提交、资源预算和长期真实实例验收仍未完成。

Memory 包新增全局 `memory.relations.search` 管理查询及 UI 入口，不再要求管理员预先知道节点 ID；关系类型、来源类型、跨主干，以及同一证据的群号/主干/作者可组合过滤。复用原有来源删除与资格复核、固定 PostgreSQL 快照和筛选绑定的键集游标；有向边在全局视图明确标为 `CANONICAL`。活动关系顺序及证据来源加索引，包版本升至 `1.65.0`、私有 schema 升至 `3.56.0`。隔离 PostgreSQL 验证全局/一跳查询的快照、证据过滤、翻页和游标冲突，以及真实 Generation 管理端口（共 `3 passed`）；UI 脚本 Node 语法与包构建通过。它仍是权威关系事实浏览；人工审核、发布图投影和百万级性能验收尚未完成。

组件选择读取器已从已验证的 Worker Host 包快照绑定私有 schema 和三个 Domain Query 函数，宿主不再写死这些包内函数名。候选替换沿用当前读取器取得选择围栏，在发布前核对候选包查询同一账本、同一快照；新绑定只在 Graph 成功发布后切换，schema 身份变化拒绝热替换。选择账本 PostgreSQL 用例与完整宿主启动/重建用例 `2 passed`，新增一致性复验后的完整宿主用例 `1 passed`；本地宿主与 schema 非数据库用例 `19 passed`，Ruff 通过。授权启停的事务 preflight、资源预算和 Web/Shell 写入口仍未完成。

Graph 选择的通用管理事务 preflight 现有 `runtime.selection-commit` 能力，生产绑定仅接受当前官方包摘要。命令输入显式携带启停方向，包内 SQL 验证它与实际写端口一致；Worker Host 包版本升至 `1.6.9`，私有 schema 升至 `1.7.0`。检查在宿主 Graph 锁与调用方事务内取得选择独占围栏，包内操作探针先识别准确重放；新操作用包内投影和正式 Graph 规划器重算 `plan_digest`，配置头在提交模式按组件 ID 顺序锁定。只读预览与提交检查共用投影/规划入口。隔离 PostgreSQL 的选择 CAS/错绑拒绝与完整 Worker Host 热重载用例 `2 passed`（199.31 秒）；完整宿主用例核对正确摘要通过、篡改摘要拒绝且选择账本未写。本地包/宿主检查 `16 passed`，Ruff 通过。Worker Host 尚未声明公开启停 action；资源预算和真实 Web/Shell 授权提交仍待接通。

选择 preflight 的重放路径补充真实 PostgreSQL 验证：首次选择已提交、随后选择修订再次变化时，准确旧 operation ID 与输入仍通过包内探针返回；相同 ID 改动输入仍冲突。定向选择账本用例 `1 passed`；这验证了提交前检查的重放短路，而公开 Web/Shell 启停动作仍未开放。

Memory 关系全局浏览把展示用跨主干聚合移至键集分页之后；只有显式按跨主干状态筛选时才对候选行预先计算该条件，查询真值仍由同一个包内 SQL 函数定义。包版本升至 `1.65.1`、私有 schema 升至 `3.56.1`。隔离 PostgreSQL 的关系采样/全局筛选/游标、图快照和多跳浏览用例 `3 passed`；本地非数据库用例 `3 passed`，Ruff 通过。此改动减少无跨主干筛选的候选行聚合，但尚未做数十万关系规模的延迟压测；新 baseline 依未上线部署约定需清库重建。

十万条合成关系实测指出上一版仅解决了无跨主干筛选的路径：第一页约 9 毫秒、深页约 3 毫秒，但筛选不存在的跨主干结果仍逐行聚合，约 2.1 秒。本版在关系贡献写入事务中固定 `cross_trunk` 事实并建立 `(Bot,cross_trunk,relation_id,contribution_id)` 活动关系索引；生产跨群联想按两个来源主干显式写入，缺失派生值的直接写入被表约束拒绝。相同十万条合成关系的稀有筛选约 2 毫秒，索引规划、首尾键集页及完整输出均通过；跨主干联想、关系筛选、图快照和多跳的隔离 PostgreSQL 回归 `5 passed`。Memory 包版本升至 `1.66.0`、私有 schema 升至 `3.57.0`；这是未上线破坏性布局变更，旧库需显式清库重建。该数据集没有十万条证据，也不能代表真实多月混合筛选和写入负载。

关系布局变化后的完整假 Bot 宿主启动、退役和重启用例 `1 passed`（140.20 秒），Memory 批次资格参数化用例 `5 passed`，遗忘选择与管理试验用例 `2 passed`。独立 Memory 测试夹具原来省略 Worker Host 却仍携带其保留依赖，现仅在该隔离夹具内剥离缺席的保留边；正式包描述符和生产 Graph 约束未放松。Ruff 与差异检查通过，未向 QQ 发送消息。

Worker Host 候选图预览现显式返回每个启用包的六项 `worker_limits`，并将其纳入计划事实摘要；候选检查和实际 Python Worker 启动共用 `WorkerLimits.from_component()`，避免省略清单字段时分别套用不同默认值。包内 UI 展示目标包限额，包版本升至 `1.6.10`，私有 SQL 不变。本地包/宿主非数据库回归 `92 passed, 1 skipped`，Ruff、清单构建、输出 schema 和内嵌脚本检查通过。隔离 PostgreSQL 的完整假 Bot Worker Host 预检、授权事务复核、Shell 重载、配置重建和外部包热加入/停用用例 `1 passed`（200.77 秒）；预检中的 Agent 限额与其精确包工件声明一致。Bot 总量策略、硬 CPU/内存限制和公开启停 action 仍未完成。

Worker 限额现作为通用 RuntimeModuleDescriptor 的不可变字段进入 Graph 事实和描述符摘要；正式规划结果同时保留旧图活动 Worker 与新图预热 Worker 的资源声明，热替换峰值包含新旧两代，保留成员不重复预热。限额模型加入隔离 Worker SDK 快照，实际子进程启动复验通过。本地相关回归 `139 passed, 1 skipped`，Ruff 通过；隔离 PostgreSQL 的完整假 Bot Worker Host 预检、授权复核、Shell 重载、配置重建和外部包加入/停用用例 `1 passed`（199.49 秒）。本步只给出准确容量事实，尚未设置 Bot 总量阈值、提交时 admission 或 CPU/内存硬隔离；公开启停 action 仍未完成。

Worker Host 只读预检现在返回 `worker_capacity.active/preparing` 的逐组件限额，计划摘要绑定同一容量投影，包内管理页显示热替换时当前、预热及峰值 Worker 数；包版本升至 `1.6.11`，SQL 不变。独立规划器用例验证被替换的旧 Worker、新 Worker、新增 Worker 与保留 Worker 的计数和来源；本地包/宿主非数据库回归 `105 passed, 1 skipped`，Ruff、包工件构建和内嵌脚本语法通过。隔离 PostgreSQL 的完整假 Bot Worker Host 预检、同事务摘要复核及热替换用例 `1 passed`（201.05 秒）。Bot 级资源阈值与硬 CPU/内存隔离仍未配置或执行，公开启停 action 仍未完成。

Conversation 的 `conversation.authorize-run-trigger` 现按触发的精确 InputBatch 成员、不可变领取分段和活动领取组核验工具资格；仅伪造同一个 Session/Run 标记而没有分段成员的 `CLAIMED` 行会被拒绝。包版本升至 `4.17.1`、SQL baseline 升至 `4.13.1`，未上线旧库需清库重建。独立 Conversation PostgreSQL 用例覆盖合法触发、跨 Run 与同 Run 非成员拒绝、组终结后撤销以及触发分段结算（`1 passed`，27.59 秒）；完整假 Bot Worker Host 启动、预检、授权复核、Shell 重载、配置重建和组件加入/停用回归 `1 passed`（206.55 秒），Ruff 与包构建通过。独立 Conversation 夹具只剥离未装配的 Worker Host、Agent、Delivery 保留边，正式 Graph 约束未改。受控重排和异常孤儿组处置仍未完成；真实树莓派当前 `alicebot.service` 活动，尚未进行 Forest 真实 QQ 发送。

2026-10-01：新增 Worker Host 后，官方供应器仍按 20 个成员检查，导致开发期 Bot 库无法按当前候选基线重建；已统一 21 成员权威清单，重建脚本在删库前预检 package catalog 可写，并修正只读探针和运维挂载说明。隔离 PostgreSQL 的重建与供应定向用例全部通过。树莓派 `alicebot.service` 已停止，Forest Supervisor 用候选镜像重建 `forest-main` 库后恢复在线；连续四次只读探针确认 21 个活动成员、无重叠 Run/过期 Embedding 预留。其后真实 OneBot 连接保持 ESTABLISHED，库内已有 38 条已分类 `NO_TRIGGER` 群消息和对应 Memory 来源；未出现 trigger 或 Agent Run，故只证明被动 ingress，不能证明真实模型回复与发送。

Worker Host 包现声明独立的 `module.enable` 与 `module.disable` 管理 action，分别绑定包私有选择写端口和宿主 `runtime.selection-commit` 事务 preflight，Web 管理票据注入管理员主体、请求摘要与计划摘要；启停方向仍由包 SQL 与端口共同校验。完整假 Bot PostgreSQL 用例不再直调选择 SQL，而是经包管理契约完成候选预检、启用、停用、任务结算和 RuntimeGraph 热替换（`1 passed`，248.03 秒）；包构建回归 `84 passed, 1 skipped`。包版本升至 `1.6.12`，真实实例尚未部署此候选版本。Bot 级资源准入阈值归属及 CPU/内存硬隔离决策尚待明确，模块 UI 和 Shell 启停入口仍未开放，不能将这两个 action 视为生产启停验收完成。

选择 action 回执现直接返回确定性的异步 `request_id`，首次提交、准确重放与只读 operation probe 均返回同一 ID；宿主严格校验版本化 `forest.component-selection-operation/2` 形状，包内 UI 后续可只凭 action 回执查询发布状态，不再读取 Worker Host 私表。Worker Host 包升至 `1.6.13`、私有 SQL 基线升至 `1.8.0`，旧开发期库需显式重建。`/1` 回执实现的完整假 Bot 热替换测试已通过（`1 passed`，250.44 秒）；升版后的隔离 PostgreSQL 选择 CAS/重放和状态投影 `2 passed`，Ruff、包基线摘要与候选镜像构建通过。真实实例仍运行旧包，尚无启停 UI/Shell、Bot 级资源 admission 与真实 Web 会话验收。

2026-10-01：Bot 级 Worker 声明容量现归 `kernel.runtime` 的版本化配置：Worker 数、帧字节和五项并发/队列/持有上限均按活动代加预热代计算。候选预览返回策略 revision/Payload 摘要、峰值及阈值，计划摘要绑定这些事实；Worker Host 的 `module.enable/disable` 事务复核会因超额拒绝，冷启动与热替换预热前也执行同一准入。`kernel.runtime` 独立管理页可提交阈值，配置提交事务先检查当前 Graph 的内核换代峰值，拒绝明显无法重建的策略；Worker Host 管理页现在展示容量事实并通过公开 action 提交启停、按回执查询状态。隔离 PostgreSQL 的完整假 Bot 验证了默认策略启动、热替换，以及调低策略后的预览/提交拒绝且选择账本不变（`1 passed`，289.47 秒）；加入 Web 配置提交拒绝且 desired 修订不变后同一用例再次通过（`1 passed`，290.54 秒）。最后 UI 工件升至 `1.6.15` 并通过包构建和内嵌脚本语法检查；本地非数据库回归 `31 passed, 7 deselected`，Ruff 通过。真实 Forest Supervisor 仍运行旧镜像，尚未部署本候选；Shell 启停、真实 Web 会话验收、CPU/内存硬隔离以及失败配置重建后的通用恢复协议仍未完成，不能将当前状态称为整体生产验收。

配置恢复协议现在允许 `desired_revision` 领先于 `effective_revision` 时，以最新 desired 为 CAS 前提继续计划和应用修复修订；Graph 发布仍须绑定最新 desired 与准确 Payload，并可从任意较早 effective 原子推进。隔离 PostgreSQL 用例验证修订 3 的候选发布失败后，旧生效修订 2 继续服务，管理员创建修订 4 并成功发布；完整模块/schema 测试 `47 passed`，完整假 Bot 的 Worker Host 热重载、配置重建与组件启停回归 `1 passed`（284.95 秒）。简化 Registry 夹具只去掉未装配 Worker Host 的保留边，正式 Graph 依赖约束未放松。Ruff、编译和差异检查通过。Shell 启停、真实 Web 会话、硬资源隔离及候选镜像的真实实例验收仍待完成。

Shell 启停入口审计：现有 Shell 仅贡献 `module.list/reload`；`module.enable/disable` 是 Web 管理 action，选择写端口要求 `management_action` 身份并在事务内运行 `runtime.selection-commit` 预检。直接从 Shell 服务调用包 Domain Command 会绕过此边界，因此不能把它视为已有 Shell 启停。 [09 的 3.1 节](09-group-shell-command-system.md#31-shell-与-web-的同一控制动作) 已固定 Shell claim、准确 action 绑定、Graph 锁序、事务内授权/预检和幂等回执协议；代码仍需实现 Shell 包的事务内 claim 验证与通用控制入口，再进行 Web/Shell 竞争及真实群命令验收。

2026-10-01：Shell `module.validate/enable/disable` 已由 Worker Host 包贡献；`enable/disable` 通过显式 `management_action` 声明进入通用管理 action，不把选择写端口降成普通服务。Shell 私有 SQL 在同一写事务内复核单次 claim、已消费授权 ticket、租约、准确发言者/群/命令/参数/目标路由和当前管理员资格；宿主随后复核目标 Generation、运行相同的选择计划 preflight，并写入 Shell 来源审计。完整假 Bot PostgreSQL 用例在假 OneBot 接入下验证群消息启用与停用 `process.executor` 后的 Graph 换代、无权发言者拒绝，以及 Shell 先写后旧 Web 计划的修订冲突（`1 passed`，237.25 秒）；Shell/schema PostgreSQL 回归 `47 passed`，Ruff 和本地非数据库相关用例 `32 passed`。这些用例没有向真实 QQ 发消息；真实 Web 会话、真实群命令、硬资源隔离及长期运行仍未验收。

同一候选镜像的 Worker Host 局部重载回归也通过（`1 passed`，289.63 秒）：Shell 重载后不受影响的 Generation 仍保持原实例。这是隔离 PostgreSQL 与假 OneBot 验证；真实实例仍运行旧镜像。

浏览器到模块控制面的验收发现控制台 iframe 消息限流将 `resize` 与 query/action 共用窗口：Worker Host UI 完成启用提交后，紧接着的状态查询可能被静默丢弃，页面停在“已提交”却没有结算状态。现按高度上报、资产分块和普通操作分别计数，并对带 request ID 的超限操作返回明确错误；Worker Host UI 同时移除“启停尚未开放”的过期提示，包版本升至 `1.6.17`。系统 Edge 中的真实浏览器用例经认证 HTTP、隔离 iframe、只读预检、`module.enable` action 和后续状态查询走通，并继续覆盖 Memory UI 的遗忘批次操作（`5 passed`）；Ruff、控制台脚本语法及包工件构建通过。此验收使用假模块契约，仍不能代替真实部署会话与 Bot 选择事务的端到端验收。

真实部署账号会话到 Bot 选择事务的隔离 PostgreSQL 验收现已打通：初始化独立部署库和首位管理员、通过 HTTP 登录并携带 Origin/CSRF，调用当前 Worker Host 的 `validate` 与 `module.enable`，轮询持久选择任务直到内核 Graph 发布 `SUCCEEDED`，并核对目标组件进入当前 Graph；同一完整假 Bot 用例保留 Shell 启用/停用及 Web 旧计划冲突验证（`1 passed`，296.45 秒）。验收中还发现 OneBot Adapter 声明 16 个并发输入却只允许 1 个宿主 RPC，第二条群事件短暂重叠时会在接入前报 `OVERLOADED`；现将 `max_state_inflight` 调至 16，包版本升至 `2.1.2`。仍使用假 OneBot/模型客户端，没有向真实 QQ 发消息；候选版本尚未部署到真实 Supervisor。

开发实例切换（2026-10-01）：树莓派 `alicebot.service` 为 inactive。旧 Bot 库的 `shell.command` schema 为 `2.1.0`、`component.worker-host` 为 `1.7.0`，不能直接与当前 `2.2.0`、`1.8.0` 基线装配。先对旧库执行一致 `pg_dump -Fc`，本地备份在忽略目录 `.local/backups/forest_bot_pre_candidate_20261001.dump`（85,795,823 字节，SHA-256 `B7DA977CFDBAF12CC6F25F4F6865CE0050B4BB6D4ECDBBBF3996CE74ABAEAA28`）；旧镜像保留为 `forest:pre-candidate-20261001`。随后优雅停止 Supervisor、确认租约已释放，并用 [开发期重建脚本](../scripts/rebuild_bot_database.py) 在派生库名及 owner 校验后重建 `forest-main` 的 Bot 库，保留 deployment catalog、加密 Secret 和冻结 Bootstrap profile。脚本返回 21 个当前 schema bundle、0 个旧 RuntimeGraph head。`forest:candidate-ec119ec`（镜像 ID `sha256:33c4c85d962222de6282c02126f09964fd06cb426e40ab8cfd03ec2bbab2799c`）已切为 `forest:local` 并重启真实 Supervisor；新库实际安装 `shell.command 2.2.0`、`component.worker-host 1.8.0`。只读探针连续 3 次确认 21 个活动成员、21 条生命周期记录、ONLINE、管理 HTTP 200、无重叠 Agent Run 或过期 Embedding 任务；容器网络表显示与树莓派 OneBot 有 1 条 ESTABLISHED 连接。重建清除了旧 Bot 运行数据；这些检查尚不能证明真实模型回复与 QQ 发送，也不是长期负载验收。本次未向 QQ 发送消息。

群触发重排的内部契约现有独立 `conversation.requeue-run` UnitOfWork：Conversation 在有效 actor 下按准确 claim ref/digest 冻结旧组，Agent 在同事务核验旧 Run 和 Session 均已终结，失败即整笔回滚。组终态 `REQUEUED` 的触发有效状态立刻为 `PENDING`；物理结算每段至多 64 项，未完成前同群新 Run 的领取和输入选择都被阻断。已提交 Context 的触发积压由 `RECOVERY_PREFILL` 分段领取、不重复写入 Context，再由 `RECOVERY_CLAIM` 收尾。隔离 PostgreSQL 已验证 130 条触发的旧组 `64/64/2` 结算、新组 `64/64/2` 领取，以及 Agent 活动 Run 和未终结 Session 的拒绝；Conversation 与 Agent 包各自的定向用例通过，完整假 Bot 装配、退役与重启回归 `1 passed`（257.85 秒）。包版本分别升至 `4.18.0`、`4.14.0`，私有 schema 分别升至 `4.14.0`、`2.9.0`，在线实例仍为上段所述旧候选镜像，不能热装新 baseline。当前尚无面向管理员或恢复器的受控重排入口；不应把这个内部 UnitOfWork 视为可用的恢复功能。异常孤儿组处置与长期积压仍未验收。

重排安全围栏（2026-10-01）：Agent 的同事务 verifier 除 Run 与 Session 均已终结外，现拒绝旧 Run 中存在任意 ToolCall 或 DISPATCHING、UNKNOWN 模型传输尝试的重排，避免新 Run 重复执行不可确定的外部效果。隔离 PostgreSQL 验证了可重排、在途模型和已登记工具调用三种边界；受控管理入口仍未接入，不能把内部 UOW 视为可用恢复功能。

受控群触发重排入口（2026-10-01）：conversation.scheduler 包新增管理员动作 conversation.requeue.request，只在当前活跃 Run/领取组上登记持久请求；动作携带 Web 主体，按管理 operation 幂等。Scheduler 在旧 Agent Run 返回 FAILED/CANCELLED 后查询该请求，持有同群 actor 租约调用既有双包 UnitOfWork；Agent 的终态与外部效果核验失败则整笔回滚。外部效果不安全时按原失败路径终结领取组，并将请求标为 REJECTED；正常最终回答也使待处理请求失效。包内管理页展示按游标有界分页的活跃领取组、请求入口、结果查询及 Context 窗口配置。Conversation 包的隔离 PostgreSQL 定向用例验证了请求、候选列表、重排提交、正常失败的请求拒绝；完整假 Bot 装配/退役/重启回归 1 passed（282.68 秒），Ruff 与 UI 脚本语法通过。尚未完成从真实 Web 管理会话到请求再到 Agent 终态重排的端到端验收，异常孤儿组处置与长期规模验收也未完成；在线 Supervisor 仍运行旧候选镜像，本次变更尚未部署。

重排管理动作复核（2026-10-01）：通过包的实际 management_contract.action 而非直接 SQL 验证了管理员主体注入、两个并发相同 operation 请求的合流及后续精确重放。复核发现 PostgreSQL 复合行含可空列时 record IS NOT NULL 不能表示查到请求；现按非空主键判断，防止把重放误判为另一管理请求。定向 PostgreSQL 用例 1 passed；正常回复使待处理请求变为 REJECTED/RUN_ANSWERED。第一次复测因 Docker 测试容器没有实际发布宿主端口而在建连阶段超时，换用已核实发布的隔离端口后通过。真实 HTTP Web 会话、浏览器操作及旧 Run 的端到端重排仍待验收；在线实例仍未部署新基线。

候选更新与在线只读观察（2026-10-01）：当前源码 `30a128b` 的完整 Worker Host 局部热重载用例在独立 PostgreSQL 通过（`1 passed`，308.49 秒），目标换代后未受影响的 Generation 保持原实例。候选 test/runtime 镜像已构建，镜像内 Process Executor 包默认配置及 Linux 聚焦测试通过（`8 passed`）。树莓派 `alicebot.service` 仍为 inactive；重建前对现有 `forest-main` Bot 库做了 `pg_dump -Fc`，备份在忽略目录 `.local/backups/forest_bot_pre_30a128b.dump`（19,646,349 字节，SHA-256 `93C4A011DA0B2A7CF717505EBEDA1B46F7B978828A1B6913ED43FF6790DE802A`），`pg_restore --list` 可读。旧 Supervisor 停止并释放租约后，开发期脚本只重建精确派生的 Bot 库 `forest_bot_40d178eeaa530f486c034901`，保留部署目录、Secret 和冻结 Bootstrap profile，返回 21 个 schema bundle、0 个旧 Graph head。在线 `forest-clean-supervisor-1` 已切换到 `forest:candidate-30a128b`（镜像 ID `sha256:bffb91c37ff94460b44c634446a740ba17c4c17a80bf4613735377786ccfd6e9`）；四次只读观察无违规：21 个 Graph 成员和生命周期均 ACTIVE，管理 HTTP 200，0 个重叠 Session/过期嵌入任务/待发送，容器重启次数 0，树莓派 OneBot 有 1 条已建立 TCP 连接。观察末尾已有 1 条被动来源和 1 个群主干入库。本次没有主动发送 QQ 消息；真实 Agent 回复、Web 重排动作闭环和长期负载仍未验收。

模型明确未发送后的受控重排闭环（2026-10-01）：完整假 Bot 宿主用真实 HTTP 管理登录、Origin/CSRF、`conversation.requeue.candidates`、`conversation.requeue.request` 与精确重放，模拟同一 Model 请求连续三次 `NOT_SENT`。Agent 现在以跨 Worker 稳定的 `(run_id,input_revision)` 操作 ID 读取同一 Context 快照；`model.input` 固定在请求级，wire 固定在 attempt 级。Model 的第 N 次尝试与其新建 Effect 的第 1 次尝试分开计数。第三次明确未发送后，Agent 的私有事务核验无已派发或 UNKNOWN 的尝试，同时写入 FAILED checkpoint、Run 和 Session 终态；Scheduler 在同群 actor 下完成重排请求结算。隔离 PostgreSQL 端到端 `requeue` 场景 1 passed（73.45 秒），Agent 包回归 1 passed，普通文本与成功回复场景 2 passed。测试使用假 Provider 与假 OneBot，不向真实 QQ 发送消息；在线 Supervisor 仍运行旧候选镜像，新 Agent baseline 尚未部署。异常孤儿组、长期规模与真实管理会话操作仍待验收。

健康 Job 包化（2026-10-01）：`kernel.runtime` 保留版本化 `runtime.status.snapshot/1` 只读状态端口；`runtime.health-snapshot` 的 JobType 声明、输入/输出 schema 和 Worker handler 已移入可热重载的 `component.worker-host` builtin 包，旧宿主文件 `builtin_runtime_job.py` 删除。目标包 Job 在派发前复核取消，保留原先的取消边界。隔离 PostgreSQL 的 JobScheduler 入队/执行/Graph 换代用例 `1 passed`，包级取消用例 `1 passed`，完整 21 组件 Bot 图启动/退役/重启用例 `1 passed`（402.11 秒），包工件构建和 Ruff 通过。Agent 状态工具仍由内核提供，因为其结果 Payload 需要带准确调用者来源；将它移进包前必须先设计受约束的来源写入端口。

本地候选镜像（2026-10-01）：提交 `34365ad` 构建为 `forest:candidate-34365ad`，不可变镜像 ID 为 `sha256:643509c6f88eb2651c1c48545e955e0f99994db2cb93cc4ac39f92c0ba63b61d`；镜像内 `component.worker-host` 工件构建通过，版本 `1.6.18`，摘要 `sha256:cc3fac1815e4f1b23fac7d077458867be34dd1f9fba61db99d67e4346c7ee5d8`。在线 `forest-clean-supervisor-1` 只读检查仍为旧镜像 `sha256:bffb91c37ff94460b44c634446a740ba17c4c17a80bf4613735377786ccfd6e9`，状态 running、重启次数 0；本次未切换或重建其 Bot 库。

Conversation 选择回放修正（2026-10-01）：`selection_operations` 的 `CONTINUE/IDLE/WAIT_INPUT` 行没有 InputBatch，整行 `record IS NOT NULL` 在 PostgreSQL 上会因可空列返回 false；同一操作重放可能再次写入并撞上唯一约束。现按非空操作主键判断回放命中；活动触发领取组也按 `claim_ref` 判断存在性，避免组终态列为 NULL 时跳过身份冲突检查。隔离 PostgreSQL 的 Conversation 包用例加入 `CONTINUE` 选择的直接 SQL 回放，`1 passed`；包工件构建与 Ruff 通过。包版本 `4.20.1`，私有 SQL 基线 `4.16.1`，未上线 Bot 库需按开发期约定重建。提交 `7241163` 已构建为 `forest:candidate-7241163`（镜像 ID `sha256:3f212500d9f5847caf8e0e7ef11e183db3a3656ef1bebf7af347c1b8cb2d90d8`），镜像内 Worker Host 与 Conversation 包工件构建通过；在线实例仍未切换。异常孤儿组的完整检测与恢复仍未验收。

Memory 非群来源身份复核（2026-10-01）：`memory_sources` 的 `DIRECT/DOCUMENT/SYSTEM/UNKNOWN` 行允许群号和主干为 NULL，捕获事务原先以整行 `record IS NOT NULL` 判断来源存在，会跳过冲突核验并可能尝试重复插入。现按非空 `source_id` 核对来源 ID 与身份摘要的两条读取，并在已有来源分支继续验证身份。隔离 PostgreSQL 用两个互相冲突的 DIRECT 来源验证明确拒绝且来源数量不变（`1 passed`）；包工件构建与 Ruff 通过。Memory 包版本升至 `1.66.1`、私有 SQL 基线升至 `3.57.1`，未上线 Bot 库需重建；此修正尚未构建成新候选，也未在真实 QQ 实例验收。

Memory 归档与迟到捕获的合流修正（2026-10-01）：`pending_input_events` 和 `pending_archive_events` 的待捕获行按约束必须含多个 NULL 列，因此整行 `record IS NOT NULL` 不能表示查到记录。归档现在按 `event_ref` 判断已有待输入，拒绝不同 InputBatch；捕获现在按同一主键判断待归档，并将匹配的来源单元推进至 `ELIGIBLE`、排空待归档。隔离 PostgreSQL 回归覆盖批次冲突与归档先于捕获的正常合流（`1 passed`），Ruff 通过；Memory 包版本 `1.66.2`，私有 SQL 基线 `3.57.2`。`13a9508` 已构建为本地候选 `forest:candidate-13a9508`，但不含本段修正。完整 21 组件 Graph 替换测试仍在复核排空挂起，在线 Supervisor 仍为旧镜像，本次未切换真实 Bot。

候选与生命周期复核（2026-10-01）：`0c0c40c` 已构建为 `forest:candidate-0c0c40c`（镜像 manifest `sha256:7511bdd490087fe0577e7356700851bc85810e43794838a17f0ccafe1c41f8d9`），镜像内 Memory 包工件构建成功，摘要 `sha256:15013e65969e040d523d26e2ba385496ecd39e00cefd3262a03c0eff889a41e8`。完整 21 组件假 Bot 启动、Graph 替换、退役和重启测试在隔离 PostgreSQL 重跑通过（`1 passed`，378.38 秒）。此前两次运行分别在最终 Shell 禁用替换等待处超时、以及预热后旧 Graph 自有工作排空处挂起超过 15 分钟；故一次通过不等于时序问题已根治。测试现对两个等待点输出替换请求状态与自有工作状态，后续若复现应据此修正生命周期协议。在线 Supervisor 未切换，新 SQL 基线仍需按未上线约定重建 Bot 库。

自有工作排空与回滚边界修正（2026-10-01）：完整 Graph 测试复现了 `shell.command` 领取事务 `DOMAIN_AUTHORITY_TIMEOUT`；此前该暂时性领取超时使后台循环退出，而 backlog 排空永久等待已退出循环才会发出的“队列空”信号。现在领取超时回滚后按声明的轮询间隔重试，失败的读取不计为空；真正的循环退出使排空立即返回组件与错误码，并由组件声明的 `drain_timeout_s` 给出总上限。复现时还发现内核回滚直接调用不存在的 `agent.resume_reload_wait()` 与 Conversation 私有方法；该越界调用已删除，回滚只走通用 Generation `resume/activate/open` 和路由恢复。针对领取失败、超时重试和 Agent 包实例回滚的非数据库回归 `4 passed`；包运行时相关回归合计 `24 passed`；完整 21 组件隔离 PostgreSQL 验收重跑 `1 passed`（436.42 秒），Ruff 通过。设计中的长 Agent `RELOAD_WAIT` 安全 checkpoint 仍未由当前 Agent Worker 实际写入；本次删除旧宿主调用不代表该交接已完成，后续必须在 Agent 包内实现停驻、接管及发布前失败恢复。在线 Supervisor 未切换。

生命周期候选镜像（2026-10-01）：提交 `7a34c16` 已构建为本地 `forest:candidate-7a34c16`，镜像 manifest `sha256:5e1a7a161cf01ea01efbdaf78a7df6a94e78ffa6e06e659be674c646bd6bf7aa`。本轮验收使用的完整假 Bot 测试进程从挂载源码运行；候选镜像本身只完成构建，尚未对在线 Supervisor 执行替换。

Agent 停驻交接推进（2026-10-01）：受影响 Worker 现在在自有任务排空前收到通用替换 operation ID，Agent 包在下一次模型调用前的安全边界写入 `RELOAD_WAIT` checkpoint 并释放 Run lease；记忆召回和上下文读取后、模型效果派发前再次检查替换信号。Candidate 发布后由新 Generation 领取。候选发布前回滚时，原 Generation 只有取得恢复身份才可领取同代停驻 Run；未 quiesce 的 Worker 通过 `abort_replacement` 撤销信号。隔离 PostgreSQL 的 Agent 包用例覆盖同代回滚接管后再次停驻及跨代接管（`1 passed`，45.51 秒），完整 21 组件 Graph 替换/退役/重启 `1 passed`（381.20 秒），聚焦的 Agent 安全边界和包生命周期用例 `14 passed`，Ruff 通过。尚未验证真实长时间模型请求中途触发替换的完整 Worker 时序，也未替换在线 Supervisor；此前 `RELOAD_WAIT` 未接入 Agent Worker 的记录以本段为准。

在途模型热替换与冻结依赖守卫（2026-10-01）：21 组件假 Bot 在首个 Provider 请求进行中收到替换通知，明确 `NOT_SENT` 后旧 Agent 写入 `RELOAD_WAIT`，Provider 调用数保持 1。定向更换 `agent.runtime` 时 Provider Generation 保留，新 Agent 发布后接管并发起下一次请求（隔离 PostgreSQL `1 passed`，105.60 秒）。同时更换 Provider 的整图替换曾暴露 `MODEL_REQUEST_ID_CONFLICT`：新代不能用新 Provider Generation 重建旧 request ID。当时 Agent 包通过只读 `replacement_guard_query` 在发布前拒绝该情形，旧图回滚并继续原请求（隔离 PostgreSQL `1 passed`，69.79 秒）；后续请求继承实现与验收见下方记录。在线 Supervisor 未切换。

跨 Provider Generation 请求继承的详细设计（2026-10-01）：[15](15-single-agent-production-execution.md#已确定未发送请求的跨-provider-generation-继承) 固定 `READY` 零 Attempt 或所有 Attempt 确定 `NOT_APPLIED` 才能继承的边界；旧 Request、Attempt、Effect、Observation 不可删除，后继使用新 request ID 和修订，复用冻结投影的语义及 wire 字节，Effect 与 Agent participant 同事务验证并切换 Run 请求头。实现与隔离 PostgreSQL 整图热替换回归已在后续提交完成；本段保留设计缘由，当前限制以下方记录为准。

Provider replacement-facts contract (2026-10-01): A candidate Worker now publishes package-owned compatibility facts after start. Nonempty facts require lifecycle.replacement_facts_schema in the immutable manifest; the host validates them before Graph publication and passes them opaquely to Agent replacement guards. provider.chat-completions declares an exact forest.provider-profile/1 schema for profile_ref, capability_digest, and model_ref. Full PostgreSQL Graph replacement with a frozen unsent model request passed (1 passed, 113.91 s); package/manifest checks passed (113 passed) and Ruff passed. This does not establish production QQ/provider acceptance or remove the still-hardcoded official builtin assembly.

Official builtin bootstrap declaration (2026-10-01): provider_chat_completions, embedding_multimodal, vector_index_milvus, and process_executor now name their bootstrap.json through components.bootstrap_config in module.toml. Package admission verifies that the reference stays inside the captured package and contains a finite JSON object; the pre-Bot reader no longer maintains a separate package-name allowlist and validates the merged seed against the package configuration schema. Four official artifacts and merged seeds built successfully; package tests 89 passed and Ruff passed. clean_bot_host still provisions official components one by one, so this is a declaration cutover, not completion of dynamic assembly.

冻结模型请求的持久阻断（2026-10-01）：Agent Worker 在发布后恢复冻结请求时，仅对确定性的 Provider profile 不兼容或冻结载荷损坏，通过包声明的 `agent.block-frozen-model` UnitOfWork 写入 `BLOCKED_UNKNOWN` checkpoint、请求头与明确错误码；后续领取返回同一诊断，不盲目重建请求或派发 Effect。暂时性 I/O 与事务失败继续沿原 operation 重试或对账。隔离 PostgreSQL 的包状态/幂等回放用例 `1 passed`，完整 21 组件 Graph 热替换回归 `1 passed`，包工件构建与 Ruff 通过。并发继承合流、回执丢失重放、首个新 Effect 失败和多次 Provider 换代仍待验收；在线 Supervisor 未切换。

继承并发合流（2026-10-02）：`agent_inherit_model_request` 先锁当前 Run 再读取继承操作账本，避免等待锁的事务在首笔提交后将同一操作误判为边界关闭。真实 PostgreSQL 三连接用例强制两笔继承同时等待 Run 锁：同 operation 返回同一后继，不同 operation 收到 `AGENT_MODEL_INHERITANCE_BOUNDARY_CLOSED`，最终只有一个继承事实、旧请求 `ABANDONED` 和新请求 `READY`（Agent 包状态用例 `1 passed`，33.38 秒）。完整 Graph 热替换回归此前通过（`1 passed`，117.96 秒）；丢失回执重放和多次 Provider 换代仍待验收。22 组件候选版本已在线，实网群触发到回复尚未验收。

官方 builtin 自动装配第一段（2026-10-02）：包 manifest 新增 `components.auto_install=true`；当时仅用于带包内首次配置、Bot 范围且没有宿主 Effect 服务的官方包。组合根按包声明发现并安装 `routing.graph`、`codec.onebot`、`conversation.scheduler`，三个包的首次配置移入各自 `bootstrap.json`，对应逐包安装代码已删除。包目录回归 `89 passed`，完整隔离 PostgreSQL Bot Graph 热替换与消息链路 `1 passed`（117.27 秒），Ruff 通过。Effect 服务的自动安装见下段；Worker Host 选择账本仍须先引导，在线 Supervisor 未切换。

官方 builtin 自动装配与 Effect kind 绑定（2026-10-02）：自动安装现接受声明 Effect 服务的官方包；组合根提供按 `kind` 索引的受限原生驱动注册表，包只解析自己声明的服务，未知 kind 拒绝构图。`provider.chat-completions`、`embedding.multimodal`、`vector.index`、`media.capture`、`conversation.trigger-policy` 和 `adapter.onebot` 接入 manifest 发现，模型/嵌入/向量端点、触发范围及 OneBot Binding 作为部署配置覆盖值合并包内 seed 后再按包 schema 校验，宿主对应逐包安装代码删除。Adapter 的包内 seed 默认禁用连接，部署覆盖后的真实 Binding 才启用。自动发现现覆盖九个官方包；包目录回归 `89 passed`，完整隔离 PostgreSQL Bot Graph 热替换与模型请求链路 `1 passed`（114.33 秒），Ruff 通过。额外 Bot 授权、Process Executor 专用校验和 Worker Host 选择引导仍为显式组合根职责，在线 Supervisor 未切换。

官方 builtin 自动装配收口（2026-10-02）：`memory.graph`、`agent.runtime`、`job.scheduler`、`process.executor`、`delivery` 与 `shell.command` 也声明包内首次配置和 `auto_install`，组合根删除对应逐包安装分支。动态 Agent preset、Bot 专属嵌入配置和 Shell 管理员身份作为部署覆盖值注入；额外 capability 授权和 Process Executor 配置校验仍由部署方显式提供给统一安装循环，包无法自行授权。官方包现有十五个按 manifest 发现，Worker Host 因提供选择账本仍先引导。包目录回归与完整隔离 PostgreSQL Graph 热替换结果见本次提交验证记录；在线 Supervisor 尚未切换。
验证（2026-10-02）：包目录回归 `89 passed`，隔离 PostgreSQL 中完整 Bot Graph 热替换与群聊触发/召回/模型请求链路 `1 passed`（113.48 秒），Ruff 通过。

Embedding attribution and batch contract (2026-10-02): The read-only live observer now reports facet states by purpose and attempt error codes, and treats DOCUMENT.UNKNOWN or DOCUMENT.FAILED as a memory coverage violation. On the current old-image test Bot it found 349 DOCUMENT facets: 18 READY and 331 UNKNOWN; 13 UNKNOWN batch attempts report EMBEDDING_INDEX_INVALID. A synthetic low-cost request to the configured VL endpoint returned indexes 0..7 for eight inputs and 0..7,0..3 for twelve inputs. Duplicate indexes cannot be assigned back to the frozen facet list. Embedding now declares max_batch_items in embedding.profile.bind/2, rejects an overlarge Qwen3-VL request before network dispatch, and Memory freezes the declared limit for its Bot domain and SQL batch claims. The new package and schema baseline require an explicit development Bot DB rebuild; the online Supervisor still runs the old image, and the 331 existing UNKNOWN facts have not been recovered or resent. Full-Graph and end-to-end verification results are recorded in the next entry.
Validation (2026-10-02): The isolated PostgreSQL complete 21-component Graph start, replace, retire and restart test passed (1 passed, 435.96 s). The Memory claim and group-trigger-to-provider tests passed together (2 passed, 147.67 s). Embedding, Memory Worker and package checks passed (156 passed); Ruff and git diff checks passed. These results verify the candidate source mounted into test containers, not deployment of the candidate image to the online Bot. The old-image instance still needs a deliberate recovery or development DB rebuild before memory coverage can pass the observer.

开发实例嵌入批次修复切换（2026-10-02）：先在当前在线容器对 forest-main 做一致 pg_dump -Fc，备份位于忽略目录 .local/backups/forest_bot_pre_5acacf0.dump（128,808,991 字节，SHA-256 3C82BB018AF1D4F6482F6B8C0BEC06F730A6962CC7768A251B5F93FFCAC29619），pg_restore --list 可读。停用旧 Supervisor 并确认租约过期后，候选镜像 forest:candidate-5acacf0 重建精确派生 Bot 库，返回 21 个 schema bundle、0 个旧 Graph head；旧镜像保留为 forest:pre-5acacf0，新镜像 manifest 为 sha256:ba7ddda2f460d9881b2c57067dd972122156e545212761581398c592f4ae7999。启动后 90 秒只读探针 7 次采样无违规。发现 PostgreSQL 重建未清 Milvus，同一 Bot 与冻结 space 派生集合仍有 23 条失去权威引用的旧向量；再次停用 Supervisor 后核对 Bot ID、space_id 与集合哈希，删除该精确集合，再启动新 Generation，集合重建为 0 行。清理时跨越 STARTING 的探针首样本按预期报告 Graph 未就绪；稳定运行窗口重新采样 5 次无违规，21 个成员 ACTIVE、管理 HTTP 200、无重叠 Run 或过期 Embedding 工作。当前新库已有 2 条被动来源及 2 个主干，尚无新 facet；本次未向 QQ 发消息，因此没有验证真实模型回复或新嵌入的线上成功结算。维护脚本现将同 Bot 与冻结空间的 Milvus 集合重置纳入开发期重建，并在清理失败时明确报告数据库已重建；隔离 PostgreSQL 用例 1 passed、Ruff 通过。

真实 VL 记忆链路复核（2026-10-02）：独立 PostgreSQL Bot 库、当前 Milvus 和 QiYu .env 中声明的 SiliconFlow Qwen3-VL 端点执行 test_real_vl_auto_archive_cross_group_recall_and_management_trial，通过（1 passed，141.85 秒）。用例将群 933179609 的图文消息静默归档为 TEXT/IMAGE 两个 READY facet，再由群 933179610 的图文触发跨群召回，验证 IMAGE 查询命中 IMAGE 文档、EvidenceBundle 保留原群号和作者，管理试验的图文查询同样返回原群与图片证据；独立 Bot 与向量集合在 finally 中清理。该用例的 OneBot 和聊天模型为测试替身，真实的是嵌入端点、PostgreSQL、Milvus 和模块链路；不等于在线 QQ 回复验收。在线 forest-main 随后的只读观测仍为 21 个 ACTIVE 成员、0 个 UNKNOWN/FAILED facet，10 条被动来源均为 REGISTERED，尚未触发新文档嵌入；在线域已冻结 dimensions=4096、max_batch_items=8。本次没有主动发送 QQ 消息。

同群排队与跨群并发的后继模型调用验收（2026-10-02）：完整 Bot 隔离 PostgreSQL 场景现让假聊天模型返回有效结果，首群的第一个请求在门闩处等待时，同群新的触发保持 PENDING，另一群可以独立形成第二个模型请求；释放首群门闩后，第三个请求取走同群排队触发，模型输入包含排队消息，并在 source_manifest 中保留作者 qq:20003。强化后的 cross-group 用例 1 passed（91.15 秒），原有 operator requeue 与旧跨群路径合计 2 passed（165.83 秒）；临时失败诊断未保留。测试使用假 OneBot、假聊天模型和隔离数据库，只证明该模块链路及归因，不替代真实 QQ 并发验收。

开发期重建向量预检（2026-10-02）：`rebuild_bot_database.py` 取得独占 Supervisor lease 后、删除 Bot 库前，先探测同 Bot 与冻结空间派生的 Milvus 集合；端点不可用时返回 `BOT_REBUILD_VECTOR_PREFLIGHT_FAILED` 和 `database_rebuilt=false`。数据库基线验证后的精确集合重置仍单独执行，其失败返回 `database_rebuilt=true`。隔离 PostgreSQL 测试模拟预检失败，验证旧库标记表仍存在，再恢复向量端点完成重建（1 passed，56.05 秒）；Ruff 通过。该预检不保证两种存储具备跨系统原子事务，维护失败仍需依据错误状态处理。

在线触发边界只读复核（2026-10-02）：当前 `forest-main` Supervisor 仍运行候选 `5acacf0`，0 次重启；只读探针显示 21 个 ACTIVE 成员、0 个并发重叠 Run、0 个过期嵌入工作和 0 个违反项。逐项排障发现 57 条已分类群事件的最终决策均为 `NO_TRIGGER/group_not_allowed`，来自三个非白名单群；冻结和有效 TriggerPolicy 均只允许群 `933179609` 的 `MENTION`，当前新库没有来自该群的事件，也没有 Agent Run 或发送意图。因此当前无回复不能归因于模型或 RAG 执行，批准群的 OneBot 入站和真实回复仍待低频验证。只读探针现按每个会话的最新入站索引项汇总决策/原因码，避免长时间采样扫描全部历史；该快照不统计累计事件数。脚本在线冒烟与 Ruff 通过。

记忆关系规模复核（2026-10-02）：隔离 PostgreSQL 中，10 万条合成关系事实的管理键集分页、后段游标及跨主干过滤用例通过（1 passed，25.84 秒），四次页查询分别约 4/2/2/1 ms。图邻居场景在已有跨群来源、证据复核与快照测试中加入同一源节点 2 万条带证据联想边；限制为 5 条时返回恰好 5 条并标记已抽样，同一查询重放一致、不同查询抽样不同，单次读取约 19 ms（整项 1 passed，48.34 秒）。贡献与证据必须同一事务写入，以满足 `created_xid` 引用围栏；第一次分事务写入被外键正确拒绝，修正夹具后通过。Ruff 通过。以上是合成关系与邻居读取的隔离库结果，尚非数月真实群消息、媒体 Payload 和多 Bot 并行负载验收。

未创建 Run 的预填组受控重排（2026-10-02）：Conversation 的候选列表与管理员请求现包含 lane 尚无 active Run、但唯一活动领取组已保存预填触发的状态。Scheduler 在 Agent 执行前先处理准确的待办重排请求；跨模块 UnitOfWork 先以 actor 围栏结算 Conversation 组，再由 Agent 在同事务及会话级 advisory lock 下证明目标 Run 与 Session 均不存在，或沿原规则证明旧 Run 已终结且外部效果安全。活动 Run/Session 不会被作为孤儿重排。独立 `agent.start-run` 计划及能力和不再使用的输入 schema 已删除，包内启动 participant 仍由带 Conversation 选择步骤的组合计划调用。两包版本分别为 `4.23.0`、`4.21.0`，SQL 基线为 `2.19.0`、`4.17.0`；未上线 Bot 库需重建。Conversation 与 Agent 隔离 PostgreSQL 用例各 1 passed（37.70/50.96 秒），完整 21 组件 Graph 启动、替换、退役和重启 1 passed（431.33 秒）；原有真实 HTTP 管理会话终态重排链路在新代码下仍通过（1 passed，82.97 秒），包目录 90 passed、Ruff 与模块 UI 脚本语法通过。第一次完整 Graph 运行时包 UI 文件被并发修改，正确报 `PACKAGE_VERSION_CONFLICT`；固定文件后从头重跑通过。尚未完成真实 Web 管理会话到预填组重排再到分段恢复的端到端验收，也未切换在线实例；其他违反持久约束的异常组仍需明确诊断。

候选 `9a28547` 在线切换（2026-10-02）：旧 Supervisor 正常退出后，先将当前镜像保留为本地标签 `forest:pre-9a28547`，再以 `pg_dump -Fc` 备份开发期 Bot 库 `forest_bot_40d178eeaa530f486c034901`。`pg_restore --list` 可读，容器与工作区文件 SHA-256 均为 `e5ee977c1ac6ad8fb47a48d7ed6b07ca43aee523de105746f0a133f401b548c5`；备份位于被 Git 忽略的 `.local/backups/forest_bot_pre_9a28547.dump`，只覆盖 PostgreSQL，不覆盖 Milvus。确认 Supervisor lease 过期后，候选重建脚本返回 21 个 schema bundle、0 个 RuntimeGraph head，且精确向量集合已重置。Compose Supervisor 现使用镜像 `sha256:08fc50864e0b52395357a0da5ae08f88e7732325b8ba45a5a5ac7816b339df03`，健康且 0 次重启。切换后的只读探针持续 90 秒、7 次采样，均显示 21 个 ACTIVE 图成员、部署 ONLINE、管理 HTTP 200、0 个重叠 Session/积压发送/过期嵌入工作及 0 个违反项。新库尚无消息入站、Memory 来源或 Agent Run；这一观察不能证明 OneBot WebSocket、获准群 `933179609` 的 @ 触发、真实模型回复和发送。预填组 Web 端到端重排及长期负载验收仍未完成。


宿主过渡命名清理（2026-10-02）：七个仍承担组合根、部署入口、管理网关及最小数据库句柄的 `clean_*` Python 文件已移至职责命名的模块，三个相应测试文件一并改名；`Clean*` 类型、`bootstrap_clean_kernel` 和 `run_clean_*` 旧名称及导入路径均未保留兼容转发。内核 `provided_services` 删除了五个已无实现和调用方的旧 Conversation/Agent UoW 服务声明。VL 嵌入大结果测试原以 32 个 facet 调用现行最大 8 个的供应商约束，已改为合法最大批量，同时仍验证结果 Payload 大于内联回执阈值。全量本地回归 474 passed、110 skipped，Ruff 通过；隔离 PostgreSQL 内核引导 1 passed，完整 21 组件 Bot Graph 启动、退役及重启 1 passed（387.19 秒）。这是工作树源码的验证；当前在线 Supervisor 仍是此前 `9a28547` 镜像，尚未切换此批重命名。


宿主路径清理候选 `ec98f48` 在线切换（2026-10-02）：从 `9a28547` 到本候选未改模块或内核 SQL baseline；运行镜像 `sha256:f208b7bd1e940258afbeb58105107f7a2f1d3e04392dc0e0dd88969341aa51d5` 内，新宿主导入有效、旧 `forest.clean_bot_host` 导入不存在，官方自动安装发现 15 个 builtin。旧 Supervisor 正常退出，原镜像保留为本地 `forest:pre-ec98f48`；原 Bot 库用 `pg_dump -Fc` 备份至 Git 忽略的 `.local/backups/forest_bot_pre_ec98f48.dump`，`pg_restore --list` 可读，容器和工作区 SHA-256 均为 `8ccaf1f8f1f1dba2798d1af4a6a261e4c8af335d8e02ef61076e3ad0d3ddd6c1`。未重建数据库，现有 2 条 Memory 来源及 1 个群主干在跨版本重启后保留。切换时过早启动的首轮探针在 Bot `STARTING` 且图尚未发布的首样本正确报 3 项启动期违规，随后 4 个样本均健康；从稳定状态重新观察 60 秒的 5 个样本均为 21 个 ACTIVE 成员、部署 ONLINE、管理 HTTP 200、0 个重叠 Session/积压发送/过期嵌入工作及 0 项违规。Supervisor 健康、0 次容器重启。最新入站仍为非白名单群的 `NO_TRIGGER/group_not_allowed`，获准群 `933179609` 的 @、真实模型回复、发送及预填组 Web 重排尚未验收。


管理 Web 壳资源命名清理（2026-10-02）：剩余 `clean_management_static` 目录改为 `management_static`；旧 `clean-audit`、`clean-authorization-policy`、`clean-runtime-capacity`、`clean-configuration` 的 HTML/CSS/JS 资源和路由声明分别改为 `kernel-*` 与 `module-configuration`，不保留旧 URL。页面仍是部署/内核管理壳，业务配置界面继续由对应包 Generation 提供。资源目录 30 个文件均保留；管理相关快速回归 10 passed、10 skipped，完整本地回归 474 passed、110 skipped，Ruff 与全部管理脚本语法通过；本地 ASGI HTTP 实际读取 9 个新的首页和内核/模块资源均返回 200。尚未切换在线 Supervisor，本次为源码及本地 HTTP 验证。


活动领取组只读对齐诊断（2026-10-02）：在线观察脚本现在按活动 `trigger_claim_groups` 与准确群 lane 的 Session/Run 做有界组级汇总。恰好一个领取组且 lane 尚无 active Session/Run 的正常预填状态单独计数；多个活动组或与 lane 不对齐返回 `CONVERSATION_CLAIM_ALIGNMENT_INVALID`，不会误判为普通可重排候选，也不会擅自改写 Trigger。Ruff、语法和现行在线库只读执行通过；在线样本有 0 个活动领取会话、0 个异常组，6 条 Memory 来源和 2 个群主干，最新两个会话决策均为非白名单群 `NO_TRIGGER/group_not_allowed`。当前仅建立异常检测入口，尚无对已损坏多组或悬挂领取消歧的自动恢复协议；获准群的真实 @ 与完整 Agent 回复仍未验收。

OneBot 账号和触发配置只读核对（2026-10-02）：树莓派上的旧 `alicebot.service` 为 inactive，Napcat 的 OneBot WebSocket 仍监听 8080，Forest 主机到该端点有已建立连接。通过 OneBot 只读 `get_group_list` 确认当前账号属于获准群 `933179609`；`get_login_info` 返回登录 QQ `3605196653`。在线 `forest-main` 冻结 Bootstrap Profile 的 `adapter.onebot.self_id` 同为 `3605196653`，WebSocket 指向树莓派 8080，`conversation.trigger-policy` 仅允许群 `933179609` 且模式为 `MENTION`。没有读取或输出群消息内容、令牌及密钥，也没有发送 QQ 消息。随后两次在线只读观测均为 21 个 ACTIVE 组件、0 个 active Run、0 个待发送、0 个活动领取组和 0 个违规项；当前最新入站仍仅来自两个非白名单群。因此账号身份、群成员资格和冻结触发配置相互吻合，但尚不能据此证明获准群的新 @ 事件被接收并完成真实回复。

获准群定点诊断（2026-10-02）：`scripts/observe_live_bot.py --group-id 933179609` 现用会话主键对该群 lane、最新入站及其对应 Trigger 做有界只读查询，仅输出序号、时间、分类决策、该条消息的 Trigger 状态和是否有活动 Run，不读取消息正文或作者。脚本语法、Ruff 通过；在线实例两次观测均为 `target_group.observed=false`，且所有既有健康约束仍满足。故在本次采样前，当前 Bot 数据库里确实没有该获准群的会话 lane；真实 @ 入站与回复仍待一次新的低频群消息验收。

获准群 OneBot 历史只读复核（2026-10-02）：通过树莓派上已连接的 Napcat 调用一次 `get_group_msg_history`，仅在本地进程统计返回的 20 条消息时间，不输出正文或用户身份；最新消息时间为 `2026-10-01T17:27:43Z`，早于本次在线采样约三小时。结合当前 Forest 库没有该群 lane，尚无证据表明 Supervisor 启动后有该群消息丢失；下一步需在在线观察期间产生一条新的获准群 @ 事件，按入站、分类、Trigger、Run、发送意图逐段核对。

当前候选镜像准备（2026-10-02）：提交 `45b73e7` 已构建为 `forest:candidate-45b73e7`，镜像 manifest 为 `sha256:665fec5906a00deaef44e6b12c9e1e7088e51e2009e318d2a69b396ca2ffe90c`。隔离容器内发现 15 个自动安装 builtin，四个改名后的内核/模块配置 HTML 页面均存在，旧 `clean_management_static` 目录不存在。构建与只读工件检查不等于在线切换；`forest-main` 目前仍运行前一镜像，待真实群触发验收时应以运行镜像身份标记证据。

Core Shell 包化（2026-10-02）：`core.status/pause/resume/reload` 的命令声明、参数 schema 和执行 handler 已从宿主 `builtin_runtime_shell.py` 移入现有 `component.worker-host` 包；旧宿主文件和 `kernel.runtime` 的 Shell 路由声明已删除。包 Worker 只通过 `runtime.status.snapshot`、`runtime.maintenance.set` 和 `runtime.graph-replacement.request` 受限服务端口调用内核；`core.reload` 保持整图请求而非单组件换代，`pause/resume` 保持 maintenance epoch CAS。包版本升至 `1.6.19`，存储 bundle 未变。包工件校验为 9 个 Shell 命令，本地全量回归 474 passed、110 skipped，Ruff 通过；隔离 PostgreSQL 的完整 21 组件图启动、Core Shell status/pause/resume、过时 reload 拒绝、退役与重启用例 1 passed（309.29 秒）。另一个覆盖多次选择、Web 配置及热替换的大用例已推进至 Graph revision 7、五次 replacement 均 SUCCEEDED，但后段在入口仍开放且进程不再耗 CPU 时等待超过 20 分钟；为取得调用栈而仅中断了一次性测试容器，pytest 未给出完整栈，隔离测试库已清理。这不能计作该用例通过；需要缩小并定位后段等待。在线实例尚未切换此源码。`audit.tail` 仍由宿主 `builtin_audit_shell.py` 实现，依照详细设计还需移出宿主；本次不宣称全部 Shell 贡献已包化。

热替换长用例复核（2026-10-02）：以仅存在于一次性测试镜像的 30 秒协程位置采样重跑同一 PostgreSQL 用例，依次观察到预览、重载、配置重建、组件启停及末段 `verify_in(conn)`，最终 `1 passed in 310.43s`；临时诊断代码已撤回，工作树测试文件无内容差异。首次等待超过 20 分钟与这次通过之间存在不可解释的时序差异，不能将其当成已修复；下一次若复现，应在 `verify_in` 内记录更深的 await 链与锁拥有者，而不是增加无来源的通用重试。

审计 Shell 包化（2026-10-02）：新增自动安装的 `audit.console` Worker 包，独立声明和执行 `audit.tail`；`kernel.audit` 只暴露审计存储与有界的 `audit.page.snapshot` 查询端口。宿主 `builtin_audit_shell.py`、内核 `shell.audit.execute` 路由及命令声明已移除；审计结果的格式化由可换代 Worker 完成。官方 Bot 图因此为 22 个组件。包工件校验、Ruff 和本地回归 `474 passed, 110 skipped` 均通过；隔离 PostgreSQL 的完整 22 组件图启动、真实 Worker Shell 路由调用、退役与重启用例 `1 passed in 314.93s`。此测试验证包进入通用 Generation 生命周期，但未单独请求替换 `audit.console` Generation；线上实例尚未切换此源码。

审计包局部换代复核（2026-10-02）：同一完整 PostgreSQL 图用例增加 `audit.console` 目标换代，核对实际影响闭包内的 Generation 改变、`kernel.audit` 等无关 Generation 保留，且换代前后 `audit.tail` 均经新旧对应 Worker 正常执行。首次运行通过这些断言后，在后续 Memory Web 动作的测试票据上出现 `WEB_AUTHORITY_TICKET_TARGET_MISMATCH`：测试原按模块自身的代身份修订签票，局部换代后票据应绑定当前 RuntimeGraph 修订。夹具改用管理网关的 `authorization_target` 后，从头重跑 `1 passed in 346.77s`。运行时代码未为此增加兼容分支；本次证据覆盖局部换代与后续 Web 操作，但未覆盖长时间实网负载。当前在线 Supervisor 仍是旧镜像。

候选 `260d426` 在线切换（2026-10-02）：当前候选构建为 `forest:candidate-260d426`（镜像 manifest `sha256:955159fca0bfe1485f7364653f494b8369a1ee9bddedffc5ff4427fcb3c034cc`），镜像内确认 16 个自动安装 builtin、22 个官方组件及独立 `audit.tail` 包。切换前只读探针显示旧图 21 个 ACTIVE 成员、0 个活动 Run/任务/待发送；对派生 Bot 库 `forest_bot_40d178eeaa530f486c034901` 做 `pg_dump -Fc`，备份位于忽略目录 `.local/backups/forest_bot_pre_260d426.dump`（2,944,673 字节，SHA-256 `e6e9283fdfac5d36a4db448a976b7b87b3ff3ffb3c14ec312a05ea223f8a6485`），`pg_restore --list` 可读，主机摘要一致。旧镜像保留为 `forest:pre-260d426`，旧 Supervisor 优雅停止后切至候选镜像，未重建数据库。启动期短暂显示 `STARTING` 和尚未发布的图；稳定后连续 30 秒四次只读采样均为 22 个 ACTIVE 成员、22 个 schema bundle、部署 ONLINE、管理 HTTP 200、0 个重叠 Session、0 个活动 Run/待发送及 0 项违规；原有 7 条被动来源和 2 个主干保留。树莓派 `alicebot.service` 仍为 inactive，Napcat 8080 与 Forest 主机有已建立 TCP 连接。获准群 `933179609` 尚无新的入站 lane，本次未发送 QQ 消息，因此真实 @ 到模型回复及长期运行仍未验收。

模型继承迟到重放修正（2026-10-02）：`agent_inherit_model_request` 的已提交操作回执不再读取后继 Request 的可变 `state/attempt_count`，而固定返回首次继承提交时的 `READY/0`；`step_seq/request_kind` 仍取受不可变触发器保护的后继身份。否则后继再次被继承后，前笔操作的迟到重放会错误返回 `ABANDONED`。Agent 包与 SQL 基线分别升至 `4.24.0`、`2.20.0`，原 Bot 库不能直接热装此 SQL 基线。隔离 PostgreSQL 包状态用例让同操作并发合流、不同操作竞争，再连续继承两代并重放首笔操作，确认回执等于首次结果且请求状态链为 `ABANDONED/ABANDONED/READY`（`1 passed in 43.83s`）；完整官方图的 Provider 整图热替换回归也通过（`1 passed in 110.71s`），Ruff 通过。Worker 进程在回执丢失后的真实重启与连续两次真实热换代仍待验收；在线 Supervisor 尚未切换此基线。

候选 `bf4a4b6` 在线切换（2026-10-02）：`forest:candidate-bf4a4b6` 镜像 manifest 为 `sha256:2993900ad10af2d31c989382ce08fcdb1d42f209167ab03a73ba40d9c599309e`。切换前只读探针显示旧图 22 个 ACTIVE、19 条被动 Memory 来源、0 个 READY facet、0 个活动 Run/任务/待发送；开发期重建工具 dry-run 精确核对派生库 `forest_bot_40d178eeaa530f486c034901`、Bot 专属角色、冻结 Bootstrap profile 和向量集合。旧库备份 `.local/backups/forest_bot_pre_bf4a4b6.dump` 为 PostgreSQL 自定义格式（3,444,476 字节，SHA-256 `f28353ac880568725b2d490250a3e8a65eaecc2ea0bb45ad9c7e7cf346cc9e2a`），`pg_restore --list` 可读，主机摘要一致；旧镜像保留为 `forest:pre-bf4a4b6`。旧 Supervisor 优雅停止、租约过期后，只重建该未上线 Bot 库并重置派生向量集合，部署目录、Secret 与冻结 profile 保留；重建结果为 22 个 schema bundle、0 个旧 Graph head。新 Supervisor 使用上述候选镜像，健康且 0 次重启。第一次启动期探针早于 Graph 发布返回 `RUNTIME_GRAPH_HEAD_MISSING`，随后连续 20 秒五次采样均为 22 个 ACTIVE、部署 ONLINE、管理 HTTP 200、0 个重叠 Session/活动 Run/待发送及 0 项违规。树莓派 `alicebot.service` 为 inactive，OneBot 8080 与 Forest 主机存在已建立连接。重建清除了旧 Bot 运行来源；当前尚未观测到获准群 `933179609` 的入站，因此真实 @ 到模型回复及长期运行仍未验收，本次未主动发送 QQ 消息。

Memory 来源分页扩展性修复（2026-10-02）：隔离 PostgreSQL 中从当前 22 组件 Bot 库复制 schema，在独立临时库插入 100,000 条同主干 ELIGIBLE 来源后，默认来源首页的旧 `memory_filtered_source_units` 因函数级 `SET search_path` 阻止 SQL 内联，先物化 100,000 行再排序取 50 条，执行约 0.67–0.71 秒、约 30 万 shared buffer hit 并使用临时文件；作者及无匹配图片筛选各约 0.45–0.48 秒。包内 SQL helper 改为可内联形式，并把关系与内置函数分别显式限定到私有 schema / `pg_catalog`；包版本升至 `1.68.0`，SQL 基线升至 `3.59.0`。在同一隔离库载入新函数定义并清除旧函数保留的 `proconfig` 后，默认首页约 4.36 毫秒（480 buffer hit）、作者筛选约 2.10 毫秒（165 hit）、无匹配图片筛选约 0.96 毫秒（11 hit）。`index_coverage=COMPLETE` 且没有匹配 facet 的最差筛选仍需扫描权威 facet 状态，约 91 毫秒、20.7 万 hit；百万级混合覆盖负载仍待专门优化与验证，不能据首页数字宣称所有过滤都常数时间。真实 PostgreSQL 来源游标和冻结遗忘选择用例 `2 passed in 37.06s`，完整 22 组件跨群召回用例 `1 passed in 84.07s`，不可变包构建通过。临时压测库由脚本自动删除；在线 Supervisor 尚未切换此 SQL 基线。

候选 `9c68bcb` 在线切换（2026-10-02）：Memory 来源分页 SQL 基线 `3.59.0` 与包版本 `1.68.0` 构建为 `forest:candidate-9c68bcb`，镜像 manifest `sha256:0615c7581890c0fa723f9a4ffb14c2c066824bd3583c03514993cd39e7df50ec`。切换前的只读探针显示旧实例 22 个 ACTIVE、0 条来源、0 个活动 Run/任务/待发送，获准群 `933179609` 未入站；重建工具 dry-run 精确确认派生 Bot 库 `forest_bot_40d178eeaa530f486c034901`、专属角色、冻结 Bootstrap profile 与向量集合。旧库的 PostgreSQL 自定义格式备份为 `.local/backups/forest_bot_pre_9c68bcb.dump`（2,717,008 字节，SHA-256 `f00e511d11d9f183e3a8e52321887cc78c3218b5ca69e081d8bfd950a7a09527`），`pg_restore --list` 可读且主机摘要一致；旧镜像保留为 `forest:pre-9c68bcb`。旧 Supervisor 优雅停止、租约过期后，只重建这一未上线 Bot 库并重置派生向量集合，部署目录、Secret 与冻结 profile 保留；结果为 22 个 schema bundle、0 个旧 Graph head。新 Supervisor 使用上述候选镜像，健康且重启次数 0。初次只读探针早于 RuntimeGraph 发布返回 `RUNTIME_GRAPH_HEAD_MISSING`，进程未重启；图发布后连续 20 秒五次采样均为 22 个 ACTIVE、部署 ONLINE、管理 HTTP 200、0 个重叠 Session/活动 Run/待发送及 0 项违规。树莓派 `alicebot.service` 为 inactive，OneBot 8080 与 Forest 主机存在已建立连接。本次没有主动发送 QQ 消息；真实获准群触发与回复、百万级混合索引覆盖筛选仍未验收。

在线定点复核（2026-10-02）：在 `forest:candidate-9c68bcb` 的只读 sidecar 中连续采样两次，当前 Graph revision 1、22 个成员和 22 份 schema bundle 均为活动状态，部署 ONLINE、管理 HTTP 200、0 项健康违规。获准群 `933179609` 的 `target_group.observed` 两次均为 false；该 Bot 库当前为 0 条来源、0 个主干、0 个 facet、0 个活动 Run 或待发送意图。这证明采样时不存在可追踪的该群入站，不证明 OneBot 连接或消息处理正确，也不能用空闲健康状态代替真实 @ 到回复验收。

Memory 稀疏索引覆盖筛选（2026-10-02）：`memory_query_sources` 对 `PARTIAL/COMPLETE` 先沿新的 READY DOCUMENT facet 部分索引有界枚举来源 ID；不超过 8192 个时由 ID 回表并用原权威覆盖计算复核，超过上限继续走来源顺序分页。普通筛选仍直接走原分页，不增加候选枚举。包版本升至 `1.69.0`、SQL 基线升至 `3.60.0`，未上线 Bot 库切换时须按既有开发期流程重建。在隔离 PostgreSQL 的 100,000 来源、无 READY facet 场景，普通首页约 4.4 毫秒，`COMPLETE` 空结果由约 90 毫秒降至约 1.5 毫秒；加入 5,000 个较早来源的 READY facet 后，`COMPLETE` 首屏约 132 毫秒，旧来源扫描约 174 毫秒，返回 50 项。来源游标和冻结遗忘选择的真实 PostgreSQL 回归 `2 passed`，覆盖 PARTIAL/COMPLETE/NO_FACETS、首读快照和选择成员。该基准尚未覆盖百万级、READY facet 广泛分布及真实浏览器操作，在线 Supervisor 仍运行 `9c68bcb` 基线；不得将这项优化表述为全部来源筛选已完成规模验收。

管理召回试验的遗忘隔离（2026-10-02）：Memory 包版本升至 `1.70.0`、SQL 基线升至 `3.61.0`。试验证据改为不含来源正文的清单与独立的逐项 Payload；遗忘来源只撤销对应项目，清单及其他项目可继续读取。管理端按当前冻结路径资格逐项过滤，结果分页以原始证据序号为游标，图片按精确项目再次复核；Agent 的普通整包证据失效规则不变。隔离 PostgreSQL 的完整 Host 场景验证了试验先成功显示跨群图片，随后单来源遗忘后状态仍为 `READY`、结果为零且图片被拒绝（`1 passed in 296.50s`）；局部测试还覆盖中间项目不可见时保留后续项目及分页。包源验证、Ruff 通过。此基线尚未部署到在线 Supervisor；多来源真实遗忘后的剩余项目联动、并发遗忘的长期运行及浏览器显示仍待验收。

候选 `c102dcf` 在线切换（2026-10-02）：运行镜像构建为 `forest:candidate-c102dcf`，manifest `sha256:66551612fefa82654ed2da2b437fa2fa86cb520597861a9f5c6517833e540ae7`，镜像内包源核验为 Memory `1.70.0` / SQL `3.61.0`；旧运行镜像保留为 `forest:pre-c102dcf`。切换前只读探针显示旧图 22 个 ACTIVE、26 条其他群被动来源、无活动 Run/待发送，获准群 `933179609` 未入站。只备份派生 Bot 库 `forest_bot_40d178eeaa530f486c034901` 至忽略目录 `.local/backups/forest_bot_pre_c102dcf.dump`（2,978,459 字节，SHA-256 `0b0d83aba8e99908f82aebdd4b3aa1a6f65ccb06c6b3b0951d092742aee4cb36`）；主机与容器摘要一致，`pg_restore --list` 可读。旧 Supervisor 优雅停止并确认租约过期后，开发期工具按精确库名重建 22 份 schema bundle、清空旧 Graph head，并重置派生向量集合；冻结 Bootstrap profile、部署目录与 Secret 保留。新 Supervisor 运行上述镜像，容器 healthy 且 0 次重启。首次观察早于 Graph 发布报 `RUNTIME_GRAPH_HEAD_MISSING`，Graph 发布后连续 20 秒五次采样均为 22 个 ACTIVE、部署 ONLINE、管理 HTTP 200、0 项健康违规及 0 个活动 Run/待发送。重建清除了旧 Bot 运行来源，当前仍无获准群入站；树莓派 `alicebot.service` 为 inactive。本次未向 QQ 发送消息，真实 @ 到回复、浏览器试验显示、多来源遗忘及长期运行仍待验收。

管理试验计数隐私收口（2026-10-02）：结果页原来把证据发布时的 `missing_count` 原样显示，虽然可见项目与 `total_count` 已经过当前资格过滤，但冻结缺失数仍可能披露不可见来源数量。现在 `memory.trial.result` 升为 `2.0` 契约及 `forest.memory-trial-result-page/2`，仅返回可见项目总数；物化回执中的缺失诊断保留在包内部，不进入管理 UI。Memory 包版本升至 `1.71.0`，SQL 基线仍为 `3.61.0`。局部试验结果回归验证冻结缺失数为 2 时管理结果不带该字段（`3 passed`），包源校验与 Ruff 通过。在线 Supervisor 仍运行上一段 `1.70.0` 候选，新的管理结果契约尚未部署。

候选 `c820508` 不重建 Bot 库切换（2026-10-02）：运行镜像 `forest:candidate-c820508` 的 manifest 为 `sha256:28a60d5cc48dbc7c8aa0f1acf9cf67817a1274f00ccf751dd9ca6fb5dc40a01d`，镜像内核验 Memory 包 `1.71.0`、SQL `3.61.0` 和管理试验结果契约 `2.0`。切换前只读探针为 22 个 ACTIVE、20 条被动来源、3 个主干、0 个活动 Run/待发送，获准群 `933179609` 未入站。精确 Bot 库备份在忽略目录 `.local/backups/forest_bot_pre_c820508.dump`（4,197,739 字节，SHA-256 `7b5eea3f2309ef00b3aec10948c6e5a8be62a7857aa83cf0f801615cbd6c9794`），主机与容器摘要一致，完整 `pg_restore --list` 成功；旧镜像保留为 `forest:pre-c820508`。仅重建 Supervisor 容器，未重建数据库或向量集合。权威 RuntimeGraph 记录显示 Memory 从 revision 1 的 `1.70.0/3.61.0` 变为 revision 3 的 `1.71.0/3.61.0`；旧 20 条来源和 3 个主干保留。首次观察跨启动期看到 revision 2 无活动成员、部署 STARTING，后两次已为 22 个 ACTIVE；稳定后另做 15 秒四次只读采样，均为部署 ONLINE、管理 HTTP 200、0 个活动 Run/待发送及 0 项违规，容器 healthy 且 0 次重启。本次是进程替换后的包升级与数据保留验证，**不是**模块级在线热重载验收；获准群真实 @ 到回复和长期运行仍未验证，未向 QQ 发送消息。

Memory 定点热重载回归（2026-10-02）：在隔离 PostgreSQL 的完整 22 组件 Host 用例中，先定点重载 `audit.console`，再对 `memory.graph` 请求同进程 Graph 定点替换，逐一核对其正式依赖闭包内的 Generation 改变、闭包外 Generation 不变，尤其 `audit.console` 保留原 Generation。其后继续执行记忆域绑定、图文归档和召回、管理试验、遗忘、整图替换及重启回归，整例 `1 passed in 324.66s`；Ruff 通过。测试使用假 OneBot/嵌入/向量客户端，证明的是完整图的模块级换代逻辑与后续链路仍可运行，不等于在线 Supervisor 已通过 Web 或群 Shell 发起定点重载。在线获准群触发与真实回复仍待验收。

在线静默 Context 诊断（2026-10-02）：当前 `forest-main` 的 4 个会话共保有 49 个活动 Context 单元，单会话最多 20 个；分类到 Context 的水位无积压，当前修订也无待领取的 Context 维护。官方配置高水位为 512、裁剪后保留 256 个单元，故目前 0 个已归档单元、0 个 READY 文档 facet 符合阈值语义，不能据此诊断为 RAG 故障。代码中的空闲维护领取要求无活动 Session、无未决触发及已排空输入；每次归档候选最多 64 个，成功后按视图修订继续领取，已有隔离 PostgreSQL 的 `64+4` 追赶验证。本次在线计数只证明低于阈值的当前状态，不证明数月负载下归档吞吐足够；获准群 `933179609` 仍无入站。

Context 前缀裁剪边界修正（2026-10-02）：`conversation_prepare_context_compaction` 过去在最旧 64 个活动单元内逐项排除未决触发/当前 Session 必留项，可能跳过中间受保护单元却归档其后的消息，破坏连续旧前缀与归档引用的时序语义。现在按 Context 顺序标出首个受保护单元，只归档它之前的连续前缀；若最旧单元受保护则本次跳过，等待触发结算或后续安全边界。Conversation 包升至 `4.22.0`，私有 SQL 基线升至 `4.18.0`。隔离 PostgreSQL 用例构造第 4 条为未决触发、第 5 条为可归档单元，验证计划只选择第 1–3 条；同一测试文件与完整 22 组件 Host 图回归 `2 passed in 338.34s`，Ruff 与差异检查通过。在线 Supervisor 仍运行旧 `4.17.0` SQL 基线，本次源码修正尚未切换；现有在线状态低于裁剪高水位，因此未在真实群中触发该边界。

宿主 `builtin_*` 边界命名审计（2026-10-02）：根目录四个残留 `builtin_*` 服务文件逐项核对后均为可信内核的受限桥接，没有业务 Worker、私有业务状态或可卸载生命周期；将候选图预检、包工件目录、RuntimeGraph 状态投影与准确来源 `runtime_status` 工具分别改为 `kernel_*` 文件，删除旧路径并更新组合根及测试引用。根目录唯一保留的 `builtin_*.py` 是不可变包装载器 `builtin_packages.py`，不是插件实现。内核工具与 Agent 工具契约定向回归 `14 passed`，内核组合根导入、Ruff 与差异检查通过。这是职责命名清理，没有把四个内核端口伪装成可热重载组件；在线实例未因该重命名重启。

候选 `aad1662` 在线切换（2026-10-02）：运行镜像 `forest:candidate-aad1662` 的 ID 为 `sha256:ee1e38741d8e0722dc524a6a70d0cd6c586fff6debfa3e406668b65ed663761b`；镜像内 Conversation 包 `4.22.0`、私有 SQL `4.18.0`，包 SQL 文件 SHA-256 `bbcbbf60b04914e87767d97d7578347af8e76d7d7b2b8a8b53812d3f3e48ac8a`。切换前只读探针为 22 个 ACTIVE、64 条被动来源、7 个主干、0 个活动 Run/待发送，获准群 `933179609` 未入站；树莓派 `alicebot.service` 为 inactive。候选重建 dry-run 精确确认库名 `forest_bot_40d178eeaa530f486c034901`、对应 owner、冻结 Bootstrap profile 和向量集合。优雅停止 Supervisor 后，精确 Bot 库备份于被 Git 忽略的 `.local/backups/forest_bot_pre_aad1662.dump`（8,891,130 字节，SHA-256 `a4578bd815e81ea67e0f5e2a40067c033bdcb0b75b7832134f8a57c162d15820`）；主机与容器摘要一致，`pg_restore --list` 完整读取 2,084 行。旧镜像保留为 `forest:pre-aad1662`，租约过期后按工具的双重确认重建这个未上线 Bot 库，结果为 22 个 schema bundle、0 个旧 Graph head，派生 Milvus 集合已重置。数据库的 Conversation bundle 摘要 `bf0fe83fb628f024fec8fcee1600df49dece9d857eb6216754406f763c389873` 与“内核配置账本 SQL + 换行 + 包私有 SQL”的实际拼接摘要一致。新 Supervisor healthy、0 次重启；第一次过早探针返回 `RUNTIME_GRAPH_HEAD_MISSING`，随后稳定期 15 秒四次只读采样均为 Graph revision 1、22 个 ACTIVE、部署 ONLINE、管理 HTTP 200、0 个活动 Run/待发送和 0 项违规。树莓派 OneBot 8080 与 Forest 主机有 ESTABLISHED TCP；这仍不能代替获准群真实 @ 到模型回复和 QQ 发送验收。本次没有向 QQ 发送消息；重建清除了此前 64 条被动来源及其派生索引。

控制包自身定点热重载回归（2026-10-02）：在隔离 PostgreSQL 的完整 Bot 图中，经 `component.worker-host` 包的 `module.reload` 管理 action 请求替换 `component.worker-host` 自己。旧 Generation 的持久入队和包 owned-work 将请求交给版本化内核替换端口；Graph revision 增加 1 后，正式依赖闭包内 Generation 换代、闭包外 Generation 保留，新 Worker Host 仍可读取原请求及内核 `SUCCEEDED` 状态并列出完整模块目录。定向用例 `1 passed in 65.34s`，Ruff 通过，测试使用假 OneBot/模型/嵌入客户端，没有向 QQ 发送消息。这验证控制包自重载协议，不代替在线已认证 WebUI 会话下的定点替换、失败回滚或真实消息流验证。

Memory 管理试验浏览器 fixture 协议校正（2026-10-02）：隔离 UI 冒烟脚本原先仍伪造旧版结果页，缺少当前状态和结果契约的必填字段，且含已移除的 `missing_count`；假 MessageChannel 绕过 Host 输出校验，所以旧脚本可误报 UI 可用。fixture 已对齐 `forest.memory-trial/1` 与 `forest.memory-trial-result-page/2`，并用包内 JSON Schema 检查本次模拟的状态、结果和图片分块响应。本机 Edge 冒烟及 Ruff 通过；它验证的是隔离 UI 与现行协议的一致性，真实管理 HTTP、数据库和供应商图文召回仍需另行验收。

Worker Host 重载状态入口修正（2026-10-02）：管理页原来只把已持久化重载请求 ID 保存在 iframe 内存变量中；页面刷新或控制包自身换代后，管理员无法从该页输入已有 ID 继续查询。现在模块 UI 显示并接受 `reload:...` 请求 ID，按包内现有读取契约校验后查询状态，包工件版本升至 `1.6.20`，存储基线未变。经认证 HTTP 和隔离 iframe 的本机 Edge 用例验证离开模块页、重新打开后输入同一请求 ID 可再次读取 `SUCCEEDED` 状态（`1 passed in 3.43s`）；包工件构建、内嵌脚本语法与差异检查通过。这验证状态入口的恢复能力，不等于在线 Supervisor 已完成定点热重载；当前运行镜像尚未包含此 UI 修正。

候选 `b1faeec` 在线切换（2026-10-02）：镜像 `forest:candidate-b1faeec` 的 ID 为 `sha256:60242e0a78a8e0be7570392f873b99ef54a9ecbc66de36a3c7abd10274d873e5`，镜像内核验 Worker Host `1.6.20`、Conversation `4.22.0`、Memory `1.71.0`。切换前只读探针为 22 个 ACTIVE、35 条被动来源、4 个主干、无活动 Run/Job/待发送，获准群 `933179609` 未入站；精确 Bot 库 `forest_bot_40d178eeaa530f486c034901` 的 owner 为 `forest_role_40d178eeaa530f486c034901`。该库的 PostgreSQL 自定义格式备份位于忽略目录 `.local/backups/forest_bot_pre_b1faeec.dump`（4,969,329 字节，SHA-256 `c3b0aae1726b001b44c3b7eccb5074ca7b1ea50b87571b241d4f191b6bf91439`），主机与容器摘要一致，`pg_restore --list` 完整读取 2,084 行；旧镜像保留为 `forest:pre-b1faeec`。仅重建 Supervisor 容器，未重建数据库或向量集合。启动期探针见 Graph revision 2、部署 STARTING、0 个当前成员；随后同一容器发布 revision 3，连续 15 秒四次采样均为 22 个 ACTIVE、22 份 schema bundle、部署 ONLINE、管理 HTTP 200、0 项违规、0 个活动 Run/待发送，来源数增长至 37、主干仍为 4。权威 Graph 的 Worker Host `1.6.20` 包摘要 `sha256:a3fc57dc5891b5fe5b96188f4b7e027ced9739f9adbee6ff4fb41db857bcbcbd` 与候选镜像所建工件一致；容器 healthy 且重启 0 次。树莓派 `alicebot.service` 为 inactive，OneBot 8080 与 Forest 主机有 ESTABLISHED 连接。此次是进程替换后的包升级与状态保留验证，**不是**模块级在线热重载；获准群仍未入站，没有主动发送 QQ 消息，真实 @ 到回复仍待验收。

Agent 冻结模型载荷失效收口（2026-10-02）：Agent 的 `resume_frozen` 原先让精确 Payload 读取中的已撤销、身份不符或摘要错误直接冒出，未进入现有 `agent.block-frozen-model` 持久阻断。现在只把确定性的引用、身份、正文撤销/缺失、摘要及继承 owner/父依赖冲突归为 `AGENT_FROZEN_MODEL_INVALID`，由执行循环固定 `AGENT_FROZEN_PAYLOAD_INVALID` checkpoint；临时 Payload authority 故障和非载荷事务错误保持原路径重试。Agent 包版本升至 `4.24.1`，私有 SQL 基线未变。定向用例覆盖撤销、摘要错误与暂时性 authority 故障（同文件 `6 passed`），包工件构建、Ruff 通过。这仍不是 Worker 丢失继承回执后真实重启的端到端验收；在线 Supervisor 运行的候选尚未包含此改动。

Agent 继承事务错误归属修正（2026-10-02）：冻结载荷错误转换原先包住整笔 `agent.inherit-model-request` UnitOfWork，使 Effect 证明或 Agent 事务返回的同名 `PAYLOAD_*` 错误可能被误判成冻结载荷损坏并永久阻断。现在转换仅围绕 Payload 读写端口；继承事务原样传播错误，仍可按稳定 operation ID 重试或对账。Agent 包升至 `4.24.2`，私有 SQL 基线未变；定向测试验证事务错误不被改写（同文件 `7 passed`），Ruff 通过。Worker 丢失继承回执后的真实重启仍待验收，在线 Supervisor 尚未包含此版。

Agent 冻结请求恢复补证（2026-10-02）：`NOT_APPLIED` 请求在重试前复制 wire 的 Payload 写入现在也区分确定性的父载荷/owner 冲突与暂时性 authority 故障，避免前者反复冒出却不形成 `AGENT_FROZEN_PAYLOAD_INVALID` 持久阻断。隔离 PostgreSQL 包级用例在继承事务提交后重新读取 Run 恢复状态，确认只暴露 `READY` 后继、其准确投影与 wire，且前驱不能再被恢复；同一用例随后继续验证两代继承及原 operation 的迟到回放（`1 passed in 33.64s`）。定向 Payload 错误用例 `9 passed`，Ruff 和包工件构建通过。Agent 包版本 `4.24.3`，SQL 基线不变；这些证据仍未覆盖真实 Worker 进程在继承提交与回执送达之间崩溃后的重新领取，在线 Supervisor 尚未包含此版。

Agent 继承回执丢失后的冷启动恢复（2026-10-02）：隔离完整 Bot 图在旧 Provider 模型尝试明确 `NOT_SENT` 后完成 Agent/Provider 整图热替换；故障注入器等待 `agent.inherit-model-request` 真正提交，再在 RPC 回执送达前杀掉新 Agent Worker。首次继承事实为前驱 `ABANDONED`、后继 `READY`，故障前没有第二次模型派发。强制崩溃使旧图无法正常 quiesce，HostLease 保留到数据库期限；新 Host 只在期限到期后接管。此前新 Agent 遇有效旧 Run owner 时会冒出 `AGENT_RUN_BUSY`，Conversation 将其频繁结算为失败。现 Agent 通过版本化 `agent.execute/2` 回执返回 `WAIT_OWNER`，Conversation 以 30 秒延期同一工作，不写 Agent 失败或抢租约；Run owner 自然到期后继续。冷启动使 Provider 又换代一次，最终请求链为三代：前两代 `ABANDONED`，第三代以首次继承后继为前驱，模型 wire 字节和摘要与首次请求完全一致。隔离 PostgreSQL/假 OneBot/假模型完整场景 `1 passed in 395.93s`，原有整图热替换回归 `1 passed in 83.27s`，定向 Agent 用例 `10 passed`，Ruff 与两个包的工件构建通过。Agent 包 `4.24.4`、Conversation 包 `4.22.1`，私有 SQL 基线未变；在线 Supervisor 尚未包含此修正，首个新 Effect 失败、长时执行中的 Run owner 续租和真实连续热换代仍未验收。

Agent 长时调用续租（2026-10-02）：原 `agent.execute` 一次可运行超过 300 秒，但只在开头获取 Run owner lease；模型或工具调用阻塞时，owner 可能过期。现在 Agent 包用调用级 `ctx.tasks.spawn_owned` 管理续租任务，每 60 秒走原有 `agent.renew-run-lease` UnitOfWork，固定 owner ID/epoch 并为每笔续租生成不同稳定 operation ID；暂时性故障有界重试，明确失主或安全余量耗尽则取消主执行，调用退出时先取消并等待任务、再释放租约。热重载停车的有意释放不会被迟到的 `STALE` 续租回执误判成执行故障。隔离完整 Bot 图把假 Provider 响应阻塞至首次续租后，PostgreSQL 证实 owner ID/epoch 不变而租约到期时间推进，且只有一次模型派发（`1 passed in 107.36s`）；整图 Agent/Provider 热替换回归 `1 passed in 82.25s`，本地定向用例 `13 passed`，Ruff 与 Agent 包工件构建通过。Agent 包 `4.24.5`、SQL 基线不变；在线 Supervisor 尚未包含此版，连续多个续租周期、真实长时 Provider/工具调用及故障恢复仍需验证。

Agent 续租拥塞处理（2026-10-02）：Worker UOW 网关容量耗尽会以 `OVERLOADED`/429 拒绝续租，属于暂时性未受理。原续租逻辑把所有 4xx 当成失主并立即取消主执行；现仅将这个明确的容量错误纳入已有的有界重试，重试沿用同一稳定 operation ID，真正的 `STALE_AGENT_RUN_OWNER` 及其他拒绝仍终止执行。Agent 包升至 `4.24.6`，SQL 基线不变；定向用例验证 429 后续租成功而主调用未取消，并保留真正失主与停车竞态用例。后续已切换在线 Supervisor，见下段。

候选 `e0932f6` 在线切换（2026-10-02）：切换前在线镜像仍为 `b1faeec` 的 `sha256:60242e0a78a8e0be7570392f873b99ef54a9ecbc66de36a3c7abd10274d873e5`；权威 Bot 库有 22 份 schema bundle，涉及此次切换的 Agent `2.20.0`、Conversation `4.18.0`、Memory `3.61.0` 基线均与当前源码同版，因此只替换 Supervisor 进程，未重建 Bot 数据库或 Milvus 集合。旧镜像保留为 `forest:pre-e0932f6`。候选 `forest:candidate-e0932f6` 的镜像 ID 为 `sha256:bf954f82ccc9fc299190c4a9c495095f6b85eb61acca83d4081caff50b72e0c1`，镜像内核验 Agent `4.24.6`、Conversation `4.22.1`、Memory `1.71.0`。切换前只读探针连续四次显示 22 个 ACTIVE、0 个活动 Run/Job/待发送，155 条来源和 8 个主干；精确 Bot 库的 `pg_dump -Fc` 备份位于 Git 忽略目录 `.local/backups/forest_bot_pre_e0932f6.dump`（19,479,376 字节，SHA-256 `f691d0929126456a7fc88e21f367895c9c26b6fda0366bf91324b3a19755f793`），主机与容器摘要一致，`pg_restore --list` 完整读取 2,084 行。旧 Supervisor 优雅停止后 HostLease 显式释放，新镜像仅重建 Supervisor 容器。启动期首次探针读到 revision 4 的 STARTING/无成员空窗；随后 Graph revision 5 发布，权威记录绑定 Agent `4.24.6`（包摘要 `sha256:2f1120293808f9b1f28bebe1cfeec94fc56fe5c8083dd8c5b950a166c1311b36`）。从稳定状态重新连续 20 秒、五次只读采样均为 22 个 ACTIVE、部署 ONLINE、管理 HTTP 200、0 个活动 Run/Job/待发送及 0 项违规；容器 healthy、重启 0 次，来源增至 158、主干仍为 8，容器内 `/proc/net/tcp` 有一条到树莓派 OneBot 8080 的 ESTABLISHED 连接。获准群 `933179609` 尚无入站；本次没有向 QQ 发送消息，也没有触发真实模型长时调用，因此真实 @ 到回复、连续续租和长期负载仍未验收。

Agent 双续租周期验证（2026-10-02）：隔离完整 Bot 图用例保持假 Provider 的首次模型请求阻塞，持续查询真实 PostgreSQL 的 Run owner 记录，直到同一 owner ID/epoch 的 `owner_expires_at` 比初始值推进超过 90 秒，随后才放行模型响应；这要求至少两次按 60 秒间隔完成的续租，而非仅观察首次续租。用例通过（`1 passed in 167.59s`），模型请求仍只派发一次；测试镜像不连接 QQ。在线进程已经运行相同的 Agent `4.24.6` 包，但该隔离验证不能替代真实 Provider/工具长时调用、在线热重载中途续租或获准群消息验收。

Linux 全量非数据库回归（2026-10-02）：当前源码的 test-stage 镜像在 `--network none`、无生产 Secret 的容器内执行 `pytest -q -p no:cacheprovider -m 'not postgres'`，结果为 `492 passed, 2 skipped, 104 deselected`（44.10 秒）。唯一警告来自故意构造重复 ZIP 条目的包路径攻击用例，不是运行时警告。该结果覆盖跨模块纯逻辑和契约回归，不覆盖 104 项数据库标记场景，也不能作为在线模块重载或群消息回复的证据。树莓派旧 `alicebot.service` 只读检查为 inactive；OneBot 8080 对普通 HTTP `/get_group_list` 返回升级要求，说明该口只提供 WebSocket，不能用 HTTP 404/426 推断 Bot 群成员资格。

在线定点重载观测准备与获准群新入站（2026-10-02）：只读探针新增可重复的 `--component-id` 选项，在与 Graph、工作状态同一 repeatable-read 事务内返回最多八个指定组件的当前 Generation ID、包版本与工件摘要；缺席组件以 `null` 表示，不读取消息正文或发起控制动作。在线 sidecar 实测 Graph revision 5 的 `audit.console`、`kernel.audit`、`agent.runtime` 身份与权威 RuntimeGraph 直接查询一致，探针连续两次无违规。同期获准群 `933179609` 首次出现入站：最新序号 1，分类 `FINAL`，决策 `NO_TRIGGER/mention_required`，Context 应用水位为 1，0 个活动 Run/待发送。这证明普通群消息经 OneBot 到达并进入 Context，尚不证明 @ 触发、模型回复或发送；管理员授权的定点 `module.reload` 仍未在在线实例执行。

后续定点观测（2026-10-02）：`--group-id` 现同时读取该群 CHAT lane 的最新 Delivery 发送意图及状态，使用已有唯一索引末条查询，不读取消息正文。更新后的脚本通过 Ruff 与 Python 编译，在线只读 sidecar 两次采样无违规。获准群入站已到序号 3，分类与 Context 应用水位均为 3；最新一条仍为 `NO_TRIGGER/mention_required`，当前 `latest_chat_send=null`、无活动 Run。因此实网 @→Agent→Delivery 回复验收仍未完成。

获准群触发输入核对（2026-10-02）：又一次在线只读采样显示入站已到序号 5，Context 水位同步，Graph revision 5、22 个 ACTIVE、0 项违规，仍无 Run 或 CHAT 发送意图。一次性诊断只在容器内读取该群最近五条消息的受管 Payload，输出段类型和“mention 目标是否等于同一原始事件的 self_id”布尔值，不输出正文、目标 QQ 号或作者。序号 2 与 4 虽含 mention 段，但目标均非 Bot；序号 3 与 5 含 reply/text、无 mention；序号 1 只有 image。五条的 `NO_TRIGGER/mention_required` 因而与冻结的 `MENTION` 策略及事件本身一致，不能据此归因于 Agent、Provider 或 RAG 卡住。仍需一次明确 @ 当前 Bot 的低频实网触发，随后核对 Trigger、Run 和 Delivery；普通“回复”段目前不满足该策略。

Agent 包管理读模型候选（2026-10-02）：`agent.runtime` 新基线把不可变 `conversation_ref` 写入 Run，并以 Session 三列外键验证归属；全 Bot、单会话及状态筛选有按创建时间与 Run ID 的有界分页索引。包内 `agent.management-runs-page` Domain Query 与 `agent.runs.page`/`agent.run` 管理查询提供 Run 元数据、筛选绑定游标和准确详情，自带紧凑 UI 不经宿主 Agent 专用路由。Agent 包版本 `4.25.0`、私有 SQL 基线 `2.21.0`，与当前在线 `2.20.0` 不同；未上线部署切换需按既定清库重建或另行制定明确基线升级，不能直接把源码热装到在线库。新包构建、UI 脚本语法、Ruff 通过；隔离 PostgreSQL 经真实管理契约验证 Run 启动、群过滤、详情、两条真实 Run 的键集翻页、有效续页与篡改游标拒绝（最终 `1 passed in 33.74s`）。完整 Bot 图的跨群 Agent 场景 `1 passed in 69.05s`，使用假 OneBot/模型客户端；本机 Edge 隔离 iframe 验证列表、详情、翻页及群筛选入口，列表 fixture 经当前输出 schema 校验。尚未做带十万级 Run 的计划/延迟复核、在线部署，也未补 Session、ModelTurn、ToolCall 的独立分页及取消/UNKNOWN 管理动作；这不是 Agent 管理面完成声明。

Agent 回合与工具调用读模型候选（2026-10-02）：`agent.runtime` 包版本升至 `4.26.0`、私有 SQL 基线 `2.22.0`。ModelTurn、ToolCall 通过复合外键约束归属同一 Run、同一 step，避免仅凭分离的 ID 外键拼出错误执行链；`agent.steps.page` 在指定 Run 内分别按 step 序号有界分页读取模型回合及工具调用元数据，工具调用可按状态筛选。包内 UI 将两种记录置于 Run 详情的折叠区域，不展示模型正文或工具参数。新包构建、Ruff、包契约用例和本机 Edge iframe smoke 通过；隔离 PostgreSQL 包状态用例验证修复回合、工具提案和成功态查询（`1 passed in 34.41s`），完整 Bot 图跨群场景也通过（`1 passed in 67.44s`），均未连接实际 QQ。此版本是隔离候选，当前在线库仍为 Agent SQL `2.20.0`；不能直接热装到在线库。Session 分页、取消及 UNKNOWN 对账尚未实现，管理读模型也未经过大规模数据的查询计划验收。

Agent Session 读模型候选（2026-10-02）：`agent.runtime` 包版本升至 `4.27.0`、私有 SQL 基线 `2.23.0`。包内 `agent.sessions.page` 提供全 Bot/准确群聊及状态筛选、绑定筛选的键集游标；点击 Session 后，`agent.runs.page` 用准确 `session_id` 限定 Run，所需索引与 SQL 都在包私有存储内。紧凑 UI 将 Session 浏览折叠，避免管理页高度膨胀。根目录代码核对显示 `adapters/onebot.py` 仍是宿主网络传输，`bootstrap_contributions.py` 是安装前的通用配置目录，均非可删除的旧业务执行器。包构建、Ruff、本机 Edge iframe smoke、隔离 PostgreSQL 包状态用例（`1 passed in 34.66s`）及完整 Bot 图跨群场景（`1 passed in 68.13s`）通过；测试没有连接真实 QQ。当前在线 Agent SQL 仍为 `2.20.0`，此候选未部署；取消/UNKNOWN 管理动作和大规模 Session/Run 查询计划仍待完成。

Agent 管理查询规模复核（2026-10-02）：独立 PostgreSQL Bot 库载入各 100,000 条已关闭 Session 和已完成 Run，按四个群聊分布；真实包 Domain Query 验证群聊/状态首页、续页、靠近历史末端的游标页，以及精确 Session→Run 定位。单次样本中 Session 首页、续页、深页分别约 3.17、1.67、1.71 毫秒，Run 首页、深页及精确 Session 定位约 3.20、2.04、0.70 毫秒。`EXPLAIN ANALYZE BUFFERS` 显示 Session 群聊/状态索引命中 1 个 shared block，Run 群聊有序索引命中 21 个，Run 精确 Session 索引命中 4 个，均无临时块；优化器在全体 Run 都为 `COMPLETED` 时选群聊有序索引而非冗余的群聊/状态索引，符合该数据分布。可重复的隔离规模用例 `1 passed in 17.72s`，测试库自动清理。该样本不证明百万级、复杂状态分布或生产并发延迟；当前在线数据库仍为旧 Agent SQL 基线。

Agent 取消竞态预检（2026-10-02）：设计核对发现私有 `run_cancel_requests`、模型/工具 handoff 的 Run 锁检查已经存在，但正式 `agent.commit-final-run` 和旧版 `agent.complete-run-record` 仍可在取消意图先提交后完成回复。这两个完成 participant 现于 Run 锁内拒绝未处理的取消意图；Agent 包版本升至 `4.27.1`、私有 SQL 基线升至 `2.23.1`。隔离 PostgreSQL 包状态用例在真实持久 Run 上验证取消意图先到时两个完成入口均被拒绝、回滚意图后正常完成（`1 passed in 33.75s`）；完整 Bot 图跨群场景 `1 passed in 68.00s`。管理动作、PREPARED Effect abandon、Agent 取消 checkpoint 与 Session 关闭尚未接入，因此本改动只封闭回复提交竞态，不表示管理员已经可以取消 Run。完整取消语义与未决 Effect 的阻断条件已写入详细设计，候选基线仍未部署在线库。

Agent 管理取消候选（2026-10-02）：Agent 包 `4.28.0`、私有 SQL 基线 `2.24.0` 新增 `agent.cancel.request` 管理动作，按 Run checkpoint CAS 和可信 Web 主体写入唯一意图；Run 详情显示意图并提供取消入口。Agent Worker 在安全点将无未决效果的 Run/Session 原子关闭；PREPARED 模型或工具效果走 `kernel.effect/effect.abandon` 与 Agent participant 的同一 UnitOfWork，已交接效果保持待对账。模型恢复查询补上尚未 handoff 的 `ATTEMPT_PREPARED` 尝试，防止取消意图在该窗口永久等待；Conversation 最终回复提交若输给取消意图，会以新的 Agent 调用读取取消状态。隔离 PostgreSQL 用例验证管理动作的过期 checkpoint 拒绝、幂等请求、直接取消及 Session 终态，并以事务回滚夹具验证 PREPARED 模型/工具的 Agent participant（`1 passed in 36.46s`）；本机 Edge iframe 验证 UI 的动作消息和状态刷新；完整 Bot 图跨群正常路径 `1 passed in 69.85s`。PREPARED 分支的真实内核 Effect abandon 两 participant 合并运行、已交接 UNKNOWN 的后续人工对账以及在线部署仍待验收；在线 Bot 的 Agent SQL 仍为 `2.20.0`，此候选不能直接热装到该库。

Agent PREPARED 效果补充验收与冷启动缺口（2026-10-02）：隔离 PostgreSQL 用例现在在同一事务内建立真实 `kernel.effect` 的 PREPARED 账本行，依次调用内核 `effect.abandon` 和 Agent 取消 participant，分别验证模型与工具的 Effect Operation=`CANCELLED`、Effect Attempt=`ABANDONED`、Agent Run/Session=`CANCELLED`，随后回滚夹具以保持后续正常路径；用例 `1 passed in 35.30s`。这证实两个真实 participant 的锁序和状态合流，尚未单独覆盖包 UnitOfWork 协调器的完整计划调用。排查同时发现另一项未完成工作：无取消意图的 Agent 在 PREPARED 模型效果后崩溃时，当前恢复循环可能重建同一请求的投影与授权；它需要原效果 abandon 与冻结后继请求的原子恢复，详细设计已补约束。在线实例与库均未改动。

Agent PREPARED 模型冷启动恢复（2026-10-02）：Agent 包 `4.29.0`、私有 SQL 基线 `2.25.0` 增加 `agent.recover-prepared-model` UnitOfWork。恢复循环在任何新召回或模型组装前识别旧 `ATTEMPT_PREPARED`；从冻结 projection/wire 派生新请求与首尝试 Payload，先由内核 Effect authority 废弃原效果，再由 Agent participant 在同一事务废弃原尝试和请求、写恢复关系与新 `READY` 请求。Provider 绑定或冻结载荷不兼容时返回诊断，原效果保持待取消状态。隔离 PostgreSQL 以真实 PREPARED Effect 行验证两个 participant 的原子合流、请求继承、幂等重放及冻结查询（`1 passed in 36.83s`）；本地包构建、Ruff、Python 编译通过。包 UnitOfWork 协调器的整计划调用、Worker 跨进程崩溃重领及与管理取消的真实并发竞态尚未单独验收；在线 Bot 的 Agent SQL 仍为 `2.20.0`，未部署此候选。

Agent PREPARED 恢复整计划补验（2026-10-02）：隔离 PostgreSQL 用例另建独立的 PREPARED Run，通过真实 `PostgresPackageUnitOfWorkCoordinator` 执行包 manifest 中的 `agent.recover-prepared-model` 计划；错误后继使 Agent 步骤拒绝时，前置 Effect abandon 随事务回滚，Effect Operation/Attempt 均仍为 `PREPARED`。正确后继提交后，Run 只暴露新 `READY` 请求，Effect 为 `CANCELLED/ABANDONED`，同一 operation 重放返回原回执（整包用例 `1 passed in 39.57s`）。这补齐了协调器事务层的合流证据；Worker 跨进程崩溃重领、与管理取消的真实并发竞态及在线切换仍未验收。

Agent PREPARED Worker 崩溃恢复实测（2026-10-02）：Fake Bot 在 `agent.prepare-model-effect` 整笔事务提交后、handoff 前杀掉 Agent Worker，确认此时尚无 Provider 请求；等待数据库中旧 Run owner 的真实 300 秒租约失效后重启 Host。首轮实测暴露恢复 SQL 错把 `model_transport_attempts.wire_payload_digest`（Effect 派发命令）和 `model_requests.initial_wire_payload_digest`（模型 HTTP 请求体）相比较，导致事务反复回滚。Agent 包 `4.29.1`、SQL 基线 `2.25.1` 删除这个跨类型比较，包级 PostgreSQL 用不同字节的两类 wire 复核（`1 passed in 35.97s`）。修正后的整图 Fake Bot 故障重领测试通过（`1 passed in 364.74s`）：仅产生一个后继模型请求，保留原模型 wire、内容和来源摘要，原 Effect=`CANCELLED/ABANDONED`，Provider 只收到一次请求。测试没有发送真实 QQ 消息；管理取消的真实并发竞态和在线切换仍未验收。

Agent 取消意图与准备入口合流（2026-10-02）：Agent 包 4.29.2、SQL 基线 2.25.2 在 Run 锁内使新模型请求、模型尝试、请求继承及工具 Effect 准备服从已提交的管理员取消意图；已提交操作保留原账本事实，后续重放按各端口的身份与状态围栏收敛。隔离 PostgreSQL 用例让取消事务持锁时并发继承请求进入 UnitOfWork，提交取消后确认继承被 AGENT_CANCEL_REQUESTED 拒绝且没有额外请求行（整包用例 1 passed in 39.53s）。本轮未触碰在线 Bot；在线 SQL 基线仍为 2.20.0，不能将候选包直接热装到该库。工具准备与模型尝试的各自并发场景、UNKNOWN 人工对账以及在线切换仍待验收。
Agent 工具审批取消竞态（2026-10-02）：包版本 4.29.3、SQL 基线 2.25.3。工具审批登记现在同模型请求、模型尝试、请求继承和工具 Effect 准备一样，在 Run 锁内拒绝先提交的取消意图；已提交的审批保留原账本事实，后续重放按身份与状态围栏收敛。隔离 PostgreSQL 包状态用例验证取消存在时模型新请求、新尝试、工具审批登记和工具 Effect 准备均返回 AGENT_CANCEL_REQUESTED，回滚取消后各正常路径继续；同一用例还覆盖上轮的取消与继承并发，1 passed in 39.14s。未接触在线 Bot；其 Agent SQL 仍为 2.20.0，当前候选需独立清库或显式升级后才能部署。
Agent 已交接模型回执恢复（2026-10-02）：Agent 包 4.30.0 新增对内核只读 runtime.effect-executor-outcome/1 的依赖。内核按准确 Effect/Attempt、冻结派发命令、Provider Generation 与因果根读取持久 service_operations，只在已成功提交的服务回执中返回结果；Agent 对 RESPONDED 或明确 NOT_SENT 使用原 Attempt 的 RECONCILIATION 观察，不再次调用 Provider。无服务操作、仍在执行、失败或 UNKNOWN 仍保持待对账，不能推断未发送。隔离完整 Bot PostgreSQL 故障注入使 Provider 回执成功持久化后、Agent 观察事务前报错；后续执行从内核回执恢复并完成一次回复，Provider 请求只有一次（1 passed in 53.17s）。正常成功链与账本绑定查询通过（1 passed in 52.91s），本地非 PostgreSQL 回归 486 passed、9 skipped；未向真实 QQ 发送。Provider 结果 Payload 已写但服务回执未提交的窗口及真实 Worker 进程崩溃仍待验收，在线 Bot 尚未部署此版本。
Agent Provider 结果 Payload 恢复（2026-10-02）：Provider 包 4.3.0 通过只读 `provider.model.result.lookup/1` 查询原 Effect/Attempt 的冻结派发命令，并按 Provider owner 身份读取、校验已提交的 `model.result` 和可选 usage；Agent 包 4.31.0 在内核 service_operations 尚无成功回执时使用这条查询证据，以同一 Attempt 的 RECONCILIATION 观察继续执行，绝不据结果缺失推断未发送或重派。隔离完整 Bot PostgreSQL 故障注入在结果 Payload 已提交、服务回执尚未写入时中断，随后完成一条群回复且 Provider 请求只有一次（1 passed in 80.63s）。当前尚未覆盖真实进程在这一窗口被杀、换代后跨 Generation 查询及在线 Bot 部署；在线实例未改动。
部署包化边界审计（2026-10-02）：核对当前工作树后确认 Bot 业务组件均由包图装配，但部署管理仍由 `deployment_management.py` 中的宿主对象直接持有目录、账号、Secret、供应和审计 authority；`DeploymentManagementGateway` 为它们合成固定 `deployment/<component>/1` 身份，没有真实 PackageSnapshot、Worker、Generation 或热重载生命周期。这是未完成的部署控制面重构，不应被 Bot 包图完成度掩盖。本轮先使通用包清单能静态捕获 `bot/deployment/host` 三种明确 scope，并要求 storage.scope 与组件 scope 一致；现有 Bot 装配继续显式拒绝非 Bot 包，避免把新工件误装进 Bot 图。包目录定向回归 91 passed、1 skipped，19 个官方 builtin 工件逐一重建通过，Ruff 通过。此步只建立不可变工件与 scope 围栏，尚未提供 deployment/host 包运行时，也未删除旧部署管理对象。
跨作用域包目录隔离（2026-10-02）：通用 PackageCatalog 现在可按声明 scope 生成独立修订和键集页；Bot 图的 `deployment.package-registry.packages` 内核只读桥固定请求 `bot` scope，不接受 Worker 自选 scope，准确摘要工件查询也拒绝 deployment/host 包。混合 Bot/部署工件目录测试覆盖分页结果、scope 与摘要查询围栏；包目录回归 92 passed、1 skipped，Ruff 通过。当前 scoped page 为校验目录完整性逐项读取不可变工件，部署包数量增长后的目录索引优化仍未完成；deployment/host 真实 Generation 运行时仍未建立。
跨作用域 Worker 身份（2026-10-02）：隔离 PythonWorker 的不可变身份现包含 `scope_kind` 与 `scope_id`，Bot Worker 同时保留准确 Bot ID；deployment/host Worker 明确要求 `bot_id=null` 和自身稳定 scope ID，包清单声明与 Worker 作用域不一致在创建时拒绝。`forest plugin check` 根据工件声明构造该身份，真实 deployment 子进程回传 scope 身份并证明没有伪 Bot ID。Worker 与包目录相关回归 126 passed、1 skipped，隔离 PostgreSQL 完整 Bot 群触发到回复 1 passed in 52.01s，Ruff 通过。这只打通 Worker 启动身份；`PackageCallScope`、部署存储/授权端口、部署 Registry 和 Web 网关仍需迁入真实包图，旧 `deployment_management.py` 尚未删除。
跨作用域包调用身份（2026-10-02）：`PackageCallScope`、owned-work scope 和 `PackageGenerationAdapter` 现共用 `bind_scope_identity`，要求 Bot 调用绑定准确 Bot ID，deployment/host 调用不携带 Bot ID 且显式固定 scope ID；该校验文件进入不可变隔离 Worker SDK。包描述符在 deployment/host 作用域不再自动依赖 Bot 的 `kernel.runtime/payload/authorization`，纯服务包可构成同作用域的描述符图。定向 Worker/包运行时 44 passed；本地非 PostgreSQL 496 passed、9 skipped，隔离 PostgreSQL 完整 Bot 群触发到回复 1 passed in 51.97s，Ruff 通过。此处尚未建立部署数据库的 Generation authority、配置/授权/Domain 端口和部署 Web 网关切换，旧部署管理宿主对象仍在运行，不能称为 deployment 包热重载完成。
部署包存储作用域（2026-10-02）：`StorageBundleSpec` 与内容摘要现保留声明的 `bot/deployment/host` scope，描述符图拒绝存储 scope 与运行图不符；Bot 安装器显式只接受 Bot 包。部署数据库基线新增独立的 `forest_deployment.schema_bundles`，`DeploymentSchemaBundleInstaller` 只安装 deployment 包的冻结 SQL 到模块命名空间，按 deployment ID、schema ID、包摘要和 SQL 摘要登记并校验；部署目录重开时核对已登记命名空间。`PostgresPackageDomainAuthority` 按调用的准确 scope 身份选择 Bot 或 deployment 账本，并向 SQL Domain context 明确传递 scope_kind/scope_id；host 存储没有已建立的数据库 authority，仍被拒绝。隔离 PostgreSQL 的部署目录测试 14 passed，包目录与包运行时 104 passed、1 skipped。部署库基线摘要已改变，现存预发布部署数据库需要重建后才能由包含此改动的新 Supervisor 使用；这不是在线迁移。实际 deployment Generation authority、生命周期、配置/授权端口和 Web 网关切换仍未完成，旧部署管理对象还不能删除。
部署 Generation 持久权威（2026-10-02）：部署基线现包含 `runtime_graph_clock/runtime_graphs/runtime_generations/runtime_graph_members`，按真实 deployment ID 和 Supervisor lease 冻结候选、原子发布与退役、保留成员、局部准入和 route epoch；准备及发布均复核 deployment 包存储账本。`DeploymentGenerationAuthority` 拒绝过期来源、跨 owner epoch、未满足依赖闭包的保留和旧图写入；共享的准备绑定校验从 Bot repository 提取为 scope-neutral 纯契约。Registry 的 lease 端口改为 `runtime.authority_lease()`，BotHost 与新 `DeploymentRuntimeScope` 各自提供真实 lease。隔离 PostgreSQL 验证图准备/重放、发布、保留、竞态候选拒绝、局部冻结/恢复、退役、旧图终态、只读快照及租约失效；部署目录 15 passed，最终 deployment Generation 与完整 Bot 群触发成功链 2 passed；本地非 PostgreSQL 496 passed、9 skipped，Ruff 通过。**生产 Supervisor 尚未实例化 deployment RuntimeModuleRegistry，部署 builtin 工件、配置/授权端口和 Web 网关仍未切换，`deployment_management.py` 与伪 Generation 仍在运行。**此基线版本变化需要重建未上线部署数据库，不能直接更新当前在线库。

部署模块配置事务权威（2026-10-02）：新增可安装到每个 deployment 包私有命名空间的配置 SQL 基线及 `DeploymentModuleConfigurationRepository`，以真实 deployment ID 保存不可变修订、计划、激活与 desired/effective 头；配置值与内容摘要同库存储，不伪用 Bot Payload。初始化、计划、提交与图发布时的 `activate_in` 均按操作身份幂等，激活在同一发布事务中复核冻结修订和内容摘要。隔离 PostgreSQL 已验证真实存储账本、两次图发布、配置修订切换、错误摘要导致发布回滚且旧图/旧 effective 保持原状，以及同一操作并发重放合流、两个计划争夺同一 desired 修订时仅一个提交；部署目录 16 passed，Ruff 通过。**这仍是部署包运行时的底层端口：生产 Supervisor 尚未装配 deployment 包 Worker、授权和配置重载协调器，WebUI 仍使用旧对象与伪 Generation，旧文件不能删除。**
部署审计包切换（2026-10-02）：`deployment.audit` 已从宿主管理查询类移入 `builtin/deployment_audit` 的不可变部署包，SQL 查询、过滤/分页、输入输出 schema、隔离 Worker 服务和 UI 均随包发布；宿主只保留跨管理操作事务追加审计事实的内核账本。Supervisor 装配真实 deployment `RuntimeModuleRegistry` 和包私有配置，WebUI 经活动 Registry 发现贡献，以真实 Graph revision 与 Generation 绑定票据并在调用前复核。隔离 PostgreSQL 中完成一次候选预热、旧代际排空、图发布与新代际查询，部署目录全套 17 passed；Ruff 通过。本轮没有把生产的“选择包并触发部署热替换”入口做完，也未将账号、全局配置、Bot 目录、Secret 和供应对象迁入包；当前引导器只允许官方只读 deployment 包，通用部署授权和写端口仍缺失。部署数据库基线有变，现有预发布库须重建，线上 QQ 实例未更新或发送消息。
部署包生命周期控制器（2026-10-02）：生产 Supervisor 现在由 `DeploymentPackageController` 持有 deployment Registry，替换操作按预期图修订执行候选预热、旧代际排空、发布与失败恢复；进程再次启动时从部署数据库活动 Generation 的包绑定恢复摘要，并从持久包目录打开同一不可变工件，缺失或篡改时拒绝悄悄回退默认包。候选清理、旧图恢复或发布后关闭失败时控制器转入致命状态，Supervisor 观察该状态并退出，避免继续提供不确定的路由。部署 Web 写回执不再把图修订写死为 1：新的真实图操作在同一数据库事务中锁定活动图头并核对 `$runtime` 或模块 Generation/控制准入，旧宿主组件的伪 Generation 仍仅作为过渡分支。隔离 PostgreSQL 实测替换、错误修订拒绝、重启后保持替换版、旧图票据拒绝与新图/模块票据接受，部署目录全套 18 passed；本地管理/包运行时 15 passed、1 skipped，Ruff 通过。控制器尚无 WebUI 持久替换请求入口，因此当前运行中的生产用户还不能从管理台触发替换；其余部署宿主对象和通用部署写端口仍待迁移，在线 QQ 实例未更新。
部署图替换管理入口（2026-10-02）：新增部署内核持久替换请求账本及 Supervisor 认领循环。管理员经通用 `$runtime/module.replace` Web action 提交准确模块、不可变包摘要和预期图修订；票据回执、部署审计和 QUEUED 请求在同一事务提交，旧图/Generation 票据由图头锁复核。请求固定 activation ID，运行结果按活动或退役图证据结算；旧 owner 发布前中断的请求明确失败，避免新 lease 盲目重用旧 PREPARED activation。管理台注册表加入紧凑的部署图替换面板，可提交并查询状态，Bot scope 不显示。隔离 PostgreSQL 端到端验证管理员入队、审计只写一次、Supervisor 热替换、状态查询、同一幂等键重放及不同输入冲突；部署目录全套 19 passed，本地 Supervisor/管理回归 12 passed、1 skipped，Ruff 与 JS 语法检查通过。**当前仍仅支持已装配的官方只读 deployment 包，Web 包导入/候选检验、通用部署授权与写端口及其余宿主业务对象包化尚未完成；未上线部署数据库基线须重建，在线 QQ 实例未更新。**

部署包官方来源围栏（2026-10-02）：热替换此前只检查工件自报的 `official` SQL profile，普通构建或 ZIP 导入的同类工件可被控制器作为官方包装配。包目录现在仅由宿主显式 `build_official` 登记部署官方摘要，部署控制器启动、重启恢复和替换均按该独立事实读取；普通 `build` 工件在图修订正确时仍被拒绝。隔离 PostgreSQL 定向验证拒绝、登记后热替换和重启恢复，以及 Web 队列替换（2 passed）；Ruff 与差异检查通过。此围栏不赋予第三方部署包执行能力：其受限存储 profile、部署授权、写端口和 Web 导入仍未实现；现有部署目录只有官方只读包。

普通部署 Worker 新增与热替换（2026-10-02）：新增声明式 read 服务包编译路径，普通工件不得声明官方 SQL、私有存储、写入/Effect/后台任务或额外 capability；宿主可信的标准配置 SQL 单独安装到组件私有命名空间，包 bootstrap seed 和 schema 经冻结摘要复核。持久 `runtime_graph_change_requests` 账本区分 `ADD` 与 `REPLACE`，Web 管理台可在通用图面板提交新组件 ID 与包摘要，并通过 `module.change.status` 查询；纯新增时旧图成员直接保留，后续定点替换只排空受影响集合。隔离 PostgreSQL 端到端从普通 ZIP 导入的示例包完成 Web 新增、Worker 查询、自带管理 UI 工件读取、再次导入新版本、同进程定点热替换、未受影响审计 Generation 保留与重启恢复；部署目录全套 19 passed，本地 Supervisor/管理 12 passed、1 skipped，包目录 94 passed、1 skipped，示例包独立 Worker check、Ruff 与 JS 语法通过。**普通包目前仍仅限声明式 read 服务，Web ZIP 上传、候选预检、私有 SQL、配置换代、通用部署写/Effect 端口和其余宿主业务对象包化尚未完成；Python Worker 不是恶意代码 OS 沙箱。未上线部署目录基线已改变，在线 Supervisor 未更新。**

部署候选 Graph 预览（2026-10-02）：`DeploymentPackageController` 现在可按目标包摘要只读编译候选描述符，复用生产 Graph 规划器生成准备、排空、保留集合和容量事实，再生成规范 `plan_digest`。管理台先预览后允许提交；Web action 对新操作重算并比较摘要，持久请求冻结摘要，同一操作重放按已有回执结算。隔离 PostgreSQL 的部署目录、管理网关与 Supervisor 回归共 31 passed、1 skipped；新增预览的重复摘要稳定，预览后目标组件没有安装 schema，也没有新建图变更请求；篡改摘要被拒绝。Ruff 与 JS 语法检查通过。预览不执行 Worker 预热或发布，不能保证这些阶段成功；Web ZIP 导入仍应由部署包目录模块提供，通用部署写/授权端口和旧宿主业务对象包化仍未完成。此轮变更修改了未上线部署库基线，在线 Supervisor 尚未更新。

部署包目录真实 Generation（2026-10-02）：新增 `builtin/deployment_package_registry`，包内 Worker、声明式查询契约和 UI 持有全部署包列表与准确工件详情，允许按 `bot/deployment/host` scope 筛选；受限原生只读驱动按当前 Generation、deployment scope 和组件身份调用物理 `PackageCatalog`。官方来源登记不再强迫所有官方 deployment 包自带业务 SQL；无业务存储的目录包仅安装标准配置账本，普通 ZIP 即使模仿同组件 ID 也不能获得原生端口。隔离 PostgreSQL 验证双官方包 Graph 启动、Worker 查询、Bot 包详情、未登记目录包的原生端口拒绝、目录定点热替换保留审计 Generation 和重启恢复；部署目录、管理网关与 Supervisor 合计 32 passed、1 skipped，包目录 95 passed，Ruff 通过。**这只是 Web 目录读侧包化：Bot Worker Host 的候选只读桥仍直接访问物理目录；有界二进制 ZIP 上传/导入与下载、导入错误投影、部署写授权及旧宿主业务对象的清除仍未完成。在线 Supervisor 未切换，现有预发布部署库需按新 Graph 装配要求重建或显式增装目录包。**

Bot 包候选桥收敛（2026-10-02）：`kernel.runtime` 的 `deployment.package-registry.packages/artifact` 只读服务保留冻结 ABI，但不再直接读取 Bot Host 的物理 `PackageCatalog`；生产组合根将受限读取器绑定到部署控制器，控制器每次解析当前活动的 `deployment.package-registry` Generation 并调用包声明的管理查询。内核桥固定 `scope=bot`，包详情再次校验 scope；目录缺失、排空或热替换窗口不回退物理目录。Bot Host 自己对准确包摘要的选择与装配仍使用物理工件权威。隔离 PostgreSQL 验证真实目录 Generation 的 Bot 列表、详情、跨 scope 拒绝和热替换后继续查询；部署目录、管理网关与 Supervisor 共 32 passed、1 skipped，包目录 95 passed，Ruff 通过。此桥是明确限于目录查询的跨 scope 能力，不代表通用跨 scope Contract Router 已完成；Web ZIP 导入、通用部署写端口和旧部署管理宿主对象清除仍待实现。在线 Supervisor 尚未切换。

部署包下载 artifact（2026-10-02）：包清单新增有界只读 `management_artifacts` 声明，`deployment.package-registry` 1.1.0 把原包 ZIP 导出绑定到受限目录 Effect 端口；管理网关按活动 Generation、管理员角色、CSRF 和票据授权，验证媒体类型、文件名与 32 MiB 上限后以固定响应头交付。包内 UI 通过沙箱消息通道请求下载，壳层只执行通用声明路由。真实官方包构建、Ruff 和隔离 PostgreSQL 的包端口及 ASGI 下载回归已通过。当前端口和网关仍以有界字节数组持有完整 ZIP；通用工件句柄/流式读取、上传/不可变导入、导入错误投影和模块自有幂等导入账本仍待实现，在线 Supervisor 尚未切换。

部署包导入合流基础（2026-10-02）：`PackageCatalog.import_archive_idempotent` 现在校验完整 ZIP 的 SHA-256，并把操作键、管理员主体、ZIP 摘要和已发布工件摘要与包版本登记放在同一 SQLite 写事务；并发同键同内容重放返回同一工件，同键换主体/换内容拒绝，错误摘要和版本冲突不产生成功回执。定向并发用例及包目录完整回归 96 passed，Ruff 通过。此提交只实现物理目录的原子导入原语；部署模块仍处于只读装配，Web 二进制上传、Generation 排空租约、写票据/事务审计、模块自有导入错误投影尚未接通，不能据此声称管理员已能通过控制台导入 ZIP。

部署包 Web ZIP 上传候选（2026-10-02）：`deployment.package-registry` 1.2.0 通过声明式 upload artifact 和受限 `deployment.catalog.import` 幂等端口接收 ZIP，包内隔离 UI 仅用通用消息通道选取文件并提交。管理网关在读取正文前校验管理员会话、CSRF、媒体类型、声明字节数、SHA-256、幂等键与准确活动 Generation，并在 deployment PostgreSQL 同一事务内写 Web 签票回执和请求审计；Generation 租约覆盖有界暂存与物理导入，20 秒请求期限、最多两个并发 artifact 传输，排空等待旧请求并拒绝旧代际新请求。目录端口拒绝 Web 占用官方 `forest.*` 命名空间，完整 ZIP 校验与包版本/导入回执在 SQLite 事务中合流。隔离 PostgreSQL ASGI 回归覆盖导入、同键重放、换内容冲突、官方命名空间拒绝、容量门禁与热替换排空；扩展回归 131 passed、1 skipped，Ruff 和两处 JS 语法检查通过。仍缺模块自有的导入结果/错误列表、跨 PostgreSQL/目录库故障的自动结算、真浏览器与在线 Supervisor 上传验收，以及大工件流式句柄；这些不能由本次有界 ZIP 路径推断完成。

部署包导入恢复证据（2026-10-03）：物理 `PackageCatalog` 新增只读 `import_receipt(operation_id, actor_ref, archive_sha256)`，从持久 SQLite 成功回执重新验证不可变工件并返回同一目录投影；未知操作返回空，主体或 ZIP 摘要不符拒绝。`deployment.package-registry` 1.3.0 将这一证据经受限 Effect 端口和声明式管理员查询暴露给当前 Generation，隔离 PostgreSQL 的真实 ASGI 上传与查询用例通过。由此跨 PostgreSQL/目录库中断后不必凭 HTTP 响应或暂存文件猜测导入是否已发布。Windows 本地包目录回归 95 passed、1 skipped，Ruff 通过。详细设计已固定模块私有 `RESERVED → SUCCEEDED/REJECTED/INTERRUPTED` 账本、事务内 reserve 和以物理回执为证据的恢复规则；这些模块私有 SQL、状态查询、结算与恢复 Worker 仍未实现，现有上传网关继续使用过渡的请求审计路径。

部署包导入状态账本（2026-10-03）：`deployment.package-registry` 1.4.0 自带私有 PostgreSQL SQL bundle，`management_artifacts` 声明预留和结算 Domain Command；通用上传网关在一个事务中提交 Web 票据回执、审计及 `RESERVED`，物理 SQLite 发布成功后结算 `SUCCEEDED`。包声明 `import.page` 管理查询和自带 UI 的导入记录视图，支持状态、主体、包摘要筛选及游标分页；部署网关现在按查询声明的角色授权，管理员专用查询不再降为 `AUDITOR`。隔离 PostgreSQL 的真实上传、同键重放与状态查询已通过。失败码结算、超时 `RESERVED` 的包自有恢复工作和两库故障注入尚未完成；已知发布拒绝目前仍显示 `RESERVED`，不可据此判断仍在上传。

部署包导入确定失败结算（2026-10-03）：`deployment.package-registry` 1.4.1 为上传声明只读成功回执查询；正文大小或 SHA-256 在物理发布前校验失败时，网关先经当前模块 Generation 查询持久 SQLite 回执，有成功证据则补结算 `SUCCEEDED`，无成功证据才在 PostgreSQL 内以类型化错误码结算 `REJECTED`。已终止的拒绝操作不能再用同一幂等键导入；成功操作的损坏重放不会把 `SUCCEEDED` 改写为失败。隔离 PostgreSQL 的真实 ASGI 用例覆盖上述状态。物理发布已经开始而结果不明时仍保留 `RESERVED`，待包自有恢复工作核对；部署作用域尚未安装 owned-work 持久执行账本，不能把后台恢复伪装成一个无账本的循环。

部署作用域 owned-work 基础（2026-10-03）：`forest_deployment.package_service_operations` 成为通用 Generation 围栏的持久服务操作账本，按 deployment/component/operation 身份记录输入摘要、执行者、期限和有界 JSON 终态；同键并发只放行一次，成功与失败可重放，超时未证实的执行转为 `UNKNOWN`，迟到的旧调用不能改写终态。`DeploymentRuntimeScope` 将它作为包运行时能力提供，官方部署包可声明自有 owned-work handler；真实 PostgreSQL Generation 的入场、重放、冲突和迟到拒绝用例已通过。此变更更新了未上线部署库基线，现有预发布库需重建。`deployment.package-registry` 还没有声明恢复 handler；对原上传与恢复扫描的交错仍需设计物理发布围栏，不能仅凭超时自动标记 `INTERRUPTED`。

部署包导入物理关闭围栏（2026-10-03）：`deployment.package-registry` 1.5.0 把管理上传的确定性拒绝改为调用声明式 `deployment.catalog.import-close` 幂等 Effect。物理目录在同一 SQLite 写锁内裁决发布成功回执和关闭墓碑：已成功则返回包摘要并补结算 `SUCCEEDED`，未成功则先持久关闭操作键，再允许 PostgreSQL 结算 `REJECTED`；旧上传随后进入目录时会被拒绝。并发目录测试与隔离 PostgreSQL 的 21 项部署包回归通过。超时 `RESERVED` 的模块自有恢复 Worker、发布阶段故障分类仍待完成。

部署包导入包自有恢复（2026-10-03）：`deployment.package-registry` 1.6.0 在私有 SQL bundle 内声明恢复 claim/settle，包内 `@job` 通过受限幂等 `deployment.catalog.import-close` Effect 裁决物理回执或关闭墓碑。超时 `RESERVED` 由持久 claim token、租约、尝试号与结算账本管理；物理成功补结算 `SUCCEEDED`，确认无发布且关闭后结算 `INTERRUPTED`，Effect 失败只延迟重领并记录错误。隔离 PostgreSQL 的真实 Generation 用例已覆盖两种跨库存量、暂时性目录 Effect 失败后重领及热替换；发布阶段的确定性失败分类、真实进程崩溃重领和在线切换仍待验收。

部署包导入真实进程崩溃重领（2026-10-03）：新增独立进程故障注入，子宿主在 `deployment.catalog.import-close` 的 SQLite 关闭墓碑已提交而 owned-work 结果尚未返回时立即 `os._exit(137)`；父进程只读核对墓碑存在、成功回执不存在且 PostgreSQL 仍为 `RESERVED`/首轮 claim。真实 Supervisor 租约到期后新宿主恢复准确目录 Generation，待原工作租约到期自动重领第二次尝试，并把同一操作结算 `INTERRUPTED`；隔离 PostgreSQL 用例通过。此证据覆盖关闭后的崩溃窗口，不覆盖 ZIP 发布成功后崩溃、真实 Web 请求中途断连或在线 QQ Supervisor 切换。生产 `supervisor_host.py` 仍装配 `deployment_management.py` 的 Bot 目录、Secret、账号和供应旧宿主业务组件；这些入口还没有包化，不能直接删文件。

部署账号包切换（2026-10-03）：`deployment.accounts` 迁入 `builtin/deployment_accounts`，包拥有分页账号查询、管理 UI 和四个操作声明；Web 身份、密码摘要、会话撤销、最后管理员保护及账号操作账本仍由内核持有。创建和改密使用宿主 `secret-action` 输入密码，明文不进入包 iframe/Worker；内核命令在准确 CONTROL Generation 围栏及 Web 回执/审计同一事务内执行账号写入，耗时密码哈希在事务前准备。删除旧宿主账号伪组件和静态界面。连续热替换回归发现来源图重复持久化旧 `STOPPED` 状态，关闭流程现仅记录本次释放的组件。隔离 PostgreSQL 的部署目录与 Web 身份全套 28 passed；密码哈希准备调整后的针对性 8 passed；本地管理网关与包目录 100 passed、2 skipped，Ruff 通过。生产在线 Supervisor 尚未切换；剩余旧宿主供应管理和模块配置等边界继续迁移。

本地 Storage 供应合流（2026-10-03）：旧 `deployment.provisioning` 入口此前把 DSN Secret 和 Storage Profile 分两笔事务写入，Profile 冲突会留下孤立 Secret。现复用最终受限内核命令的 `prepare/execute_in` 边界，在同一事务中接纳 Web 回执、检查或写入 Secret、创建 Profile；按操作 ID 加事务锁，成功重放不重新生成随机 DSN，失败整体回滚。隔离 PostgreSQL 回归明确验证已存在 Profile 引起的 409 后新 Secret 不存在；两个并发同键请求返回相同回执且只生成一个 Secret 版本，WebUI/图热替换针对性用例通过（1 passed）；Ruff 通过。该命令尚未由可热重载供应包声明，旧供应 UI 与伪 Generation 仍待移除。

Bootstrap 供应事务端口准备（2026-10-03）：`BootstrapProfileKernelCommand` 已把模块贡献注册表的集合校验放在事务前准备，把服务端 schema digest、Secret 引用核对与不可变 Profile 写入放入调用者的同一部署事务；旧 `bootstrap.create` 入口复用该最终命令边界。供应 Job 的 enqueue/cancel 权威也已允许加入调用者事务，为准确 Generation 与 Web 回执围栏接入做准备。隔离 PostgreSQL 的 WebUI/图切换、Bootstrap Profile 不可变性及供应 Job 租约针对性用例共 3 passed，Ruff 通过。供应包及通用嵌套 Bootstrap UI 承载仍未实现，不能把这一步称为供应组件包化。
