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
| Memory | `memory_graph` | 多主干来源、图文嵌入、精确召回、固定匹配、按需图区域读取及检索区域冻结；规范图文证据物化已接入；Agent 语义接入待补齐 |

内核的 Runtime、Payload、Authorization、Effect 与 Audit Authority 仍是可信微内核组件。它们不是可卸载业务插件，也不应迁入 `builtin`。

## 4. 已切换的生产 builtin

生产组合根已统一通过 `bootstrap_builtin_package()` 装载以下组件。Host 只装配配置、能力授权、效果驱动和冻结契约句柄，不再构造这些组件的 Repository 或业务 Runtime：

| 组件 | 生产包 | 当前状态 | 剩余工作 |
|---|---|---|---|
| Agent | `builtin/agent_runtime` | 隔离 Worker、私有 SQL、manifest UoW 与 owned-work 已接入生产图 | 继续审计剩余宿主边界并补齐可观测管理面 |
| Conversation | `builtin/conversation_scheduler` | 入站、分类、静默归档、Agent 调度与安全边界合流已接入生产图 | 扩充管理查询与长时间压力验证 |
| Memory | `builtin/memory_graph` | 多模态来源、归档资格、精确/语义召回、DOCUMENT/QUERY 嵌入发布、按需图扩散、检索区域冻结、规范证据物化/发布、成功回复联想日志及归档提升、单来源遗忘固定计划/管理入队和撤销正文后台清理已运行 | 真实跨群学习验收、大范围遗忘选择器、管理召回试验和完整管理面 |

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

Memory 来源管理查询新增群来源集合、精确来源、平台/Realm/群号、Binding、来源状态和单来源序号区间，均在包 SQL 内服务端筛选，并进入游标的筛选摘要；新增 Binding 浏览索引。模块 UI 将较少使用的条件收进折叠区，避免拉高工作区。隔离 PostgreSQL 主链验证五组通过，覆盖组合条件、错误群来源、状态变化后的游标拒绝。尚未提供索引覆盖/关系审核筛选和批量遗忘选择器，不能用此查询替代固定批量影响计划。

关系管理查询已停止用 `created_at` 判定分页快照中的正向事实：首读捕获 PostgreSQL `pg_snapshot`，游标携带并绑定该快照，关系贡献、源端点与必要证据都以事务 ID 检查快照可见性，同时重验当前资格。结果明确返回相对 seed 的方向、创建来源、跨主干状态、关系键以及证据的来源 ID、平台、群/主干和作者；包 UI 可按关系类型、来源、方向与跨主干状态筛选。当前只有一跳权威事实浏览，审核与发布图投影尚无权威记录，因此回执不包含审核或投影状态，也不能把它展示成已审核或已发布图版本。真实 PostgreSQL 的迟提交关系测试证明旧快照不会混入该关系；图文主链的 5 组用例中一次旧测试的后台 Worker 与手动 claim 竞速，已把测试冻结点移到手动仲裁之前并复验通过。

## 8. 下一切换顺序

1. 补齐较大范围遗忘选择器与分页管理，完成管理召回试验；补齐发布崩溃重放验证。
2. 验证 Agent 语义召回的真实 QQ 群聊链、图片召回命中与跨群成功回复及联想提升；恢复及重放需与删除 epoch、Generation 围栏收敛。
3. 补齐证据浏览的进一步分页以及图文真实召回试验。
4. 验证热重载失败回滚、长任务 drain、群并发、审批、Provider/Tool 不确定结果和 WebUI 包管理贡献。
5. 在真实 OneBot 实例上只进行准许群的单次低频交互验收。

生产图已经是微内核包系统；目标重型 Bot 框架仍需完成 Memory 主链和长期运行验证。
