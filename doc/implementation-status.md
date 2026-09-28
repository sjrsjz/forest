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

向量索引的 `upsert/search` 契约现为 major 2：Milvus 只保存 `ref_id`、Bot/空间/用途路由字段和向量，ANN 只返回 ref 与分数；群来源、修订、作者和模态均由 Memory 按 ref 在 PostgreSQL 权威表复核。collection 名称按新布局分代，避免旧列布局在热重载或重建时被误用。此收敛移除了管理试验查询必须伪造来源才能写入索引的协议障碍；试验的嵌入 claim、Provider 输入、发布资格和 owned-work 尚未接通。

内核的 Runtime、Payload、Authorization、Effect 与 Audit Authority 仍是可信微内核组件。它们不是可卸载业务插件，也不应迁入 `builtin`。

## 4. 已切换的生产 builtin

生产组合根已统一通过 `bootstrap_builtin_package()` 装载以下组件。Host 只装配配置、能力授权、效果驱动和冻结契约句柄，不再构造这些组件的 Repository 或业务 Runtime：

| 组件 | 生产包 | 当前状态 | 剩余工作 |
|---|---|---|---|
| Agent | `builtin/agent_runtime` | 隔离 Worker、私有 SQL、manifest UoW 与 owned-work 已接入生产图 | 继续审计剩余宿主边界并补齐可观测管理面 |
| Conversation | `builtin/conversation_scheduler` | 入站、分类、静默归档、Agent 调度与安全边界合流已接入生产图 | 扩充管理查询与长时间压力验证 |
| Memory | `builtin/memory_graph` | 多模态来源、归档资格、精确/语义召回、DOCUMENT/QUERY 嵌入发布、按需图扩散、检索区域冻结、规范证据物化/发布、成功回复联想日志及归档提升、单来源遗忘固定计划/管理入队和撤销正文后台清理，以及大范围遗忘的冻结成员目录、有界批次预览和只读核对 UI、私有执行事务已运行 | 真实跨群学习验收、大范围遗忘的批次 P0 授权/执行进度与 UI、管理试验的真实浏览器及供应商图文验收、完整管理面 |

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

Memory 来源管理查询新增群来源集合、精确来源、平台/Realm/群号、Binding、来源状态和单来源序号区间，均在包 SQL 内服务端筛选，并进入游标的筛选摘要；新增 Binding 浏览索引。模块 UI 将较少使用的条件收进折叠区，避免拉高工作区。隔离 PostgreSQL 主链验证五组通过，覆盖组合条件、错误群来源、状态变化后的游标拒绝。关系审核筛选和批量遗忘授权执行仍未完成，不能用来源查询替代固定批量影响计划。

来源浏览现以首次查询的 `pg_snapshot` 绑定游标，并以 `source_units.accepted_xid` 和归档提交的 `eligible_xid` 判断首读可见性；`accepted_at` 仅作时间边界与显示元数据，不再冒充提交快照。隔离 PostgreSQL 验证：来源事务在首屏读时尚未提交、提交后其旧时间戳不会插入后续页，新查询仍可看到它；当前删除/资格过滤继续生效。模块 UI 与查询契约已传递 `snapshot_xid`，但大范围选择器及完整管理召回试验仍未实现。

关系管理查询已停止用 `created_at` 判定分页快照中的正向事实：首读捕获 PostgreSQL `pg_snapshot`，游标携带并绑定该快照，关系贡献、源端点与必要证据都以事务 ID 检查快照可见性，同时重验当前资格。结果明确返回相对 seed 的方向、创建来源、跨主干状态、关系键以及证据的来源 ID、平台、群/主干和作者；包 UI 可按关系类型、来源、方向与跨主干状态筛选。当前只有一跳权威事实浏览，审核与发布图投影尚无权威记录，因此回执不包含审核或投影状态，也不能把它展示成已审核或已发布图版本。真实 PostgreSQL 的迟提交关系测试证明旧快照不会混入该关系；图文主链的 5 组用例中一次旧测试的后台 Worker 与手动 claim 竞速，已把测试冻结点移到手动仲裁之前并复验通过。

Kernel Payload 的遗忘基础路径已将多来源根选择改为一次 `unnest` 集合连接，将来源目标与闭包 Payload 记录改为集合插入；删除屏障、完整闭包摘要、100000 Payload 上限及重放围栏不变。真实 PostgreSQL 的多来源共享派生载荷验证表明，同一派生 Payload 只进入闭包一次，未选来源仍保留；原有单来源、预览变更和双锁顺序并发测试均通过。这是大范围遗忘选择器需要的下层基础，不等于已交付 Memory 私有选择目录、分批计划或管理 UI。

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

1. 将已有冻结选择与批次预览接到 P0 授权、持久分批执行、部分完成状态和分页管理 UI；完成管理召回试验，补齐发布崩溃重放验证。
2. 验证 Agent 语义召回的真实 QQ 群聊链、图片召回命中与跨群成功回复及联想提升；恢复及重放需与删除 epoch、Generation 围栏收敛。
3. 补齐证据浏览的进一步分页以及图文真实召回试验。
4. 验证热重载失败回滚、长任务 drain、群并发、审批、Provider/Tool 不确定结果和 WebUI 包管理贡献。
5. 在真实 OneBot 实例上只进行准许群的单次低频交互验收。

生产图已经是微内核包系统；目标重型 Bot 框架仍需完成 Memory 主链和长期运行验证。

现行 Memory 包 UI 增加独立真实浏览器冒烟脚本 `scripts/smoke_memory_module.py`：本机 Edge 验证了隔离 MessageChannel 初始化、管理试验结果中的群号/作者归因、超过两个 96 KiB 块的原图显示，以及分块被篡改时摘要拒绝显示。旧管理静态目录已经移除，依赖它的旧面板脚本及仍引用旧页面 DOM 的旧整站冒烟脚本同步删除；此浏览器 fixture 不替代真实 HTTP 会话、数据库和供应商图文召回验收。

试验元数据回收现位于 `memory_graph` 包的向量删除 owned-work 认领事务：先有界抑制到期 facet 并为任何曾尝试派发的 facet 登记删除屏障；没有待领删除任务时，每轮最多回收一份到期试验、检查八份候选。Bot 级持久游标跨轮轮转和回绕，使前八份因有效租约而阻塞时，第九份仍能被回收。事务锁定试验输入，要求召回工作终结、所有 facet 已抑制、相关向量删除均结算、嵌入 claim/attempt/发布/恢复工作没有在途状态、预算预留已结算且没有成功回复联想日志。过期但没有外部尝试的旧 claim 与召回工作由该事务收敛，再按外键顺序删除试验 part、查询、facet、证据区域和包内结算行；向量删除 tombstone 与费用预算保留。仅对应 Bot、试验查询、bundle 和 claim 的不可变事实触发器在该事务内放行，正常群查询不受影响。真实 PostgreSQL 冻结 bundle/证据、过期 claim、有效租约阻断、向量屏障、游标轮转及并发认领用例通过，Memory 包 19 项数据库回归通过；Kernel Payload 的物理清扫仍按其独立 TTL 路径运行。真实供应商图文召回和长时间运行验收尚未完成。

部署管理网关现经真实本机 HTTP 与 Edge 浏览器验证登录、Bot 作用域选择、Memory 包 UI 的摘要读取与隔离 iframe 初始化、管理 query 的 MessageChannel 往返、卸载和登出。验收发现原 Blob iframe 继承控制台 CSP 后无法执行包内脚本，已改为带摘要的认证路由加载并独立施加 sandbox CSP；作用域路径改写曾把全局登出误送往 Bot 路由，现只改写 Runtime 与模块接口。切换工作区时，旧 UI 的异步挂载和 MessageChannel 会失效。该浏览器验收使用内存管理后端，证明 HTTP 会话和包 UI 接线，不证明真实 PostgreSQL 管理试验、供应商图文召回或长期运行。

旧 `FOREST_MANAGEMENT_TOKEN` 单 Bot HTTP 入口已删除；该入口使用另一套令牌 API，无法与现行部署会话控制台及包 UI 协议正确配合。`CleanManagementGateway` 现必须持有部署 Web authority ticket signer，Bot scope 由部署控制台提供；单 Bot 启动函数只管理 Bot 生命周期和状态输出。真实 Edge 会话下的 5 项部署管理验收通过，单 Bot 主机本地 5 项测试通过、5 项需数据库环境而跳过；此清理不替代真实数据库管理试验。

包管理动作的最后入口也已收紧：`PackageGenerationAdapter.invoke_management_action` 必须同时收到 Web authority ticket 与事务内授权 guard，缺失即拒绝；声明 `actor_field` 的动作只从非空 `web_subject_ref` 注入审计主体，不再使用 `management:shared-token`。隔离 PostgreSQL 下的 Memory 管理试验、Job Scheduler 动作和完整 Bot 宿主遗忘/重启链路已通过；这些测试使用显式测试票据和 guard，生产路径仍由部署网关签发、消费真实票据。大范围遗忘选择器和长期压力验收尚未完成。

Memory 单次遗忘的执行阶段已将来源锁定、抑制记录、资格/观察更新、向量删除屏障、facet 和准备任务收敛改为有界集合写入，保留 Payload 删除纪元、固定影响计划、幂等操作回执和关系撤销。私有 PostgreSQL 上 19 项 Memory 包回归通过；修改后真实宿主遗忘、向量删除故障、热替换及重启链路通过。此改动是大范围遗忘的写入基础，仍缺分批计划和批量管理 UI。

Memory 已新增私有的 `forget_selections` 和 `forget_selection_members`：选择动作与来源浏览使用同一个 SQL 过滤函数，在 PostgreSQL 快照和接受时间下冻结最多一百万个来源的修订、作者载荷锚点、筛选条件、有序成员摘要和管理员主体；成员目录按序号分页，已冻结事实不可修改。同一创建操作幂等，新消息不会追加到旧选择，其他管理员不能读取成员。隔离 PostgreSQL 下新增选择、分页、主体隔离和来源浏览快照回归通过。选择目录与批次预览仍不能被标为完成了大范围遗忘。

批次预览现已接通 `memory.preview-forget-batch` UnitOfWork：Memory 从冻结目录按序号取最多 64 个当前有效成员，Kernel Payload 在删除时钟锁下对去重作者锚点形成闭包，Memory 再计算跨来源影响并固定成员、闭包、删除纪元和影响摘要。管理查询注入管理员主体，Worker 只携带选择引用与批次范围。真实 PostgreSQL 验证了两个根来源、共享派生 Payload、后续新来源隔离，以及包运行时管理查询的完整入口。此预览不能执行撤销；P0 批次授权、持久进度、执行和批量 UI 仍待实现。

单来源 Memory 遗忘的事务写入效果现收敛为包内私有的 `memory_apply_forget_effects`：它按稳定顺序锁定所有受 Payload 闭包影响的来源，并统一写来源抑制、向量删除屏障、facet/准备任务失效和关系贡献撤销。单来源操作继续校验原有精确计划、保存原有幂等回执；这个私有函数只是供后续批次执行复用的写入基础，不提供批次授权或批次执行入口。

Memory 包内现有私有 `memory.forget-batch` 三步 UnitOfWork：从冻结成员目录取根锚点，Kernel Payload 按预览的删除纪元及闭包摘要撤销，Memory 在同一事务内重算完整批次影响计划、复核管理员所属选择及成员修订，再复用遗忘效果函数并写入独立的不可变批次回执。错误计划使整笔事务回滚，重放返回原回执；PostgreSQL 用例覆盖两个根、共享派生 Payload、后续新来源隔离、错误计划回滚及幂等重放。此计划目前只供包内部组合，尚无管理 P0 入队、跨批进度、部分完成状态或批量 UI，不能作为已交付的大范围遗忘操作。

管理图文召回试验现经完整 Bot 宿主、真实 PostgreSQL、包自有 owned-work、假 Embedding HTTP 和假 Milvus 贯通：两份经过管理资产权威绑定的文本/PNG 输入启动独立试验，自动准备/匹配/冻结/物化后达到 READY；结果分页返回原群号与作者，图片按归属 part 读取，另一管理员无法读取。发现并修正试验没有当前来源时证据节点把 `is_current_source` 投影成 SQL NULL 的协议错误，现明确为 `false`。宿主测试随后继续通过遗忘、向量删除故障重试、热替换和重启。冷启动时 30 秒 HostLease 在慢装配中可能过期；宿主现以十秒续期频率使用两分钟 TTL，测试验证本轮完整启动。

图文试验宿主验收现进一步走部署管理 App 的真实 ASGI HTTP 路由：登录会话经 CSRF 与 Web authority ticket，按管理资产声明分块上传文本和 PNG，管理 action 入队后由包自有 Worker 完成召回；状态、结果与归属图片均经 HTTP 管理 query 读取。此路径与真实 PostgreSQL、假 Embedding HTTP 和假 Milvus 的完整 Bot 宿主集成用例通过。当前证明 HTTP 协议及事务授权链路可用；真实浏览器对实际宿主、真实 Embedding/向量供应商请求和长期运行仍待验收。

Memory 包自带管理 UI 现接入已存在的冻结选择和有界批次预览端口：管理员从来源页当前筛选生成不可变成员目录，按序号分页核对根来源及作者锚点，并按起点/大小查看 Payload、来源、向量、关系及联想日志影响。界面明确只作核对和预览；跨批 P0 授权、执行进度及批量确认仍未接入。

QiYu 配置中的 SiliconFlow `Qwen/Qwen3-VL-Embedding-8B` 已用于一次低频真实供应商请求：Forest 的 `EmbeddingClient` 按自身冻结请求格式同时发送一段文本和一张 64×64 PNG 的 data URI，收到 HTTP 200、两条 4096 维向量以及供应商报告的 41 个总 token（含 4 个图片 token）。该结果证明当前端点对 Forest 图文请求格式兼容；它没有经过 Bot 宿主的完整索引与召回链，也不证明远端模型修订身份或长期稳定性。

跨主干学习的隔离 PostgreSQL 验收现把“回复联想日志 → 归档提升”接到后续真实查询快照：后续群消息使旧触发来源不再是当前输入，包内邻接查询从另一群种子读到提升后的 `ASSOCIATED` 边；`graph_region.expand` 沿该边扩散，节点分别保留原群号、主干和作者。这证明包内关系与扩散路径可合流，仍未证明真实 QQ 的跨群成功回复或完整 Agent 证据物化。

真实 Milvus 与隔离 PostgreSQL 的向量索引测试现覆盖外部写入成功而确认丢失：测试专用 collection 中第一笔 upsert 已写入 Milvus，随后故意抛出传输错误；PostgreSQL ref 权威记录仍为空。关闭该 Generation 的驱动并重新创建后，相同 ref/向量重放成功、ref 变为 ACTIVE，后续检索与删除仍通过。该验证覆盖向量索引包的写后失联/重启窗口；Memory 包 Worker 对整批发布的崩溃重放仍需单独验收。

完整 Bot 宿主的隔离 PostgreSQL + 假 Milvus 用例又在文档 facet 首次向量发布时注入“一条 ref 已写入、确认丢失”：Memory 包自有 Worker 将该批工作重试至少一次，所有文档 facet 最终 READY，后续召回、遗忘、热替换和重启仍通过。这证明 Memory 整批发布在运行中可从部分外部成功收敛；实际进程在该窗口立即崩溃并重启的验收尚未完成。

Memory 包的 PostgreSQL 用例还覆盖了另一个崩溃边界：批次发布事务已把全部 facet 置为 READY，但 Worker 尚未写任务结算就失去租约。租约到期后，新 claim 对同一批次重新计算并提交幂等发布回执；旧 claim 的结算被拒绝，新 claim 可结算为 SETTLED。此验证没有实际杀死进程，也不替代外部 Milvus 写入时进程崩溃后的整批重放验收。

冻结遗忘选择的成员页现在按当前权威来源与抑制记录标记“有效 / 本选择已遗忘 / 外部已抑制”，并返回对应遗忘操作 ID；UI 在同一分页表中显示状态。真实 PostgreSQL 用例核对了批次事务提交前后的状态变化和管理员主体隔离。它只展示已发生的成员事实，不提供跨批任务进度、P0 授权或自动继续执行；批次授权语义确定后仍需实现这些能力。

来源浏览现返回当前 DOCUMENT facet 总数、READY 数和四态覆盖情况，并允许服务端按覆盖状态筛选；冻结选择目录可沿用这一条件固定成员。PostgreSQL 用例覆盖无 facet、部分就绪和全部就绪及原来源游标。覆盖状态随 facet 发布实时变化，游标仅固定来源接纳事实，不保证跨页的历史覆盖快照，也不代表 Milvus 单次查询可用。

一次本地合成规模探针在受限 PostgreSQL 容器中插入 5 万条无 facet 来源：普通第一页、无 facet 第一页和无匹配的完整覆盖筛选分别耗时约 0.168、0.154、0.105 秒。它只检查该数据分布和查询成本，不能代替数月真实消息、混合图文 facet 与并发写入的长期压力验收。

长时在线观测脚本已按当前候选基线修正：检查 20 个模块、实际 `embedding_batch_attempts` 与预算预留的 deadline，并移除旧版 OneBot Binding/Client 表查询，因为当前模块不持久化连接状态。用现有在线 Bot 库做一次只读采样时，20 个 bundle 虽在，但缺少候选基线的 `embedding_batch_attempts`；探针明确返回 `BOT_SCHEMA_INCOMPATIBLE`，因此该在线库不能用作当前候选版本的长时验收。在线 Supervisor 和 Bot 未重启或写入，仍需在隔离的当前基线 Bot 库上跑完整探针与真实连接验收。

当前工作树重新构建候选测试镜像后，在独立无持久卷 PostgreSQL 容器中执行完整 Bot 宿主的文本、图片、跨群并发及成功回复四个场景，结果 `4 passed`（288.03 秒）。同一候选镜像与独立 PostgreSQL 再接入现有真实 Milvus，向量 ref 的写后失联重试、查询、删除围栏场景 `1 passed`（4.68 秒）；该测试自行删除临时 collection，测试 PostgreSQL 容器已移除。宿主四场景仍使用假 Embedding、假 Milvus、假 OneBot；Milvus 场景是索引包单独验收，不能合并宣称真实 Embedding＋Milvus 的完整宿主图文召回已通过。

宿主图片场景现在有凭据文件显式启用的真实传输入口：从 QiYu 的只读 `.env` 只提取 VL 嵌入配置，将 OneBot/模型响应保留为假客户端，Memory 使用真实 SiliconFlow `Qwen/Qwen3-VL-Embedding-8B` 和真实 Milvus。隔离 Bot 库运行时核对了 TEXT/IMAGE 查询 facet 均为 READY、其 ref 在索引包 PostgreSQL 权威表为 ACTIVE、两个查询匹配回执已提交。中途一次运行在嵌入前报 `MEMORY_QUERY_INPUT_UNAVAILABLE`；将图片捕获确定性延迟 2 秒后复现：pending input 为 RESOLVED、occurrence 为 ADMITTED，但未写 `eligibility_operations`，查询成员集因而为空。Memory 捕获事务现按与即时接纳相同的身份和摘要补写资格事实，schema 基线升级至 3.51.0。修复后的延迟捕获宿主场景 `1 passed`（77.75 秒）、Memory 包资格事务的五个参数分支 `5 passed`（202.06 秒），真实 VL＋Milvus 图片查询场景 `1 passed`（71.12 秒）；测试后删除临时 collection 与 Bot 库。当前证据只证明图文**查询侧**的真实嵌入与 Milvus 搜索，不证明已有文档图片命中后的跨群证据物化，也不等于长期实网稳定验收。

完整跨群图文链已有隔离验收：假 OneBot 在群 `933179609` 注入一条文字＋有效 PNG，真实 SiliconFlow VL 嵌入和 Milvus 把 TEXT/IMAGE DOCUMENT facet 发布为 READY；另一群 `933179610` 用相同图片触发 Agent，模型请求的证据清单出现原群 `933179609`、原作者 `qq:20001` 和准确来源单元。数据库中的 `query_seed_matches` 还确认 IMAGE 查询 facet 命中该 IMAGE 文档 facet，而非仅由文字相似度带出来源。最初手工调用 Memory 归档事务的用例 `1 passed`（77.43 秒）。随后将同一用例改为通过模块配置计划和热替换把 Conversation 窗口设为 2/1，连续注入四条无触发的群 A 消息；生产 `conversation.silent-drain` 自行提交裁剪和 Memory 归档，首条图文来源达到 ELIGIBLE，两个 DOCUMENT facet 均达到 READY，群 B 的图片查询与来源归因继续通过（`1 passed`，135.14 秒）。测试没有再直接调用归档 participant；临时 PostgreSQL 和 Milvus collection 均已清理。OneBot 与模型回复仍为假客户端，尚未验收真实 QQ 发送或数月运行稳定性。

旧的顶层审批等待、最终回复、会话交接、投递协调器及 Conversation/Memory 事务协调器现已删除；旧 `storage.conversation_scheduler` Repository、其独立 SQL 基线和专测旧基线的用例也已删除。生产 Conversation 仍由 `builtin/conversation_scheduler` 的包声明、私有 schema 和 UnitOfWork 运行，没有增加兼容层。随后 Agent 包状态用例改用当前包的租约 UnitOfWork 和公开 Run 查询端口，旧 `storage.agent_runtime_authority` Repository 也已删除。该用例为单独检验终态 UnitOfWork，经当前 Agent schema 的合法状态边设置 FINAL_READY 前置条件；真正的模型输出到回复提交仍由宿主成功回复用例验证。移除旧 Agent 文件后重建的 Python 3.11 镜像中，无数据库回归为 `452 passed, 85 skipped`，隔离 PostgreSQL 的 Agent 包用例 `1 passed`；临时数据库已移除。
