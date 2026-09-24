# Forest 实现状态

> 本页只记录当前工作树事实，不定义目标架构。架构约束以 [00 架构总契约](00-architecture-contract.md)、[13 微内核控制面重构](13-microkernel-control-plane-redesign.md)和[15 单 Agent 生产执行](15-single-agent-production-execution.md)为准。当前生产装配仍是混合图，Forest 尚不可发布。

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
- Generation 专属 Effect Driver，Secret、socket、子进程和远端客户端不进入 Worker；
- 包自带管理 UI 与配置贡献；
- Job、Shell 和 Routing 三类冻结贡献目录。

这些能力构成 builtin 模块系统本身。业务组件只有经过这些端口运行，才算使用了该系统。

## 3. 当前生产装配

`CleanBotHost._assemble_registry()` 当前直接以真实 builtin 包装入下列业务组件：

| 组件 | builtin 包 | 当前边界 |
|---|---|---|
| Provider | `provider_chat_completions` | Worker 拥有 profile；同代 materialize/dispatch driver 通过 PayloadRef 生成模型投影、读取媒体并执行 HTTP；响应正文和 usage 由 Provider Generation 保存，dispatch/Effect/Agent observation 只传精确引用；Secret 不进入 Worker |
| 多模态 Embedding | `embedding_multimodal` | Worker 拥有模型投影；VL profile 随 Bot 生命周期冻结 |
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

内核的 Runtime、Payload、Authorization、Effect 与 Audit Authority 仍是可信微内核组件。它们不是可卸载业务插件，也不应迁入 `builtin`。

## 4. 尚未切换的生产组件

当前生产图仍通过宿主组合根构造以下组件，因此它们尚未完成包化：

| 组件 | 当前生产入口 | 已有候选 | 缺口 |
|---|---|---|---|
| Agent | `bootstrap_clean_agent()` | `builtin/agent_runtime` | Worker 内 `agent.execute/2`、Provider/Tool/Payload 端口、管理贡献、Conversation 原子交接、生产切换 |
| Conversation | `bootstrap_clean_messaging()` | `builtin/conversation_scheduler` | Agent 调度入口、final-reply/session-handoff plans、生产 poller/owned work 切换 |
| Memory | `bootstrap_clean_memory()` | `builtin/memory_graph` | 捕获/嵌入/召回 Worker 与端口、完整管理查询、生产切换 |

`clean_*` 是重构期间遗留的宿主实现名称，不是新的模块层。对应 builtin 完成生产切换后，应直接删除这些文件和旧 baseline，不保留兼容转发。

## 5. Agent 候选包已经拥有的部分

`builtin/agent_runtime` 已拥有唯一 CorePrompt 与 XML 协议解析器，并以私有 SQL 和 manifest UoW 封闭以下状态转换：

- Session/Run 创建、输入追加、owner lease、释放和热重载停车/恢复；
- `agent.acquire-model-input/1` 由 manifest 固定 Conversation、Kernel Payload、Memory 与 Agent 四个 participant，在同一 PostgreSQL 事务内完成模型安全点消息切口、删除 epoch 冻结、Memory 资格确认和 Agent Input revision 推进；任一步拒绝都会整体回滚；
- `agent.start-from-conversation/1` 使用相同四方 participant，在初始 `IDLE` 边界把 trigger 选择、删除 epoch、Memory 资格确认与首个 Agent Run 建立合并为一个事务；Conversation 不再需要先提交 InputBatch 再用 callback 启动宿主 Agent，选择与建 Run 之间不存在可见裂缝；
- Provider Generation 与模型投影先冻结；授权、Kernel Effect permit、wire Payload 和 Agent Attempt 由 `agent.prepare-model-effect/1` 原子建立；其中 `kernel.effect` 的 `effect.prepare-command` participant 在协调事务内校验授权票据、父 Payload 与 Generation fence，并以自身身份写入 `effect.input`/`effect.wire`，Agent Worker 只提交声明式 command，不能伪造内核 Payload owner；该 UoW 的最终结果保留完整 authorization ticket、input/wire PayloadRef 和 permit，后续 handoff 不从宿主取旁路凭据，也不重新猜测 Effect 事实；
- `agent.handoff-model-effect/1` 原子推进 Effect 与 Agent handoff；`agent.observe-model-effect/1` 通过 `effect.observe-command-result` 让内核校验 Provider 结果引用并以自身身份冻结 `effect.evidence`，Agent participant 同时核对 evidence 摘要、父引用集合与观察状态，随后原子收敛两本账及 `UNKNOWN` 对账；
- 错误 XML 的一次协议修复、合法 ModelTurn 提交；
- ToolCall 的准确 component/Generation/service/contract/binding 冻结；
- 工具审批、handoff、终态观察和可查询 `UNKNOWN` 对账；
- terminal ModelTurn 先停在 `SAFE_BOUNDARY`；确认没有新触发批次后，Worker 以最新 terminal turn 的准确 `assistant.candidate` 为父边生成独立 `final.reply`，`agent.finalize-candidate/1` 同时核对来源与正式候选并原子推进到 `FINAL_READY`；内部解析候选不会直接交给 Delivery；
- 最终完成、同 Session 后继 Run 和 Session 关闭的私有 participant。

包内已加入 `model_execution.py`，模型 attempt 的 profile bind、`model.input` 写入、Provider materialize、授权与 Effect prepare、原子 handoff、Provider dispatch、Effect/Agent 原子 observe 及 Provider 结果读取均只经过 `ctx.services`、`ctx.uow`、`ctx.payloads` 与冻结的 `ctx.identity`。它没有 Repository、数据库连接或宿主 Agent adapter；Provider 异常或回执 fence 不一致在 handoff 后统一收敛为 `UNKNOWN`，不会盲目重发。

这段模型 attempt 尚未接入公开 `agent.execute/2` 循环。当前 Worker 对外仍只提供 `agent.protocol.describe/1`、`agent.run.query/1` 和 reload-resume job，工具 Effect 链、协议修复续调、最终回复计划与 Conversation owned-work 也尚未闭合，所以 `clean_bot_host.py` 仍使用旧宿主执行器。不得把“模型 attempt 已进入包”表述成“Agent 已完成生产包化”。

包运行时现已具备能力受控的分块 Payload 端口：`payload.read` 只接受完整 owner/digest 引用并在 Worker 端复核最终摘要，`payload.write` 强制当前组件所有权、连续 offset、声明长度/摘要与精确父引用；单代暂存最多 32 MiB，并随调用和 Generation 回收。该端口解决了长期 Context、模型输入和图片不能穿过 256 KiB 控制帧的问题，是把 Agent、Memory、Conversation 执行逻辑移入 Worker 的基础设施。它本身不代表三个模块已完成切换。

Provider 的 `provider.model.dispatch/2` 已移除内联模型正文：效果驱动在所属 Generation 下原子写入 `model.result`/`model.usage`，服务回执与 Kernel Effect evidence 只持有精确引用；旧宿主 Agent 过渡执行器和新的包内 model attempt 都只能把这些引用交给 Agent observation，不能再以 Provider 身份复制载荷。Effect/Agent 私有 schema 已用独立 PostgreSQL 重建验证；完整 Agent 执行循环仍待接入 Worker 公共服务。

## 6. Conversation 候选包已经拥有的部分

`builtin/conversation_scheduler` 已封闭消息接纳、连续分类水位、截至最后 trigger 的输入选择、带作者来源的 Context、无 LLM 摘要的窗口裁剪、静默排空和 `conversation.context.snapshot/2`。快照不再把完整历史塞进 Worker 帧：权威查询最多返回 32 个活动单元，Worker 写入带正文/作者父边的 `context.snapshot.page`，超过 128 页时构造 `context.snapshot.index` 树，服务只返回最终 manifest 的精确 PayloadRef。同群串行和跨群并行依靠数据库 actor lease，而不是进程锁。

该候选尚未拥有完整 Agent 调用与回复/Session 交接，所以尚未加入生产组合根。现有 `bootstrap_clean_messaging()` 仍是旁路，应在对应端口完成后删除。

## 7. Memory 候选包已经拥有的部分

`builtin/memory_graph` 已经是可由 `PackageCatalog` 构建的独立包，声明自己的隔离 Worker、私有 storage baseline、Generation 配置、管理 UI 和 `memory.qualify-input-batch/1` UnitOfWork participant。嵌入配置将 `TEXT_IMAGE` 模型种类、部署标识、模型修订、维数与归一化方式绑定到 Bot 模块生命周期；端点轮换仍应由后续受限 Effect 配置承担，不能在已创建 Bot 上改换模型种类。

首个私有状态边界已经封闭 Agent 输入资格确认：Conversation 给出的准确消息切口与 Kernel Payload 删除 epoch 在同一事务进入 Memory；尚未捕获的 event 只登记 `PENDING_CAPTURE`，已有来源 occurrence 从 `REGISTERED` 原子进入 `ADMITTED`，operation 重放不重复产生资格记录。该路径已用真实 PostgreSQL 安装包 baseline 并验证状态转换与 Registry 资源回收。

这仍是候选而非生产切换。消息来源捕获、图片/文本 facet、嵌入预算和异步合流、独立向量索引 ref、QiYu 图区域扩散、超大关联点随机有界采样、跨主干联想、召回 Payload、遗忘，以及带筛选/关系遍历/真实召回试验的管理契约仍需迁入该包。当前 `bootstrap_clean_memory()` 继续构成宿主旁路，完成前不得把 `memory_graph` 目录存在表述为 Memory 已包化。

## 8. 下一切换顺序

1. 在 `agent_runtime` Worker 中实现可序列化的 `agent.execute/2`，只使用声明式 Service、Effect、Payload、UnitOfWork 和工具贡献目录；禁止 callback、Repository、数据库连接和宿主 Agent adapter。
2. 在 `conversation_scheduler` manifest 中补齐 acquire-model-input、final-reply 和 session-handoff plans，使 Agent 与 Conversation 只交换 DTO、PayloadRef 和 owner fence。
3. 用 `bootstrap_builtin_package()` 替换 `bootstrap_clean_agent()` 与 `bootstrap_clean_messaging()`，随后删除宿主 Agent/Conversation Runtime、Repository 装配和旧 baseline。
4. 补齐 `memory_graph` 的捕获、嵌入、图扩散、召回和管理端口，用包内权威状态替换 `bootstrap_clean_memory()`，随后删除宿主 Memory Runtime、Repository 装配和旧 baseline。
5. 从空开发库启动完整图，验证热重载安全点、失败回滚、群并发、Provider/Tool 不确定结果、真实 OneBot 单次低频交互和 WebUI 管理贡献。

在第 3 步完成前，生产图仍是“微内核包系统 + 三个宿主业务模块”的混合图；在第 4 步完成前，目标重型 Bot 框架仍未完成。
