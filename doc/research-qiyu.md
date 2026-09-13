# QiYu / RAG 源码调研记录

状态：设计输入，依据 2026-09-13 的本地源码快照；不代表 Forest 已实现相应能力。Agentlib 的逐模块实现、优缺点与 Forest 采用矩阵见[专项审计](research-qiyu-agentlib.md)。

阅读范围为 `../../RAG/backend/app`、相关迁移与测试、`../../RAG/libs/agentlib/agentlib` 和对应测试。先阅读了 `../../RAG/AGENTS.md`，以实际实现优先于设计说明。未读取运行配置、凭据、数据库内容或私人对话，也未修改源项目。Agentlib 的 HEAD 为 `471a5c2`；下述行号指阅读时工作区文件，未把 HEAD 当作工作区无改动的证明。使用 Forest 现有 Python 环境直接加载 Agentlib 源码，关闭 pytest cache/bytecode 写入后运行不依赖缺失可选包的 7 个核心测试文件，结果为 `68 passed`；完整收集因当前环境没有 Agentlib 的 `openai`、`websockets` 可选依赖而未执行。

**结论：QiYu 适合作为 Agent 执行、记忆证据链、持久任务和应用鉴权的参考；其当前自动记忆按会话隔离，不能直接用作 Forest 的 Bot 全局记忆模型。** 热重载模块管理、Bot 身份和跨进程执行恢复也需要单独设计。

## 1. 已实现的 Agent 执行机制

| 机制 | 实现证据 | 对 Forest 的意义 |
| --- | --- | --- |
| 每会话串行、不同会话可并行 | [runtime_registry.py](../../RAG/backend/app/agent/runtime_registry.py) 第 59–63、99–142、659–673 行：`_ConversationActor` 持有一个锁和等待计数，注册表按 `conversation.id` 建立 Actor；默认最多 8 个等待请求 | 单写者范围应是 `(bot_id, conversation_id)`。不能因为一个 Bot 共享记忆就把它所有群聊放进同一个执行锁 |
| 有界事件流 | 同文件第 173–175 行使用 `asyncio.Queue(maxsize=32)`；第 998–1008 行用 `TaskGroup` 驱动 Agent 并消费队列 | 流式输出必须有容量、等待期限和取消策略；不要改成无限订阅列表或无限队列 |
| 执行拥有资源 | [factory.py](../../RAG/backend/app/agent/factory.py) 第 78–97 行：`ConversationRuntime` 拥有 Agent、Provider、摘要 Provider、ContextStore、证据收集器、确认 Broker；关闭过程逐级 `finally` 释放 | Forest 的 `RunScope` 应登记资源和子任务，模块卸载通过 Scope 收敛，而不是要求插件自行回忆所有资源 |
| 私有分支与原子提交 | [agent.py](../../RAG/libs/agentlib/agentlib/runtime/agent.py) 第 143–168、178–182、356–473 行：模型轮次、工具响应、RAG 和追加消息先进入私有分支，终止回答出现后才提交；不是每个 token 修改共享 Context | 聊天历史提交点、事件流和外部发送应分别定义，已流出的 token 不等同于已提交的对话事实 |
| 安全边界接收追加消息 | 同文件第 298–325 行：`enqueue_pending` 不打断当前 Provider 或 Tool，FIFO 消息在下一次模型调用前生效；每次执行默认最多接受 8 条 | 可用于同一对话的跟进消息；其他群聊消息不能作为此分支的 pending message 注入 |
| 有界 Agent 循环 | 同文件第 202–205、398–415、474–478 行：默认工具迭代 8 次、非终止思考 4 次、协议修复 1 次；单模型步骤最多接收一个原生工具调用 | Forest 采用每个 ModelTurn 至多一个工具调用。Kernel 服务内部可以有受生命周期监督的组件 RPC 调用树，但它不是另一个 Agent，也不改变群聊响应链 |
| 失败分支恢复 | 同文件第 334–354 行保存 `_RunState` 与 `ExceptionContext`；[runtime_registry.py](../../RAG/backend/app/agent/runtime_registry.py) 第 66–75、306–332 行保存 `_SuspendedRuntime` | 恢复保留失败阶段和待提交分支；现有恢复依赖 Python 对象存活，不是可迁移的持久 checkpoint |
| 关闭传播取消 | [runtime_registry.py](../../RAG/backend/app/agent/runtime_registry.py) 第 345–375、641–657 行清理 pending、记录取消、取消并等待活动任务、关闭挂起资源 | 可借鉴顺序；Forest 还需明确关闭的最大耗时、不可取消任务如何终止、旧模块写入如何被 fence 拒绝 |

底层 `agentlib` 将基础设施作为 Provider、ContextStore、MemoryRetriever、ToolPolicy 等接口注入。应用层负责把会话、授权、数据库和工具连接起来。这个边界适合保留：Forest 微内核处理生命周期和运行时边界，Agent 的模型协议、思考循环与检索策略由可替换模块提供。

## 2. 记忆实际作用域与信息流

### 2.1 当前的两个作用域不能混为一谈

[domain/memories.py](../../RAG/backend/app/domain/memories.py) 第 10–19 行的 `MemoryScope` 包含 `tenant_id`、`user_id`、`assistant_id`、`epoch` 和关闭时间。它表达某用户与某助手的一代长期记忆。显式记忆检索在 [application/memories.py](../../RAG/backend/app/application/memories.py) 第 269–303 行以 `memory_scope_id` 查询候选，再由数据库重新物化和授权。

自动归档检索则进一步按会话隔离：

1. [memory_bridge.py](../../RAG/backend/app/agent/memory_bridge.py) 第 56–108 行把当前 `conversation.id` 传给归档检索，方法说明明确为 `conversation-local`。
2. [application/memories.py](../../RAG/backend/app/application/memories.py) 第 326–384 行使用 `conversation_memory_vector_scope_id(memory_scope_id, conversation_id)` 查向量，并将会话 ID 传给归档物化与图快照。
3. 同文件第 485–493 行通过 SHA-256 派生专属会话向量 scope。
4. [memory_jobs.py](../../RAG/backend/app/infrastructure/postgres/memory_jobs.py) 第 88–115 行在自动归档建索引时使用同一会话 scope；不是只在查询侧临时过滤。
5. [test_memory_similarity_threshold.py](../../RAG/backend/tests/test_memory_similarity_threshold.py) 第 433–469 行明确断言同一个记忆 scope 下，不同会话使用不同向量 scope；[test_memory_worker_scope.py](../../RAG/backend/tests/test_memory_worker_scope.py) 第 140 行对应建索引测试。

因此，不能把 `assistant_id` 改名为 `bot_id` 就认为满足本次需求。Forest 应定义一个Bot独立的持久记忆域；向量、归档、语义记忆与图关系均以bot_id隔离。群聊在该全局图中各有独立主干；conversation_id保留具体上下文/线程来源，group_source_id标识稳定群归属，二者都不能派生互相隔绝的长期记忆库。是否允许某条来源用于当前群，是域内读取策略；独立图主干不等于独立群数据库。

### 2.2 已实现且值得保留的记忆分层

| 层次 | 当前行为与证据 |
| --- | --- |
| 运行时预设 | [agent.py](../../RAG/libs/agentlib/agentlib/runtime/agent.py) 第 172–176、250–253 行：PresetContext 为固定前缀，不进入历史和记忆流水线 |
| 活动 Context | 同文件第 143–150、245–248 行：已提交上下文为 append-only；分支提交前不可见 |
| 归档事实 | [memory_bridge.py](../../RAG/backend/app/agent/memory_bridge.py) 第 31–53 行：归档被 Context 视图省略的完整单元；历史关闭时跳过；排除 `RagMessage` 和 `ArchiveSummary` |
| 召回证据 | 同文件第 94–156 行：构造包含节点、关系、种子分数、图分数、预算与截断标记的 `RagMessage`，而不是一段无法追溯来源的拼接文本 |
| 向量派生索引 | [application/memories.py](../../RAG/backend/app/application/memories.py) 第 349–384 行：向量先产生候选 ID/相似度，数据库再恢复授权后的事实；向量库不是授权真相源 |
| 图扩展 | [graph.py](../../RAG/libs/agentlib/agentlib/memory/graph.py) 第 58–108 行：最强路径的权重乘积传播；稳定排序，同分时依赖持久顺序与 ID；应用层第 385–420 行应用边权、节点上限和完整单元字符预算 |
| 联想学习 | [memory_bridge.py](../../RAG/backend/app/agent/memory_bridge.py) 第 159–165 行的 `register_associations` 在应用实现中为空操作，因为 [postgres/conversations.py](../../RAG/backend/app/infrastructure/postgres/conversations.py) 第 1192–1198、1476–1532 行已在 turn 事务中写入联想 journal；不是未实现占位 |
| 明确遗忘与派生清理 | [memory_jobs.py](../../RAG/backend/app/infrastructure/postgres/memory_jobs.py) 第 123–190 行：向量写入完成时重新检查 scope 关闭、显式遗忘、来源失效；必要时转为 `delete_pending`，避免旧任务重新把已撤回事实变成可用索引 |

Forest 需要在这套分层上增加 Bot 级提交版本、跨会话来源归属和竞争更新规则。两个群聊可以同时读同一 Bot 的已提交记忆快照、同时调用模型，再以短事务提交各自事实；不应在整个模型或工具调用期间持有 Bot 记忆写锁。对于互相冲突的语义记忆，保留来源和修订关系，不能以向量 upsert 的最后写入覆盖事实。

## 3. 持久化、鉴权和工具执行

### 3.1 提交时重新检查真实状态

[context_store.py](../../RAG/backend/app/agent/context_store.py) 第 56–76 行先做分支 ID 校验，再调用数据库提交，数据库成功后才更新内存副本。

[postgres/conversations.py](../../RAG/backend/app/infrastructure/postgres/conversations.py) 第 986–1082 行在一个短事务中锁定执行、会话和用户偏好，检查执行仍为 `running`、会话仍可写、运行快照的助手归属正确、当前助手和知识授权仍有效。只有开始时允许保存、提交时仍允许保存且 `history_mode_generation` 未变化才保存历史。第 1192–1205 行在该事务中同时写联想 journal 和语音输出工作。

可继承的不变量是“配置快照用于一致执行，撤权和生命周期 fence 必须在副作用及最终提交时重新检查”。[runtime_auth.py](../../RAG/backend/app/application/runtime_auth.py) 第 13–42 行显式区分普通禁用与 ACL 撤销：普通禁用作用于下一轮，撤销当前权限可阻止这一轮后续外部调用。[authorization.py](../../RAG/backend/app/agent/authorization.py) 第 45–52、74–76 行把该检查放在 Provider 和 Tool 边界。

配置在执行期间保持快照，行为由 [test_agent_runtime_configuration.py](../../RAG/backend/tests/test_agent_runtime_configuration.py) 第 182–238 行覆盖；数据库存储快照内容哈希和版本信息，见 [postgres/conversations.py](../../RAG/backend/app/infrastructure/postgres/conversations.py) 第 1383–1450 行。Forest 可以进一步把模块代次、契约版本和策略版本放入 RunPlan，保持运行过程可解释。

### 3.2 工具调用已有记账，尚不是通用恰好一次执行

[tools/executor.py](../../RAG/libs/agentlib/agentlib/tools/executor.py) 第 85–118 行先校验参数，再在有限超时内授权和执行，把预期工具错误转换为关联的 `FunctionResponse`。默认 `AllowAllToolPolicy` 适合库的最小集成，不适合直接作为 Forest 的生产默认策略。

[tool_middleware.py](../../RAG/backend/app/agent/tool_middleware.py) 第 36–84 行用 `(execution_id, call_id, arguments_hash)` 预留调用，同参数的终态结果可重放。需确认的工具要求参数哈希匹配确认回执，并由可信应用侧注入幂等键；结果记录限制为 32 KiB。模型不能自行提供可信确认或可信幂等身份。

需要补齐的窗口见 [postgres/tool_executions.py](../../RAG/backend/app/infrastructure/postgres/tool_executions.py) 第 66–87 行：已终态调用可重放，但仍是 `running` 的记录会再次允许执行。外部服务已产生副作用、成功回执尚未落库时崩溃，通用工具无法据此判断有没有执行。Forest 必须按工具声明区分纯读、幂等写与不可幂等副作用；后一类在不确定时进入 `UNKNOWN` 并对账，不能自动重试或在热重载时重新调用。确认回执也不能替代外部服务本身的幂等保证。

### 3.3 已实现的后台工作机制

[workers/host.py](../../RAG/backend/app/workers/host.py) 第 15–90 行使用 PostgreSQL `LISTEN/NOTIFY` 唤醒 Worker，处理完可用任务后等通知或有界扫描期限。通知是唤醒信号，持久任务行是事实来源。

[0008_worker_claims.sql](../../RAG/backend/migrations/0008_worker_claims.sql) 第 43–90、92–127 行实现记忆索引和 Outbox 的 `FOR UPDATE SKIP LOCKED` 领取，设置 `lease_owner` 与过期时间。[workers/memory.py](../../RAG/backend/app/workers/memory.py) 第 119–156 行每次处理一项，执行后确认，失败指数退避，取消继续传播；[postgres/outbox.py](../../RAG/backend/app/infrastructure/postgres/outbox.py) 第 106–177 行包含发布、重试和死信转换。

这些代码适合借鉴可靠任务分发，但租约拥有者字符串不能直接充当 Forest 热重载的代次 fence。Forest 需为每次领取增加单调递增的租约代次或等价 token，领取、续租、结果提交都校验；对于索引这种数据库外写入，还需版本化记录、提交后再检查或幂等删除以防旧 Worker 写回。

## 4. 不能直接继承的边界和风险

以下是对本次已阅读路径的设计评估，不宣称完成整个 QiYu 的质量审计。

| 观察 | 证据与限制 | Forest 要求 |
| --- | --- | --- |
| 挂起恢复只在当前进程有效 | `runtime_registry.py` 第 66–75、119、323–332 行保留 runtime 对象；数据库执行记录不等于 `_RunState` 序列化 | 显式区分进程内 resume 与持久 checkpoint；热重载跨版本只迁移有 schema 的状态 |
| 本地 Actor 不是分布式所有权 | `runtime_registry.py` 第 116、142 行为内存字典与 `asyncio.Lock` | 多进程运行时以 `(bot_id, conversation_id)` 建立带 fencing token 的 owner lease；数据库唯一约束和提交检查作为最终防线 |
| 自动记忆按会话分区 | `memories.py` 第 349–368、485–493 行及对应隔离测试 | Bot全局持久域内，每群有独立工作上下文和图主干；跨主干联想与来源明确的召回共同设计 |
| 返回节点上限不等于计算上限 | `graph.py` 第 90–107 行先传播到所有达到阈值的节点，再截断为 `max_nodes`；应用先装载图 snapshot | Bot 级大图需要邻接读取预算、最大访问边/节点数、总 deadline，避免全图加载；截断需要在证据里标记 |
| 总执行 deadline 的含义尚需加强 | `runtime_registry.py` 第 144 行在出队时检查 deadline；`tools/executor.py` 第 93 行有单工具超时；[0012_execution_deadline_reconciliation.sql](../../RAG/backend/migrations/0012_execution_deadline_reconciliation.sql) 第 3–22 行把过期执行收敛为失败。已阅读的执行路径未见覆盖整轮的 `timeout_at` | Forest 把绝对 deadline 贯穿排队、模型、工具、checkpoint 和发送，局部超时不得重置整体预算 |
| 资源容量分散 | `runtime_registry.py` 第 108、740 行的 `maximum_runtime_entries` 限制无历史上下文缓存；不能据此推断 `_suspended`、跨会话活动任务也受此上限控制 | 每 Bot、每模块、每对话分别设置活动数、排队数、挂起数、字节数和 TTL；统一配额服务执行 |
| 工具结果与聊天提交是不同事务 | `tool_middleware.py` 第 67–76 行先外部调用后记录结果，随后 Agent 才提交聊天分支 | 文档明确补偿、UNKNOWN、幂等与对账，禁止承诺外部副作用随 Context 原子回滚 |
| 源代码运行对象没有模块代次 | Agent/runtime/factory 管理 Python 资源与实例，不提供候选模块准备、路由代次切换、旧代次排空等状态机 | 热重载控制面、通信 ABI、能力令牌、资源注册和旧代次回收由 Forest 独立实现 |

## 5. 建议进入 Forest 验收的场景

1. 同一个 Bot 同时在群 A、群 B 执行 Agent：两条流独立推进；共享同一个 Bot 记忆域；群 A 工具等待不阻止群 B 完成。
2. Bot A 写入记忆后，Bot B 使用相同外部用户 ID、群 ID 和向量候选 ID 也不能读取；隔离由可信访问上下文和存储约束执行。
3. 同一 Bot 的群 A 事实在提交后可按可见性策略被群 B 召回；正在生成但尚未提交的分支不能被另一会话检索。
4. 检索出的证据再次进入历史并归档时，不重复向量化 `RagMessage`，不把模型摘要伪装成原始来源。
5. 模型调用期间撤销权限、关闭记忆域或更新模块代次，旧执行的最终写入被拒绝；模型参数的普通修改保持到下一轮生效。
6. 外部工具成功后、结果落库前模拟进程崩溃：幂等工具以同一键对账或重放；不可幂等工具进入 `UNKNOWN`。
7. Worker 租约过期被新代次接管后，旧 Worker 继续返回也不能确认新任务或覆盖新版本索引。
8. 模块排空期间，排队、活动、挂起和流式消费者均有可验证的结束路径；没有永远等待队列空间的清理任务。
9. Bot 全局记忆图超过单轮预算时，仍返回有界、来源完整、注明截断的证据，不先读取和遍历完整图再截断展示。

这些是 Forest 的目标验收条件。本次只进行源码阅读和文档整理，没有将已有测试名称或测试源码误记为已运行通过。

## 6. 群聊全量上下文需求的补充核对

本节补充静态源码证据，用于解释[08 群聊交互协议](08-group-context-interaction.md)为何单独设计；不修改 QiYu 的既有需求。

| 当前实现 | 证据 | Forest 的差异 |
|---|---|---|
| `enqueue_pending` 在活跃 Run 中接收 steering 消息，默认每 Run 最多 8 条，没有普通/触发记录的独立切口 | [runtime/agent.py](../../RAG/libs/agentlib/agentlib/runtime/agent.py) 第 205、298–325 行 | 群的所有正常消息先持久保存，再分类触发；不能把普通群消息全部当作延长响应的 steering |
| `_apply_pending_messages` 在模型安全边界按 FIFO 排空待接入消息；最终 Assistant 到达后仍可因 pending 继续该 Run | [runtime/agent.py](../../RAG/libs/agentlib/agentlib/runtime/agent.py) 第 378、462–535 行 | Forest 在每个安全点只取截至最后待触发记录的前缀，普通尾部等后续触发或结束后的静默排空 |
| `_commit` 到最终分支才 `push_many`；模型视图首次构建后再以 extend 追加 | [runtime/agent.py](../../RAG/libs/agentlib/agentlib/runtime/agent.py) 第 741–755、836–873 行；[context/view.py](../../RAG/libs/agentlib/agentlib/context/view.py) 第 144 行起 | 群输入批次独立提交；完整模型/工具组按因果顺序flush；每次输入及静默维护都检查裁剪，而不只在首次创建视图时检查 |

因此可以参考 QiYu 的安全点和私有执行步骤，不直接沿用其 pending 队列作为 Forest 的全量群聊邮箱。Forest 的历史作者是逐条可信身份快照，触发归属与权限也逐条保留，不能把一批成员发言并成一个 UserMessage 主体。

## 7. 多主干与模型可见来源的补充核对

准确区分两层：agentlib默认内存图实例只有一条主干；QiYu的PostgreSQL实现已经按conversation接续序列边，但自动召回只加载当前conversation主干，跨会话联想也受到显式限制。不能据此笼统说整个QiYu数据库只有一条全局链。

| 当前实现 | 静态源码证据 | Forest的差异 |
|---|---|---|
| 内存图只有一个_order，append连接最后元素 | [integrations/in_memory_graph.py](../../RAG/libs/agentlib/agentlib/integrations/in_memory_graph.py) 48–51、63–78行；[memory/graph.py](../../RAG/libs/agentlib/agentlib/memory/graph.py) 122–127行接口无trunk参数 | 每群独立根与稳定来源位置，不能共享一个全Bot尾指针 |
| PostgreSQL归档先锁scope计数器，批内按Context位置，找前驱时限定conversation | [postgres/memories.py](../../RAG/backend/app/infrastructure/postgres/memories.py) 669–727、779–799行 | 来源位置在权威源提交时固定，归档Worker不按完成顺序决定链；迟到可重建中间邻接 |
| 图快照明确只装一条conversation主干，边两端都在该节点集内 | 同文件884–991行，特别918–938、953–967行 | Bot全局多主干查询，顺序扩展与跨主干联想分开计额 |
| 联想journal和promotion限制两端同conversation | [postgres/conversations.py](../../RAG/backend/app/infrastructure/postgres/conversations.py) 1476–1532行，特别1522行；[postgres/memories.py](../../RAG/backend/app/infrastructure/postgres/memories.py) 1082–1148行 | 允许同Bot跨主干关系，同时重验端点、来源、删除屏障与当前群输出权限 |
| RagNode只含ref/source，召回组装未强制群来源 | [core/units.py](../../RAG/libs/agentlib/agentlib/core/units.py) 135–145行；[memory_bridge.py](../../RAG/backend/app/agent/memory_bridge.py) 122–156行 | Evidence每项及多源claim保存来源群、作者、时间和源单元引用 |
| 模型序列化剔除metadata，图节点显式输出仅ref/archive_id/block_type | [context/serializer.py](../../RAG/libs/agentlib/agentlib/context/serializer.py) 47–58、154–164行；[provider/compatible.py](../../RAG/libs/agentlib/agentlib/provider/compatible.py) 190–191行实际使用该序列化 | 来源必须进入真实Provider输入文本，不只加metadata字段；当前使用群与原始来源群分开 |

这些观察用于[10 多主干记忆图设计](10-multi-trunk-memory-graph.md)，不改变对QiYu原需求的评价。Forest另按已确认的成本要求移除裁剪时的记忆摘要，使用[08的确定性归档/裁剪](08-group-context-interaction.md)；不能直接继承源项目摘要路径作为流式消息的必经阶段。
