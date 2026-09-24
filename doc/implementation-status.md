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
| Provider | `provider_chat_completions` | Worker 拥有 profile；Generation Effect Driver 拥有 HTTP/Secret |
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
| Memory | `bootstrap_clean_memory()` | 尚无完整 builtin 包 | 图写入/召回/归档 Worker、工具和管理贡献、生产切换 |

`clean_*` 是重构期间遗留的宿主实现名称，不是新的模块层。对应 builtin 完成生产切换后，应直接删除这些文件和旧 baseline，不保留兼容转发。

## 5. Agent 候选包已经拥有的部分

`builtin/agent_runtime` 已拥有唯一 CorePrompt 与 XML 协议解析器，并以私有 SQL 和 manifest UoW 封闭以下状态转换：

- Session/Run 创建、输入追加、owner lease、释放和热重载停车/恢复；
- Provider Generation 与模型投影先冻结；授权、Kernel Effect permit、wire Payload 和 Agent Attempt 由 `agent.prepare-model-effect/1` 原子建立；
- `agent.handoff-model-effect/1` 原子推进 Effect 与 Agent handoff，`agent.observe-model-effect/1` 原子收敛两本账及 `UNKNOWN` 对账；
- 错误 XML 的一次协议修复、合法 ModelTurn 提交；
- ToolCall 的准确 component/Generation/service/contract/binding 冻结；
- 工具审批、handoff、终态观察和可查询 `UNKNOWN` 对账；
- 最终完成、同 Session 后继 Run 和 Session 关闭的私有 participant。

这些端口证明 Agent 状态所有权可以进入包，但当前 Worker 只提供 `agent.protocol.describe/1`、`agent.run.query/1` 和 reload-resume job。它还没有提供生产需要的 `agent.execute/2`，所以 `clean_bot_host.py` 仍使用旧宿主执行器。不得把“包内状态端口完成”表述成“Agent 已完成生产包化”。

## 6. Conversation 候选包已经拥有的部分

`builtin/conversation_scheduler` 已封闭消息接纳、连续分类水位、截至最后 trigger 的输入选择、带作者来源的 Context、无 LLM 摘要的窗口裁剪、静默排空和 `conversation.context.snapshot/1`。同群串行和跨群并行依靠数据库 actor lease，而不是进程锁。

该候选尚未拥有完整 Agent 调用与回复/Session 交接，所以尚未加入生产组合根。现有 `bootstrap_clean_messaging()` 仍是旁路，应在对应端口完成后删除。

## 7. 下一切换顺序

1. 在 `agent_runtime` Worker 中实现可序列化的 `agent.execute/2`，只使用声明式 Service、Effect、Payload、UnitOfWork 和工具贡献目录；禁止 callback、Repository、数据库连接和宿主 Agent adapter。
2. 在 `conversation_scheduler` manifest 中补齐 acquire-model-input、final-reply 和 session-handoff plans，使 Agent 与 Conversation 只交换 DTO、PayloadRef 和 owner fence。
3. 用 `bootstrap_builtin_package()` 替换 `bootstrap_clean_agent()` 与 `bootstrap_clean_messaging()`，随后删除宿主 Agent/Conversation Runtime、Repository 装配和旧 baseline。
4. 将 Memory 权威状态与召回执行迁入独立 builtin 包，再删除 `bootstrap_clean_memory()` 路径。
5. 从空开发库启动完整图，验证热重载安全点、失败回滚、群并发、Provider/Tool 不确定结果、真实 OneBot 单次低频交互和 WebUI 管理贡献。

在第 3 步完成前，生产图仍是“微内核包系统 + 三个宿主业务模块”的混合图；在第 4 步完成前，目标重型 Bot 框架仍未完成。
