# QiYu Agentlib 实现审计与 Forest 采用决策

## 1. 审计范围与结论

本次审计以 `RAG/libs/agentlib` 的实际源码和测试为准，而不是按概念文档猜测。审计基线为 Agentlib 子仓库提交 `471a5c248de6f5842ea82917d7f72a580dd25b95`。重点阅读了以下实现：

- [`runtime/agent.py`](../../RAG/libs/agentlib/agentlib/runtime/agent.py)：单 Agent 的模型、工具、RAG、协议修复、pending message 与恢复状态机；
- [`core/prompt.py`](../../RAG/libs/agentlib/agentlib/core/prompt.py)、[`core/units.py`](../../RAG/libs/agentlib/agentlib/core/units.py)：不可覆盖的 CorePrompt、原子 ModelTurn 和强类型 ContextUnit；
- [`context`](../../RAG/libs/agentlib/agentlib/context)：PresetContext、规范序列化、协议解析、append-only store 与模型视图裁剪；
- [`provider`](../../RAG/libs/agentlib/agentlib/provider)：Provider 无关端口、Chat Completions 映射、能力声明和流事件；
- [`tools`](../../RAG/libs/agentlib/agentlib/tools)：工具注册、JSON Schema 校验、授权、超时和结果归一化；
- [`memory`](../../RAG/libs/agentlib/agentlib/memory)、[`multimodal`](../../RAG/libs/agentlib/agentlib/multimodal)：archive-only RAG、向量种子、图扩散、多模态嵌入和能力投影；
- QiYu 的 [`factory.py`](../../RAG/backend/app/agent/factory.py)、[`runtime_registry.py`](../../RAG/backend/app/agent/runtime_registry.py)、[`context_store.py`](../../RAG/backend/app/agent/context_store.py)、[`memory_bridge.py`](../../RAG/backend/app/agent/memory_bridge.py) 和 [`tool_middleware.py`](../../RAG/backend/app/agent/tool_middleware.py)：应用如何补足持久化、授权和工具幂等。

在不安装缺失的可选 OpenAI/Web 依赖的条件下，使用 Agentlib 源码直接运行核心、上下文、归档、工具、摘要和多模态记忆测试，结果为 `68 passed`。完整测试收集还需要 Agentlib 的 `openai` 和 `websockets` 可选依赖；本次没有联网安装依赖，也没有运行真实模型集成测试。

Agentlib 中的 `Agent` 是**一个对话执行状态机**。它没有“在群聊里启动另一个模型 Agent”的语义。Forest 先前增加的 `forest_analyze`、`ChildScheduler` 和私有子 Agent 改变了这一含义，应当移除。Forest 每个 `(bot_id, conversation_id)` 同时只能有一个活动 ConversationSession 和一条 Agent 响应链。

Forest 不直接引用 `../RAG/libs/agentlib` 的 editable/path dependency。相邻仓库并不是可部署制品，版本发布、Docker 构建和独立升级都无法据此复现。Forest 将采用经审计的协议与状态机不变量，在自己的持久执行引擎中实现，并为 CorePrompt、wire protocol 和 checkpoint 分别版本化。

## 2. Agentlib 的真实运行模型

### 2.1 三层上下文

每次 Provider 调用的逻辑输入顺序为：

```text
CORE_PROMPT
  → PresetContext(SystemMessage...)
  → 本次冻结的 ContextView
```

`CORE_PROMPT` 是库级常量，`Agent` 构造函数没有替换入口。当前文本长 10,693 UTF-8 字节，SHA-256 为 `ce4f7e0a8013e79630c5a7630661788036711f982eaad5d8d2cc4e21870a731e`。它规定 XML 根、块顺序、转义、工具边界、RAG 位置和终止条件。

`PresetContext` 是只含 `SystemMessage` 的不可变元组。它固定在动态历史之前，不写入 `ContextManager`，不归档、不向量化、不入图，也不被 RAG 召回。QiYu 把助手身份、人格、语气、当前时间、显式记忆和数字人输出约定放在这里。

`ContextView` 是 append-only 历史的只读后缀。构建器从新到旧保留完整单元，不切割结构；固定前缀和 `min_view_units` 优先于估算预算。一次 Agent run 首次建好视图后冻结，后续模型轮次、工具结果、协议修复和 pending 输入只追加，避免破坏同一 run 的因果前缀。

### 2.2 原子 ModelTurn 协议

模型输出必须是唯一的 XML 根：

```xml
<valid_agent_protocol_content_block block_type="ModelTurn">
  <payload>
    <blocks>
      <!-- Thinking / Assistant / FunctionCall -->
    </blocks>
  </payload>
</valid_agent_protocol_content_block>
```

`ModelTurn` 的不变量是：

1. 至少一个 block；
2. `Thinking` 只能出现在 `Assistant` 之前；
3. 最多一个 `FunctionCall`，且它必须是保留结果的最后一个 block；
4. 有 `Assistant` 且没有 `FunctionCall` 才是终止轮次；
5. 模型给出的运行时 ID 和时间不可信，解析后重新生成；
6. `FunctionCall` 之后的内容被截掉，防止模型伪造工具结果；
7. 真实 `FunctionResponse` 加入私有分支后才进行下一次模型调用。

因此一个 Agent run 可以有多次 ModelTurn，但每次 ModelTurn 都是一个不可拆的模型事实。工具循环是顺序的，默认最多 8 次工具调用、4 次仅 Thinking 的非终止调用和 1 次协议修复。

### 2.3 私有分支与提交

`Agent.run(UserMessage)` 先创建 `_RunState.pending_units`。RAG、用户输入、ModelTurn、FunctionResponse 和工具返回的证据都先写入该私有列表。只有得到终止 ModelTurn 后，才调用一次 `ContextStore.append_many()` 提交完整有序分支。

```text
prepare RAG once
  → freeze first model view
  → provider response
      ├─ invalid protocol → private repair feedback → provider again
      ├─ Thinking only    → append private ModelTurn → provider again
      ├─ FunctionCall     → append turn → execute one tool → append response → provider again
      └─ final Assistant  → atomic append_many → relation handoff
```

意外异常会保存当前阶段、私有单元和原始输出。`pending_call` 让工具阶段恢复时不要求模型重新生成调用；`context_committed` 让提交后的关系登记失败只重试关系，不重复追加上下文。这些标志在单个 Agent 实例内语义清楚。

### 2.4 运行中追加消息

`enqueue_pending()` 接受一个有界 FIFO 的 `UserMessage`，不取消当前 Provider 流，也不中断工具。队列在 Provider/工具完成后的模型安全边界排空。若模型刚产生终止回答但此时队列非空，既有 ModelTurn 保留为已发生事实，pending 消息进入同一私有分支，Agent 再发起模型调用；只有终止边界队列为空时才提交。

这解决了单用户应用里的 live steering，但不等价于 Forest 的群聊协议：Agentlib 的每个 pending message 都是主动 steering，没有“所有普通群消息先入队、只有 trigger 才要求模型继续”的区分，也没有“推进到最后一个 trigger、保留其后普通尾部”的切口。

### 2.5 Provider、流与工具

Provider 端口只暴露模型标识、能力和 `StreamEvent`，厂商 SDK 对象与凭据不跨边界。Agent 累积完整文本后才解析 XML；`IncrementalProtocolParser` 虽然存在，主 Agent loop 没有使用它提前执行工具。

通用 OpenAI-compatible adapter 把 CorePrompt 与工具声明序列化为首条 system message，再把每个 ContextUnit 的 canonical XML 映射到 user/assistant role。它能读取 provider-native tool call delta，但发送请求时没有提供 native `tools` 字段；QiYu 自己的 adapter 也只使用 XML 工具协议。`native_function_calling` 能力字段在这条实际路径中没有形成完整闭环。

工具执行顺序为：注册表查找 → Draft 2020-12 JSON Schema 校验 → policy 授权 → 有界超时 → handler/middleware → 规范 FunctionResponse。QiYu middleware 再用数据库 reservation 和参数摘要让相同 call ID 的工具结果可重放，并为高风险工具加入确认票据。

### 2.6 记忆与多模态

Agentlib 把长期内容事实与派生索引分开：

```text
ContextArchive        完整 ContextUnit 事实源
VectorStore           archive ID → vector
MemoryGraph           archive-backed 时序/联想边
ActiveRelationBuffer  已提交但端点尚未都归档的关系
```

裁剪预览识别将离开模型窗口的已提交单元，先写 archive，再写时序图，最后尝试 embedding。向量失败不会删除归档或图节点。检索先取向量 seed，再从旧图按最大乘积路径扩散，最后从 archive 重建完整单元并包装为 `RagMessage`。检索本身不写图；成功 turn 后只提议“触发 UserMessage → 本次保留的直接 seed”，待触发消息也归档后再提升。

`VLRetrievalEncoder` 可以将规范文本和每张图片分别嵌入，再对同维向量取等权平均并 L2 归一化。模型投影不会修改历史：支持的媒体保留；不支持的 RAG 媒体移除，音视频可退回 transcript。

QiYu 应用并没有完整启用这条多模态记忆路径。`ApplicationMemoryBridge._query_text()` 只取文本，检索按 `conversation_id` 隔离；其 OpenAI-compatible adapter 只为直接 `UserMessage` 附加真实图片数据。因此“Agentlib 具备 VL 参考实现”不能推导成“QiYu 已有完整多模态、跨会话记忆”。

## 3. 优点

### 3.1 语义边界清楚

- CorePrompt、PresetContext 和动态历史分层，角色配置不会混入可召回聊天事实。
- ContextUnit 与 ModelTurn 使用 frozen dataclass 和递归冻结 JSON，减少异步流程中的共享可变状态。
- durable serialization 与 model serialization 分开，运行 ID、URI、诊断 metadata 不会无意暴露给模型。
- Provider、工具、memory 和存储均通过窄端口注入，适合替换基础设施。

### 3.2 因果与失败处理强于普通聊天循环

- 模型、工具和最终回答在私有分支内按顺序闭合，终态才批量提交。
- 工具调用后截断模型后续文本，真实结果回来前不能伪造成功。
- pending steering 只在安全边界接入，不篡改已经发出的模型请求。
- RAG 紧邻触发消息，工具证据紧随 FunctionResponse；同一 run 的模型视图冻结后只追加。
- 工具调用、thinking、修复和 pending 数量都有上限，不允许无限自循环。

### 3.3 记忆事实与检索索引分离正确

- archive 是唯一内容事实源，向量库只提供 seed，图只提供关系。
- `RagMessage` 与 `ArchiveSummary` 都不会再次归档、向量化或入图，避免递归污染。
- embedding 失败是可修复的派生失败，不破坏归档事实和时序邻接。
- 精确 archive ID 召回绕过 embedding，但仍经过 archive 校验、图扩散和完整单元预算。

### 3.4 可验证性较好

核心测试覆盖固定提示词、原子提交、pending FIFO、PresetContext、工具续跑、关系交接续跑、协议修复、调用后截断、多模态嵌入和 text-only RAG 投影。实现中的默认上限与测试断言相互对应，便于 Forest 抽取不变量而不是复制接口名称。

## 4. 缺点与风险

### 4.1 CorePrompt 和 XML 协议成本高

- 固定提示词约 10.7 KB，按常见粗略估算约占数千 token；每个模型 profile 都要承担这段固定输入，只有 Provider 前缀缓存能缓解费用与延迟。
- 模型必须对所有自然语言和代码执行 XML 转义，`&`、`<` 和嵌套 JSON 很容易造成格式失败；修复调用会再次消耗模型费用。
- 修复反馈把完整失败候选再次放入上下文，缺少独立的反馈字节预算。
- CorePrompt 数据结构没有显式 `protocol_version` 或 digest，文档曾建议添加但源码只有 `content`。

Forest 应提供版本和 digest，限制候选/修复载荷，并将协议错误作为明确的可观测状态。首版可以沿用完整 XML 轮次以获得稳定语义，但要通过测量决定后续是否提供等价的结构化输出 adapter。

### 4.2 解析器没有达到其文档所称的全部严格性

`ProtocolParser` 严格检查主要标签、属性、block 顺序和 JSON object，但仍有以下缺口：

- 没有独立的输入字节、节点数、深度和 XML 实体限制；
- 没有检查所有结构节点的 `.text`/`.tail`，部分夹杂文本会被解析时忽略；
- 用正则和首个 `</block>` 实现 FunctionCall 后截断，依赖模型遵守 XML 文本转义；
- Agent 等完整流结束后才解析，已经产生的无效内容可能先被应用的预览投影展示；
- parser 不知道工具目录，未知工具要到 ToolExecutor 才形成错误 FunctionResponse。

Forest 的 parser 必须有请求级字节上限、禁止 DTD/entity、节点/深度/文本上限、完整 mixed-content 检查和确定性错误码。群聊外发只能来自已验证并提交的 Assistant block；WebUI 可以展示带“未提交预览”标识的流，不能把预览当事实。

### 4.3 恢复只在进程内成立

Agentlib 的 `_RunState`、`ExceptionContext`、pending queue、活动锁和 suspended branch 都在内存中。QiYu 的 `_active_runtimes` 与 `_suspended` 同样是进程内字典，并明确要求单 API worker。数据库会保存 execution 的 `suspended` 标记，却没有保存能够在进程重启后重建的完整 Agent 状态。

QiYu 的 `ConversationContextStore.commit_turn()` 对成功分支使用 PostgreSQL 事务，`DurableToolExecutionMiddleware` 也保存工具幂等结果，这是重要补强；但它不能让丢失的模型私有分支跨进程恢复。Forest 已有持久 Run、step、attempt、owner epoch、lease 和 UNKNOWN 对账，应继续以数据库 checkpoint 为权威。

### 4.4 通用 Context 端口不够适合大规模持久历史

- `ContextStore.snapshot()`/`get()` 是同步接口，持久 adapter 通常只能先把完整历史加载进内存。
- InMemory store 的写入有 lock，读取只依赖单 event loop 和不可变对象；这不是跨线程/跨进程一致性协议。
- `ContextEventBus` 在提交后并发调用 observer；observer 失败会向调用方抛错，但事实已经提交，部分 observer 也可能已完成。没有 outbox 时重试边界含糊。
- ContextView 用 canonical 字符数除以固定系数估算 token；`min_view_units` 优先后可能主动超过模型硬限制。
- 同一 run 冻结视图后持续追加，虽保证因果和 cache 稳定，却可能在多次工具/steering 后超出上下文窗口。

Forest 应保留整单元裁剪和冻结前缀，用持久 ContextView、精确 source manifest、预算预留和 outbox 取代内存 observer。若受保护因果后缀超出硬限制，应在发送前失败并要求管理处理，不能静默删掉触发或工具响应。

### 4.5 Provider 与指令优先级存在折损

- Agentlib adapter 把 PresetContext 的 `SystemMessage` 序列化后按普通 `user` role 发送，只靠 CorePrompt 文本声明其优先级；支持 system/developer role 的 Provider 没有充分利用原生优先级。
- 参考 OpenAI adapter 能接收 native tool delta，却没有发送 native tool declarations；能力字段与实际 wire 行为不完全一致。
- XML 与 native tool 两条输出路径并存，混合文本与 native call 时会生成与纯 XML 不同的 ModelTurn。
- QiYu adapter 宣称支持 image，但只把直接 UserMessage 的图片解封为模型原生图片；RAG 和其他单元中的图片不会真正交给模型。

Forest adapter 必须冻结并记录实际能力；CorePrompt 使用最高可用 system/developer channel，PresetContext 紧随其后；不支持该 channel 的降级方式写入 manifest。原生工具调用必须先归一成同一个 ModelTurn 状态机，且每轮只接纳一个调用。

### 4.6 不满足 Forest 的群聊与全局记忆要求

- Agentlib 的 UserMessage 模型视图默认去掉 metadata，无法直接表达群聊中不同作者和来源群；QiYu 是面向单用户会话的应用。
- pending queue 不区分普通消息和 trigger，不能表示 Forest 的 trigger-cut prefix。
- QiYu memory bridge 按 conversation 隔离，图本质上是一条会话主干；没有 Bot 全局域内的多群主干和跨主干联想。
- `RagNode` 模型视图没有 Forest 所需的稳定来源群、作者与当前使用群字段。
- `VLRetrievalEncoder` 使用简单等权平均，未定义模型版本迁移、文本/图片权重校准和跨版本重建；嵌套 ModelTurn 图片也没有进入当前 encoder 的图片扫描。
- Agentlib 对活动上下文的默认不支持媒体策略是 `STRICT`，可能直接失败；Forest 要求 text-only LLM 对所有上下文统一做不变历史上的移除投影。
- 可选 ArchiveSummary 会产生额外模型调用。Forest 的高流量群聊明确禁止裁剪摘要模型。

## 5. Forest 的采用矩阵

| Agentlib 机制 | Forest 决策 | Forest 变化 |
|---|---|---|
| 一个 Agent 实例拥有一条活动 run | 采用语义 | 由数据库 ConversationSession/Run lease 实现跨进程单写者 |
| 硬编码 CorePrompt | 采用并加强 | 添加 protocol version、digest、只读 WebUI 展示和请求 manifest 固定值 |
| PresetContext | 采用 | Bot 可编辑人格/策略进入独立、版本化、不可裁剪的 SystemMessage 列表 |
| 原子 ModelTurn | 采用 | 每个模型响应保存完整有序 blocks，至多一个 FunctionCall |
| 私有 pending branch | 采用 | 每个 step/checkpoint 持久化；成功后按因果边界发布，重启可恢复 |
| pending FIFO | 采用安全边界，替换选择策略 | 使用持久 ingress + 分类连续水位；每次只推进到最后一个新 trigger |
| 最终边界有 pending 就继续 | 采用 | 新 trigger 接入同一 Session 的下一次模型调用；普通尾部不触发调用 |
| 空闲时提交 | 扩展 | 无新 trigger 时静默排空剩余普通消息并执行本地裁剪，不调用 LLM |
| 顺序单工具循环 | 采用 | 一轮 ModelTurn 至多一个工具；Kernel 内部的组件 RPC 子调用仍是调用树，不是子 Agent |
| 协议 repair | 采用并限制 | 最多一次；保存错误码、候选 digest 和有界诊断，修复反馈受预算 |
| ArchiveSummary | 不采用 | 不配置、不实现摘要 Provider；裁剪只归档并重建确定性视图 |
| archive-only memory | 采用 | PostgreSQL 权威操作/归档 + outbox 派生向量/图，带 deletion epoch |
| 单序列 MemoryGraph | 替换 | 每 Bot 一个全局 memory domain，每群一条 trunk，ASSOCIATED 可跨 trunk |
| RagMessage | 采用结构思想 | 每个 node 强制模型可见 source bot/group/conversation/user/ingress/time/provenance |
| VL encoder | 采用接口，替换细节 | 文本+图片 facet、版本化共享空间、可配置融合和按图查询 |
| 不支持媒体的投影 | 加强 | 所有历史/证据/工具层递归删除不支持媒体并给出明确占位，原事实不变 |
| 内存 ExceptionContext | 不采用实现 | 持久 checkpoint、attempt、tool receipt、UNKNOWN 和 owner fence |
| post-commit EventBus | 不采用可靠性边界 | 事务内 outbox/journal，Worker 幂等处理 |

## 6. Forest Agent 核心契约

Forest 的 Agent 核心由以下稳定层组成：

```text
ForestCorePrompt(protocol_version, content, sha256)        宿主只读
PresetContext(revision, ordered SystemMessage[])           Bot 管理配置
ContextView(view_revision, retained immutable units)       群会话事实投影
RunBranch(checkpoint_seq, ModelTurn/FunctionResponse...)   持久私有分支
```

CorePrompt 只描述 Agent 协议、来源边界、输出语法、工具状态机和多模态诚实性，不包含 Bot 人格、群资料、当前时间或管理员配置。PresetContext 只包含宿主生成的 SystemMessage；更新配置只影响新 Run/新配置 epoch，不能改写活动 step 的固定请求。

模型调用返回一个 ModelTurn。Thinking 是模型作者的内部协议块，可保存于受保护运行事实和管理诊断，但不能直接发送到群；Assistant 是候选可见输出；FunctionCall 是宿主待执行意图。只要出现 FunctionCall，本轮不是终态，任何其后内容均丢弃。工具结果进入下一次模型调用之前必须已有持久终态或 UNKNOWN 阻塞记录。

群消息不通过 `Agent.enqueue_pending()` 临时塞进内存对象。它们始终先写入 Inbox，再由安全边界在事务中选择连续前缀：

```text
cut = 最后一个已连续完成分类、尚未接入且 trigger=true 的 ingress_seq
InputBatch = (context_applied_seq, cut]
```

若不存在新 trigger，当前工具链可以继续下一模型调用，但不得吸收 trigger 后的普通尾部。当前 Agent 得到终态后，若已有新 trigger，则把截至最后 trigger 的前缀接入同一 Session 并进行下一模型调用；没有新 trigger 则提交当前回复、静默排空普通尾部并结束 Session。任何时候都不创建并发的第二条群 Agent 响应链。

## 7. 必须加入的验收

1. CorePrompt 内容、版本和 digest 固定，Agent 配置/API 不能覆盖；WebUI 只读可见完整文本。
2. PresetContext 永远在 CorePrompt 后、动态历史前，更新只影响规定的下一运行边界，且不入 Context/memory。
3. parser 拒绝 DTD/entity、mixed content、未知标签/属性、过深/过多节点、超长文本、多个调用和调用后可信输出。
4. 一个响应只形成一个 ModelTurn；Thinking 不外发；终止 Assistant 与 FunctionCall 互斥。
5. 工具调用、结果和下一模型请求顺序持久可恢复；进程在每个阶段退出都不重复外部效果。
6. 同群慢模型期间到达“普通 A、触发 B、普通 C、触发 D、普通 E”，下一模型调用只接入 A–D，E 留尾。
7. 当前终态与新 trigger 竞争由同一会话守卫裁决；不会并发回复或丢失唤醒。
8. 无新 trigger 时 E 静默入 Context 并正常裁剪；没有摘要 API 调用。
9. 多用户消息和跨群 RAG 节点在实际 wire 中都有稳定作者/来源群；模型不能把来源群误当当前群。
10. 图片 query 可以召回图片或文本；text-only LLM wire 不含被移除媒体 URI/bytes，且含明确不可见标记。
11. 两个群并行调用模型，但各自只有一条响应链；它们向 Bot 全局记忆并发写入时通过事实操作、幂等键、revision/outbox 合流。
12. `forest_analyze`、ChildScheduler、children 配置和任何模型驱动的产品子 Agent 均不存在。旧数据库子 Agent 表只能作为不可写的兼容遗留物，不能重新调度。
