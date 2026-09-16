# 异步并发写入与合流性详细设计

本文补充[总体架构](01-architecture.md)、[通信与调度设计](03-communication-scheduling.md)、[多 Bot、Agent 与记忆设计](04-bot-agent-memory.md)、[持久化设计](05-storage-security-operations.md)及[多主干记忆图](10-multi-trunk-memory-graph.md)。实现基线为 Python 异步内核、各 Bot 独立事实存储域、PostgreSQL 短事务、业务 outbox 和异步派生索引。不同群聊可以并发推理并写入同一 Bot 的全局记忆；每群来源具有独立主干，跨主干联想显式保留双方证据。

插件作者使用普通 Python 模块/类、异步钩子与 Memory SDK；自动文件监视和 Worker/generation 切换由框架透明管理。本文的事务、IPC 上下文绑定、租约和发布校验由内核及官方 Memory 服务实现，插件不需要自行实现传输协议，也不能自行填写可信代际身份绕过校验。业务插件负责提供稳定业务操作身份、类型化事实和必要来源；SDK 可从消息/作业步骤派生操作身份。

本文中的“必须”是实现契约。“合流”仅用于满足下述条件的数据类型；不将 CAS 成功、消息最终消费或数据库存在提交顺序等同于业务状态合流。

## 1. 合流目标、适用范围与非目标

### 1.1 定义

给定 Bot `b`、已接纳操作集合 `O`、删除策略状态 `T`、确定性归约器版本 `v`，定义事实投影：

```text
F(b, O, T, v) = Reduce_v(Canonicalize(VisibleAssertions(b, O, T)))
```

在相同 `O`、`T`、`v` 下，操作的传输顺序、合法并行交错、重复投递次数及批次边界不能改变 `F` 的语义结果。对允许乱序执行的操作，要求：

```text
Idempotence: Apply(Apply(S, a), a) = Apply(S, a)
Commutativity: Apply(Apply(S, a), b) = Apply(Apply(S, b), a)
Associativity of merge: (A ⊔ B) ⊔ C = A ⊔ (B ⊔ C)
```

追加断言集合和明确引用目标 ID 的撤回集合可以使用集合并作为 `⊔`，满足这些性质。纯投影必须由完整的规范源集合计算，或使用经过证明等价的增量算法。

“相同 `O`”指数据库已接纳的、具有固定语义载荷的操作。权限撤销、删除屏障、配额竞争可能导致不同操作被拒绝，因此不能声称任意客户端请求调度都会产生相同结果。合流性不消除业务上的选择与仲裁。

### 1.2 与串行化、CAS、确定顺序的区别

| 机制 | 实际保证 | 不保证的事情 |
|---|---|---|
| 唯一键 / 幂等记录 | 同一个操作重试不会增加第二份已接纳效果 | 两个不同操作是否交换 |
| revision CAS | 基于旧版本的覆盖写被拒绝 | 谁先 CAS 成功不影响业务结论 |
| 数据库串行事务 | 并发执行等价于某个串行顺序 | 换一个合法顺序仍得到相同状态 |
| Shell 服务优先级 / 独立控制通道 | 忙碌时仍能调度受权控制请求；保留确定的请求/效果身份 | 抢先处理的命令可交换、可以夺取资源 lease，或能重排群历史 |
| 单会话 Actor lease | 当前会话只有一个有效提交者 | 其他会话不会修改同 Bot 记忆 |
| deterministic reducer | 相同规范输入产生相同语义输出 | LLM 的两个实际生成结果相同 |
| CRDT 式集合合并 | 满足明确定义的幂等、交换、结合性质 | 预算扣减、授权变更、外部发消息也可直接合并 |

例如“记忆文本 = LLM(旧记忆 + 新内容)”即使包在数据库锁中，两种输入顺序仍可能得到不同文本；改成失败后重试 CAS 也没有解决这种语义问题。Forest 不将此模式用于权威记忆写入。

### 1.3 保证的层次

- **权威事实层：** 断言和显式关系具有稳定操作身份；可合流部分采用集合语义，冲突保留。
- **确定性投影层：** 对相同有效事实集合、规范化规则和 reducer 版本，得到相同值集合、冲突标志、依据集合与可见性结果。
- **图主干层：** 对相同稳定来源单元、已固定的源内位置和有效成员集合，得到相同的每群主干及显式关联集合；后台完成先后和归档批次不能改变这些语义。
- **多模态来源层：** 对相同来源单元、有序 ContentPart、固定图片字节修订与合法 lineage 集合，得到相同的群/作者/part 依据及可见性；下载先后、存储去重与嵌入批次不能抹去任何独立来源。模型请求的能力投影只描述该请求实际所见，不归约或覆写 canonical 记忆。
- **LLM 派生层：** 承诺输入可追踪、候选可审计、已选择结果冻结、旧结果不能覆盖新来源；不承诺跨运行、跨模型或跨节点生成的自然语言字节相同。
- **命令与副作用层：** 采用受序列约束的提交、持久账本和对账语义，不宣称合流。

审计时间、物理行顺序、`source_revision` 与 `projection_revision` 的具体数值不属于跨调度合流比较值。相同事务组的分批方式不同可能产生不同 revision 数，但有效事实与确定性投影必须相同。

群聊交互序列另有边界：原始 ingress 顺序、触发分类和模型安全点的输入切口属于实际因果输入，不要求交换两个人的发言仍得到同样回答。给定这些输入边界，InputBatch 重试/接管必须不重复、不遗漏来源，不回退 context_applied_seq，不误消耗触发；不同安全点选到不同消息集合时，LLM 回答不同是合法的，不能据此声称违反或实现了事实合流。详见[08 群聊上下文设计](08-group-context-interaction.md)。

群聊 Shell 的控制执行顺序与历史接入顺序也不同。核心 ShellGate 可以在普通分类缺口或 Agent 忙碌时先接纳管理请求，P0–P4 服务等级只决定有资格请求的调度倾向；该命令的 Context 来源仍处在原 ingress_seq 位置。固定优先级无法证明配置、取消、批准或授权操作合流，完成顺序也不能用作全局记忆的真值依据。控制路由和回执规则见[09 群聊 Shell](09-group-shell-command-system.md)。

群来源的 `source_order_seq` 在权威事实接纳时分配，记录实际历史顺序。给定这些固定位置，图投影必须对重复、乱序和分批交付合流；交换两条原始消息的接纳位置则改变了历史输入，不承诺得到同一主干。Bot `source_revision` 仅协调共享事实的新鲜度和删除屏障，不能被拿来构造一个串起全部群的单主干。

## 2. 操作分类与执行策略

| 操作 | 类型与合并语义 | 执行方法 |
|---|---|---|
| 新增消息事实、观察、记忆断言 | 按稳定 ID 集合并；同 ID 相同载荷重试无效应 | 幂等事务追加 |
| 增加来源、证据边、显式矛盾边 | 边集合并；端点与边类型进入身份 | 幂等事务追加 |
| 撤回已知断言 | 追加 `Retract(target_assertion_id)`；撤回先到仍可保存，目标后到也不恢复可见 | 按目标 ID 归约，不按到达顺序删除数组元素 |
| 同 `logical_key` 不同值 | 多值冲突集；不能无依据地选“最近写入” | 确定性投影输出 candidates 与 conflict |
| 事实计数、标签并集 | 先按 operation/fact ID 去重，再 count / set union | 从集合计算，或维护严格去重增量 |
| 平均分、排序与权重 | 按版本化固定数值规则计算；不能逐批平均、依赖线程完成顺序或不稳定浮点累计 | 使用计数+定点和或规范有序重算 |
| 群主干节点与 TRUNK_NEXT | 稳定源单元集合与固定 source_order_seq 的确定性排序投影 | 每群一根，原子发布完整图修订；不按归档批次追加 last_node |
| 跨主干联想与证据支持 | 显式端点/证据集合；固定规则从去重集合归约权重 | 同 Bot 校验，证据单独授权及可撤回，不能重复投递就执行 weight += 1 |
| 活动 Context 裁剪 | 固定输入、预算/分词版本与 pin 集合的确定性视图选择 | 原始来源先持久归档后移出视图；无 LLM 摘要、无隐式每消息抽取 |
| 图片字节、part 来源与变体 lineage | 不可变内容修订及稳定关联集合；字节去重不合并语义来源 | capture 接纳/READY 转换受版本和 lease 仲裁；后续修订显式接纳，不按最后下载覆盖 |
| 原生文本/图片嵌入与多模态索引 | 固定 ordered parts、空间、编码/预处理版本的派生工件 | 同逻辑输入复用工件，索引代际隔离；不声称外部编码服务跨运行浮点字节确定 |
| 每次模型请求的模态投影 | 固定 canonical 视图、输入切口、能力/投影/预算版本的确定性映射 | 持久 ModelRequestManifest；fallback 新映射沿用原输入切口，不改 Context、图片或图索引 |
| 显式请求的 LLM 抽取、语义说明、整合 | 可选且受预算的非确定性派生产物，不参与任意顺序 merge | 固定输入清单，持久候选，单选冻结，验证后发布；不是裁剪依赖 |
| 人工裁决冲突、身份合并 | 有明确授权、预期冲突集合与版本的业务命令 | DB 串行提交；版本变化返回冲突，不自动替用户扩大裁决范围 |
| 删除/遗忘 | 显式 tombstone + 单调 epoch 屏障；不与任意旧生成结果直接交换 | DB 线性化，拒绝过期工作，派生删除与物理清理 |
| 配额预留/消费、唯一名分配、配置与授权变更 | 存在全局约束和不可交换的竞争结果 | 约束表短事务串行提交；重试复用操作身份 |
| Shell 取消/暂停/恢复、批准与重载 | 依赖目标状态、预期版本和维护意图，结果可能随合法提交顺序改变 | 复用目标事务、approval_requests 与生命周期资源 lease；优先级不越权或夺锁 |
| 发消息、付款类工具或外部修改 | 外部系统未必支持幂等与查询 | outbox + effect 账本；不确定结果为 `UNKNOWN` |

“后续事实取代先前事实”需要领域定义。例如时间区间允许“周一住杭州，周二住上海”同时成立；`observed_at` 必须作为事实语义之一。它不是覆盖数据库旧行的排序技巧。缺少有效时间、可信来源或显式裁决时，地点不同就是待解析冲突。

## 3. 标识、修订与表结构

### 3.1 统一术语

| 字段 | 定义与限制 |
|---|---|
| `operation_id` | 一个业务操作的稳定重试身份；同一次消息处理/作业步骤重试必须复用；不能每次网络重试生成新 ID |
| `logical_key` | 版本化规范化后的语义键，例如 `(subject_id, predicate, valid_time_scope)`；不同 operation 可以写同一 key |
| `input_digest` | 规范语义输入及相关配置版本的摘要；规范化包含类型、字段、集合排序及编码规则；同 operation 不同 digest 必须报 `IDEMPOTENCY_CONFLICT` |
| `reducer_version` | 确定性归约实现与规范化规则的不可变版本；升级需独立重建和兼容验证 |
| `source_revision` | 当前 Bot 事实域的已提交单调修订；仅用于快照/脏标记和覆盖范围，不等同消息时间或“更真实” |
| `group_source_id` / `trunk_id` | Bot 域内的稳定群来源及其唯一主干；与 Binding、Conversation、线程和群显示名分离，详见 10 |
| `source_unit_id` / `source_order_seq` | 原子来源身份及其源内固定位置；原始群消息在权威 Inbox 接纳时分配，闭合模型/工具在事实提交时分配，不随裁剪/归档批次或 Worker 完成顺序变化 |
| `part_id` / `part_index` | 同一来源内容版本内稳定 ContentPart 身份及顺序；文本和图片混排保留语义位置，不因某模态尚不可用而重新编号 |
| `media_revision` / `ordered_parts_digest` | 前者固定受管图片字节修订，后者覆盖有序 part 身份、类型、文本摘要/图片修订与编码输入选择；不以临时 URL、下载 attempt 或批次号作内容身份 |
| `space_id` / `profile_revision` | 经声明对齐的嵌入语义空间及完整模型/预处理配置版本；同维度或同 Provider 名不能替代兼容证明 |
| `request_id` / `source_cut_digest` | ModelRequestManifest 的具体模型请求身份、实际可见内容及原输入切口；与持久 context_view_revision 不同，omitted image 仍是 canonical 来源 |
| `graph_revision` | 原子发布的完整图快照身份；读者在同修订下读根、head、主干邻接与关联，不把不同图修订混用 |
| `deletion_epoch` | Bot 级单调删除屏障；每次遗忘事务递增，旧工作不能越过；首版取保守 Bot 级粒度 |
| `projection_revision` | 已发布投影的单调发布修订；用于读者和 CAS，不决定事实真伪 |
| `module_generation` / `route_epoch` | 正在执行的模块制品代际与数据库写入 fencing；与删除 epoch 是不同边界 |
| `actor_epoch` | Conversation 当前写入租约的 fencing；只约束该会话的上下文/Run 提交 |
| `maintenance_epoch` | Bot 控制维护期望的单调版本；旧 resume/重载收尾不得覆盖后续暂停，不能代替 route/resource/Actor fence |
| `source_manifest` | 规范有序的源事实/单元 ID、版本与内容摘要、稳定来源群/主干、逐条作者、ContentPart/media 修订、关系/可见性依赖和选择器版本；跨主干计算保留全部源分组，完整描述实际输入 |

`input_digest` 不包含重试节点、attempt ID、RPC trace ID 或当前 Worker generation 等执行元数据；否则合法接管会改变同一业务操作身份。语义变更必须创建新业务操作并关联原操作，不能覆盖既有 digest。删除后重建工作必须重新读取有效事实，禁止给旧载荷换上新 epoch 就重新提交。

### 3.2 最小逻辑表

以下字段是对存储总设计的补充。每个表在共享物理部署时都必须将 `bot_id` 纳入主键/唯一键和访问隔离；独立 Bot 数据库仍保留字段，便于接口自校验及迁移。

| 表 | 关键字段与约束 |
|---|---|
| `memory_clock` | `bot_id PK, source_revision, deletion_epoch, projection_clock`；短事务持有行锁分配 revision 与检查删除屏障 |
| `memory_operations` | `bot_id, operation_id PK, operation_kind, input_digest, semantic_payload_ref, source_revision, admitted_deletion_epoch, origin_group_source_id?, origin_conversation_id?, origin_run_id?, source_event_id?, module_generation, route_epoch`；已接纳语义不可变；单个 origin 仅作操作来源审计，不代替多源 source_manifest |
| `memory_assertions` | `bot_id, assertion_id PK, operation_id FK, logical_key, key_version, normalized_value, valid_time_scope, source_manifest_ref, source_visibility, assertion_kind, source_revision, admitted_deletion_epoch, payload_ref`；`assertion_id` 由操作与稳定输出序号派生 |
| `memory_relations` | `bot_id, relation_id PK, operation_id FK, relation_kind, target_assertion_id, evidence_assertion_id?, source_revision`；包括 retraction、contradiction、scoped-resolution |
| `memory_tombstones` | `bot_id, tombstone_id PK, operation_id, selector, selector_version, deletion_epoch, source_revision, reason_code`；不得存储待遗忘明文的备份 |
| `suppressed_sources` | `bot_id, source_identity, suppression_kind, deletion_epoch` 唯一；阻止同一已删除原消息/归档/工件由重放路径重新抽取 |
| `memory_scope_heads` | `bot_id, scope_id PK, selector_ref, source_revision, desired_source_revision, processed_source_revision`；所有可能改变该 scope 输入的事实、关系和依赖变更都必须推进它 |
| `memory_projections` | `bot_id, scope_id, reducer_version` 唯一；`input_digest, source_manifest_ref, source_revision, validated_source_revision, deletion_epoch, projection_revision, candidates, conflict_state, selected_artifact_id?` |
| `projection_jobs` | `bot_id, job_key` 唯一；`scope_id, reducer_version, desired_source_revision, deletion_epoch, status, lease_epoch, lease_until, attempt_count`；desired revision 只增不减 |
| `llm_artifact_slots` | `bot_id, artifact_key` 唯一；`input_digest, source_manifest_ref, prompt_version, model_config_digest, reducer_version, deletion_epoch, selected_artifact_id?, status` |
| `llm_artifact_candidates` | `bot_id, artifact_id PK, artifact_key FK, attempt_id, output_ref, output_digest, provider_receipt?, validation_status`；结果冻结，可被废弃而不被悄悄改写 |
| `index_documents` | `bot_id, document_id PK, source_manifest_ref, source_ids, trunk_ids, source_part_refs, ordered_parts_digest, embedding_id, profile_id, profile_revision, space_id, model_revision, preprocess_revision, chunker_revision, normalization_revision, input_mode, index_generation_id, input_digest, deletion_epoch, projection_revision, graph_revision?, coverage_ref, status`；版本化 ID 不能覆盖不同 parts/空间文档；群/作者/图片出处及来源资格仍须从事实库逐项复核 |

`semantic_payload_ref`、source manifest 和候选输出可能含敏感内容；遗忘任务必须覆盖这些存储，不仅清理 `memory_assertions`。逻辑事实追加模型不免除物理清理要求：删除后的有效集合由 tombstone 决定，载荷可随清理任务抹除或销毁加密密钥，只留下所需的无明文删除审计信息。

来源/主干/图发布表以[05 的存储总览](05-storage-security-operations.md#24-记忆与派生)及[10 的完整图契约](10-multi-trunk-memory-graph.md)为准。`memory_source_clocks` 在全局锁序中位于 `memory_clock` 之后、scope heads 之前，多个源按 source_id 排序。普通 Inbox/闭合 Context 提交先取得需要的 Conversation 锁，再接纳源位置；归档/图 Worker 不反向获取会话锁。共享 Bot 时钟不要求持有全 Bot 锁等待模型或一次完整图重建。

媒体/嵌入/请求表以[05](05-storage-security-operations.md#26-多模态来源受管媒体与嵌入)和[11](11-multimodal-memory-and-model-projection.md)为准，复用 `media_assets/media_captures/media_source_links/media_variants`、`embedding_profiles/embedding_artifacts/embedding_index_heads` 与 `model_request_manifests`。物化事实、source/scope 脏标记和索引 Job 同事务，外部下载/编码在事务外执行；媒体/索引业务行进入统一业务表锁序，不持媒体行反向获取 Conversation 或 memory_clock。

### 3.3 key 与身份规范化

主体身份优先使用 Bot 域内的稳定 `subject_id`，由平台身份映射提供。不能直接把姓名字符串当全局唯一身份；两个群里的同名成员不会自动合并。同一个人的跨平台合并是显式命令，产生版本化身份映射事实，触发相关投影重建。

规范化规则更新时，新 `key_version` 与旧版并存迁移。禁止两个 Worker 分别用不同字符串清洗规则写同一个声称稳定的 `logical_key`。所有规范化后的值保留源证据，便于更换规则时从事实重建。

## 4. 必须保持的不变量

| 编号 | 不变量 |
|---|---|
| C-01 | `∀ operation_id: count(distinct accepted input_digest) ≤ 1`；同 ID 异载荷绝不能按重复成功返回 |
| C-02 | 同一 Bot 的相同有效操作集合、删除状态与 reducer 版本产生相同确定性投影语义；无数据库时间或随机值作为隐式胜者 |
| C-03 | `Conflict(k)` 中每个候选都能追溯到仍有效的依据；存在互斥候选时不得仅因最后完成的 LLM 输出消失冲突 |
| C-04 | 任意持久化提交在同一最终事务内检查当前 `route_epoch`，必要时检查 `actor_epoch` 与 `deletion_epoch`；检查和提交之间不得存在允许旧代越界的空档 |
| C-05 | 已发布派生产物的 `input_digest` 与存储的 source manifest 一致，并通过发布时 scope 输入和删除屏障验证；旧输入不能覆写当前输入 |
| C-06 | `source_revision`、每 scope 的 desired revision、`projection_revision`、`deletion_epoch` 只单调前进；旧事件不会降低水位 |
| C-07 | 遗忘线性化后，带旧 deletion epoch 的工作不能新建事实、发布派生产物/索引或领取新的相关发送动作；被 tombstone 覆盖的来源不得通过重试重新接纳 |
| C-08 | 向量索引及缓存不成为可见性的权威；返回内容前以当前事实、tombstone、Bot 和来源权限复核 |
| C-09 | 对话上下文只有当前 Actor epoch 能提交；同 Bot 其他 Conversation 的独立事实追加不需要持有本会话 lease |
| C-10 | 事实变更、scope 脏标记和业务 outbox 原子提交；无通知时数据库扫描仍能找回待派生工作 |
| C-11 | 全局预算、配置、授权与显式裁决的命令结果以事务约束为准；其结果不得由合流 reducer 改写 |
| C-12 | 外部动作超时与结果未知不能被投影归约成“未执行”；`UNKNOWN` 只由提供方查询、可信回执或明确人工处理转出 |
| C-13 | 同群稳定来源只对应一条主干；TRUNK_NEXT 的两个端点必须同 Bot、同 trunk，顺序由固定 source_order_seq 决定 |
| C-14 | 相同有效源单元/位置、关联证据集及图规则版本，产生相同图语义；裁剪分批、归档完成先后、投影重复投递不改变节点身份和最终边集 |
| C-15 | 任一召回片段、关联解释或冲突依据都保留获准公开的稳定群出处；多源事实不压成单个 origin，隐藏来源不能从边、路径、名称或计数泄露 |
| C-16 | 默认流式接入与 Context 裁剪不创建 LLM 摘要或隐式语义整合作业；裁剪保留稳定来源、合法 pin 与预算版本，模型调用计数为零 |
| C-17 | 同 blob 字节可以在同 Bot 复用存储/编码，所有独立源单元、群/作者、part 与权限依据仍分别保存；存在相同图片不能成为跨群授权 |
| C-18 | TEXT/IMAGE/MIXED 索引工件绑定原始模态、固定空间及完整输入/预处理版本；新旧空间不因维度相同而混查，晚到图片不重排主干 |
| C-19 | 对话模型不支持图片时，每次 API 输入移除非文本内容；只改变该请求投影，canonical Context/归档/图片索引保持，未看过图片不得记为已见 |
| C-20 | 图片删除/来源 suppression 传递至变体、索引、缓存、模型工件及新 egress 许可；同 Bot 仍有效的其他来源可保留共用字节，但不能借其恢复已删来源 |

## 5. 事实写入与冲突归约算法

### 5.1 接纳事务

```python
async def append_operation(request, execution_context):
    canonical = canonicalize_semantic_input(request)
    digest = hash_bytes(canonical)
    async with bot_db.transaction():
        # 锁的全局顺序与其他提交入口一致；FOR SHARE 必须阻止 epoch 更新。
        route = await lock_route_for_share(execution_context.component_instance)
        require_current_route_epoch(route, execution_context.route_epoch)
        await require_current_capability(execution_context, request.operation_kind)
        if request.also_commits_conversation:
            actor = await lock_actor_for_share(request.conversation_id)
            require_current_actor_epoch(actor, execution_context.actor_epoch)
        clock = await lock_memory_clock_for_update()
        require_equal(request.deletion_epoch, clock.deletion_epoch)
        require_sources_not_suppressed(request.source_manifest)

        existing = await get_operation(request.operation_id)
        if existing:
            require_equal_or_idempotency_conflict(existing.input_digest, digest)
            return existing.receipt

        revision = clock.source_revision + 1
        await insert_operation_and_immutable_assertions(request, digest, revision)
        await append_explicit_relations(request, revision)
        await mark_affected_scopes_dirty(revision)  # 同事务、按 scope_id 固定锁顺序
        await update_memory_clock(source_revision=revision)
        await append_business_outbox("memory.changed", request.operation_id, revision)
        return Receipt(request.operation_id, revision)
```

入口的授权预检查用于快速失败，不能替代事务内检查。使用 PostgreSQL 行锁时，`FOR KEY SHARE` 通常不能阻止非键 epoch 字段被更新，不能误用它替代这里的排他语义。所有路径采取统一锁顺序，事务冲突/死锁仅重试整个短事务，重试沿用 operation ID。

首版为 Bot 修订分配持有短暂 `memory_clock` 行锁，物理落库会形成提交顺序。这个实现有利于快照与删除屏障；事实语义仍必须独立满足合流条件。锁内不能调用模型、嵌入服务、外部工具或等待消息响应。优化为分区修订前，应先证明删除屏障和全 scope 水位仍正确。

### 5.2 同主体的并发断言

群甲生成 `assert(A, subject=U, preference.tea=green)`，群乙生成 `assert(B, subject=U, preference.tea=black)`：

```text
Assertions = {A, B}
Retractions = {}
Candidates[U, preference.tea] = {
  green: supporting_assertion_ids={A},
  black: supporting_assertion_ids={B}
}
Conflict = unresolved
```

如果谓词允许多偏好，领域 reducer 输出合法多值；如果谓词要求单值，输出 unresolved conflict。不同谓词的冲突规则属于版本化 schema，而非 LLM 自由决定。排序只为展示稳定，可按规范值/ID 排列，不能将排在最后者当真值。

上述 A/B 示例假定调用者有权使用两份依据。内部全 Bot 事实投影不能直接作为面向群聊的检索响应：对外必须先按当前主体、ReplyRoute 与来源策略过滤断言/关系，再计算该授权视图的候选和冲突。不能过滤完文本后仍返回“另有一个隐藏冲突”或隐藏候选数量。若缓存这种授权投影，scope 与 input digest 必须绑定可见性上下文和 policy revision。

人工或可信领域命令可提交 `Resolve(expected_candidate_digest, chosen_value, supersedes={A,B})`。事务内复核预期候选集；若群丙期间写入 C，命令返回 `CONFLICT_SET_CHANGED` 或仅明确处理 A/B，而 C 必须继续参与候选。禁止裁决隐式压制尚未观察到的并发事实。

撤回是针对稳定断言 ID 的关系。先接收到 `Retract(A)`、后接收到 A，和先 A 后撤回的有效集合相同。它不是“删掉当前 key 上碰巧存在的最后一条记录”。对于不引用具体 ID 的全主体遗忘，使用第 8 节 epoch 协议。

## 6. 异步派生与发布算法

### 6.1 先捕获完整输入，再异步计算

1. Worker 领取 `projection_jobs` 的 lease；job desired revision 只作为“至少需要检查至此”的提示，事件载荷不能取代事实库。
2. 在短 `REPEATABLE READ` 事务中读取 `memory_clock`、scope head、当前授权策略依赖与该 scope 的有效事实/关系，生成 source manifest。记下 `source_revision=s0`、scope head revision `h0`、`deletion_epoch=d0`。随后关闭事务。
3. 纯 reducer 在事务外计算，已配置的异步向量化任务也在事务外执行；LLM 仅用于显式启用且有预算的语义任务。默认归档、图主干和裁剪不调用 LLM，合法 Context 视图也不等待语义或向量 Job。事实 ID、输入和版本均固定，不能计算一半再读取可变全局记忆补入输入。
4. 结果提交到候选工件存储，然后执行独立的发布事务；任何 Worker 超时或被替换时都可从固定输入/候选恢复。

source manifest 必须覆盖**选择器的输入范围及其依赖**。例如“最近 50 条事实的视图”不能只记录已选 50 条的 ID 而忽略新事实会挤入窗口；应记录 scope selector、窗口规则与 scope head。身份映射、可见性、schema 及适用的提示词/抽取规则变化也必须进入 digest 或使依赖 scope head 前进。

首版使用 Bot 范围或明确 key 范围的保守 scope。所有可能影响 scope 的写入必须在事实事务内推进该 scope head；无法证明选择器增量依赖完备时，标记整个 Bot 的对应派生 scope 为脏，不能静默跳过“暂时没有命中的”新事实。

### 6.2 发布事务

```python
async def publish_projection(candidate, execution_context):
    async with bot_db.transaction():
        await lock_and_require_current_route_epoch(execution_context)
        clock = await lock_memory_clock_for_update()
        head = await lock_scope_head_for_update(candidate.scope_id)
        require_equal(candidate.deletion_epoch, clock.deletion_epoch)
        require_equal(candidate.scope_head_revision, head.source_revision)
        require_current_input_versions(candidate)  # selector / policy / reducer / prompt
        require_manifest_digest(candidate.source_manifest, candidate.input_digest)

        current = await lock_projection_for_update(candidate.scope_id, candidate.reducer_version)
        if (current and current.input_digest == candidate.input_digest
                and current.deletion_epoch == candidate.deletion_epoch):
            return current.receipt  # 相同输入已发布，不用重复提升修订
        require_expected_projection_revision(current, candidate.base_projection_revision)
        if candidate.is_llm_artifact:
            require_selected_artifact(candidate.artifact_key, candidate.artifact_id)
        revision = clock.projection_clock + 1
        await replace_projection_with_verified_candidate(candidate, revision,
                                                        validated_source_revision=clock.source_revision)
        await update_projection_clock(revision)
        await advance_processed_revision_without_lowering_desired(
            head, through_scope_revision=candidate.scope_head_revision)
        await append_business_outbox("projection.published", candidate.artifact_id, revision)
```

scope head 未改变是“有效源集合未变化”的快速证明，前提是所有输入变化都遵守 scope 标脏不变量；再验证工件的 manifest digest，可阻止固定输入与输出记录错配。若 head 变化，即使怀疑只是无关变更，也不能直接放行：重新取得当前完整源集合，核对 digest 后才能复用候选并生成新的验证凭据。

`source_revision=s0` 记录最初读取的全局快照，`validated_source_revision=s1` 记录发布时核验到的事实域修订。s1 大于 s0 且 scope 输入不变时可以发布，但这只说明该 scope 仍有效，不代表这个投影包含 Bot 的所有 s1 事实。

候选 CAS 失败不能循环把同一旧输出套到最新 projection revision 上覆盖。应重新读取；相同输入已发布则结束，不同输入则重新计算或复用经过完整输入核验的产物。

### 6.3 重复、乱序与丢失唤醒

- 消费 outbox 时按 event ID 去重；`desired_source_revision = max(old, event.source_revision)`，不使用事件抵达顺序确定真值。
- Worker 从事实库读取完整有效输入，允许事件先到 12 再到 10；不能以“已经见过 12”为由推断 1–11 的逐条增量必定都处理过。
- 如果实现逐条增量 reducer，必须额外记录已应用 operation ID 集合或可证明连续的日志位置；仅保存 `max_seen_revision` 不足够。
- 同一发布事务更新 projection、processed 水位及 outbox。计算期间发生新写入时，scope head 校验失败或 desired 水位仍高于 processed；Job 必须再次被扫描领取，不能把整个 scope 简单标成 clean。
- 多个 Worker 可重复计算纯投影；只发布一个有效输入版本。通知系统丢失时，恢复器扫描 `desired_source_revision > processed_source_revision` 和超时 lease。
- 旧 generation 的候选不能自行使用新 route epoch 发布。新 Worker 可以显式接管固定工件，在当前权限、来源、删除 epoch 和输入版本下重新验证后发布；接管不迁移旧 Python 调用栈。

### 6.4 多主干图的顺序与证据合流

源事实先固定群来源和位置，再由确定性投影选择满足归档/输入可见性条件的主干成员。`source_unit_id` 来自平台事件或闭合模型/工具步骤的稳定身份；同一逻辑源重放不能另造节点，同一批次拆成十次裁剪也不能改变来源粒度。收到较大 source_order_seq 不表示较小位置已归档完毕，Worker 必须从权威成员集合和明确覆盖 manifest 重建，不以 `max_seen_seq` 声称连续完整。

```text
Members_g = unique(valid eligible source_unit_ids where group_source_id = g)
Ordered_g = sort(Members_g, by=(source_order_seq, source_unit_id))
TRUNK_NEXT_g = adjacent_pairs(Ordered_g)
Graph = {one stable root for each g} ∪ Members ∪ TRUNK_NEXT ∪ ExplicitAssociations
```

这里的 eligible 至少要求源载荷已可靠归档且群消息已进入合法 InputBatch/静默输入；Inbox 中登记身份并不等于进入图召回。删除的源位置保留删除屏障，投影按当前有效成员重新形成邻接，不能重分配其他节点的 source_order_seq。分组由稳定 group_source_id 决定，不按群名、最新 Binding 或当前 Conversation 分组。若某个原始源身份已固定了位置，后续归档不能换一个更大的位置表示“新记录”。原接纳因果顺序不同的两份历史不属于相同固定输入。

跨群关系必须使用显式 `ASSOCIATES_WITH`、`EVIDENCE`、`CONTRADICTS` 等类型；不能修改 TRUNK_NEXT 把两个群接成同一条时间链。联想提案包含稳定 relation/operation ID、类型、规范化端点、证据 ID 集合、模型/规则版本。对称关系先规范化两端排序；有方向关系保留方向。重复传输只合并已接纳证据集合，权重由去重证据及版本化定点规则重算，不执行“收到一次就加一”或让最后 LLM 分数覆盖旧分数。撤回证据显式引用其稳定 ID，撤回先于补投也不会复活。

LLM 联想是显式可选 Job：冻结两端 source/assertion revision、各自群来源、完整 evidence manifest、policy dependencies、deletion epoch 与算法版本，候选单选后仍执行发布复核。端点变更或遗忘不能只重贴新 epoch；须重读两端及全部证据，必要时重新生成。多源语义事实可复用一个事实节点，但必须通过全部来源关系保留群甲与群乙的依据，不能把当前查询群重写成内容的发生群。

图 Worker 捕获固定源头后在事务外构建完整候选修订；发布事务校验源头、关联依据、删除屏障与预期图修订，原子发布同一 `graph_revision` 的 root/head、成员和边 manifest。跨主干关系与两端修订在同一图快照中可解释，不能先发布新边而让读者仍使用旧端点。重试发现相同 manifest 已发布即结束；新事实使输入变化则重新构建或完整验证，不能仅刷新 CAS 值后覆盖。

允许显式冻结截至s0的历史覆盖；范围外新事实不使该历史快照冒充错误计算，也不要求全Bot停写才能发布。候选必须如实记录各主干覆盖/缺口，不降低当前已发布覆盖，重验删除及实际依赖；声称包含最新输入的候选仍按最新scope head检查。持续写入用desired/processed追赶，并发更新通过graph head CAS复核/合成分片，不能将另一主干的合法进度覆盖成旧版本。

可验证为同一平台事件的多个Binding观察归到同一规范source_unit，固定位置不重复分配；memory_source_occurrences保留每个Conversation/ingress/InputBatch关联。来源集合和其观察依赖分别并集，不能用最后到达的会话字段覆盖其他观察。当前会话仍在切口外的同源观察继续限制召回及其派生物；平台不能提供跨Binding稳定身份时保留较弱去重域，不凭文本相似性声称事实相同。

检索先固定图修订，随后按当前输出主体、ReplyRoute、来源权限、删除状态及模型输入切口过滤节点、边和完整支持证据。每一跳都不得穿过隐藏端点产生公开路径解释；相同事实的多个合法来源必须逐项标出 group_source_id、安全群名快照及消息/作者定位。当前群仍在队列中的尾部及其派生产物即使在图物理存储中存在，也不能绕过 InputBatch 通过联想返回。固定图语义的合流并不赋予跨群公开权限。

### 6.5 无模型摘要的确定性裁剪

裁剪输入固定为 `context_revision + context_view_revision + context_view_profile_ref + source_manifest + token_budget + tokenizer_version + trim_policy_version + pin_set`。先持久化原始来源或不可变范围引用并确认 retention pin，再按版本化规则移出闭合、可移出单元；发布的是所保留单元与移出范围的视图 manifest。未回答触发的必要原文、系统约束和未闭合工具结构保留；若最小合法视图仍超预算，按 08 的显式超限/暂停策略处理，不能偷偷调用模型总结或丢弃触发原文。canonical 视图预算由明确 view profile 固定，含图片的完整 ContentPart 和归档引用保留；当前对话 LLM 不支持图片或请求预算改变都不能成为持久删图/改写此视图规则的理由。

该操作可因新输入 CAS 失败而重算，但重算仅使用确定性预算和选择规则，不生成“记忆摘要”。不同裁剪时机可产生不同活动窗口，属于输入版本变化；在同一固定版本与 pin 集合下，重复执行必须得到相同视图且不新建重复图节点。归档打包、压缩与索引完成次序不修改源身份、图位置或逐条出处。后续有需要时 Agent 显式读取历史片段，使用原来源标签；不在每条消息、每次超预算或每次归档后隐式追加 LLM 调用。

### 6.6 多模态来源、下载与索引合流

同一消息的 `(source_unit_id, source_revision, part_id, part_index)` 在接纳时固定，图片使用已分配的 capture/media 修订关联。下载延迟只改变物化可用性与覆盖版本，不产生第二个 source unit，不把图片追加成主干末尾的新发言。READY 接纳校验 capture lease、预期摘要、来源 suppression 及 deletion epoch；固定 media revision 一旦有合法字节就不能用后续下载覆盖。若没有原始摘要且可变 URL 的两次读取返回不同图，承认外部输入已发生变化：必须以明确捕获/修订仲裁选定字节并保留其时间来源，不能把两次内容不同的物理执行宣称为同输入合流。

blob 去重键限定 Bot 和字节摘要。两群相同图片可复用字节与编码计算，但 `media_source_links` 对每个原始来源分别保存；同图命中应展开为各有资格的 source part，再执行当前群切口和可见性验证。不同独立来源是不同证据，单个来源重投递不增加依据或联想权重；不能以 blob hash 去重整个 memory source，亦不能把一个群的公开许可推广到同图的其他群。源与变体的 lineage 采用稳定关联集合，变体按父修订和转换版本/参数摘要幂等，跨群图边依然引用明确来源依据。

每项嵌入的规范 key 至少覆盖：

```text
embedding_key = H(bot_id, profile_id, profile_revision, space_id,
                  model_revision, preprocess_revision, chunker_revision,
                  normalization_revision, input_mode,
                  ordered_parts_digest, encoded_media_manifest_digest,
                  embedding_input_digest)
document_id = H(bot_id, source_manifest_digest, source_part_refs,
                embedding_key, index_generation_id, validation_epoch)
```

`ordered_parts_digest` 保留图文顺序、text 摘要、每张图的字节修订及变体选择；`encoded_media_manifest_digest` 进一步固定实际物化版本、原图摘要与变换，pending占位不能与后来真实图片共用编码key。原生 IMAGE 与图片说明的 TEXT 工件永不共用 key。已接纳工件冻结，重试复用结果；外部 embedding 服务即使给同 key 返回略异向量，也只能经一次选定记录固定可重放工件，不能依最后完成覆盖，不承诺服务端浮点输出确定。相同字节/配置可在 Bot 内复用计算结果，但每份文档仍独立保留完整来源 manifest 与当前验证 epoch，计算缓存不充当授权。

文本先就绪时可以发布声明 TEXT 覆盖的文档/索引修订；图片后来 READY 使对应依赖水位变脏，并在固定原 part 位置生成 IMAGE 或已配置 MIXED 工件。没有图片的工作不能将整体文档标为完整，旧 PARTIAL 修订晚到不能覆盖已经发布的扩展覆盖；索引发布对相同版本的覆盖以集合并/规范 manifest 重建，若源内容修订变化则重新验证而非盲目并。捕获失败或预算不足保持有界 PARTIAL/FAILED 状态和修复 Job，不回退源顺序，不让无图分支触发隐式 caption/OCR API。裁剪不等待嵌入完成。

EmbeddingProfile 与 Bot 生命周期绑定，同一 Bot 不执行空间原地迁移。模型、修订或维度变化时创建新 Bot，在其独立 index generation 中按显式迁移输入重建；来源写入通过各自 desired/processed 水位补齐，发布 `embedding_index_heads` 时 CAS 校验覆盖与当前源/删除依赖。查询固定某个已发布 head，并使用与其匹配的 query encoder/预处理；同维度不同空间不能混算余弦或其他距离。若跨 Bot 迁移工具需要并查新旧空间，只能分别排序后按版本化规则融合候选，来源重复项只作一次证据，融合配置与选中的各 head 进入查询快照。

### 6.7 模型能力投影与并发请求

`ModelRequestManifest` 是固定 `context_revision/context_view_revision/input_revision/source_cut_digest` 上的确定性投影记录；增加能力快照、projection version、请求预算与实际 included/omitted part refs 后，才能准确比较同输入映射。text-only 模型从 canonical 图文 Context、召回和完整工具结果中递归移除非文本载荷，保留合法文本与安全来源定位；映射不改 canonical parts、图像索引或裁剪 profile，不将缺失图像编造为文本描述。

能力热重载、Provider fallback 与迟到图片是不同事件。本次 dispatch 固定能力/可用媒体修订与 manifest，后来图片完成不能回写“本次模型已看到图片”。fallback 只有在原请求状态允许新调用时，才沿同一个 source cut 按新能力重新投影并生成独立 manifest；不能把超时一律当作旧调用未执行，也不能混入新触发尾部。新的合法模型安全点可以选择已经就绪的图片版本，但要保存新的源物化快照和实际输入依据。召回索引空间迁移不改变已冻结 evidence refs，图片源变化/遗忘则必须重新验证这些引用。

请求预算仅在当前投影上计算文本/图片开销，必要的有界缩减进入本次 request manifest；不能把 text-only 的 omitted parts 或请求保留集合提交为 `context_compactions.result_ref`。同一 canonical 视图先调用文本模型再调用视觉模型，第二次仍可读取当前授权、未遗忘且可用的原图片。投影、预算和缺失标记都不创建摘要、OCR 或 caption 调用；这些额外语义服务仅可作为用户/Agent 显式请求的独立能力与预算任务，不能被记作原生图片嵌入或成本免费的裁剪步骤。

语义请求与实际传输分别固定。`ModelRequestManifest`保存语义input/projected content摘要；Gateway先在事务外基于合法冻结refs准备精确body（含实际短效URL/字节），最终permit短事务再验授权/删除、Session和预算，并写不可变 `model_transport_attempts` 的wire `payload_digest`，提交后才发送。URL刷新产生新transport_attempt_id，原请求语义不变时可复用manifest，但不覆盖旧hash；能力fallback则创建新语义request。调用state/receipt/usage和UNKNOWN仍由RunStep/checkpoint统一管理，不能因为建立了新attempt就绕过旧调用的不确定状态；亦不能先冻结一个未知wirehash再在事务外悄悄改变body。

RunStep通过 `model_request_id/transport_attempt_id/model_call_epoch` 固定单个当前有效调用；旧attempt终态或明确放弃并fence失效后，接替CAS才提升epoch。最终候选必须匹配该三元组和Run/Session fence，迟到旧响应只进入对账费用，不能追加第二份ModelTurn、触发工具或覆盖当前结果。这是调用仲裁，允许不同获准fallback策略产生不同文本，不把一个赢家结果伪装成多模型输出合流。

## 7. 可选语义派生的非确定性与发布

本节只适用于显式启用、获准且有独立预算的语义抽取、联想或按需内容说明。默认群消息接入、主干维护与 Context 裁剪均不创建这些模型任务，也不依赖其完成；裁剪不存在“记忆摘要”阶段。配置开启某类语义任务仍必须说明触发条件、频率、最大费用和恢复策略，不能把每条消息隐式变成模型调用。

### 7.1 单选冻结工件

LLM 的输出不是可交换更新函数。Forest 将一次调用表示为固定输入上的候选工件：

```text
artifact_key = H(bot_id, task_kind, source_manifest_digest,
                 prompt_version, model_config_digest, reducer_version, deletion_epoch)
```

1. 先按 artifact key 创建持久 slot，并领取有 fencing 的 lease。
2. 已有 selected artifact 时复用，不再次请求模型。
3. 没有结果时提交模型请求，将响应原样保存为不可变 candidate。
4. 结构、引用与业务规则验证通过后，以 slot CAS 选择一个 candidate；同 slot 的其他响应只留审计状态，不覆盖 selected artifact。
5. 发布仍执行第 6 节输入和 epoch 核验。选择成功不代表当前来源仍有效。

这里的选择是一项持久化仲裁，承认不同物理运行可能选到不同文本。它只保证重试后沿用已选择结果，不把这种仲裁描述为 LLM 输出合流。更换模型或提示词得到不同 artifact key，可以并行评估后显式升级投影。

### 7.2 抽取到权威事实

同一原始消息可能被多个抽取 Worker 重试。抽取的 `logical operation` 应由 `(bot_id, source_event_id, extractor_version, extraction_step)` 固定；只有已选择 artifact 能生成对应的新增断言操作。选中的输出列表按 schema 规范化，输出断言 ID 使用稳定序号或规范内容键派生。

不同 extractor 版本的输出不是同一个重试。它们保留各自来源、置信信息与版本，不自动覆盖；替换旧版本结果需提交明确的 supersession/retraction 关系。LLM 自己提出的冲突裁决标记为 suggestion，不能无授权地变为压制其他证据的权威命令。

### 7.3 可选语义产物不替代原始记忆

用户或 Agent 显式请求的语义说明保留完整 source manifest；召回可回到事实与冲突候选。说明不能通过丢失一个候选而抹去 unresolved conflict。对单值冲突，结构化部分必须保留冲突标记、每条依据的群出处，即使自然语言说明只写一句“偏好存在不一致”。这些产物不替代原始来源，不作为裁剪后活动 Context 的自动补偿文本。

显式大范围语义任务可采用固定来源分块，按任务预算决定是否生成块级产物；分块规则、块顺序、来源群及版本进入 artifact key。相同输入复用已选工件，不因新消息到达自动重建全 Bot 语义说明。合并文本时不允许按异步完成先后决定拼接顺序，也不假定 `LLM(text(A), B)` 与 `LLM(text(B), A)` 等价。

显式语义队列使用 coalescing/debounce 合并等价作业，按 Bot 设置最大延迟和并发预算；过期候选可以保留缓存但不能发布。连续写入时，指定不可变归档切片的产物只能发布为**该切片**的工件；必须标注来源群和覆盖区间，不得冒充包含最新全部事实的全局记忆，也不能反向阻塞默认无模型裁剪。

## 8. 删除、遗忘与防复活

### 8.1 删除线性化事务

```text
校验授权与当前 route epoch
→ 锁 memory_clock
→ deletion_epoch += 1；source_revision += 1
→ 写 tombstone（断言/来源/主体/时间范围等版本化选择器）
→ 为已遗忘源消息、归档和派生来源写 suppressed_sources
→ 标记受影响事实不可见，作废旧派生发布资格
→ 推进全部已注册派生 scope 的验证 head，标记 epoch_revalidation
→ outbox 写索引删除、工件清理、原载荷清理任务
→ 同一事务提交
```

Bot 级 deletion epoch 是首版保守屏障，会使其他正在计算的派生产物也需要重新验证。这个额外工作量换取明确的正确性边界；以后优化为细分 scope epoch 时，必须证明所有传递依赖都进入屏障。

采用全 Bot 屏障时，必须为全部仍需服务的旧 epoch 投影登记重新验证工作，而非只给命中删除选择器的 scope 创建任务；否则未受影响的旧投影也会因 epoch 失效而永久不可用。重新验证源集完全相同的工件可以在新 epoch 下生成新的验证/slot 记录并复用既有字节或向量；命中删除来源的工件必须重建，不能仅更新 epoch 字段。

事实的 `admitted_deletion_epoch` 是接纳审计信息。不能把所有旧 epoch 的历史事实一概删除：仍有效事实由当前 tombstone 和权限判断。需要当前 epoch 的是新派生发布/索引文档的验证凭据，而非每条既有事实的创建时间。

### 8.2 旧工作与索引乱序

- epoch d 的抽取任务在 d+1 完成时，接纳事务拒绝其新事实。任务不能只修改请求 epoch 重新发送；必须重新读取来源且确认未被 suppress，再创建合法的新计算。
- tombstone 与 suppressed source 身份保留到所有重放源、备份恢复和延迟任务的治理条件满足；不能索引删除完成就立即删除屏障记录。
- 向量物理删除后，一个旧 upsert 仍可能在索引服务晚到。故索引文档 ID 必须包含 source/input/embedding 版本，查询命中后重新查事实库；旧文档不能覆盖新版本文档，更不能因为仍被向量召回就被直接输出。
- 首版保守要求派生文档的 deletion epoch 与当前验证 epoch 一致。遗忘后，在新 epoch 下重新验证未受影响的 source manifest 才能复用现有嵌入；无需盲目重算所有向量，但未经验证的旧文档不可直接服务。
- 回复 outbox、缓存和模型工件也携带 source manifest 与 deletion epoch；发送动作领取前重新核验。已在遗忘线性化前开始的外部请求可能之后完成，必须记录这种在途边界；框架不承诺撤回已经提交给外部模型或平台的内容。
- 物理清理失败保留可重试任务，但事实可见性在删除事务完成时已失效。需要展示删除进度时分别显示逻辑失效与载荷/索引清理完成状态。

恢复旧备份必须同时恢复或外部补入不早于备份的删除日志/屏障，然后才能开放读取与派生任务；缺少删除日志时不能假定恢复出来的数据均可重新公开。

### 8.3 图片与派生闭包的遗忘

删除选择器作用于稳定 source/part/media 身份及其版本化 lineage。命中来源后，逻辑失效闭包包括该来源的 media_source_links、缩略图/裁切等变体依据、IMAGE/TEXT/MIXED 索引文档、显式图像说明工件、召回/计算缓存的来源验证记录，以及使用它的请求/回复 source manifests。旧下载、变体或 embedding 即使物理完成，也要在最终接纳/发布处被 suppression/epoch 拒绝；物理 blob 可暂留清理队列，但从删除线性化后不能给该来源新发下载句柄或模型发送许可。

引用计数只在同 Bot 内对仍有效来源关联和合法 retention pin 计算，作为物理清理资格而非可见性依据。两群独立合法发送相同图片，遗忘群甲消息只删除甲的来源证据，乙仍可按自己的权限引用共用字节；甲不能借乙的 blob 缓存复活原来源。若删除命令明确指向该 Bot 的媒体内容及全部来源，则所有符合选择器的关联均失效。无有效引用且无合法 pin 时，经当前 deletion/GC revision 再验才清理原图和变体；GC 与新链接接纳共享对应业务锁/CAS，不能先计数再无锁删除。retention pin 不抵消已生效遗忘，其用途只是在常规归档/请求恢复中防止仍合法载荷提前释放。

模型 manifest 可保留无载荷的请求审计身份，但含原图/文本的 projected payload、调试缓存和签名 URL 缓存遵守同一清理闭包，不能因“请求已记录”永久保留已忘内容。已签发 URL 的 TTL 不等于可即时撤回能力；新发签名之前重新鉴权，已许可在途请求按第 8.2 节记录边界。跨 Bot 不共享可访问 blob 命名空间、引用计数、能力句柄或删除屏障。

## 9. 会话租约、全局命令和外部副作用

### 9.1 同会话与跨群的边界

同 Conversation 仅有一个逻辑活跃 Session；Actor 执行 lease 可释放，WAIT/RELOAD 不能因此启动第二条同群响应链。所有普通/触发群消息都通过 InputBatch 或静默输入独立追加 Context；每个模型安全边界按最后待触发记录冻结输入，完整模型/工具步骤在后续消息之前按因果顺序flush，最终ModelTurn与回复意图原子提交。旧Actor失去lease后不能修改Context；若同次提交还追加Bot记忆，应在同一个Bot事务校验session/actor/route/deletion epochs。

群甲和群乙有不同 Actor lease，二者能并发读相同 Bot 记忆快照、等待不同模型请求、提交不同 operation。对共同 `logical_key` 的冲突由事实投影处理，不让群甲的长推理持有全 Bot 锁，也不让任一 Actor 以整份内存字典覆盖共享记忆。

静默和主动 Run 的裁剪都发布确定性视图 manifest，记录准确作者/群来源、context_view_revision、预算/算法版本和当前删除屏障，不创建模型摘要。模型中的多用户 role 相同不代表 subject 相同，长期断言必须从相应源消息确定主体。输入水位只证明已入历史，trigger claim/answered 另行持久；整批排空不能把没有得到回答的触发误归约为已完成。源顺序、Context 输入切口与图发布修订分别验证，不能互相替代。

### 9.2 非合流全局命令

预算预留使用 `budget_operations(operation_id UNIQUE, bot_id, reserved, settled, status, ...)`，对应 05 的 budget_accounts。预算行短事务锁下检查 `consumed + reserved + requested <= limit` 后预留；同操作重试返回原预留。结算和释放有明确状态转换，UNKNOWN 的外部模型费用不能直接作为零消费释放。两个任务竞争最后一份预算，谁成功由数据库仲裁；这是合理的顺序相关结果。

配置/授权命令带 `expected_revision`，修改时校验并递增。两个管理员基于相同配置做互斥修改，一个成功、另一个收到冲突；不能把两个完整 JSON 配置按字段盲目 merge。若某字段明确定义集合并语义，可将该字段建模为专用可合流操作，仍须满足授权与约束。

[WebUI后台](12-webui-administration.md)与Shell竞争相同目标配置的权威修订；浏览器多标签页也没有最后保存优先权。界面可展示base/current/draft差异并由管理员选择修改，再重新校验、预览和提交；三方比较不是自动合流，也不绕过schema/策略/依赖版本变化。配置期望提交和资源生效分别记录，旧生命周期操作在发布时检查其是否仍被允许接续当前期望，不能把晚完成的候选无条件设为最新配置；跨Bot批量结果逐目标展示。

群聊 Shell 复用这些仲裁入口，不通过独立管理内存对象修改状态。取消与最终回复提交使用同一 Run/效果事务边界：取消先提交时迟到正常回复失败，回复先提交时取消不能抹去已完成事实。批准在唯一 `approval_requests` 记录中对固定命令/计划单次决定，不能将多个批准令牌集合并成更宽的权限；授权变更仍由目标 Bot 当前 policy 事务决定。

暂停/恢复按 `bot_runtime_state.maintenance_epoch` 和显式 admission 期望仲裁。例：重载 L 读取维护版本 m，随后暂停命令 P 提交 m+1 并关闭 data admission；L 的路由切换可以完成自身合法阶段，但其收尾不能恢复 m 所记住的旧准入。旧 resume 也必须因版本变化重新检查最新期望。P0 取消/暂停只能快速提交控制意图；正在重载的资源 lease 不被抢走，不因高优先级将旧操作伪装成已排空。资源的冻结、排空、取消/恢复仍以[生命周期设计](02-kernel-lifecycle.md)与[05 的统一锁序](05-storage-security-operations.md#3-事务与锁顺序)为准。

同一 shell 请求的重试固定 `command_id`、规范 `input_digest`、注册表/契约快照与目标；生命周期/Job/效果使用稳定的 `operation_ref={kind,id}`。跨 Bot 在来源库持久请求、目标库幂等操作和来源 CONTROL 回执之间恢复，不存在覆盖两库的隐式事务。目标操作已成功而来源回执未提交，只能查询/续接原操作，不能以新 ID 再执行；这保证可追踪的恢复，不把多目标的部分成功说成合流或原子完成。

### 9.3 外部效果

事实归约器仅产生新的意图，不能在重放过程中直接执行外部动作。发送/工具请求沿用 effect ID 与 outbox 状态机。提供方已接纳但网络响应丢失时，账本为 `UNKNOWN`；支持幂等键的提供方可按同键重试，支持查询的先查询，无能力的进入对账/人工策略。

`source_revision` 较新或某份投影最终成为当前版本，并不授权重复发送历史消息。消息回复不是可以无限重算并覆写的投影。

CHAT 按会话 `response_seq` 顺序发送；CONTROL 按命令 `control_seq` 保存回执顺序，控制回执可以越过 CHAT 的 UNKNOWN 队首。它们仍共享实际平台/账号限流、绑定所有权和发送许可，分别保留不确定结果，不宣称两通道具有一个全群发送总序。任何优先级都不能撤回已被平台接受的请求，亦不能使未知结果自动成为可安全重放的失败。

## 10. 性质验证与并发时序验收

纯 reducer 必须具有可执行的性质测试；事务与 Worker 协议必须有真实数据库并发及故障注入验证。测试数据使用合成事实，不运行真实外部副作用。

| 编号 | 时序或性质 | 通过条件 |
|---|---|---|
| CC-01 | 对相同断言/明确目标撤回集合随机排列、随机分批，并插入任意重复 | 规范化最终候选集合、冲突与有效依据完全一致；不比较审计 revision 具体值 |
| CC-02 | 两 Worker 同时提交同 operation ID 和相同 digest | 恰好一组事实和一份逻辑 outbox 效果；两个调用得到同一已接纳回执 |
| CC-03 | 同 operation ID 同时携带两个不同 digest | 仅一个可接纳，另一个明确 `IDEMPOTENCY_CONFLICT`；不得默认为重复成功 |
| CC-04 | 群甲写 A、群乙写 B，二者同 key 但值互斥；互换完成顺序 | 两种执行都产生 A/B 多值冲突，不出现 LLM 最后写胜出 |
| CC-05 | 先撤回 A 再收到 A；另一执行先 A 再撤回 | 两种执行中 A 均不可见；无“先删不存在元素”导致的复活 |
| CC-06 | Worker 在 s10 捕获输入；s11 增加该 scope 的事实；s10 结果最后返回 | scope head 校验拒绝旧发布；重读后处理 s11；s10 输出不得仅更新 base revision 后复用 |
| CC-07 | s10 捕获 scope 甲；s11 只改变无依赖的 scope 乙 | 甲可在证明源集不变后发布；coverage 标记仍局限甲；不会强迫所有无关派生产物重算 |
| CC-08 | 事件 12 先到，10 后到，11 重复；计算时再提交 13 | desired 水位不倒退，13 不被错误标 clean；恢复扫描能完成有效输入的当前投影 |
| CC-09 | 两 LLM 候选针对同 artifact key 返回不同文本，交换响应先后 | 每次运行最多一个 selected artifact，重试固定复用；测试明确允许不同运行选到不同文本，权威冲突依据不丢失 |
| CC-10 | d 下显式语义模型/embedding 开始，d+1 遗忘完成，旧结果晚到 | 旧事实接纳/派生发布被拒绝；旧索引 upsert 即使物理成功，读取复核也不返回已忘内容 |
| CC-11 | 旧源消息在遗忘后被 Inbox/归档重放，并尝试携带新 epoch | suppressed source 拒绝重新抽取；不能通过更换 epoch 绕过遗忘 |
| CC-12 | 模块 route epoch 切换或 Actor lease 接管发生于模型返回前 | 旧上下文和旧代写入拒绝；新 Worker 只有重新验证候选后可发布；另一个群持续推进 |
| CC-13 | 两群同时竞争最后一份全局预算 | 不超额、不重复预留；允许顺序相关的单方成功，失败方获得明确预算错误 |
| CC-14 | 人工裁决读取 A/B 后，群丙增加 C，裁决才提交 | 预期候选 digest 检测变化；C 不被隐式删除或被裁决覆盖 |
| CC-15 | 事实事务成功后进程在通知前崩溃，或投影提交后在 ACK 前崩溃 | outbox/dirty 扫描恢复；重复消费不增加第二份事实或回退 projection |
| CC-16 | 外部发送实际成功后断开回执连接，Worker 随即被替换 | 保留 `UNKNOWN` 或按可查询提供方对账；不能因新 generation 重放而无条件再次发送 |
| CC-17 | 任意分组累计评分、计数或标签，包含重复投递 | 计数以唯一事实为准；平均使用稳定 `(sum,count)`，结果不随批次变化 |
| CC-18 | 删除后恢复早期事实备份再恢复派生任务 | 删除日志先应用，旧事实不因恢复重新可见；缺日志时恢复流程不能直接开放服务 |
| CC-19 | 两条配置/授权Shell命令基于同revision，互换优先级或提交先后 | 最多一个互斥修改成功；另一方明确冲突，承认赢家可随仲裁改变，不错误标为合流 |
| CC-20 | 重载读取维护版本m后，新暂停提交m+1，再让旧重载/resume收尾 | 最新data admission保持关闭；旧流程不能恢复旧期望，P0没有夺取生命周期资源lease |
| CC-21 | 普通消息分类有缺口，后面的取消命令已执行且CONTROL先发出 | Context仍按原ingress_seq推进，CHAT自身response_seq不乱，命令未生成第二Agent触发 |
| CC-22 | 跨Bot目标命令成功后丢失回执，来源重试或重启 | 查询同一operation_ref/稳定子操作ID，不产生第二份效果；结果可恢复而不宣称跨库原子 |
| CC-23 | 取消与Run最终提交/效果DISPATCHING以受控barrier交错 | 各顺序只有符合权威事务的结果，迟到取消不抹除已完成事实，在途效果UNKNOWN继续留账 |
| CC-24 | 固定多群 source_unit_id/source_order_seq，将归档和图事件随机乱序、分批、重复 | 每群根/成员/TRUNK_NEXT 和最终显式关联语义一致；节点身份不随 archive_id 或批次改变 |
| CC-25 | 两群同名，同群换 Binding/Conversation/线程或改名，再跨群召回 | 唯一群主干映射稳定；每段模型输入明确真实群出处，联合事实保留两群可公开证据 |
| CC-26 | 同一跨主干关联证据重复提交、撤回先于补投，并互换 Worker 完成顺序 | 有效证据集合及定点权重相同；不重复加分、不以最后 LLM 输出覆盖，不产生跨群 TRUNK_NEXT |
| CC-27 | 图候选完成期间端点变化/遗忘，或物理图写入一半崩溃 | 当前指针只指完整合法 graph_revision；不混合 root/head/邻接，新版本不能复活旧端点或证据 |
| CC-28 | 召回路径经过隐藏群或当前群尚未入 InputBatch 的尾部 | 隐藏名称/数量/路径解释及尾部派生内容均不返回；当前图修订不替代当前权限与输入切口 |
| CC-29 | 固定 Context/预算/pin 输入重复裁剪；再模拟高流量多次合法裁剪 | 固定输入视图一致、源不重分配、必要触发原文保留；裁剪模型请求与费用计数为零 |
| CC-30 | 交换两条群消息的权威 source_order_seq 分配结果 | 承认实际历史输入变化可改变主干顺序；不能用排序投影或固定锁序宣称原始顺序仲裁合流 |
| CC-31 | 固定图文 source/parts，将下载、变体和索引事件乱序、重复、分批 | 原 part 顺序、每群/作者依据与最终有效覆盖一致；图片晚到不新建源位置，重复不增加证据权重 |
| CC-32 | 两群相同图片、不同作者/可见性，反转 blob 去重与向量生成顺序 | 同 Bot 可复用计算，独立来源和授权仍保留；命中图片仅返回当前有资格的来源 part |
| CC-33 | 文本 PARTIAL 与图片扩展覆盖候选交错发布，旧候选最后完成 | 旧结果不能降低同版本覆盖，当前 head 只指合法完整 manifest；内容版本改变时重验源集而非盲目并 |
| CC-34 | 同维度异 embedding space/preprocess 的查询和重建并发 | 匹配 query encoder/head，隔离空间代际；跨空间仅声明的名次融合，旧 Worker 不覆写新索引 |
| CC-35 | 固定 canonical 图文视图先 text-only、再 vision 请求，或获准 fallback | 每请求生成准确 included/omitted manifest；原图片不因投影丢失，fallback 不复用旧 payload 或纳入新尾部 |
| CC-36 | 请求冻结后图片下载完成、能力更新或图像 source revision 改变 | 旧 manifest 实际所见不变；新请求显式固定新物化/能力版本，旧结果不得声称看过迟到图片 |
| CC-37 | 图片遗忘先于旧下载/缩略图/embedding 返回，随后缓存命中 | 接纳/发布与新媒体签发被 epoch/suppression 拒绝；所有来源派生验证失效，物理字节残留不恢复可见性 |
| CC-38 | 共享 blob 的群甲来源删除与群乙引用接纳/GC 交错 | 当前权限及 GC CAS 保护合法乙来源，甲依据不复活；无合法引用才物理清理，不跨 Bot 计数 |
| CC-39 | 同 capture 的可变 URL 返回不同图片且交换完成顺序 | 摘要验证/明确修订仲裁，无已固定 media revision 覆盖；承认输入不同，不假称下载顺序合流 |
| CC-40 | text-only 请求预算变化与 canonical 裁剪、嵌套工具输出并发 | 请求省略不回写 canonical view；最终 API 无非文本载荷，工具结构合法，裁剪/投影额外模型调用为零 |
| CC-41 | prepare签名URL后撤权/删除，或permit过期需刷新URL；另一次发送UNKNOWN | final permit拦截失权；合法刷新建新transport attempt/hash且旧hash不变，语义manifest按需复用；UNKNOWN不因此被盲目重发 |
| CC-42 | 新fallback已成为当前调用，旧模型响应与新响应同时到达 | request/attempt/call epoch及Run/Session最终CAS只接纳当前结果；旧响应仅费用对账，最多一个ModelTurn/工具步骤，不因本地取消将UNKNOWN归零 |

性质测试的核心比较应类似：

```python
expected = reduce_semantically(operation_set, tombstones, reducer_version)
for delivery in generated_permutations_batches_and_duplicates(operation_set):
    actual = consume_and_reduce(delivery, tombstones, reducer_version)
    assert canonical_semantics(actual) == canonical_semantics(expected)
```

对非合流操作单独测试安全不变量和状态机，例如预算不超额、裁决不扩大范围、UNKNOWN 不被误判。不能通过强制测试只采用一种到达顺序来掩盖不合流实现。
