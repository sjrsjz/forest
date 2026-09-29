# 群聊上下文、触发排队与串行交互详细设计

## 1. 交互契约与边界

本章规定群聊前台 Agent 的消息接入语义，是[通信调度](03-communication-scheduling.md)、[Agent 与记忆](04-bot-agent-memory.md)、[存储事务](05-storage-security-operations.md)在该场景的具体协议。这里的“群”指完整 `ConversationKey`，包含 Bot、绑定、平台、频道及可选线程。同一 Bot 的不同群仍可并发工作，并读写同一个 Bot 全局长期记忆域。

已确认的 `next_input_boundary = model_safe_point`：**新触发可在当前模型调用及其工具因果组闭合后的下一次模型调用前接入，不必等待整段 Agent 回答结束。** 流式输出未结束、工具结果尚未闭合时只排队，不修改已提交给 Provider 的输入。

必须保持以下行为：

1. 所有已接纳、允许读取的群消息都进入会话上下文管线，包括没有提及 Bot、没有触发 Agent、由其他用户发出的普通消息；它们同样参与 token 计数、归档和裁剪。
2. 同一群最多存在一个未终结的 `ConversationSession`，其中最多一个前台 Run 在执行模型步骤。新触发入队，不启动第二个同群响应 Agent。
3. 忙碌时每次安全点冻结最后一个待处理触发的序号，把尚未推入的连续消息前缀推到该条为止；其中的普通消息和更早触发都要保留。最后触发之后的普通尾部继续排队。
4. 没有新触发时，正常工具循环不吸收普通尾部。Agent 完成本次回答后，若还有触发则继续串行处理；若没有，则静默推入剩余普通队列并排空。
5. 空闲期间只有普通消息时，直接静默推入、可靠归档并按本地预算裁剪，不启动 Agent 或摘要模型，不生成群回复、工具行动或自动的新对话触发。裁剪路径的LLM API调用次数为零；可选向量索引在独立批量任务中维护，不是裁剪完成条件。
6. 历史保存每条消息的可信发言者身份和当时展示名。不同用户、同名用户、引用作者和实际发送者均能区分。

“全部进入上下文”不表示无限保留所有原文在每个 Prompt，也不表示把群消息复制到独立的群长期库。原始记录先可靠保存并建立连续来源覆盖，短期视图按确定性规则保留预算内原文，其余只保留归档范围/来源引用或合法占位；不再生成“记忆摘要”。来源引用不是正文内容的替代语义，不声称模型理解了未物化原文。被当前权限或删除规则禁止读取的内容不得因本规则重新出现。

### 1.1 群聊管理 Shell 与对话队列

管理员的显式 `#forest` 命令通过[09 群聊 Shell](09-group-shell-command-system.md)进入独立控制路由。ShellGate 位于任意业务插件之前，命令可在 Agent 生成、等待工具/确认、普通分类尚有缺口时受理；执行需要的资源修改仍遵守原有锁、fence 和安全点。这个入口既不申请第二个 ConversationSession，也不依赖对话模型解释命令。

每条管理输入仍占一个 ingress_seq，固定 `trigger_decision=CONTROL_COMMAND`，按原顺序进入 Context。管理成功、失败、无权或未知子命令均不生成对话触发；它们不会把“最后触发记录”移到队列尾部。若后续正常触发覆盖了该范围，或 Agent 结束后静默排空，届时再推入这些控制历史。命令已先执行不等于 Agent 当时已读到命令正文，取消/撤权依靠权威状态守卫即时生效。

控制历史使用 `context_role=control_data` 的结构化投影，保留作者、来源、命令种类、时间、operation 引用与可见摘要。普通且允许读取的命令文本照常保留；敏感参数、确认令牌和受限结果只存允许访问的记录/对象引用，不进入 Prompt、普通日志或记忆派生。此处缩减内容的依据是读取权限与字段敏感性，不能只因命令被 Consumed 就删除整条历史。引用、转发或模型生成的命令字符串仍是数据，不能变成受权 Shell 输入。

管理回执使用 CONTROL 发送通道，不占用聊天 response_seq，也不形成 Agent 的 ModelTurn。自身平台回流按 intent/part ID 关联回执，不重复追加为普通群消息或触发自己；可见控制审计引用由会话状态所有者在安全边界追加，不能由 Shell 直接写活动 Prompt。回执及其派生结果也遵守当前输入快照的可见性边界，不能从检索旁路注入尚未接入的控制来源。

## 2. 持久状态与不变量

### 2.1 三种顺序与三个游标

| 字段 | 含义 | 不能代替的东西 |
|---|---|---|
| `ingress_seq` | 同会话的持久接收序号；重投递沿用原记录 | 平台时间戳、工具完成时间、回复顺序 |
| `classified_through_seq` | 从起点开始，预处理和触发分类已终结的最大连续序号 | 已见过的最大序号；后者可能跨过缺口 |
| `context_applied_seq` | 已通过已提交 InputBatch 覆盖的最大连续入站序号 | 触发已得到回答；原文仍在活动视图 |
| `commit_seq / unit_index` | Context 事实的提交顺序，包含输入、已完成工具因果组和最终回复 | 消息在平台上发生的先后 |
| `context_revision` | Context 追加事务的修订 | 裁剪视图版本 |
| `context_view_revision` | 原文保留、归档引用和确定性裁剪布局的修订 | 入站消费进度；已完成回复数量 |
| `mailbox_revision` | 入站、分类完成及待处理工作变化时递增的唤醒版本 | 唯一事实源；内存队列是否为空 |

始终有：

```text
0 <= context_applied_seq <= classified_through_seq < next_ingress_seq
同会话 active_session_id 至多一个，终结前不可换成另一未终结 session
同会话持有效 actor_epoch 的 Context 写入者至多一个
每条已接纳来源只被一个已提交的输入范围首次覆盖
每个触发都有独立持久状态；context_applied_seq 前进不自动终结触发
```

Pipeline 可以并发完成预处理。每条记录必须最终进入已定义的分类终态，事务推进连续头时跨过连续已终结项，并同时写待处理标记/outbox。`NO_TRIGGER` 是普通消息分类，仍有正常 Context 内容，不能等同于丢弃。禁止“未命中 Agent，因此不进入历史”的入口分支。

`context_disposition` 与 `trigger_decision` 分开：`SKIPPED / REJECTED / FAILED` 等处理终态不自动决定正文能否进入 Context。只要正文已成功规范化、允许读取，就仍追加正文和作者信息；未命中 Agent、已被命令处理器消费、触发分类故障都不能把这条正常消息改成空占位。分类故障另有明确错误/终态，不能永久阻塞连续头。

只有正文因权限、删除、格式损坏等原因确实不可物化时，才使用来源序号及允许保留的原因占位。不成功的媒体解析可保留原始媒体引用和失败状态。预处理重试必须有 deadline 和终态，不允许一个死亡 Worker 留下永久序号缺口。

图片消息在规范化时即保留有序image part、可信来源和受管媒体获取引用；图片字节尚未到达不等于空消息或无触发。获取作业有独立deadline及PENDING_FETCH/READY/UNAVAILABLE等明确状态，完整枚举见[11](11-multimodal-memory-and-model-projection.md#4-图片接纳归档和可恢复性)。分类可在已知类型/身份和有界获取结果下终结，不能永久等待图片/OCR而阻塞连续水位。迟到媒体作为版本化materialization/enrichment记录保存，只能由下一次合法安全点请求选入；不能在已发请求中补图、修改原ingress_seq或触发第二个同群Agent。

### 2.2 物理所有权、Session、Run、输入批次与触发记录

下表是物理所有权契约，不是可以合并进同一 Repository 的概念清单。`conversation.scheduler` 只拥有消息连续序、分类水位、触发领取、输入切口与 Context 提交顺序；`agent.runtime` 才拥有 QiYu Agentlib 语义下的单一 Agent 执行状态机。调度器不能创建模型步骤、工具调用或“子 Agent”，Agent 也不能自行扫描 Inbox 改写切口。

| 所有者 | 记录 | 关键字段与含义 |
|---|---|---|
| `conversation.scheduler` | `conversation_lanes` | `conversation_id, next_ingress_seq, classified_through_seq, context_applied_seq, mailbox_revision, context_revision, context_view_revision, actor_owner_id, actor_epoch, actor_expires_at`；可保留 `active_session_ref/active_run_ref`，但它们只是不可解引用的稳定引用 |
| `conversation.scheduler` | `inbox_messages` | 不可变来源身份、`ingress_seq`、正文/作者 PayloadRef、分类状态、触发决定；每条已接纳消息都存在，不因 `NO_TRIGGER` 或控制命令而删除 |
| `conversation.scheduler` | `conversation_triggers` | `trigger_id, event_id, ingress_seq, status, principal_ref, claimed_session_ref, claimed_run_ref, input_batch_id, answered_reply_ref` |
| `conversation.scheduler` | `input_batches` | `input_batch_id, kind, start_seq?, end_seq?, trigger_cut_seq?, selected_input_end_seq, session_ref?, run_ref?, context_revision`；固定来源切口与输入语义 |
| `conversation.scheduler` | `selection_operations` | 每次选择 operation 的输入摘要、边界、冻结结果和可选 InputBatch 引用；`WAIT_INPUT/CONTINUE/IDLE` 也有可重放回执 |
| `conversation.scheduler` | `input_batch_events / input_batch_triggers` | 规范化批次成员；长时间运行的数据不能塞入 JSON 数组后失去索引、唯一约束和逐项审计能力 |
| `conversation.scheduler` | `trigger_requeues / trigger_requeue_items` | 可恢复的 Worker/Run 明确失效后的幂等重排事实及逐 trigger 成员；不能靠 `context_applied_seq` 猜测触发已经完成 |
| `conversation.scheduler` | `trigger_terminal_operations / trigger_terminal_items` | Run 失败或取消后的幂等终结事实、目标状态、原因及逐 trigger 成员；保留原 Session/Run 归属并释放 lane |
| `conversation.scheduler` | `context_units` | 输入及其他模块已提交事实的有序 PayloadRef；保存来源、作者、所属批次和 `commit_seq/unit_index`，不复制正文 |
| `conversation.scheduler` | `context_append_operations` | Agent/控制模块追加闭合步骤或最终回复的幂等回执；固定 Context revision、unit 集及可选回复引用 |
| `conversation.scheduler` | `context_compactions` | 裁剪覆盖范围、源集digest、归档引用、保留/移出单元清单、裁剪策略版本、基础/发布视图版本；无摘要模型工件 |
| `agent.runtime` | `conversation_sessions` | `session_id, conversation_id, status, active_run_id, session_epoch`；承载同群持续工作期间的 Agent 逻辑独占 |
| `agent.runtime` | `runs / run_inputs` | Run 生命周期及追加的 InputBatch 引用；新增批次不覆盖早期输入 |
| `agent.runtime` | `run_steps / run_checkpoints` | 持久保存每个模型请求的输入版本、已闭合步骤、工具操作和 continuation |
| `agent.runtime` | `model_request_manifests / model_transport_attempts` | 模型能力投影、实际请求载荷摘要、派发状态和回执 |

两个模块之间只交换类型化引用和带摘要的消息，不建立跨 schema 外键或 join。调度器提交 InputBatch 后发布 `InputBatchCommitted`；Agent 以幂等 operation 接受该引用并更新自己的 Session/Run。Agent 最终回答、闭合步骤或取消结果再经调度器端口追加 Context 单元、终结所领取 trigger。任一侧崩溃时重放同一 operation；不得用双写表或定时扫描猜测另一侧是否成功。

一个模型调用固定`input_revision, context_revision, context_view_revision, selected_input_end_seq, trigger_ids, source_manifest_digest`及ModelRequestManifest。后者记录实际Provider/model、能力快照、included_part_refs/omitted_part_refs、媒体revision与projected_content_digest；精确载荷在事务外构建后，由最终许可事务另存ModelTransportAttempt的payload_digest，结合派发/回执状态说明该模型实际收到哪些内容。input_revision随同一Run新增InputBatch递增，媒体物化/模型投影另行版本化，不能冒充新增群输入；正在进行的调用不更改自己的版本。trigger_ids包含该Run为完整回复累计领取、尚未终结的触发，不只保存最近一批。

InputBatch 的 `kind` 分为：

- `TRIGGER_INPUT`：选择触发后增加新的来源前缀，并把对应触发领取给当前 Session/Run。
- `CONTEXT_PREFILL`：触发切口距离已提交游标超过 64 条时，每次只提交至多 64 条连续消息和它们的 Memory 输入资格；不领取任何触发，不创建或推进 Agent Run，最后一个触发消息始终留在最终的 `TRIGGER_INPUT` 中。早期触发的作者、来源和待领取状态保持不变。
- `SILENT_DRAIN`：无触发时追加普通来源前缀，并在越过 Context 高水位时于同一维护事务归档、裁剪；`run_id = null`，不产生回复型 Run。
- `RECOVERY_CLAIM`：恢复尚未完成的触发，而其来源已经提交过；`start_seq = end_seq = null`，引用既有 Context 单元，不重复追加消息。

输入选择端口必须显式带边界原因，不能用一个含糊的“拉取下一条”接口：

- `IDLE`：没有活动 Session 时，若连续头内有 trigger，则先分批预填超长前缀，再截到最后一个 trigger 并返回 `TRIGGER_INPUT`；只有普通尾部时直接提交 `SILENT_DRAIN`，不创建 Session/Run。
- `MODEL_SAFE_POINT`：仅在当前模型调用和工具因果组已经闭合、下一次模型调用尚未冻结时使用。有 trigger 则先分批预填超长前缀，再截到最后一个 trigger；无 trigger 时返回 `WAIT_INPUT` 或 `CONTINUE`，绝不吸收普通尾部。
- `SESSION_END`：Agent 已结束本次回答且没有可领取 trigger 时，把连续普通尾部提交为 `SILENT_DRAIN`；若已有 trigger，则返回新的 `TRIGGER_INPUT`，由同群串行状态机继续工作。

旧实现中的字符串 `SAFE_POINT` 只是过渡名称，干净契约使用 `MODEL_SAFE_POINT`。Repository 必须拒绝未知边界、过期 actor epoch、与现有 operation 不同输入的幂等键重用。每次选择都先写不可变 `selection_operations` 回执，包括不产生批次的 `WAIT_INPUT/CONTINUE/IDLE`；同一 operation 永远重放第一次观察到的结果，状态变化后调用者必须使用新 operation。`PREFILL_ONLY` 只在有效 actor fence 下推进连续前缀，跨 Conversation 不共享行锁；普通 `ANY` 选择若遇到超过 64 条尚未提交的触发前缀，必须拒绝并要求预填。预填和 Memory 资格、归档处于同一 UOW，最后一个触发留给最终批次以维持召回输入。`SESSION_END` 在领取下一轮触发前也执行相同预填，当前回复的触发集合不受新消息影响。

### 2.3 触发状态独立于消费游标

```text
PENDING -> CLAIMED -> ANSWERED
              |----> FAILED
              |----> CANCELLED
              `----> PENDING     确定放弃旧执行并受控重排
PENDING ------------> FAILED / CANCELLED
```

`CLAIMED` 只说明某个会话执行承诺处理该触发；模型启动、一次工具调用结束、输入被裁剪或 Agent 已阅读，都不等于 `ANSWERED`。最终回复事务把实际纳入该回答输入、归属于该 Run 的触发关联到回复意图并标记 `ANSWERED`。该状态表示回复事实已提交，平台是否送达另查 Delivery；它不声称模型回答的内容质量已经得到证明。

最终回复必须经调度器的 Context 追加端口提交：同一事务写 `context_append_operations`、不可变 `REPLY` Context unit，并把明确列出的、仍由该 Session/Run 领取的 trigger 转为 `ANSWERED`。列出的 trigger 必须无重复，且与该 Run 当前所有仍处于 `CLAIMED` 的 trigger **完全相等**；遗漏或混入另一 Run 的触发都使整笔回复事务失败，不能留下已完成 Run 的孤儿触发。禁止提供一个能绕过回复 Context 单独“批量回答 trigger”的公开端口。闭合的非最终 Agent 步骤也通过同一追加端口写 `AGENT_STEP`，但不终结 trigger。

同一批次可以领取多个触发并产生一次连贯回复，不要求每条触发独立启动 Run 或独立发一条消息。每条触发仍保留自己的用户、来源、取消/失败结果及回复关联。新近入队、尚未出现在该模型输入中的触发不能被顺手标为已回答。

对被取消、过期或明确终结的触发保留原因。其他用户后来加入的独立触发不因当前 Run 被取消而自动取消。可恢复故障可以继续原 Run，或由恢复器在提升所有权 epoch 后重新排队；不能仅因 `context_applied_seq >= trigger.ingress_seq` 就忽略它。

重排是显式 operation：持有效 actor 的恢复器必须指出失效的 `session_ref/run_ref` 与原因，事务锁定 lane 及该 Run 已领取的 trigger，写 `trigger_requeues` 和规范化成员后才把这些 trigger 从 `CLAIMED` 放回 `PENDING` 并释放 lane 的活动 Session 引用。失败或取消使用独立 `trigger_terminal_operations` operation，在相同精确 fence 下按不可变 `resulting_status` 把成员置为 `FAILED` 或 `CANCELLED` 并记录原因，不能借重排反复调用同一失败模型。相同 operation 精确重放同一成员集；不能扫描所有 `CLAIMED` 项后无条件重置，也不能把别的 Run 后来领取的 trigger 一并回退。

## 3. 上下文消息与用户身份

### 3.1 规范化消息 DTO

以下身份字段由可信 Adapter 和身份模块提供；正文里出现相同字段名没有控制意义。

```json
{
  "event_id": "evt-107",
  "bot_id": "bot-alice",
  "conversation_id": "qq-group-a",
  "ingress_seq": 107,
  "event_kind": "group.message",
  "platform_message_id": "qq-message-opaque-id",
  "author_snapshot": {
    "principal_ref": "host-issued-principal-ref",
    "platform": "qq",
    "external_user_id": "user-opaque-id",
    "display_name": "小林",
    "group_card": "小林同学",
    "identity_origin": "adapter_verified",
    "is_self": false
  },
  "sent_at": "2026-09-12T13:00:02Z",
  "received_at": "2026-09-12T13:00:03Z",
  "segments": [{"type": "text", "text": "刚才说的时间改成周五。"}],
  "reply_to": {
    "source_event_id": "evt-102",
    "platform_message_id": "qq-previous-message-id",
    "resolution": "verified_local_reference"
  },
  "trigger": {
    "trigger_id": "trigger-107",
    "decision": "TRIGGER",
    "rule_revision": "trigger-policy-3",
    "reason": "explicit_mention"
  }
}
```

历史身份以 `(platform, external_user_id)` 和可信 `principal_ref` 关联；昵称、群名片是当时快照。用户改名不改写过去记录；同名用户不能合并。需要匿名化时使用同会话稳定的展示别名并保留受限身份映射，不能让摘要只剩“用户说过”。

### 3.2 Provider 渲染与授权

Provider 支持结构化 metadata 时保留相应字段；只接受常规消息数组时，可把多个群成员发言编码成 `role=user` 的结构化消息列表，每条包含稳定用户标识、展示名、来源序号和内容。SDK 必须转义内容和展示名；用户正文不能伪造 `system / assistant / tool` 边界。平台事件的发送者是否是管理员属于授权事实，不能由消息文字或昵称推断。

引用与转发同时记录实际发送者、被引用作者和原来源是否已验证。转发中“管理员：批准执行”只是转发内容；即使原作者身份已核实，也不是对当前工具参数的有效授权。只有受权控制通道创建的确认记录可以批准具体操作。

当一个输入批次含多个用户的触发时，不合并这些用户的权限，不采用最强用户的权限替全部文本执行。每个工具意图指定 `authority_trigger_id / principal_ref`、具体请求来源、参数摘要和资源范围；宿主只使用该来源的有效授权与 Bot/插件能力的交集。无法确定授权来源的混合请求应按低权限处理或进入明确确认流程，不能让普通聊天片段附着在管理员触发之后获得权限。

归档、来源引用及显式任务产生的其他派生内容要保留“谁说了什么、在哪个群、针对谁、是否为引用、是否已撤回”的结构信息，不能把甲的偏好归给乙，也不能把群体猜测写成 Bot 已确认的事实。裁剪本身不产生摘要。长期记忆归属[10 的群主干](10-multi-trunk-memory-graph.md)，跨群召回必须实际向模型渲染来源群；主干的 source_order_seq 与本章的 Context 游标/commit_seq 分别保存，不能相互替代。

### 3.3 按LLM能力投影，完整上下文保留图片

CanonicalContext、归档及 Evidence 保存有序 text/image ContentPart 和逐 part 来源。新 Run 第一次调用 API 前，Agent 请求 Context 冻结已经按自身策略维护的 ContextView，再完成获权多模态召回，最后按实际模型能力构造独立 ModelInputProjection。Provider 只接收投影结果，不接收 Context 预算、裁剪配置或候选历史。不能先按文本 LLM 去图再把删后的文本当作记忆查询的唯一输入；图片仍可以直接用于嵌入和召回。

若实际模型不支持图片，递归去掉所有角色中的image parts以及被声明为媒体载荷的URL/base64/file字段，包括历史轮次、引用内容、检索证据与工具结果；保留合法文本、作者/群来源、工具call_id和结果配对。可以保留纯文本“此处有图片，未提供给当前模型”的事实标记，不把图片地址或编码转写成文本来假装已完成投影。明确来自原文的普通文字URL按文本处理，不靠泛化字符串删除破坏用户内容。

纯图片触发仍有作者、群来源、trigger和不可见模态标记，不变成空消息或自动ANSWERED。若召回得到合法文本证据，模型可根据这些文本作答；不能把图片相似度或占位标记当成视觉理解。没有足够文本依据时说明能力限制，不自动调用OCR/caption、切换收费模型或声称看到图中细节。已有显式生成且仍获权的OCR/描述文本可保留，但必须标明派生来源，不伪装为原图已传入。

投影只作用于当次API请求。它既不删除Context中的image part/媒体引用，也不改变归档、嵌入索引、主干归属或触发水位。后续合法视觉请求可重新从原始媒体物化图片；文本/视觉fallback每次新建attempt与manifest，仍使用原输入切口，不能借切换模型吸入排队尾部。详细能力、媒体生命周期及恢复契约见[11](11-multimodal-memory-and-model-projection.md)。

## 4. 触发切口选择与模型安全点

### 4.1 一次选择只冻结一个切口

在持有效 Actor 权限的短事务中锁定会话协调行，读取：

```text
A = context_applied_seq
H = classified_through_seq
P = 当前 PENDING 且 ingress_seq <= H 的触发集合
若 P 非空：T = max(P.ingress_seq)
trigger_cut_seq = T
selected_input_end_seq = max(A, T)
新来源区间 = (A, T]，当 T <= A 时为空
本次领取 = P 中不晚于 T 的全部待处理触发
```

提交的切口、领取的触发 ID 和批次 ID 固定。事务之后再来的触发属于下一次选择，不把已经冻结的 `T` 持续向后拖动。若存在序号大于 H 的已分类触发，必须等待它前面的分类缺口终结，不能越过缺口推入乱序历史。

`P` 的查询独立于 `A`，恢复时可能出现消息早已入上下文、触发仍待处理的情况。该触发使用既有上下文及恢复批次重新获得执行，不反向移动 A，也不重复追加原消息。正常新触发路径中的 T 大于 A。

正常路径的模型输入上界就是最后触发 T；仅恢复旧触发且 T 小于 A 时，实际已有历史的可见上界仍为 A，不能用旧触发序号假装此前已入库的历史不存在。trigger_cut_seq 记录本次触发选择，selected_input_end_seq 记录实际输入前缀；此差异不允许读取 A 之后仍排队的普通尾部。精确恢复原模型步骤时优先复用原 checkpoint 的冻结快照，不重新选切口。

新旧触发可以在同一次选择中混合出现。例如 A=10，同时有待恢复的触发 5 和新触发 12，则追加的来源范围只是 11..12，但本次 `run_inputs`、累计触发清单与原文保留引用必须同时包含触发 5 的既有来源。触发清单不等于新来源区间；不能只在“整个批次没有新消息”的分支恢复旧来源。待恢复触发若已移出活动视图，按当前授权从可靠归档取回并pin其必要原文；已删除的来源走明确取消/失败处理，不得复活。

输入推入事务同时执行：校验 Actor/Session/Run 所有权及权限/删除屏障；幂等校验；发布尚未进入 Context 的已闭合步骤；追加输入批次和来源引用；关联 `run_inputs`；领取触发；推进 A 和 `input_revision`；更新 Context revision 和持久待处理标记。事务只处理本地事实和引用，不在其中等待 LLM、摘要或工具。

### 4.2 模型与工具步骤的闭合

```text
固定输入视图 -> 一次 Provider 调用
             -> 无工具的完整输出
             -> 或：完整 tool-call 消息 + 每个调用的终态结果
             -> 持久 checkpoint
             -> 安全点：决定是否接入下一输入批次
```

模型请求进行中仅接收 Inbox 和分类结果。不能直接修改传给 Provider 的消息列表，不能在 assistant 的 tool-call 和对应 tool-result 之间插入新群聊输入。多个并行工具的结果按该模型步骤固定的调用顺序保存，不能按网络完成顺序拼接。

未完成、等待批准或结果为 UNKNOWN 的工具组尚未闭合，只有在结果确定或按已定义失败语义形成终态记录后，才进入下一次模型调用。授权确认走控制通道，不排在普通聊天队列后等待自己解除阻塞。

闭合步骤先由 `agent.runtime` 记录 ModelTurn/ToolCall 与 checkpoint，尚未发布到群 Context 的部分按稳定步骤序号保存。若确有下一次模型调用，Conversation Scheduler 在安全点先选择新 InputBatch；Context participant 先追加此前尚未发布的完整 `AGENT_STEP` 引用，再追加该批输入。若没有新模型调用，Agent 可以继续使用私有步骤构造最终候选。最终候选只能走 Conversation 的专用 reply UnitOfWork：它以同一 `logical_step_id` 引用 Agent ModelTurn，并同时创建 Delivery SendIntent；不能先作为普通步骤追加一次、再作为正式回答追加一次。逻辑步骤唯一键和 checkpoint 的已发布位置避免恢复重复追加。

中间工具交互是“已发生的执行事实”，不是已向群里发出的正常回复；失败或取消后的已完成工具记录保留执行状态与归属，不伪造一条成功回答。未完成模型流、半截文本和未经提交的最终候选只进入受限执行记录，不进入正式回复历史。这里持久化的是协议允许保存的输出与工具事实，不要求提取或保存 Provider 的隐藏推理过程。

### 4.3 没有新触发的工具回合

若当前 Run 仍需继续调用模型，而且根本没有 PENDING 触发，则保持现有输入版本，仅增加已闭合的执行步骤。若存在已分类触发，但全部位于连续分类头 H 之后，则转 WAIT_INPUT 等待缺口，保留当前 Run；不能把“没有可立即消费的触发”当作“没有新触发”继续调用模型。忙碌期间收到的普通尾部仍在 Inbox，不能因为“正好又要调用模型”就全部追加。

不能通过旁路 observer、额外检索或摘要刷新把排队尾部重新塞入本轮 Prompt。基于本会话新消息的记忆抽取至少等待该消息进入已提交 InputBatch；召回物化也必须检查来源，当前会话尚未推入的入站记录及其派生物不得进入该冻结输入。其他群的获准共享事实仍按 Bot 全局记忆规则参与召回。

## 5. 回复结束、静默排空与竞态

### 5.1 最终回答的原子提交

完整最终候选进入协调器主持的 participant UnitOfWork。它验证候选固定的输入版本和所答 trigger 集合：Agent participant 关闭准确 Run，Conversation participant 追加尚未发布步骤引用与正式 `REPLY`、把本次实际纳入的 trigger 标为 `ANSWERED` 并分配 `response_seq`，Delivery participant 创建 SendIntent。各模块只写自己的 schema。仅有新触发排队不丢弃已生成的最终候选；持续来消息不会无限延迟当前回答，新触发留给同一 Session 的后继串行 Run。

该 UnitOfWork 锁定 Conversation lane，并通过 Agent participant 核对 Session/Run fence 后决定后续：

- 有可用待处理触发：保留当前 `active_session_id`，转 `READY_NEXT`，由同一 Session 建立后继 Run，在其第一次模型调用前选择新的触发切口。
- 有尚在连续分类头之外的已分类触发：保留 Session，转 `WAIT_INPUT`，等待缺口终结。
- 没有待处理触发：转 `DRAINING`，只做静默输入维护；完成排空的事务再决定是否关闭 Session。

最终提交后到达的消息由相同协调行的事务先后关系决定：若 Session 尚在 DRAINING，新触发将其唤醒到 READY_NEXT；若已经关闭，则由持久待处理工作创建下一个 Session。两条路径都不能创建并发的同群 Session。

CHAT 回复分配同会话递增 `response_seq`，Delivery 依序领取，避免两个串行生成的回复在异步发送时倒序。较早 CHAT 回复 UNKNOWN 时保留未确定状态，并按明确的对账/超时终结策略处理后续 CHAT 发送；不能换一个 Agent 或新发送 ID 绕过顺序约束。独立 CONTROL 命令回执按 command_id/control_seq 排序，可先获得下一次允许的发送机会；两者仍共用平台限流、绑定所有权和实际网络容量，因此只保证各自排序域，不声称全部 Bot 输出共享一个严格总序。

### 5.2 静默推入的两种入口

空闲收到普通消息和 Agent 结束后的尾部排空使用同一个 `SILENT_DRAIN` 协议。每个有界维护批次都在协调行锁内先检查触发，再冻结 `silent_end_seq = min(classified_through_seq, context_applied_seq + 64)`，只追加这一段连续前缀并推进游标；结算释放 actor 后，仍有未消费消息就重新领取下一批。提交前按“现有活动单元 + 本批单元”检查 Context 高水位；越界时可靠归档至多 64 个旧单元并发布裁剪视图。若本批后仍高于水位，由 Conversation 自有的持久 Context 维护任务继续领取和归档，不把全部历史塞进一个事务。它不创建回复型 Run，也不调用 LLM。

如果检查时有待处理触发，立即改走触发选择，不能先把整条普通尾部静默推完。安全点之后新消息的序号大于已冻结边界，不影响已选择的输入批次；下一维护批次再次检查触发，不沿用上次“没有触发”的判断。

静默操作只保存输入、归档和确定性裁剪事实，不创建回复 outbox，不触发前台 Agent、摘要模型或自动工具。Bot 保持静默，下一次被触发时具有预算内近期原文及更早记录的可检索来源引用。被移出的内容需按实际需要召回，不能因没有摘要就假定旧内容仍在模型视图中。

### 5.3 空队列判断与丢失唤醒

关闭 Session 的事务必须在同一个会话协调锁内重验：

```text
没有 PENDING 或仍需恢复的 CLAIMED 触发
没有活跃/等待中的 Run 或未解决 continuation
context_applied_seq == classified_through_seq
没有本 Session 必须完成的输入准备/裁剪工作
```

然后才将 Session 终结并清空 `active_session_id`。不得使用“内存 queue.empty() 为真 -> await -> 把 Agent 标空闲”的流程。

若仍有已接纳但未分类完成的消息，可以结束当前回复 Session，但必须保留分类 Job 和持久待处理标记；它们完成时即使没有新的网络消息也会重新唤醒会话。后台恢复器持续检查 `context_applied_seq < classified_through_seq`、PENDING 触发、未完成 Session 和超时 lease，不依赖一次性通知。

入站事务、分类终结事务、Session 结束事务共享同一协调行的锁/CAS 规则，并更新 `mailbox_revision`。消费者只能清除自己已经确认处理完的版本，不能把处理期间出现的新版本标为已处理。通知与 outbox 重复或丢失只影响调度延迟，不改变输入覆盖或触发状态。

## 6. 严格串行的 Session 状态机

| 当前状态 | 事件与动作 | 后继状态 |
|---|---|---|
| `IDLE` | 仅普通消息；静默追加并运行裁剪维护 | `IDLE` |
| `IDLE` | 存在可处理触发；原子创建唯一 Session 与 Run | `PREPARING` |
| `PREPARING` | 提交选定输入，准备有界视图、完成必要裁剪 | `RUNNING` |
| `RUNNING` | Provider/工具正在工作；入站只排队 | `RUNNING / WAIT_TOOL` |
| `RUNNING` | 非最终安全点发现触发；同 Run 增加固定批次 | `PREPARING` |
| `RUNNING` | 非最终安全点没有触发；保留排队尾部，继续模型步骤 | `RUNNING` |
| `WAIT_TOOL / WAIT_APPROVAL` | checkpoint 后释放实际执行资源；保留逻辑 Session | 同状态，直到控制结果到达 |
| `RELOAD_WAIT` | 保存输入/步骤/触发归属，交接执行权限 | `READY_RESUME` |
| `READY_RESUME` | 同 Session 获取新 Actor/Run incarnation；复核 checkpoint | `PREPARING / RUNNING` |
| `RUNNING` | 最终 ModelTurn 提交，有后续触发 | `READY_NEXT` |
| `READY_NEXT` | 同 Session 创建后继 Run 并选择下个切口 | `PREPARING` |
| `WAIT_INPUT` | 连续分类头推进到可用触发，仍有原活跃 Run | `READY_RESUME`，恢复原 Run |
| `WAIT_INPUT` | 连续分类头推进到可用触发，上一 Run 已完成 | `READY_NEXT`，创建后继 Run |
| `RUNNING` | 最终提交后没有待处理触发 | `DRAINING` |
| `DRAINING` | 发现新触发 | `READY_NEXT / WAIT_INPUT` |
| `DRAINING` | 无触发，有普通前缀；静默追加 Context | `DRAINING` |
| `DRAINING` | 重验无待处理工作，原子关闭 Session | `IDLE` |

这里的 IDLE 是会话无活跃 Session 的状态；其余状态由持久 Session 协调。Actor 的内存实例可以淘汰，Worker 可以更换，逻辑 Session 身份仍连续。

`holds_actor = false` 只表示不持有实际执行/提交租约，不能解释为同群可以启动另一 Agent。WAIT_APPROVAL、长 WAIT_TOOL、RELOAD_WAIT 和公平调度让出 Provider 槽时，`active_session_id` 仍被占用。新触发仍正常持久排队；取消/批准经控制通道推进当前 Session。等待有 deadline、显式取消和故障终结机制，不能永久占住同群对话。

自主 Job 若要向正在工作的群发起交互，也必须提交该 Conversation 的受权触发并参加相同队列。后台记忆维护、索引和组件 RPC 可并行，但它们不是 Agent，不拥有该群的 Context 或回复提交权。

## 7. 伪代码与事务边界

以下是协议伪代码；`BotUnitOfWork` 只负责同一数据库事务、participant 顺序和 operation 回执，不暴露万能 Store，也不允许 participant 写入其他模块 schema。所有模型、工具、Adapter 和媒体 I/O 均在事务外。

```python
async def select_for_next_model_call(ids, actor_fence, operation_id):
    async with bot_uow.transaction(
        operation_id,
        participants=[conversation, agent],
    ) as uow:
        lane = await conversation.lock_lane_in(uow, ids.conversation_id, actor_fence)
        await agent.require_model_safe_point_in(
            uow, ids.session_ref, ids.run_ref, actor_fence
        )

        # Conversation 独占分类水位、trigger 和 Context 顺序。
        selection = await conversation.select_input_batch_in(
            uow,
            lane=lane,
            session_ref=ids.session_ref,
            run_ref=ids.run_ref,
            boundary="MODEL_SAFE_POINT",
            operation_id=operation_id,
        )
        if selection.kind in {"WAIT_INPUT", "CONTINUE"}:
            await agent.record_selection_receipt_in(uow, ids.run_ref, selection)
            return selection

        # 批次必为 (context_applied_seq, last_eligible_trigger] 的连续前缀；
        # 较后的普通尾部留队，旧来源 trigger 以关系表显式 pin。
        await conversation.append_complete_agent_steps_before_input_in(
            uow,
            run_ref=ids.run_ref,
            step_refs=await agent.closed_unpublished_step_refs_in(uow, ids.run_ref),
            input_batch_ref=selection.batch_ref,
        )
        await agent.attach_input_batch_in(
            uow,
            run_ref=ids.run_ref,
            batch_ref=selection.batch_ref,
            batch_digest=selection.batch_digest,
            trigger_refs=selection.trigger_refs,
            principal_refs=selection.principal_refs,
            actor_fence=actor_fence,
        )
        return selection


async def run_model_step(ids, actor_fence):
    saved = await agent.load_checkpoint(ids.run_ref)
    if saved.phase == "FINAL_READY":
        # 恢复时先提交原候选；新 trigger 留给后继 Run。
        return await commit_final(ids, saved.final_candidate_ref, actor_fence)

    if saved.requires_next_model_call:
        selection = await select_for_next_model_call(
            ids, actor_fence, operation_id=new_selection_operation(ids)
        )
        if selection.kind == "WAIT_INPUT":
            return await agent.release_execution_keep_session(ids.run_ref, "WAIT_INPUT")

    snapshot = await prepare_bounded_context_view(ids.run_ref, actor_fence)
    prepared = await provider.prepare_projection_and_transport(snapshot)
    permit = await agent.commit_model_dispatch_permit(
        ids.run_ref, prepared.manifest_ref, actor_fence
    )
    observation = await provider.dispatch_once(prepared, permit)
    closed = await agent.observe_and_checkpoint(ids.run_ref, observation, actor_fence)
    if closed.is_waiting:
        return await agent.release_execution_keep_session(ids.run_ref, closed.reason)
    if closed.is_final:
        return await commit_final(ids, closed.final_candidate_ref, actor_fence)
    return "SCHEDULE_SAME_RUN_NEXT_SAFE_POINT"


async def commit_final(ids, final_candidate_ref, actor_fence):
    operation_id = stable_reply_operation(ids.run_ref, final_candidate_ref)
    commit_plan = await runtime_graph.reply_commit_plan(
        required_contracts=["agent.finalize/1", "conversation.reply/1", "delivery.intent/1"],
        optional_contracts=["memory.committed-source/1"],
    )
    async with bot_uow.transaction(
        operation_id,
        participants=commit_plan.participants,
    ) as uow:
        final = await agent.verify_final_candidate_in(
            uow, ids.run_ref, final_candidate_ref, actor_fence
        )
        step_refs = await agent.closed_unpublished_step_refs_in(uow, ids.run_ref)
        reply = await conversation.commit_reply_in(
            uow,
            conversation_id=ids.conversation_id,
            session_ref=ids.session_ref,
            run_ref=ids.run_ref,
            final_payload_ref=final.assistant_payload_ref,
            model_turn_ref=final.model_turn_ref,
            answered_trigger_refs=final.trigger_refs,
            prior_step_refs=step_refs,
            operation_id=operation_id,
        )
        await delivery.create_intent_in(
            uow,
            cause_ref=reply.context_unit_ref,
            reply_route=reply.reply_route,
            lane="CHAT",
            sequence=reply.response_seq,
            parts=final.reply_parts,
            operation_id=operation_id,
        )
        if memory_source := commit_plan.optional("memory.committed-source/1"):
            await memory_source.enqueue_in(
                uow, source_ref=reply.context_unit_ref, operation_id=operation_id
            )
        await agent.complete_run_in(
            uow, ids.run_ref, final.checkpoint_ref, actor_fence
        )
        await conversation.choose_post_reply_state_in(
            uow, ids.conversation_id, ids.session_ref, ids.run_ref
        )
        return reply


async def silent_drain_or_close(conversation_id, actor_fence, operation_id):
    async with bot_uow.transaction(
        operation_id,
        participants=[conversation],
    ) as uow:
        result = await conversation.select_input_batch_in(
            uow,
            lane=await conversation.lock_lane_in(uow, conversation_id, actor_fence),
            boundary="SESSION_END",
            operation_id=operation_id,
        )
        if result.kind == "TRIGGER_INPUT":
            return await conversation.schedule_trigger_work_in(uow, result)
        if result.kind == "SILENT_DRAIN":
            await conversation.apply_silent_batch_in(uow, result.batch_ref)
            await conversation.register_deterministic_compaction_in(uow, result.batch_ref)
            return "CONTINUE_MAINTENANCE"
        await conversation.close_idle_lane_in(uow, conversation_id, actor_fence)
        return "IDLE"
```

`BotUnitOfWork` 在开始前冻结 participant 列表、contract version、调用顺序和输入摘要；重试复用同一 operation。Conversation 不能通过该对象写 Agent/Delivery 表，Agent 也不能直接回答 trigger。进程内 `asyncio.Lock` 只减少同进程竞争，数据库唯一约束、lease、epoch 和 participant 自己的最终检查才是恢复后的权威。

## 8. 无模型摘要的裁剪与历史顺序

### 8.1 持久 Context 维护与 Run 模型视图冻结

持久 Context 维护不依赖 Agent 是否正在工作。`SILENT_DRAIN` 与新 Run 首次模型调用前的裁剪在提交后按 Context 自身策略检查活动单元；越过高水位就以至多 64 个单元的事务批次可靠归档。空闲且无待处理触发时，包自有维护任务依据持久 Context/视图修订持续领取，直到降到策略水位或没有可归档单元；配置 Generation 改变也重新检查已有群会话。活动视图由事务内 `active_context_units` 索引读取，完整原文仍在不可变 `context_units` 与 Archive 中可追溯。

新 Run 准备第一次模型调用时冻结当时的持久 ContextView，并加入本 Run 当前 RAG、触发输入和必要因果后缀。这里的“一次”指每个 Run 只做一次模型视图选择，不表示空闲期停止持久裁剪。同一 Run 后续模型、工具、思考、修复以及安全点接入消息只调用 `extend`，不得重新选择历史前缀；活动 Run 期间也不发布会改写该冻结前缀的后台裁剪。Run 的步骤数、工具数和排队接入量由 Agent 自身上限约束。

持久 ViewPlan 按明确 `context_view_profile_ref` 管理完整多模态近期窗口、归档引用和本地资源预算，不随当前 LLM 是否支持图片而改变保存语义。Context 是历史选择与裁剪的唯一所有者；Provider 只按目标能力序列化已经定稿的 ModelInputProjection，不能读取 `context_view_profile_ref`，也不能删减历史或 RAG 证据。模型或能力变化只重建当次能力投影，不把去图结果写回 `context_compactions`。正常持久裁剪仍可按既定保留策略整体移出已归档旧单元，但“不支持模态”不能成为删除原图或永久移出图片的理由。

确定性选择先计固定开销与必留集合，再按 Context 提交顺序从近到远选择完整可选单元；同序号按稳定 unit_id 决胜，不按 Worker 读取完成先后选择。ViewPlan 使用 `context_view_profile` 的估算算法和高低水位，触发裁剪后以配置低水位为目标；第一个可选旧单元放不下时停止扩展旧窗口，整组移出，不切断 call/result 配对。预算估算只在 Context 内按固定版本完成，不调用远端模型，也不进入 Provider manifest。

完整归档覆盖清单保存在事实库；Prompt只保留有界的归档提示及manifest引用，不随历史增长逐条累积占位。多个省略范围可引用同一不可变manifest，准确映射仍可按权限查询。证据来源头、近期原文和必要原文全部计入实际预算，不能因引用在数据库里而声称对应正文已进入模型。

选定前缀可能超过模型窗口。允许用已持久 Inbox 的不可变范围引用和保留引用计数提交 InputBatch，随后有界流式读取、确定性分块和可靠归档；按固定tokenizer/策略计算必要保留集合，再选择预算内近期完整单元，移出旧的可淘汰单元并发布视图。裁剪不调用LLM，也不通过压缩、整合、摘要Job等别名间接调用模型。来源游标覆盖整段前缀，不能只持久化最后若干条；模型实际只看到了视图物化清单里的正文。

正常推入/裁剪保留：

- 当前仍待回答的触发及其稳定用户身份、原始请求引用、必要原文和图片引用，不能被普通滚动窗口直接淘汰；图片不被文本模型接收的事实只记在该请求omitted清单中。
- 被归档普通消息的连续来源覆盖、参与者/来源群关系和可检索的原文引用；省略标记只表明范围已移出，不伪造自然语言概括。
- 一个完整工具因果组，不能只保留 tool-result 却删去对应 tool-call，或相反。
- 删除、撤回及不可读取内容的合法占位，不保留被禁止内容的残片。

若 Context 判断仅待回答触发的最小必要原文就超过其策略硬界，不能偷偷遗漏早期触发或无限等待。记录明确的 Context 裁剪失败并按有界恢复/失败策略处理，保留每个触发状态；切口不被悄悄改成最后一条消息。若只是 Provider 配置中的估算值超限，仍应发送已定稿请求，由上游真实结果裁决。正文超过传输层硬字节限制时使用已声明的附件/内容引用策略，不能伪称模型已读完未提供的原文。

### 8.2 确定性视图发布与合流

先可靠写 Archive 及来源映射，再以 `context_compactions` 发布裁剪视图。裁剪器只计算确定性的 ViewPlan：保留单元、移出单元、归档引用、token计数和来源覆盖，不能输出自然语言摘要。Context 发布经Actor/会话协调事务；后台任务不能修改正在使用的共享消息数组。可靠归档已完成但Embedding/图邻接仍滞后时，裁剪可以完成，召回显式报告索引水位。

ViewPlan固定有序源集、覆盖范围、基础context_view_revision、context_view_profile_ref、tokenizer/策略版本、预算、必留触发/因果组及deletion epoch。发布时核验所有依赖；老计划晚完成不得覆盖新输入或新视图。若只裁剪已声明前缀，必须证明保留当前完整后缀，且整体预算仍合法，否则重建计划；不能覆盖旧的整段Context快照。相同来源、策略、预算和必留集合得到相同保留/移出语义，不发生LLM工件选择。ModelRequestManifest 只记录能力投影，不产生第二套历史保留/移出清单。来源/删除屏障仍遵守[06](06-concurrent-convergence.md)。

当前基线以 `context_view_unit_count/context_archived_unit_count` 避免为判断阈值反复扫描完整历史。一次发布先写受管 `context.archive` JSON 引用，再写 `context_compactions` 和精确 `context_compaction_units` 成员关系，最后单调推进 `context_view_revision`；原 `context_units` 与 Payload 不更新、不删除。快照用反连接只物化活动单元，并在最前面加入最新机器可读归档引用。新 Run 的触发输入和当前必要后缀受保护；ViewPlan 发布后，该 Run 在所有后续模型步骤保持同一 `context_view_revision`，只追加因果单元。

新 Session 的首个 Run 在 `agent.execute` 选择触发 InputBatch 之前，由 `conversation.scheduler` 持有 Actor 围栏执行 `conversation.compact-before-run`。这个独立 UnitOfWork 使用 Conversation 包自身的视图策略，按“确定性计划 → 归档 Payload → 视图提交 → Memory 归档资格”顺序原子提交；它不虚构 InputBatch，也不把裁剪策略交给 Agent 或 Provider。已提交的归档按操作身份重放；若重试时 Session 已启动且本操作未归档，则跳过新裁剪，由 Agent 读取已经存在的 Run。随后 Agent 在同一 Actor 围栏下选择输入，`context_view_revision` 因而是裁剪后的版本。Run 中途的模型安全点只接入新输入，不重新选择历史窗口；后续 Run 的 Session 交接仍须明确设计安全裁剪边界，不能沿用前一个 Run 的固定视图假装已经裁剪。

高流量下合并同会话尚未执行的维护唤醒，维护desired/processed水位；合并只影响本地工作批量，不能遗漏任何来源。采用裁剪高/低水位避免每条消息反复重算，大对象使用缓存token计数与受监督本地计算。系统指令、作者/群来源标题、必留内容和输出预留先计费；超窗时减少普通原文/Evidence项，不能剥掉来源标题凑预算。待答触发必要原文仍放不下就走上述明确失败策略，不自动发摘要请求，也不自动调更昂贵模型。

当前会话的 `context_applied_seq` 是串行前缀消费，不是可交换的消息集合归约；顺序由持久入站和已记录切口确定。不同物理到达时机可能产生不同批次和回复，这是交互调度语义。持久重放必须复用已记录批次、步骤和回复，不重新挑选切口或重新生成已完成回答来追求所谓合流。

### 8.3 两条时间轴不能混淆

模型历史按 Context 的 `commit_seq / unit_index` 物化；每条输入仍携带原 `ingress_seq` 和平台时间。例如用户消息 13 在模型工作期间已经到达，但只有普通尾部、当时未触发，则模型先基于截止 12 的输入产生回复 R，之后静默推入 13。Context 为 `输入至12 -> R -> 输入13`，同时保留 13 的真实接收时间；不能把 13 倒插到 R 前面，伪造模型已经看过它。

在工具安全点接入时顺序则为 `旧输入 -> 完整tool-call/result组 -> 新输入前缀 -> 下一模型调用`。用户界面可以另按平台时间展示实际群历史，但必须区分群时间线与模型当时看到的上下文。

### 8.4 自身消息回声

Bot 已提交的最终回复通过 ModelTurn 入 Context；平台再次推送同一发送的 echo 时，用 SendIntent/平台消息关联去重，不追加第二份相同 Bot 发言，也不再次产生触发。echo 入站若已经分配序号，保留“关联既有回复”的来源占位，使连续游标仍能前进。

由该账号其他合法路径发出的新消息可以按已声明策略记录为 `self_external`，不能未经验证冒充本 Agent 的成功 ModelTurn。来自其他 Bot 的消息保留实际作者身份，是否触发遵守独立策略、因果链和 hop 限制，避免双方自动回复形成无限循环。

## 9. 时序示例

### 9.1 忙碌时取到最后一个触发，保留普通尾部

假设开始时 `context_applied_seq = 100`，用户甲在 101 触发。表中“*”表示触发消息。

| 时刻 | 已发生的事件 | 提交与下一模型所见 | 仍排队 |
|---|---|---|---|
| t1 | 101* 用户甲：“查一下周末天气” | 输入批 B1 覆盖 101，领取 T101，M1 开始 | 空 |
| t2 | M1 工作；102 乙普通聊天，103* 丙补问 | 当前 M1 的输入不变 | 102、103* |
| t3 | 104 甲补充，105* 乙更改地点，106 丙普通聊天 | 当前模型/工具组尚未闭合 | 102、103*、104、105*、106 |
| t4 | M1 的工具组闭合，进入安全点 | 先存完整工具组；B2 冻结 T=105，追加 102..105；T103/T105 被领取；M2 可见两位新触发者 | 106 |
| t5 | M2 又调用一次工具，期间仅来 107 普通聊天 | 无 PENDING 触发，M3 沿用输入截止 105，不能把 106/107 带入 | 106、107 |
| t6 | M3 产生最终回复，事务正常提交 | T101/T103/T105 关联该回复；Session 转 DRAINING | 106、107 |
| t7 | 静默维护 | SILENT_DRAIN 追加 106..107，A=107；按 Context 水位归档/裁剪但不调用模型，无新触发后关闭 Session | 空 |

### 9.2 最终候选已完成时仍有触发

| 时刻 | 事件 | 结果 |
|---|---|---|
| s1 | R1 的输入截止 201；模型生成完整最终候选 | 该候选只关联它实际看过的触发 |
| s2 | 最终提交前，202 普通消息、203* 新触发、204 普通消息完成分类 | 不创建并发 Session，不自动废弃 R1 |
| s3 | 最终事务提交 R1 | 产生 response_seq=k，旧触发 ANSWERED；203 仍 PENDING，Session 保留 |
| s4 | 后继 Run 的下一输入选择 | B2 推入 202..203，204 留队；开始下一模型调用 |
| s5 | 后继回复提交 | response_seq=k+1；Delivery 保持 k 在前 |

若 203 在 s3 的事务之后才到达，仍由会话协调行的事务顺序唤醒同一尚未关闭 Session，或在关闭后创建下一个串行 Session；不会丢失该触发。

## 10. 崩溃、取消与热重载

| 故障位置 | 必须恢复的行为 |
|---|---|
| Inbox 已提交，通知之前崩溃 | 分类 Job/待处理扫描找回消息；不依赖网络重发 |
| 后面的消息分类完成，前面 Worker 死亡 | 重新领取缺口 Job 或写明确失败终态，推进连续分类头并唤醒 |
| InputBatch 提交前崩溃 | 事务整体回滚，来源和触发仍可选择 |
| InputBatch 与 CLAIMED 已提交，模型尚未调用 | 恢复同批次、同输入版本和触发归属；消息不重复，触发仍待回答 |
| 模型调用返回但 checkpoint 未提交 | 未承诺回复；可按模型调用恢复策略重新计算，已发生工具效果必须查原 operation |
| 完整工具组 checkpoint 后，InputBatch flush 前崩溃 | 从 checkpoint 恢复并按固定步骤序号发布，再接入新消息 |
| 最终候选的 FINAL_READY checkpoint 已提交，最终回复事务前崩溃 | 先按原候选/输入版本完成最终事务，不先选择新触发、不重调模型；新触发留给后继Run |
| 最终 ModelTurn/outbox 事务中崩溃 | 全部未提交，或全部已提交；不出现有回答历史却无回复意图 |
| 回复已提交，Session 尚未排空时崩溃 | 不重跑旧回答；继续 pending 触发或静默排空 |
| 静默追加后裁剪候选未发布 | 消费游标保留，维护 Job 重试；不能因“已消费”而忘记完成视图维护 |
| Actor/模块热替换，旧 Provider 迟到 | 旧 epoch 提交失败；新 Worker 按 checkpoint 和有效输入继续 |

取消针对明确 Run/触发范围，并与最终提交在同一守卫边界竞争。取消赢得事务后不接受旧模型回答；**已经提交的 InputBatch 仍是群里实际发生过的消息，不能随 Run 回滚。** 已发生工具效果留在账本，外部 UNKNOWN 继续对账。剩余未取消触发继续串行执行；没有剩余触发时执行静默排空。

热重载交接保存 Session 身份、触发领取关系、输入批次、输入/视图版本、步骤发布位置和工具 operation ID。新代接管只更换执行 incarnation 并提升 fencing，不新建第二个同群 Agent，不给已回答触发换 ID 重做。持久协调属于框架会话服务，不能只藏在可热重载插件的 Python 对象中。

`persist_closed_step_checkpoint` 对完整最终候选记录 FINAL_READY。恢复调度先按 checkpoint phase 分发，再考虑新输入：最终候选完成原事务，等待工具继续查询原操作，只有尚需下一模型步骤的执行才进入 select_at_safe_point。候选若因当前取消/权限/删除校验失效，则显式终结或重新规划，不能把它绑定到新输入版本蒙混提交。

## 11. 验收场景

实现阶段使用假 Provider、假平台和真实数据库事务，以 barrier 控制交错；以下是设计验收要求，当前文档不声称已经执行这些测试。

| 编号 | 场景 | 通过条件 |
|---|---|---|
| GI-01 | 空闲群连续发 100 条未触发消息并跨过裁剪阈值 | 全部来源被覆盖，正常归档/裁剪；所有LLM调用（含摘要）与发送次数均为0 |
| GI-02 | 普通消息后出现一条触发 | 首次模型视图含预算内普通原文、明确归档范围引用及触发必要原文，作者/群来源可区分，无裁剪摘要调用 |
| GI-03 | 忙时到达普通、触发、普通、触发、普通 | 下个安全点恰好推到第二个触发，最后普通尾部未进入该模型输入 |
| GI-04 | 只有普通尾部，但 Agent 连续运行多个工具/模型步骤 | 每步保留原输入切口；工具循环、召回和摘要旁路都不能偷带尾部 |
| GI-05 | 当前工具闭合后仍未最终回答，有新触发 | 同 Run 下一模型调用接入新批次，无须等待完整回答，无第二并发 Agent |
| GI-06 | tool-call 未闭合时到达新触发 | 只排队；Context 中 tool-call/result 配对完整，之后才插入新输入 |
| GI-07 | 多个工具乱序返回 | 重放和正常执行的步骤次序一致，不按网络完成顺序排列历史 |
| GI-08 | 最终候选已完成，又有新触发 | 当前完整回答正常提交；新触发留给同 Session 后继 Run，不被错误标 ANSWERED |
| GI-09 | 最终回答完成，仅有普通尾部 | 自动静默排空且裁剪，不额外生成回答；队列最终为空 |
| GI-10 | 新触发分别在关闭事务前、事务后到达 | 前者保留/唤醒当前 Session，后者创建下一 Session；都只有一个逻辑所有者且无丢请求 |
| GI-11 | 新触发在静默维护批次之间到达 | 下一批先选择触发切口，其后普通尾部不被一次旧的“无触发”判断排空 |
| GI-12 | 第三条先分类为触发，第二条分类阻塞 | 不跳过第二条；缺口终结后即使没有新网络消息也能唤醒并正确消费 |
| GI-13 | 提交 InputBatch 后立即杀 Worker | 新 Worker 复用输入批次；CLAIMED 仍被处理，不因消费游标越过它而丢触发 |
| GI-14 | Run 被取消，随后旧 Provider 返回 | 原消息保留；旧回答不可提交；其他独立 pending 触发仍可串行处理 |
| GI-15 | WAIT_APPROVAL、长工具等待或 RELOAD_WAIT 时收到触发 | 资源可让出，逻辑 Session 不让出；确认能经控制通道完成，同群无第二响应 Agent |
| GI-16 | 两群和两个 Bot 同时处于慢调用与消息洪峰 | 各群独立排队且获公平配额；记忆按 Bot 隔离，同 Bot 全局事实仍按 06 合流 |
| GI-17 | 昵称相同、用户改名、引用另一用户、伪造角色分隔符 | 历史/归档/召回主体正确；正文不能伪造Provider角色或授权主体 |
| GI-18 | 管理员触发与普通用户危险请求进入同一批次 | 工具只使用真实来源的授权；不能通过混合输入获得权限并集 |
| GI-19 | 选定长前缀远超窗口 | 有界分块归档与完整来源覆盖，活动视图按预算保留原文/引用；必要触发保留或明确超额失败，裁剪LLM调用为0 |
| GI-20 | 新输入/遗忘提交后旧裁剪候选才返回 | 源集/视图/删除屏障拒绝旧覆盖；被删内容不通过旧ViewPlan或重放复活 |
| GI-21 | Bot 自己的发送 echo 被重复接收，或另一 Bot 自动回复 | 自身回复不重复进历史、不自触发；其他 Bot 策略及因果上限有效 |
| GI-22 | 静默追加后崩溃，或通知丢失/重复 | 维护和待触发工作由持久扫描找回；消息、批次和回复意图不重复 |
| GI-23 | 先生成回复 R，之后才静默推入早已到达的普通消息 | 模型历史维持真实可见顺序，来源时间仍保留，不倒插伪造模型所见 |
| GI-24 | 两个串行 CHAT 回复的网络发送有延迟或 UNKNOWN | response_seq 保持发送策略规定的顺序；不以并发发送或换 ID 消除不确定性；CONTROL 独立排序见09 |
| GI-25 | A=10，待恢复触发 5 与新触发 12 同批选中 | 只新增 11..12 的 Context 来源，同时关联并保留 5 的原来源；两个触发都进入本次待回答清单 |
| GI-26 | final checkpoint 已提交后，在最终回复事务前后分别重启 | 通用 flush 不追加 final；同 logical_step_id 至多一个正式最终 ModelTurn 及一份逻辑发送意图 |
| GI-27 | Agent 忙时普通消息、管理命令、普通消息依次到达，无新聊天触发 | 管理按权限独立执行；三条来源仍留队直到静默排空，不额外调用模型 |
| GI-28 | 控制先成功，但前面的普通消息分类仍未完成 | 控制回执可发送，Context 水位不跨缺口；后续接入保留命令作者和安全投影 |
| GI-29 | 高流式普通消息反复跨裁剪阈值，关闭摘要Provider并让Embedding不可用 | 接纳/可靠归档/确定性裁剪仍进展，裁剪LLM调用为0，索引故障独立可恢复 |
| GI-30 | 当前模型未接入的同群尾部，经另一群主干/共享实体形成可遍历路径 | Recall逐端点和lineage过滤，不能经跨主干联想把尾部带进冻结输入 |
| GI-31 | 纯图片触发，LLM只支持文本 | 触发/来源/原图保留；可先以图召回，API只含合法文本和未见图标记，无媒体载荷/隐式OCR或caption |
| GI-32 | 历史、跨群Evidence与tool-result同时含图，Provider降级为文本 | 各层投影去图且工具配对/群来源完整，新attempt固定原切口和所见manifest，不改完整Context |
| GI-33 | 下载/图像索引在请求冻结后完成 | 旧请求不补图；后续合法安全点可选新媒体revision，同群不另开Agent |
| GI-34 | 文本模型运行和多次裁剪后切回视觉模型 | 文本请求省略不造成持久删图；仍在合法窗口或获权召回的原媒体可被视觉请求物化 |
| GI-35 | 130 条已分类消息中，第 30 与 130 条分别由不同用户触发 | Context 以 64 条为上限预填，预填不领取触发；最终批次仍包含第 130 条，两个触发及其各自身份共同归属同一 Run |
| GI-36 | 预填事务已提交而调用响应丢失，随后新消息又入队 | 相同 operation 重放原批次；新 operation 从已提交游标继续，最终切口只在下一次合法安全点冻结 |

除上述场景外，性质检查应持续验证输入范围恰好覆盖连续前缀、触发状态不由消费游标推导、单群唯一 Session/有效提交者、所有输出可追溯到固定输入批次。可交换的 Bot 全局记忆操作另按[06 的性质验收](06-concurrent-convergence.md#10-性质验证与并发时序验收)执行，不能把消息顺序、授权仲裁或模型文本误当作可交换更新。
