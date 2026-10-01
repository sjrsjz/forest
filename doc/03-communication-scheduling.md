# 通信与调度详细设计

## 1. 通信规则

组件不互相 import 实现，不通过共享变量、原始对象或 WebSocket 传递业务请求。所有跨组件通信使用版本化 DTO；同 Worker 优化也必须保留相同的验证、身份、deadline 和观测语义。

| 模式 | 用途 | 投递与完成语义 |
|---|---|---|
| Query | 读取记忆、查询工具结果、能力发现 | 有界 request/reply；可重试，但不同重试可能读到不同 revision |
| Command | 修改配置、提交轮次、请求受控操作 | 有 `operation_id`；接收成功与业务完成分开，持久命令可查询状态 |
| Event | 已提交事实通知，例如 `memory.operation.accepted` | outbox 至少一次；每个订阅独立 ack、重试与消费位置 |
| Stream | 模型增量、进度、可丢弃预览 | 有序且有界；断开不等于撤销业务，终态另行持久化 |
| Effect | 请求宿主存储、通信、外部调用 | 校验能力与资源范围；按操作类别选择事务/意图账本 |

Event 是已经发生的事实，不接受“消费者必须全部成功，否则撤销源事实”的隐式事务。业务需要多步骤一致性时使用显式 Saga/Job 状态机及补偿记录。

## 2. 信封与类型系统

```json
{
  "wire_version": 1,
  "message_id": "msg-opaque-id",
  "kind": "command",
  "contract": "memory.assert/1.0",
  "schema_digest": "sha256:example",
  "operation_id": "op-stable-across-retries",
  "input_digest": "sha256:canonical-payload",
  "correlation_id": "run-opaque-id",
  "causation_id": "inbound-message-id",
  "trace_id": "trace-opaque-id",
  "bot_id": "bot-alice",
  "conversation_id": "conversation-group-a",
  "run_id": "run-opaque-id",
  "sender": {"component_id": "agent.runtime", "generation_id": "gen-a"},
  "target": {"service": "memory.assert", "contract_major": 1},
  "deadline_at": "2026-09-12T13:00:30Z",
  "attempt": 1,
  "auth_context_ref": "opaque-host-issued-handle",
  "payload": {"subject_id": "principal-x", "predicate": "preference", "value": "tea"}
}
```

示例中的身份字段是宿主封装后的可信信封。来自插件的同名字段必须丢弃或与宿主值校验一致；不能接受插件任意拼接 `auth_context_ref`。

`message_id` 标识一次传输消息，`operation_id` 标识一次逻辑操作，重试只改变 attempt/message_id。相同 operation_id 与不同 input_digest 返回 `IDEMPOTENCY_CONFLICT`。operation_id 在同 Bot 和操作类型内唯一，不能以随机的新 ID 规避幂等。

同源平台事件使用 `(binding_id, platform_event_id, event_kind)` 去重。跨 Bot 收到同一群消息是两个独立事实，不共用去重域。无稳定事件 ID 的 Adapter 必须声明较弱保证，提供短窗口指纹去重且记录碰撞可能性，不能把时间戳当严格唯一键。

DTO 用显式 schema；禁止 pickle、任意 Python 类名和可执行表达式。小消息直接编码，大媒体使用有 scope、摘要、大小和过期策略的 `MediaRef`。默认信封上限 256 KiB，单字段与嵌套深度也要限制。

### 2.1 契约版本

契约 major 表示不兼容变更；minor 仅增加可忽略字段或有默认值的可选能力。新增枚举值是否兼容由 schema 明确，不能假设所有消费者能忽略。包版本与服务版本分别管理。

注册表按 `(scope, service, contract_major)` 解析，返回短寿命路由句柄；请求入场后绑定确定 Generation。模块只缓存服务名及契约，不能长期缓存目标实例。跨 major 转换由显式 Adapter 完成，禁止运行时悄悄改写语义。

### 2.2 多模态内容与受管媒体

规范消息和工具/记忆结果使用有序ContentPart，至少支持text和image，part_id及part_index随来源固定；不把整个消息拼成一段含图片URL的字符串。长期图片由ManagedMediaRef定位Bot域内不可变media_revision，临时平台URL只是获取凭据/定位信息，传输授权句柄与持久媒体身份分开。媒体获取、验证和派生在受监督有界任务中完成，具有deadline、大小/像素/数量限制与明确失败状态。

Provider和Embedding服务使用不同能力契约：Embedding必须能接收原始图文并按兼容向量空间查询；Provider只接收按其实际模型能力生成的ModelInputProjection。文本Provider的请求不得携带image parts或媒体载荷字段，投影不会改写原始消息。大小受限IPC只传类型化引用，最终外部请求由获权网关物化对应媒体并再次检查来源/删除/请求切口，详见[11](11-multimodal-memory-and-model-projection.md)。

## 3. 路由与执行

Router 的顺序固定为：

```text
验证 wire/schema/size
→ 绑定可信 bot/principal/generation
→ 验证能力与 target scope
→ 检查 deadline、预算与 admission 状态
→ 解析服务版本及代际
→ 领取有限调用槽并生成 invocation token
→ 投递 Worker
→ 校验规范结果/错误
→ 释放调用槽、资源与追踪状态
```

持久化或外部效果仍在最终执行边界重新校验权限与 fencing。服务注册成功不等于获得全部底层能力。

跨进程首版采用长度前缀帧的异步本地 IPC；握手使用宿主生成的一次性 Worker 身份，绑定 instance/generation。若用本机 TCP，必须认证且不接受外部连接，传输介质不构成授权。远程 Worker 后续增加传输认证，线协议语义不变。

调用树有 `parent_call_id`、深度上限和共享 deadline。同步服务依赖环在声明和运行时都拒绝；持有独占事务锁时禁止进行模型、工具或其他不受控 RPC。模块停止新顶层请求后，仅允许原调用树必要的子调用完成排空。

## 4. Inbox 与会话顺序

### 4.1 规范身份

```text
ConversationKey = (
  bot_id,
  binding_id,
  platform,
  channel_kind,
  channel_id,
  thread_id_or_none
)
```

私聊和群聊使用不同 channel_kind；平台用户、群和消息 ID 都是带命名空间的不透明字符串。绑定重连只改变 connection_epoch，不改变 ConversationKey。

### 4.2 先接收排序，再异步处理

入站事务去重成功后，在该 Conversation 的窄计数器行上分配 `ingress_seq`，写 Inbox。顺序定义为 Forest 持久接收顺序；平台重排的原始时间仅作元数据，不倒插已提交会话历史。

Pipeline 分别保存 `context_disposition`（可读正文/受限占位/无效格式）与 `trigger_decision`（触发/不触发/命令已消费/分类失败）。所有已接纳的正常群消息，无论是否触发 Agent，都进入持久上下文流水；命令 `Pass/Consumed` 不能删除消息历史。不可读取的正文保留有来源的占位，不能因跳过触发形成 sequence 空洞。

分类可并发执行，完成结果按 ingress_seq 推进 `classified_through_seq`，表示截至此处每条记录均已完成分类。预处理使用持久 Job 的 lease、重试上限和 deadline；Worker 死亡后恢复器重新领取，耗尽预算则记录分类失败并推进连续水位。耗时联网工具不进入分类阶段。

同群只有一个活跃 ConversationSession。新消息持续写入队列，不对每个触发另开 Agent：在下一次模型调用的安全边界，取已连续分类前缀中最后一个待触发记录为 `selected_input_end_seq`，按原顺序追加截至该条的全部尚未入 Context 的群消息，并领取对应触发。其后的非触发尾部不注入当前模型；没有新触发的工具继续回合也不自动注入尾部。

Agent 结束且没有待处理触发时，剩余消息按有限批次静默写入 Context 并触发正常裁剪，不调用对话 Agent、不创建回复；即使群从未触发 Agent，空闲时也执行此路径。每批的固定切口、写入、游标推进与触发 claim 形成持久操作，退出空闲/结束检查与入站唤醒在同一会话协调记录上串行化。完整规则与竞态见[08 群聊交互设计](08-group-context-interaction.md)。

取消、工具确认及管理 Shell 走受权控制通道，不等待正在工作的 Agent、Context 写租约或前面的异步分类缺口；等待时仍保留 Session 所有权，控制操作不创建另一条同群 Agent 响应链。涉及 Context 的控制效果通过该状态所有者的短事务或安全点执行，不能因此并发改写 Prompt。

接入顺序为 `trusted normalize → (Inbox + ShellGate route 同事务持久化) → business filter/command/agent selection → observe`。ShellGate 是稳定内核中的有界确定性检查，在任意插件代码之前认领 `#forest` 保留命名空间；完整解析和管理实现可由热替换模块承担。识别控制命名空间后即固定 `command_route=CONTROL, trigger_decision=CONTROL_COMMAND`；未知命令、参数错误、无权请求或解析模块离线都不回落 Agent。核心救援子命令不依赖该模块。控制路由与历史消费是两个决策：控制已执行也不跳过更早的 ingress_seq，连续水位只约束历史管线。

接收平台帧的 Adapter 若每个在途帧都可能调用宿主服务，其 Worker `max_state_inflight` 至少要覆盖 `max_inflight`；否则内部 RPC 配额会在接入前直接拒绝第二条消息，即使输入队列尚有空间。官方 OneBot Adapter 两者均声明为 16。要采用更窄的宿主 RPC 容量时，必须先在接入边界建立有界等待和可靠重投，而不是把 `OVERLOADED` 抛回无回放保证的实时连接。

普通 Pipeline 的纯处理器输出新的不可变 DTO/决策，禁止修改共享消息。每个允许阶段内按 `handler.priority` 高值优先、同值 component_id 字典序执行，部署时固定排序快照；插件不能凭任意高值覆盖保留前缀或提升级别。普通命令冲突默认拒绝启用，显式配置选择策略后才可共存。旁路 observer 不享有前台自动回复权，另行发送必须申请独立受控意图。命令注册、鉴权、错误回执及历史安全投影见[09](09-group-shell-command-system.md)。

同会话并不等于整个 Bot 加锁：Actor 的局部单写者只约束该会话 Context，不限制群乙读取或追加同一个 Bot 的长期记忆。

## 5. 公平性、背压与资源预算

服务类别由宿主授权后的 CommandSpec/任务类型确定，数值越小越优先：P0 紧急控制与批准/拒绝，P1 普通管理，P2 业务命令，P3 Agent，P4 后台维护。类别内先按 Bot 权重公平选择，再在 Bot 内按主体/资源队列轮转；Agent 队列以 Conversation 为单位。宿主为控制预留少量入口、执行、数据库和短回执容量，业务不能耗尽这些槽；控制也有自己的并发、速率、deadline 和积压上限。类间采用有界优先和最低服务份额，持续管理流量不能永久饿死聊天或裁剪。

任务领取同时满足宿主、Bot、组件和 Provider 配额；等待某 Provider 不占满宿主槽。`handler.priority` 是同阶段匹配顺序，`service_class` 是准入后的资源调度级别，资源锁/CAS 则裁定真正的提交顺序；三者不能互相替代。P0 不抢占已经许可的网络调用、不夺取生命周期操作租约，也不回滚已提交事实；仅先处理可独立提交的取消/维护意图，由目标所有者在适用边界执行。队列 aging 可以改善等待，不能提升权限或把任意插件改成核心救援服务。

| 项目 | 默认初值 | 满额行为 |
|---|---:|---|
| 单会话邮箱内存预取 | 32 条 | 其余消息留在持久队列，不以未触发/缓存已满为由丢弃群历史；控制命令保留独立小队列 |
| 单 Bot ready 任务 | 1024 | 接入背压；已持久接收的项目保持终态可追踪 |
| 单 Bot 前台同时运行 | 8 | 公平排队，不绕过宿主总额 |
| 宿主前台同时运行 | 64 | 根据实际 CPU/内存调整，保留控制面与恢复容量 |
| 单流缓冲 | 64 帧 / 256 KiB | credit 背压；可合并预览 delta，不能丢最终结果 |
| 单工具链深度 | 16 | 返回预算耗尽，不无限递归 |
| Actor 缓存 | 1024 项，空闲 30 分钟 | 只淘汰无任务/无租约的项，从持久状态重建 |

这些值用于首轮压测，不是容量保证。内存预取、模型执行和持久存储分别限额，触发响应有等待 deadline，普通消息没有“无人触发就过期丢掉”的策略。持久队列有每 Bot 存储额度和明确接入背压；协议无法施加上游背压时，只能公开记录无法接收的过载故障，不能默默丢弃已承诺接收的消息。大消息前缀分批入 Context 并归档/裁剪，不能通过只保留触发文本满足模型窗口限制。

长任务在安全 checkpoint 边界释放执行槽，返回调度器。等待用户确认不占用模型槽和前台 Actor 提交租约，但 active_session_id 持续占有同群响应资格；该群新触发只排队，其他群可使用释放的槽。静默排空与裁剪有独立的低成本任务预算，避免无触发群的 Context 永久积压。重载候选的资源预留与正常请求资源分开。

裁剪预算只用于本地token计数、可靠归档和确定性视图更新，不调用LLM或排入摘要任务。相同会话的维护唤醒可以合并，但来源覆盖和水位不能跳过。Embedding/图索引使用独立的有界批量队列；其失败不会阻止已可靠归档内容从活动视图移出，召回报告实际可用水位。

## 6. Deadline、取消与迟到结果

每个入口确定绝对 deadline。单进程等待使用单调时钟计算剩余预算；跨进程传 UTC deadline 与最大剩余时长，以更小者为准并考虑配置的时钟偏差界限。下游不得因重试延长上游预算。

取消首先写入 Run/Job 的 `cancel_requested` 事实，然后通知任务树。排队任务直接进入 CANCELLED；运行中任务传播取消并关闭流，外部操作依其已到达阶段返回 CANCELLED 或 UNKNOWN。

取消与提交在同一行锁/CAS 边界竞争：提交先成功则该轮已完成，取消不能抹去已完成事实；取消先成功则迟到提交必须失败。取消事务同时取消尚未 DISPATCHING 的关联效果，执行许可事务按相同锁序锁定 Run 与 operation 并检查取消标记。已许可请求仅取消后续执行/继续对账。Worker 收到取消通知不是唯一防线，数据库最终检查 Run 状态与 owner_epoch。

已取消 Run 的迟到模型内容不发群、不写正常回复、不进入长期记忆；已经独立提交的群消息 InputBatch 和有效工具事实保留。取消只终结其明确覆盖的触发，不取消之后排队的触发；Session 调度器继续处理后者或静默排空。外部操作的迟到回执可以由受权结果收集器写入该操作账本，用于对账，不得因此重新激活 Run。

## 7. 持久事件与消费者

领域事实与 outbox 在一个事务中写入。Dispatcher 读取未投递事件；本机通知仅用于唤醒，即使通知丢失也能通过轮询/重启找回。

每个 durable subscription 以稳定 `subscriber_id + contract_major` 标识，消费位置不能绑定进程 ID 或 Generation，否则升级会重新消费全部历史。订阅升级时原 checkpoint 延续；需要重建时创建新的显式 replay 作业。

消费者用 `(bot_id, subscriber_id, event_id)` 作为 Inbox 去重键；在与其业务更新相同的事务中写处理结果。跨独立存储无法同事务时采用可查询操作账本和幂等接口，不能先 ack 再尝试不可重放操作。

一个消费者失败不阻塞其他订阅；指数退避带抖动、次数和总 deadline，耗尽后进入 Dead Letter，保留原事件、原因和 replay audit。死信重新投递沿用 operation_id，不能成为制造重复副作用的入口。

流式 token、临时 typing 和性能遥测可以走非持久通道；最终回复意图、工具结果、记忆变更、权限变更与生命周期提交必须持久。

## 8. 流式通信

流帧包含 `stream_id, frame_seq, kind, payload`，frame_seq 严格增加。支持 `opened, delta, progress, completed, failed, cancelled`，终态只有一个。接收方通过 credit 限制发送量，关闭流必须回收上下游资源。

模型 delta 是未提交预览。默认 QQ 输出在轮次成功后发送最终内容；若平台支持编辑并启用流式预览，必须明确标记其临时性质，绑定同一 SendIntent 派生的预览消息 ID。失败后更新预览状态，不把部分文本冒充成功轮次。

消费者断线可查询 Run 状态和已提交结果；首版不承诺重放所有 token。持久 `ModelTurn` 才是上下文事实，不能逐 token 覆写共享会话列表。

## 9. 对外发送与 Adapter

### 9.1 固定回复路由

`ReplyRoute` 包含 `bot_id, binding_id, platform, channel_kind, channel_id, thread_id, reply_to_message_id, visibility`。由入站事实或已受权 Job 生成，进入 Run 后不可由共享全局变量改变。模型希望跨群发消息时必须提出新的工具意图并通过目标范围授权。

发送经过：`FinalCandidate/ReplyProposal → Conversation 正式 REPLY + SendIntent UnitOfWork → Delivery 分片/顺序/Effect → Adapter TransportAttempt → Observation`。Adapter 负责 CQ/OneBot 等平台编码和单次传输，Delivery 负责持久发送语义；Agent 或业务插件只认识规范内容段，不能直接调用 Adapter。

持久意图固定 `operation_id, contract_version, schema_digest, owner_generation, input_digest, ReplyRoute`。在途工作绑定原 Generation；热重载必须先排空或由已验证的 takeover contract 接管，否则阻止切换。Forest 首发前不保留开发期 schema 兼容执行器，也禁止用新 schema 重新解释旧参数。

群聊领取组按至多 64 项的分段持久保存触发身份和逐条作者；跨模块批次只传组引用、累计摘要与末触发身份。回复的会话目标固定，引用锚点可取末触发消息；全部实际覆盖成员由 Conversation 的领取组及终结事实确定，不能把锚点用户当成其他消息的作者或授权主体。每次 CHAT 最终回复事务分配会话单调 `response_seq`，Delivery 按 `(conversation_id, response_seq, part_index)` 发出，同群后续轮次不能因独立 Worker 更快而抢先发送。

管理回执使用 `send_lane=CONTROL`，按 `(command_id, control_seq, part_index)` 排序，不占用 Agent 的 response_seq 或 Session。不同命令之间可以按服务类别调度，长输出分页并受额外额度限制。CONTROL 可越过 CHAT 的队首等待，共用 Binding 所有权、平台限流、当前输出权限和效果账本；不能越过已发出网络请求，也不能突破平台对同群或账号的硬限制。控制回执不得作为对话 Agent 的另一个 ModelTurn，具体规则见[09](09-group-shell-command-system.md)。

### 9.2 发送状态

```text
PENDING → CLAIMED → PREPARED → DISPATCHING → SENT
                      │              ├────────→ UNKNOWN（可能已应用）
                      │              └────────→ FAILED（确定拒绝且未应用）
                      └─────────────→ CANCELLED（交接前撤销）
DISPATCHING / UNKNOWN → SENT / NOT_APPLIED（仅带 evidence 的对账）
NOT_APPLIED → 同一 part/effect 的新 attempt（策略允许时）
```

claim 使用 `delivery_owner_epoch`，只授予准备权。Delivery 在短事务中锁定 lane/intent/part 与对应 `kernel.effect` operation，复核当前授权、Binding/client fence、目标、payload 和取消事实后建立 `PREPARED` attempt；实际 I/O 前再用第二个短事务把 effect 与 attempt 一起交接为 `DISPATCHING`，提交后才调用 Adapter。它不锁或修改 Agent Run；已完成 Run 的合法回复按已提交 cause 继续发送。交接是不可撤回边界，稍后撤权不能把在途网络请求改写成未执行。账本分别记录许可、交接、观察与对账。

平台支持幂等键时传稳定 operation_id；支持查询时按平台消息标识对账。若两者都不支持，断线后 UNKNOWN 默认不自动重发，提供查询/人工决定。不能用“失败后换新 echo 再发”冒充可靠重试。

一个 ReplyProposal 分成多个平台消息时，子项使用稳定 `(intent_id, part_index)`，据此派生各自的 `part_operation_id` 并作为平台幂等键，不能所有分片共用父键；记录部分成功。重试仅补确定未发送部分，UNKNOWN 子项先对账。

同群 CHAT 先前 response_seq 处于 DISPATCHING/UNKNOWN 时，阻塞后续 CHAT 回复外发，直到带证据的显式对账把原 attempt 解析为 `SENT` 或 `NOT_APPLIED`；不存在无证据的 skip/继续决议。确认 `NOT_APPLIED` 后才可为同一 part/effect 创建下一 attempt。CONTROL 在同一 command_id 内遵守同样的分片和未知结果规则，其他独立命令的短回执仍可调度。未知状态不持有执行槽或数据库事务；两个通道共享的平台实际串行发送约束仍然有效。

Delivery 模块发布 `delivery.unknown.query`、`delivery.unknown.read` 与 P0 `delivery.unknown.resolve`。查询只返回有界元数据，并以 `(updated_at, intent_id)` 的快照复合游标稳定分页；详情明确展示 `Intent → Part → Attempt → Effect` 与不可变 Observation 历史。处置输入必须绑定当前 `intent_id`、`attempt_id` 和 `SENT | NOT_APPLIED_RETRY | NOT_APPLIED_FINAL` 结论，并提交 `PROVIDER_QUERY | PLATFORM_OBSERVATION | OPERATOR_OBSERVATION` 类型、可复核引用和可选备注。目标 Delivery 事务会再次验证 Web authority ticket，把证据、Effect 观察和整条 Delivery 状态迁移一次提交；授权失败或状态竞争会整体回滚。

### 9.3 连接所有权

Binding 是持久账号归属，Session 是一次连接，`connection_epoch` 每次接管递增。请求回执按 `(binding_id, connection_epoch, echo)` 定位 Future；重连时旧 Session 的未完成请求进入确定失败或 UNKNOWN，清理 Future 与超时任务。

默认一个平台账号在部署中只绑定一个 Bot。若两个独立人格要共享账号，需要显式路由模块及发送冲突策略，不能隐式复制入站消息到所有 Bot。

Adapter 重载时先停止接新发送、排空在途请求、保存可恢复接收游标，再关闭旧连接和打开新连接。重叠接收可由 Inbox 去重；重叠发送必须受租约与实际连接终止共同约束。平台没有回放能力时，连接交接存在接收空窗，必须在绑定能力报告中披露。

## 10. 错误契约

错误统一为 `code, category, retryable, operation_id, stage, safe_message, detail_ref`。safe_message 可发用户，detail_ref 指向受限日志，禁止把凭据、完整 Prompt 或任意堆栈发到群里。

WebUI管理API将此错误契约映射到版本化HTTP响应：配置版本竞争返回409/REVISION_CONFLICT，保留客户端草稿并重新获取权威版本；请求超时先按幂等键或既有operation_ref查询结果，不能换新ID盲重发。浏览器实时进度采用有界订阅，事件只提供更新通知；断线、重复、游标过期时按授权重新取快照，不改变操作状态。管理读写、取消和批准复用控制类调度，长任务交还持久执行器，不占满P0/P1容量；HTTP细节见[12](12-webui-administration.md)。

| 类别 | 例码 | 处理 |
|---|---|---|
| 契约 | `INVALID_SCHEMA`, `UNSUPPORTED_CONTRACT` | 不重试，修复调用方 |
| 授权 | `PERMISSION_DENIED`, `GRANT_REVOKED` | 不自动重试，保留审计 |
| 所有权 | `STALE_EPOCH`, `RUN_NOT_ACTIVE` | 旧执行者停止；由协调器决定恢复 |
| 并发 | `REVISION_CONFLICT`, `INPUT_SET_CHANGED` | 重新读取并重新计算，不盲写旧结果 |
| 幂等 | `IDEMPOTENCY_CONFLICT` | 同一 ID 内容不同，拒绝并告警 |
| 容量 | `OVERLOADED`, `DEADLINE_EXCEEDED` | 遵守上游预算，可返回 retry_after |
| 生命周期 | `SERVICE_RELOADING`, `DEPENDENCY_UNAVAILABLE` | 有界排队/降级 |
| 外部效果 | `EFFECT_OUTCOME_UNKNOWN` | 查询账本/对账，禁止无依据重放 |

## 11. 通信与并发验收

1. 群甲先到的 Pipeline 比第二条慢，Actor 仍按 ingress_seq 消费，并能跨过显式失败项。
2. 群甲持续慢模型请求时，群乙和 Bot B 在自己的配额内获得服务。
3. 相同 command 重复、乱序投递不会追加第二份业务事实；同 ID 不同 payload 被拒绝。
4. Consumer 写完事务、ack 前死亡，重放后业务状态不重复变化。
5. 用户取消与轮次提交竞态只出现一个有定义的结果，迟到模型内容不发出。
6. Adapter 发出后断网，记录 UNKNOWN，重载后不盲目再次发送。
7. 对话流消费者缓慢或断线时，内存和 task 数受限，最终状态仍可查询。
8. epoch 失效的 Worker 不能用缓存句柄继续写；只读响应也必须遵守当前访问权限。
9. 普通分类永久失败前仍在重试、Agent 长等待及 CHAT 发送 UNKNOWN 时，受权管理命令可独立准入并尝试发送短回执；管理输入历史不越过分类缺口。
10. 管理命令持续到达时，服务类别配额仍保证业务和维护获得其配置份额；高 priority 插件不能夺取核心命令路由或操作租约。

以上保证安全投递与顺序；共享记忆语义的合流另按[06 并发合流设计](06-concurrent-convergence.md)验收。
