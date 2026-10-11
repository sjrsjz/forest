# 持久语义召回与 Agent 接入

本文展开 Memory 语义召回与单 Agent 模型步骤的目标执行边界。完成度和验证证据以 [实现状态](implementation-status.md) 为准。

## 1. 所有权与身份

Memory 管理查询准备、QUERY 嵌入等待、ANN 固定匹配、区域冻结和证据物化。Agent 只管理模型步骤需要的查询身份、等待状态和最终证据引用，不选择向量索引参数，也不读取 Memory 私有表。Scheduler 只管理执行租约及唤醒，不解释召回阶段。一次召回不是另一个 Agent。

每个普通模型步骤在接纳安全边界输入后，以持久 `run_id + step_seq + input_revision` 推导唯一 query_id，并用该 revision 对应的 `input_batch_ref + context_revision` 向 Memory 登记；Memory 回执必须与这两项持久事实一致。登记尚未完成来源捕获时，保持同一输入 fence 等待；不得吸收之后的普通消息或重新登记不同查询。协议修复步骤使用原失败候选，不启动召回，也不读取新输入。

Agent 先持久提交模型步骤输入；query identity 是该持久 Run 输入的确定性函数，不额外写一张重复的绑定表。查询注册成功但 Agent 回执丢失时，以同一 query identity 重试登记；禁止由内存变量决定是否创建第二条查询。模型请求已经准备后，恢复沿同一 query_id 读取已发布证据，不重新执行 ANN。

## 2. 从事实推导阶段

`memory.query.progress/1` 是只读服务，输入已登记的 query_id。它不创建查询、不派发嵌入、不匹配、不物化、不占用预算。私有查询从同一个数据库语句快照推导以下阶段；不另存一份容易漂移的业务阶段表。

| 阶段 | 权威事实 | 后台工作需要的动作 |
|---|---|---|
| NEED_PREPARATION | 尚无完整 query_facet_preparations | 幂等准备各 part，提交完整回执 |
| WAIT_EMBEDDING | 已完整准备，但 QUERY facets 尚未全部 READY | 交还租约，等待现有嵌入和向量发布工作 |
| BLOCKED_UNKNOWN | 至少一个 QUERY facet 为 UNKNOWN | 保留预算及原 Effect，等待确定观察或管理员处理，不重发 |
| FAILED | QUERY facet 为 FAILED 或 SUPPRESSED | 明确失败，不能伪装成零命中 |
| NEED_MATCHING | 向量全部 READY，但匹配准备或每 facet 回执未完整 | 仅匹配缺少回执的 facets |
| NEED_FREEZING | 每 facet 固定匹配完整，查询仍 OPEN | 扩散并原子冻结检索区域 |
| NEED_MATERIALIZATION | 查询 FROZEN，但无证据发布事实 | 按冻结区域生成并发布规范证据 |
| READY | 证据已发布，当前来源和路径及 Payload 读取仍有效 | 返回已发布证据引用 |

输入来源失效、固定路径失效或 Payload 撤销返回明确错误，绝不降级为 NEED_PREPARATION 或空结果。FROZEN 查询不重新准备 facets。零 facet 的有效查询可以完成零匹配并冻结空结果；“尚未准备”和“完整准备但零 facet”必须区分。READY 只是本次读取观察，不是永久读取许可，发布模型请求及 handoff 仍需复核引用。

## 3. 后台工作与等待

后续召回请求服务登记包内 owned-work，工作记录只存不可变 query identity、租约、下一次唤醒时间、总截止时间和明确终态。阶段由 progress 权威事实决定。每次 claim 执行至多一个阶段；已有阶段回执直接跳过，不重复付费请求。租约到期后新的 Worker 从事实恢复，旧 claim 不能结算新 claim。

WAIT_EMBEDDING 不在 Worker 内循环 sleep。UNKNOWN 不进入普通重试队列。网络或模块暂时不可用只允许有界退避；总截止时间到达写明失败，不能让群永久保持无响应。取消召回工作不等价于取消已经 handoff 的嵌入：必须观察原 Effect，并保持费用结算和来源撤销规则。

公开 `memory.query.request/1` 只接受已经登记的 query_id 和绝对 deadline_at，以 query_id 唯一登记 Memory owned-work。截止时间不得超过登记时的十分钟；重放必须给出相同时间，不能延长。`memory.query.work-status/1` 只读返回 PENDING、CLAIMED、BLOCKED_UNKNOWN、READY 或 FAILED，以及原截止时间、错误码和已验证证据引用。未登记工作返回明确错误。其 READY 仍必须重验规范证据和 Payload。

工作表只保存调度事实，不保存业务阶段。每次 claim 在同一 Bot 内用 SKIP LOCKED 领取到期工作，独占 token、attempt 和 lease；同 token 重放返回原 claim。settle 锁住准确 claim，再从查询事实归约终态或下一次唤醒；重复 settle 要求原输入完全相同。过期 claim 不能提交结算，截止失败不能因重放恢复为运行态。普通连续暂时错误最多三次；成功观察重置该错误计数，嵌入等待不消耗重试次数。

BLOCKED_UNKNOWN 是独立状态，只周期观察既有事实与截止时间，不执行准备、ANN、冻结或物化。召回编排步骤的回执丢失与嵌入 Effect 的 UNKNOWN 不同：前者允许从已有幂等阶段事实恢复，后者始终不重发。每次 Worker 只调用一个幂等阶段，且受 claim 的剩余绝对时间和有界 handler 时间约束。超时不会撤销已经提交的事实；settle 必须重新读取后决定结果。

`memory.query.cancel/1` 用 query_id 和固定原因 SUPERSEDED 请求停止旧工作。与领取/结算按同一工作行串行：未领取的工作直接进入 CANCELLED；已领取的只登记取消意图并对调用者显示 CANCELLING，当前阶段已发生的外部请求仍按其原 Effect 结算，Worker 在下一次读取时停止推进，当前 claim 结算或租约恢复后落为 CANCELLED。若 READY/FAILED 已先提交，取消返回 TERMINAL，不删除历史结果。已发布的受管证据仍受来源遗忘与读取权限约束。Agent 不得以“取消查询”伪装为撤销已派发的嵌入。

同一模型步骤如果已接纳输入版本 v，之后 Memory 等待期间新 trigger 到达，安全边界选择必须使用新的选择 operation identity，不能重放 v 的旧 `BATCH` 或 `CONTINUE` 回执。若新选择成功，Agent 先持久推进 input revision，再请求取消旧查询并登记新查询；取消回执丢失时，以同一旧 query_id 重试即可。`CONTINUE` 是一次观察，不是永远没有新消息的证明；下一次有资格的安全边界可以进行新的选择。若选择事务提交了 BATCH 而调用回执丢失，Agent 从已提交的 Run input revision 恢复，不重复追加同一批次。

Agent 等待时释放本次执行租约，保留同一个逻辑 Session/Run 和同群排他性。新 trigger 继续入队，在下一次合法模型边界接纳；不会创建并发响应。查询的输入 cut 已固定，新输入只能使尚未准备模型请求的步骤进入明确的新输入版本，不能偷偷改写旧 query。

召回完成后、冻结模型请求前，再通过原安全边界契约检查排队 trigger。若选出新批次，先提交该模型步骤的新输入版本及对应新 query identity，再由 Memory 处理该查询；旧 query 的证据不能冒充新版本的召回结果。已经 handoff 的旧嵌入按原 Effect 结算，不能为了新版本重发同一 attempt。持续触发可能使该步骤多次更新输入；当前实现为全部步骤设定不超过 Run 绝对截止时间和 Run 创建后九分钟的共同召回截止时间，不因新输入或重新 claim 延长。模型请求冻结后到达的 trigger 留给下一次模型调用，不能修改正在发出的请求。

## 4. 证据与投影

Memory 发布 `forest.memory-evidence/3`，保留文本、图片引用、历史作者身份、群/主干、时间和实际路径，并标记受检索媒体预算省略的图片。Agent 读取规范证据，按绑定的模型能力产生请求投影；纯文本模型移除图片内容，图文模型读取获选的原图片引用。去图不会改变 Memory 的 QUERY 嵌入模型或查询语义，也不会抹掉原始多模态记忆。

普通模型步骤在派发前由 Agent 冻结核心提示词、预设、工具、Run 因果单元、Memory 证据和输出预留的成本报价；Conversation 在本群 Actor/Run 围栏下，从完整 Context 来源中选择待答触发及预算内的可选单元，并发布不可变 `forest.model-view-plan/1`。Agent 按该计划投影输入并在派发前复核成员和报价引用。后继步骤不能删掉此前已选单元，也不能把此前省略的旧单元重新选回；Provider 不参与选择或预算。Context 当前仍先构造有界完整快照，再应用选择计划，因此大规模快照成本和本地 token 估算精度仍需继续验收。

Agent model.input、model.projection 与 model.wire 必须继承 Context、Preset、规范 Memory 证据及原媒体的受管父链。Provider 只校验并传输 Agent 已冻结的请求，不能裁剪上下文、估算拒绝预算或自行选取证据。规范证据、模型投影与 wire 是不同层的不可变产物，必须分别标明所有者与用途。

## 5. 验收证据

需要在真实 PostgreSQL 与实际隔离 Worker 路径验证：准备中断恢复、部分 ANN 回执恢复、零 facet/零结果、UNKNOWN 不重发、截止失败、并发登记合流、热重载后同 query 恢复、删除后 READY 失效，以及同群等待不并发、跨群不互相阻塞。Agent 验收还必须检查实际模型请求包含正确来源证据，纯文本请求没有图片内容，协议修复不启动新召回。阶段查询测试通过不能代替这些端到端验收。

已有隔离 PostgreSQL 验收通过伪 OneBot 群 @ 入站、真实 Scheduler/Agent/Memory 隔离 Worker、伪模型调用串起请求，检查证据进入 Provider 请求体并保留群、主干和作者来源，纯文本 wire 不含图片。使用 QiYu 的真实 VL 嵌入端点和独立 Milvus 集合时，隔离用例已验证大图缩放、跨群 IMAGE→IMAGE 命中、来源归因、回复后的联想日志及来源归档后的跨主干联想提升；聊天模型和 OneBot 仍为替身。本轮再次运行同群触发续轮与 Agent 模块热发布整链，`2 passed`；10 万条关系分页与 2 万条高出度联想边采样用例也 `2 passed`。这些证据不等于真实 QQ 低频端到端、真实聊天供应商回复或数月混合图文负载验收；在线实例尚未切换到当前基线。
