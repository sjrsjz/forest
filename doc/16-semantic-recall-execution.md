# 持久语义召回与 Agent 接入

本文展开 Memory 语义召回与单 Agent 模型步骤的目标执行边界。完成度和验证证据以 [实现状态](implementation-status.md) 为准。

## 1. 所有权与身份

Memory 管理查询准备、QUERY 嵌入等待、ANN 固定匹配、区域冻结和证据物化。Agent 只管理模型步骤需要的查询身份、等待状态和最终证据引用，不选择向量索引参数，也不读取 Memory 私有表。Scheduler 只管理执行租约及唤醒，不解释召回阶段。一次召回不是另一个 Agent。

每个普通模型步骤在接纳安全边界输入后固定 `run_id + step_seq + input_batch_ref + context_revision`，由这些事实确定 query identity。登记尚未完成来源捕获时，保持相同输入 fence 等待；不得吸收之后的普通消息或重新登记不同查询。协议修复步骤使用原失败候选，不启动召回，也不读取新输入。

Agent 必须先持久提交模型步骤输入和 query identity，再请求 Memory 工作。查询注册成功但 Agent 回执丢失时，以同一 query identity 重试登记；禁止由内存变量决定是否创建第二条查询。模型请求已经准备后，恢复沿其既有 evidence ref 继续，不重新执行 ANN。

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

Agent 等待时释放本次执行租约，保留同一个逻辑 Session/Run 和同群排他性。新 trigger 继续入队，在下一次合法模型边界接纳；不会创建并发响应。查询的输入 cut 已固定，新输入只能使尚未准备模型请求的步骤进入明确的新输入版本，不能偷偷改写旧 query。

召回完成后、冻结模型请求前，再通过原安全边界契约检查排队 trigger。若选出新批次，先提交该模型步骤的新输入版本及对应新 query identity，再由 Memory 处理该查询；旧 query 的证据不能冒充新版本的召回结果。已经 handoff 的旧嵌入按原 Effect 结算，不能为了新版本重发同一 attempt。持续触发可能使该步骤多次更新输入，因此截止时间属于整个步骤，不因新输入或重新 claim 延长。模型请求冻结后到达的 trigger 留给下一次模型调用，不能修改正在发出的请求。

## 4. 证据与投影

Memory 发布 `forest.memory-evidence/1`，保留文本、图片引用、历史作者身份、群/主干、时间和实际路径。Agent 读取规范证据，按绑定的模型能力产生请求投影；纯文本模型移除图片内容，图文模型读取原图片引用。去图不会改变 Memory 的 QUERY 嵌入模型或查询语义。

Agent model.input 和投影载荷必须继承 Context、Preset、规范 Memory 证据及原媒体的受管父链。Provider 只持有已投影请求的序列化/传输责任，不能裁剪上下文、估算拒绝预算或自行选取证据。规范证据、模型投影与 wire 是不同层的不可变产物，必须分别标明所有者与用途。

## 5. 验收证据

需要在真实 PostgreSQL 与实际隔离 Worker 路径验证：准备中断恢复、部分 ANN 回执恢复、零 facet/零结果、UNKNOWN 不重发、截止失败、并发登记合流、热重载后同 query 恢复、删除后 READY 失效，以及同群等待不并发、跨群不互相阻塞。Agent 验收还必须检查实际模型请求包含正确来源证据，纯文本请求没有图片内容，协议修复不启动新召回。阶段查询测试通过不能代替这些端到端验收。
