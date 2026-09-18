# Forest 当前实现状态

更新日期：2026-09-18。

> 本页只记录当前工作树事实，不定义目标架构。发生冲突时依次以 [00 架构总契约](00-architecture-contract.md)、[13 微内核控制面重构](13-microkernel-control-plane-redesign.md)、[14 干净 Schema 与载荷来源](14-clean-schema-and-payload-provenance.md)和[15 单 Agent 生产执行](15-single-agent-production-execution.md)为准。Forest 尚不可发布。

## 不可改变的当前语义

- Agent 是 `agent.runtime` 模块中的单会话状态机，不存在产品子 Agent。
- 每群至多一条活动 Agent 响应链；群之间可以并行。新触发在下一次模型调用边界接入截至最后一个触发的连续消息前缀，普通尾部在终止后静默排空。
- 每个 Bot 只有一个全局记忆域，每个群是一条独立主干；联想可以跨主干，召回必须携带来源群和作者。
- 裁剪不调用 LLM，也不生成记忆摘要。文本与图片均进入权威载荷、嵌入和检索；text-only Provider 只在单次请求投影中移除图片。
- VL 嵌入空间与 Bot 生命周期绑定，只允许更换兼容 endpoint 或凭据。
- WebUI 壳只负责身份、作用域、模块发现和受控工作区；业务页面、schema、查询和动作由准确模块 Generation 贡献。
- 未发布结构直接删改并重建开发数据库，不新增迁移、兼容读取、双写、旧 API 转发或 fallback。

设计文档已经先于后续代码完成语义纠偏：00 明确列出被废止的 Bot/Agent 中心组合根、产品子 Agent、固定业务后台、模糊记忆关系、第二套执行链和开发期兼容迁移；10 固定百万级来源的服务端组合筛选、快照键集分页与可解释关系 DTO；12 固定 PC 工作台的有界 pane 布局和模块 UI 尺寸协议。仍存在的旧代码和旧页面均列入删除范围，不能反向解释这些目标契约。

## 已落地的干净微内核切片

### 最小 Bootstrap 与五个内核模块

`forest_kernel` 只保存 Bot 身份和 schema bundle 账本。以下模块各自拥有独立 namespace、baseline、Repository、模块配置和真实 grant set：

- `kernel.runtime`
- `kernel.payload`
- `kernel.authorization`
- `kernel.effect`
- `kernel.audit`

`bootstrap_clean_kernel()` 可以从空数据库安装这五个模块并把它们作为普通 Generation 发布，不依赖旧 `public` 业务表。

### 精确 Generation 与 RuntimeGraph

干净 Registry 已执行以下固定协议：

1. `prepare_generation` 冻结包、配置、授权、存储和状态契约；
2. `kernel.runtime` 持久化候选图并分配精确 `generation_id`；
3. `bind_generation` 把数据库分配的身份交给模块；
4. `start` 只预热待命资源；
5. 模块 publication participant、配置 effective、Generation ACTIVE、Route 和 graph head 在一个 Bot 数据库事务中提交；
6. `activate_generation` 只打开本地 admission gate，不执行 I/O；
7. 关闭先原子冻结所有 route admission，保留当前 graph head 供已持 lease 的工作以 member fence 排空；逆依赖达到 QUIESCENT 后才撤下 head并落 STOPPED/FAILED。

新工作通过 `verify_active_generation_in()` 在最终事务内复核准确组件、Generation、RuntimeGraph、Route epoch 和 DATA/CONTROL admission；已经领取的排空工作只能通过 `verify_generation_member_in()` 继续，并同时复核自己的 actor/run/call lease。绑定身份失败、预热失败、图提交失败、本地 activation 不变量失败和 freeze 后新准入拒绝都有回收测试，不会遗留可接纳工作的部分图。

### Provider、Agent、Conversation 与 OneBot Messaging 图

> **生产接线状态：** Conversation Worker、单 Agent 执行、Context 投影、Provider 调用、最终回复与 Session 交接已经进入同一干净 RuntimeGraph。Agent→Provider 与 Conversation→Agent 使用冻结到准确图/代际/契约版本的 Contract Router 句柄；Conversation 只调用输入选择、最终回复和 Session 交接三个内核 UoW 服务，Agent、Conversation、Memory、Delivery participant 由内核按声明解析。生产模块不再通过共享模块字典、直接运行实例或 bound participant 完成这条链。PostgreSQL 集成测试已从真实 OneBot 入站事实自动生成回复、原子提交 Context/SendIntent、经 Delivery 发送并静默关闭 Session。
`provider.chat-completions`、`embedding.multimodal`、`memory.graph`、`agent.runtime`、`conversation.scheduler`、`adapter.onebot` 和 `delivery` 已经作为平级业务模块与五个内核模块共同进入生产 RuntimeGraph：
- `provider.chat-completions` 独占共享 HTTP 客户端、端点/模型能力快照和单次 `ModelInputProjection`；每个请求绑定精确 Provider Generation 与能力摘要，实际发送前在短许可事务中再次复核 DATA admission；
- text-only Provider 的请求投影会递归移除图片、受管媒体 URL/base64/file 字段，同时保留作者、来源群、trigger 与不可见模态标记；该投影不修改权威多模态 Context；
- `agent.runtime` 的全部状态变更经准确 Agent Generation 在最终数据库事务内复核；
- Agent `COMPLETED` Run 不再擅自关闭 Session；Conversation 排空决定显式关闭，或在同一 Session 下原子推进唯一后继 Run；
- `conversation.scheduler` 的接纳与分类端口检查自身准确 Generation；
- `adapter.onebot` 只拥有 Binding、配置修订、客户端代际、平台事件、网络客户端和 OneBot transport；
- Adapter 不拥有 Conversation lane、Shell、Agent、Memory、Delivery 或发送账本；
- Delivery 已有独立 schema、配置、grant set、Generation、Repository、`DeliveryAuthorizationPort` 和生命周期所属的发送 Worker，依赖 Adapter 的公开 transport-fence 端口与 Kernel Effect 端口，不读取 Adapter 私有表。Worker 从持久队列领取 part，将不可变业务载荷投影成 OneBot wire，在交接前写入 Effect permit 并复核当前 Binding/Adapter Generation；网络交接后只接受 `SENT`、明确 `NOT_SENT` 或 `UNKNOWN` 观察。重启时，过期 `PREPARED` attempt 会经 Effect abandonment 回到队列，过期 `DISPATCHING` attempt 只能落为 `UNKNOWN`，不能猜测未发送或自动重发。动态 part 不再诱导内核创建逐 part grant，`delivery.send` 票据绑定 Bot 能力、part semantic digest、精确 binding plan 与 Adapter Generation，最终 operation 的单次消费仍防止跨 part/参数重放。
- `kernel.effect` 的 fence verifier 已按 `fence_kind` 注册且禁止覆盖；工具走 `runtime.graph`，Delivery 走 `adapter.binding`。Adapter 从持久 client incarnation 反查 binding、generation、epoch 与 fence digest 后再复核当前 head，调用方不能自行拼出一张连接 fence。
- OneBot 客户端先以 `BUFFERING` 绑定配置 revision、Adapter Generation 和 executor fence；
- Binding client publication 作为 RuntimeGraph 事务 participant，与 Adapter Route 一起提交；
- WebSocket 在预热期只缓存，图提交后才打开事件流；
- 旧 Adapter Generation 即使仍持有 Binding epoch，也不能接纳事件或发送；
- raw event、normalized parts 和 author snapshot 先成为带来源闭包的 payload；文本消息再生成独立 UTF-8 Memory part，由一个调用方 UoW 原子提交 Adapter platform event、Conversation Inbox、群主干和 Memory occurrence；
- Binding head、client 和事件接纳已统一锁序，同一账号不同群可以并行。

Conversation 已增加受精确 Generation 约束的 Worker 内核：它从持久 lane 领取 actor，同群依靠数据库 lease/epoch 串行，不同群受有界并发槽并行；注入的单一 `AgentExecutionPort` 只能在下一次模型调用回调取得新的 `InputBatch`。最终候选后到达的 trigger 会创建同一 Session 的后继 Run，普通尾部在 Session 结束时静默排空。Worker 已不再直接追加回复 Context 或自行提交 `SESSION_END` 选择，也不会在执行异常后擅自把仍有持久 Agent Session 的 Run 退回空闲；它强制依赖 `ConversationCompletionPort` 和 `SessionDecisionPort`。`TransactionalConversationCompletionPort` 逐项核对 Agent completion 与 `FinalReplyPlan` 后调用三方 UoW；`TransactionalSessionDecisionPort` 从已完成 Run 固定 deadline 和稳定操作 ID 后调用 Agent/Conversation 交接 UoW。领取、续租、输入选择和释放仍在调用方事务内复核 Conversation Generation。活跃 Session 本身已作为恢复候选；Worker 重启后读取持久 `session_ref/run_ref` 并恢复原执行链，Agent authority 也能枚举包含已完成但尚未排空 Session 的活动 Run 头。完成端口必须显式返回实际回答的 trigger 集合，不能依赖丢失即不可恢复的进程内累计列表。`FinalReplyUnitOfWork` 已在写入前复核 Agent、Conversation、Delivery 三个精确 Generation，并在一个 Bot PostgreSQL 事务中把 Run 从 `FINAL_READY` 置为 `COMPLETED`、追加正式 `REPLY`/终结明确 trigger、创建 CHAT SendIntent；Delivery participant 失败时三者整体回滚。`SessionHandoffUnitOfWork` 也已把 `SESSION_END` 的输入选择与 Agent Session 的后继 Run 建立/显式关闭放入同一事务；InputBatch 现在冻结 `context_view_revision`，后继 Run 不再从后来状态猜测模型视图。`DurableAgentModelExecutor` 已按持久 Run 输入 fence 构造 Provider 投影，写入 ModelRequest/TransportAttempt，在 Provider 的发送前许可事务中原子标记 Agent handoff，持久化 SUCCEEDED/NOT_APPLIED/UNKNOWN 观察，解析固定 CorePrompt 协议并把终止回答推进到 `FINAL_READY`；输入版本、trigger 和 InputBatch 任一不符都会在调用前拒绝。FunctionCall 现在冻结准确工具 component/Generation/service/contract 绑定和原触发 principal，并持久化单个 ToolProposal；`DurableAgentToolExecutor` 只接受回显相同输入摘要和 audience Generation 的授权票据收据，通过 Effect 端口的事务回调原子标记 `DISPATCHING`，结果丢失后由新 owner 查询原 effect，不能重新 dispatch；成功载荷必须由工具组件以自身身份发布，协调器不能冒充工具写 payload。`KernelToolAuthorizationPort` 已把可委托的 `tool.invoke` grant 收窄为目标工具的精确 Generation，按策略建立通用批准信封，并在同一事务中单次消费已批准对象和签发短票；批准决定也必须消费准确 scope、决定摘要、计划和主体的 `approval.decide` 票据。票据已经拆分唯一 issuance operation 与最终 effect operation；handoff 前过期会形成新签发 attempt，同一 effect 的并发票据消费由 operation advisory lock 和唯一消费事实确定地收敛。`KernelToolExecutionPort` 已把票据消费、当前 RuntimeGraph fence、Effect handoff 和 Agent `DISPATCHING` 放入一个事务，外部返回丢失时持久化 `UNKNOWN`，恢复只查询原效果；工具结果引用随 Effect evidence 持久化且保持工具组件所有权。恢复投影会携带当前 request/attempt/tool/checkpoint；无法查询的 `DISPATCHING` 模型或工具 attempt 幂等落为 `BLOCKED_UNKNOWN`。`conversation.scheduler` 只拥有调度权威；生命周期 Worker 经冻结的 agent.execute/1 句柄调用单一 Agent 状态机，并把最终回复与 Session 交接交给内核 UoW 服务。内核协调器按声明解析 participant，业务模块不持有其他运行实例。PARSING 和 SAFE_BOUNDARY 已支持从持久事实确定性自动重放；工具服务目录、WAIT_APPROVAL 唤醒、REPAIR_READY 与热重载交接仍未完成；目标设计不增加拥有聊天语义的生产执行模块。

当前真实网络模块支持正向 Universal WebSocket 与 HTTP action 客户端，启动时调用 `get_login_info` 校验账号。HTTP 上报通过模块端口接入，尚未接入新的生产 Web transport 宿主。

`memory.graph` 与 `embedding.multimodal` 已作为两个独立成员进入干净 RuntimeGraph。Memory 独占图 schema、facet/claim、预算、向量发布和召回语义；Embedding Provider 独占 profile identity、endpoint/凭据、请求投影、HTTP 客户端、传输尝试和用量/结果载荷。Memory 的生命周期 Worker 只持有冻结的 `embedding.encode/1` 服务句柄，Provider 无法读取 Memory 私有表。Provider `SENT` 与 Memory `DISPATCHED` 在同一事务合流；交接后的崩溃只会恢复为 `UNKNOWN`，不会重复推断发送。默认冻结 QiYu 使用的 `Qwen/Qwen3-VL-Embedding-8B`、4096 维 L2 空间；Bot 首次初始化后 deployment、model revision、模型和维度由 Provider identity 与 Memory domain 双重拒绝变更，endpoint/凭据/连接参数可经配置 plan/apply 和新 Generation 替换。OneBot 文本入站已在 Adapter/Conversation/Memory 三参与者事务中登记来源；IDLE、模型安全点与 Session 结束的 InputBatch 选择会在同一事务提交 Memory eligibility，并为新获资格的来源修订创建幂等 `DOCUMENT` embedding facets。`memory.recall/1` 已把精确 InputBatch 物化为 `QUERY` facet，冻结检索包并逐条标注原始群聊和作者；`agent.runtime` 在同一单 Agent 的下一次模型调用前通过冻结路由消费召回，工具循环复用同一输入修订的查询身份。实际 OneBot 图片抓取仍未接线。

### 已有干净业务 Authority 候选

以下模块已经有独立官方 baseline、Repository 和定向 PostgreSQL 测试，但尚未全部成为生产 RuntimeGraph 中的完整 Generation/Worker：

- `memory.graph` 的媒体摄入后半段：生命周期 Worker、Bot 级冻结语义空间、每日请求/Token 预算、每群主干、文本来源摄入、InputBatch 资格合流、查询向量、Agent 召回、图文来源及跨主干关系查询已进入 RuntimeGraph；实际 OneBot 图片抓取和受管媒体物化尚未接线；
- `delivery`：CHAT/CONTROL lane、顺序、Effect 与 Adapter 双 fence、生命周期 Worker、崩溃恢复与 UNKNOWN 对账；
- `shell.command`：Binding/群/主体策略、P0/P1 claim、授权复核和控制回执；
- `conversation.scheduler`：连续分类水位、同群 actor lease、安全边界、trigger cut、静默排空和恢复领取。

这些 Repository 测试证明的是各自权威状态机，不代表生产启动链已经切换。

## 仍需删除或替换的旧实现

以下对象仍存在于源码或旧测试中，全部失去设计权威：

- `BotStore` 及 `public` 大表；
- `migration_v*`、升级夹具和 writer-fence 升级测试；
- 旧 `OneBotRuntime` 中的 `BindingStore`、`MessageStore`、`ShellIngressGuard`、`DeliveryStore` 和发送 Worker 组合根；
- 旧 `RuntimeModuleRegistry` 路径及旧 RuntimeGraph 表；
- 旧业务 `contract.router` 模块及其 Agent 选择、模板 responder 和聊天执行职责；内核 Contract Router 保留为无聊天语义的版本化调用机制；
- 固定 Agent/Memory/OneBot/Bot 管理页面和壳层专用 API；
- `BotProvisioner`、Fleet、Restore 和 CLI 对 `BotStore.migrate()` 的调用。

切换时直接删除这些路径并重建开发数据库，不建立桥接层。

## 当前阻断项

1. 补齐独立 `media.capture` 的 OneBot 图片抓取与受管媒体物化，并将 `shell.command` 接入相同的 `prepare → bind → prewarm → atomic publish → local activate` 协议；Memory/Embedding 生命周期 Worker、文本摄入/InputBatch 资格 UoW、Agent 召回与 Delivery Worker 已进入该协议，后续需要在完整生产启动图验证热替换排空。
2. 为 `agent.runtime` 注册真实工具目录、授权/Effect 与查询路由，补齐 WAIT_APPROVAL 唤醒、REPAIR_READY 与 RELOAD_WAIT 的生产恢复；PARSING 和 SAFE_BOUNDARY 已支持确定性自动重放，当前自动链仍只验证无工具终止回答。Adapter 入站已经改由内核 UoW 服务提交，Delivery→Adapter 已使用冻结服务句柄；下一步删除旧业务 `contract.router` 与旧组合根。
3. 为多模态 Memory 做十万至百万来源的筛选、稳定分页、关系展开、图片负载和资源上限测试。
4. 建立模块贡献式管理协议与新 WebUI 壳；删除当前固定业务 DOM、专用 API 和旧大页面。
5. 继续把多 Bot provisioning 和恢复控制面切到干净宿主；单 Bot `forest bot`、HostLease 续租/释放、遗留 OneBot 客户端结算和 Docker 默认入口已经切到最小 bootstrap + bundle + 冻结 RuntimeGraph。
6. 完成 OneBot 热替换时旧客户端排空、新候选 publication、长连接抖动验证；当前已验证正常关闭后立即重启以及新宿主对遗留客户端的失败结算。
7. 删除旧 schema、迁移、兼容测试和失去所有权的代码。
8. 完成完整 Docker、真实 QQ 许可群 `933179609` 的低频验收；真实启动前先停止树莓派 `alicebot.service`。

## 最近可复现证据

- 文档权威链与 CorePrompt：`14 passed`。
- Runtime Registry、Schema authority、干净 Conversation/OneBot/Delivery/Shell、OneBot codec/WebSocket 和 Agent 协议相关组合：上一基线 `183 passed`。
- Agent/Conversation Worker、持久调度 Repository、十成员 Messaging RuntimeGraph、Provider、Delivery 与生命周期相关定向组合：`124 passed`；其中 Worker 单元用例验证同群互斥、跨群并行、安全点接入、原 Session/Run 恢复、强制事务完成端口、稳定 Session 决策计划，以及 quiesce 时释放正在等待新输入的群 actor；PostgreSQL 用例验证 Agent/Conversation/Provider Generation fence、同 Session 后继 Run、最终回复三参与者事务回滚、Delivery Bot 能力到精确 Adapter Generation 的票据、PREPARED abandonment/requeue、DISPATCHING 崩溃后 UNKNOWN、通用批准的受权决定/单次消费、可委托能力到准确 audience Generation、Effect 与 Agent 原子交接、交接后结果丢失与只查询恢复、Adapter 持久 binding fence、freeze 后拒绝新准入、member fence 排空、来源作者与 trigger 所有权。
- 十一成员 Kernel + Provider + Memory + Agent + Conversation + OneBot + Delivery RuntimeGraph 集成：真实 PostgreSQL 下通过，验证准确代际、text-only 多模态请求投影、OneBot 文本来源/群主干登记、InputBatch 与 Memory eligibility 原子提交、发送前许可、生命周期所属 Delivery Worker 的真实 OneBot wire 调用与 SENT 落账、作者/群来源和逆序关闭。最新 clean authority/RuntimeGraph 定向组合为 `68 passed`；并发测试覆盖 InputBatch 与延迟媒体来源两种完成顺序，最终没有未解析的 `PENDING_CAPTURE`。
- OneBot 单模块生命周期集成：真实 PostgreSQL 下通过，验证 BUFFERING、原子 publication、事件 UoW、quiesce 和 CLOSED。
- [单 Bot 生产宿主](operations-clean-production-launch.md)与自动触发策略：真实 PostgreSQL 下通过。13 成员图可以从空库启动、退休、显式释放 HostLease 并立即以新 incarnation 重启；Embedding endpoint 配置的新 Generation 生效而冻结 profile 变更被拒绝；许可群的 mention 由独立 `conversation.trigger-policy` 分类并在 ingress UoW 内原子提交，不再由测试手工制造 Trigger。
- 最近一次无 PostgreSQL 全套证据为 `755 passed, 36 skipped, 1197 deselected`；它包含大量旧实现测试，只用于回归发现，不能作为目标架构完成证明。

测试数字只对应相应提交和命令。真实 QQ、长时间连接、完整 Docker 进程验收、百万级记忆以及多 Bot/WebUI 控制面 cutover 尚未完成。
