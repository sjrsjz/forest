# Forest 当前实现状态

更新日期：2026-09-21。

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

同一 HostLease 下的热替换主链已经改为先排空再发布：候选图及其 OneBot 客户端先在 `BUFFERING` 状态预热但不取得路由；旧图随后原子冻结 DATA/CONTROL 顶层准入，并按反向依赖顺序排空仍拥有持久工作的可恢复 Worker。Conversation 会在下一次模型或工具外部调用前观察排空信号，在已有持久边界释放 Run/actor；确认排空后 RuntimeGraph publication 事务才把旧图/旧客户端置为 `RETIRED/RETIRING`、切换准确 Generation route 与 Binding epoch，候选连接随后打开事件流。候选在发布前失败时，authority 恢复旧图原声明的准入位和 route epoch，本地 Worker 按正向依赖顺序恢复；发布成功后旧 Adapter、Provider 与 Registry 才关闭并记录 `STOPPED/DISPOSED`。宿主用单一替换锁串行这些阶段，配置 revision 可在不停 HostLease 的情况下生效。

新工作通过 `verify_active_generation_in()` 在最终事务内复核准确组件、Generation、RuntimeGraph、Route epoch 和 DATA/CONTROL admission；已经领取的排空工作只能通过 `verify_generation_member_in()` 继续，并同时复核自己的 actor/run/call lease。绑定身份失败、预热失败、图提交失败、本地 activation 不变量失败和 freeze 后新准入拒绝都有回收测试，不会遗留可接纳工作的部分图。

### Provider、Agent、Conversation 与 OneBot Messaging 图

> **生产接线状态：** Conversation Worker、单 Agent 执行、Context 投影、Provider 调用、最终回复与 Session 交接已经进入同一干净 RuntimeGraph。Agent→Provider 与 Conversation→Agent 使用冻结到准确图/代际/契约版本的 Contract Router 句柄；Conversation 只调用输入选择、最终回复和 Session 交接三个内核 UoW 服务，Agent、Conversation、Memory、Delivery participant 由内核按声明解析。生产模块不再通过共享模块字典、直接运行实例或 bound participant 完成这条链。PostgreSQL 集成测试已从真实 OneBot 入站事实自动生成回复、原子提交 Context/SendIntent、经 Delivery 发送并静默关闭 Session。
当前生产 RuntimeGraph 共 16 个成员：五个内核模块，以及 `provider.chat-completions`、`embedding.multimodal`、`memory.graph`、`media.capture`、`agent.runtime`、`conversation.scheduler`、`conversation.trigger-policy`、`adapter.onebot`、`delivery`、`job.scheduler`、`shell.command` 十一个平级业务模块：
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

Conversation 已增加受精确 Generation 约束的 Worker 内核：它从持久 lane 领取 actor，同群依靠数据库 lease/epoch 串行，不同群受有界并发槽并行；注入的单一 `AgentExecutionPort` 只能在下一次模型调用回调取得新的 `InputBatch`。最终候选后到达的 trigger 会创建同一 Session 的后继 Run，普通尾部在 Session 结束时静默排空。Worker 已不再直接追加回复 Context 或自行提交 `SESSION_END` 选择，也不会在执行异常后擅自把仍有持久 Agent Session 的 Run 退回空闲；它强制依赖 `ConversationCompletionPort` 和 `SessionDecisionPort`。`TransactionalConversationCompletionPort` 逐项核对 Agent completion 与 `FinalReplyPlan` 后调用三方 UoW；`TransactionalSessionDecisionPort` 从已完成 Run 固定 deadline 和稳定操作 ID 后调用 Agent/Conversation 交接 UoW。领取、续租、输入选择和释放仍在调用方事务内复核 Conversation Generation。活跃 Session 本身已作为恢复候选；Worker 重启后读取持久 `session_ref/run_ref` 并恢复原执行链，Agent authority 也能枚举包含已完成但尚未排空 Session 的活动 Run 头。`WAIT_APPROVAL` 不再作为 Worker 失败高频重领：Conversation lane 持久化短期 `resume_not_before` 和原因，释放 actor/并发槽但保留逻辑 Session；期限到达后恢复同一 Run，新消息只继续入队。完成端口必须显式返回实际回答的 trigger 集合，不能依赖丢失即不可恢复的进程内累计列表。`FinalReplyUnitOfWork` 已在写入前复核 Agent、Conversation、Delivery 三个精确 Generation，并在一个 Bot PostgreSQL 事务中把 Run 从 `FINAL_READY` 置为 `COMPLETED`、追加正式 `REPLY`/终结明确 trigger、创建 CHAT SendIntent；Delivery participant 失败时三者整体回滚。`SessionHandoffUnitOfWork` 也已把 `SESSION_END` 的输入选择与 Agent Session 的后继 Run 建立/显式关闭放入同一事务；InputBatch 现在冻结 `context_view_revision`，后继 Run 不再从后来状态猜测模型视图。`DurableAgentModelExecutor` 已按持久 Run 输入 fence 构造 Provider 投影，写入 ModelRequest/TransportAttempt，在 Provider 的发送前许可事务中原子标记 Agent handoff，持久化 SUCCEEDED/NOT_APPLIED/UNKNOWN 观察，解析固定 CorePrompt 协议并把终止回答推进到 `FINAL_READY`；输入版本、trigger 和 InputBatch 任一不符都会在调用前拒绝。FunctionCall 现在冻结准确工具 component/Generation/service/contract 绑定和原触发 principal，并持久化单个 ToolProposal；`DurableAgentToolExecutor` 只接受回显相同输入摘要和 audience Generation 的授权票据收据，通过 Effect 端口的事务回调原子标记 `DISPATCHING`，结果丢失后由新 owner 查询原 effect，不能重新 dispatch；成功载荷必须由工具组件以自身身份发布，协调器不能冒充工具写 payload。`KernelToolAuthorizationPort` 已把可委托的 `tool.invoke` grant 收窄为目标工具的精确 Generation，按策略建立通用批准信封，并在同一事务中单次消费已批准对象和签发短票；批准决定也必须消费准确 scope、决定摘要、计划和主体的 `approval.decide` 票据。票据已经拆分唯一 issuance operation 与最终 effect operation；handoff 前过期会形成新签发 attempt，同一 effect 的并发票据消费由 operation advisory lock 和唯一消费事实确定地收敛。`KernelToolExecutionPort` 已把票据消费、当前 RuntimeGraph fence、Effect handoff 和 Agent `DISPATCHING` 放入一个事务，外部返回丢失时持久化 `UNKNOWN`，恢复只查询原效果；工具结果引用随 Effect evidence 持久化且保持工具组件所有权。恢复投影会携带当前 request/attempt/tool/checkpoint；无法查询的 `DISPATCHING` 模型或工具 attempt 幂等落为 `BLOCKED_UNKNOWN`。协议解析失败会原子保存失败 request/attempt、候选摘要、有界反馈与 checkpoint；同一 Run 只允许一次绑定原失败请求的 `REPAIR` 调用，重启后从 `REPAIR_READY` 继续，修复耗尽则准确终结已领取 trigger 并释放 lane。`conversation.scheduler` 只拥有调度权威；生命周期 Worker 经冻结的 agent.execute/1 句柄调用单一 Agent 状态机，并把最终回复与 Session 交接交给内核 UoW 服务。内核协调器按声明解析 participant，业务模块不持有其他运行实例。PARSING、REPAIR_READY 和 SAFE_BOUNDARY 已支持从持久事实确定性自动重放；RuntimeGraph 现从各模块描述符汇总工具贡献并拒绝全图重名，`agent.runtime` 只消费受限目录和冻结服务句柄。首个只读 `runtime_status` 工具由 `kernel.runtime` 自身贡献，已贯通真实模型调用、授权、Effect、工具所有载荷和结果查询重放。WAIT_APPROVAL 现由跨模块事务先锁定批准对象，再写入带 approval_ref 的精确持久阻塞；批准提交后只提前唤醒匹配的 Conversation，若决定抢先提交则 Worker 在同一 actor 内立即续跑，进程丢失仍由恢复期限兜底。热替换现用 lifecycle activation ID 把旧 Worker 的安全点交接持久化为 `RELOAD_WAIT`，记录原执行阶段和检查点，候选发布或发布失败恢复旧图时只以同一 operation 精确释放 Agent 与 Conversation 阻塞；新宿主启动后也会回收崩溃遗留的交接等待。目标设计不增加拥有聊天语义的生产执行模块。

当前真实网络模块支持正向 Universal WebSocket 与 HTTP action 客户端，启动时调用 `get_login_info` 校验账号。HTTP 上报通过模块端口接入，尚未接入新的生产 Web transport 宿主。

`memory.graph` 与 `embedding.multimodal` 已作为两个独立成员进入干净 RuntimeGraph。Memory 独占图 schema、facet/claim、预算、向量发布和召回语义；Embedding Provider 独占 profile identity、endpoint/凭据、请求投影、HTTP 客户端、传输尝试和用量/结果载荷。Memory 的生命周期 Worker 只持有冻结的 `embedding.encode/1` 服务句柄，Provider 无法读取 Memory 私有表。Provider `SENT` 与 Memory `DISPATCHED` 在同一事务合流；交接后的崩溃只会恢复为 `UNKNOWN`，不会重复推断发送。默认冻结 QiYu 使用的 `Qwen/Qwen3-VL-Embedding-8B`、4096 维 L2 空间；Bot 首次初始化后 deployment、model revision、模型和维度由 Provider identity 与 Memory domain 双重拒绝变更，endpoint/凭据/连接参数可经配置 plan/apply 和新 Generation 替换。OneBot 文本入站已在 Adapter/Conversation/Memory 三参与者事务中登记来源；IDLE、模型安全点与 Session 结束的 InputBatch 选择会在同一事务提交 Memory eligibility，并为新获资格的来源修订创建幂等 `DOCUMENT` embedding facets。`memory.recall/1` 已把精确 InputBatch 物化为 `QUERY` facet，冻结检索包并逐条标注原始群聊和作者；`agent.runtime` 在同一单 Agent 的下一次模型调用前通过冻结路由消费召回，工具循环复用同一输入修订的查询身份。

`media.capture` 现已作为独立 Generation 进入生产图。Adapter/Conversation/Media 四参与者 ingress UoW 原子登记图片任务，规范 Context 只保存稳定 `media_ref`，临时 URL 仅留在 Media 私有 schema。生命周期 Worker 使用租约、重试和资源上限下载并验证图片，把像素保存为 Kernel Payload 后通过 Memory 公共端口发布保持原始段序的 TEXT/IMAGE parts；InputBatch 先于图片完成时沿用 pending eligibility，最终失败显式收敛为 `UNAVAILABLE`。当前 Context 图片由 `media.context.resolve/1` 在模型调用边界物化，历史图片由 EvidenceBundle 物化，二者均使用来源绑定的 `MediaSnapshot`。

`shell.command` 现已作为独立 Generation 和生命周期 Worker 进入同一生产图。稳定 ShellGate 在业务触发前只识别直接 `#forest` 前缀；规范参数、未知命令和拒绝载荷均写入受管 Payload。Adapter、Conversation、Memory、Media、Shell 与拒绝回执由一个入站 UoW 原子合流，因此控制消息仍进入带真实作者的群 Context 和全局记忆，但绝不回退给 Agent。命令目录由各模块通过 `RuntimeModuleDescriptor.shell_commands` 贡献；通用解析器从参数 schema 生成语法，Contract Router 为 `shell.command` 解析准确贡献 Generation 的服务句柄，Shell 本体不含 `core.status` 等命令分支。命令 registry 与管理员策略在 RuntimeGraph publication 事务中绑定准确图、目标 Generation、schema、能力摘要及服务 owner；执行前及提交结果前均重验当前策略、Binding fence、route fence 和一次性 `management.invoke` 票据。P0 claim 先于 P1，CONTROL SendIntent 独立于 CHAT；`core.pause/resume` 由 `kernel.runtime` 贡献的类型化命令服务通过 `maintenance_epoch` CAS 关闭/恢复 DATA admission，关闭期间普通消息不会落业务事件，而 Shell 接纳、`core.status`、`core.resume` 和回执仍可完成。`kernel.audit` 自己贡献 `audit.tail` 及查询服务，支持动作、生产者、来源、资源、操作和稳定游标筛选；Shell 只能经其准确 Generation 端口读取脱敏元数据。每次 Shell 终态、CONTROL SendIntent 与不可变审计 envelope 在同一短事务提交，投递参与者失败时三者整体回滚，重放使用稳定 audit ID 收敛。策略在排队后撤销会以无管理效果的安全失败事实、审计及 CONTROL 回执收敛，不会杀死整个 Worker 或把失败记录伪装成一次获准执行。

`job.scheduler` 现已作为独立 Generation 进入生产图。任务类型由执行模块以 `RuntimeJobTypeContribution` 发布版本、输入 schema、服务、超时、最大尝试和并发约束；Scheduler 通过冻结的 Contract Router 句柄调用准确执行 Generation，不读取或硬编码业务任务。其私有 baseline 保存幂等入队、单调 revision、租约 token、尝试次数、取消意图和不可变终态，任务输入与结果正文由 `kernel.payload` 保存并保留父链。并发 claim 使用 `FOR UPDATE SKIP LOCKED`，旧 claim 的完成会被 token fence 拒绝；租约耗尽而无法证明结果时落 `UNKNOWN`。群聊 `jobs list/inspect/run/cancel` 与模块自带 Web 工作区共用这一 authority；Web 查询支持状态、类型、请求主体、快照和复合游标。首个真实贡献者是 `kernel.runtime` 的只读 `runtime.health-snapshot`，用来验证完整目录、路由、Worker 与结果载荷链。

### 已有干净业务 Authority

以下模块均有独立官方 baseline、Repository 和定向 PostgreSQL 测试；其生产接线范围以上文逐项说明为准：

- `memory.graph`、`embedding.multimodal` 与 `media.capture`：生命周期 Worker、Bot 级冻结语义空间、每日请求/Token 预算、每群主干、图文来源摄入、InputBatch 资格合流、查询向量、Agent 召回、当前图片投影及跨主干关系查询已进入 RuntimeGraph；
- `delivery`：CHAT/CONTROL lane、顺序、Effect 与 Adapter 双 fence、生命周期 Worker、崩溃恢复与 UNKNOWN 对账；
- `shell.command`：Binding/群/主体策略、RuntimeGraph 命令注册表、P0/P1 claim、双阶段授权复核、CONTROL 回执和撤权后安全终结；
- `conversation.scheduler`：连续分类水位、同群 actor lease、安全边界、trigger cut、静默排空和恢复领取。
- `job.scheduler`：模块任务目录、持久排队、租约/代次 fence、重试、取消意图、结果载荷和有界管理查询。

Repository 测试证明各自权威状态机；生产启动链是否切换只按上文列出的 16 成员 RuntimeGraph 与集成测试判断。

## 仍需删除或替换的旧实现

以下对象仍存在于源码或旧测试中，全部失去设计权威：

- `BotStore` 及 `public` 大表；
- `migration_v*`、升级夹具和 writer-fence 升级测试；
- 旧 `OneBotRuntime` 中的 `BindingStore`、`MessageStore`、`ShellIngressGuard`、`DeliveryStore` 和发送 Worker 组合根；
- 旧 `RuntimeModuleRegistry` 路径及旧 RuntimeGraph 表；
- 内核 `RuntimeContractRouter` 只保留准确 Generation 的版本化服务调用机制，不包含 Agent 选择、模板 responder 或聊天推进语义；旧业务 `contract.router` 已删除；
- 固定 Agent/Memory/OneBot/Bot 管理页面和壳层专用 API；
- `BotProvisioner`、Fleet、Restore 和 CLI 对 `BotStore.migrate()` 的调用。

切换时直接删除这些路径并重建开发数据库，不建立桥接层。

生产 `CleanBotHost` 已不再导入或实例化旧 `BotStore`。它使用仅含固定 Bot 身份与受限 PostgreSQL 连接池的 `CleanBotStore`；HostLease 类型也已移入无迁移依赖的 runtime identity 模块。导入 `forest.clean_bot_host` 不会再加载 `forest.storage.bot` 或任何 `migration_v1`—`migration_v26` helper。旧对象暂时只为尚未删除的旧入口和旧测试保留。

可执行 CLI 现只发布 `forest bot` 与离线 `forest plugin init/check/build/lock/export/import`。会启动旧固定业务后台的 `forest serve`、调用旧迁移 Kernel 的 `forest dev`、旧 setup/recovery 命令均已从命令面删除；新控制台由干净 Bot Host 的模块贡献承载。插件 watch 将在接入同一 Generation publication 协议后重新提供，不能复活旧开发宿主。

## 当前阻断项

1. 扩展模块贡献式 Shell 命令：描述符目录、通用 schema 解析、准确贡献 Generation 路由，以及 `core.status`、`core.pause/resume`、`core.reload`、`module.list` 已进入生产路径；`memory.graph` 已独立贡献主干、来源和关系的有界查询，`kernel.audit` 已独立贡献带组合筛选和稳定游标的审计元数据查询；`agent.runtime` 已贡献 `run.inspect` 与 P0 `run.cancel`；`job.scheduler` 已贡献 `jobs list/inspect/run/cancel`。Job 类型由实际执行模块声明，运行时冻结准确执行 Generation，持久队列使用租约、claim token、revision CAS、取消意图和 `UNKNOWN` 终态收敛，Shell 与 WebUI 共用同一控制端口。受控原生进程仍需实现隔离的 `ExecutionProfile` 后端，不能退化为宿主 shell。短回执明确标注来源群或审计主体/资源/操作。目录贡献者自动进入 Shell/Agent/Job 的生命周期依赖与 Generation 描述，图构建会拒绝动态依赖环，旧图按消费者先排空、提供者后退出的顺序关闭。
2. 扩展模块贡献式工具目录的实际工具集合；协议失败、PARSING、REPAIR_READY、SAFE_BOUNDARY、WAIT_APPROVAL 与 RELOAD_WAIT 的生产恢复已落地。热替换已具备候选预热、旧 Worker 安全点排空、带 lifecycle activation ID/CAS 的持久 `RELOAD_WAIT`、候选发布或回滚后的精确释放、宿主崩溃后的遗留等待回收，以及发布后旧资源回收。`WAIT_APPROVAL` 已用带精确批准引用的持久恢复期限替代失败热循环，批准决策提交后会直接提前唤醒匹配阻塞，决定先提交时也不会在其后安装丢失唤醒的阻塞。目录发现、全图冲突拒绝、贡献者所有权核对和动态 Agent service requirements 已落地，`runtime_status` 已贯通真实模型 FunctionCall、准确 Generation 绑定、授权票据、Effect handoff、工具所有结果载荷、下一轮模型输入及查询重放。Adapter 入站已经改由内核 UoW 服务提交，Delivery→Adapter 已使用冻结服务句柄；旧 Bot 组合根和旧业务 `contract.router` 已删除。
3. 继续把多模态 Memory 的规模验证从十万来源扩到百万来源，并补图片字节负载、并发查询和资源上限测试。十万来源的组合筛选、快照稳定分页、两端点关系展开及逐证据群/主干标注已经由真实 PostgreSQL 执行计划测试覆盖；基线索引与公共查询的复合游标顺序一致。
4. 继续扩展模块贡献式管理协议与紧凑 WebUI 壳：当前切片已经从 Registry 动态发现贡献，并提供配置 plan/apply、Generation 热替换、Memory 的主干/来源/关系有界查询、`kernel.audit` 自有的脱敏审计筛选/游标工作区、`agent.runtime` 自有的 PresetContext/CorePrompt 与 Run 控制工作区、`job.scheduler` 自有的类型发现/筛选/详情/运行/取消工作区，以及 `adapter.onebot` 自有的 Binding 状态/fence/端点工作区。deployment scope 也由 Catalog、Secret 和 Provisioning 三个组件贡献紧凑工作区。Agent Run 和 Job 列表均使用服务端组合筛选、快照与复合游标；Memory 工作区逐边显示类型、端点、权重和带群/主干/作者的证据。旧固定业务 DOM、专用 API 和旧大页面已经删除，仍需补 UNKNOWN 对账、跨 Bot 模块工作区路由与完整 Web 身份/细粒度授权。
5. 继续把多 Bot provisioning 和恢复控制面切到干净宿主；独立 `deployment.bot-catalog` baseline、稳定 Bot 身份、存储 profile 引用、组件级 bootstrap profile、持久供应 Job、官方 provisioner、deployment scope 管理贡献、runtime intent CAS、运行观测、Supervisor lease/fence、多宿主独立启停、`forest supervisor` 入口和默认 Compose 编排已经落地。仍需接入动态 PostgreSQL 数据库分配、主密钥多版本 keyring、跨 Bot 管理路由与真实多 Bot 验收；单 Bot `forest bot` 继续作为显式诊断 profile。
6. 补充 OneBot 热替换的长连接抖动、候选 publication 后进程崩溃和持续流量压力验证；正常热替换现已验证候选 BUFFERING、RuntimeGraph/Binding 原子换代、旧客户端 fence、旧图排空，以及新宿主对遗留客户端的失败结算。
7. 删除旧 schema、迁移和失去所有权的代码；旧固定管理栈、旧 Bot 组合根、兼容测试与历史迁移夹具已经删除。
8. 完成真实 QQ 许可群 `933179609` 的低频消息/图片/排队验收；Docker 真实进程已经启动，树莓派 `alicebot.service` 已停止，当前等待人工触发而不由验收脚本主动发送群消息。

## 最近可复现证据

- 文档权威链与 CorePrompt：`14 passed`。
- Runtime Registry、Schema authority、干净 Conversation/OneBot/Delivery/Shell、OneBot codec/WebSocket 和 Agent 协议相关组合：上一基线 `183 passed`。
- Agent/Conversation Worker、持久调度 Repository、十成员 Messaging RuntimeGraph、Provider、Delivery 与生命周期相关定向组合：`124 passed`；其中 Worker 单元用例验证同群互斥、跨群并行、安全点接入、原 Session/Run 恢复、强制事务完成端口、稳定 Session 决策计划，以及 quiesce 时释放正在等待新输入的群 actor；PostgreSQL 用例验证 Agent/Conversation/Provider Generation fence、同 Session 后继 Run、最终回复三参与者事务回滚、Delivery Bot 能力到精确 Adapter Generation 的票据、PREPARED abandonment/requeue、DISPATCHING 崩溃后 UNKNOWN、通用批准的受权决定/单次消费、可委托能力到准确 audience Generation、Effect 与 Agent 原子交接、交接后结果丢失与只查询恢复、Adapter 持久 binding fence、freeze 后拒绝新准入、member fence 排空、来源作者与 trigger 所有权。
- 十一成员 Kernel + Provider + Memory + Agent + Conversation + OneBot + Delivery RuntimeGraph 集成：真实 PostgreSQL 下通过，验证准确代际、text-only 多模态请求投影、OneBot 文本来源/群主干登记、InputBatch 与 Memory eligibility 原子提交、发送前许可、生命周期所属 Delivery Worker 的真实 OneBot wire 调用与 SENT 落账、作者/群来源和逆序关闭。最新 clean authority/RuntimeGraph 定向组合为 `68 passed`；并发测试覆盖 InputBatch 与延迟媒体来源两种完成顺序，最终没有未解析的 `PENDING_CAPTURE`。
- OneBot 单模块生命周期集成：真实 PostgreSQL 下通过，验证 BUFFERING、首次 publication、运行中候选预热、原子客户端换代、旧连接 fence、旧连接异步关闭、新连接继续接纳、quiesce 和 CLOSED。
- [单 Bot 生产宿主](operations-clean-production-launch.md)与自动触发策略：真实 PostgreSQL 下通过。16 成员图可以从空库启动、在同一 HostLease 下热替换并排空旧图、退休、显式释放 HostLease 并立即以新 incarnation 重启；Embedding endpoint 配置 revision 可通过候选 Generation 立即生效而冻结 profile 变更被拒绝；OneBot 图文混排经独立 Media Worker 保序发布、可作为 VL 文档召回且临时 URL 不进入规范 Context；许可群的 mention 由独立 `conversation.trigger-policy` 分类并在 ingress UoW 内原子提交，不再由测试手工制造 Trigger；群 Shell 在 DATA admission 关闭时仍能完成受权状态查询和拒绝回执，普通 DATA 消息保持未接纳；Job Worker 通过模块目录执行 `runtime.health-snapshot` 并持久化结果。
- Shell、干净模块、RuntimeGraph、Schema bundle、Delivery 与 CONTROL/DATA 准入定向组合：`170 passed`；覆盖原子控制入站、P0/P1、授权重验、撤权后安全失败、CONTROL 优先发送、群聊 pause/resume 及 DATA 维护态隔离。
- Shell 审计垂直切片：`28 passed`；真实 PostgreSQL 验证 Shell 终态、CONTROL SendIntent 与 `kernel.audit` envelope 原子合流，失败整体回滚；完整生产图验证 `audit.tail` 的服务句柄归属准确 `kernel.audit` Generation，而不是壳层或 `shell.command`。
- `kernel.audit` 管理贡献：真实 PostgreSQL 宿主集成通过；Registry 只为该准确 Generation 发布审计工作区，通用模块查询网关提供动作、生产者、来源、资源、操作及游标筛选，页面在工作区内分页滚动且不读取详情载荷正文。
- 干净管理面首个垂直切片：真实 PostgreSQL 宿主集成通过，验证管理 token、模块 Generation 贡献发现、Memory 主干/来源筛选、结构错误 422、VL profile 不可变约束、Embedding endpoint plan/apply、RuntimeGraph 原子热替换、固定 `100dvh` PC 工作台及 CSP；Docker Compose 使用完整占位环境变量可通过配置解析。
- `agent.runtime` 管理贡献：真实 PostgreSQL 宿主集成通过，完整 CorePrompt 正文、协议版本、parser revision 与固定摘要可见且不可编辑；PresetContext 经 Agent 自有 Payload authority 生成带来源的新 PayloadRef，再通过配置 CAS 和完整 RuntimeGraph 替换生效。不存在子 Agent、Agent 目录或第二套聊天执行链。Preset bootstrap 只在未初始化 authority 中创建受管 Payload 和 revision 1，宿主重启时外部 seed 漂移不会覆盖模块状态。
- Chat Provider 配置现与 VL Embedding 一样只保存版本化 `credential_ref`，Generation 启动时按引用与配置 revision 解析凭据，不再把密钥绑定在无引用的进程闭包中。`adapter.onebot` 管理贡献：Binding 的账号、连接状态、desired/effective revision、binding/connection epoch、当前 client fence 与端点配置由 Adapter authority 投影；修改先生成有期限的原生 Binding plan，再由 Adapter apply，候选连接在 BUFFERING 中验证账号后随 RuntimeGraph 原子换代。应用回执丢失后用同一幂等键重放不会再替换图。访问令牌不回显；在 Secret authority UI 落地前仅允许沿用现有 secret 引用与 transport。
- 多 Bot 部署控制面首个切片：独立空库 baseline 保存不可复用 Bot 身份、opaque storage profile、期望运行状态和实际运行观测，三者不压成 Bot 配置行；目录操作支持幂等重放与 revision CAS，Supervisor lease 使用 owner epoch 阻止旧进程写观测。`CleanMultiBotSupervisor` 只解析 profile 并管理独立 `CleanBotHost` 生命周期，一个 Bot 启动失败进入带期限退避的 FAILED 观测，不终止其他 Bot。部署 Secret authority 使用 AES-GCM、部署/引用/用途/版本关联数据与主密钥 ID 保存不可变密文版本，写入按 operation 幂等并以 revision CAS 轮换；普通 metadata 不暴露明文或明文摘要。Storage profile authority 将每个 profile 不可变绑定到准确 Bot、storage generation 与 PostgreSQL DSN secret_ref；Catalog 外键和 Supervisor 解析边界同时复核该绑定，settings resolver 不能替换 DSN。不可变 bootstrap profile 同时绑定准确 Bot、storage profile 和 generation，把每个组件的 schema/config 分行保存并校验组件摘要与整单摘要；它只接受非敏感配置和已经存在的 `secret_ref`，模块安装后由各模块自己的配置 authority 接管，部署目录不成为长期巨型 Bot 配置。真实 PostgreSQL 定向组合为 `7 passed`。
- 部署供应 Job：部署库按 Bot 永久保留唯一供应身份，任务冻结 storage/bootstrap profile 与 manifest digest；领取同时受当前 Supervisor lease 和任务 lease epoch 围栏，续租不推进公开 revision，过期 owner 无法结算，失败按有界次数进入 `RETRY_WAIT` 或终态。官方 profile 对 Provider、VL Embedding、触发策略、Shell、Agent preset 和 OneBot Binding 执行准确 schema digest 与 JSON Schema 校验，只通过 Secret authority 解析三类凭据；provisioner 在目标空库安装 16 个模块 authority、写入模块自有 revision 1 与 Binding 配置，但不发布 RuntimeGraph 或建立网络连接。成功后才以稳定 operation 发布带 bootstrap profile/manifest 来源的 Bot `READY`；正式 `forest supervisor` 同时监督供应 Worker 和各 Bot Host。真实 PostgreSQL 定向组合为 `10 passed`，覆盖并发唯一 claim、无语义续租、重试接管、陈旧 claim 拒绝、目标库无活动图和发布顺序。
- deployment scope WebUI：Supervisor 挂载与单 Bot 相同的紧凑壳，但其模块目录由 `deployment.catalog`、`deployment.secrets` 和 `deployment.provisioning` 三个控制面组件贡献；壳层只负责 token、发现和通用 query/action 路由。Catalog 工作区投影 Bot desired/observed 状态并以 intent revision 切换启停；Secret 工作区写入版本化密文且只列出引用、用途、revision 和 key ID；Provisioning 工作区按折叠步骤创建 storage profile、六组件 bootstrap profile 与供应 Job。真实 API 用例验证未授权拒绝、动态贡献资源、Secret 值不回显和官方 schema 发现，deployment catalog 定向组合为 `11 passed`。
- Supervisor Compose：默认服务集合已经切换为 `initialize + postgres + supervisor`，旧单 Bot 入口仅存在于 `single-bot` profile。PostgreSQL 新卷分别创建 `forest_deployment` 与 `forest_bot`；Supervisor 从受保护的数据库口令文件构造部署 DSN，从独占 `0400` 文件读取 Base64 32 字节主密钥和管理 token，三者不进入容器环境或对象 repr。独立 `forest_supervisor_smoke` project 的真实冷启动验证 PostgreSQL/Supervisor 均为 healthy、loopback 管理 API 返回三个 deployment 组件，随后已定向清理隔离容器、网络和卷；现有在线单 Bot project 未受影响。
- 干净生产存储入口：真实 PostgreSQL 下 `CleanBotStore` 从空库完成 kernel baseline、16 个模块 bundle、运行图多次热替换和重启；定向导入探针确认生产宿主不加载旧 `BotStore` 或 26 代迁移 helper。
- 干净 CLI/镜像入口：本地 Docker runtime 镜像 `forest-goal:repair-ready` 与 test target 构建成功，容器直接从不可变的 `/app/src` 启动 `python -m forest bot`；依赖按 lock 在源码复制前安装，源码变化不再触发未锁定的 `hatchling` 下载。容器内 CLI 只暴露 `bot` 与离线 `plugin` 制品命令；旧 `serve/dev/setup/recovery` 不再能启动错误架构。Compose 不再把模型、Embedding、OneBot 或管理端凭据注入环境，四者通过无网络 bootstrap 容器从标准输入原子写入专用 secret volume，并在每次启动前复核文件类型、所有权、权限、大小和单行可打印 ASCII 约束；安装器会移除 Windows PowerShell 管道添加的单个 UTF-8 BOM。虚构凭据的初始化、逐项安装、重验和临时卷清理已在 Linux 容器通过。插件 scaffold/declaration 组合为 `35 passed, 2 skipped`。
- 旧管理栈切除：`src/forest/management`、旧 Bot 默认模块图与组合根、旧业务 `contract.router`、固定页面/API 测试、兼容迁移测试及历史夹具已物理删除；`RuntimeModuleRegistry` 必须显式接收 Generation 描述符，不再存在旧图 fallback。裁剪后的权威套件在真实 PostgreSQL 下为 `543 passed, 4 skipped`，Ruff、Python 编译、管理端 JavaScript 语法和干净导入边界均通过；`forest:local` 从干净入口构建成功并通过真实 Supervisor Compose 冷启动。该基线包含模块贡献式 Agent 工具、Shell 命令和 Job 类型目录及其派生生命周期依赖、Memory 主干/来源/关系群聊查询、`module.list`、`audit.tail`、`run.inspect`、`run.cancel`、`core.reload` 与 `jobs list/inspect/run/cancel` 群聊控制命令、Shell 结果/回执/审计三方原子合流、Agent Run 和 Job 管理工作区，以及 `runtime_status` 的真实模型调用、RuntimeGraph、授权、Effect、结果载荷、下一轮模型输入和查询重放集成验证。Job authority 增加幂等入队、并发唯一 claim、Generation/member fence、租约接管、revision CAS 取消、重试、受管输入/结果和不确定终态；租约续期不推进公开语义版本，过期 owner 无法续租或结算，取消与成功按同一任务行锁确定唯一终态。RuntimeGraph 重载请求继续使用 graph revision CAS、稳定 activation ID、持久领取租约和宿主监督任务。Conversation poller 在 DATA 维护态关闭期间保持存活，恢复后可继续领取持久 trigger；协议错误保留准确 request/attempt、候选摘要和有界反馈，只允许一次可跨重启恢复的 REPAIR；热替换在候选发布前冻结并排空旧 Worker，Agent 以 operation 绑定的 `RELOAD_WAIT` 检查点持久交接；`WAIT_APPROVAL` 使用精确批准引用和提交后通知提前唤醒，同时保留期限恢复。
- Memory 十万来源规模基线：单个真实 PostgreSQL 测试库写入 `100,000` 个来源单元及内容部件、`20,000` 个关系贡献和证据，验证主干/作者/模态/类型组合筛选、消息与关系的快照复合游标续页、无重复页、每条关系证据的群号与主干标注。`EXPLAIN (FORMAT JSON)` 证明来源查询不对 `source_units` 做顺序扫描，关系查询同时使用左右端点索引；定向用例约 `18s`。
- Linux 容器调度复核发现并修复 Conversation Worker 的唤醒串扰：Runtime 激活只唤醒全局 durable-work poller，入站/退出另用 active-session 输入信号，避免新 Session 的 `WAIT_INPUT` 消费遗留激活 Event 后无消息空转。Agent/Conversation 组合在三个并行 Linux 容器中各自 `20 passed`。
- 真实单 Bot 实例：树莓派旧 `alicebot.service` 保持 `inactive`，PC 通过 SSH loopback 隧道连接其 OneBot；全新 PostgreSQL 卷上的 `forest-forest-1` 已发布包含 `shell.command` 在内的 15 成员 RuntimeGraph 并持续为 `healthy`，WebUI `127.0.0.1:8080` 返回 200。启动过程实际发现并修复 Windows PowerShell secret 管道 BOM 污染；固定后的四类凭据均为应用 UID 独占 `0400` 且不进入容器环境。独立低成本探针确认 `deepseek-flash` 接受当前 SSE Chat Completions 参数，QiYu 的 `Qwen/Qwen3-VL-Embedding-8B` 返回准确 4096 维向量。尚未由自动验收向 QQ 发送消息。
- 最近一次无 PostgreSQL 全套证据为 `755 passed, 36 skipped, 1197 deselected`；它包含大量旧实现测试，只用于回归发现，不能作为目标架构完成证明。

测试数字只对应相应提交和命令。真实 QQ 低频交互、长时间连接、百万级记忆以及多 Bot/WebUI 控制面 cutover 尚未完成。
