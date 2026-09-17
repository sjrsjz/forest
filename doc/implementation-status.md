# Forest 当前实现状态

更新日期：2026-09-18。

> 本页只记录当前工作树事实，不定义目标架构。发生冲突时依次以 [00 架构总契约](00-architecture-contract.md)、[13 微内核控制面重构](13-microkernel-control-plane-redesign.md) 和 [14 干净 Schema 与载荷来源](14-clean-schema-and-payload-provenance.md) 为准。Forest 尚不可发布。

## 不可改变的当前语义

- Agent 是 `agent.runtime` 模块中的单会话状态机，不存在产品子 Agent。
- 每群至多一条活动 Agent 响应链；群之间可以并行。新触发在下一次模型调用边界接入截至最后一个触发的连续消息前缀，普通尾部在终止后静默排空。
- 每个 Bot 只有一个全局记忆域，每个群是一条独立主干；联想可以跨主干，召回必须携带来源群和作者。
- 裁剪不调用 LLM，也不生成记忆摘要。文本与图片均进入权威载荷、嵌入和检索；text-only Provider 只在单次请求投影中移除图片。
- VL 嵌入空间与 Bot 生命周期绑定，只允许更换兼容 endpoint 或凭据。
- WebUI 壳只负责身份、作用域、模块发现和受控工作区；业务页面、schema、查询和动作由准确模块 Generation 贡献。
- 未发布结构直接删改并重建开发数据库，不新增迁移、兼容读取、双写、旧 API 转发或 fallback。

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
`provider.chat-completions`、`agent.runtime`、`conversation.scheduler` 和 `adapter.onebot` 已经作为四个平级业务模块与五个内核模块共同进入一张九成员 RuntimeGraph：
- `provider.chat-completions` 独占共享 HTTP 客户端、端点/模型能力快照和单次 `ModelInputProjection`；每个请求绑定精确 Provider Generation 与能力摘要，实际发送前在短许可事务中再次复核 DATA admission；
- text-only Provider 的请求投影会递归移除图片、受管媒体 URL/base64/file 字段，同时保留作者、来源群、trigger 与不可见模态标记；该投影不修改权威多模态 Context；
`agent.runtime`、`conversation.scheduler` 和 `adapter.onebot` 已经作为三个平级业务模块与五个内核模块共同进入一张八成员 RuntimeGraph：

- `agent.runtime` 的全部状态变更经准确 Agent Generation 在最终数据库事务内复核；
- Agent `COMPLETED` Run 不再擅自关闭 Session；Conversation 排空决定显式关闭，或在同一 Session 下原子推进唯一后继 Run；
- `conversation.scheduler` 的接纳与分类端口检查自身准确 Generation；
- `adapter.onebot` 只拥有 Binding、配置修订、客户端代际、平台事件、网络客户端和 OneBot transport；
- Adapter 不拥有 Conversation lane、Shell、Agent、Memory、Delivery 或发送账本；
- OneBot 客户端先以 `BUFFERING` 绑定配置 revision、Adapter Generation 和 executor fence；
- Binding client publication 作为 RuntimeGraph 事务 participant，与 Adapter Route 一起提交；
- WebSocket 在预热期只缓存，图提交后才打开事件流；
- 旧 Adapter Generation 即使仍持有 Binding epoch，也不能接纳事件或发送；
- raw event、normalized parts 和 author snapshot 先成为带来源闭包的 payload，再由一个调用方 UoW 原子提交 Adapter platform event 和 Conversation Inbox；
- Binding head、client 和事件接纳已统一锁序，同一账号不同群可以并行。

Conversation 已增加受精确 Generation 约束的 Worker 内核：它从持久 lane 领取 actor，同群依靠数据库 lease/epoch 串行，不同群受有界并发槽并行；注入的单一 `AgentExecutionPort` 只能在下一次模型调用回调取得新的 `InputBatch`。最终候选后到达的 trigger 会创建同一 Session 的后继 Run，普通尾部在 Session 结束时静默排空。领取、续租、选择、Context 追加、失败重排和释放都在调用方事务内复核 Conversation Generation。活跃 Session 本身已作为恢复候选；Worker 重启后读取持久 `session_ref/run_ref` 并恢复原执行链，Agent authority 也能枚举包含已完成但尚未排空 Session 的活动 Run 头。完成端口必须显式返回实际回答的 trigger 集合，不能依赖丢失即不可恢复的进程内累计列表。`FinalReplyUnitOfWork` 已在写入前复核 Agent、Conversation、Delivery 三个精确 Generation，并在一个 Bot PostgreSQL 事务中把 Run 从 `FINAL_READY` 置为 `COMPLETED`、追加正式 `REPLY`/终结明确 trigger、创建 CHAT SendIntent；Delivery participant 失败时三者整体回滚。`SessionHandoffUnitOfWork` 也已把 `SESSION_END` 的输入选择与 Agent Session 的后继 Run 建立/显式关闭放入同一事务；InputBatch 现在冻结 `context_view_revision`，后继 Run 不再从后来状态猜测模型视图。`DurableAgentModelExecutor` 已按持久 Run 输入 fence 构造 Provider 投影，写入 ModelRequest/TransportAttempt，在 Provider 的发送前许可事务中原子标记 Agent handoff，持久化 SUCCEEDED/NOT_APPLIED/UNKNOWN 观察，解析固定 CorePrompt 协议并把终止回答推进到 `FINAL_READY`；输入版本、trigger 和 InputBatch 任一不符都会在调用前拒绝。恢复投影会携带当前 request/attempt/tool/checkpoint；新 owner epoch 遇到无法查询的 `DISPATCHING` Chat Completions attempt 时，幂等落为 `BLOCKED_UNKNOWN`，不会自动重发。当前 Worker 尚未接入这个执行器、工具 Effect/授权参与者以及最终提交协调器，因此生产启动图没有启用 Worker 任务。

当前真实网络模块支持正向 Universal WebSocket 与 HTTP action 客户端，启动时调用 `get_login_info` 校验账号。HTTP 上报通过模块端口接入，尚未接入新的生产 Web transport 宿主。

### 已有干净业务 Authority 候选

以下模块已经有独立官方 baseline、Repository 和定向 PostgreSQL 测试，但尚未全部成为生产 RuntimeGraph 中的完整 Generation/Worker：

- `memory.graph` 与多模态检索：Bot 级冻结语义空间、每群主干、图文来源、跨主干关系、遗忘 epoch、text/image/mixed 检索和 embedding 批次合流；
- `delivery`：CHAT/CONTROL lane、顺序、Effect 与 Adapter 双 fence、UNKNOWN 对账；
- `shell.command`：Binding/群/主体策略、P0/P1 claim、授权复核和控制回执；
- `conversation.scheduler`：连续分类水位、同群 actor lease、安全边界、trigger cut、静默排空和恢复领取。

这些 Repository 测试证明的是各自权威状态机，不代表生产启动链已经切换。

## 仍需删除或替换的旧实现

以下对象仍存在于源码或旧测试中，全部失去设计权威：

- `BotStore` 及 `public` 大表；
- `migration_v*`、升级夹具和 writer-fence 升级测试；
- 旧 `OneBotRuntime` 中的 `BindingStore`、`MessageStore`、`ShellIngressGuard`、`DeliveryStore` 和发送 Worker 组合根；
- 旧 `RuntimeModuleRegistry` 路径及旧 RuntimeGraph 表；
- 固定 Agent/Memory/OneBot/Bot 管理页面和壳层专用 API；
- `BotProvisioner`、Fleet、Restore 和 CLI 对 `BotStore.migrate()` 的调用。

切换时直接删除这些路径并重建开发数据库，不建立桥接层。

## 当前阻断项

1. 将 `memory.graph`、`delivery`、`shell.command` 和必要的 Provider/Embedding 模块接入相同的 `prepare → bind → prewarm → atomic publish → local activate` 协议。
2. 将已验证的 `conversation.scheduler` Worker 接到 `DurableAgentModelExecutor`、工具 Effect/授权参与者、Delivery 与最终回复/Session 交接 UnitOfWork；补齐工具执行、授权及 WAIT_TOOL/WAIT_APPROVAL 恢复，不得复用旧组合根。
3. 把完整多模态 Memory/Embedding Worker 接入 RuntimeGraph，并做十万至百万来源的筛选、稳定分页、关系展开和资源上限测试。
4. 建立模块贡献式管理协议与新 WebUI 壳；删除当前固定业务 DOM、专用 API 和旧大页面。
5. 将 provisioning、启动、恢复和 Docker 入口切到最小 bootstrap + 所选模块 bundle + 冻结 RuntimeGraph。
6. 完成 OneBot 热替换时旧客户端排空、新候选 publication、崩溃恢复和长连接抖动验证。
7. 删除旧 schema、迁移、兼容测试和失去所有权的代码。
8. 完成 Docker、完整 PostgreSQL、真实 QQ 许可群 `933179609` 的低频验收；真实启动前先停止树莓派 `alicebot.service`。

## 最近可复现证据

- 文档权威链与 CorePrompt：`14 passed`。
- Runtime Registry、Schema authority、干净 Conversation/OneBot/Delivery/Shell、OneBot codec/WebSocket 和 Agent 协议相关组合：上一基线 `183 passed`。
- Agent/Conversation Worker、持久调度 Repository、九成员 Messaging RuntimeGraph、Provider、Delivery 与生命周期相关定向组合：`82 passed`；其中 Worker 单元用例验证同群互斥、跨群并行、安全点接入、原 Session/Run 恢复和后继 Run，PostgreSQL 用例验证 Agent/Conversation/Provider Generation fence、同 Session 后继 Run、最终回复三参与者事务回滚、freeze 后拒绝新准入、member fence 排空、来源作者与 trigger 所有权。
- 九成员 Kernel + Provider + Agent + Conversation + OneBot RuntimeGraph 集成：真实 PostgreSQL 下通过，验证准确代际、text-only 多模态请求投影、发送前许可、延迟开放 WebSocket、作者/群来源和逆序关闭。
- OneBot 单模块生命周期集成：真实 PostgreSQL 下通过，验证 BUFFERING、原子 publication、事件 UoW、quiesce 和 CLOSED。
- 最近一次无 PostgreSQL 全套证据仍为 `747 passed, 36 skipped, 1184 deselected`；它包含大量旧实现测试，只用于回归发现，不能作为目标架构完成证明。

测试数字只对应相应提交和命令。真实 QQ、长时间连接、完整 Docker 启动、百万级记忆以及生产 cutover 尚未完成。
