# Forest 当前实现状态

更新日期：2026-09-17。

## 结论

Forest 正在进行激进重构，**不可发布**。已有测试只能证明若干纵向协议曾经工作，不能证明当前代码已经形成目标架构，也不能把“能创建 Bot、配置 Agent 和浏览记忆”解释成微内核已经完成。

当前唯一有效的架构基线是[微内核运行时与控制面重构](13-microkernel-control-plane-redesign.md)：

- 微内核只负责模块注册、依赖图、生命周期、契约路由、能力授权、持久任务和效果出口；
- Agent、Memory、OneBot、Scheduler、Shell、Delivery 都是平级可安装模块；
- `BotRuntimeSupervisor` 只装配冻结的模块图，不能成为上述业务对象的组合根；
- WebUI 是控制台宿主，按运行时模块描述符发现配置、诊断和管理视图，不能硬编码 Agent/Memory 产品栏目；
- 每个 `(bot_id, conversation_id)` 只有一条持久 Agent 执行链，不存在产品“子 Agent”；
- 项目未上线，允许删除错误 API、表、页面和测试并重建开发数据库，不为未发布结构增加兼容迁移。

## 已落地且仍可复用的能力

以下能力有实现与定向测试，但仍须在新模块边界下复验：

- 多 Bot 数据作用域、OneBot 入站归档、全量群消息作者来源；
- 同群 Agent 串行、跨群并行，以及下一次模型调用安全点接入排队消息；
- 固定 CorePrompt、独立 PresetContext、原子 ModelTurn、工具闭合、取消和审批；
- 每 Bot 多群主干、图文来源、图文嵌入/召回、跨主干人工联想、删除 epoch 与遗忘闭包；
- 群聊 Shell 的固定 Core Gate、类型化命令、优先级、作用域授权和审计；
- 组件 manifest、依赖 DAG、代际发布、配置 plan/apply、持久事件和 Job；
- 可靠发送、管理票据、审计、备份与隔离恢复的若干纵向协议。

这些实现中仍有宿主直接持有业务对象和固定页面入口。它们是必须继续删除或迁移的重构输入，不是兼容资产，也不是最终结构；旧 Child 表、迁移与存储 API 已在本轮从最终基线删除。

## 本轮已经完成的重构切片

- 文档入口已经按“13 号纠偏基线 → 通用架构/协议 → 专项设计 → 实现状态”建立明确权威顺序；P0–P5 实现切片和旧迁移调查不再与目标架构并列，也不能生成兼容任务。
- WebUI 设计已删除固定 Agent/Memory/LLM 页面和专用 Memory 查询端点，控制台只保留内核导航、RouteGraph 与通用模块工作区；官方模块同样通过描述符、schema、query/action 和沙箱 UI 暴露管理能力。
- 历史载荷文档已经删除产品 Child 数据流和“先迁移开发期旧表”的实施措辞；当前未发布数据库只接受干净最终基线。
- 官方核心模块进入显式注册表，并声明稳定 `component_id`、依赖和能力；
- 通用管理入口按 `(bot_id, component_id, operation_id)` 解析模块的 query/action contract；
- Agent 与 Memory 的专用 `/agent/*`、`/memory/*` 管理端点已删除；
- 模块描述符可投影管理操作目录和 `management_ui` 工件；
- 控制台使用受限 iframe、摘要校验和有界 `MessageChannel` 装载模块 UI；
- `BindingRuntime` 的若干 Agent/Memory/Scheduler/Shell 便利属性已删除，调用方开始按组件 ID 取能力。
- `BotModuleRegistry` 已拥有严格生命周期状态；失败模块会清理自身，已启动依赖按逆序排空，关闭失败可重试，控制面不再采用模块自报 `state`。
- `adapter.onebot` 已拥有 Binding/Inbox/Shell ingress/Delivery 端口、连接集合、候选切换、网络接纳、发送 Worker 与连接监督任务；`memory.graph` 已拥有来源回填；`command.router` 与 `message.router` 已拥有各自策略和容量存储。`BindingRuntime` 对应便利属性及 OneBot 私有状态已删除。
- 可安装组件的 service 声明与依赖要求已成为不可变 Generation 事实；service head 与组件 route、command、message handler、event、job 在同一发布事务切换，普通 service 调用会核对持久 head、Generation 和 route epoch。
- `routing.graph` 已作为官方模块注册，通过通用 management contract 输出 `REPEATABLE READ` 的确定性 RouteGraph；WebUI 视图由模块描述符装载，可筛选节点并查看依赖/事件边及缺失、不兼容、歧义状态。
- 新消息接纳会在既有 Bot runtime/注册表锁序下保存不可变 `graph_digest`；稳定路由按 registry revision 与 route 摘要直接复用内容寻址快照，只有路由键变化才重建全图。`routing.graph` 可按会话、Binding、群、消息、组件、代际、结果和时间读取由现有权威事实投影的 dispatch trace，并回看消息当时的冻结图。Trace 已沿显式身份扩展 Kernel call 树、component event/delivery、component job/执行 call 和外部 SendPart/attempt/resolution；SendIntent 强制保存类型化 `cause_kind/cause_id`，不再从字符串或载荷猜来源，UNKNOWN 发送状态保持不确定。
- 官方运行时模块已拥有持久 RuntimeGraph/Generation、依赖边、追加式生命周期事实和原子 graph head。启动全部成功后才发布整图；失败启动不暴露部分模块；关闭先撤下 head 再逆序排空。RouteGraph 冻结 runtime revision，Adapter/Shell/Agent trace 从消息旧快照解析代际，宿主换代后不会改写历史。
- 产品 Child Agent 已从最终基线删除：019/032/050 Child 迁移、两张 Child 表、数据搬运器、旧配置投影和冻结升级夹具均不存在；Kernel 内部组件调用统一使用 nested call 命名，Run token 账户改为 `bot_agent_run_budgets`。

这只是控制面所有权、传输和模块路由的收口。运行时组合根、官方/可安装驱动的统一健康与恢复策略、旧 `bot_outbox_events` 的来源归一、面向数十万消息的 lineage 游标索引与资源归属尚未全部迁完；当前 trace 对深层筛选采用有界候选扫描并明确报告截断，不能作为全量历史查询的最终实现。

本次 Memory 管理切片在 PostgreSQL 17 上完成 109 项组合回归，覆盖来源授权、媒体、图归约/存储、组合筛选、游标绑定、主干统计、关系投影、实际 Web 管理路由和模块描述符；定向 Web 回归为 3 项。真实 Edge 在 1440×1100 PC 视口验证默认只展开“来源、主干与遗忘”，页面高度小于 2500 px，配置、状态、人工关系和发布图按需折叠，同时保留配置草稿、联想变更、延迟响应与 Bot 切换保护。该证据不包含十万至百万来源容量压测、真实图片查询入口或关系图布局。

本次 OneBot 所有权切片把连接集合、候选发布/恢复、HTTP/WebSocket 接入、Delivery 准备与两个根监督任务迁入 `adapter.onebot`。PostgreSQL 定向回归包括运行时/接入/关闭 44 项、记忆运行时 4 项、Agent 记忆 2 项、控制发送 3 项以及 Web Binding/Fleet 18 项；无数据库全套为 803 通过、1069 跳过。尚未执行完整 PostgreSQL 全套、真实 QQ 低频验收或长时连接抖动测试，因此这些结果不构成发布结论。

本次持久 RouteGraph 切片完成 service Generation 事实、原子 service head 发布、调用前路由核对和模块化只读图视图。PostgreSQL 验证分别为 RouteGraph 定向 2 项、干净数据库门禁 1 项、Kernel/service 36 项、平台与 Web 20 项、跨注册表存储 61 项、writer fence 9 项；压平基线后的 `test_web_packages.py` 复跑 4 项通过。随后补齐了消息入场 `graph_digest`、按摘要读取历史图和 dispatch trace 投影；定向数据库门禁/消息/RouteGraph 3 项、Web 模块契约与静态视图 3 项、Job 触发 18 项、P3/P4 基线 12 项通过。消息并发全套先发现整事务 `REPEATABLE READ` 会让重复接纳出现 serialization failure；改为沿既有 runtime/registry 锁序冻结图后，35 项全部通过；业务命令和消息 Handler 的真实执行 trace 各有定向用例。当前无数据库全套为 711 通过、36 跳过、1108 因 PostgreSQL marker 未选择。上述集合有重叠，不能相加。仍需完成官方运行时模块持久图、effect/outbox trace 引用和完整 PostgreSQL 全套。Ruff、Python compileall、RouteGraph JavaScript 语法检查和 `git diff --check` 通过。

本次单 Agent 基线清理删除 019/032/050 Child 迁移、Child payload handler、冻结 041 兼容包和只服务旧 Child/配置升级的测试；保留的组件递归调用改称 nested call，Run 预算表与模块也改用 run 命名。干净数据库门禁明确检查 Child 表和迁移不存在。PostgreSQL 回归为 Agent/配置/nested call/正文授权 63 项、配置与当前基线 19 项、P3/P4 保留不变量 12 项、Agent payload/Kernel/service/Web 77 项以及更新后的 Agent 配置 19 项；集合有重叠。无数据库回归为 711 通过、36 跳过、1105 因 PostgreSQL marker 未选择。尚未执行完整 PostgreSQL 全套。

本次官方运行时持久图切片在 PostgreSQL 17 上完成 runtime registry/graph、消息 RouteGraph/trace、平台生命周期和 writer-fence 组合回归 71 项；另有启动失败不发布、关闭失败重试、宿主失租接管以及换代后旧消息仍绑定旧 Adapter Generation 的定向用例。无 PostgreSQL 套件为 711 通过、36 跳过、1113 因 marker 未选择；Ruff 与 Python compileall 通过。该集合尚未覆盖完整 PostgreSQL 全套、长时间反复失租/数据库分区和 effect/outbox trace。

本次执行因果切片把 Kernel call 树、component event/subscriber delivery、component job/worker call 和外部发送 part/attempt/resolution 接入只读 dispatch trace，并把 SendIntent 的类型化原因写入干净数据库基线。PostgreSQL 组合回归为 105 项通过，另有 Web 模块回归 4 项通过；无 PostgreSQL 套件为 712 通过、36 跳过、1114 因 marker 未选择。Ruff、Python compileall、RouteGraph JavaScript 语法和 `git diff --check` 通过。该证据没有完成旧 `bot_outbox_events` 来源归一、lineage 游标索引、十万级容量压测或完整 PostgreSQL 全套。

## 阻断项

### P0 微内核

- `BindingRuntime` 仍负责跨模块会话执行、组件 Worker 确保和总体健康汇总；这些剩余职责需要迁入 Contract Router、组件自己的依赖端口及通用 supervisor；
- 官方模块已发布独立的持久 RuntimeGraph/Generation 并进入同一 RouteGraph；它和可安装 Worker 仍使用不同运行驱动，需统一健康检查、按依赖边故障隔离、失租恢复和监督器接口；
- command、message handler、service、event、job 与官方 runtime module 已进入同一可解释 RouteGraph，消息入场冻结精确 `graph_digest`；Kernel call、component event/job 与 Delivery 因果边已进入 trace，仍需归一旧 `bot_outbox_events` 来源；
- dispatch trace 已能按消息、群、组件、代际、结果和时间查询，且与可编辑业务配置分离；仍需用不可变 lineage 索引和稳定游标替换深层筛选的有界候选扫描，完成十万级容量验证；
- 模块资源所有权仍需继续清理，宿主不得越过契约持有 JobStore、AgentStore 或 Memory 内部对象。

### P1 群聊调度与 Shell

- 必须保持一群一 Agent 执行链：工作期间的新触发只排队；当前模型流/工具步骤结束后的下一次模型调用接入截至最后触发记录；未触发尾部在 Agent 静默结束后排入短期上下文；
- 历史上下文必须保留每条消息的稳定作者身份和群来源；
- Shell Gate 优先处理安全与管理命令，普通业务命令与 Agent 触发按冻结路由和优先级执行，不能启动另一条 Agent；
- 需要在新模块边界下重跑同群竞态、跨群并发、热重载接管和真实 QQ 低频验收。

### P3 Agent 模块

- Agent 必须忠实采用 QiYu Agentlib 的单执行链语义：核心提示词、上下文投影、模型调用、工具调用、checkpoint 和恢复属于同一个 Agent；
- Agent 只是 `conversation.scheduler` 的消费者以及 Memory/Tool/Delivery 契约的调用者，不能承担框架调度或模块管理。

### P4 多模态多主干记忆

Memory 管理契约已补齐群主干目录、来源组合筛选、稳定键集分页、人工关系筛选和实际发布图的 scope/节点/边遍历。控制台已使用折叠配置、紧凑筛选网格和分页结果，关系 DTO 会明确审核来源、贡献、图作用域、投影版本和跨主干状态。以下剩余项目仍为发布阻断项：

- 用十万至百万来源数据验证现有索引、组合筛选、键集分页和有界正文读取的延迟与内存上限；
- 文本和图片查询入口、混合召回解释和跨主干关联路径可视化；
- 发布图遍历结果的删除/抑制诊断以及面向人类的关系图布局；
- 记忆异步写入的幂等合流、乱序提交和删除 epoch 防复活证明；
- LLM 不支持多模态时，在每次 Provider 调用前投影掉图片正文，同时保留允许的文字来源说明；
- 裁剪不得调用“记忆摘要”模型或增加摘要 API 调用。

VL 嵌入模型的种类在 Bot 创建时冻结并与 Bot 生命周期绑定；创建后只能更换兼容 endpoint/凭据，不允许改模型种类或向量语义。

### P5 控制台

- 顶层只展示部署、运行实例、模块注册表、操作追踪、安全和存储；Agent/Memory/Adapter 只能作为模块出现；
- 模块配置由 schema 或模块携带的受限 UI 呈现，宿主不能按模块 ID 硬编码业务表单；
- Memory 模块 UI 已有主干、组合筛选、分页和发布图浏览；仍需关系图可视化并用普通 PC 视口完成浏览器验收；
- WebUI、群聊 Shell 和自动化入口必须调用同一类型化控制契约和授权模型。

## 验收规则

只有满足以下条件后才能重新标记为发布候选：

1. 第 13 号文档的退出条件全部有代码与测试证据；
2. 干净数据库从最终基线创建，不依赖错误架构的兼容层；
3. 无数据库与 PostgreSQL 全套回归通过；
4. 模块 UI、路由、生命周期、同群排队、跨群并发、记忆写合流及图文召回完成集成测试；
5. Docker Compose、真实 OneBot 许可群低频交互和重启恢复通过；
6. 状态文档记录当次提交、命令、通过数和明确未覆盖边界。

旧测试数字、旧发布候选结论和旧迁移升级记录已从本文删除，避免它们被当作当前架构完成证明。需要追溯时使用 Git 历史。
