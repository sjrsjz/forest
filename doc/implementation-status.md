# Forest 当前实现状态

更新日期：2026-09-17。当前提交基线：`01300ca`。

## 结论

Forest 正在进行激进重构，**不可发布**。本页只记录代码事实和验证证据，不定义目标架构；目标架构以[微内核运行时与控制面重构](13-microkernel-control-plane-redesign.md)和[干净数据库基线](14-clean-schema-and-payload-provenance.md)为准。

当前代码已经具备多 Bot、群聊单执行链、Agentlib 风格 Agent、Shell、记忆图、模块代际和部分模块化控制面，但仍有三个根本阻断项：

1. Bot 创建、挂载和恢复仍调用巨型 `BotStore.migrate()`，多数官方模块业务表仍位于历史 `public` schema；
2. 通用宿主仍残留旧组合根留下的唤醒、健康和资源耦合，尚未完全收口为不理解业务模块的 `BotRuntimeSupervisor`；
3. Web 壳仍安装一批 Bot/Event/ComponentJob/策略/诊断/审计/备份/账号专用路由与面板，尚未完全由作用域和 `ManagementContribution` 驱动。

因此，能在 WebUI 中创建 Bot、配置 Agent、查看 Memory 或运行 OneBot，不能作为微内核完成的证据。

## 必须保持的语义

- Agent、Memory、OneBot、Conversation Scheduler、Shell、Job 与 Delivery 是平级模块；Agent 只执行一个会话的模型/工具状态机。
- 每个 `(bot_id, conversation_id)` 同时只有一条持久 Agent 执行链；不存在产品子 Agent。
- Bot 是身份、配置、授权、预算和全局记忆的作用域，不是后台导航中心或业务对象组合根。
- 一个 Bot 只有一个全局记忆域；每个群是其中一条独立主干，联想可以跨主干，召回逐项标出来源群和作者。
- 所有群消息进入上下文流水；Agent 忙碌时新触发排队，在下一次模型调用前接入截至最后触发记录的连续前缀；无新触发时静默排空尾部。
- 裁剪不调用 LLM、不生成记忆摘要。记忆和嵌入完整保留文本与图片；文本模型只在单次请求投影中移除图片载荷。
- VL 嵌入模型种类和语义空间与 Bot 生命周期绑定；创建后只允许更换兼容 endpoint 或凭据。
- WebUI 壳只管理身份、作用域、模块发现和工作区；业务配置、查询、动作与 UI 由准确模块 Generation 贡献。
- 未发布结构不保留兼容层。错误 API、表、迁移、页面、DTO、测试和开发数据库可直接删除或重建。

## 已完成且可继续复用

### 模块运行时与路由

- `BotModuleRegistry` 实现 `DISCOVERED → VALIDATED → PREPARED → ACTIVE → QUIESCING → QUIESCENT → STOPPED/FAILED`，按依赖顺序启动、反向排空，并记录持久 RuntimeGraph/Generation。
- 官方描述符携带 `package_id/version/digest/scope/driver/trust_level`；工件摘要进入 Generation、管理投影和 RouteGraph 历史。
- command、message handler、service、event、job 和官方 runtime module 已进入同一 RouteGraph；消息入场冻结 `graph_digest`。
- `component.worker-host` 拥有可安装 Worker 的准备与启动；`contract.router` 拥有冻结后的命令、Handler、Agent 和模板 responder 分派。
- `scheduler.jobs` 拥有 JobStore、授权适配、六项管理操作和自己的管理 UI；旧 Job 专用管理路由与壳层面板已删除。

### Agent、群聊与 Shell

- 固定 CorePrompt、独立 PresetContext、原子 ModelTurn、单工具步骤、checkpoint、取消和审批已实现；Child Agent 的表、迁移、配置、handler 和升级夹具已删除。
- 同群串行、跨群并行、下一模型调用安全点接入新触发、作者/群来源保留和静默排空已有实现与定向测试。
- ShellGate、保留命令、独立 CONTROL 回执、当前 Binding 管理员授权和持久控制请求已有实现。

### 记忆与多模态

- Bot 全局域、每群主干、稳定来源单元、人工跨主干联想、删除 epoch、图文媒体来源和图文嵌入管线已有实现。
- Memory 管理契约已提供主干目录、来源组合筛选、稳定键集分页、人工关系筛选和发布图节点/边遍历。
- 现有列表和 JSON 图遍历尚不等于图片查询式召回、混合召回解释、关系图可视化或大规模容量证明。

### 控制面

- 运行时描述符支持多个带作用域的 `management_contributions`；Bot scope 可从统一目录发现，并经通用 query/action 网关调用模块契约。
- Agent、Memory、Routing、Scheduler 与 OneBot 已贡献各自的 UI 工件；OneBot 的 Binding、配置、会话和 Delivery 共九项操作已迁入模块契约。
- 旧 OneBot 管理路由与 `bindings_api.py` 已删除；凭据在 Adapter 内加密暂存，通用授权票据只携带摘要。
- 控制台具备摘要绑定的受限 iframe 和有界 `MessageChannel` 原型，但官方可信静态贡献与可安装包沙箱仍使用两条装载路径，尚未统一为最终协议。

### 并发与观测

- dispatch trace 已关联 Kernel call 树、component event/subscriber delivery、component job/worker call 与外部发送 part/attempt/resolution。
- 查询可按消息、群、组件、代际、结果和时间从不可变因果根精确选根，并使用绑定筛选条件的稳定游标分页。
- 权威事实采用幂等操作、修订、CAS、短事务和 outbox；最近修复了并发同修订命令策略应用泄漏 `CONTROL_CONTENT_REVOKED` 的竞态，使其收敛为 `REVISION_CONFLICT`。

## 当前阻断项

### P0：清除业务组合根

- 删除 `BindingRuntime` 类型及兼容别名，完成 `BotRuntimeSupervisor` 收口。
- Supervisor 只持有冻结模块图、内核级任务、宿主 lease、统一工作通知与归约健康状态；不得 import 或保存具体 Agent、Memory、Adapter、Shell、Job、Delivery 实例或 Store。
- 模块工作通知、失败和健康必须经通用端口上报；Fleet 不读取模块私有任务集合。
- 官方模块与可安装 Worker 仍需统一监督、失租恢复和按依赖边故障隔离语义。

### P0：拆分存储所有权

- `BotProvisioner._initialize`、`BotFleet.attach`、CLI 和 Restore 仍调用 `BotStore.migrate()`；必须改为最小 Kernel bootstrap 加所选模块的准确 schema bundle。
- 当前 `forest_kernel.bot_identity` 和 `forest_kernel.schema_bundles` installer 已能校验摘要、namespace、Repository contract、并发安装和事务回滚，但官方模块尚未提供完整业务 bundle。
- 不能把历史 001…087 迁移压成包含全部业务表的单一“官方 bundle”；模块只能拥有自己的 namespace 和 Repository。
- 干净库拒绝旧迁移账本、未知业务表和非空升级路径；开发数据库直接重建。

### P0：完成模块化控制台

- 将 Event、ComponentJob、命令/消息策略、诊断、审计、备份、账号和其余固定页面迁入所有者模块贡献。
- 删除壳层业务 DOM、业务 JavaScript import、模块 ID 分支和专用 HTTP API；deployment/host 作用域使用与 Bot 相同的发现协议。
- Memory 页面现有筛选、分页和图浏览逻辑迁入 Memory 包后，壳层不得保留其脚本或数据端点。
- 重新完成 PC 视口布局验收；大型结果必须服务端筛选、稳定分页和虚拟/有界渲染，不能靠纵向卡片堆叠。

### P1–P4：发布前验证

- 在新模块边界下重跑同群结束/入站竞态、跨群并行、重载接管、Shell 优先级和真实 QQ 许可群低频验收。
- 补齐记忆幂等合流、乱序提交、删除 epoch 防复活、文本/图片/mixed 查询和 text-only Provider 投影的端到端证明。
- 使用十万至百万来源验证组合筛选、索引计划、键集分页、单页图展开、延迟和内存上限。
- 完整 PostgreSQL 套件、Docker Compose、长时 OneBot 连接抖动和重启恢复仍未形成最终发布证据。

## 最近可信验证

提交 `01300ca` 的验证基线：

- 无 PostgreSQL 全套：`735 passed, 36 skipped, 1115 deselected`；
- PostgreSQL 17：受影响的 contribution、Binding、Delivery、Job、Agent、Memory、策略、诊断和控制回执共 29 个定向场景均取得通过结果；
- Ruff、Python compileall、JavaScript 语法检查和 `git diff --check` 通过；
- 临时 PostgreSQL 容器已删除，常驻 `forest-forest-1` 与 `forest-postgres-1` 未被该验证改动。

这些结果只证明该提交所覆盖的切片，没有覆盖完整 PostgreSQL 套件、真实 QQ 低频验收、长时连接抖动、十万级记忆容量或正在进行的运行时组合根重构。工作区未提交变更不计入本节证据。

## 发布判据

只有同时满足以下条件，才能重新评估发布候选：

1. 13、14 号文档的退出条件都有代码和测试证据；
2. 干净数据库只由最小 Kernel baseline 与所选模块 bundle 创建；
3. `BindingRuntime`、宿主业务对象捷径、固定业务后台页面和旧专用 API 全部不存在；
4. 无 PostgreSQL 与完整 PostgreSQL 回归通过；
5. 模块 UI、生命周期、同群排队、跨群并发、记忆写合流和图文召回通过集成测试；
6. Docker Compose、真实 OneBot 许可群低频交互与重启恢复通过；
7. 本页更新为对应提交、命令、通过数和未覆盖边界。
