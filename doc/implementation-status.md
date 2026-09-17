# Forest 当前实现状态

更新日期：2026-09-17。本页覆盖截至 `kernel.runtime` 管理面收口的实现事实。

## 结论

Forest 正在进行激进重构，**不可发布**。本页只记录代码事实和验证证据，不定义目标架构；目标架构以[微内核运行时与控制面重构](13-microkernel-control-plane-redesign.md)和[干净数据库基线](14-clean-schema-and-payload-provenance.md)为准。

当前代码已经具备多 Bot、群聊单执行链、Agentlib 风格 Agent、Shell、记忆图、模块代际和部分模块化控制面，但仍有三个根本阻断项：

1. Bot 创建、挂载和恢复仍调用巨型 `BotStore.migrate()`，多数官方模块业务表仍位于历史 `public` schema；
2. 可安装 Worker 仍由官方 `component.worker-host` 模块内部管理，尚未完成与官方模块一致的代际热替换、失租恢复和按依赖边故障隔离；
3. Web 壳仍硬编码可安装组件配置和备份恢复等专用路由与面板；Bot 的运行维护已迁入 `kernel.runtime`，deployment 注册表已包含 `deployment.configuration`、`deployment.audit`、`deployment.accounts`、`deployment.bot-catalog` 与 `deployment.package-registry`，host 作用域注册表尚未落地。

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

- `RuntimeModuleRegistry` 实现 `DISCOVERED → VALIDATED → PREPARED → ACTIVE → QUIESCING → QUIESCENT → STOPPED/FAILED`，按依赖顺序启动、反向排空，并记录持久 RuntimeGraph/Generation。
- 官方描述符携带 `package_id/version/digest/scope/driver/trust_level`；工件摘要进入 Generation、管理投影和 RouteGraph 历史。
- command、message handler、service、event、job 和官方 runtime module 已进入同一 RouteGraph；消息入场冻结 `graph_digest`。
- `component.worker-host` 拥有可安装 Worker 的准备与启动；`contract.router` 拥有冻结后的命令、Handler、Agent 和模板 responder 分派。
- `scheduler.jobs` 拥有 JobStore、授权适配、六项管理操作和自己的管理 UI；旧 Job 专用管理路由与壳层面板已删除。
- `routing.graph` 拥有组件 Event 与 ComponentJob 的持久存储、查询、失败重放和 Handler 退役操作；对应页面由该模块贡献，旧专用管理路由和壳层 DOM 已删除。
- `command.router` 通过声明的 Adapter 依赖拥有 Binding 目录投影、命令策略、容量、脱敏诊断和恢复共九项管理操作；对应 UI 由模块贡献，旧命令专用路由和壳层 DOM 已删除。
- `message.router` 通过声明的 Adapter 依赖拥有 Binding 目录投影、普通消息 Handler 策略、容量、脱敏诊断和恢复共九项管理操作；对应 UI 由模块贡献，旧消息路由专用管理面已删除。
- `kernel.audit` 已拥有脱敏查询契约和 Bot 级 UI；专用 Web API、壳层 DOM/JavaScript 已删除，`shell.command` 通过显式依赖端口读取，不再构造审计 Store。各生产者的动作语义仍归生产者；历史写入路径尚待随 schema bundle 拆分收口为该模块的显式写入端口。
- 不可卸载的 `kernel.runtime` 已拥有 Bot runtime/admission 投影、维护 revision 与暂停/恢复 action，并携带自己的管理 UI；旧 `/bots/{bot_id}/runtime`、`/maintenance`、壳层维护 DOM 和 JavaScript 已删除。可安装组件的配置与启停仍留待 `component.worker-host` 收口。

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
- Agent、Memory、Routing、Command Router、Message Router、Scheduler 与 OneBot 已贡献各自的 UI 工件；Routing 的 Event/ComponentJob、两类 Router 的策略/容量/诊断，以及 OneBot 的 Binding/配置/会话/Delivery 均已迁入模块契约。
- 旧 OneBot 管理路由与 `bindings_api.py` 已删除；凭据在 Adapter 内加密暂存，通用授权票据只携带摘要。
- 控制台具备摘要绑定的受限 iframe 和有界 `MessageChannel` 原型，但官方可信静态贡献与可安装包沙箱仍使用两条装载路径，尚未统一为最终协议。

### 并发与观测

- dispatch trace 已关联 Kernel call 树、component event/subscriber delivery、component job/worker call 与外部发送 part/attempt/resolution。
- 查询可按消息、群、组件、代际、结果和时间从不可变因果根精确选根，并使用绑定筛选条件的稳定游标分页。
- 权威事实采用幂等操作、修订、CAS、短事务和 outbox；最近修复了并发同修订命令策略应用泄漏 `CONTROL_CONTENT_REVOKED` 的竞态，使其收敛为 `REVISION_CONFLICT`。

### 通用运行时监督

- `BindingRuntime` 类型、兼容别名和 `forest.platform` 组合根已删除；`BotRuntimeSupervisor` 只监督模块图、宿主 lease、内核任务、通用工作通知和归约健康状态。
- 唤醒由描述符声明 `wakes_on_work/work_notify_order`，Supervisor 不再按 Agent、Memory、Job 或 OneBot 分支；Fleet 只读取公开 `healthy` 状态。
- 每次 Registry 激活分配独立 UUID；同一宿主关闭后重启会创建新的 RuntimeGraph 和 Generation，不能把已 `STOPPED` 的代际重新激活。

## 当前阻断项

### P1：统一可安装 Worker 生命周期

- `component.worker-host` 已是普通官方模块，但其内部 Worker 尚需获得与官方模块一致的代际热替换、失租恢复和按依赖边故障隔离语义。

### P0：拆分存储所有权

- `BotProvisioner._initialize`、`BotFleet.attach`、CLI 和 Restore 仍调用 `BotStore.migrate()`；必须改为最小 Kernel bootstrap 加所选模块的准确 schema bundle。
- 当前 `forest_kernel.bot_identity` 和 `forest_kernel.schema_bundles` installer 已能校验摘要、namespace、Repository contract、并发安装和事务回滚，但官方模块尚未提供完整业务 bundle。
- 不能把历史 001…087 迁移压成包含全部业务表的单一“官方 bundle”；模块只能拥有自己的 namespace 和 Repository。
- 干净库拒绝旧迁移账本、未知业务表和非空升级路径；开发数据库直接重建。

### P0：完成模块化控制台

- 为 deployment/host 建立独立作用域注册表，将账号、部署审计、备份/恢复、Bot 目录和存储供应迁入准确的控制面服务贡献。它们不是 Bot 模块，也不能继续作为壳层硬编码页面。
- `ManagementScopeProvider` 注册表已分别实现 Bot 与 deployment 的身份解析、可见性和授权适配；deployment 不复用 `bot.store`、Bot policy revision，也不伪造 Bot ticket。首个 deployment 服务 `deployment.audit` 已进入独立生命周期注册表，旧 `/management/v1/audit`、固定审计 DOM 和专用控制器已删除。
- `deployment.accounts` 已接管账号目录、创建、变更计划/提交和 UI；旧专用路由、固定页面与壳层控制器 import 已删除。自我降权后以 viewer 会话恢复相同幂等回执的语义仍由 `authenticated` apply action 与存储事务共同保证。
- `deployment.configuration` 已接管 schema、当前投影、计划/提交契约和 UI；旧 `/configuration`、`/change-plans`、`/management-requests` 与固定表单已删除。登录与 session 端点只读取生效配置以计算会话 TTL 和壳层品牌文字。
- `deployment.bot-catalog` 已接管列表、创建、重命名、供应重试和供应 Job；旧 `/management/v1/bots` 目录 CRUD、`/management/v1/provision-jobs/*`、固定目录 DOM 和壳层表单处理已删除。目录选择只调用壳层通用 `openScope({kind:'bot',id,row})`，其中 `row` 只是显示提示；壳层必须再经 `/scopes/{kind}/{id}/modules` 和 Bot provider 解析真实 ACTIVE 注册表。Bot 维护、组件配置和 Agent/Memory 模块仍经目标 Bot authority 处理。
- 通用管理传输已增加受限 `upload`/`download` 操作：模块描述符固定媒体类型、字节上限、处置方式、主体和幂等语义，网关在读流前完成 scope/Generation/会话/CSRF 解析，下载只接受有界 `ArtifactResult`。`deployment.package-registry` 已接管包列表、ZIP 导入、导出和包内管理 UI 工件；旧 `/management/v1/packages*`、固定导入表单和包列表已删除。某个 Bot 选择哪个包代际仍属于 Bot scope 的 `component.worker-host`。
- 下一步建立 `component.worker-host` 管理贡献，使其拥有包选择、组件配置/历史/验证/启停/调用。可选工件必须经 BotRuntimeSupervisor 的受限包目录端口提供；壳层不应继续直接查询 `deployment.package-registry`。
- 删除壳层业务 DOM、业务 JavaScript import、模块 ID 分支和专用 HTTP API；deployment/host 作用域使用与 Bot 相同的发现协议。
- Memory 页面现有筛选、分页和图浏览逻辑迁入 Memory 包后，壳层不得保留其脚本或数据端点。
- 重新完成 PC 视口布局验收；大型结果必须服务端筛选、稳定分页和虚拟/有界渲染，不能靠纵向卡片堆叠。

### P1–P4：发布前验证

- 在新模块边界下重跑同群结束/入站竞态、跨群并行、重载接管、Shell 优先级和真实 QQ 许可群低频验收。
- 补齐记忆幂等合流、乱序提交、删除 epoch 防复活、文本/图片/mixed 查询和 text-only Provider 投影的端到端证明。
- 使用十万至百万来源验证组合筛选、索引计划、键集分页、单页图展开、延迟和内存上限。
- 完整 PostgreSQL 套件、Docker Compose、长时 OneBot 连接抖动和重启恢复仍未形成最终发布证据。

## 最近可信验证

当前工作区的验证证据：

- 无 PostgreSQL 全套：`834 passed, 1059 skipped`；
- `routing.graph` 管理面：隔离 PostgreSQL 17 下模块贡献、包投影、Event 与 ComponentJob 查询/重放/退役共 `9 passed`；模块 UI 所有权与运行时描述符定向组为 `24 passed`。
- `command.router` 管理面：隔离 PostgreSQL 17 下策略、容量、诊断、恢复、模块发现和包投影共 `10 passed`；密码重新鉴权、载荷脱敏、热重载失效和并发 CAS 均包含在内。
- `message.router` 管理面：隔离 PostgreSQL 17 下策略、容量、诊断、恢复、模块发现和包投影共 `10 passed`；Binding 目录、密码脱敏、Handler 恢复幂等和并发 CAS 均包含在内。
- `kernel.audit` 管理面：隔离 PostgreSQL 17 下 Web 脱敏查询、稳定游标、Shell 查询、模块发现和旧端点消失共 `12 passed`；完整插件运行时模块图另有 `4 passed`。
- `deployment.audit` 与作用域 provider：Bot/deployment 模块发现、管理员边界、旧端点消失和 PostgreSQL 查询共 `10 passed`；跨作用域工件拒绝、生命周期和 provider 注册表定向单元组共 `31 passed`；备份审计链 `1 passed`，Web 包与持久运行图回归 `9 passed`。
- `deployment.accounts`：创建幂等、密码投影、变更计划、自我改密、并发降权、viewer 回执恢复、旧端点消失和模块投影的 PostgreSQL/ASGI 回归共 `11 passed`；契约与 UI 所有权定向组共 `32 passed`。
- `deployment.configuration`：重启恢复、并发 revision、幂等提交、CSRF、viewer 只读、旧端点消失和模块投影的 PostgreSQL/ASGI 回归共 `10 passed`；备份与包管理回归另有 `5 passed`。
- `deployment.bot-catalog`：隔离 PostgreSQL 17 下目录创建幂等、供应 Job、单项/列表查询、重命名回放、重启恢复、viewer 读取、CSRF、模块发现和旧目录端点消失的首轮为 `16 passed, 1 failed`；失败仅因一个包测试仍读旧 Bot 详情投影，改为读目录模块后定向重跑为 `1 passed`。第二次整组重跑前 15 个通过后在无日志的 Worker 退出阶段停滞，已主动中断，不计作整组通过。
- `deployment.package-registry` 与 artifact 传输：隔离 PostgreSQL 17 下 ZIP 导入幂等、原工件导出、包内 UI、包版本恢复、模块发现、配置历史和跨边界回执组为 `7 passed`；契约与 UI 所有权定向组为 `9 passed`。Bot 主回归重跑的首个双 Bot/热重载场景通过后再次停在 Windows Worker 退出阶段，已主动中断，不计作该组通过。
- `kernel.runtime`：运行描述符与 UI 所有权定向组 `26 passed`；隔离 PostgreSQL 17 下模块发现、运行投影、维护 CAS/幂等、旧端点消失、双 Bot 配置与重启组合为 `2 passed`，组件启停与维护隔离另有 `1 passed`。包升级长场景再次在 Windows Worker 退出阶段无输出停滞并被中断，不计作通过。
- PostgreSQL 17：RuntimeGraph、Fleet、OneBot 平台与载荷边界的 40 个场景中，长批次前 30 个测试体通过后停止推进；剩余 10 个按两组重跑为 `5 passed` 与 `5 passed`。
- 多模态记忆运行时的 QiYu VL profile、Agent 禁用、text-only Agent 投影和在途资源关闭四个场景分别通过；runtime descriptor 单元组为 `22 passed`。
- Ruff、Python compileall、四个受影响 ES module 的 `node --check` 和 `git diff --check` 通过；
- Embedding 连接前失败改由传输夹具确定性产生 `ConnectError`，发送后断开仍使用真实 TCP，消除了 Windows accept backlog 与未监听端口超时差异；产品的 `NOT_SENT/UNKNOWN` 判定未改动。
- 旧升级夹具仍把 001–022 当成历史快照，但当前 001 已是改写后的干净开发基线；该冲突没有被伪装成兼容迁移，必须随存储拆分删除旧迁移链与升级夹具。
- 验证使用隔离的临时 PostgreSQL 17 容器；常驻 `forest-forest-1` 与 `forest-postgres-1` 未被改动。

这些结果只证明本次运行时监督切片，没有覆盖完整 PostgreSQL 套件、真实 QQ 低频验收、长时连接抖动或十万级记忆容量。长批次停止推进仍需在后续资源隔离工作中复现和消除。

## 发布判据

只有同时满足以下条件，才能重新评估发布候选：

1. 13、14 号文档的退出条件都有代码和测试证据；
2. 干净数据库只由最小 Kernel baseline 与所选模块 bundle 创建；
3. `BindingRuntime`、宿主业务对象捷径、固定业务后台页面和旧专用 API 全部不存在；
4. 无 PostgreSQL 与完整 PostgreSQL 回归通过；
5. 模块 UI、生命周期、同群排队、跨群并发、记忆写合流和图文召回通过集成测试；
6. Docker Compose、真实 OneBot 许可群低频交互与重启恢复通过；
7. 本页更新为对应提交、命令、通过数和未覆盖边界。
