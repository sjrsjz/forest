# Forest 当前实现状态

更新日期：2026-09-18。本页覆盖干净 Conversation、Agent、OneBot、Delivery 与 Shell authority 候选的实现事实。目标设计先以[00 架构总契约](00-architecture-contract.md)裁决，再由 01–14 展开；未通过相应设计门禁与运行验证的工作树文件不计为已实现。

## 结论

Forest 正在进行激进重构，**不可发布**。本页只记录代码事实和验证证据，不定义目标架构；目标架构以[微内核运行时与控制面重构](13-microkernel-control-plane-redesign.md)和[干净数据库基线](14-clean-schema-and-payload-provenance.md)为准。

当前代码已经具备多 Bot、群聊单执行链、Agentlib 风格 Agent、Shell、记忆图、模块代际和部分模块化控制面，但仍有三个根本阻断项：

1. Bot 创建、挂载和恢复仍调用巨型 `BotStore.migrate()`，多数官方模块业务表仍位于历史 `public` schema；
2. 可安装 Worker 仍由官方 `component.worker-host` 模块内部管理，尚未完成与官方模块一致的代际热替换、失租恢复和按依赖边故障隔离；
3. 备份、恢复和物理存储回收已经分别进入 `deployment.backup`、`deployment.restore` 与 `host.storage`；固定页面、`backup-panel.js` 和旧专用 HTTP API 已删除。当前阻断项转为这些模块在干净 schema bundle 下的数据所有权收口，以及恢复/回收故障注入和大制品流式传输的发布级验证。

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
- 每个模块必须先明确 owner、namespace、Repository contract、跨模块 participant、管理贡献和并发 fence，再开始编码；发现设计偏移时先改文档和删除错误实现。

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
- 不可卸载的 `kernel.runtime` 已拥有 Bot runtime/admission 投影、维护 revision 与暂停/恢复 action，并携带自己的管理 UI；旧 `/bots/{bot_id}/runtime`、`/maintenance`、壳层维护 DOM 和 JavaScript 已删除。
- `component.worker-host` 已拥有 Bot 可选包投影、组件配置与历史、验证、显式启停、重载、服务调用和包内管理 UI 工件；固定组件 DOM、壳层业务控制器和旧 `/bots/{bot_id}/components/*` API 已删除。壳层只按当前 Bot 注册表装载该模块贡献，不读取 deployment 包目录或识别组件字段。

### Agent、群聊与 Shell

- 固定 CorePrompt、独立 PresetContext、原子 ModelTurn、单工具步骤、checkpoint、取消和审批已实现；Child Agent 的表、迁移、配置、handler 和升级夹具已删除。
- 同群串行、跨群并行、下一模型调用安全点接入新触发、作者/群来源保留和静默排空已有实现与定向测试。
- ShellGate、保留命令、独立 CONTROL 回执、当前 Binding 管理员授权和持久控制请求已有实现。

### 记忆与多模态

- 旧生产路径曾实现 Bot 全局域、每群主干、图文媒体和嵌入管线；它仍建立在 `public` schema、旧组合根和固定管理实现上，只能作为待删除的行为参考，不能作为干净微内核基线已经完成的证据。
- 干净 `memory.graph` 候选现在拥有 Bot 级冻结语义空间、每群独立主干、稳定来源单元、一次来源的多次平台观察、图文 payload 引用、来源作者/群标记、跨主干关系贡献与证据、抑制和删除 epoch 防复活。来源资格只能由 `conversation.scheduler` 已提交的精确 `InputBatch` 授予，二者通过同一调用方事务整体提交或回滚。
- 干净查询端已经提供服务端来源筛选、固定可见性快照、复合键集分页、关系端点/证据查询和管理员抑制视图。新的多模态检索权威还实现了文本确定性分块、原图 facet、QUERY/DOCUMENT 分离、单次外部交接与 `UNKNOWN` 状态、冻结查询切口、text→image/image→text/mixed 精确候选融合、来源明确的 EvidenceBundle、晚到索引隔离和遗忘时派生向量/回执撤销；它没有生成 caption 或裁剪摘要。VL Worker 原语已经能把同一 purpose 的最多 32 个原图/文本 facet 合并为一次受限 HTTP 请求，在最终网络 fence 处原子 handoff 全批，并把响应分别作为各输入 payload 的派生回执发布。Bot 日预算采用持久 reservation：明确未发送释放额度，已知响应按实际 usage 计费，`UNKNOWN` 保留预留 token 暴露，过期未结算 reservation 保守恢复为 `UNKNOWN`。持久待处理批次扫描、真实 QiYu endpoint 联调、ANN 索引、Agent 调用链、模块管理贡献及大规模容量证明仍未完成。

### 控制面

- 运行时描述符支持多个带作用域的 `management_contributions`；Bot scope 可从统一目录发现，并经通用 query/action 网关调用模块契约。
- Agent、Memory、Routing、Command Router、Message Router、Scheduler 与 OneBot 已贡献现有 UI 工件；Routing 的 Event/ComponentJob 与两类 Router 的策略/容量/诊断已进入模块契约。当前 OneBot 贡献仍把 Binding/连接与 Delivery 观察放在同一页面，这只是旧实现事实，不能据此让 Adapter 取得 Delivery 数据所有权；干净基线必须拆成 `adapter.onebot` 与 `delivery` 两个贡献。
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
- 当前 `forest_kernel.bot_identity` 和 `forest_kernel.schema_bundles` installer 已能校验摘要、namespace、Repository contract、执行档位、并发安装和事务回滚。第三方 `declarative` SQL 与 distribution 绑定的 `official` 完整不变量 SQL 已严格分流，但官方模块尚未提供完整业务 bundle。
- 首个 `kernel.runtime` 干净 baseline 已建立并可由官方 bundle 安装器原子安装：只含租约、模块代际、通用调用、状态引用和 RuntimeGraph，不含旧表中的 `agent_tool_id`、OneBot 控制服务特例、业务枚举或跨业务外键。它目前是已验证的 cutover 候选；在其他官方模块 bundle 与 Repository 一起完成前，生产启动路径仍由旧 `BotStore.migrate()` 建库，不能把候选存在误报为切换完成。
- `kernel.payload` 的干净 baseline 与 Repository 已建立：规范化保存来源 manifest、正文、派生边、删除 epoch、精确遗忘选择和 scrub 状态；载荷接纳与遗忘按同一 clock 行收敛，先接纳会被随后遗忘闭包撤销，先遗忘会拒绝晚到正文。`kernel.audit` 候选只保存可筛选的脱敏 envelope，详情必须经 payload 端口复核，不保存 JSONB 正文或跨模块 join。`kernel.authorization` 候选只保存规范化能力授予集、最长 30 秒的精确票据、一次性消费、撤销事实、单调策略 revision 与通用批准；票据消费和撤销按同一 policy clock 合流，不吞入 Web 会话、OneBot 管理员、Agent 工具或模块策略正文。`kernel.effect` 候选通过 Payload、Authorization 与执行器 fence 端口形成通用 permit/handoff/outcome 状态机：已交接结果丢失保持 `UNKNOWN`，只有显式对账证据可解析，冲突证据留账且不覆盖，未交接 attempt 才能放弃、重试或取消；它不持有 Delivery SendIntent、OneBot action、Agent ToolCall 或 Provider 状态。这四个候选仍未接入生产启动链与旧生产者，下一步是补齐各业务模块 bundle 后执行一次性 provisioning/cutover，不能建立双写兼容桥。
- `conversation.scheduler` 干净 baseline 与 Repository 已实现连续分类水位、同群 actor lease、三种安全边界、最后 trigger 切口、普通尾部静默排空、原子 reply/trigger 提交和恢复领取；`agent.runtime` 候选已实现单 Agent Session/Run、CorePrompt/Preset 冻结、模型 attempt/UNKNOWN 对账、原子 ModelTurn、工具主体绑定与 checkpoint。两者没有子 Agent 表或互相写表。
- `memory.graph` 干净 baseline 与 Repository 已实现冻结的 Bot 级语义空间、群主干、稳定来源单元、图文 part、平台观察、来源资格、跨主干关系贡献/证据、抑制、删除 epoch 复核，以及来源和关系的稳定快照键集查询。`ConversationMemoryInputUnitOfWork` 在同一数据库事务内完成 Scheduler 输入切口和 Memory 来源资格提交；任一批次事件缺失对应来源观察时两侧整体回滚。`MemoryRetrievalRepository` 进一步拥有图文 facet、编码 attempt、原始 payload materialization、向量发布、查询切口和 EvidenceBundle；`MemoryEmbeddingWorker` 只取得受 fence 的单个 facet 调用许可并通过受管 transport 执行。该候选尚未接入运行时批处理调度、Agent 或生产组合根，当前精确数组扫描也不是最终大规模 ANN 方案。
- `adapter.onebot` 干净 baseline 与 Repository 已实现 Binding 配置 plan/apply、连接候选/发布/退役、不可变冲突观察和平台事件接纳；Adapter 只写平台事实，调用方 UnitOfWork 再让 Conversation 与可选 Shell participant 在同一事务接纳，重复平台消息不会再次分配 ingress。Adapter 表不再保存 Conversation ingress 序号。该候选不含 MessageStore、Shell、Delivery、Agent 或凭据正文，尚未替换生产 `OneBotRuntime` 组合根。
- `delivery` 干净 baseline 与 Repository 已实现 CHAT/CONTROL 独立 lane、CHAT 顺序、短租约 claim、Effect 与 Adapter 双重 fence、prepare/handoff 边界、UNKNOWN 显式对账、取消和晚到冲突证据留账。CHAT 意图只接受 `conversation.scheduler` 已提交的正式回复作为 cause；Agent 只能提供候选回复载荷，不能直接创建发送意图。Conversation 的 REPLY、trigger ANSWERED 与 Delivery SendIntent 已通过同一调用方事务验证整体提交或整体回滚。该候选尚未替换旧发送 Worker。
- `shell.command` 干净 baseline 与 Repository 已实现按 Binding/群/主体生效的修订化管理员策略、冻结命令 registry、P0/P1 资格、受管参数引用、短租约 claim、当前策略/Binding/Generation 复核和精确授权票据消费端口。Core Gate 只识别有界 `#forest` 前缀并签发短期进程内 proof，不读取管理员名单。Adapter 平台事实、Conversation Inbox、CONTROL_COMMAND 分类与 Shell 请求已通过同一调用方事务验证；Shell 结果与 CONTROL SendIntent 也整体提交或回滚。该候选尚未替换旧 `ShellDispatcher`、版本化 registry 和 `bot_shell_requests` 路径。
- 不能把历史 001…087 迁移压成包含全部业务表的单一“官方 bundle”；模块只能拥有自己的 namespace 和 Repository。
- 干净库拒绝旧迁移账本、未知业务表和非空升级路径；开发数据库直接重建。
- 现有 `OneBotRuntime` 仍直接构造 `BindingStore`、`MessageStore`、`ShellIngressGuard`、`DeliveryStore` 和发送 Worker，是尚未删除的旧平台组合根。目标 `adapter.onebot` 只保留 Binding/连接/平台事件与 OneBot action 传输端口；Conversation、Shell、Delivery 的 Repository、队列、attempt 和 UI 分别由对应模块拥有。切换时直接删除旧组合，不增加转发兼容层或双写。

### P0：完成模块化控制台

- 为 deployment/host 建立独立作用域注册表，将账号、部署审计、备份/恢复、Bot 目录和存储供应迁入准确的控制面服务贡献。它们不是 Bot 模块，也不能继续作为壳层硬编码页面。
- `ManagementScopeProvider` 注册表已分别实现 Bot 与 deployment 的身份解析、可见性和授权适配；deployment 不复用 `bot.store`、Bot policy revision，也不伪造 Bot ticket。首个 deployment 服务 `deployment.audit` 已进入独立生命周期注册表，旧 `/management/v1/audit`、固定审计 DOM 和专用控制器已删除。
- `deployment.accounts` 已接管账号目录、创建、变更计划/提交和 UI；旧专用路由、固定页面与壳层控制器 import 已删除。自我降权后以 viewer 会话恢复相同幂等回执的语义仍由 `authenticated` apply action 与存储事务共同保证。
- `deployment.configuration` 已接管 schema、当前投影、计划/提交契约和 UI；旧 `/configuration`、`/change-plans`、`/management-requests` 与固定表单已删除。登录与 session 端点只读取生效配置以计算会话 TTL 和壳层品牌文字。
- `deployment.bot-catalog` 已接管列表、创建、重命名、供应重试和供应 Job；旧 `/management/v1/bots` 目录 CRUD、`/management/v1/provision-jobs/*`、固定目录 DOM 和壳层表单处理已删除。目录选择只调用壳层通用 `openScope({kind:'bot',id,row})`，其中 `row` 只是显示提示；壳层必须再经 `/scopes/{kind}/{id}/modules` 和 Bot provider 解析真实 ACTIVE 注册表。Bot 维护、组件配置和 Agent/Memory 模块仍经目标 Bot authority 处理。
- 通用管理传输已增加受限 `upload`/`download` 操作：模块描述符固定媒体类型、字节上限、超时、流式模式、处置方式、主体和幂等语义，网关在读流前完成 scope/Generation/会话/CSRF 解析。小工件可返回有界 `ArtifactResult`，大工件返回带精确长度的 `ArtifactFileResult` 并由网关持有和关闭文件句柄；模块不能返回任意 HTTP 响应或响应头。`deployment.package-registry` 已接管包列表、ZIP 导入、导出和包内管理 UI 工件；旧 `/management/v1/packages*`、固定导入表单和包列表已删除。某个 Bot 选择哪个包代际仍属于 Bot scope 的 `component.worker-host`。
- `component.worker-host` 管理贡献已完成：可选工件经 BotRuntimeSupervisor 的受限包目录端口提供，浏览器只调用 Bot scope 的类型化 query/action/download，不跨作用域读取 `deployment.package-registry`。
- bootstrap authority 已持久保存稳定 `host_id`；每次管理服务启动分配新的 incarnation 和 host module activation。`HostManagementScopeProvider` 只解析当前稳定 ID，`host.runtime` 通过独立 host registry 与管理 UI 投影两类代际，错误 host ID 返回 404。
- `deployment.backup` 已拥有备份、导入、校验和流式制品传输；`deployment.restore` 已拥有隔离恢复与激活；`host.storage` 已拥有物理代际清单和回收计划/任务。三者均通过自己的管理契约和 UI 贡献公开能力，固定备份/恢复/存储 DOM、`backup-panel.js` 与旧专用 HTTP API 已删除；不存在过渡性的 `deployment.storage`。
- Memory 页面现有筛选、分页和图浏览逻辑迁入 Memory 包后，壳层不得保留其脚本或数据端点。
- 重新完成 PC 视口布局验收；大型结果必须服务端筛选、稳定分页和虚拟/有界渲染，不能靠纵向卡片堆叠。

### P1–P4：发布前验证

- 在新模块边界下重跑同群结束/入站竞态、跨群并行、重载接管、Shell 优先级和真实 QQ 许可群低频验收。
- 补齐记忆幂等合流、乱序提交、删除 epoch 防复活、文本/图片/mixed 查询和 text-only Provider 投影的端到端证明。
- 使用十万至百万来源验证组合筛选、索引计划、键集分页、单页图展开、延迟和内存上限。
- 完整 PostgreSQL 套件、Docker Compose、长时 OneBot 连接抖动和重启恢复仍未形成最终发布证据。

## 最近可信验证

当前工作区的验证证据：

- 无 PostgreSQL 全套：`839 passed, 1116 skipped, 1 warning`；
- 干净 `memory.graph` 与 Conversation-Memory 组合：隔离 PostgreSQL 17 下来源注册/并发收敛、图文 part、资格门禁、稳定快照分页、跨主干关系、复合关系游标、删除防复活、双方事务回滚、text→image、image→text、mixed 查询、完整模态门禁、晚到索引隔离、embedding 重放合流、单请求图文批处理、批 handoff 整体回滚、持久批次领取、PREPARED/已 handoff 到期恢复、真实 transport handoff 原语、预算并发 reservation/UNKNOWN 暴露/过期恢复、无关遗忘后的合法 facet 重新领用、payload lineage 和遗忘撤销共 `25 passed`；同一 embedding prepare/handoff 并发重放与预算 reservation/结算/恢复均另连续 `10 × 1 passed`，上一来源图切片的 Memory 定向组曾连续十轮均为 `8 passed`。
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
- `component.worker-host`：契约、贡献发现、UI 所有权和载荷定向组为 `26 passed`；隔离 PostgreSQL 17 下双 Bot 配置/调用/重启、组件验证与启停、历史正文清理、跨边界回执、跨 Bot 计划隔离、包升级失败恢复、Bot scope 包内 UI 工件和旧组件端点消失共 `10 passed`，随后对新增 artifact 与旧端点断言各自重跑为 `1 passed`。
- host scope：稳定 identity、独立 Registry 生命周期和静态 UI 定向组 `49 passed`；隔离 PostgreSQL 17 下 host provider、模块发现、运行投影和错误 scope 拒绝为 `1 passed`，管理会话/账号/配置整组为 `9 passed`。
- `deployment.backup`、`deployment.restore`、`host.storage`：隔离 PostgreSQL 17 下完成创建备份、网关文件流下载、网关流式导入、只读校验、隔离恢复、宿主回收、权限边界和旧端点 404 的端到端链路 `1 passed`；作用域发现、包制品回归与基础管理整组 `14 passed`。
- Schema bundle 与干净 kernel 模块候选：隔离 PostgreSQL 17 下 bootstrap、第三方声明式 SQL、官方函数/触发器执行、档位账本身份、并发安装、回滚、Runtime 通用事实、Audit 筛选/幂等/不可变/payload-port、Payload 来源闭包/遗忘/scrub/双锁序收敛、Authorization 精确票据/撤销/幂等消费/双锁序收敛，以及 Effect 许可/交接/UNKNOWN/对账/放弃重试整组 `49 passed`；Payload 相同 owner 的双共享锁幂等准入另连续回归 `10 × 1 passed`，已消除正文触发器共享锁升级造成的死锁；Effect 相反对账并发另连续回归 `10 × 1 passed`，每次只接纳一个最终结果。第三方描述符冒充 `official` 在 SQL 执行前拒绝。
- 干净 Conversation/Agent/OneBot/Delivery/Shell 组合：隔离 PostgreSQL 17 下 bundle、连续 trigger cut、同群串行、模型/工具 UNKNOWN 对账、单 Agent principal 绑定、Binding 配置并发 CAS、连接单向发布/退役、调用方多 participant 接入、平台消息并发去重、CHAT/CONTROL lane、正式回复与 SendIntent 原子提交、Effect/Adapter fence、UNKNOWN 对账、Shell 策略/registry CAS、P0/P1 claim、撤权复核及 Shell 结果与 CONTROL SendIntent 原子提交共 `56 passed`；同平台消息相反并发接纳、Delivery 相反对账和 Adapter/Conversation/Shell 三方接纳各连续回归 `10 × 1 passed`，分别只产生一个 Inbox ingress、一个最终发送结果及一组一致的三方事实。
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
2. 干净数据库只由最小 `forest_kernel` bootstrap 和冻结 RuntimeGraph 所需的准确模块 bundle 创建；
3. `BindingRuntime`、宿主业务对象捷径、固定业务后台页面和旧专用 API 全部不存在；
4. 无 PostgreSQL 与完整 PostgreSQL 回归通过；
5. 模块 UI、生命周期、同群排队、跨群并发、记忆写合流和图文召回通过集成测试；
6. Docker Compose、真实 OneBot 许可群低频交互与重启恢复通过；
7. 本页更新为对应提交、命令、通过数和未覆盖边界。
