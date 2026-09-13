# Forest 实现状态与验证记录

更新日期：2026-09-13。本文记录当前代码的实际能力，[01–12 设计文档](README.md)继续描述首版目标。**已接通 Web 多 Bot、原生微内核、OneBot HTTP、P2 插件升级、P3 持久 Agent/定时任务/私有分析，以及 P4 跨群图文记忆链路；首版框架仍在实现中。**页面支持模型、人格、图文能力、工具权限、嵌入及媒体/查询预算、审批、任务配置与运行取消。模型/工具步骤以数据库安全点接入群消息，输入投影保留作者及群来源，裁剪不调用摘要 API。全链路遗忘、记忆图管理、通用事件/Job 扩展和完整管理 Shell 仍待完成。

## 1. 当前可以执行的流程

从空部署启动独立管理服务，使用本机 CLI 交付的一次性配对码在浏览器创建首管理员，登录后读取配置、预览差异、基于预期版本提交，再读取实际生效修订。注销使服务端会话失效；重启后从原 PostgreSQL 和本地冻结计划恢复管理状态。此流程不依赖 Bot、插件、QQ、LLM 或 Embedding 凭据。

可操作命令见[根 README](../README.md)。Docker Compose 提供应用、私网 PostgreSQL 和一次性卷初始化服务；原生 Python 模式允许在首次 Web 初始化时填写 PostgreSQL URI。

插件流程是：静态构建完整包 → 启动无生产权限的候选 Worker → 校验配置和服务 → 在专用 Bot 数据库接纳代际与调用 → 经宿主代理读写组件状态 → 记录结果并回收。`forest plugin build/check/lock/import/export` 提供开发工具，`forest dev --package` 执行一次服务调用，`forest dev --watch` 持续监听完整包。开发源变化固定新 desired；失效候选不能越过最新请求发布，重启恢复持久 last-good。见 [P2 包/环境说明](p2-plugin-packages.md)。

Web 新流程为：创建 Bot → 供应器按冻结计划创建专用数据库和受限角色 → Fleet 启动该 Bot 宿主 → 预览并提交组件配置 → 候选准备与排空切换 → 生效修订推进 → 页面调用组件。双 Bot 计数器、热改倍率与完整重启恢复已有真实 HTTP/PostgreSQL/Worker 测试；后台支持发行版内置组件和通过完整 ZIP 导入的可信插件。

P1 流程为：登记固定 OneBot 账号 → Web 预览并保存连接配置 → 实际 API 身份探测 → 持久发布 Binding/connection epoch → HMAC 验证消息后持久 ACK → 全量 Context/最后触发切口 → 公平群 Actor → 模板 Worker → 最终回复和发送意图原子提交 → 事务外 HTTP → SENT/NOT_SENT/UNKNOWN。受限 `#forest help/status` 独立处理并使用 CONTROL 发送通道，普通业务暂停不关闭接收。详细行为和未实现边界见 [P1 接入说明](p1-integration.md)。

| 能力 | 当前实现及边界 | 主要证据 |
|---|---|---|
| 初始化状态 | 本地 SQLite 事务冻结唯一 setup ID、目标与计划；计划使用受保护主密钥加密，数据库成功后才封闭本地初始化入口 | [bootstrap.py](../src/forest/bootstrap.py)、[bootstrap 测试](../tests/test_bootstrap.py) |
| 初始化恢复 | FROZEN 只能恢复原计划；COMPLETE 先核对目标 setup 记录，身份缺失不重新建库建管理员；启动恢复失败后定时重试固定目标 | [管理运行时](../src/forest/management/app.py)、[HTTP 集成测试](../tests/test_management.py) |
| 本机凭据与密钥 | `forest setup-token` 取得或轮换未冻结部署的配对码；密码使用带随机盐的 scrypt；密钥有 POSIX 权限或 Windows DACL 保护，丢失不自动重新生成 | [security.py](../src/forest/security.py)、[安全测试](../tests/test_security.py) |
| PostgreSQL 权威 | 显式异步连接池、带版本和 checksum 的事务迁移、初始化 singleton、账号与会话、计划、不可变修订、配置头、审计和 outbox | [PostgresStore](../src/forest/storage/postgres.py)、[初始迁移](../src/forest/storage/migrations/001_deployment.sql) |
| 配置提交 | 同事务核验当前管理员和 Web 会话、计划摘要/期限、schema 与预期 revision；配置、管理请求、审计和 outbox 原子提交 | [PostgreSQL 测试](../tests/test_postgres.py) |
| 并发与恢复 | 不同进程基于同一旧 revision 仅一个成功；同幂等键同输入返回原请求，异输入拒绝；成功请求在计划到期或服务端 schema 升级后仍可恢复 | [PostgreSQL 测试](../tests/test_postgres.py) |
| Web 安全边界 | 服务端会话、CSRF、严格 Origin/Host、请求体限制、登录尝试限制、有限并发密码计算；错误响应不回显密码/token输入 | [管理 API](../src/forest/management/app.py)、[HTTP 集成测试](../tests/test_management.py) |
| 管理页面 | 初始化、登录、配置表单、差异预览、版本显示、应用、冲突提示和注销；草稿保留在当前页面内存中 | [页面](../src/forest/management/static/index.html)、[页面逻辑](../src/forest/management/static/app.js) |
| 容器部署 | 专用卷初始化、随机数据库密码、非 root 应用、只读应用文件系统、loopback HTTP 端口；数据库只在 Compose 网络内访问 | [Compose](../compose.yaml)、[Dockerfile](../Dockerfile)、[卷初始化脚本](../scripts/container_init.py) |
| 不可变 Python 包 | 静态编译与封闭 manifest/schema 校验，捕获源码、helpers、schema 和资源；内容寻址、同版本内容冲突、链接/路径逃逸、变化捕获、文件/字节/目录上限检查 | [packages.py](../src/forest/packages.py)、[包测试](../tests/test_packages.py) |
| 当前 SDK | `Plugin`、`@service`、只读配置、受调用作用域限制的 state、受监督异步任务、稳定步骤 ID；显式 manifest，不自动生成装饰器声明 | [sdk.py](../src/forest/sdk.py)、[counter 示例](../examples/counter/python/counter.py) |
| 原生 Worker | 全新解释器、认证 loopback IPC、版本化且有界的 JSON 帧；prepare/start/health、业务 admission、调用并发/排队、取消与进程回收 | [Worker Driver](../src/forest/drivers/python_worker.py)、[Worker 运行时](../src/forest/drivers/worker.py)、[Worker 测试](../tests/test_worker.py) |
| Bot 存储权威 | 一个数据库固定一个 Bot 身份，与部署库分离；事务迁移、宿主 lease/incarnation、组件 generation/route、调用 grant、状态 revision/步骤回执、审计/outbox | [BotStore](../src/forest/storage/bot.py)、[内核迁移](../src/forest/storage/bot_migrations/001_kernel.sql)、[BotStore 测试](../tests/test_bot_store.py) |
| 最终提交守卫 | 在实际状态读写/调用完成事务核验当前 Bot、宿主、代际、调用、期限、取消、权限与维护修订；CAS 与幂等在同一目标事务裁决 | [BotStore](../src/forest/storage/bot.py)、[内核集成测试](../tests/test_kernel.py) |
| 生命周期协调 | 候选预热与正式写入分离；freeze/drain 后捕获最终状态，纯迁移双次验证，状态/路由/effective 原子发布；失败回收、受限恢复与宿主接管 | [kernel.py](../src/forest/kernel.py)、[状态迁移说明](p2-state-migration.md) |
| 开发 CLI | `plugin build` 静态构建，`plugin check` 候选运行检查，`dev` 在专用开发库执行一次指定服务；JSON 可经标准输入提供 | [cli.py](../src/forest/cli.py)、[README 用法](../README.md#原生插件开发切片) |
| 依赖与服务 | required/optional、契约 major/range、安装前 DAG 校验、依赖先启动；精确 `service.call:<name>`、调用树深度和共享 deadline | [kernel.py](../src/forest/kernel.py)、[服务集成测试](../tests/test_services.py) |
| 调用树排空 | 持久记录依赖闭包预留；禁止新树进入冻结目标，已接纳树可完成子调用；所有预留与自身调用排空后才 QUIESCENT/发布 | [调用树迁移](../src/forest/storage/bot_migrations/002_call_trees.sql)、[调用树测试](../tests/test_child_calls.py) |
| Bot 目录与供应 | 独立角色/数据库、加密固定计划、CAS/幂等、逐 DDL 许可、lease/epoch；SCRAM verifier 与 DB owner 对账，不接管陌生资源 | [目录](../src/forest/management/catalog.py)、[供应器](../src/forest/management/provision.py)、[测试](../tests/test_bot_catalog.py) |
| Web 到 Bot 授权 | 30 秒用途限定 Ed25519 票据；目标库检查签名、Bot、输入、计划、当前 policy/admission 与最终期限，记录原票据依据 | [签发器](../src/forest/management/authority.py)、[BotControl](../src/forest/storage/control.py)、[授权测试](../tests/test_web_authority.py) |
| 组件配置生效 | 目标库持久 plan/desired/CAS；生命周期发布同事务验证当前 desired、包、配置、grants、policy，推进 effective | [控制迁移](../src/forest/storage/bot_migrations/003_control.sql)、[Web/Bot 集成测试](../tests/test_web_bots.py) |

## 2. 已实现配置契约

部署设置使用资源 `resource_key=deployment`，这张配置表不替代独立 BotStore。Bot 组件配置在各目标库的 `bot_config_*` 表中保存。管理配置 schema 封闭，拒绝未知字段。

| 字段 | 默认值 | 合法范围 | 生效范围 |
|---|---|---|---|
| `display_name` | `Forest` | 1–80 字符 | 后台读取最新配置时使用的新显示名称 |
| `session_ttl_minutes` | `480` | 整数，5–1440 分钟 | 后续登录会话的到期时间；不修改已有会话 |

两个部署字段都属于动态配置，所以 `desired` 与 `effective` 在同一提交中推进。返回值包含各自的 revision、values、schema digest 和创建时间；head 为 0 时返回空修订，由服务端提供默认值。组件配置则先保存 desired，由 Fleet 完成代际切换后推进 effective。重启时持久 effective 表示最后成功发布的修订，页面还检查本次宿主实际加载的修订和 Worker 状态；不能仅凭旧 effective 指针显示运行就绪。

Web 请求采用“服务器保存计划 → 页面显示差异 → 按 plan ID/digest、expected revision 与稳定幂等键提交”的流程。配置竞争属于显式仲裁，不宣称可交换或最后写入覆盖。计划通常有效 10 分钟；同一次已成功请求的重试不因为计划过期而重复创建修订。

### 2.1 当前插件包与配置

包格式限定 `runtime=python-worker`、一个 `scope=bot` 组件、`state_schema=1`、`reload_mode=drain`；服务声明输入/输出 JSON schema 和契约 major.minor。能力包括显式 `state.read/state.write` 与 `service.call:<name>`，跨组件依赖必须同时在 manifest 声明。服务重复、缺少必需依赖、契约不兼容和依赖环在执行插件前拒绝；缺少可选依赖时固定降级状态。不支持的约束或 manifest 字段会明确拒绝，不能把未执行的 `memory_mb` 等限制显示为受保护。

资源字段包含 `max_frame_bytes`、`max_inflight`、`max_queue_items`、`max_state_inflight`、`max_owned_tasks`，生命周期字段包含 prepare/start/drain/stop 的 timeout。它们约束本切片的 IPC、调用与受监督任务，不能等同操作系统沙箱、CPU/内存硬配额或完整资源 SDK。普通 Python 插件仍要求可信来源。

`check/dev --config-json` 默认提供 `{}`，按包配置 schema 验证；Kernel 为一次安装固定配置，Worker 取得只读视图。管理员可在 WebUI 创建 Bot、导入第三方包、选择版本、编辑 schema 表单、查看 desired/effective 和配置历史，无需编辑服务器配置文件。开发者仍显式维护 manifest 与 schema；模型配置在 P3 接入。见 [Web 插件管理](p2-web-plugin-management.md)。

### 2.2 状态、调用与替换的实际语义

BotStore 首次迁移要求专用空库，已经初始化的库只接受其固定 Bot；既有部署数据库和其他 Bot 库不能被转换为当前 Bot 的存储。宿主持有 DSN，Worker 不能通过 RPC 参数选择数据库、Bot、组件命名空间或伪造 CallGrant。不同组件使用相同 state key 时互不覆盖。

状态步骤以预期 revision 做 CAS，步骤回执、状态新修订、审计和 outbox 在同一事务保存。并发冲突明确拒绝，不声称 CAS 就证明业务合流。一次服务可以先提交状态，再返回结果；状态步骤与整次调用完成是两个提交边界，后续异常不回滚已接纳步骤。稳定 operation ID 防止重试悄悄重做，失效调用不能凭旧回执或缓存 Context 获得新的执行权。

排空后捕获带 schema、聚合 revision 与 checksum 的不可变状态来源。显式 `Plugin.migrate` 在候选中执行两次，验证输出确定性与目标 schema；最终同一事务 CAS 来源、写状态/键修订/tombstone、切路由并推进 effective。旧代不能继续提交；候选失败保留或按守卫恢复旧入口，过期宿主由新 incarnation 接管。支持有界持久 KV 格式迁移，不迁移 Python 栈、任意外部数据库或完整 Agent checkpoint；详见 [迁移说明](p2-state-migration.md)。

### 2.3 已实现服务调用与 Web 控制边界

`ctx.services.call()` 的目标只包含服务名、契约 major、输入、稳定 operation ID 与可选更短 timeout。宿主依据已安装服务目录和调用者 manifest 选择本 Bot 目标，分配 child CallGrant；子调用继承根树期限上界和权限修订，深度上限 8，组件环和越权调用被拒绝。父调用结束前必须处理完子调用；父取消、失权、到期或代际变化会阻止后代最终写入。调用回执不能跨父调用树恢复成新的子调用权限。

一次顶层接纳同时记录可能调用的依赖闭包。目标冻结后不接纳新的相关调用树，但已有树可以在 QUIESCING 阶段调用该目标；所有自身调用和预留树排空后，宿主才调用 quiesce 并进入 QUIESCENT。候选发布在数据库再次检查屏障。可选依赖和降级状态在安装时固定，依赖出现不会静默扩张已有调用者的权限。

`install_many` 是预校验 DAG 后按序执行的批量操作，不是多组件原子事务。若中途失败，已成功发布的组件及逐项回执保留，失败候选按其独立生命周期清理；不能通过“回滚批量”撤销已经发生的状态或外部效果。

Web 账号和会话在部署库，Bot 配置和执行权限在目标库。后端签发的 Ed25519 票据最多有效 30 秒，绑定主体、会话修订、目标、action、固定 operation ID、输入/计划摘要和 Bot policy。目标最终许可事务验证票据与当前权限，记录票据依据；Worker 和浏览器不取得票据或数据库凭据。会话撤销停止后续签发，在途票据仍保留最多 30 秒窗口；目标 policy 撤销会即时阻止尚未接纳的旧权限动作。服务接纳到 Kernel admission 之间仍固定原 policy 版本，不能借后来的组件重载升级旧请求权限。签名实现使用 [cryptography Ed25519 API](https://cryptography.io/en/latest/hazmat/primitives/asymmetric/ed25519/)。

Bot 供应任务在部署库固定原始计划、稳定 Bot UUID、数据库/角色名及加密凭据。角色创建把预先冻结的随机 SCRAM verifier 与角色同时提交，数据库创建使用该已证明归属的角色作为 owner；后续 COMMENT 仅作辅助标记。崩溃发生在 CREATE 与 COMMENT 之间也能对账，不凭名称接管现有对象。每次 DDL 前接纳短效许可并检查 lease/epoch 与当前管理员；外部 DDL 没有跨连接事务 fence，已许可的在途效果按同一不可变计划核对。退出浏览器不取消已接纳任务，禁用原管理员阻止后续许可，另一当前管理员可以显式重试原计划。

托管 Bot 角色没有 SUPERUSER/CREATEDB/CREATEROLE，各 Bot 库撤销 PUBLIC CONNECT；测试验证 Bot A 不能登录 Bot B 库。外部存储要求专用非特权账号和空库，拒绝不匹配身份或已有业务表。目录不提供数据库删除；失败创建保留同计划资源以便恢复。

## 3. 与完整设计的差距

| 阶段 | 状态 | 尚未实现的必要内容 |
|---|---|---|
| P0 契约与可靠内核 | 部分实现 | 已有依赖 DAG、受监督服务调用树及 Web/Fleet 接线；仍缺通用事件/Job、自动声明生成、完整资源 SDK 和其余消息契约 |
| P1 群聊与多 Bot | 接入切片可运行 | OneBot HTTP、Web Binding、全量 Inbox/作者来源、同群串行跨群并行、Actor fencing、静默裁剪和可靠发送；完整通用消息插件链、平台能力覆盖与持续压力验收仍待补齐 |
| P2 热重载与恢复 | 原生插件切片已接通 | 完整 ZIP、离线锁定环境、持久 watch、受控 KV 迁移、操作终结恢复、Web 版本/历史配置与 last-good 恢复已接入；完整 Agent/Job checkpoint、通用状态后端与长期容量治理继续后续阶段 |
| P3 Agent 与上下文 | 持久运行、计划及私有分析已接通 | interval/cron 受权队列、非重叠与取消、depth 1 私有子 Agent、父方接纳、共享树预算及群管理已接入；通用事件/Job 扩展与更完整的资源契约继续实现 |
| P4 多模态多主干记忆 | 图文索引与 Agent 召回已接通 | 多主干持久图、独立原图捕获、来源向量发布、图片/混合查询、固定证据及 Agent 投影、Web 记忆配置与预算已实现；关联图扩展召回/页面、旧载荷完整迁移、全链路遗忘及大规模检索容量仍待完成 |
| P5 完整管理与容量 | 部分基础已接入 | WebUI 连接/队列/发送对账、计划任务与子 Agent 管理、群聊运行/任务命令已接通；完整类型化管理服务、角色/密钥/预算管理、任务日志、备份迁移恢复及容量治理尚未完成 |

初始化 bootstrap 仍使用跨进程事务固定计划并依靠目标库幂等初始化；它的 `owner_epoch=1` 表示该初始化子系统没有接管 API。新 BotStore 的宿主/组件执行租约与 fencing 是独立协议，不能据此宣称 bootstrap 或 Bot 存储迁移已经支持完整接管恢复。

部署配置及已实现 Bot 状态/生命周期事实均保存相应审计/outbox，尚无通用事件后台消费者。OneBot 发送已有持久投递、确定未发送时的重试及 UNKNOWN 人工对账；这不代表任意外部效果都有通用恢复协议。配置页直接读取 PostgreSQL 当前生效值，因此这个管理闭环不依赖 outbox 消费来宣称生效。原生 Worker 的 state RPC 也不能被视为完整 Agent 工具或平台 Effect 系统。

当前只有首管理员创建和登录，没有账号管理、密码重置、浏览器持久草稿或 Web 备份恢复。已有组件配置历史页面，可将历史配置重新预览并作为新修订提交；恢复时仍使用当前持久状态。初始化完成后不能用 `setup-token` 再次注册或删除标记重开注册。后台已有 Bot、插件、组件、OneBot 连接、Agent 模型配置和审批、计划任务、私有子 Agent 及记忆嵌入/媒体/查询配置。自由上传检索、图浏览器和完整遗忘管理尚未接入。

## 4. 验证证据与适用范围

当前正文迁移新增 029–035：输入与图片、上下文与回复、Agent、子 Agent、发送正文使用永久事实头和可清理 body；原正文列禁止再次写入。内容接纳与遗忘通过 deletion epoch 串行裁决，旧模型请求仍保留精确 bytes、原摘要和费用事实。运行恢复会在读取已删除 checkpoint 失败时结束该 Run，保留静默排空步骤；子 Agent 在派发和接纳时同时复验载荷与当前记忆访问权限。工具的 JSON `null` 作为合法结果迁移，不与 SQL 空值混淆。详见[正文迁移协议](p4-content-migration.md)、[Agent 载荷](p4-agent-payloads.md)和[发送载荷](p4-delivery-payloads.md)。

旧来源不能证明完整依赖时使用显式保守范围。升级前已经发生删除的旧库，导入未知派生物时不会写入正文；034 为遗留的不完整 SourcePayload 追加独立删除决策，原请求和回执不变。真实 022 非空旧库验证覆盖未删除的无操作升级、预先删除和 257 条来源跨批次清理。001–028 的原 SQL checksum 不改，029 起连同版本化 handler 和冻结 helper 一起校验；升级要求先停止宿主，失败回滚 DDL、搬运和所有权失效。

035 处理自身回声早于成功回执进入上下文的情况：确认平台身份后，原输出、回声及确认前的派生副本建立删除依赖。原删除先发生时追加新的补偿决策，回执先发生时正常删除沿关联扩展；原事实均不改写。其他用户独立发送的相同文本保留。Web 发送记录按当前权限读取实际载荷；正文不可用时保留发送状态与平台消息 ID，页面显示内容已不可用。

全链路遗忘尚未完成：Kernel/状态、Agent 冻结配置和其他控制面仍有待迁移正文，Worker 跨调用缓存也需要代际暴露来源记录与回收。已发送给外部服务的内容、WAL 和备份不属于在线 body 清理范围。晚到的同源新 occurrence 当前会被拒绝，尚无持久化 metadata-only ACK；不得把这一内部机制当作完整公开遗忘功能。

| 正文迁移与来源关联切片 | 结果 | 范围 |
|---|---|---|
| Windows 全量基线 | `1090 passed, 13 failed, 4 skipped`，1893.40 秒 | 实现期间的基线；失败涉及旧正文列读取及 Run 终结后的静默排空断言，随后修复并纳入最终定向 |
| 非 root Linux 全量基线 | `1113 passed, 9 failed, 1 skipped`，1628.63 秒 | 较晚快照；九项旧正文读取断言已调整为核对真实载荷及永久事实 |
| Windows 定向 | `404 passed, 1 failed`，904.80 秒 | 40 个受影响模块；唯一失败为下行同一旧图断言，已由最终增补复验 |
| 非 root Linux 定向 | `404 passed, 1 failed`，741.31 秒 | 唯一失败为确认回声不推进来源版本的旧断言，已改为验证事实版本推进、重复确认幂等和旧候选失效 |
| 最终 Windows 增补 | `3 passed`，9.94 秒 | 上述图断言及发送许可前/网络交接前来源超限；后继消息真实 HTTP 送出，无 UNKNOWN 阻塞 |
| 最终非 root Linux 增补 | `3 passed`，9.57 秒 | 同样三个用例，使用发布快照及专用 PostgreSQL |
| Edge + 独立 Compose | 通过 | 双 Bot、初始化、配置竞争、Worker 热重载/升级、OneBot 实际 HTTP、Agent 回复、任务投递、UNKNOWN 对账及重启恢复 |
| Edge 发送面板与静态检查 | 通过 | 已不可用正文不被旧响应重新显示，SENT 事实保留；六个管理 JS 模块语法检查、Ruff 与本地文档链接 |
| 最终镜像核对 | 通过 | 238 个源码/测试文件与工作区逐项一致；测试镜像和运行镜像中 150 个已安装运行文件均通过 SHA-256 核对 |

两平台全量基线、405 项定向和最后增补是独立运行。定向期间只补入发送 Worker 的超限错误分类：网络交接前能证明没有发送时记录 NOT_SENT；相应行为由最终两平台增补验证。图断言更新仅修改测试。另一次 Windows 定向因系统临时目录 ACL 在 fixture 准备阶段报错，已中止并改用工作区内的独立目录，不把该次尝试计入通过结果。基线中的 ZIP 重名警告来自刻意构造的非法归档。真实 Edge 验证使用最终运行镜像，OneBot 和模型对端仍为本地 HTTP 测试服务；没有将其作为真实 QQ 或商业模型联调结论。

以下为上一提交 `9fb8fac` 的图文运行链路验证。该提交接通独立原图捕获、图/向量持久发布、原生图片和混合查询、固定 EvidenceBundle、Agent 模型/工具边界，以及 Web 记忆配置。新增 166 项定向测试覆盖配置、媒体、图存储、索引预算/并发、查询、投影、Agent 全链及旧库升级；均已分别运行通过。实现细节与剩余边界见[记忆运行链路](p4-memory-runtime.md)。

| P4 图文记忆与 Agent 接入 | 结果 | 范围 |
|---|---|---|
| Windows 全量基线 | `1040 passed, 8 failed, 4 skipped`，1751.05 秒 | 1052 项；8 项失败均为旧升级测试未计入新增的逐项记忆权限映射，已修正断言并复验 |
| 非 root Linux 全量基线 | `1045 passed, 8 failed, 1 skipped`，1429.20 秒 | 1054 项；与 Windows 相同的 8 项旧断言失败，仅跳过 Windows DACL |
| Windows Agent/升级修正 | `82 passed`，382.29 秒 | 固定切口、真实图文 Agent、Kernel 工具撤权、关闭/超时、新旧数据库升级，含全部原失败用例 |
| 非 root Linux Agent/升级修正 | `82 passed`，364.18 秒 | 与 Windows 相同的最终 12 个模块；包含实际 Worker、HTTP 和 PostgreSQL |
| Windows 最终来源筛选回归 | `88 passed, 1 failed`，269.22 秒 | 89 项，覆盖来源、图、查询、索引与 Agent；唯一失败为图重启测试提前判断发布完成，随后修正测试等待条件 |
| 非 root Linux 最终来源筛选回归 | `89 passed`，253.48 秒 | 与 Windows 相同的 10 个模块及最终运行源码 |
| Windows 最终运行时复验 | `3 passed`，25.83 秒 | 确认图片已进入发布视图再停机，保留重启不增加图版本及不重复下载/嵌入的严格断言 |
| 非 root Linux 最终运行时复验 | `3 passed`，17.81 秒 | 最终测试镜像，相同三项真实 PostgreSQL/Worker/HTTP 测试 |
| 测试清单核对 | 通过 | 当前 1068 项均被各平台自身的基线与后续回归覆盖；每项最后一次结果无失败，平台跳过单独保留 |
| 静态、文档与镜像核对 | 通过 | Ruff、补丁空白、本地文档链接；129 个运行包文件、198 个测试镜像源码/测试文件逐文件 SHA-256 一致 |

两个基线保留真实失败记录，不计为全量通过。旧权限断言只按 023 的既有能力逐项扩展，未改业务迁移；只读、禁用和无内容权限的限制继续验证。后续专项修复 Python 3.11 的携带费用回执的取消异常，并将查询全流程纳入同一超时边界。收尾另优化来源元数据筛选，拒绝缺失正文的来源及依赖，避免应用结果预算前读取候选正文。

最终来源回归中的 Windows 失败来自测试等待条件：文本已建主干不代表图片捕获事实已进入图视图。测试现在比对实际捕获与发布的来源、可用分段及原图摘要，并在停机前确认刷新无需新版本；运行实现及重启版本不变断言未改动，三项运行时复验在两平台通过。以上基线与定向不是同一次最终全量运行。

Edge 加独立 Compose 已验证实际记忆表单的预览/应用、两 Bot 预算与密钥隔离、重启保存和密钥不回显。该流程保持记忆禁用，检查不会因保存配置产生推理请求；原图、嵌入和 Agent 召回另由真实 PostgreSQL/Worker/本地 TCP 集成验证。该浏览器流程在最后的来源元数据优化前完成，界面及管理协议随后没有改动。最终运行镜像的 129 个包文件、测试镜像的 198 个源码/测试文件与工作区逐文件 SHA-256 一致。真实 QQ、商业模型、语义质量及供应商账单仍需使用真实凭据联调。

以下为上一提交 `394c7cb` 的 P4 来源/载荷基础历史验证：新载荷 PostgreSQL 与多进程 13 项、来源 Store 16 项、纯图归约 25 项、原生图文嵌入 TCP 协议与生命周期 61 项、平台真实来源/任务回收 2 项、真实 001–020→022 非空旧库升级 3 项。该提交尚未接通完整图/向量发布、Agent RAG 和记忆 WebUI；全部旧副本遗忘在当前版本仍未完成。

| P4 来源、载荷、图归约及嵌入基础 | 结果 | 范围 |
|---|---|---|
| Windows 全量基线 | `891 passed, 4 skipped`，1099.67 秒 | Python 3.11.13、真实 PostgreSQL、原生 Worker；跳过 POSIX/大小写路径专项 |
| Windows 嵌入与旧库升级增补 | `64 passed`，20.45 秒 | 最终 61 项嵌入测试及 3 项真实旧库升级 |
| 非 root Linux 全量基线 | `898 passed, 1 failed, 1 skipped`，830.93 秒 | 唯一失败为子 Agent 调度器关闭竞态，已按下述确定性复现修复；仅跳过 Windows DACL |
| 最终 Windows 定向 | `58 passed`，115.33 秒 | 子 Agent、来源完整性、运行时回收、Agent 执行、Web Binding/Job 与旧库升级 |
| 最终非 root Linux 定向 | `58 passed`，82.12 秒 | 与 Windows 相同的最终 7 个测试模块，包含原失败用例及新竞态回归 |
| 测试清单核对 | 通过 | 当前 902 项测试均被各平台自身的全量基线与补充覆盖；各测试最后一次结果无失败，平台跳过单独保留 |
| 最终 Edge + 独立 Compose | 通过 | 最终镜像的初始化、双 Bot、插件热重载/升级、OneBot、Agent 配置/回复、Job 幂等运行/投递/停用、子 Agent 配置、UNKNOWN 对账、重启恢复与注销 |
| 静态与文档检查 | 通过 | Ruff、补丁空白、Markdown 本地相对链接 |
| 最终镜像文件核对 | 通过 | 112 个已安装运行文件、测试镜像 166 个实现/测试文件分别与工作区 SHA-256 一致 |

Linux 全量暴露了已有关闭路径的问题：子 Agent 扫描依赖若在取消时返回结果，原循环忽略关闭标记，可能在关闭快照后创建任务并使回收一直等待。新故障注入测试在旧代码稳定复现相同超时；修复在循环入口及扫描返回后检查关闭标记，受影响流程在两平台最终定向中通过。另一项最终修正要求来源候选及全部已登记依赖完整，避免把尚未完整迁移的回复作为召回依据。上述基线与定向不是同一次最终全量运行，原失败记录保留，不计为基线通过。

最终构建遇到 PyPI TLS 连接失败，使用已缓存的相同锁定依赖，在临时 Dockerfile 的两条 `uv sync --frozen` 命令增加 `--offline` 完成构建；项目 Dockerfile 与依赖锁未改动，正常首次构建仍需要网络。文件核对证明镜像包含最终实现，不替代运行行为验证。所有 Provider/OneBot 对端均为本地测试服务，尚未验证真实 QQ、商业模型联调、嵌入语义质量或实际计费。

以下 P3 定时切片为提交 `c93b6a4` 的历史验证，新增 [定时任务](p3-jobs.md)、[私有子 Agent](p3-children.md)和[群聊管理](p3-control.md)。验证使用独立 PostgreSQL 17.10、原生 Worker、本地 TCP HTTP/SSE 与真实浏览器；尚未接入真实 QQ、付费 LLM 或 Embedding 凭据。

| P3 定时、私有分析与群管理切片 | 结果 | 范围 |
|---|---|---|
| Windows 全量基线 | `757 passed, 4 skipped`，1022.96 秒 | Python 3.11.13、真实 PostgreSQL；跳过 POSIX/大小写路径专项 |
| 非 root Linux 全量基线 | `760 passed, 1 skipped`，833.33 秒 | 真实 PostgreSQL、原生 Worker；仅跳过 Windows DACL |
| 最终 Windows 定向 | `171 passed`，336.80 秒 | 消息与任务取消、任务权限、模型投影、Agent 配置/存储/执行、子 Agent、Shell 和旧库升级 |
| 最终非 root Linux 定向 | `171 passed`，302.30 秒 | 与最终 Windows 定向相同的 12 个测试模块，使用最终 Python 实现与迁移 |
| 测试清单核对 | 通过 | 当前收集的 781 项测试均被各平台自身的全量基线和最终定向覆盖；平台跳过单独保留 |
| 001–017→020 非空旧库升级 | `9 passed`，已计入定向 | 固定历史迁移 checksum、既有数据及不同权限签发者保留；新增 Job 权限按已有能力映射 |
| Edge + 独立 Compose | 通过 | 双 Bot、插件升级/历史恢复、OneBot、模型配置与回复、任务创建/预览/手动运行/实际投递、重复请求幂等、停用及重启恢复、子 Agent 配置、注销 |
| Edge 任务与 Agent 表单专项 | 通过 | 共享 API 参数/请求头、草稿竞态、Bot 切换、任务来源刷新、启停提交及移动布局 |
| 静态与文档检查 | 通过 | Ruff、补丁空白、Markdown 本地相对链接 |
| 最终运行镜像核对 | 通过 | 105 个已安装运行文件与工作区 SHA-256 逐项一致 |

两平台全量基线之后，修正了取消任务的上下文展示、模型输入中的任务来源标记，以及启用私有分析时预留内置工具名额的配置限制；相关实现与新增测试通过上表的最终定向回归。基线与定向不是同一次最终全量运行。测试期间的重复 ZIP 名称警告来自刻意构造的非法包。

完整 Edge/Compose 验证使用最终前端行为，随后上述 Python 修正通过两平台定向验证；最终镜像另包含工具数量提示文案。浏览器验收未在这些 Python 修正后完整重跑，镜像核对证明发布文件一致，不代替运行行为测试。所有 HTTP Provider/OneBot 对端均为本地测试服务，实际送达结果不等同真实 QQ 或商业模型联调。

以下 P3 持久 Agent 基础切片是提交 `ee26859` 的历史验证：使用独立 PostgreSQL 17.10、真实原生 Worker、本地 TCP HTTP/SSE 服务与非 root Docker。真实 Edge 加独立 Compose 验证模型配置、秘密不回显、Agent 回复、重启恢复、原插件版本与双 Bot 流程。

P3 的事务和执行细节见 [Agent 运行](p3-agent.md)及[状态与并发边界](p3-agent-state.md)。旧库升级测试从 001–012 非空数据库迁移至 017，保留旧上下文、调用、媒体来源、密钥与停用状态，并按已有动作映射增加相应 Agent 管理权限；不会把只读签发者升级为可写权限。

| P3 持久 Agent 切片 | 结果 | 范围 |
|---|---|---|
| Windows 全量基线 | `664 passed, 4 skipped`，724.91 秒 | Python 3.11.13、真实 PostgreSQL；四项跳过属于 POSIX/大小写专项 |
| Windows 旧库升级增补 | `6 passed`，3.52 秒 | 全量收集后新增的 001–012→017 非空旧库、完整/只读/空权限/停用签发者 |
| 非 root Linux 全量 | `673 passed, 1 skipped`，594.74 秒 | 含 6 项旧库升级测试；仅跳过 Windows DACL |
| Edge + 独立 Compose | 通过 | 初始化、双 Bot、插件版本/历史、OneBot、模型 Web 配置、实际 Agent HTTP 回复、密钥与重启恢复、注销 |
| Edge Agent 表单专项 | 通过 | 真实共享 API 参数/幂等头、审批、读取/应用草稿、Bot 切换和移动布局 |
| Agent 状态与执行 | 已包含于全量 | 原子安全点、工具组/触发裁剪保护、持久重试/UNKNOWN、审批、只读与运行中撤权、取消、FINAL_READY 和最终提交回滚 |
| 多模态与成本 | 已包含于全量 | HTTP/SSE、文本/图片投影、图片真实解码、后台权限复核、跨群配额、日预算、超时与客户端关闭 |
| 最终镜像核对 | 通过 | 92 个已安装运行文件与工作区 SHA-256 逐项一致 |

Windows 基线运行期间仅增加旧库升级测试，并修正 `agent-panel.js` 调用公共管理 API 的参数格式；Python 运行代码未改动。Linux 全量包含增补测试，使用同一 Python 运行代码；其镜像中的该单个 JS 文件随后由最终发布镜像替换。前端修改后重新通过完整 Edge/Compose 与表单专项验证，最终镜像 92 个运行文件全部与工作区一致。测试期间的重复 ZIP 名称警告来自刻意构造的非法包。

以下 P2 是提交 `184952e` 的历史验证，使用独立 PostgreSQL、真实原生 Worker 与非 root Docker；没有使用真实 QQ、LLM 或 Embedding 凭据。

| P2 插件升级切片 | 结果 | 范围 |
|---|---|---|
| Windows 全量 | `540 passed, 4 skipped`，600.44 秒 | Python 3.11.13、真实 PostgreSQL；跳过 POSIX/大小写路径专项 |
| 非 root Linux 全量 | `543 passed, 1 skipped`，478.19 秒 | 仅跳过 Windows DACL；包含持久 watch、迁移、Web 升级/重启与旧功能回归 |
| Edge + 独立 Compose | 通过 | 双 Bot、OneBot、ZIP 导入、通用服务表单、版本升级、历史恢复、重启、注销；恢复配置后当前状态继续增长 |
| Edge 组件并发专项 | 通过 | 包列表刷新时保留选择、读取/应用期间保留新草稿、旧调用结果隔离、历史载入与类型化 schema 字段 |
| Edge Binding 专项 | 通过 | 既有响应乱序、秘密草稿、冻结核对请求与内容展示回归 |
| 迁移与 watcher 故障矩阵 | 已包含于全量 | v1→v2→v3 及显式反向迁移、在途写排空、非确定输出拒绝、提交丢 ACK、宿主真实退出、锁等待过期、100 次实际重载资源回收 |
| 最终静态/镜像核对 | 通过 | Ruff、JS 语法、补丁空白、文档链接；最终运行镜像与工作区 78 个运行源码文件 SHA-256 全部一致 |

Windows 全量运行期间只调整了 `app.js` 的 Bot 切换表单竞态及 `test_watch.py` 的同名 fixture 导入写法；Python 业务代码保持不变，前端调整后重新通过上述真实浏览器流程。Linux 全量使用最终运行源码，测试中的同一 fixture 导入随后只改为显式别名以通过 Ruff；未修改测试行为。两个全量中的 ZIP 重名警告来自故意构造的非法归档测试。

以下 P1 接入切片是上一提交 `d6e6589` 的历史记录，使用真实 PostgreSQL、原生 Worker、实际 TCP OneBot 测试服务及 Edge 浏览器验证。当时最终 Linux 镜像中 94 个源码/测试文件及实际安装包逐文件核对一致；没有接入真实 QQ、LLM 或 Embedding 服务。

以下结果均已执行结束。Windows 基线运行之后，仅修正消息身份/空内容和发送提案冻结，并对相关模块重新验证；不把基线冒充最终快照完整重跑。

| P1 接入切片 | 结果 | 范围 |
|---|---|---|
| Windows 全量基线 | `459 passed, 4 skipped`，324.54 秒 | 原生 Python 3.11、专用 PostgreSQL 17.10；随后修改的五个文件另做定向复验 |
| 最终 Windows 定向 | `89 passed`，90.81 秒 | 消息、调度、Shell、Delivery 与 BindingRuntime 五个模块；94 个源码/测试文件在此次运行前后哈希一致 |
| 最终非 root Linux 全量 | `469 passed, 1 skipped`，281.61 秒 | 最终源码/测试/已安装包核对一致；仅跳过 Windows DACL |
| 最终 Edge + 独立 Compose | 通过 | 初始化、双 Bot/计数器、OneBot 配置、真实 HTTP 收发、静默 Context、UNKNOWN 核对、重启恢复及注销 |
| Edge 表单并发专项 | 通过 | 响应乱序、刷新与提交竞态、跨 revision 的核对表单、配置应用期间保留新草稿/密钥输入 |
| 静态检查与快照 | 通过 | Ruff、JavaScript 语法、暂存补丁空白；最终 Windows/Linux 94 个源码/测试文件差异为空 |

以下为上一多 Bot/服务调用切片 `51291d2` 的已完成验证；两套全量测试均连接专用 PostgreSQL 17.10：

| 上一多 Bot/服务切片 | 结果 | 范围 |
|---|---|---|
| Windows / Python 3.11 全量 | `259 passed, 4 skipped`，163.01 秒 | 当前全部测试；跳过 3 项 POSIX 专项及大小写路径碰撞 |
| 非 root Linux Docker 全量 | `262 passed, 1 skipped`，132.63 秒 | 当前全部测试；仅跳过 Windows DACL |
| 真实 Edge + 独立 Compose | 通过 | 初始化、登录、部署配置、双 Bot 创建/隔离、组件热改、双窗口冲突保留草稿、重启恢复、注销撤销及桌面/移动页面 |
| 调用树专项 | `16 passed` | 祖先权限/期限/取消、深度与循环、依赖预留、QUIESCENT 屏障、父结束与子准入竞争 |
| 跨 Worker 服务专项 | `18 passed` | DAG、版本/schema、能力、背压、子调用稳定回执、短期限恢复和目标冻结后迟到子调用 |
| Fleet 恢复专项 | `9 passed` | 失权重建、旧进程回收、同修订崩溃恢复、独立并发、退避、超时清理与固定 Web policy |
| Web 票据及发布竞争 | `24 passed` | 严格票据、跨 Bot/用途/签名、锁等待到期、撤权窗口、CAS/幂等、过期 host/operation 与配置发布整体回滚 |
| 目录、部署库与升级专项 | `38 passed` | 供应/归属与恢复、许可/heartbeat 锁等待到期、会话到期、001→003 原事实保留与调用树回填 |
| 静态检查与镜像一致性 | 通过 | Ruff、git 补丁空白；Docker 中源码/测试与工作区的差异列表为空 |

以下为上一内核切片 `3fd9097` 与更早管理切片的历史记录，保留用于追溯：

| 验证 | 已有结果 | 能证明的范围 |
|---|---|---|
| 上一内核切片：Windows 全量 | `162 passed, 4 skipped`，46.62 秒 | 当时管理、包/Worker/BotStore/Kernel 回归 |
| 上一内核切片：Linux 全量 | `165 passed, 1 skipped`，26.16 秒 | 当时非 root 管理、Worker、内核和数据库回归 |
| 上一内核切片：不可变包 | Windows `45 passed, 1 skipped` | 静态构建、完整快照、schema/资源、链接/捕获竞态及失败工件清理 |
| 上一内核切片：Worker / SDK | `27 passed` | 真实解释器、认证 IPC、队列/任务、取消/回收及调用 Context |
| 上一内核切片：BotStore / Kernel | `25 passed` / `15 passed` | 数据库隔离、CAS、fencing、同 schema 排空及错误恢复 |
| 上一内核切片：开发 CLI | 实际 `plugin check` 和三个独立 `dev` 宿主通过 | `first / first / second` 得到 value/revision `1/1 → 1/1 → 2/2`，状态持久且同操作不重复加一 |
| 上一管理切片：Windows / Python 3.11 全测试，独立 PostgreSQL 17.10 | `50 passed, 3 skipped`，13.31 秒 | 安全、bootstrap、repository 与 HTTP 管理；3 项为当时 Windows 不适用的 POSIX/符号链接测试 |
| 上一管理切片：PostgreSQL repository 专项 | `10 passed` | 两连接/不同管理员与两个独立进程的 CAS 竞争；初始化唯一性；锁等待后撤销；outbox 回滚；计划/幂等及迁移不可变性 |
| 上一管理切片：Linux 容器 / Python 3.11、真实 PostgreSQL | `52 passed, 1 skipped`，6.69 秒 | POSIX 权限及当时的管理/事务测试，跳过 Windows DACL 专项；不证明新增 Worker/Kernel 的 Linux 结果 |
| 上一管理切片：真实 Edge 浏览器完整流程 | 复验通过 | 初始化、登录、预览、应用、双窗口冲突、重启持久化、注销撤销及桌面/移动布局 |
| 上一管理切片：静态检查 | 全仓库 Ruff 与页面 JavaScript 语法检查通过 | 当时 Python 静态问题及 JavaScript 解析；本轮需另外重跑 |
| 上一管理切片：Compose 实际启动 | 应用镜像 healthy，PostgreSQL 保持私网 | 当时管理服务及容器启动闭环，不代表新增内核或容量验收 |
| QQ / LLM / Embedding 联调 | 未执行，相应实现也尚未完成 | 不存在以模拟调用替代真实外部服务验收的结论 |

测试运行方式见[README 测试说明](../README.md#运行测试)。未设置 `FOREST_TEST_DSN` 时 PostgreSQL 测试会跳过，这类结果不能用作事务验收证据。HTTP 测试通过 ASGI transport 调用真实应用和真实 PostgreSQL；它不等同于浏览器渲染测试。

[浏览器验收脚本](../scripts/smoke_browser.py)创建独立随机 `forest-e2e-*` Compose project 和临时端口，执行上表所列部署与双 Bot 流程。脚本 `finally` 清理自己的容器与卷，截图保存在 `.local/browser-artifacts/`。本轮实际使用 Windows 上安装的 Edge；`--browser` 可指定浏览器。

P1 增加了真实 PostgreSQL 分类排列性质、群 Actor 竞争/失权、最后触发切口、发送 UNKNOWN 对账、连接回收与 HTTP 重启测试。P2 增加离线运行环境、迁移、watch 与 Web 插件验证。P3 增加模型/工具安全点、checkpoint 恢复、审批/取消、图文投影和真实 HTTP 纵向测试，详见 [P3 说明](p3-agent.md)。通用 reducer、多模态长期索引/删除竞争及持续容量压力仍需后续实现和可执行证据。

## 5. 下一步

继续补齐通用事件/Job 扩展和资源契约，推进 P4 多主干多模态记忆与 P5 完整管理和容量治理。已接通的 Web/Fleet、插件运行环境、OneBot/Binding、消息/发送、调用树、持久 Agent、定时及私有分析作为后续基础；首版目标仍按 01–12 实施，当前切片可运行不代表整个目标已经完成。
