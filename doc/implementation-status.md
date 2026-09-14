# Forest 实现状态与验证记录

更新日期：2026-09-14。本文记录当前代码的实际能力，[01–12 设计文档](README.md)继续描述首版目标。**已接通 Web 多 Bot、原生微内核、OneBot HTTP、P2 插件升级、P3 持久 Agent/定时任务、P4 跨群图文记忆链路，以及 P5 加密多数据库备份、独立只读校验、未注册隔离恢复与保留期清理；首版框架仍在实现中。**页面支持模型、人格、图文能力、工具权限、嵌入及媒体/查询预算、审批、任务配置与运行取消，也能按群和作者核对记忆来源后预览遗忘、建立/解除/恢复来源联想，并通过维护版本暂停或恢复 Bot 业务。模型/工具步骤以数据库安全点接入群消息，输入投影保留作者及群来源，裁剪不调用摘要 API。

Agent 核心已经按[Agentlib 专项审计](research-qiyu-agentlib.md)和[单 Agent 详细设计](p3-agent.md)改为每个 `(bot_id, conversation_id)` 一条模型—工具执行链。宿主固定不可覆盖的 CorePrompt v1 及摘要，Bot 人格进入独立 PresetContext；Provider 输出按原子 XML ModelTurn 解析，一轮至多一个 FunctionCall，非法输出仅允许一次有界修复。`forest_analyze`、ChildScheduler、Child 配置和页面入口已删除，迁移 050 终结并封存旧记录。旧验证章节只证明被撤销代码当时可运行。公开来源遗忘已经通过 Web 和 Shell 接通；`shell-gate-v4` 新增 P0 Bot 暂停和 P1 恢复，v5 又接通组件事件与组件持久任务管理，v2–v4 继续解析既有排队命令。审核式来源联想通过 Web 接入既有纯图归约器和 Agent 召回，群 Shell 暂不提供跨群联想命令。组件事件与组件持久任务都已提供 Web/Shell 元数据观察、DEAD 回放和显式退役；两者均不创建产品 Agent，组件任务仍与会话 Agent 定时任务保持独立。Shell 参数、结果和 CONTROL 回复链已进入来源遗忘闭包；外部 WAL 清理、其余自由文本族和完整管理 Shell 仍待完成。

本次 P5 恢复切片使用迁移 061 增加持久 restore Job、逐数据库目标、严格状态边和不可变事件。管理员可在 Web 对 READY 本地备份重新认证并提交整套隔离恢复；后台认证解密、精确核对 manifest/schema/成员、用当前迁移建立随机未注册数据库、binary COPY、核对行数/序列/外键/身份，并把 Bot 副本封存为无 data/control admission。数据库以 `ALLOW_CONNECTIONS=false` 创建，写任务 marker 并撤销 `PUBLIC` 后才向恢复 Worker 开放；独立续租任务覆盖大库 COPY，失败会取消当前恢复事务。每库写入另持 session advisory lock，取得后复核 owner epoch，每个目标提交也核对租约；陈旧 Worker 与接管者不能并发写同一副本，已完成目标不会重复恢复。恢复专项为 `9 passed`，28.71 秒；部署/存储/目录/管理/备份扩大回归为 `98 passed`，115.75 秒；加入目标库互斥后的恢复、接管和取消定向复验为 `3 passed`，15.23 秒。最终镜像的真实 Edge + 独立 Compose 验收通过包含插件工件的部署及双 Bot 备份、下载与本地认证解密，并成功建立 3 个隔离数据库；随机容器和卷均已清理。该历史切片的直接恢复口令准入已由下述独立校验凭据协议替换；外部工件导入、恢复激活和删除屏障追平尚未实现。详细边界见[多数据库备份与隔离恢复](p5-backup-restore.md)。

随后补齐备份长任务的 owner 收束：捕获和加密由独立租约任务监督，续租失败会取消主任务；线程内加密/包导出收到取消后先完成并 join，不能与临时目录清理并发。每个 owner epoch 使用独立候选文件名，发布时在 Job 行锁内执行 rename/fsync 并二次核对租约，陈旧 Worker 无法覆盖当前工件。备份、恢复和 Web 联合专项为 `11 passed`，28.42 秒；部署/存储/目录/管理/备份扩大回归为 `100 passed`，117.02 秒。最终镜像的真实 Edge + 独立 Compose 复验通过稳定 `backup_id` 下的新 epoch 工件下载、认证解密和部署加双 Bot 三库隔离恢复，临时容器与卷由脚本清理。

保留期清理切片使用迁移 062 增加五分钟删除计划、持久清理 Job、幂等请求、不可变事件和孤儿回执。应用计划先在同一事务撤销备份下载/恢复能力或把隔离副本推进 `EXPIRED`，后台再用 owner epoch/lease 删除精确密文或逐库复核名称、owner marker 和目录未登记证明。默认保留期是备份 30 天、隔离副本 7 天；到期扫描复用同一 tombstone 协议。marker、路径或目录登记冲突固定 `BLOCKED` 且不删除，管理员排除冲突后可重新认证重试。超过两小时的未发布候选与临时目录只有在 Job/epoch 反向证明、无 artifact 引用且无有效活动租约时才清理。备份/恢复专用回归为 `13 passed`，32.40 秒；部署、存储、目录、插件、Web 和旧库升级扩大回归为 `101 passed, 3 skipped`，168.35 秒，跳过项均为平台条件测试。最终 `forest:storage-cleanup` 镜像构建成功，真实 Edge + 独立 Compose 完成三库隔离恢复，随后通过页面 plan/apply 删除三个 owner marker 匹配的隔离库和加密备份，确认下载入口失效；随机容器和卷均已清理。

独立备份校验切片使用部署迁移 063 增加持久 verification Job、不可变事件和恢复任务的 verification 身份。管理员在 Web 提交恢复口令和当前密码后，后台按 owner epoch/lease 对精确密文执行认证解密，并只读核对 ZIP、manifest、成员摘要、roster 与每库 cut；校验过程不创建或连接恢复数据库。成功回执绑定备份、密文摘要与清单摘要，有效期 24 小时。恢复接纳不再接收恢复口令，只接受当前有效回执，并在事务内重验工件状态及两个摘要；实际恢复仍会重新解密和校验，阻断校验后替换。活动校验与清理互斥，续租失败会取消解密任务。备份/Web 专项为 `14 passed`，35.22 秒；加入活动校验—清理互斥、凭据审计和部署迁移后，部署、目录、Fleet、账号/审计、备份、恢复与 Web 扩大回归为 `69 passed`，160.73 秒，测试同时核对校验前后 PostgreSQL 数据库集合不变。`forest:backup-verification` 镜像构建成功，真实 Edge + 隔离 Compose 确认校验前无恢复入口、恢复表单不再索取口令，并完成部署加双 Bot 三库隔离恢复与页面清理；随机容器和卷均已清理。

本次组件事件管理切片使用迁移 055 增加独立 management revision、replay epoch、不可变回放/退役审计和 RETIRED 终态。事件状态机与权限迁移 `10 passed`，Web 权威、并发回放和退役纵向链路 `2 passed`，四组旧库权限升级 `12 passed`，Kernel/Generation/WebAuthority/维护交叉回归 `58 passed`；真实 Edge 页面验证通过状态筛选、回放、plan/apply 退役、Bot 切换迟到响应隔离和移动布局。这些是分次定向结果，不冒充一次最终全量测试。详细语义见[组件事件管理](p0-component-event-management.md)。

本次 P0 组件持久任务切片使用迁移 056 增加 immutable generation handler 声明、稳定 handler head、Bot 内任务序列和租约执行状态。SDK/PostgreSQL 专项为 `10 passed`，覆盖并发同 operation 合流、正文冲突、延迟、丢 ACK、默认串行、失败后热重载接续、旧 generation 围栏不消耗尝试、schema 身份与隐式删除拒绝；Kernel、事件、调用树、Worker、包和四条旧库升级交叉回归为 `195 passed, 1 skipped`，214.20 秒，跳过项是既有 POSIX 权限测试。实现前的 SDK/manifest/Worker 基线另为 `105 passed, 1 skipped`。这些是分次定向结果，不代表新的全仓库或 Linux 验收。详细语义见[组件持久任务](p0-component-jobs.md)。

组件任务管理切片使用迁移 057 增加独立管理修订、回放 epoch、RETIRED 终态和不可变回放/退役审计。组件 SDK、数据库及 Web 纵向测试为 `8 passed`，相关旧库权限升级交叉回归为 `35 passed`，WebAuthority、事件、Kernel、Web Bot 与管理交叉回归为 `65 passed`；全仓库 Ruff、全部管理 JavaScript 语法和补丁空白检查通过。真实 Edge 已验证服务端筛选、死信回放、plan/apply 退役、Bot 切换的晚到读取隔离和移动端布局。退役会立即阻止旧代提交新任务和 dispatcher 认领，同时允许已成功 operation 的精确幂等重试。详细语义见[组件持久任务管理](p0-component-job-management.md)。

群聊组件管理切片将当前接纳门推进为 `shell-gate-v5`，新增 `event` 与 `component-job` 两个分离域，旧 v2–v4 注册表保持可解析。冻结 registry/语法测试 `67 passed`，真实 PostgreSQL/Worker 组件管理纵向测试 `2 passed`，Shell、消息、平台、维护、撤权与旧 gate 交叉回归 `137 passed`。群回执只投影处理器、订阅、delivery/Job 状态和管理修订；组件 Job 测试同时断言产品 Agent 表为空。详细语义见[群聊 Shell 设计](09-group-shell-command-system.md)和[群管理协议](p3-control.md)。

P5 账号管理切片使用迁移 058 增加不可变账号计划/请求和独立加密 HMAC 摘要密钥。WebUI 已可创建 `admin/viewer`、查看会话计数，并以 plan/apply 调整角色、停用状态或密码；目标变更同事务撤销全部会话，部署级锁保护最后管理员。查看者可读取部署与 Bot 元数据，签发器只允许读取 action。真实 PostgreSQL HTTP 与目标 Bot 授权定向测试合计 `33 passed`，部署事务、目录、任务授权与旧库升级交叉回归 `94 passed`；Edge 专项验证创建、脱敏预览、应用、晚到读取隔离和移动布局。最终镜像重新构建，并以独立 Compose 项目完整通过真实初始化、账号创建、查看者登录/读取/服务端写拒绝及既有双 Bot、Agent、记忆、热重载和重启恢复流程。详细语义见[账号管理协议](p5-account-management.md)。

部署审计切片使用迁移 059 为不可变部署事实增加有界查询索引。管理员页面支持动作、资源前缀和操作者筛选，以 `(created_at,audit_id)` 稳定游标分页；服务端按 action 白名单返回标量详情，未知或额外字段不透传。部署管理与旧库升级组合回归 `11 passed`，Ruff、管理 JavaScript 语法和补丁空白检查通过；重新构建的镜像已在独立 Compose/Edge 流程中核对真实 `account.create` 记录及操作者，并继续通过完整多 Bot 流程。它不合并多个 Bot 数据库的局部审计顺序，查看者当前无权读取。详细语义见[部署审计查询](p5-deployment-audit.md)。

Bot 审计查询切片以 Bot 迁移 058 增加时间/action 索引和既有 issuer 的最小只读升级。`BotAuditStore` 使用 `(created_at,audit_id)` 键集分页且不选择 `details` 或 hydrate 控制正文；WebUI 在当前 Bot 页面显示固定元数据，`shell-gate-v6` 增加 P1 CONTROL 的 `audit list`（`log/logs` 别名）。它不经过 LLM、不取得 Agent 会话所有权、不会因读取递归生成审计事实。真实 PostgreSQL authority/Shell 与 ASGI 定向测试 `3 passed`，v2–v6 registry/语法测试 `74 passed`，旧库升级组合测试 `5 passed`。镜像已重新构建，独立 Compose/Edge 完整流程验证了每个 Bot 的脱敏审计面板，并继续通过双 Bot、OneBot、Agent、记忆、组件热重载和重启恢复。详细语义见[Bot 审计协议](p5-bot-audit.md)。

P5 备份创建切片使用部署迁移 060 增加持久租约任务、每库切口、加密工件与不可变事件。后台按稳定 READY Bot 名单依次使用 `REPEATABLE READ READ ONLY` 和 PostgreSQL binary COPY 导出部署库及各 Bot 库；名单前后摘要不同则发布 `INCONSISTENT` 取证工件。随机数据密钥流式 AES-GCM 加密整个容器，同时由恢复口令和部署 SecretVault 分别包装；Web 创建前复核当前密码，viewer 不能列表或下载。容器、真实双数据库抓取和 ASGI 解密下载专项为 `6 passed`，管理事务、旧库升级、Bot 与插件 Web 扩大回归为 `53 passed`；全仓库 Ruff、Python 编译、全部管理 JavaScript 语法和补丁空白检查通过。镜像已重新构建，独立 Compose/Edge 完整流程实际备份部署库和两个 Bot 库、包含 Web 导入插件、下载后在宿主侧解密并逐成员校验，同时继续通过 OneBot、单 Agent、定时任务、图文 Memory、热重载与重启恢复。隔离恢复、激活、显式删除和自动保留期清理尚未实现，不能把当前逻辑导出称为 PITR。详细语义见[多数据库备份与隔离恢复](p5-backup-restore.md)。

P4 自动语义联想切片使用 Bot 迁移 059，把文档向量发布转换为按来源持久水位，并以每来源有向 Top-4 投票聚合无向跨主干 `ASSOCIATES_WITH`。它复用既有文本/图片 embedding，不增加模型或摘要调用；稳定 operation/contribution、显式 retraction 和提交后的反向重排使并发处理收敛。Memory Web 配置增加独立开关，召回标记 `reviewed/semantic` 来源和定点分数，旧冻结证据继续兼容。语义关联、召回和模型投影专项为 `58 passed`，001/022/P3 真实旧库升级为 `9 passed`，MemoryRuntime/语义维护复核为 `10 passed`；扩大到 Memory 与 Agent Memory 的 242 项回归时其余 240 项一次通过，两个新增调度依赖的测试桩补齐后相关参数组 `4 passed`。镜像已重建，独立 Compose/Edge 完整流程继续通过双 Bot、OneBot、真实 Agent HTTP、图文 Memory 配置、组件热重载和重启恢复。详细语义见[自动跨主干语义联想](p4-semantic-associations.md)。

P4 Job 载荷切片使用迁移 060/061 将 writer gate 提升为 generation 3，并把会话 Job 的 PLAN/REVISION settings 与 delegation 搬入两个来源绑定的 `control.config` payload。调度元数据不含 name、prompt 或管理员主体；原 `values`/`delegation` 列强制为空。apply 的冻结输入、计划 settings、修订 settings、delegation 与结果回执形成数据库复核的父链；来源遗忘后读取显示 `UNAVAILABLE`，手动触发、定时扩展、领取、重放和 trigger claim 均不再使用该定义。并发领取在行锁内复验 occurrence 和 overlap，SQL 授权使用无 memory-clock 锁的不可变快照，避免与 Job head 形成反向锁序。专项正文/遗忘/并发测试 `4 passed`，059→061 writer fence 与 v3 校验和绑定 `2 passed`；包含 Job、Scheduler、Web、审计、001–P3 升级、配置依赖、writer fence 和所有既有配置载荷族的扩展组合回归 `135 passed`。最终 `forest:local` 镜像构建成功，独立 Compose + Edge 完成从全新 generation-3 数据库初始化到 Job 实际 Agent 回复、投递、禁用和重启恢复的完整浏览器验收。详细语义见[Job 定义载荷](p4-job-payloads.md)。

P4 Shell 正文载荷切片使用迁移 062/063 将 writer gate 提升为 generation 4，并把冻结参数、Shell 结果与内部 delivery receipt 分别移入 shell.arguments、control.result 和 control.outbox payload。参数只继承原群消息；类型化管理输入继承参数；Shell 结果继承真实管理结果和 Binding 配置；CONTROL send part 与 receipt 继续传递结果父链。请求行原 arguments/outcome/delivery 列强制为空，待执行正文被遗忘时固定取消，已完成事实保持终端且不重执行业务。专项遗忘、并发与数据库防篡改测试为 5 passed；Shell/消息/平台扩展回归为 178 passed；writer fence 和真实旧库升级组合为 27 passed。Ruff、compileall 和补丁空白检查通过；forest:local 镜像重建成功，独立 Compose + Edge 全流程在全新 generation-4 Bot 数据库上通过初始化、双 Bot、OneBot、Agent、Job、Memory、组件热重载、重启恢复和注销。详细语义见[Shell 参数与结果载荷](p4-shell-payloads.md)。

## 1. 当前可以执行的流程

从空部署启动独立管理服务，使用本机 CLI 交付的一次性配对码在浏览器创建首管理员，登录后读取配置、预览差异、基于预期版本提交，再读取实际生效修订。注销使服务端会话失效；重启后从原 PostgreSQL 和本地冻结计划恢复管理状态。此流程不依赖 Bot、插件、QQ、LLM 或 Embedding 凭据。

可操作命令见[根 README](../README.md)。Docker Compose 提供应用、私网 PostgreSQL 和一次性卷初始化服务；原生 Python 模式允许在首次 Web 初始化时填写 PostgreSQL URI。

插件流程是：静态构建完整包 → 启动无生产权限的候选 Worker → 校验配置和服务 → 在专用 Bot 数据库接纳代际与调用 → 经宿主代理读写组件状态 → 记录结果并回收。`forest plugin build/check/lock/import/export` 提供开发工具，`forest dev --package` 执行一次服务调用，`forest dev --watch` 持续监听完整包。开发源变化固定新 desired；失效候选不能越过最新请求发布，重启恢复持久 last-good。见 [P2 包/环境说明](p2-plugin-packages.md)。

Web 新流程为：创建 Bot → 供应器按冻结计划创建专用数据库和受限角色 → Fleet 启动该 Bot 宿主 → 预览并提交组件配置 → 候选准备与排空切换 → 生效修订推进 → 页面调用组件。双 Bot 计数器、热改倍率与完整重启恢复已有真实 HTTP/PostgreSQL/Worker 测试；后台支持发行版内置组件和通过完整 ZIP 导入的可信插件。

P1/P3 流程为：登记固定 OneBot 账号 → Web 预览并保存连接配置 → 实际 API 身份探测 → 持久发布 Binding/connection epoch → HMAC 验证消息后持久 ACK → 全量 Context/最后触发切口 → 公平群 Actor → 单一持久 Agent 的模型—工具链 → 最终回复和发送意图原子提交 → 事务外 HTTP → SENT/NOT_SENT/UNKNOWN。受限 `#forest` 管理命令独立处理并使用 CONTROL 发送通道；Shell/Web 的 Bot 维护写以 maintenance epoch 合流，普通业务暂停不关闭接收。详细行为和未实现边界见 [P1 接入说明](p1-integration.md)、[单 Agent 详细设计](p3-agent.md)与[群聊 Shell 详细设计](09-group-shell-command-system.md)。

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
| 当前 SDK | `Plugin`、`@service`、`@subscribe`、只读配置、调用作用域 state/events、受监督异步任务、调用/代际资源监督与稳定步骤 ID；显式 manifest，不自动生成装饰器声明 | [sdk.py](../src/forest/sdk.py)、[资源监督说明](p0-resource-sdk.md)、[事件说明](p0-component-events.md)、[counter 示例](../examples/counter/python/counter.py) |
| 持久组件事件 | manifest 发布/订阅与 schema、调用内能力核验、内容引用、Bot 事务序列、稳定订阅 head、独立 claim/ACK、指数退避、死信、热重载接续和确认丢失对账 | [EventStore](../src/forest/storage/events.py)、[迁移 054](../src/forest/storage/bot_migrations/054_component_events.sql)、[真实测试](../tests/test_events.py) |
| 组件事件管理 | 有界元数据读取、DEAD 回放 epoch/CAS、旧 INFLIGHT operation 恢复、订阅退役 plan/apply、发布/claim 围栏、不可变审计和 WebUI | [管理协议](p0-component-event-management.md)、[迁移 055](../src/forest/storage/bot_migrations/055_component_event_management.sql)、[Web 测试](../tests/test_web_events.py) |
| 组件持久任务 | manifest/装饰器声明、调用内调度能力、正文来源、幂等任务事实、租约 claim/ACK、重试预算、每 handler 并发和热重载接续；不创建或调度产品 Agent | [详细设计](p0-component-jobs.md)、[迁移 056](../src/forest/storage/bot_migrations/056_component_jobs.sql)、[真实测试](../tests/test_component_jobs.py) |
| 组件任务管理 | 元数据读取、DEAD 回放 epoch/CAS、处理器退役 plan/apply、旧代调度/认领围栏、不可变审计和 WebUI | [管理协议](p0-component-job-management.md)、[迁移 057](../src/forest/storage/bot_migrations/057_component_job_management.sql)、[Web 测试](../tests/test_web_component_jobs.py) |
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

包格式限定 `runtime=python-worker`、一个 `scope=bot` 组件和 `reload_mode=drain`；`state_schema` 是正整数版本，版本变更须声明并执行状态迁移。服务声明输入/输出 JSON schema 和契约 major.minor。能力包括显式 `state.read/state.write` 与 `service.call:<name>`，跨组件依赖必须同时在 manifest 声明。服务重复、缺少必需依赖、契约不兼容和依赖环在执行插件前拒绝；缺少可选依赖时固定降级状态。不支持的约束或 manifest 字段会明确拒绝，不能把未执行的 `memory_mb` 等限制显示为受保护。

资源字段包含 `max_frame_bytes`、`max_inflight`、`max_queue_items`、`max_state_inflight`、`max_owned_tasks`、`max_owned_resources`，生命周期字段包含 prepare/start/drain/stop 的 timeout。`ctx.resources` 已对正常 SDK 路径提供调用/代际归属、LIFO 关闭及失败聚合；这些约束仍不能等同操作系统沙箱或 CPU/内存硬配额。普通 Python 插件仍要求可信来源。

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
| P0 契约与可靠内核 | 部分实现 | 已有依赖 DAG、受监督服务调用树、持久组件事件及 replay/retirement 管理、组件持久 Job SDK 与管理、调用/代际资源监督和 Web/Fleet 接线；仍缺自动声明生成、宿主级资源种类和其余消息契约 |
| P1 群聊与多 Bot | 接入切片可运行 | OneBot HTTP、Web Binding、全量 Inbox/作者来源、同群串行跨群并行、Actor fencing、静默裁剪和可靠发送；完整通用消息插件链、平台能力覆盖与持续压力验收仍待补齐 |
| P2 热重载与恢复 | 原生插件切片已接通 | 完整 ZIP、离线锁定环境、持久 watch、受控 KV 迁移、操作终结恢复、Web 版本/历史配置与 last-good 恢复已接入；完整 Agent/Job checkpoint、通用状态后端与长期容量治理继续后续阶段 |
| P3 Agent 与上下文 | 单 Agent 核心协议已接通 | interval/cron 受权队列、非重叠与取消、同群 trigger-cut、持久 attempt、固定 CorePrompt、PresetContext、原子 ModelTurn/一次修复和旧 Child 封存已接入；仍需真实付费 Provider 长时间联调、更多 Provider adapter 和容量验收 |
| P4 多模态多主干记忆 | 图文索引、Agent 召回、来源遗忘与联想扩展已接通 | 多主干持久图、独立原图捕获、来源向量发布、图片/混合查询、固定证据及 Agent 投影、Web 记忆配置与预算已实现；Web 可跨群列出来源，审核建立/解除/恢复联想并查看发布状态，一跳召回保留种子路径、原群和作者；Shell v4 只管理当前群主干，后台有界 scrub。自动语义联想复用既有多模态向量，以持久 Top-K 投票跨主干合流并在删除后显式撤回；会话 Job settings/delegation 已纳入同一来源遗忘闭包。部署库剩余正文、备份/WAL 清理及大规模检索容量仍待完成 |
| P5 完整管理与容量 | 部分基础已接入 | WebUI 连接/队列/发送对账、计划任务、组件事件/持久任务管理、Bot 暂停/恢复、固定 `admin/viewer` 账号生命周期、部署审计、Bot 脱敏审计、群聊运行/任务/维护/组件/审计命令、CorePrompt、PresetContext、加密多库备份、独立只读校验、隔离恢复与保留期清理已接通，子 Agent 页面已删除；Bot 级账号 scope 与完整角色模板、外部备份导入、恢复激活、任务正文日志及容量治理尚未完成 |

初始化 bootstrap 仍使用跨进程事务固定计划并依靠目标库幂等初始化；它的 `owner_epoch=1` 表示该初始化子系统没有接管 API。新 BotStore 的宿主/组件执行租约与 fencing 是独立协议，不能据此宣称 bootstrap 或 Bot 存储迁移已经支持完整接管恢复。

部署配置及已实现 Bot 状态/生命周期事实仍写入只供审计的旧 outbox，该 outbox 没有后台消费者。组件通信使用独立的持久 EventStore，具有发布声明、逐订阅投递、租约、ACK、重试和死信语义；它不消费或重新解释旧审计 outbox。OneBot 发送已有持久投递、确定未发送时的重试及 UNKNOWN 人工对账；这不代表任意外部效果都有通用恢复协议。配置页直接读取 PostgreSQL 当前生效值，因此这个管理闭环不依赖 outbox 消费来宣称生效。原生 Worker 的 state RPC 也不能被视为完整 Agent 工具或平台 Effect 系统。

当前已有首管理员创建、账号增设、固定 `admin/viewer` 角色、停用、改密、会话撤销和最后管理员保护；尚无 Bot 级账号 scope、MFA 或浏览器持久草稿。已有组件配置历史页面，可将历史配置重新预览并作为新修订提交；恢复时仍使用当前持久状态。初始化完成后不能用 `setup-token` 再次注册或删除标记重开注册。后台已有 Bot、插件、组件、OneBot 连接、Agent 模型配置和审批、计划任务、记忆嵌入/媒体/查询配置，以及来源列表、审核后遗忘、人工联想管理、备份/校验/隔离恢复与资源清理；Agent 页面只读显示 CorePrompt 版本和摘要，并编辑独立 PresetContext，不再存在子 Agent 表单。自由上传检索、完整图浏览器、身份资料删除、外部备份导入、恢复激活和 WAL 回收尚未接入。

## 4. 验证证据与适用范围

本次单 Agent 协议与配置正文切换完成后的验证采用同一 Windows 工作树和 PostgreSQL 17 测试容器，分组执行以控制运行时间。最终组合回归为 `82 passed`，205.70 秒，覆盖 CorePrompt/ModelTurn、Agent 配置与执行、Web Agent、三条旧库升级路径、配置历史和来源可见性；无数据库全集为 `544 passed, 862 skipped`；载荷、来源、暴露和回显集中回归为 `91 passed`；来源域为 `7 passed`；100 次真实热重载资源基线为 `1 passed`，243.84 秒；Binding 锁等待竞态隔离复验为 `1 passed`。Ruff、全部管理端 JavaScript 语法及补丁空白检查通过。这些是相互独立的通过结果，不冒充一次新的数据库全量运行；下列旧 Child 结果仍按各自历史版本保留。

本次公开来源遗忘切片在同一 Windows 工作树和专用 PostgreSQL 17 容器完成 `107 passed`，129.16 秒，覆盖载荷删除闭包、来源授权、plan/apply、同 epoch 竞争、预览后新增派生、后台 scrub、Shell v2/v3 恢复与群作用域、实际 Web/OneBot 两群流程，以及 001–051 非空旧库升级。最终 28 项来源/管理/Shell 复验和 5 项 Web/Shell gate 复验也分别通过；Ruff 全库、`memory.js` Node 语法和补丁空白检查通过。此次没有重跑完整浏览器 Compose、Linux 或付费模型/真实 QQ 验收。

本次跨主干联想切片在同一环境完成 `108 passed`，168.60 秒，覆盖人工关系 plan/apply/replay、同 revision 并发合流、解除/恢复、非主干离散窗口、持久待发布恢复、向量种子的一跳扩展、旧输入切口隔离、模型可见的关系路径、实际 Web/OneBot 管理流程及 001–052 非空旧库升级。最终直接影响范围另完成 `63 passed`，104.85 秒；更宽的记忆/Agent 绑定/Shell/Web 回归先通过 265 项，唯一失败是旧 051 权限测试未计入 052 派生权限，修正该精确断言后已纳入最终复验。无头 Edge 验证来源双选、联想预览/提交/解除/恢复入口以及原有配置竞态；Ruff、全部管理 JavaScript 语法和补丁空白检查通过。没有接入真实 QQ、商业嵌入/LLM 或 Linux 全量验收。

本次 Bot 维护控制切片完成 `170 passed`，293.41 秒的 Windows 组合回归，覆盖 Shell v2/v3/v4、P0 保留槽、Web 与群 Shell 的共享维护事务、幂等重放、并发 maintenance epoch CAS、暂停时继续接收、恢复后的 Actor 换代、控制参数上下文脱敏、平台调度、原 BotStore 守卫，以及多条真实非空旧库升级链。另有 `9 passed` 最终增补复验最后触发输入切口和完整权限期望；真实 HTTP/Worker 的双 Bot Web 测试为 `2 passed`。无头 Edge 验证维护表单及迟到旧状态读取不能覆盖暂停结果；Ruff、管理 JavaScript 语法和补丁空白检查通过。没有接入真实 QQ、商业模型或 Linux 全量验收。

本次 P0 资源监督切片完成 `146 passed, 1 skipped` 的 Windows 组合回归，覆盖 SDK、真实 Worker 子进程、manifest、Kernel、调用树与 Kernel 正文；其中唯一跳过项是既有 POSIX 权限测试。最终资源/Worker/manifest 聚焦复验为 `104 passed, 1 skipped`。正常返回、插件异常、调用取消、stop hook 失败、同步/异步关闭器、关闭期间取消、额度与跨作用域误用均有可执行证据；Ruff 与补丁空白检查通过。它不证明可信 Python 无法绕过 SDK，也不替代持久 Job 和宿主级网络/文件资源网关。

本次 P0 持久组件事件切片的 SDK/manifest/Worker/PostgreSQL 组合回归为 `108 passed, 1 skipped`；持久区分新 claim 与接管 claim 后，最终事件状态机复验为 `9 passed`。Kernel、服务路由、BotStore 与调用调度交叉回归为 `61 passed`，此前包含正文与历史迁移的更宽组合回归为 `105 passed`。真实进程验证双订阅 fan-out、独立死信、确认已提交但回包丢失、热重载接续、并发同 operation 收敛、事务事件序列和过期 claim 沿用同一 attempt operation；新增定向回归还验证容量延后、发送前换代不消耗 handler attempt、关闭兜底过期 claim，以及 ACK 必须对应已完成调用的正文回执。代码/manifest 不一致、契约 schema 冲突与隐式删除 durable subscription 均在发布前拒绝；Ruff 通过。

039–041 接入管理输入、结果和确切关联的审计/outbox 载荷。原 operation、输入摘要、票据依据与修订事实保留，原结果正文列严格为空；重放区分 JSON null 和不可用回执，不再触发 Kernel、登记 Binding 或展示成功提示。Web 配置到 generation、人工工具裁定与送达对账均继承实际管理输入；旧配置升级后的 Worker 回复使用限定的来源证明。详见[管理输入与回执](p4-control-payloads.md)。

管理页面在内容失效后清除旧预览，并拒绝同资源迟到响应重新填回；重新加载可读取独立的当前配置。注销会清空记忆配置与密钥草稿。迁移 042–049 已将 Agent、Memory、Binding、组件和 Watch 的 plan/revision 值、Agent frozen spec 及实际运行配置父链接入载荷存储；迁移 051 增加不可变遗忘计划，迁移 052 增加不可变联想计划/变更和可恢复关系头，两者都按原 issuer 能力升级权限；迁移 053 仅为已有 message.read 映射 Bot runtime 读取，并要求同时已有 config.apply 与 message.cancel 才映射维护写。迁移 060/061 又以 writer generation 3 原子切换会话 Job，将计划/修订 settings 和 delegation 接入来源闭包，并让既有 occurrence 在正文失效后停止领取。迁移 062/063 以 writer generation 4 原子切换 Shell，把类型化参数、结果和 CONTROL delivery receipt 接入原消息、配置及实际控制结果父链；正文失效的待执行请求固定取消，终端事实不重执行业务。部署库正文和备份/WAL 仍待迁移，当前来源遗忘能力不等于所有外部副本均已回收。

| 管理回执切片 | 结果 | 范围 |
|---|---|---|
| 最终 Windows 综合回归 | `482 passed`，1582.86 秒 | 59 个相关模块；控制输入/回执、所有管理配置族、Agent、Shell、发送、来源范围、Watch 和旧库升级 |
| 最终非 root Linux 综合回归 | `482 passed`，1298.30 秒 | 相同测试集合，真实 PostgreSQL、原生 Worker 和本地 HTTP/TCP，使用独立测试镜像 |
| Edge + 独立 Compose | 通过 | 当前运行候选；初始化、双 Bot、配置竞争、真实 Worker 热重载/升级、OneBot/Agent/Job 本地 HTTP、UNKNOWN 对账、重启恢复及注销 |
| Edge 管理回执专项 | `14 passed` | 受控 HTTP fixture + 实际发布 JS；五类面板、失效与旧响应、任务取消提示、跨 Bot 隔离、注销及 1440/390 像素布局 |
| 镜像和静态检查 | 通过 | 两个独立候选镜像与当前 289 个源码/测试文件、各 171 个安装文件逐项 SHA-256 一致；Ruff、七个 JS 语法、补丁空白及文档链接 |
| 测试清单核对 | 通过 | 当前 1316 项由各平台自身历史基线与后续回归覆盖；最后结果无失败，Windows 保留 4 项平台跳过，Linux 保留 1 项 |

两个 482 项综合回归使用同一冻结源码，均实际运行结束。清单核对不是一次最终全量重跑，也不把历史基线中的失败计为通过。浏览器专项不替代 PostgreSQL 或真实供应商验收。完整浏览器流程首次因未指定已安装的浏览器而退出，随后显式指定 Edge 完整通过；两次均只创建并清理独立随机 Compose 项目。以下保留各历史切片自身的验证范围。

内置 Shell 命令使用按 gate 版本冻结的 CommandSpec 表生成帮助、选择 action/执行方法并提供优先级与参数 schema。`shell-gate-v2` 的词法、摘要和执行映射不变，v3 保留本群来源列表、遗忘计划和提交，v4 增加 Bot 暂停/恢复，v5 增加组件事件和组件持久任务的列表、DEAD 回放和 plan/apply 退役，当前 v6 增加 Bot 审计元数据查询。Dispatcher 按请求持久化的 gate 版本直接执行类型化参数，不重新解析原文本；遗忘 apply 和 Bot pause 为 P0，resume、计划、退役和列表为 P1，后台回放接纳为 P2。Bot 维护写与 Web 共用 `bot.maintenance`、控制回执和维护版本 CAS；组件命令与 Web 共用 EventStore/ComponentJobStore 的管理修订和回执；审计命令与 Web 共用不读取详情的 BotAuditStore。控制输入不进入 Context，群回执不含原因、组件正文、审计详情或操作者主体。此表不等同动态插件命令或持久 registry snapshot，完整 P5 管理协议仍在实现。见[群聊 Shell 详细设计](09-group-shell-command-system.md)、[群管理协议](p3-control.md)和[Bot 审计协议](p5-bot-audit.md)。

| 固定 Shell 契约切片 | 结果 | 范围 |
|---|---|---|
| 最终 Windows 定向 | `57 passed`，12.39 秒 | 契约/帮助/schema、旧词法、真实排队恢复、当前撤权、跨群拒绝和优先控制槽 |
| 最终非 root Linux 定向 | `57 passed`，9.43 秒 | 相同五个测试模块，独立容器、专用 PostgreSQL 和实际 CONTROL 发送意图 |
| 静态与测试镜像核对 | 通过 | Ruff；266 个源码/测试文件及 162 个已安装运行文件与冻结工作树逐项 SHA-256 一致 |

本切片未修改管理页面或 Provider/OneBot 传输；群聊回执由真实存储与发送意图验证，未接入真实 QQ。下述 Kernel 的完整基线与浏览器证据保留各自运行范围，不将其冒充本切片重新执行的全量验证。

当前 036–038 将 Kernel 调用输入/结果、状态值/步骤回执、迁移源/候选及 generation 配置接入可清理载荷。Worker 实际暴露按代际累计，派生结果继承整个集合；真实 IPC 槽位后的交付、生命周期交付及最终提交均复验来源。来源失效回收进程，容量耗尽关闭新准入并排空已有调用，监督任务与 Host 续租独立。状态恢复支持 512 个键的确定分页；已删值保留 revision，通过明确的不可用错误及 CAS 重建，合法 JSON null 不受混淆。详见 [Kernel 正文与代际来源](p4-kernel-payloads.md)。

OneBot 原始 bytes 在进入 codec 前绑定真实平台来源；完整 codec 投影须与 Host 解析一致。模板回复、Agent 工具接纳/恢复和发送许可均继承真实 Kernel result 引用；发送许可还核对完成回执及精确 wire 值。配置来源域只作为删除依赖，不成为可召回记忆项。Web/watch 发布和图资格 SQL 已接入实际配置载荷。此前构建的 `7138ac2` 候选镜像保持原状，主服务仍运行 `9fb8fac` 镜像；本切片使用独立测试数据库和镜像验证。

全链路遗忘仍未完成：部署控制面剩余正文族、WAL/备份恢复约束和完整容量治理继续后续实施。Agent 配置、控制回执、公开来源管理、会话 Job 定义和群聊 Shell 参数/结果已经进入在线遗忘闭包。已获得许可的外部效果保留原有对账语义；晚到同源新 occurrence 仍缺持久 metadata-only ACK。

| Kernel 正文与代际来源切片 | 结果 | 范围 |
|---|---|---|
| Windows 全量基线 | `1220 passed, 10 failed, 4 skipped`，2850.57 秒 | 1234 项；旧调用接线、旧正文列断言、旧 Run 恢复权限及两个时序断言，修复结果由后续定向分别记录 |
| 非 root Linux 全量基线 | `1225 passed, 8 failed, 1 skipped`，2181.88 秒 | 相同 1234 项；无上述两个 Windows 时序失败，仅跳过 Windows DACL |
| `1976e01` Windows 集成 | `70 passed`，335.83 秒 | Kernel、调用树、实际 Worker 交付、代际回收和 Agent 来源撤销；包含延迟 admission 确认的完成、撤销及期限交错 |
| `1976e01` 非 root Linux 集成 | `70 passed`，291.50 秒 | 相同七个模块，独立镜像与专用 PostgreSQL |
| `1976e01` Edge + 独立 Compose | 通过 | 初始化、双 Bot、配置 CAS、热重载/升级、OneBot/Agent/Job 实际本地 HTTP、UNKNOWN 对账及重启恢复 |
| `1976e01` 镜像与静态检查 | 通过 | 261 个源码/测试文件及两个镜像中各 160 个已安装运行文件均与工作区 SHA-256 一致；Ruff 和本地文档链接通过 |
| 最终 Windows 兼容回归 | `200 passed`，302.26 秒 | 旧库升级、同群历史恢复与越权反例、Agent/Child、上下文、模型投影及平台来源；真实 PostgreSQL、Worker 和本地 HTTP |
| 最终非 root Linux 兼容回归 | `200 passed`，231.87 秒 | 与 Windows 相同的最终测试集合，独立容器和专用数据库 |
| Windows 生命周期时序复验 | `2 passed`，240.90 秒 | 完整 100 次热重载后维持原资源数量上限；100 毫秒子调用期限包含准入阶段 |
| 当前测试清单核对 | 通过 | 当前 1242 项均被各平台自身的基线与后续运行覆盖，每项最后一次结果无失败；Windows 保留 4 项平台跳过，Linux 保留 1 项 |
| 最终 Edge + 独立 Compose | 通过 | 兼容修复后的运行镜像；初始化、双 Bot、配置竞争、热重载/升级、OneBot/Agent/Job、UNKNOWN 对账和重启恢复 |
| 最终源码、镜像与静态检查 | 通过 | 263 个源码/测试文件及两个镜像中各 161 个已安装运行文件逐项 SHA-256 一致；Ruff、补丁空白和本地文档链接通过 |

`1976e01` 的定向补入了排空复核：持久 admission 已提交而 Host 尚未收到确认时，必须等待该调用，不能只检查内存集合。两项旧 Agent 测试调整为拦截真实 `invoke_with_receipt` 边界，并接受实际最先生效的两种来源撤销码；零状态写、零回复及 Run 终态断言继续保留。

最终兼容修复为旧 Run 增加有界的同会话历史证明，原精确 HTTP 请求、幂等身份和费用不重建；混合未发表私聊缓存、切口外记录和已遗忘内容仍被拒绝。五项新反例与旧流水线升级用例均包含于两平台 200 项回归。旧调用/状态升级断言改为独立读取实际载荷并核对原摘要、树身份和 revision，旧正文列仍要求 SQL NULL。Windows 两项时序断言只修正可观察边界：准入前超时允许没有子调用行，但已提交子调用必须终结且无残留；100 次重载在有界稳定空闲期内保持原任务数量上限，不增加泄漏容差。

以上基线、70 项集成、200 项兼容及生命周期复验是独立运行，不能作为一次最终全量通过的记录；原失败均已在相应最终回归中解决。浏览器验证使用最终兼容运行镜像，对端为本地 HTTP 测试服务，不代表真实 QQ、商业模型语义质量或实际账单联调。全部验证仅操作专用测试资源，主部署更新仍待许可。

以下是 `7138ac2` 的 029–035 历史证据：输入与图片、上下文与回复、Agent、子 Agent、发送正文使用永久事实头和可清理 body；原正文列禁止再次写入。内容接纳与遗忘通过 deletion epoch 串行裁决，旧模型请求仍保留精确 bytes、原摘要和费用事实。运行恢复会在读取已删除 checkpoint 失败时结束该 Run，保留静默排空步骤；子 Agent 在派发和接纳时同时复验载荷与当前记忆访问权限。工具的 JSON `null` 作为合法结果迁移，不与 SQL 空值混淆。详见[正文迁移协议](p4-content-migration.md)、[Agent 载荷](p4-agent-payloads.md)和[发送载荷](p4-delivery-payloads.md)。

旧来源不能证明完整依赖时使用显式保守范围。升级前已经发生删除的旧库，导入未知派生物时不会写入正文；034 为遗留的不完整 SourcePayload 追加独立删除决策，原请求和回执不变。真实 022 非空旧库验证覆盖未删除的无操作升级、预先删除和 257 条来源跨批次清理。001–028 的原 SQL checksum 不改，029 起连同版本化 handler 和冻结 helper 一起校验；升级要求先停止宿主，失败回滚 DDL、搬运和所有权失效。

035 处理自身回声早于成功回执进入上下文的情况：确认平台身份后，原输出、回声及确认前的派生副本建立删除依赖。原删除先发生时追加新的补偿决策，回执先发生时正常删除沿关联扩展；原事实均不改写。其他用户独立发送的相同文本保留。Web 发送记录按当前权限读取实际载荷；正文不可用时保留发送状态与平台消息 ID，页面显示内容已不可用。

该历史切片尚未覆盖 Kernel/状态和长期 Worker 缓存；这部分由上述 036–038 补齐。已发送给外部服务的内容、WAL 和备份不属于在线 body 清理范围；不得把内部机制当作完整公开遗忘功能。

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

以下 P3 定时切片为提交 `c93b6a4` 的历史验证，当时新增了[定时任务](p3-jobs.md)、现已撤销的[产品子 Agent](p3-children.md)和[群聊管理](p3-control.md)。这些结果只证明旧代码在当时可运行，不能作为现行 Agent 语义或验收证据。验证使用独立 PostgreSQL 17.10、原生 Worker、本地 TCP HTTP/SSE 与真实浏览器；尚未接入真实 QQ、付费 LLM 或 Embedding 凭据。

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

继续补齐宿主资源契约和其余消息契约，推进 P4 剩余载荷迁移、检索容量、备份保留/WAL 治理、恢复激活，以及 P5 完整管理和容量治理。已接通的 Web/Fleet、插件运行环境、OneBot/Binding、消息/发送、调用树、持久组件事件、组件持久任务及其管理面、持久单 Agent、会话定时任务和隔离恢复作为后续基础；首版目标仍按 01–12 实施，当前切片可运行不代表整个目标已经完成。
