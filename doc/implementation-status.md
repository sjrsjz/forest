# Forest 实现状态与验证记录

更新日期：2026-09-13。本文记录当前代码的实际能力，[01–12 设计文档](README.md)继续描述首版目标。**P0 目前包含独立 Web 管理与原生插件内核两个可运行切片；整个 P0 和首版框架仍在实现中。**后台还未接入 Bot 管理，开发命令与内核测试不代表 QQ、Agent 或记忆系统已经可用。

## 1. 当前可以执行的流程

从空部署启动独立管理服务，使用本机 CLI 交付的一次性配对码在浏览器创建首管理员，登录后读取配置、预览差异、基于预期版本提交，再读取实际生效修订。注销使服务端会话失效；重启后从原 PostgreSQL 和本地冻结计划恢复管理状态。此流程不依赖 Bot、插件、QQ、LLM 或 Embedding 凭据。

可操作命令见[根 README](../README.md)。Docker Compose 提供应用、私网 PostgreSQL 和一次性卷初始化服务；原生 Python 模式允许在首次 Web 初始化时填写 PostgreSQL URI。

新增的插件流程是：静态构建完整包 → 启动无生产权限的候选 Worker → 校验配置和服务 → 在专用 Bot 数据库接纳代际与调用 → 经宿主代理读写组件状态 → 记录结果并回收。`forest plugin build/check` 不需要数据库，`forest dev` 在 `FOREST_BOT_DSN` 指定的开发库中执行一次服务调用。程序接口还支持对已运行组件进行同 schema 的 drain 替换；CLI 尚无持续 watch。

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
| 生命周期协调 | 候选预热与正式写入分离；freeze/drain 后按目标库发布路由；同 schema 的 snapshot/restore；失败撤权、回收、受限恢复与宿主租约接管 | [kernel.py](../src/forest/kernel.py)、[内核集成测试](../tests/test_kernel.py) |
| 开发 CLI | `plugin build` 静态构建，`plugin check` 候选运行检查，`dev` 在专用开发库执行一次指定服务；JSON 可经标准输入提供 | [cli.py](../src/forest/cli.py)、[README 用法](../README.md#原生插件开发切片) |

## 2. 已实现配置契约

当前 WebUI 只支持部署资源 `resource_key=deployment`，这张配置表不替代新增 BotStore 的独立存储域。管理配置 schema 封闭，拒绝未知字段。

| 字段 | 默认值 | 合法范围 | 生效范围 |
|---|---|---|---|
| `display_name` | `Forest` | 1–80 字符 | 后台读取最新配置时使用的新显示名称 |
| `session_ttl_minutes` | `480` | 整数，5–1440 分钟 | 后续登录会话的到期时间；不修改已有会话 |

两个字段都属于当前管理切片的动态配置，所以 `desired` 与 `effective` 在同一提交中推进。返回值包含各自的 revision、values、schema digest 和创建时间；head 为 0 时返回空修订，由服务端提供默认值。管理 API 尚未接入需要代际切换、重启或迁移的配置，不能用此行为推断未来所有配置都立即生效。

Web 请求采用“服务器保存计划 → 页面显示差异 → 按 plan ID/digest、expected revision 与稳定幂等键提交”的流程。配置竞争属于显式仲裁，不宣称可交换或最后写入覆盖。计划通常有效 10 分钟；同一次已成功请求的重试不因为计划过期而重复创建修订。

### 2.1 当前插件包与配置

包格式限定 `runtime=python-worker`、一个 `scope=bot` 组件、`state_schema=1`、`reload_mode=drain`；服务声明输入/输出 JSON schema 和契约 major.minor。当前能力只提供显式 `state.read/state.write`，尚不接纳跨组件启动依赖；不支持的约束或 manifest 字段会明确拒绝，不能把未执行的 `memory_mb` 等限制显示为受保护。

资源字段包含 `max_frame_bytes`、`max_inflight`、`max_queue_items`、`max_state_inflight`、`max_owned_tasks`，生命周期字段包含 prepare/start/drain/stop 的 timeout。它们约束本切片的 IPC、调用与受监督任务，不能等同操作系统沙箱、CPU/内存硬配额或完整资源 SDK。普通 Python 插件仍要求可信来源。

`check/dev --config-json` 默认提供 `{}`，按包配置 schema 验证；Kernel 为一次安装固定配置，Worker 取得只读视图。此入口服务插件开发，尚未连接 WebUI 的 Bot 配置修订和 desired/effective 页面。管理员的日常配置仍应通过目标设计中的 WebUI 完成，不要求手改插件目录文件；开发者目前显式维护包 manifest 与 schema，和管理员配置是不同职责。

### 2.2 状态、调用与替换的实际语义

BotStore 首次迁移要求专用空库，已经初始化的库只接受其固定 Bot；既有部署数据库和其他 Bot 库不能被转换为当前 Bot 的存储。宿主持有 DSN，Worker 不能通过 RPC 参数选择数据库、Bot、组件命名空间或伪造 CallGrant。不同组件使用相同 state key 时互不覆盖。

状态步骤以预期 revision 做 CAS，步骤回执、状态新修订、审计和 outbox 在同一事务保存。并发冲突明确拒绝，不声称 CAS 就证明业务合流。一次服务可以先提交状态，再返回结果；状态步骤与整次调用完成是两个提交边界，后续异常不回滚已接纳步骤。稳定 operation ID 防止重试悄悄重做，失效调用不能凭旧回执或缓存 Context 获得新的执行权。

排空替换要求新旧包均满足当前 schema，路由在目标 Bot 库切换，旧代不能继续提交；候选失败保留或按守卫恢复可用旧代。过期宿主由新 incarnation 接管，旧宿主/旧 Worker 的最终写入被 fencing 拒绝。当前存在 JSON snapshot/restore 钩子和持久状态保留，但没有任意状态迁移、Python 栈迁移、完整 checkpoint 恢复器或后台自动恢复全部未完成操作。

## 3. 与完整设计的差距

| 阶段 | 状态 | 尚未实现的必要内容 |
|---|---|---|
| P0 契约与可靠内核 | 部分实现 | 已有管理和原生 Worker/SDK/Scoped Store 切片；仍缺依赖 DAG、完整消息/服务契约、自动声明生成、通用服务调用与事件/Job、完整资源 SDK，以及管理与 Bot 内核接线 |
| P1 群聊与多 Bot | 未完成 | 已有独立 Bot 数据库与组件作用域基础；尚无 Web Bot 创建/接入、OneBot/Binding、全量入站、作者来源、同群串行跨群并行、公平调度与可靠发送 |
| P2 热重载与恢复 | 基础协议部分实现 | 已有不可变包、generation/fencing、同 schema 排空与路由切换；仍缺自动 watch、完整依赖环境锁定、任意状态迁移、checkpoint/操作自动恢复、需代际生效的 Web 配置 |
| P3 Agent 与上下文 | 未开始 | Session/Run、下一模型调用切口接入、触发排队、静默排空、零摘要裁剪、工具/审批/取消/定时与子 Agent |
| P4 多模态多主干记忆 | 未开始 | 每 Bot 全局图文记忆、每群主干、原生图文嵌入、图片/混合召回、模型输入投影、来源/删除屏障、合流 reducer |
| P5 完整管理与容量 | 未开始 | 完整 WebUI、群聊 Shell、共用类型化管理服务、角色/密钥/预算管理、任务日志、备份迁移恢复及容量治理 |

初始化 bootstrap 仍使用跨进程事务固定计划并依靠目标库幂等初始化；它的 `owner_epoch=1` 表示该初始化子系统没有接管 API。新 BotStore 的宿主/组件执行租约与 fencing 是独立协议，不能据此宣称 bootstrap 或 Bot 存储迁移已经支持完整接管恢复。

部署配置及已实现 Bot 状态/生命周期事实均保存相应审计/outbox，尚无通用后台消费者、投递重试或外部效果对账。配置页直接读取 PostgreSQL 当前生效值，因此这个管理闭环不依赖 outbox 消费来宣称生效。原生 Worker 的 state RPC 也不能被视为完整 Agent 工具或平台 Effect 系统。

当前只有首管理员创建和登录，没有账号管理、密码重置、浏览器持久草稿、配置历史页面、回退操作或 Web 备份恢复。初始化完成后不能用 `setup-token` 再次注册或删除标记重开注册。后台还不能创建 Bot、连接 QQ、配置模型或管理记忆；这些是后续实现工作，不能从页面与目录命名推断已存在。

## 4. 验证证据与适用范围

验证分为本轮内核结果与此前管理切片记录。此前 Linux/浏览器结果证明当时的管理实现，不代表新增内核已经接受相同平台的验证。没有接入真实 QQ、LLM 或 Embedding 服务。

本轮新增内核已在 Windows 与非 root Linux 容器中完成全量测试，两者均连接真实 PostgreSQL。以下计数是已经结束的测试结果，不是待执行计划；后续改动仍需重跑相应检查。

| 验证 | 已有结果 | 能证明的范围 |
|---|---|---|
| 本轮：Windows / Python 3.11 全测试，真实 PostgreSQL | `162 passed, 4 skipped`，46.62 秒 | 既有管理与新增包/Worker/BotStore/Kernel 回归；跳过 3 项 POSIX 专项及 1 项大小写路径碰撞测试 |
| 本轮：非 root Linux Docker / Python 3.11 全测试，真实 PostgreSQL | `165 passed, 1 skipped`，26.16 秒 | 当前代码的管理、原生 Worker、内核和数据库回归；仅跳过 Windows DACL 专项 |
| 本轮：不可变包 | Windows `45 passed, 1 skipped` | 静态构建不执行代码、完整快照/篡改检测、schema/资源约束、链接/捕获竞态及失败工件清理；跳过大小写路径碰撞 |
| 本轮：真实 Worker / SDK | `27 passed` | 真实解释器和认证 IPC、候选权限、服务调用、有界队列/任务、取消/回收、调用 Context 绑定及状态 RPC 清理 |
| 本轮：BotStore / Kernel | `25 passed` / `15 passed` | 真实数据库隔离与 CAS、host/generation/call fencing、同 schema 排空替换、错误恢复、启动/关闭/取消及幂等回归 |
| 本轮：`forest plugin check` | 实际命令通过 | 示例包的静态检查、候选启动/健康检查与回收；不代表完整开发 watch 或真实平台接入 |
| 本轮：`forest dev` 独立进程与持久幂等 | 三个宿主进程依次调用 `first / first / second`，得到 value/revision：`1/1 → 1/1 → 2/2` | 使用 stdin JSON 与临时独立 Bot 数据库；宿主退出重启后状态保留，同操作不重复加一；测试结束只清理本次数据库 |
| 本轮：静态与补丁检查 | 全仓库 Ruff、页面 JavaScript 语法、`git diff --check` 通过 | 当前 Python/JavaScript 静态问题及空白检查，不代替执行或浏览器测试 |
| 上一管理切片：Windows / Python 3.11 全测试，独立 PostgreSQL 17.10 | `50 passed, 3 skipped`，13.31 秒 | 安全、bootstrap、repository 与 HTTP 管理；3 项为当时 Windows 不适用的 POSIX/符号链接测试 |
| 上一管理切片：PostgreSQL repository 专项 | `10 passed` | 两连接/不同管理员与两个独立进程的 CAS 竞争；初始化唯一性；锁等待后撤销；outbox 回滚；计划/幂等及迁移不可变性 |
| 上一管理切片：Linux 容器 / Python 3.11、真实 PostgreSQL | `52 passed, 1 skipped`，6.69 秒 | POSIX 权限及当时的管理/事务测试，跳过 Windows DACL 专项；不证明新增 Worker/Kernel 的 Linux 结果 |
| 上一管理切片：真实 Edge 浏览器完整流程 | 复验通过 | 初始化、登录、预览、应用、双窗口冲突、重启持久化、注销撤销及桌面/移动布局 |
| 上一管理切片：静态检查 | 全仓库 Ruff 与页面 JavaScript 语法检查通过 | 当时 Python 静态问题及 JavaScript 解析；本轮需另外重跑 |
| 上一管理切片：Compose 实际启动 | 应用镜像 healthy，PostgreSQL 保持私网 | 当时管理服务及容器启动闭环，不代表新增内核或容量验收 |
| QQ / LLM / Embedding 联调 | 未执行，相应实现也尚未完成 | 不存在以模拟调用替代真实外部服务验收的结论 |

测试运行方式见[README 测试说明](../README.md#运行测试)。未设置 `FOREST_TEST_DSN` 时 PostgreSQL 测试会跳过，这类结果不能用作事务验收证据。HTTP 测试通过 ASGI transport 调用真实应用和真实 PostgreSQL；它不等同于浏览器渲染测试。

[浏览器验收脚本](../scripts/smoke_browser.py)创建独立的随机 `forest-e2e-*` Compose project，默认选择临时端口，执行首次初始化、登录、预览应用、双窗口冲突、重启持久化和注销撤销。脚本 `finally` 清理自己创建的容器与卷，截图保存在 `.local/browser-artifacts/`。浏览器使用 Playwright Chromium，或 `--browser` 指定已安装的 Edge/Chromium。

本轮新增的验证范围包括真实子进程失败/取消与资源回收、包快照完整性、Bot 隔离、数据库状态 CAS、宿主接管/旧代拒写、候选失败和同 schema 替换。Linux 结果覆盖当前新增内核；本轮没有重新执行真实浏览器流程，界面证据仍属于上一管理切片。仍未覆盖的首版门槛包括通用 reducer 性质、Agent 输入切口竞态、发送 UNKNOWN 对账、多模态索引/删除竞争、完整依赖环境与 checkpoint 恢复，以及持续容量压力；这些依赖对应实现，后续必须提供可执行证据。

## 5. 下一步

继续补齐 P0 的契约与资源边界，将独立管理底座连接到已有 BotStore/Kernel；扩展依赖解析、声明工具链和通用服务/事件/Job。随后按 01–12 推进真实平台消息、完整热重载恢复、Agent、多模态记忆与后台管理，保持实现状态和验证证据同步。当前两个切片可运行，不代表整个 P0 或首版框架已经完成。
