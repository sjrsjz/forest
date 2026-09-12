# Forest 实现状态与验证记录

更新日期：2026-09-13。本文记录当前代码的实际能力，[01–12 设计文档](README.md)继续描述首版目标。**当前仅完成 P0 中独立 Web 管理的纵向切片；整个 P0 和首版框架仍在实现中。**

## 1. 当前可以执行的流程

从空部署启动独立管理服务，使用本机 CLI 交付的一次性配对码在浏览器创建首管理员，登录后读取配置、预览差异、基于预期版本提交，再读取实际生效修订。注销使服务端会话失效；重启后从原 PostgreSQL 和本地冻结计划恢复管理状态。此流程不依赖 Bot、插件、QQ、LLM 或 Embedding 凭据。

可操作命令见[根 README](../README.md)。Docker Compose 提供应用、私网 PostgreSQL 和一次性卷初始化服务；原生 Python 模式允许在首次 Web 初始化时填写 PostgreSQL URI。

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

## 2. 已实现配置契约

当前只支持部署资源 `resource_key=deployment`，不把这张表用于假装实现 Bot 独立存储域。配置 schema 封闭，拒绝未知字段。

| 字段 | 默认值 | 合法范围 | 生效范围 |
|---|---|---|---|
| `display_name` | `Forest` | 1–80 字符 | 后台读取最新配置时使用的新显示名称 |
| `session_ttl_minutes` | `480` | 整数，5–1440 分钟 | 后续登录会话的到期时间；不修改已有会话 |

两个字段都属于当前切片的动态配置，所以 `desired` 与 `effective` 在同一提交中推进。返回值包含各自的 revision、values、schema digest 和创建时间；head 为 0 时返回空修订，由服务端提供默认值。涉及代际切换、重启或迁移的配置尚不存在，不能用此行为推断未来所有配置都立即生效。

Web 请求采用“服务器保存计划 → 页面显示差异 → 按 plan ID/digest、expected revision 与稳定幂等键提交”的流程。配置竞争属于显式仲裁，不宣称可交换或最后写入覆盖。计划通常有效 10 分钟；同一次已成功请求的重试不因为计划过期而重复创建修订。

## 3. 与完整设计的差距

| 阶段 | 状态 | 尚未实现的必要内容 |
|---|---|---|
| P0 契约与可靠内核 | 部分实现 | Worker/SDK、消息契约、Scoped Store、通用 CommitGuard、受监督生命周期、普通 Python 插件加载及失败清理 |
| P1 群聊与多 Bot | 未开始 | OneBot、Bot/Binding 独立身份和存储、全量入站、作者来源、同群串行跨群并行、公平调度、可靠发送 |
| P2 热重载与恢复 | 未开始 | 不可变插件包、代际/epoch fencing、冻结排空、状态迁移、路由切换、故障恢复和需代际生效的配置 |
| P3 Agent 与上下文 | 未开始 | Session/Run、下一模型调用切口接入、触发排队、静默排空、零摘要裁剪、工具/审批/取消/定时与子 Agent |
| P4 多模态多主干记忆 | 未开始 | 每 Bot 全局图文记忆、每群主干、原生图文嵌入、图片/混合召回、模型输入投影、来源/删除屏障、合流 reducer |
| P5 完整管理与容量 | 未开始 | 完整 WebUI、群聊 Shell、共用类型化管理服务、角色/密钥/预算管理、任务日志、备份迁移恢复及容量治理 |

当前 bootstrap 使用跨进程事务固定计划并依靠目标库幂等初始化；`owner_epoch=1` 表示没有接管 API。**尚未实现设计中的可接管 execution lease/fence**，也没有把该局部方案推广到生命周期、外部效果或 Bot 存储迁移。

当前 outbox 已和配置事实原子保存，尚无通用后台消费者、投递重试或效果对账。配置页直接读取 PostgreSQL 当前生效值，因此这个管理闭环不依赖 outbox 消费来宣称生效。

当前只有首管理员创建和登录，没有账号管理、密码重置、浏览器持久草稿、配置历史页面、回退操作或 Web 备份恢复。初始化完成后不能用 `setup-token` 再次注册或删除标记重开注册。后台还不能创建 Bot、连接 QQ、配置模型或管理记忆；这些是后续实现工作，不能从页面与目录命名推断已存在。

## 4. 本轮验证证据

以下结果来自当前开发过程，后续代码修改后应重跑对应检查。测试中使用真实 PostgreSQL，但没有接入真实 QQ、LLM 或 Embedding 服务。

| 验证 | 已有结果 | 能证明的范围 |
|---|---|---|
| Windows / Python 3.11 全测试，配置独立 PostgreSQL 17.10 | `50 passed, 3 skipped`，13.31 秒 | 当前安全、bootstrap、repository 与 HTTP 管理测试，包含数据库故障安全响应与恢复；3 项跳过为 Windows 不适用的 POSIX/符号链接测试 |
| PostgreSQL repository 专项 | `10 passed` | 两连接/不同管理员与两个独立进程的 CAS 竞争；初始化唯一性；锁等待后账号或会话撤销；outbox 故障整事务回滚；计划/幂等及迁移不可变性 |
| Linux 容器 / Python 3.11 全测试，真实 PostgreSQL | `52 passed, 1 skipped`，6.69 秒 | POSIX 权限与相同管理/事务测试；仅跳过 Windows DACL 专项，不等同于浏览器或完整容量验收 |
| 真实 Edge 浏览器完整流程 | 最终复验通过 | 初始化、登录、预览、应用、双窗口冲突、重启持久化、注销撤销；桌面/移动截图已检查，移动标题与注销按钮排版修复后重跑通过 |
| 静态检查 | 全仓库 Ruff 与页面 JavaScript 语法检查通过 | Python 静态问题及 JavaScript 解析；不代替执行测试 |
| Compose 实际启动 | 当前应用镜像启动且 healthy，PostgreSQL 保持私网 | 当前管理服务及容器启动闭环，不代表全部设计能力或容量目标通过 |
| QQ / LLM / Embedding 联调 | 未执行，相应实现也尚未完成 | 不存在以模拟调用替代真实外部服务验收的结论 |

测试运行方式见[README 测试说明](../README.md#运行测试)。未设置 `FOREST_TEST_DSN` 时 PostgreSQL 测试会跳过，这类结果不能用作事务验收证据。HTTP 测试通过 ASGI transport 调用真实应用和真实 PostgreSQL；它不等同于浏览器渲染测试。

[浏览器验收脚本](../scripts/smoke_browser.py)创建独立的随机 `forest-e2e-*` Compose project，默认选择临时端口，执行首次初始化、登录、预览应用、双窗口冲突、重启持久化和注销撤销。脚本 `finally` 清理自己创建的容器与卷，截图保存在 `.local/browser-artifacts/`。浏览器使用 Playwright Chromium，或 `--browser` 指定已安装的 Edge/Chromium。

测试尚未覆盖的首版门槛包括通用 reducer 性质、Kernel/Worker 故障、Agent 输入切口竞态、发送 UNKNOWN 对账、多模态索引及删除竞争、真实热重载恢复和容量压力；这些依赖对应实现，后续必须提供可执行证据。

## 5. 下一步

继续完成 P0 的持久执行权与微内核纵向切片，将独立管理底座与受监督 Worker、SDK、Scoped Store 和 CommitGuard 连接。之后按设计的 P1–P5 推进实际 Bot、Agent、多模态记忆和完整管理能力，并同步此记录。当前管理切片完成不代表长期目标或整个 P0 已完成。
