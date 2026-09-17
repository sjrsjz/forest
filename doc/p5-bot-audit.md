# P5 Bot 审计元数据与群聊查询

## 1. 两层审计权威

Forest 不把多库日志拼成虚假的全局时间线。部署数据库的 `audit_records` 记录账号、目录、
部署配置和包管理；每个 Bot 数据库的 `bot_audit_records` 记录该 Bot 的控制与运行事实。
部署审计当前仍由旧的 `GET /management/v1/audit` 读取，待迁入 deployment 作用域贡献；Bot
审计由 `kernel.audit` 的通用管理查询
`GET /management/v1/scopes/bot/{bot_id}/modules/kernel.audit/queries/records` 和
`#forest audit list` 读取。旧 `/management/v1/bots/{bot_id}/audit` 已删除，不保留兼容入口。
两层结果只能分别分页和核对。

Bot 审计查询是观察接口。它不读取 Agent Context，不创建模型调用，不取得 ConversationSession，
也不启动任何产品 Agent。群聊入口进入独立 CONTROL 队列，优先级为 P1；同群 Agent 正在模型或
工具步骤中时也可执行。输出仍走原命令会话的 CONTROL lane，因此不会混入聊天回复序列。

## 2. 授权与范围

`audit.read` 由官方 `kernel.audit` 的 `forest.module-management/1` 契约声明，作用域固定为目标
`bot_id`；该模块以普通 `RuntimeModuleDescriptor` 参加 RuntimeGraph 和 Generation 生命周期。
Web 通过通用模块网关解析当前 ACTIVE Generation，不能直接构造 Store。`shell.command` 在描述符
中显式依赖 `kernel.audit`，并通过调用方控制对象绑定只读端口；Shell 不再 import 或构造
`BotAuditStore`。命令接纳时冻结 gate version 和规范参数。

执行事务开始时检查持久命令 claim、route/action/类型化参数、Host lease、Binding epoch 和原消息
的可信 principal；返回前再读取当前 Binding 配置并确认作者仍在 `admin_user_ids`。群主、昵称、
引用内容、OCR 和模型输出都不能授予权限。

当前 Shell 恢复仍由新宿主的 `shell.command` 实例处理持久 claim，尚未把目标 `kernel.audit`
Generation 单独固化到命令记录。因此这里只能声称权限 schema 与参数冻结，不能声称重启后继续
调用消息入场时的旧审计 Generation；这一缺口必须随统一官方/可安装 Worker 生命周期收口。

Web 后端先核对会话对目标 Bot 的目录可见性，再签发最长 30 秒、绑定 Bot/action/input digest/
policy revision 的票据。`audit.read` 属于固定 `READ_ACTIONS`，因此 admin 和 viewer 均可读取
脱敏 Bot 元数据。部署审计包含账号与资源关联，仍只允许 admin；两个权限不能相互推导。

## 3. 查询和稳定游标

存储按 `(created_at DESC, audit_id DESC)` 排序。当前表和时间/action 索引仍在巨型 Kernel 开发
基线中；目标形态是由 `kernel.audit` 的 schema bundle 从空命名空间创建。迁入管理贡献不代表
存储 bundle 已拆分完成。
请求可带精确 `action`、`before` 与 `limit`：Web 默认 50、上限 100；群命令默认 12、上限 20。
`before` 是上一页最后一项的 `audit_id`。服务端先在同一 Bot 内恢复它的 `created_at`，然后使用
严格小于 `(created_at,audit_id)` 的键集条件读取下一页。

审计事实不可更新或删除，所以同一时间戳由 ID 消除并列歧义。第一页之后出现的新记录位于旧
游标之前，不会混进续页；不存在的游标返回 `AUDIT_CURSOR_NOT_FOUND`，不会退化为从头读取。
action 筛选可以配合游标使用，但游标只表示 Bot 全局时间边界，不携带查询正文或签名票据。

## 4. 脱敏投影

`kernel.audit` 内部查询仓库的 SQL 只选择以下字段：

| 字段 | 含义 |
|---|---|
| `audit_id` | Bot 内不可变审计身份 |
| `action` | 已提交事实的动作名 |
| `operation_id` | 可为空的运行/控制关联 ID |
| `origin_kind` | 由受信控制操作元数据归类为 `web`、`shell` 或 `runtime` |
| `has_receipt` | 是否关联受保护的控制结果正文 |
| `created_at` | 带时区的提交时间 |

查询不选择 `details`，也不调用控制正文 hydration。响应不含 `control_subject_id`、QQ principal、
issuer、ticket basis、输入/结果 payload、消息、工具参数、模型文本或密钥。Shell 再次把结果压缩
为 action、来源类型、时间、audit ID 与续页命令，最多 6000 字符。WebUI 只以 `textContent`
建立节点，不把 action、ID 或日志内容解释为 HTML、Markdown、命令和配置。

读取自身不写 `bot_audit_records` 或 `bot_web_operations`，否则刷新页面会产生递归日志并改变
游标。票据仍在读取前后检查期限；查询超时或权限撤回不会提交部分业务效果。

## 5. 恢复与容量

`audit.read` 必须显式授予；`config.read`、`message.read` 或写权限都不能自动扩大为审计读取能力。
Web 查询始终解析当前 ACTIVE `kernel.audit` Generation；Shell 的跨宿主冻结目标缺口见第 2 节，
在完成前不得把它描述成旧 Generation 恢复。

审计当前仍随 Bot 数据库永久保存。本切片提供有界观察、索引和脱敏，不提供删除、全文检索、
宿主 stdout、容器终端、WAL 归档或备份恢复。长期容量必须由后续 Bot 备份/保留协议处理；不能
通过可变日志轮转破坏已有不可变事实。

## 6. 验证

真实 PostgreSQL 测试固定三条相同时间的记录，验证二元组分页、并发新增后续页隔离、action
筛选、非法输入和 issuer 最小权限。Shell 测试从两个群读取同一 Bot 的元数据，确认结果走
CONTROL 且敏感 `details` 不出现。ASGI 测试通过通用模块网关和真实 Web authority 读取目标
Bot，确认未知查询字段被拒绝、浏览器响应只有固定字段，并确认旧专用端点返回 404。运行时模块
图测试确认 `shell.command -> kernel.audit` 依赖、管理契约与 UI 工件都进入描述符投影。
