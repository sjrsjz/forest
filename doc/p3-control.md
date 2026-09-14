# P3 群聊管理与任务监督

当前内置命令接纳版本为 `shell-gate-v9`：除既有 Agent、Job、Memory、Bot 维护、组件事件、组件持久任务、审计、Context 和模块 list/status/reload 外，已增加固定 Core status/cancel/disable/reload。Core 只为既有权威提供无插件依赖的稳定 form；status/cancel/disable 是 P0，reload 是 P1。完整协议见 [P5 Core 救援入口](p5-core-shell.md)与 [P5 组件生命周期 Shell](p5-module-lifecycle-shell.md)。

本文描述当前代码提供的 Shell 与后台监督入口；完整管理命令、运维进程和账号治理仍以 [09](09-group-shell-command-system.md)、[12](12-webui-administration.md) 为目标契约。

## 1. 群聊中的实际操作

Binding 的 Web 连接设置维护 `admin_user_ids`。Shell 每次操作以原消息的 Adapter 可信账号重新校验当前名单；昵称、群主角色、匿名身份、引用消息、Bot 自己的消息不授予管理权。本轮命令管理发令会话的任务、Agent 和记忆主干；Bot 全局配置与跨群来源视图在 Web 后台修改。

```text
#forest help
#forest status
#forest run list
#forest run cancel <run_id> --expect-checkpoint <checkpoint_seq> --reason "停止回答"
#forest run approve <approval_id> --digest <arguments_digest>
#forest run reject <approval_id> --digest <arguments_digest>
#forest jobs list
#forest jobs status <job_id>
#forest jobs run <job_id> --expect-revision <job_revision>
#forest jobs cancel <job_id> --expect-revision <job_revision> --scope future --reason "停止未来计划"
#forest jobs cancel <job_id> --expect-revision <job_revision> --scope pending --reason "取消尚未领取的任务"
#forest jobs cancel <job_id> --expect-revision <job_revision> --scope all --reason "停止计划及待处理任务"
#forest jobs replay <occurrence_id> --expect-revision <occurrence_revision>
#forest memory list
#forest memory forget-plan <source_unit_id> --expect-deletion-epoch <deletion_epoch>
#forest memory forget-apply <plan_id> --digest <plan_digest> --expect-deletion-epoch <deletion_epoch>
#forest event list
#forest event replay <delivery_id> --expect-revision <management_revision>
#forest event retire-plan <subscriber_id> --contract-major <major> --expect-revision <management_revision>
#forest event retire-apply <plan_id> --digest <plan_digest> --expect-revision <management_revision>
#forest component-job list
#forest component-job replay <component_job_id> --expect-revision <management_revision>
#forest component-job retire-plan <component_id> <handler_id> --expect-revision <management_revision>
#forest component-job retire-apply <plan_id> --digest <plan_digest> --expect-revision <management_revision>
#forest audit list
#forest audit list --action <action> --limit <1..20>
#forest audit list --before <audit_id> --limit <1..20>
```

`job/jobs`、`run/runs`、`event/events`、`component-job/component-jobs` 是语法别名，`log/logs` 是 `audit` 的别名。取消必须提交列表显示的检查点或修订。`future` 不撤回已创建任务；`pending` 不停止未来计划；`all` 同时执行两者，但若有关联任务已经进入活动 Run，则整个请求拒绝，要求显式取消该 Run，因为它可能还覆盖其他用户的触发。会话 Job 的 `replay` 只允许从未入队、确定失败且委托仍有效的同一 occurrence，不通过新 ID 重发未知效果。组件事件/任务回放仅接受 DEAD 和当前管理修订，退役必须先 plan 再 apply，且不抢断 INFLIGHT Worker。审计列表是 Bot 全局的脱敏元数据查询，不是宿主系统 Shell，不返回日志正文或操作者身份。

会话列表只把本群的状态、稳定 ID、检查点、参数摘要和发送状态发群；Bot 全局组件列表只显示处理器/订阅及 delivery/Job 的受限元数据。工具详细参数在受认证 Web 页面核对。批准仍针对已经冻结的参数摘要。管理查询不会把其他群的模型内容、任务提示、组件正文、密钥或完整工具结果发到本群。

## 2. 固定解析与共同管理协议

`shell_commands.parse_command` 是有界确定性解析器：只识别直接开头的 `#forest`，允许前置空白及一次对当前 Bot 的 mention。后续含图片、转发等其他内容则保留控制命名空间，但不执行成合法命令；引号按固定语法处理，不使用 `eval` 或系统 shell。命令长度、标识、数值和公开原因均有限制。

007 既有来源记录继续保留原 gate v1；020 追加 `arguments`、`priority` 不可变列。gate v2 把成功解析的类型化参数独立保存，未知/坏语法只保存空参数和原始内容摘要。`shell-gate-v3` 复制 v2 的全部冻结契约并增加三条记忆命令，v4 增加 Bot 暂停/恢复，v5 增加组件事件和组件持久任务管理，v6 增加 Bot 审计元数据查询，v7 增加 Context inspect/compact，v8 增加 module list/status/reload，v9 增加固定 Core form 与 P0 status。新接纳命令持久保存 v9，既有 v2–v8 请求仍由各自原表解析。原参数从首次接收起不进入 Context；群历史仍保留原作者、ingress 位置和固定控制消息投影。既有请求不会按新语法重新解释。

`shell_registry.BUILTIN_GATE_V2` 固定 11 个旧命令键；v3、v4、v5、v6、v7、v8 依次复制旧表并增加 3、2、8、1、2、2 个命令键，v9 不增加 action/key，只为三项既有权威增加 Core form 并把新接纳的 Core status 改为 P0。每个 `CommandSpec` 给出版本、参数 schema 与摘要、目标 action/方法、当前管理员要求、会话/群主干/Bot 组件作用域、优先级、执行种类、群输出范围和 Host 期限策略引用；帮助按请求 gate 从对应 schema 与命令形式生成。注册表不可修改，导出的 schema/manifest 是独立副本。期限仍由 Host 的 `command_timeout` 决定，默认 5 秒、允许 0.1–30 秒；这不是所有已领取命令的固定 deadline，领取记录及最终事务守卫保持原语义。

这次接线没有改变 `shell-gate-v2` 的 `shlex.split(..., posix=True)` 词法、别名或参数边界；后续 gate 继续使用相同词法，新的 Windows 路径词法、`--key=value` 和 `--` 扩展尚未实现。排队后直接使用已冻结的 `gate_version/command_name/arguments/priority`，不重新解析原消息或套用新 schema。数据库持久保存接纳 gate 版本；导出的 command/schema 摘要还不是逐请求持久的动态 registry snapshot，插件命令注册、热替换命令所有者和多版本动态快照恢复仍属后续工作。

`ShellControl` 使用宿主内部的不透明 admission，而非伪造 Web 登录或 QQ 用户。它在目标事务中锁定当前 Binding、Bot/Host 与原命令领取记录，复验所有权、期限、当前管理员、冻结 gate、原参数、实际动作和目标会话，再复用 `AgentStore`、`JobStore`、`MemoryAdministration`、`EventStore`、`ComponentJobStore`、`ModuleAdministration` 和 `BotAuditStore` 的管理方法。记忆计划按原群筛选来源，提交再次验证计划的主体、issuer、来源主干、删除 epoch、目标闭包和影响数。模块重载的进程内不透明委托只能调用该命令固定的 `config.plan/config.apply` 子操作，配置值由 Host 从预期不可变修订读取，用户参数不能指定值或替换 action。共同的操作账本保持稳定 `shell-action:<command_id>`；提交后丢失本地确认可以读取原结果，不能再次取消、删除或创建任务。审计读取不创建新操作回执，避免递归日志。Web 使用自己的主体与短效签名票据调用相同方法。

Shell 参数、结果与 CONTROL 回复的正文治理、遗忘终止和锁序见 [P4 Shell 参数与结果载荷](p4-shell-payloads.md)。该协议保持这里的命令语义与优先级不变。

## 3. 控制优先级与资源归属

Shell 不占群 Actor 或模型槽。默认最多 4 个控制操作，取消/审批/遗忘提交 priority 0，查询与遗忘预览 priority 1，手动执行/重放 priority 2。普通操作最多占 3 槽，为 P0 操作预留 1 槽。持久扫描以 command ID 作稳定分页，页内按 priority 排序；页游标取原 ID 最大值，避免改变排序后漏掉命令。

优先级只影响尚未开始的操作；在执行中的数据库事务和平台请求仍受自身期限约束。扫描页和并发数均有界；不能声称拥有整个待处理集合上的严格全序或操作系统抢占能力。CONTROL 回执独立于 CHAT 序号，不完成会话触发、不扩大模型切口，也不被早期 CHAT UNKNOWN 阻塞。

长任务由受监督的持久执行器运行。Shell 接纳任务后返回 operation/occurrence，随后可以查询；它不在控制槽里等待模型完成。关闭平台时，先停止 Job 接纳，再回收群调度、Shell、发送器、私有分析及其 HTTP 客户端。

## 4. 定时任务的执行许可边界

`JobDispatcher` 周期扫描和通知共同驱动任务，重启后依据数据库恢复；默认最多 4 个入队操作。每个执行操作有 10 秒上限，持久领取 lease 为 30 秒。临时失败延迟 30 秒再尝试，同一 occurrence 保持原身份；已确认入队的记录不会因本地确认丢失重复插入。

Web 创建定义时保存有明确期限的长期委托。后台 `WebAuthority.issue_delegation` 不使用旧浏览器 session，每次入队前重新校验创建账号仍启用且为管理员、账号与密钥修订未改变，签发仅适用于具体 occurrence/委托/领取 epoch 的 30 秒执行票据。目标 Bot 再校验当前 issuer、policy、定义和委托。

**成功持久入队是任务执行接纳点。** 注销不会取消计划；账号随后停用或修订改变会阻止新的执行接纳，但不会追溯撤销已接纳队列。既有短票据有最多 30 秒撤销窗口。Bot 本地 policy/issuer、委托到期在领取群触发前再次复核；已失效的未领取任务不能开始新的回答。取消既有任务使用上述 `pending/all/run cancel` 明确动作。

本轮定时委托只授予 `conversation.trigger`，没有工具委托。任务作者标记为 `host_job`，保留委托和 occurrence，不能成为平台管理员或借用其他用户身份调用插件。任务回复仍由唯一群会话链提交。

## 5. 验证入口与当前范围

- `tests/test_shell_commands.py`：语法、边界、未知参数丢弃和来源识别。
- `tests/test_shell_registry.py`、`test_shell_registry_v7.py`–`v9.py`：固定契约、不可变导出、帮助形式与 schema/别名一致、既有词法边界和 Core form/priority。
- `tests/test_shell_registry_runtime.py`：真实持久帮助回执，以及 Dispatcher 重建后不重解析已接纳参数、不重复业务操作和发送意图。
- `tests/test_shell_management.py`：忙时取消、费用 UNKNOWN 保留、同操作重放、当前撤权、跨群目标拒绝、安全输出及预留控制槽。
- `tests/test_job_authority.py`：注销后的有效委托、停用/角色/修订撤销、目的绑定和签名检查。
- `tests/test_job_scheduler.py`：两个扫描器的同 occurrence 接纳、真实群队列切口、账户撤权退避、任务与协程回收。

动态命令注册、跨 Bot 授权、进程/runbook、日志订阅、配置 Shell 动作、记忆图编辑、部署账号管理及账户停用的可选批量取消尚属后续 P5。本文测试使用独立 PostgreSQL 和本地 Provider/Adapter 服务，未使用真实 QQ 或付费模型凭据。
