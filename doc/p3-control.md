# P3 群聊管理与任务监督

> **历史快照｜禁止作为当前实现规格。** 本文只记录旧纵向切片及其当时的局部验证。它服从 [00 架构总契约](00-architecture-contract.md) 与 01–14 号详细设计；冲突内容已经失效。不得据此新增或恢复旧表、编号迁移、兼容读取、固定业务页面、专用宿主 API、巨型 Store、业务组合根或产品子 Agent。需要复用算法时，必须先把 owner、namespace、Repository contract、Generation、UnitOfWork participant、并发 fence 和 management contribution 映射到当前设计。

> **历史文档原标签：实现切片。** 目标协议以[09 群聊 Shell](09-group-shell-command-system.md)为准。本页只说明 Shell 如何调用现有模块管理契约，不定义 gate 版本链或兼容旧 registry。

## 1. 命令来源与模块所有权

内核 Core Gate 只拥有 `core status/cancel/disable/reload`。其余命令随当前模块 Generation 注册：

| 命令域 | 所有模块 | 管理对象 |
|---|---|---|
| `run` | `agent.runtime` | Session/Run/checkpoint、取消、审批、UNKNOWN 工具核对 |
| `jobs` | `scheduler.jobs` | 会话触发 Job、occurrence、委托与重放 |
| `memory` | `memory.graph` | 当前群主干来源、关系、遗忘 plan/apply |
| `context` | `conversation.scheduler` | 当前会话视图 inspect/compact/reset |
| `event` | `routing.graph` | 组件事件 delivery、DEAD 重放、订阅退役 |
| `component-job` | `routing.graph` | 组件持久任务、DEAD 重放、handler 退役 |
| `module` | `component.worker-host` | 可安装组件的 validate/enable/disable、配置与 desired/effective 状态 |
| `runtime` | `kernel.runtime` | Bot runtime/admission 投影、维护 revision 与暂停/恢复业务接纳 |
| `audit` | `kernel.audit` | 当前 Bot 的脱敏审计元数据 |

Shell 控制器不能直接持有 `AgentStore`、`MemoryStore`、`JobStore` 或模块数据库连接。它从冻结 RouteGraph 解析准确 management operation，经 Contract Router 调用，并由目标模块执行最终授权、revision、lease 和资源所有权检查。Event 与 ComponentJob 是 `routing.graph` 的两类资源，不是两个伪内核模块；Bot 审计也只能通过 `kernel.audit` 端口读取。`kernel.runtime` 不代理可安装组件，`component.worker-host` 也不能改写 Bot admission。

## 2. 典型操作

```text
#forest run list
#forest run cancel <run_id> --expect-checkpoint <checkpoint_seq> --reason "停止回答"
#forest run approve <approval_id> --digest <arguments_digest>
#forest jobs list
#forest jobs run <job_id> --expect-revision <job_revision>
#forest jobs cancel <job_id> --expect-revision <job_revision> --scope pending
#forest jobs replay <occurrence_id> --expect-revision <occurrence_revision>
#forest memory sources --group here --limit 20
#forest memory forget-plan <source_unit_id> --expect-deletion-epoch <epoch>
#forest memory forget-apply <plan_id> --digest <digest> --expect-deletion-epoch <epoch>
#forest context inspect
#forest context compact --expect-view <context_view_revision>
#forest context reset --expect-view <context_view_revision>
#forest module validate <component_id> --expect-revision <revision>
#forest module reload <component_id> --expect-revision <revision>
#forest runtime pause --expect-maintenance-epoch <epoch> --reason "维护"
#forest runtime resume --expect-maintenance-epoch <epoch> --reason "恢复"
#forest audit list --action <action> --limit 20
```

列表只向原会话返回稳定 ID、阶段、revision、时间和脱敏摘要。工具参数、模型正文、其他群消息、管理员主体、密钥和完整 payload 只能在获得独立读取权限的模块 WebUI 中查看。

会话 Job 的 `replay` 只允许尚未入队且确定失败的同一 occurrence；UNKNOWN 外部效果不能换 ID 重发。事件/组件任务重放只接受 DEAD 与当前管理 revision。关系修改、遗忘、handler 退役和其他大影响操作使用 `plan -> apply`，plan 绑定主体、准确目标、当前 revision、影响摘要和期限。

## 3. 接纳、恢复与幂等

ShellGate 在 Inbox 接纳时保存命令的 owner component/generation、management operation、schema digest、规范参数载荷、RouteGraph digest、实际 service class 和资格依据。Dispatcher 重启后使用这些事实，不重新解析群消息，也不查当前 registry 猜测旧命令含义。

稳定 operation ID 由命令请求和目标动作派生。相同输入重投读取原回执；不同输入复用 ID 拒绝。plan 已提交、apply 已提交或业务结果已提交而外层 Shell 回执缺失时，恢复器按原 operation 查询并补齐 CONTROL 回执，不能再次取消、删除、创建任务或发出外部效果。

模块热重载后，旧命令仅在原 Generation 仍被允许排空且 schema/operation 完整可用时继续；否则明确终结为 `GENERATION_RETIRED`。新 Generation 不能用自己的 schema 解释旧参数。Forest 未发布基线直接删除旧 gate/helper/tests，不为它们建立 takeover 或升级表。

## 4. 优先级与群 Agent

Shell 使用独立有界控制池。P0 保留给受权取消、紧急 Bot disable、批准/拒绝和最小状态；查询、计划和模块 reload 为 P1；实际后台重算由 P4 Job 执行。优先级不抢断事务，不改变锁序，也不授予权限。

Shell 不占 Conversation Agent lane。控制消息按原 ingress 顺序进入安全历史投影但不触发 Agent。`context compact/reset` 仅登记请求，由唯一 Conversation Actor 在 IDLE 或下一次模型调用安全边界发布视图；不调用摘要模型、不删除未答触发、不并行修改 Prompt。

活动 Agent 期间的新群消息照常进入普通分类流水。Shell 完成不会让它们越过连续分类水位；下一次模型调用仍只推进到最后一个待触发记录，Agent 静默结束后再排空普通尾部。

## 5. Job 执行许可

Job Dispatcher 只把到期 occurrence 作为明确 `host_job` 来源写入指定 Conversation。它不直接调用模型、不拥有发送连接，也不创建第二个 Agent。定义冻结创建主体、目标 Conversation、许可范围、到期时间和 revision；每次入队前取得绑定具体 occurrence/claim epoch 的短效证明，并在目标 Bot 最终事务中复核。

成功持久入队是执行接纳点。浏览器注销不取消已经接纳的任务；账号停用或 policy revision 改变会阻止新的入队。已进入活动 Run 的任务必须通过具体 Run/checkpoint 取消，不能让 Job 的 `all` 隐式撤销同一 Run 中其他用户的触发。

## 6. 验收

- 当前 Core grammar 与模块 registry 的 schema/帮助一致，未知参数和非法来源拒绝；
- 重启后不重解析已接纳命令，相同 operation 不重复业务效果或发送；
- Agent 忙碌时 P0 仍有保留槽，未授权主体不能通过文本占用 P0；
- WebUI 与 Shell 基于同一 revision 竞争时最多一个 apply 成功；
- 跨群 source/run/job 目标按 scope 拒绝，群输出不泄露其他群正文；
- Context 操作只在 Actor 安全点发布且裁剪模型调用数为零；
- Job 双扫描者只接纳一个 occurrence，任务进入同一群队列和同一 Agent 执行链；
- 源码与测试不存在开发期 gate 版本 registry 或非空旧库升级夹具。
