# P0 组件持久任务管理

> **历史快照｜禁止作为当前实现规格。** 本文只记录旧纵向切片及其当时的局部验证。它服从 [00 架构总契约](00-architecture-contract.md) 与 01–14 号详细设计；冲突内容已经失效。不得据此新增或恢复旧表、编号迁移、兼容读取、固定业务页面、专用宿主 API、巨型 Store、业务组合根或产品子 Agent。需要复用算法时，必须先把 owner、namespace、Repository contract、Generation、UnitOfWork participant、并发 fence 和 management contribution 映射到当前设计。

历史快照当时状态：首版已实现。本协议管理插件组件自己的持久 Job。它不读取群聊上下文，不向任何
会话排队消息，也不创建产品 Agent；群会话的唯一 Agent 定时任务仍由 P3 Job 协议管理。

## 1. 管理资源与授权

每个处理器由 `(bot_id, component_id, handler_id)` 唯一标识。head 保存 `active`、
`management_revision`、当前 generation、schema 摘要和执行预算。每个 Job 另有独立的
`management_revision` 与 `replay_epoch`。运行时 lease、重试和 ACK 不推进管理修订，Web
操作者只和显式管理动作竞争。

目标 Bot 数据库验证 30 秒内有效、绑定账号会话、Bot、action、operation、输入摘要、
policy revision 和可选 plan digest 的 Ed25519 票据。管理权限按旧能力最小派生：

| action | 迁移时所需旧权限 |
|---|---|
| `component_job.read` | `config.read` 或 `message.read` |
| `component_job.replay` | read 条件并且 `service.invoke` |
| `component_job.retire.plan` | read 条件并且 `config.plan` |
| `component_job.retire.apply` | read 条件、`config.apply` 和 `message.cancel` |

写请求必须带稳定 `Idempotency-Key`。Web 层将账号、Bot、action 和该 key 摘要为 operation；
目标库保存规范化输入与结果正文回执。相同 operation 重试返回原结果，不重新执行状态转换。

## 2. 元数据读取

`routing.graph` 在 Bot scope 声明 `component-jobs` query，通用管理传输路径为
`GET /management/v1/scopes/bot/{bot_id}/modules/routing.graph/queries/component-jobs`。
该查询支持 `component_id`、`handler_id`、
`status` 和 1–200 的 `limit`。结果包含处理器路由、预算、活动状态和管理修订，以及 Job
身份、顺序、状态、计数、时间、回放轮次和正文可用性。接口不读取或返回 Job payload、
结果正文、claim token、宿主 lease 或 Worker 帧。

`replay_allowed` 只在 Job 为 `DEAD`、处理器仍活动且正文仍可用时成立。页面中的状态筛选
发生在服务端；切换 Bot、注销或收到相关正文失效通知会推进页面 epoch，使此前发出的晚到
读取不能覆盖当前视图。

## 3. 死信回放

`component-job.replay` action 接收 `job_id` 和 `expected_revision`，通过
`POST .../modules/routing.graph/actions/component-job.replay` 调用。事务依次验证票据、
原操作回放、正文删除 epoch、处理器 head 和 Job 行锁，并要求：

- Job 当前为 `DEAD`，管理修订与请求一致；
- 处理器仍为 active；
- 原 Job payload 及其来源闭包仍可用。

成功后保留 `job_id`、顺序、执行主体、来源和原调度 operation，增加 `replay_epoch` 与
管理修订，清零本轮 attempt/dispatch，清除旧 claim、结果和错误，并进入立即可认领的
`RETRY_WAIT`。新的调用 operation 包含 replay epoch，因此不会误取上一轮失败或完成的调用
账本。不可变 `bot_component_job_replays` 行保存操作者、operation 和回放前计数。并发使用
同一旧修订时只有一个事务成功，其他请求返回 `COMPONENT_JOB_REVISION_CONFLICT`。

## 4. 处理器退役

退役采用 preview/apply 两步协议：

1. `component-job.retirement.plan` action 锁定 head，核对管理修订，统计各状态数量，
   保存 impact digest、操作者、有效期和不可变计划回执；
2. `component-job.retirement.apply` action 必须提交同一计划的 ID、digest 和修订，
   再次锁定 head 并重算影响；过期、操作者不同、状态变化或修订变化都拒绝提交。

存在 `INFLIGHT` Job 时 apply 返回 `COMPONENT_JOB_HANDLER_BUSY`，不会抢断 Worker。提交成功
会原子地把 head 设为 inactive，并把该处理器所有 `PENDING` / `RETRY_WAIT` Job 转成
`RETIRED`，错误码固定为 `JOB_HANDLER_RETIRED`。审计行记录终结数量，apply 回执把 plan
回执正文列为父来源。

inactive head 立即参与三道围栏：旧 generation 不能调度新 Job，dispatcher 不能认领，
DEAD Job 不能回放。退役前已成功调度的相同 operation 与相同输入仍可返回原 receipt，
用于处理调用响应丢失；它不会创建新 Job。之后部署可以删除 handler 声明，但不能以相同
稳定身份重新启用，避免历史退役决定被普通热重载绕过。

## 5. 合流与锁顺序

新调度先做无锁幂等查询；若需要创建，按 payload deletion clock、component job clock、
head/Job 的顺序进入串行区，并在 job clock 后再次查询 operation。这使并发相同调度只产生
一个事实，同时保持与认领、回放和退役一致的锁顺序。

退役计划只冻结管理相关的状态计数。运行时从 pending 到 inflight、retry 或 dead 会改变
impact digest，apply 因而要求重新预览；单纯 lease 时间流逝不会制造虚假管理冲突。退役
提交和调度都经过 job clock：先提交的新任务会使旧计划失效，先提交的退役会使新调度在
active head 检查处失败。

## 6. 验证范围

真实 PostgreSQL、Worker、HTTP 与 Edge 冒烟覆盖：死信形成和新 epoch 回放、并发 CAS、
幂等管理回执、正文不泄露、INFLIGHT 拒绝退役、待处理 Job 原子终结、旧代即时调度围栏、
精确调度重试、退役后部署移除、相同 handler 禁止复活、最小权限授予、Bot 切换的晚到读取
围栏和移动端布局。

群聊 Shell 从当前 RouteGraph 注册的 management operation 复用同一 read/replay/retirement 状态机。命令域固定为
`component-job`，避免与注入群会话唯一 Agent 的 `job` 计划混淆；群回执只含处理器和
任务的受限元数据。命令作者在每个目标事务和 CONTROL 回复前都要通过当前 Binding 管理员
名单、binding epoch、host lease 与命令 claim 复核。完整语法见
[群聊 Shell 设计](09-group-shell-command-system.md)。
