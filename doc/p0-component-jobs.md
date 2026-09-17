# P0 组件持久任务详细设计

> **历史快照｜禁止作为当前实现规格。** 本文只记录旧纵向切片及其当时的局部验证。它服从 [00 架构总契约](00-architecture-contract.md) 与 01–14 号详细设计；冲突内容已经失效。不得据此新增或恢复旧表、编号迁移、兼容读取、固定业务页面、专用宿主 API、巨型 Store、业务组合根或产品子 Agent。需要复用算法时，必须先把 owner、namespace、Repository contract、Generation、UnitOfWork participant、并发 fence 和 management contribution 映射到当前设计。

历史快照当时状态：首版已实现，包含显式死信回放、处理器退役和 WebUI。本文中的“组件任务”是插件请求内核稍后执行的持久工作，和
`p3-jobs.md` 中向某个群会话的唯一 Agent 注入提示词的“会话定时任务”是两个独立
领域。组件任务不会创建 Agent、不会获得群上下文，也不会直接形成群聊回复。

## 1. 目标与边界

组件可以在一次已准入的数据调用中用 `ctx.jobs.schedule(...)` 提交有界 JSON 输入。
提交和调用账本处于同一个 Bot 数据库中；调用返回后、Worker 重启后以及兼容热重载
后，内核仍会把任务交给该组件当前发布的 generation。

P0 支持一次性立即或延迟执行。周期工作由处理器在完成当前周期前，以稳定的下一周期
`operation_id` 再次调度；这使每个周期都有独立事实、重试预算和审计记录，也避免在
manifest 中同时维护第二套 cron 语义。单次处理最长 120 秒；更长的工作必须持久化
检查点并调度后续步骤。

以下行为不属于组件任务：

- 群聊触发、群消息排队和 Agent 的下一次模型调用；
- P3 会话定时任务、主动对话以及模型成本预算；
- 在 `Plugin.start()` 中脱离调用上下文提交后台工作；
- 任意组件指定另一个组件为执行目标，或携带用户/管理员权限快照。

## 2. 声明与 SDK

组件在 immutable manifest 的 `jobs` 数组中声明处理器：

```toml
[[components.jobs]]
handler_id = "index.rebuild"
input_schema = "schemas/rebuild.json"
max_attempts = 8
timeout_s = 60
max_concurrency = 1
```

Python 实现必须有完全一致的装饰器：

```python
from forest.sdk import job

@job("index.rebuild")
async def rebuild(self, task, ctx):
    # task: job_id、scheduled_at、attempt、payload
    ...
```

`handler_id` 是同一 Bot 内、同一组件的稳定任务身份。`input_schema` 的摘要在该身份的
生命周期内不可变化；修改输入契约需要新身份。处理器一旦显式退役不能由普通部署重新
启用；恢复相同语义也必须使用新的 `handler_id`。`max_attempts`、`timeout_s` 和
`max_concurrency` 可以随 generation 调整，并在 publication 后用于新认领。省略时分别
是 8、30 秒和 1；上限分别是 32、120 秒和 16。

调度 API 为：

```python
receipt = await ctx.jobs.schedule(
    "index.rebuild",
    {"partition": 7},
    operation_id=ctx.operations.stable("rebuild:7"),
    delay_seconds=10,
)
```

`delay_seconds` 使用数据库时钟，范围为 0 到 30 天。调用只能调度本组件当前 generation
声明的处理器，并且 generation 必须请求且获授 `job.schedule:<handler_id>`。返回值只含
`job_id`、`handler_id`、`status`、`available_at`；不得暴露正文引用或 claim token。

## 3. 固定执行主体与权限

一次任务的执行主体在创建时固定为 `bot_id + component_id + handler_id`。来源 generation、
来源 call 和调度 operation 作为不可变溯源保存。任务不继承调用它的用户、群管理员或
Web 操作者权限，也不携带可延长的 bearer token。

执行时通过普通根 DATA call 准入当前路由；处理器只获得当前 generation 已发布的
组件 capability grants。这样撤销 capability 或暂停 Bot 数据准入会立即约束尚未执行的
任务。任务不能切换 Bot、组件或 handler。

预算由声明固定：单次 deadline、最大尝试数、每处理器并发数、Worker 帧大小以及内核
总调用容量共同约束执行。调度正文还受 IPC 256 KiB 上限约束。

## 4. 持久模型

`bot_generation_job_definitions` 保存 generation 的不可变声明。它参与 generation 注册的
幂等比较，旧 generation 不能在注册后增加声明。

`bot_component_job_heads` 保存稳定 handler 到当前 generation 的路由头：组件、输入
schema 摘要不可改变；publication 仅更新 generation 和可调预算。已有活动 handler 不能
通过普通部署消失，P0 会以 `JOB_HANDLER_REMOVAL_REQUIRES_MANAGEMENT` 拒绝。显式退役后
head 立即停止新调度与认领，普通部署才可移除声明。详细管理协议见
[组件持久任务管理](p0-component-job-management.md)。

`bot_component_job_clock` 为每个 Bot 分配严格递增 `job_seq`。`bot_component_jobs` 保存
不可变工作事实和可变执行状态：

- 不可变：任务身份、顺序、执行主体、来源、调度 operation/input digest、正文引用与
  摘要、首次可用时间、创建时的最大尝试数；
- 可变：`PENDING / INFLIGHT / RETRY_WAIT / ACKED / DEAD / RETIRED`、尝试/派发计数、
  回放 epoch、管理修订、当前 claim、当前调用 operation、结果正文引用、最后错误、完成
  和退役时间。

唯一键 `(bot_id, component_id, operation_id)` 提供调度幂等性。相同 operation 和相同
handler、payload、delay 返回原 receipt；任何差异返回 `IDEMPOTENCY_CONFLICT`。任务正文
写入 removable payload store，父引用包含调度 generation 的配置来源和来源 call 输入。

## 5. 认领、执行与重试

内核有独立 `_component_job_dispatcher`，但与服务、事件共享 `max_calls`。每次认领按
`available_at, job_seq` 选择工作，并在 SQL 中限制同一 handler 的 INFLIGHT 数不超过当前
head 的 `max_concurrency`。并发为 1 时即获得该 handler 的严格顺序；大于 1 时只保证认领
顺序，不保证完成顺序。

认领结果绑定 host incarnation 与 owner epoch、claim token 和到期时间、当前 target
generation 与 route epoch，以及当前 attempt operation。任务 handler 经内部服务名
`@job:<handler_id>` 进入已有调用账本。实际发送 IPC 前，内核再次验证 host、Bot
admission、claim、handler head、generation route 和 route epoch。Worker 只接受 manifest
已登记的 `invoke_job` 目标。

调用 operation 由 `job_id + replay_epoch + dispatch` 派生。新尝试先增加 attempt；如果容量竞争
发生在创建调用账本之前，deferral 回退这次尝试。Worker 已经可能执行后，claim 到期或 ACK
丢失时继续使用原 operation：

1. 调用账本已 COMPLETED：`_invoke` 返回同一结果 receipt，handler 不重跑；
2. 调用账本仍 ADMITTED：暂缓并等待账本收敛；
3. 调用明确失败：按稳定指数退避进入下一尝试；
4. 达到上限或正文已移除：进入 DEAD。

ACK 必须证明相同 operation、component 已有 COMPLETED call，且 result payload 相同。关闭
内核时取消本机 handler 并把 claim 立即过期；新 host 只能在 lease 到期后继续原 attempt。

## 6. 热重载与锁顺序

候选 Worker 在 prepare 阶段验证装饰器与 manifest 完全一致。publication 事务在切换组件
route 前发布 handler heads，所以随后认领只会看到一个原子的新路由。已经认领到旧
generation 的任务会在发送前被 route fence 拒绝，且在调用尚未建立时不消耗尝试；之后
重新认领并投递给新 generation。

涉及正文和 handler 路由的事务统一遵循：

1. 生命周期资源、component route、Bot runtime、host、来源 call（需要时）；
2. payload deletion clock；
3. component job clock；
4. handler head；
5. job row。

调度在既有调用守卫后先取得 payload clock再取得 job clock。publication 在既有生命周期
锁内、已持有 payload clock时才能更新 head；claim 在 runtime/host 守卫后同样先取得
payload clock再锁 job clock、head 和 job。任何事务都不得在持有数据库锁时调用 Worker
或外部 API。

## 7. 可观测性与管理面

内核审计记录 `component.job.scheduled` 和 `component.job.dead`，正文不进入审计 details。
SDK receipt、日志和 Web API 只显示有界元数据。Bot 后台的“组件持久任务”面板已经提供：

- 按 Bot、组件、handler、状态读取任务元数据；
- 仅对 DEAD 任务执行带 CAS 的重放；
- 处理器退休 preview/apply，并拒绝有 INFLIGHT 工作时 apply；
- 明确区分“组件持久任务”和“会话 Agent 定时任务”，避免产品语义混淆。

## 8. 必须验证的性质

- 并发相同 operation 只产生一个任务事实；不同正文稳定冲突；
- Worker 完成后 ACK 丢失不会再次运行 handler；
- 热重载把待处理任务路由到契约兼容的新 generation；
- 被 fence 的旧 generation 派发不会消耗 handler 尝试；
- `max_concurrency=1` 时同一 handler 不会并发；
- Bot maintenance、host lease、policy revision 和总调用容量均能阻止新派发；
- 正文删除使任务终止为 DEAD，且任何元数据接口都不泄露正文；
- prepare 时 manifest/装饰器不一致、隐式删除 handler、未获调度 capability 均被拒绝。
- 并发死信回放只有一个 CAS 胜者；相同管理幂等键返回原回执；
- 退役立即阻止旧 generation 提交新任务，已成功调度操作的精确重试仍返回原 receipt；
- WebUI 在切换 Bot 或正文失效后丢弃晚到读取，窄屏不产生横向溢出。
