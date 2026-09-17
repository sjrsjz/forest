# P0 持久组件事件

> **历史快照｜禁止作为当前实现规格。** 本文只记录旧纵向切片及其当时的局部验证。它服从 [00 架构总契约](00-architecture-contract.md) 与 01–14 号详细设计；冲突内容已经失效。不得据此新增或恢复旧表、编号迁移、兼容读取、固定业务页面、专用宿主 API、巨型 Store、业务组合根或产品子 Agent。需要复用算法时，必须先把 owner、namespace、Repository contract、Generation、UnitOfWork participant、并发 fence 和 management contribution 映射到当前设计。

历史快照当时状态：本页定义 P0 组件事件的实现边界。它用于插件间的已提交事实通知；同步请求仍使用 `ctx.services.call`，定时与长工作流仍属于 Job。

## 1. 声明与身份

发布方在 manifest 的 `components.publishes` 中声明 `topic`、精确 `major.minor` 契约和包内 JSON Schema，并请求 `event.publish:<topic>` 能力。宿主只授予 manifest 已声明且管理员批准的能力。插件只能在活跃调用 Context 内调用：

```python
await ctx.events.publish(
    "profile.changed",
    {"user_id": "42", "revision": 3},
    contract="1.0",
    operation_id=ctx.operations.stable("profile.changed:42:3"),
)
```

订阅方通过 manifest `components.subscribes` 声明稳定 `subscriber_id`、topic、精确契约、输入 schema 和最大尝试次数，并在代码上使用 `@subscribe(subscriber_id, topic, contract)`。代码声明与不可变 manifest 必须完全一致。`subscriber_id + contract_major` 是 Bot 数据库内的消费身份；Generation、进程 PID 和 Worker incarnation 都不能进入这个身份。

同一稳定订阅在热重载时可以升级实现，但不能换组件、topic、精确契约或 schema。首版尚未提供可声明的 minor 兼容范围，因此也拒绝暗中改变 minor。移除已有 durable subscriber 必须经过[显式退役](p0-component-event-management.md)，不能借一次普通热重载丢掉积压；退役身份也不能由部署隐式复活。

## 2. 发布事务与内容

`ctx.events.publish` 经过认证 IPC 回到宿主。宿主核验当前调用、当前 policy、调用方 Generation、manifest 声明、实际 grant、契约和 payload schema，然后在一个 PostgreSQL 事务内写入：

- 不可变 `component event` 事实；
- 由 `publisher_component_id + operation_id` 唯一约束的幂等发布身份；
- 内容寻址、可删除的 payload body 及来源依赖；
- 当时所有匹配 durable subscription 的独立 delivery 行。

payload 正文不复制到事件和 delivery 表。事件 envelope 明确携带 `event_id`、topic、契约、发布组件、发布时间和 payload；Bot 身份由专用数据库与宿主 Context 固定，不允许 Worker 选择。

同一发布 operation 重放且输入完全相同时返回原 `event_id`，输入变化返回 `IDEMPOTENCY_CONFLICT`。每个 Bot 的事务事件时钟同时串行化订阅 head 发布和事件 fan-out，`event_seq` 因而表示可见的提交顺序，不使用可能倒退或在提交前取得的墙钟时间排序。事件是已经发生的事实；没有订阅、某个消费者失败或稍后出现新订阅都不会回滚或重新解释该事实。首版新订阅从其发布成功之后的事件开始，不自动回放旧历史。

## 3. 投递、热重载和合流

Kernel dispatcher 以当前 Host lease 短事务 claim 到期 delivery，执行期间不持有数据库事务。claim 同时绑定 token、Host incarnation 和 owner epoch，后续延后、发送授权、ACK 与失败提交都复核同一身份；宿主崩溃或接管后旧 claim 不能提交。新宿主只领取已经恢复到自身 incarnation/epoch 的当前 Generation。本地 wake 只降低延迟，定期扫描保证通知丢失和重启后仍能恢复。

一次 delivery 使用稳定的 attempt operation。ACK 事务要求该 operation 在持久调用账本中已经完成，且 result payload 引用与调用回执完全一致。调用已经完成但 ACK 丢失时，内核从账本取得原结果并只补 ACK，不再次执行 handler。handler 明确失败或超时后，delivery 进入带上限的指数退避；下一次 handler attempt 使用新调用 operation，插件因而必须按 envelope `event_id` 做业务幂等。发送前遇到换代、容量或 admission 围栏时会更新独立的 dispatch 序号而不消耗 handler attempt；若尚未建立调用账本，连原 dispatch operation 也可以安全复用。插件可把已处理 event ID 与相关 durable state 放在同一 CAS 值中提交；跨外部存储必须使用该系统的幂等接口或操作账本。

热重载发布事务同时把稳定订阅 head 指向候选 Generation。已经获准执行的旧代调用参与正常 drain；尚未执行的 delivery 由新 head 接收。claim 后发生路由切换会被调用 admission 的 generation/route fence 拒绝并重试，不会把新授权送给旧 Worker。

不同订阅各自 claim、确认和重试，一个失败不阻塞其他订阅。首版同一 `subscriber_id + major` 同时最多一个 INFLIGHT delivery，从而维持该订阅的提交顺序；不同订阅和不同 Bot 可以并行。事件调用与普通数据调用共享 Kernel 总容量，dispatcher 另有八条并发上限；若 claim 后容量被同步调用占满，delivery 回到 `RETRY_WAIT` 且撤销尚未执行的 attempt 计数。优雅停机让在途 claim 立即过期，接管者沿用该 attempt 的持久调用 operation 对账。

## 4. 失败与死信

delivery 状态为：

```text
PENDING / RETRY_WAIT -> INFLIGHT -> ACKED
                              `-> RETRY_WAIT -> ... -> DEAD
```

claim 租约过期的 INFLIGHT 会回到可领取集合。失败记录只保存稳定错误码和 attempt 计数，不保存任意异常正文。达到 manifest `max_attempts` 后进入 DEAD，保留事件、payload 身份和最后错误码。[显式 replay 管理操作与 WebUI](p0-component-event-management.md)为新一轮尝试分配 replay epoch；重启或重装仍不能暗中清零 attempt。

payload 已按来源遗忘、Bot 暂停、订阅组件无当前可用路由或 Kernel 没有 Host 权威时都不会把正文交给 Worker。内容撤销属于不可自动重试的 DEAD；暂停和换代属于可恢复等待。

## 5. 首版验收

- manifest 与 `@subscribe` 声明不一致时，候选在发布前失败；
- 无 grant、未声明 topic、错误契约或 schema 不匹配均不能产生事件事实；
- 发布 ACK 丢失的重放只产生一个 event 和每订阅一条 delivery；
- 两个订阅独立确认，其中一个失败不会阻塞另一个；
- handler 完成后 ACK 丢失只补确认，不重复执行；
- 失败按有界退避重试并最终 DEAD，错误正文不进入账本；
- 热重载延续稳定订阅积压，发送前路由竞争不消耗 handler attempt，旧代已获准调用完成后才退出，新代不会重放全部历史；
- Kernel 重启和 claim 租约过期后可恢复未完成 delivery；
- 容量竞争不会消耗未执行 attempt，关闭与宿主接管不能借旧 claim 提交；
- 真实 Worker 进程覆盖发布、投递、状态写入与资源清理路径。
