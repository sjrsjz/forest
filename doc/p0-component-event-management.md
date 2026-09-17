# P0 组件事件管理

> **历史快照｜禁止作为当前实现规格。** 本文只记录旧纵向切片及其当时的局部验证。它服从 [00 架构总契约](00-architecture-contract.md) 与 01–14 号详细设计；冲突内容已经失效。不得据此新增或恢复旧表、编号迁移、兼容读取、固定业务页面、专用宿主 API、巨型 Store、业务组合根或产品子 Agent。需要复用算法时，必须先把 owner、namespace、Repository contract、Generation、UnitOfWork participant、并发 fence 和 management contribution 映射到当前设计。

历史快照当时状态：本页定义 durable component event 的只读观察、死信回放和订阅退役协议。管理入口属于 Bot 控制面；它不会把事件处理器解释为 Agent，也不会产生任何产品子 Agent。

## 1. 管理对象与并发版本

管理接口公开两类资源：

- `SubscriptionHead`：由 `subscriber_id + contract_major` 唯一标识。`management_revision` 只在显式管理变更时递增，运行时热重载更新 Generation 不改变它；`active=false` 表示永久退役的旧消费身份。
- `Delivery`：由 `delivery_id` 唯一标识。`management_revision` 只在显式回放时递增；claim、ACK、重试和自动 DEAD 不改变它。页面刷新不会因为短暂租约变化产生无意义的 CAS 冲突。

列表最多返回 200 条订阅和 200 条投递，支持状态、topic、subscriber 过滤。公开视图只包含身份、契约、来源组件、状态、计数、稳定错误码与时间；事件正文及处理结果正文不在此接口返回。正文可能已经被来源遗忘，管理观察不能绕过内容撤销。

## 2. DEAD 回放

只有当前状态为 `DEAD`、所属订阅仍 active 的 delivery 可以回放。请求必须携带读取到的 `management_revision`，并使用稳定管理 `operation_id`；同一操作重试返回原回执，换输入则返回幂等冲突。

回放在一个事务中：

1. 锁定 subscription head，再锁定 delivery；
2. 复核订阅 active、delivery 仍为 DEAD、CAS 版本一致；
3. 写入不可变 replay audit，保存回放前的 attempt、dispatch 与错误码；
4. `replay_epoch += 1`，清零 attempt/dispatch，清除 claim、结果和错误字段，将状态置为 `RETRY_WAIT`；
5. `management_revision += 1`，提交管理回执，再唤醒 dispatcher。

每次 dispatch 的调用 operation 包含 `delivery_id + replay_epoch + dispatch_count`。因此回放后的第一次调用绝不会碰撞回放前已经完成或失败的 `bot_call`；同一 replay epoch 内丢 ACK 仍沿用原 operation 对账。回放不改变 `event_id`、`event_seq`、delivery 身份或原 payload 摘要，插件继续以 `event_id` 保证业务幂等。

每个 INFLIGHT delivery 从首次创建即持久化准确 operation；租约恢复先完成原调用对账，不按当前代码重新计算身份。只有下一次新 attempt 或显式 replay 才使用递增的 replay epoch。

若正文已被撤销，回放请求返回 `EVENT_PAYLOAD_UNAVAILABLE`，不会制造必然再次 DEAD 的工作。回放比同订阅中更晚的未完成 delivery 具有原 `event_seq` 顺序；它重新成为 open delivery 后会阻挡后续领取，直到 ACKED 或再次 DEAD。

## 3. 订阅退役

普通安装、watch 或热重载不能删除或复活 stable subscriber。退役必须先 plan 再 apply：

- plan 读取 `management_revision`，给出将转为 `RETIRED` 的 `PENDING/RETRY_WAIT` 数量、已 ACKED/DEAD 数量和 INFLIGHT 阻塞数；计划绑定 Bot、操作者、subscriber identity、版本、影响快照摘要并在十分钟后过期；
- apply 重新锁定事件时钟、subscription head 与相关 delivery，核对计划、操作者、版本和当前影响；存在 INFLIGHT 时返回 `EVENT_SUBSCRIPTION_BUSY`，管理员等待该次 handler 得到确定 ACK/失败后重新预览；
- apply 将 head 置为 inactive、递增管理版本，并把所有尚未领取的 `PENDING/RETRY_WAIT` 置为终态 `RETIRED`，写入稳定原因 `SUBSCRIPTION_RETIRED`；ACKED 与 DEAD 历史保持原状态。

事件发布和退役都锁定同一个 Bot event clock。若发布先取得时钟，本次 fan-out 先落库，退役随后会终结其待投递项；若退役先取得时钟，后续发布看不到该 active subscriber。claim 同时锁 subscription head 与 delivery，因而不能在退役事务越过 active 边界。退役不抢断已经进入 handler 的外部副作用；通过拒绝 INFLIGHT apply 避免把正在执行的结果谎报成已取消。

退役后，新 Generation 可以省略该声明。再次出现同一 `subscriber_id + major` 会返回 `SUBSCRIPTION_REACTIVATION_REQUIRES_MANAGEMENT`；首版不提供复活，重新订阅必须使用新的 stable subscriber identity。这使旧积压和新语义不能被一次普通部署混合。

## 4. 状态与顺序

退役后的完整状态机为：

```text
PENDING / RETRY_WAIT -> INFLIGHT -> ACKED
          |                  `-> RETRY_WAIT -> ... -> DEAD
          `-> RETIRED
DEAD -- explicit replay --> RETRY_WAIT
```

`ACKED`、`DEAD` 和 `RETIRED` 都是顺序屏障的终态。退役记录保留 event/delivery identity 和摘要，但不再可 claim 或 replay。管理 read 与 replay/retire 事务都在目标 Bot 数据库中执行，部署数据库只负责登录、目录和短时签名票据。

## 5. 权限与入口

控制动作分离为：

- `event.read`：读取订阅与投递元数据；
- `event.replay`：显式回放一个 DEAD delivery；
- `event.retire.plan`：预览一个订阅的退役影响；
- `event.retire.apply`：按计划提交退役。

Event 模块安装时各管理能力分别显式授予：`event.read`、`event.replay`、`event.retire.plan` 和 `event.retire.apply` 不能由通用读写权限自动推导。只读身份不会获得 replay 或 retirement 写能力。

WebUI 在 Event 模块工作区显示订阅和最近投递。DEAD 行只在服务端返回 `replay_allowed=true` 时显示回放按钮；active subscription 提供退役预览，apply 按钮只提交当前 plan。Bot 切换、内容撤销或并发刷新会清空草稿和 idempotency key。群聊 Shell 从当前 RouteGraph 注册的 `event` 命令复用相同 management contract：提供受限列表、DEAD 回放和订阅 plan/apply 退役，不另写直达数据库的旁路。每次目标事务和 CONTROL 回复都重新检查当前 Binding 管理员、binding epoch、host lease 与命令 claim。

## 6. 验收

- 管理读取不返回事件正文、结果正文或私有异常文本；
- 同一 replay operation 重试不产生第二个 epoch，换输入冲突；
- replay epoch 进入 handler 调用 identity，旧调用账本不能短路新一次回放；
- 两个并发 replay 只有一个通过 management revision；
- 退役与并发发布无遗漏窗口，退役后不再生成 delivery；
- INFLIGHT 阻止退役 apply，ACK/失败确定后重新 plan 可以提交；
- 退役后普通热重载允许省略旧声明，但拒绝同身份隐式复活；
- 干净安装的最小权限不扩大只读或无权 issuer；
- Web 会话、CSRF、幂等键、plan 绑定与 Bot 隔离均经过真实 PostgreSQL 管理链路验证。
- Shell 冻结 RouteGraph/Generation/schema、当前管理员复核、受限群回执和同状态机管理经过真实 PostgreSQL/Worker 验证。
