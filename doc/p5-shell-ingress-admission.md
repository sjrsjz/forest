# P5 群聊 Shell 接入容量与资格缓存

状态：**首版已实现。** 本文处理 webhook 签名校验以后、消息写入 Bot Inbox 以前的容量隔离。Shell 的命令语法、持久优先级、执行前鉴权和结果发送继续由 [群聊 Shell 详细设计](09-group-shell-command-system.md)、[群管理协议](p3-control.md)与各管理权威定义。

## 1. 问题与不变量

BotStore 当前最多持有 8 条数据库连接。若普通消息和明显无权的 `#forest` 请求可以无界并发进入原始正文、Adapter Worker 与 Inbox 事务，它们会先占满连接，再让真正的 P0 cancel/pause/disable 排队。执行调度器保留 P0 槽不能修复这个更早的入口拥塞。

接入隔离必须同时保持这些不变量：

- webhook 签名仍先验证；正文、昵称、群角色或模型输出不能产生资格；
- 宿主内置 OneBot 解析器只做有界预分类，不运行插件、不联网；
- Forest 已返回成功 ACK 的每条群消息都写入 Inbox、保留作者与来源，并按 `ingress_seq` 进入 Context；
- 容量不足发生在 Inbox 提交前时返回明确 429，不伪造成功 ACK，也不生成虚假的 `event_id`、`command_id` 或 Context 记录；平台重试仍由平台消息 ID 在正常接纳后幂等合流；
- 资格缓存只选择入口通道和是否保留 P0，不能授予管理 action。ShellDispatcher 在执行前仍读取当前 Binding 配置并重新鉴权；
- Binding 换代、配置生效修订变化、Bot 接管或进程重启自然使旧缓存失配，不能把过期管理员资格带入新连接；
- 日志和 Web 状态只暴露聚合计数，不暴露被限速主体或命令正文。

## 2. 预分类与资格证明

签名验证后，宿主以固定 OneBot 解析器生成与后续 `stage_platform_input` 相同的规范消息，再用当前冻结的 Shell registry 判断是否命中保留命名空间。非消息事件和普通消息不做 Shell 资格查询。命中保留命名空间的消息直接沿用两次宿主解析完全相等的投影，不调用可热替换 Adapter Worker；因此 Adapter 损坏时 Core 救援命令仍可进入持久 Shell 队列。普通消息继续要求 Worker 投影与宿主投影完全一致。

每个在线 Connection 已绑定：

```text
(bot_id, binding_id, binding_epoch, effective_config_revision,
 config_payload_digest, effective config)
```

资格缓存键固定为：

```text
(binding_id, binding_epoch, effective_config_revision,
 principal_bucket, command_name, requested_priority)
```

值只包含 `is_current_admin` 和按当前策略算出的实际 priority，默认 TTL 5 秒、最多 512 项，按最近使用淘汰。`principal_bucket` 对验证账号使用稳定 `principal_ref`；匿名主体按 Binding 与群形成共享桶，不能通过省略匿名 ID 为每条消息创建新额度。

缓存命中产生宿主内部 `ShellPriorityProof`：绑定 Bot/Binding 修订、命令名、gate、声明 priority 与作者 principal。MessageStore 在 Inbox 事务中对当前 Binding 行、作者和重新解析出的命令逐字段复核；不一致返回 `SHELL_PRIORITY_PROOF_INVALID`。没有宿主证明的直接 MessageStore 调用保留原数据库读取路径，用于恢复和兼容测试。

该证明不是可序列化票据，不进入 Worker、HTTP、群输出或数据库。Python 插件拿不到 MessageStore 或 guard 对象；即使宿主进程内对象被错误复用，修订与作者复核也会拒绝跨 Binding/跨主体重定向。

## 3. 三条入口通道

首版每 Bot 运行时使用以下默认值：

| 通道 | 内容 | 并发上限 | 等待上限 |
|---|---|---:|---:|
| `ORDINARY` | 普通消息、合格管理员的 P1/P2 Shell | 4 | 10 秒 |
| `PRIVILEGED_CONTROL` | 当前管理员的声明 P0 Shell | 2 | 2 秒 |
| `REJECTION` | 明显非管理员提交的任意 Shell 命令 | 1 | 50 毫秒 |

三条 semaphore 在取得数据库连接、写原始正文和调用 Adapter Worker 以前领取，并覆盖直到 Inbox 提交。普通入口最多占用 4 条并发链，拒绝入口最多占用 1 条，因而在 BotStore 的 8 条连接中为紧急控制与其它内核工作留下余量。资格未知时按拒绝通道处理，不能乐观进入 `PRIVILEGED_CONTROL`。

通道互不借位。普通入口满载时，合格 P0 仍可进入自己的两条通道；P0 自身超过有界等待返回 `CONTROL_INGRESS_OVERLOADED`。普通消息等待 10 秒后返回 `INGRESS_OVERLOADED`，不会静默丢弃。拒绝入口忙时立即返回 `SHELL_REJECTION_OVERLOADED`。

这不是数据库锁优先级，也不能在数据库整体失效时保证救援；它隔离单个 Bot 进程主动发起的 webhook 工作量。多 Host 场景仍由唯一 Host lease 与 Binding owner 决定接收者，后续可把同一配额协议提升为共享网关服务。

## 4. 拒绝令牌桶

非管理员 Shell 请求在进入 `REJECTION` 通道前还要通过进程内令牌桶。默认每个 principal bucket 可突发 4 条，之后每 5 秒恢复 1 条；最多保存 2048 个桶，最近最少使用项被淘汰。淘汰或进程重启会使限制短暂变宽，因此令牌桶只能保护可用性，不能承担安全授权或审计完整性。

通过令牌桶的请求仍按普通消息完整持久化，Shell request 持久 priority 至少为 P1，Dispatcher 最终写入 `DENIED` CONTROL 回执。桶耗尽的请求在写任何 raw payload、Inbox 或 shell request 前返回 `SHELL_REJECTION_OVERLOADED`；调用方没有收到成功 ACK，应依平台退避后重试。相同平台消息重试一旦被接纳，仍由原 Inbox 幂等键合流。

不对普通聊天或已经确认的管理员 P1/P2 使用拒绝桶。被攻陷的真实管理员仍可能提交大量 P0；其治理需要管理员主体速率策略和审计告警，不能把合法资格猜测为攻击而静默降级。

## 5. 可观察性与生命周期

Guard 随 BindingRuntime 创建和销毁，不跨 Bot 共享身份。运行时状态提供以下无主体聚合计数：

- 三通道当前占用、累计接纳和累计 overload；
- 资格缓存 hit/miss、当前条目数；
- 拒绝桶限速总数和当前桶数。

计数不是持久审计，重启归零。已进入 Inbox 的拒绝请求仍由既有消息、Shell、CONTROL 回执和 Bot audit 事实承担持久可追溯性。Binding 换代无需主动扫描缓存；修订键失配后旧项不会再命中，LRU/TTL 负责有界回收。Runtime close 不取消已经完成的 Inbox 提交，只取消或等待当前受监督 receive task，沿用现有 Binding 关闭协议。

## 6. 验证矩阵

实现至少覆盖：

- 普通 4 槽满载时，合格 P0 仍能取得独立通道；
- 拒绝通道忙或令牌耗尽时在 stage/raw payload/Inbox 前返回 429；
- 允许进入的无权命令仍形成带作者的 `control_data`、P1 request 和 `DENIED` 回执；
- 同一 Binding/config/principal 命中缓存，Binding epoch 或 effective revision 变化后必定 miss；
- 匿名事件不能通过轮换 event-scoped principal 绕过共享桶；
- proof 不能跨 Bot、Binding、作者、命令、gate 或 revision 重放；
- 没有 proof 的既有 MessageStore 测试继续从数据库计算 priority；
- Web runtime 状态只有聚合计数；关闭后没有 semaphore owner 或 receive task 泄漏；
- 真实 PostgreSQL 并发测试证明普通入口饱和不阻塞 P0 的应用层接纳，fake Adapter 阻塞和失败不会泄漏槽；
- Docker + 浏览器流程仍能提交真实 P0 Core 命令并读取新的入口统计。
