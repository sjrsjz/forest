# P5 已遗忘来源的迟到观察接纳

## 1. 问题与目标

同一真实平台事件可能因 OneBot 重连、HTTP 重试、多个 Binding 观察或延迟队列再次抵达。正常情况下，Forest 以稳定 `source_event_id` 合并 SourceUnit，并为每个 Binding/Conversation 观察登记独立 occurrence。若该 SourceUnit 已被遗忘，PayloadStore 会拒绝任何重新携带其正文的写入；旧实现因此返回 `SOURCE_SUPPRESSED`，没有形成可重放的持久接纳结果，平台可能持续重推。

本切片增加 `SUPPRESSED_OCCURRENCE` 收据。它证明 Forest 已认证并观察到这次迟到投递，也证明该观察因既有 suppression 没有进入 Inbox、Context、Shell、Agent、媒体捕获或记忆索引。收据只保存稳定身份、哈希、首个观察位置和删除依据，不保存作者、消息段、图片 URI、原始 wire bytes、Shell 参数或回复候选。

这不是撤销遗忘、恢复来源或新建 Agent。正常消息仍全部进入原群队列；只有稳定 SourceUnit 已存在永久 suppression 的迟到观察走此闭环。

## 2. 稳定身份与收据

宿主使用已经认证的 Binding 与固定 OneBot 解析器计算：

```text
conversation_id = H(bot_id, binding_id, platform, channel_kind, channel_id, thread_id)
event_id        = H(conversation_id, platform_message_id)
source_event_id = H(group_source_id, self_id, platform_message_id)
source_unit_id  = H(bot_id, source_event_id)
occurrence_id   = H(bot_id, INPUT, event_id)
receipt_id      = H(bot_id, SUPPRESSED_OCCURRENCE, occurrence_id)
input_digest    = H(canonical normalized event)
```

`bot_suppressed_input_occurrences` 保存：Bot、收据/事件/来源/occurrence 身份，首个 Binding epoch 与 Conversation 定位，平台及群/私聊地址，平台消息 ID，规范输入摘要、消息发生时间，以及命中的 `suppression_operation_id/deletion_epoch`。表中不得出现作者身份、显示名、正文、parts、URI 或通用 JSON 扩展列。记录写入后不可更新或删除。

同一 `event_id` 的重试只有在 `input_digest`、来源及位置完全相同时返回原收据。相同平台身份携带不同规范内容固定返回 `INGEST_CONFLICT`，不能覆盖首条收据，也不能把冲突内容写入调试日志。Binding epoch 变化只表示同一观察被重投，不改变已经提交的收据。

## 3. 接纳顺序与并发线性化

已认证传输帧按以下顺序处理：

1. 使用宿主固定解析器得到规范事件，并核对 Binding 的账号与平台；
2. 计算稳定身份，先查询同 `event_id` 的普通 Inbox 事实；若存在且摘要相同，返回它的既有元数据 ACK，必要时继续做精确自身回声对账，不再创建 raw staging payload；
3. 锁定 Bot `memory_clock`；
4. 查询 `source_unit_id` 的任意 `*` 或 part suppression；
5. 命中时插入或读取不可变 `SUPPRESSED_OCCURRENCE` 收据，复核当前 Host/Binding 后提交；
6. 未命中时才创建受管 raw staging payload，并继续既有 Worker 投影与 Inbox 接纳。

步骤 3 与遗忘事务使用同一行锁。若接纳先取得锁，正文只能在旧 deletion epoch 内提交，随后遗忘会撤销它；若遗忘先取得锁，接纳只能提交 metadata-only 收据。不存在 suppression 已生效后又写入新正文的窗口。

raw staging 与正式 Inbox 是两个事务，因此第二次检查不可省略：来源可能在 staging 提交后、Inbox 接纳前被遗忘。正式接纳事务取得同一 `memory_clock` 锁后再次检查；命中时提交相同收据并返回，不创建 Inbox 行或推进 Conversation ingress 水位。此前的 staging payload已经被遗忘事务撤销，按既有 scrub 流程清理。

正式接纳沿用全系统的 `Binding → Conversation → memory_clock` 锁序。新 Conversation 在事务内先作为候选行创建并锁定；若第二次 suppression 检查命中，本事务在提交收据前删除该候选行。未提交状态不会对其他事务可见，提交结果中没有空 Conversation，同时不会把锁序倒置成 `memory_clock → Conversation` 而与 Actor 形成死锁环。

## 4. 运行时结果

平台入口对已提交收据返回成功：

```json
{
  "accepted": true,
  "kind": "SUPPRESSED_OCCURRENCE",
  "receipt_id": "...",
  "duplicate": false
}
```

同一观察重放返回同一 `receipt_id` 且 `duplicate=true`。它不调用可热重载 Adapter Worker，不执行 Shell，即使被删除正文原先看起来像 `#forest` 命令；不通知群 Actor，也不创建 trigger。页面或审计若以后展示该记录，只能显示表内的封闭元数据。

普通 Inbox 已存在时返回 `DUPLICATE`，继续复用原 `event_id/ingress_seq/trigger_decision`。这条路径不读取已遗忘正文，因此同一消息在遗忘后的网络重试仍能取得稳定 ACK。

## 5. 升级与恢复

迁移 070 新建封闭收据表，并把 `forest.writer_generation` 提升到 10。升级要求旧 Host 停止；迁移提交后 generation 9 进程不能重新取得或续签 HostLease，避免同一 schema 上一部分进程持久 ACK、另一部分进程仍返回瞬时拒绝。

备份和隔离恢复按普通 Bot 表保存这些收据及其 suppression 外键。收据不参与 Context、图、索引或 payload scrub，也不阻止正常存储代际清理。恢复开放前的既有 schema/行数/外键核对会覆盖该表。

## 6. 验收

- 遗忘先提交、迟到观察后到达：只增加一条收据，没有 Inbox、Context、媒体、Shell、SourceUnit 或 payload body；
- staging 后遗忘、正式接纳后到达：正式接纳收敛为同一 metadata-only 结果，旧 staging 正文保持撤销；
- 并发相同迟到观察：全部返回同一收据，恰有一个 `duplicate=false`；
- 同一事件身份换正文：返回 `INGEST_CONFLICT`，原收据不变；
- 普通 Inbox 在遗忘后重投：返回原 Inbox 元数据 ACK，不读取或恢复正文；
- HTTP POST 与正向 WebSocket 入口返回相同结果，均不触发 Agent 或发送；
- generation 9 Host 在迁移 070 后不能重新取得租约；旧 001–069 非空数据库可升级且原事实不变。
