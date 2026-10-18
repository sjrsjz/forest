# v34 隔离候选：同群 Agent 排队与跨群异步（2026-10-17）

在独立 `forest-accept-v34` 中，通过正式模块配置计划与应用，把第一 Bot 的 Provider base URL 指向候选网络命名空间内的受控假模型，配置修订 2；把触发策略的许可群扩至 `<GROUP_ID>` 和仅存在于假端点的 `<GROUP_ID_2>`，配置修订 2。Bot 运行图 r22 为 ACTIVE、22/22 组件。嵌入端点仍返回固定假向量；没有连接真实 QQ 或聊天模型。

假模型收到第一群首条显式 `@` 后暂停第 1 个 HTTP 模型请求。暂停期间依次注入同群第二条显式 `@` 和第二群一条显式 `@`；三条入站均被 Conversation 结算为 `FINAL / TRIGGER / bot_mentioned`。模型端点最终记录的请求顺序为 `FIRSTBLOCK`、`OTHERGROUP`、`QUEUEDGROUP`。持久模型传输账本显示第一群第 1 步从 10:38:45 至 10:39:47 UTC 在途，第二群第 1 步于 10:39:16 UTC 已独立完成，第一群第 2 步直到 10:39:58 UTC 才发出并完成。因此同一群没有并发模型调用，另一群不等待第一群的模型释放。

第一群只有一个 Agent Run；其持久 `run_inputs` 从修订 1／入站序号 7 前进到修订 2／序号 8，两个 `model_requests` 分别冻结输入修订 1 和 2。假模型第三次请求包含后到消息标记 `QUEUEDGROUP`，触发 claim 集合计数 2、最后触发序号 8，最终在第二次模型步骤之后以同一最终回复结算为 `ANSWERED`。第二群有独立 Run、输入修订 1 与单触发 claim，亦为 `ANSWERED`。这符合“当前模型调用结束后的下一次模型调用接入排队消息”的边界；短暂只观察到前两次请求时不能提前判断队列被吞并。

假 OneBot 明确拒绝 `send_group_msg`，因此两个 Delivery intent 当前仍为 `QUEUED / PLATFORM_REJECTED`，**没有真实 QQ 出站**。第一 Bot 已通过正式意图退役为 OFFLINE，假端点已停止；这个含有待发送假回复和固定向量的 Bot 只用于隔离验收，不能改绑真实 QQ。现役 v33 未更换工件或配置。此试验证明最新候选 Supervisor 的 Agent 调度时序及来源 InputBatch 合流，不证明真实 QQ 回复、真实模型质量、长期并发负载或遗留待发意图的线上处置。

随后查明该隔离候选上的两条拒绝意图分别累计 130、54 次发送尝试：OneBot 已明确返回失败，但 Adapter 将其归为可重试的 `NOT_SENT`，Delivery 又立即重新入队。源码现已将明确的 HTTP／OneBot 拒绝归为 `REJECTED`，经 Delivery 既有的 `REJECTED_NOT_APPLIED` 语义将效果记为确定未应用、业务 part 和 intent 终结为 `FAILED`；未派发的 `NOT_SENT` 仍可重试，结果不明的 `UNKNOWN` 仍等待核对。隔离 PostgreSQL 集成测试验证终结后不能再次 claim。这个修复尚未安装到运行中的 v34/v33，v34 原有两个待发意图仍在旧库中；禁止将其重新绑定真实 QQ。
