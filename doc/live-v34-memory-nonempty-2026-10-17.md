# v34 隔离候选：非空双主干记忆与召回（2026-10-17）

本次仅在 `forest-accept-v34` 的假 OneBot/假嵌入端点进行；树莓派 QQ 与现役 v33 均未接入候选 Bot。经 Bot 作用域的正式配置计划与应用，把 `embedding.multimodal` 的 endpoint 改为候选容器本机 `127.0.0.1:3003`，**保持冻结的 Qwen3-VL 模型 profile 不变**。把 `conversation.scheduler` 的空闲上下文窗口临时设为高水位 2 单元、保留 1 单元，两项配置各升到修订 2 并完成热重建。假嵌入端点返回维度正确、内容固定的测试向量及 0 token 用量；因此本次只验证流程和归因，不验证语义质量。

假 OneBot 向 `bot/accept-v34-offline-20261017` 顺序注入两个群各 3 条无触发词的文本消息：群 `<GROUP_ID>` 的作者为 `qq:20001`，另一个**仅为假事件**的群 `<GROUP_ID_2>` 的作者为 `qq:20002`。第二群未列入 Bot 的触发群白名单，但其消息仍正常进入记忆源。Memory 在同一 Bot 库中建立两个不同的群主干；空闲裁剪使每群前 2 条成为 ELIGIBLE，末条保持 REGISTERED。4 个 DOCUMENT facet 均为 READY 且各有向量索引引用。

正式 `memory.trial.start` 管理试验用第一群文本得到 READY，并通过 `memory.trial.result` 读取 2 条非空结果，均标明 `<GROUP_ID>`、`qq:20001` 和该群主干。第二群文本的试验同样 READY，返回 4 条结果，覆盖两个主干；每条的群号、作者和主干归因与原事件对应。固定测试向量让两群都成为种子，**不代表真实模型能做语义区分或已产生跨主干联想边**。正式 `memory.trunks.list` 返回两个主干；`memory.sources.query` 按群号筛选各返回 2 条 `COMPLETE` 归档单元，按 `REGISTERED` 状态还能分别筛出末条短期单元。

Memory 成本账本显示 4 次嵌入请求已结算、预留与 UNKNOWN 均为 0，charged tokens 为 0（假端点报告的用量）；另有 2 个 QUERY facet READY。第二个 Bot 的**独立物理库**保持 0 个源、单元、facet 和成本日账本。原 Bot 经正式意图退役、重新 ONLINE 重新物化 Bot Host 后，图 r10 ACTIVE、两条主干、筛选结果和两项配置修订均保留；最后再次退役至 OFFLINE，假端点已停止。两库最终均无 ACTIVE 图，假端点收到的发送动作数为 0，v33 Supervisor 保持 healthy。

这次证明最新镜像的生产 Supervisor 中，空闲裁剪、facet 嵌入、向量引用、Bot 内多主干召回归因、Bot 间内容隔离、管理筛选及 Bot Host 重新物化可以贯通。真实 VL 嵌入质量、图片召回、联想边扩散、真实 QQ 回复和长期成本仍需各自验收。
