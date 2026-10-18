# v34 隔离候选：单条记忆遗忘与物理清理（2026-10-17）

沿用 [v34 非空双主干记忆验收](live-v34-memory-nonempty-2026-10-17.md)中的独立 Bot 库、假 OneBot、假嵌入和真实 Milvus 索引。Bot 重新上线为 RuntimeGraph r12 ACTIVE、22 个组件；上一 Generation 的管理试验输入返回 `410 MEMORY_TRIAL_INPUT_UNAVAILABLE`，因此在当前 Generation 新建正式管理试验。新试验 READY，遗忘前可见 4 条证据，分别来自群 `<GROUP_ID>` 和仅存在于假端点的群 `<GROUP_ID_2>`，群号与作者均保留。

从正式 `memory.sources.query` 中选择群 `<GROUP_ID>` 的第一条已归档单元 `onebot-unit:5fc10adf0c1e872f297fa38988bfccd40dd5f6976b400e53d3537dcfc3819c92`，通过 `memory.forget.preview` 得到只影响 1 个单元、删除 1 个向量引用、无关系边撤回的计划，再经 `memory.forget.enqueue` 提交。持久请求 `web:sha256:d3c68fe548107e17e8e2e2b6be968228cf81d6a44c7eb0f9de9f869e3738782a` 从 RUNNING 结算为 SUCCEEDED，删除 epoch 升到 1，结果记录 1 个被抑制单元和 1 个向量删除。

同一份已冻结试验在遗忘后从 4 条**可见**结果降为 3 条；第一群管理筛选从 2 条 ELIGIBLE 降为 1 条，第二群保持 2 条。数据库中的目标单元为 SUPPRESSED，另一主干的单元未受影响。Payload authority 对该请求选中的 16 个 Payload 均记录 `SCRUBBED`，对应正文行数为 0。目标向量引用已 TOMBSTONED，`vector.tombstones.sweep` 对该准确 ref 至少一次以 SUCCEEDED 结算，回执列出目标 ref，证明 Milvus prune 路径已完成，而不只是从召回结果中隐藏。

最终经正式意图使 Bot 回到 OFFLINE，假端点停止。第一个 Bot 库无 ACTIVE 图，第二个 Bot 库仍为 0 个记忆单元/facet 且无 ACTIVE 图；假端点发送动作数为 0，现役 v33 Supervisor 保持 healthy。此证据覆盖单条文本单元的遗忘与异步清理；图片、跨主干联想证据、批量遗忘以及真实 QQ/模型并发竞态仍需分别验证。
