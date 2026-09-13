# P4 跨主干联想管理与召回扩展

状态：已接入的 P4 实现切片。本文把已有纯图归约器和持久图分片接到公开管理与 Agent 召回，不引入逐消息语义模型调用。

## 1. 首版语义边界

一个 Bot 仍只有一个全局记忆域，每个群来源仍只有一条独立 `TRUNK_NEXT` 主干。首版公开联想连接两个完整 `SOURCE_UNIT`，关系类型固定为无向 `ASSOCIATES_WITH`，关系键固定为 `manual-source-association-v1`。它表达“管理员确认这两个来源可互相作为一次扩展线索”，不表达同群、时间先后、事实相等、因果或授权。

管理员不能在首版输入自由关系说明。自由说明会成为需要来源、删除和提示注入边界的新正文；在它接入 PayloadStore 前，不把文本塞进图元数据。权重固定为 `1_000_000`，同一规范端点对只有一个可管理 association head。

裁剪、消息接纳、索引入队和主干构建都不调用 LLM。人工联想直接使用已经审核的两个来源；未来 Agent/维护任务生成语义联想时，必须另建有预算的候选工件与选择协议，不能复用人工确认身份。

## 2. 管理协议

Web 使用三项独立动作：

1. `memory.association.read` 分页读取仍有可用端点的 association head、active 状态、当前修订和图发布状态；
2. `memory.association.plan` 以 `expected_deletion_epoch` 和当前 association revision 预览建立或解除；
3. `memory.association.apply` 携带 `plan_id + plan_digest + expected_deletion_epoch` 提交。

建立计划接受两个不同 `source_unit_id` 和 `expected_revision=0`。解除计划接受一个已有 `association_id` 及页面显示的当前 revision。计划冻结规范端点、来源 revision、图 scope、目标 active 状态、主体、issuer、删除 epoch 和十分钟期限。apply 重新验证计划身份、来源资格、当前 deletion epoch、association revision 和 Host lease。

建立时写新的 `RelationContribution`；解除时写针对当前 contribution 的 `Retraction`。association head 与不可变 change 记录、图 operation、memory fact revision、管理 operation、audit 和 outbox 在同一事务提交。提交后图投影可以暂时为 `PENDING`，MemoryRuntime 处理持久待发布 scope；API 不用长事务等待 CPU 归约。

同一 operation ID 同输入只返回原回执。两个管理员从同一 association revision 提交时只有一个成功，另一请求返回 revision conflict。两个端点顺序先规范化，所以 A↔B 和 B↔A 是同一 `association_id`。解除后再次建立产生新的 contribution revision；旧 contribution 和 retraction 永久保留，不复活。

## 3. 图分片和生命周期

主干 scope 继续使用每个来源一个连续 256 位置窗口，并生成 `MEMBER_OF/TRUNK_NEXT`。联想 scope 使用 `partition=associations-v1`，每个端点只覆盖其确切位置；同群中相隔超过 256 条的两个端点也可以使用两个互不重叠的单点窗口。

非主干 scope 不生成结构边，也不取主干 predecessor halo。这样两个离散证据窗口不会伪造中间缺口上的 `TRUNK_NEXT`。`GraphScope` 对同一来源的重复窗口要求互不重叠；主干 partition 仍保持原先“每来源一个连续窗口”的约束。

MemoryRuntime 先扫描 `operation_revision` 尚未被最新 publication 覆盖的持久 scope，再循环发现普通主干窗口。append 成功、宿主重启或上次 publish 失败都能从同一 scope head 继续；多个刷新器仍由 scope revision 和全局 graph revision CAS 仲裁。

状态转换为：

```text
ABSENT --associate apply--> ACTIVE/PENDING --graph publish--> ACTIVE/CURRENT
ACTIVE --retract apply--> INACTIVE/PENDING --graph publish--> INACTIVE/CURRENT
```

来源遗忘不需要先修改 association head。任一必要来源被 suppression 后，管理读取和查询授权都会立即排除该边，旧投影只作为历史派生工件保留，不能成为正文权限或恢复 association 可见性的依据。

## 4. 一跳召回扩展

向量融合先产生有序种子。仅取排名前 16 个种子，在当前已发布 graph revision 上沿有效 `ASSOCIATES_WITH` 扩展一跳，最多检查 64 条边、加入 32 个不同来源。首版不沿 `TRUNK_NEXT` 扩展，不做二跳，不让高连接节点无限放大结果。

每条边必须同时满足：

- association head 当前 active，publication 覆盖当前 scope operation revision，且边包含 head 的当前 contribution；
- seed、邻居和边的全部 `source_refs` 均属于当前 Bot；
- 在当前 Conversation 的冻结 `input_cut` 下逐来源重新授权；
- deletion epoch、配置与运行时 fence 在最终 EvidenceBundle 事务中仍有效；
- 邻居不是本次 query source，也未被更高排名候选重复选中。

图只提供候选。邻居正文仍从 MemorySourceStore/PayloadStore 物化，并完整保留原群、作者、事件和有序图文 parts。送给模型的证据头增加固定 `retrieval.kind=graph_association`、seed source、edge ID、association ID 和 pinned graph revision；模型能区分向量命中与联想扩展，不能把来源群误认为当前群。

图扩展排在全部向量候选之后，再按 seed 排名、association ID 和邻居 ID 确定排序。它共享现有 `query_limit/max_result_tokens/max_result_images/max_result_bytes`，不能挤掉更高排名的向量证据。EvidenceBundle 的来源清单包含 query sources、返回来源及边的全部证据来源，使重放和 Agent 最终提交都重新检查完整授权闭包。

若没有当前图、图仍待发布或图查询失败，向量结果照常返回，`completeness.graph` 明确为 `UNAVAILABLE/PENDING/PARTIAL`；图派生故障不阻塞普通对话。

## 5. WebUI 与可见性

来源页选择恰好两项后可以预览建立联想；关系列表显示有效和已解除的 head、两个端点各自的群、作者和来源 ID、association revision 及 `CURRENT/PENDING`。有效关系可以预览解除，已解除关系可以按其当前 revision 预览恢复。切换 Bot、刷新来源、删除 epoch 改变或内容失效会清除旧计划和幂等键，迟到响应不能写回另一个 Bot 的页面。

Web 可以跨群管理当前 Bot。群聊 Shell 暂不增加联想命令；以后接入时只能从发令群选择至少一个端点，并复用同一管理协议，不能通过图边扩大群管理员的来源可见范围。

## 6. 验收

- 两群来源建立联想后，只有 seed 向量命中时可一跳召回另一群，EvidenceBundle 和模型输入都标出原群、作者及 association path；
- 同群相距超过 256 条的端点不产生伪造 `TRUNK_NEXT`；主干分片行为保持不变；
- 同端点反向建立归一到同一 head；同 revision 并发 apply 只有一个成功；解除和再次建立不复活旧 contribution；
- 图 publish 失败或宿主重启后从持久 pending scope 恢复，未发布期间向量召回继续；
- 当前群未进入 input cut 的端点、私有来源、已撤回/删除来源和缺少任一边证据的候选不泄露节点、群名或计数；
- apply 原子回滚 head/change/graph operation/Web operation/audit/outbox；001–052 非空旧库升级保持既有 issuer 限制；
- Web 双 Bot、迟到响应和计划失效隔离；前端不要求编辑 JSON 或服务器文件。
