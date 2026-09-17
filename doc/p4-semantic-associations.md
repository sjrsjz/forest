# P4 自动跨主干语义联想

状态：已接入的 P4 实现切片。本文描述向量发布后的持久维护队列、关系投票合流、删除失效和 Agent 召回投影。它复用已经为原生图文检索生成的 embedding，不增加 LLM、摘要或额外 embedding 请求。

## 1. 语义边界

自动联想只连接同一 Bot 中两个不同来源主干的完整 `SOURCE_UNIT`。关系类型仍为无向 `ASSOCIATES_WITH`，但关系键包含 embedding space，且与 `manual-source-association-v1` 分离。它表示两个来源在固定多模态向量空间中高度接近，可以作为一跳扩展线索；不表示两条消息来自同一群、事实相等、存在因果关系或获得新的读取权限。

当前固定算法为 `semantic-cross-trunk-top4-v1`：对一个来源的全部文本和图片 facet，计算它与其他主干来源的最大余弦相似度，只保留分数至少 `880000 ppm` 的前四个来源。分数相同时按稳定 `source_unit_id` 排序。单次精确候选物化上限为 4096；超过上限时把任务结算为 `CAPACITY_EXCEEDED`，不提交部分邻接。后续专用向量驱动可以替换候选扫描，但必须保持相同的稳定候选和投票协议。

`semantic_associations_enabled` 是 Memory 配置中的独立开关，WebUI 以普通复选框管理。关闭时不领取自动维护任务，召回也不读取自动边；人工审核边和普通向量召回不受影响。旧配置读取时补入默认值 `true`，保存后形成完整的新配置修订，不要求管理员编辑文件。

## 2. 发布、排队和处理水位

文档 embedding 结果与 Memory 模块向量表在一个短事务中发布。数据库触发器随后按 `(bot_id, space_id, source_unit_id)` 增加 `requested_revision`；同一批中的多个 facet、重复通知和并发发布合并到一个来源任务行，并单调推进该行水位。干净 bundle 从首次写入就建立该维护水位，不存在升级前向量补建路径。

MemoryRuntime 每次只让一个 `semantic:reconcile` worker 领取该 Bot 的维护工作。不同宿主尝试仍由 Host lease 约束；同一宿主中的并发调用使用 `FOR UPDATE SKIP LOCKED` 领取不同来源。处理器在事务开始按 Kernel 锁序取得 runtime、Host 和 deletion clock，再锁任务水位。外部 embedding HTTP 不在这个事务中发生。

处理完成时把 `processed_revision` 推进到领取时冻结的 `requested_revision`。若处理期间又发布新 facet，触发器会等待任务行锁并继续增加 requested 水位，因此新事实不会被旧完成覆盖。失败事务不推进 processed；重启后继续领取同一水位。

## 3. 有向投票与无向图边

Top-K 是每个来源的有向选择，但图中的 `ASSOCIATES_WITH` 是无向边。每个方向保存一条 `semantic_vote`：

```text
SemanticVote
  bot_id, space_id
  origin_source_unit_id, neighbor_source_unit_id
  association_id, scope_id
  revision, active, current_contribution_id
  score_ppm, origin_facet_id, neighbor_facet_id
```

规范 `association_id` 由 Bot、算法版本、向量空间和排序后的两个来源 ID 派生。两个方向共享 association ID 和图 edge ID，但各自有独立 contribution。任一方向仍在 Top-K 时关系边存在；两个方向都撤回后边消失。

一个来源重算时，在同一事务内比较旧投票集合与新 Top-K：

- 新邻居写入新的 `RelationContribution`，投票 revision 加一；
- 离开 Top-K 的邻居写入指向当前 contribution 的 `Retraction`，投票 revision 加一；
- 邻居集合不变时只更新可重建的分数/facet 选择，不伪造新的图贡献；
- 每次 active 状态变化另写不可变 change 记录，图 operation、投票 head 和任务水位原子提交。

关系贡献 ID 包含来源方向和投票 revision。相同事务重试得到相同 operation/input digest；不同方向是独立证据集合，纯图归约器按 contribution ID 去重，不能把重试次数当权重。

## 4. 异步合流

新来源首先计算自己的 Top-K，同时检查其他候选当前是否缺少反向投票、反向分数是否因新 facet 过时，或新来源是否会挤入其稳定前四。需要重算的来源在当前事务提交后按 ID 排序重新排队。排队与当前任务不同时持有两组任务行锁，避免 A 处理器等待 B、B 处理器又等待 A 的死锁。

相反方向的处理器可以同时到达同一个图 scope。scope 行锁串行化 operation revision；稳定 operation ID 和 contribution ID 让重复到达收敛。最终有向邻居只由 `(score_ppm DESC, neighbor_source_unit_id ASC)` 决定，不依赖 Worker 完成顺序。六来源等分并发测试验证每个来源都稳定选择 ID 最小的四个其他来源。

联想 scope 使用 `partition=semantic-associations-v1`，只包含两个端点的单点窗口。它不生成 `TRUNK_NEXT` 或 `MEMBER_OF`，因此跨群关系不会把两条主干拼成一条时间线。

## 5. 删除、配置与重建

召回始终重新授权 seed、neighbor 和边的全部 `source_refs`。来源抑制或载荷撤销后，即使旧图 publication 尚在，也不能泄露正文、群身份或隐藏邻接计数。自动边还要求两个记录的证据 facet 及向量载荷仍为 ACTIVE，并要求 query 使用相同 embedding space。

后台修复扫描查找端点或证据向量已失效的 active vote，并重新排队其 origin。重算写显式 retraction；新图 publication 随后去掉边。删除 epoch 在每次关系 operation 提交前再次校验。来源恢复且原向量仍合法时可以重新计算；向量已物理清理时，要等重新索引发布后由触发器排队。

Memory 配置修订变化会撤销旧 embedding 发布许可。自动召回只读取当前查询的 space；旧 space 的关系账本作为历史派生事实保留，不能混入新 space。关闭自动联想时保留这些可审计记录，重新启用后从尚未处理的来源水位继续。

## 6. 召回与模型投影

向量融合仍先产生最多 16 个 seed。关系查询在固定 `graph_revision` 下合并人工 head 与当前 space 的自动 active vote，每个自动 association 只选一个当前 contribution 参与邻接扫描。所有边继续受 64 条检查和 32 个新增来源的上限约束。

自动路径在 EvidenceBundle 中使用固定字段：

```json
{
  "kind": "graph_association",
  "association_origin": "semantic",
  "score_ppm": 925000,
  "association_id": "memory-semantic-association:...",
  "edge_id": "graph-edge:...",
  "seed_source_unit_id": "source-unit:...",
  "graph_revision": 42
}
```

人工边使用 `association_origin=reviewed` 和 `score_ppm=null`。旧的冻结 EvidenceBundle 没有新增字段时仍按 reviewed 路径读取，保证重启和升级兼容。模型输入验证只接受这两种 origin 和 `0..1000000` 的整数分数；来源正文继续完整保留原群、作者、事件及有序 text/image parts。文本模型只在单次 API 投影中去掉图片载荷，不改变自动关联所依据的多模态向量或证据来源。

## 7. 验收证据

- 两个不同群来源使用相同向量时，并发处理相反方向后形成一个无向边、两个独立 contribution；重复排队不增加 change 或边权。
- 六个不同群来源等分并发写入后，每个来源稳定保留四个按 ID 决胜的邻居，所有 requested/processed 水位相等。
- 同一群主干的两个来源不建立自动跨主干边。
- 候选规模超过 4096 时，不发布截断的 Top-K，并显式撤销该 origin 上一次的自动投票，任务以零邻接和 `CAPACITY_EXCEEDED` 结算。
- 来源遗忘后读取立即被来源授权阻断；修复扫描为两个方向写 retraction，发布后图中不再存在该边。
- 自动一跳投影明确标记 `semantic`、定点分数、seed、edge 和 graph revision；模型协议拒绝未知 origin 和越界分数。
