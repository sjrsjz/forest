# P4 PostgreSQL 图分片与发布

> **历史快照｜禁止作为当前实现规格。** 本文只记录旧纵向切片及其当时的局部验证。它服从 [00 架构总契约](00-architecture-contract.md) 与 01–14 号详细设计；冲突内容已经失效。不得据此新增或恢复旧表、编号迁移、兼容读取、固定业务页面、专用宿主 API、巨型 Store、业务组合根或产品子 Agent。需要复用算法时，必须先把 owner、namespace、Repository contract、Generation、UnitOfWork participant、并发 fence 和 management contribution 映射到当前设计。

`forest.storage.memory_graph.GraphStore(messages)` 将[纯事实归约器](p4-graph-reducer.md)接入真实 Bot PostgreSQL。Memory 模块的内容寻址 schema bundle 保存不可变事实、图发布记录和有索引的节点/边邻接表；事实值正文仍放在该模块拥有的受管载荷存储。它不增加隐式实体抽取、LLM 摘要或按消息调用模型。

这是一种原生图驱动实现，不以 Neo4j 等独立服务作为必要前提。图仍是候选索引；它不授予任何跨群读取权限，也不将联想边的权重解释成事实置信度。

## 有限分片与完整主干

`GraphWindow(source_id, start_seq, end_seq)` 固定最多 256 个连续原始来源位置。`GraphScope(windows, partition='trunk')` 可以包含不同来源的窗口，因而支持跨主干关系；每个来源在同一 scope 中只能出现一个连续窗口，避免在未检查的区间上造出邻接边。范围和 partition 一起决定稳定 scope ID。

`discover(lease, after_source_id='', after_start_seq=0, limit=16)` 按来源身份和窗口起点返回有限 keyset 页。默认物理窗口为 1–256、257–512 等，不限制一个 Bot 或一个群的总来源数。每个非空窗口自动附加当前最近的合格前驱 halo：前驱可越过未接纳、已删除、已取消或缺少原图的区段；它不是机械地取 `start_seq-1`。数据库按来源位置索引选择，随后由共享 `MemorySourceStore.authorize_sources_in` 复验。

纯归约器只接收该窗口的合格来源及已声明的 halo。同一群的 TRUNK_ROOT 在所有分片中使用同一个稳定 ID，前驱与本窗第一个成员之间生成 TRUNK_NEXT，因此按页构建可以连接任意规模主干。全域读取应按稳定节点/边 ID 去重。资格晚到或删除可能改变本窗和后继窗的 halo，需要重新构建；未完成、失效和历史分片不能被描述为当前全图完整覆盖。

每个 scope 最多 8 个窗口、包含 halo 在内最多 512 个来源单元、1024 个事实/相关撤回。源正文读取前按载荷头限制为合计 2 MiB；图 metadata/projection 各最多 4 MiB。超限返回 `GRAPH_COVERAGE_LIMIT`，不截掉证据。可用更小窗口或新的显式 partition 继续接纳事实。不同 partition 的事实候选必须联合归约，才能判断跨分片 logical key 冲突；单分片的 `conflict=false` 不是整个 Bot 已无冲突的证明。

## 接口与事务

```python
graph = GraphStore(messages)
page = await graph.discover(lease, limit=4)
for scope in page['scopes']:
    result = await graph.refresh(lease, scope)

# 外部 Worker 构建也可明确分开：
snapshot = await graph.capture(lease, scope)
candidate = graph.build(snapshot)              # 纯 CPU，事务外
receipt = await graph.publish(lease, candidate, operation_id)

view = await graph.read(
    lease, scope.scope_id,
    graph_revision=receipt['graph_revision'],
    node_ids=known_node_ids, limit=64,
)
```

所有方法属于受信任 Host Repository。写入与发布先锁 runtime/Host，再锁 memory clock 和 scope，最终复核数据库时钟中的 HostLease。capture 固定 `fact_revision`、`deletion_epoch`、`storage_generation`、`maintenance_epoch`、scope head、操作集、来源 metadata 与 halo；维护恢复为 ACTIVE 不会使旧候选重新有效。关闭 data admission 时拒绝新图发布。

`capture` 在有限窗口内分页调用共享来源资格检查，要求完整 lineage、全部依赖已获实际 InputBatch 资格、ACTIVE 载荷、READABLE 原始输入、非取消 Job 和非已确认自身回执。没有当前 Conversation 时只去掉群可见性和本地 cut 条件，保留其他资格。SourceUnit 元数据只保存作者稳定身份、来源、part 类型与摘要；不复制正文、昵称、URL 或图片 bytes。图片 part 仅在受管 capture 为 CAPTURED 且其载荷仍可用时加入 available_parts。

`build` 只调用 `reduce_graph`。`publish` 在进入事务前验证候选等于纯归约结果，事务中校验 scope CAS、上述全部 epoch，再重新选择窗口/halo、资格、媒体状态和事实集合。Job 取消与 echo 确认不一定增加 memory clock，因此不能只比较 clock。最后一次 HostLease 复核失败会回滚 global revision、发布记录和全部邻接写入。

`refresh` 用同样发布检查，但来源、halo、操作集合和投影未变时返回 `unchanged=true`，不写新发布记录、不增加修订。无关群的新消息和进程重启不会让同一内容每秒复制一份。

## 不可变事实与撤回

`append(lease, scope, command, expected_deletion_epoch=...)` 接受纯归约器定义的 `Assertion`、`RelationContribution` 或 `Retraction`。每个稳定 operation/fact ID 在 Bot 内唯一：完全相同的重试返回旧事实，不重复增加事实修订；不同 metadata 报 `IDEMPOTENCY_CONFLICT`。另一个 Bot 的 DTO、窗口外证据和不存在的 source revision/part 被拒绝。

Assertion 的 `ValueRef` 必须指向同 Bot 中已接纳且可用的 `memory.assertion` Payload，其 digest 和完整来源清单必须与断言一致。永久事实表只保存该引用及摘要。关系支持纯归约器的有类型端点、显式证据与整数贡献。SOURCE_UNIT 端点可跨群；ASSERTION/ENTITY 端点只有在本次分片归约实际包含其合法断言时可见，不能用另一个分片的节点存在性绕过证据闭包。

撤回可先于目标事实接纳。按 Bot 稳定目标 ID 查找的相关撤回跨 partition 生效，不因以后目标落入另一个物理分片而失效。更新后的相关操作集会使旧读版本报 `GRAPH_STALE`，不会借历史 head 恢复已撤回断言。来源删除后同 ID 的接纳回执仍可保留，但不授权读取被删除内容。

## 全局修订与候选读取

每次真实 scope 发布在同一事务递增 Bot 的 `graph_revision`，同时写入 scope 的本地 revision。`coverage(lease, graph_revision=..., after_scope_id=..., limit=...)` 固定该全局修订，返回每个 scope 在此修订以前最近的发布记录。跨页必须回传同一个 graph_revision，不能每页重新取 head。

`read` 同样固定 global revision，可按节点 ID 查询有限邻接页，返回节点 metadata、边及贡献证据、coverage/halo、构建时 fact_revision、当前删除 epoch 和 `required_source_refs`。它再次检查该版本必要来源仍合法可用；旧来源失效时报 `GRAPH_SOURCE_CHANGED`。新增来源允许读取显式固定的历史覆盖，这不代表它包含新增来源。调用方必须用实际 Conversation 和冻结 input_cut 再执行 SourceStore 授权，才可物化内容或交给模型。

返回值明确标记 `requires_conversation_authorization=true` 和 `requires_partition_union_for_global_facts=true`。图的部分覆盖、历史邻接和联想分数均不能取代原始事实复核。本模块不负责将图接入 RAG、提供公开遗忘 API，或清理旧 Inbox/Agent/模型请求等明文副本。

## 验证

`tests/test_memory_graph_store.py` 使用真实 PostgreSQL 检查实际静默批次资格、迟到插入、跨群证据和冲突、撤回先到与跨分片撤回、重复/异载荷操作、两个 Worker 的发布 CAS、固定 global revision 与分页、Host 最终失效整笔回滚、维护/存储 epoch、数据库不可变约束、无变化 refresh、真实 Job 取消、真实发送回执形成的晚到 echo、原图 HTTP 捕获，以及 513 个来源跨三个窗口仍保持一根和 512 条连续主干边。
