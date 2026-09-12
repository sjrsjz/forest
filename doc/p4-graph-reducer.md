# P4 纯图事实归约器

`forest.memory_graph` 实现已接纳来源与事实集合的确定性归约，版本为 `forest-graph-v1`。它不执行数据库查询、网络请求、实体抽取、摘要、嵌入或图 head 发布。输入是宿主已经固定的来源修订、资格和删除状态，输出只是可发布候选；调用者负责来源权限闭包、覆盖范围、删除 epoch、发布 CAS，以及每次召回的最终复核。它不是可以直接发给群聊的检索响应。

## 公共接口

```python
graph = reduce_graph(
    bot_id,
    sources=source_units,
    assertions=assertions,
    relations=relation_contributions,
    retractions=retractions,
    suppression=Suppression(bot_id, source_unit_ids=("deleted-unit",)),
    source_heads={"revised-unit": 2},
)
semantic_bytes = graph.canonical_bytes()
```

`SourceRef` 共用 `memory_types.SourceRef(source_unit_id, source_revision=1, part_id='*')`。Bot 归属来自可信仓库及整个输入；带其他 Bot 的来源、断言、关系、端点或抑制集合直接拒绝。

所有 DTO 都是冻结 dataclass：

| 类型 | 内容 |
| --- | --- |
| `SourceUnit` | Bot、稳定 source/unit ID、来源类型、固定位置与内容修订、内容摘要和 payload 引用、作者稳定 ID、群平台身份、会话/ingress 与 occurrence 引用、parts、输入资格及可用性 |
| `SourcePart` | part ID、原位置、text/image/json/control 类型、part 修订和摘要 |
| `LogicalKey` | 稳定主体、谓词、有效时间域、规范化版本及 one/many 基数规则 |
| `ValueRef` | 类型、规范值摘要、可清理 payload 引用；不把断言正文放入图元数据 |
| `Assertion` | 稳定 operation/assertion ID、logical key、value ref 和全部必要 SourceRefs |
| `RelationContribution` | 稳定 operation/contribution ID、类型、端点、关系键、全部必要 SourceRefs、整数权重 |
| `Retraction` | 稳定 operation ID，明确的 assertion/contribution/relation 目标 ID |
| `Suppression` | source/source-unit、跨修订的 part、assertion 和 relation 抑制集合 |
| `GraphProjection` | 规范排序的节点、边、事实候选及冲突状态；无存储时钟或发布 head |

每个 Assertion、RelationContribution 和 Retraction 表示一个原子操作；一个业务步骤产生多个事实时需派生稳定子操作 ID。同 operation ID 或同事实 ID 异载荷返回 `IDEMPOTENCY_CONFLICT`，即使该事实已被删除也不按成功重复处理。SourceUnit 同 unit/revision 异载荷同样拒绝。同源位置不能分配给不同单元，内容修订不能移动单元；同一外部群身份不能注册成两个主干。

来源只有一个输入修订时可直接选用。存在多个已接纳修订时必须通过 `source_heads` 显式选择，缺少选择报 `GRAPH_SOURCE_HEAD_REQUIRED`；不得按到达顺序或隐式最大 revision 选胜者。选择的 head 缺失时报错。可用性和资格也是该次冻结输入的一部分，不能将两个不同状态的同版快照混在一次归约中。

`available_parts=None` 规范化为本修订全部 parts 可用；显式空集合表示没有可用 part。图节点保留元数据与原 part 顺序，输出的 available_parts 再扣除当前抑制集合。没有正文、图片字节、昵称或群名字段；显示信息和原文应从获权 payload 物化。

## 主干、证据与冲突

每个具有可用合格成员的 GROUP 来源生成一个稳定 TRUNK_ROOT。每个成员只有一个 MEMBER_OF，TRUNK_NEXT 从同源固定位置排序后的相邻对重建。迟到的 102 会将 101→103 替换为 101→102→103；删除 102 后可重新连接 101→103，其位置不会重编号。非群来源不生成群主干。结构边不能被显式关系操作或撤回单独篡改，改变成员须通过来源资格或抑制状态。

每个可读断言生成 ASSERTION、稳定主体 ENTITY、SUBJECT_OF 和逐来源 EVIDENCE。相同 logical key/value 的独立断言归入一个 Candidate，保留全部 assertion IDs、payload refs 和来源集合。不同值并列：one 规则标记 conflict，many 规则保留合法多值；不会根据写入时间选择真值。不同稳定主体不会因为同名自动合并。

ASSOCIATES_WITH 和 CONTRADICTS 的端点规范排序，有稳定无向身份；EVIDENCE 和 SUBJECT_OF 保留方向。端点或任何必要来源缺失、未获输入资格、不可用、修订不符或受到抑制时，整项贡献不可读。ENTITY 因其他合法断言而存在，不能使隐藏关系或隐藏候选的数量泄露到输出。

权重版本为 `source-manifest-max-sum-micros-v1`：同一规范关系和同一必要 SourceRef 集合的贡献取最大整数权重，再对不同来源清单求整数和。所有合法贡献 ID 和来源仍保留；换一个操作 ID 重提同一来源清单不会重复加权。权重不是概率，不推断不同清单的统计独立性；单项为 0～10¹² 的整数 millionths，禁止浮点、布尔值及到达次序累计。

## 撤回与删除

撤回按稳定目标集合归约，先到的撤回同样对后到目标有效；它不撤回同 logical key 上尚未明确观察到的其他断言。撤回一个 contribution 只移除其证据贡献；撤回显式规范 relation 作用于该边全部贡献。断言自身内嵌的必要来源只能通过撤回该断言或抑制来源使其失效，不能靠修改独立 EVIDENCE 提案改写原断言。

删除 unit 或 `(unit,'*')` 抑制其所有修订及所有依赖。删除 `(unit,part)` 同样跨修订：整单元 `SourceRef(...,'*')` 依赖失效，精确指向其他 part 的依赖仍可保留。缺失图片不会隐式变成 caption；精确文本依据可继续可读，但整单元依据必须等全部必要 parts 可用。独立合法断言继续支持同一 Candidate，剩余输出不包含已删除的那份证据。

这里实现的是纯可见性归约，不负责物理删除、接纳新 epoch 或阻止外部存储晚写。调用者必须保留原始操作集合与抑制身份；只保存上次可见输出、丢弃先到撤回，再增量补投，无法维持这些保证。

## 验证

`tests/test_memory_graph.py` 验证完整来源排列、带随机重复的分批合并、迟到插中间、同群线程共主干、跨 Bot 身份隔离、多源证据、显式冲突、撤回先到、关系类型和整数聚合、局部图片抑制、旧修订防复活、缺失来源闭包以及相同身份异载荷拒绝。测试不调用外部服务，也不把这些纯归约性质等同于数据库授权和发布仲裁的合流性。
