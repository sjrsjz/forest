# P4 记忆来源、关系与遗忘管理

> **文档级别：Memory 模块详细设计。** 本文定义长期运行后的管理能力，不定义控制台顶层页面。所有查询和动作由 `memory.graph` 的 management contract 暴露，WebUI 壳只负责通用模块工作区、授权、游标和沙箱 UI。

## 1. 管理对象

管理员面对的是一个 Bot 的全局多主干记忆域。群聊对应稳定主干，私聊、内部任务和外部知识使用明确的其他来源类型；它们不是独立数据库。每个来源、节点、关系和召回结果都必须显示原始群/来源、作者、时间、模态、证据及当前可用状态。

管理视图至少包含：

- **主干目录**：群来源身份、平台/realm/外部群 ID、名称快照、最早/最新来源位置、来源数、图发布水位和索引水位；
- **来源检索**：按主干、作者平台 ID、时间、来源类型、text/image/mixed、正文状态、索引状态和全文条件组合筛选；
- **关系浏览**：从来源、断言、实体或关系进入邻接图，明确边类型、方向、两端来源、人工/向量/规则贡献、权重、证据、有效状态、图修订和是否跨主干；
- **召回试验**：以文本、图片或混合输入调用真实 Memory 查询契约，显示 facet、候选、过滤原因、跨主干扩展路径和最终 EvidenceBundle；
- **遗忘操作**：选择完整来源单元，预览影响闭包，应用 suppression/deletion epoch，并观察异步 scrub 进度。

Memory 页面是模块自带的管理 UI。控制台不能复制一套 Memory 专用 REST API、固定表单或顶层导航；无自定义 UI 时由同一 query/action schema 生成通用视图。

## 2. 大规模浏览和稳定分页

来源规模按数十万至数百万设计。所有过滤都在服务端进入有索引的查询计划，正文只在命中有界 ID 后按权限加载。不得把全量来源、节点或边送到浏览器再筛选。

默认排序为 `(occurred_at DESC, source_unit_id DESC)`，游标同时绑定：

```text
bot_id + operation_id + normalized_filters + sort + snapshot_revision
```

换 Bot、筛选、排序或图修订后旧游标失效。页面返回 `items/has_more/next_cursor/snapshot_revision`；不默认执行精确全表计数。前端使用虚拟列表或有限 DOM 分页，详情抽屉按 ID 延迟读取正文和关系。持续写入期间，固定 snapshot 的翻页无重复和遗漏；用户主动刷新才进入新 snapshot。

全文条件使用 Memory 模块可重建的授权搜索投影和 GIN/等价索引。图片查询使用原生图文嵌入，不通过 caption 代替图片向量。大图、原图下载和敏感正文预览分别鉴权，列表本身只返回摘要与稳定来源标签。

关系图查询必须有起点、方向、边类型、深度、节点/边上限和时间预算。达到上限时返回截断原因与续查游标，不能无界递归，也不能把“当前页没有边”表述为全局没有关系。

## 3. 遗忘协议

首版公开遗忘的最小目标是完整 `SourceUnit`。部分图片或一句文本仍属于该来源的有序内容，不能原地删 part 后保留旧 revision；未来若支持部分删除，必须生成新内容修订和新的来源身份。

协议分为：

1. `sources.query`：按权限分页读取可见来源；
2. `forget.plan`：固定请求目标、解析后的精确派生闭包、影响数、当前 `deletion_epoch`、plan digest 和期限；
3. `forget.apply`：同一主体提交 plan/digest/expected epoch，事务中重新解析闭包并 CAS；集合变化返回 `FORGET_PLAN_CHANGED`；
4. `forget.status`：报告 `REVOKED`、在线 body/media/vector 待清理量、备份/WAL 保留边界及最终状态。

apply 与新载荷写入按[干净数据库基线](14-clean-schema-and-payload-provenance.md#5-删除与异步完成的线性化)处理：新写先提交则必须进入闭包，删除先提交则晚到发布观察新 epoch 并失败。相同 operation 与相同输入返回原回执；相同 operation 的不同输入拒绝。apply、suppression、载荷撤销、管理 operation、audit 和必要 outbox 在同一短事务提交。

来源正文不可用时，永久事实仍显示 opaque ID、群/作者的授权摘要、epoch、影响数和 scrub 状态；不得以空 JSON、缓存副本或审计详情恢复正文。外部模型、平台或用户已经收到的字节无法由本地 scrub 撤回，界面将它们列为独立边界。

## 4. 人工关系管理

人工联想同样使用 `plan -> apply`。plan 固定左右端点的 source/node identity、关系类型、方向、审核理由载荷、当前 graph/deletion revision 和预期影响。apply 重验端点仍存在、当前管理员可查看两端来源、跨主干传播策略允许、图修订未越过冲突边界，再写不可变 relation contribution。

删除关系通过追加 retraction/contribution reversal 表达，不修改历史边。自动向量或规则贡献与人工贡献分别显示；同一逻辑边可以有多个证据贡献，UI 不能把聚合权重冒充单条事实置信度。跨主干边必须在两端都显示来源群；从当前群召回时仍执行输出可见性检查。

## 5. 群聊 Shell 边界

群聊 Shell 提供当前群作用域的有界查询、forget plan/apply 和关系计划状态。它不能通过提交其他群的 source ID 绕过作用域，也不能在聊天命令中返回大段正文或整张图。

ShellGate 只解析固定命令并签发当前 Binding/群/主体上下文；实际动作调用与 WebUI 相同的 Memory management contract。命令优先级、独立 CONTROL 回执、最终授权和资源租约仍按[群聊 Shell](09-group-shell-command-system.md)执行。Shell registry 与当前模块 Generation 一起冻结，不维护为了开发期旧命令版本而存在的兼容分派器。

## 6. 验收

- 以至少十万条多群、多作者、图文混合来源验证组合索引、固定 snapshot 键集分页、正文有界加载和浏览器 DOM 上限；
- 新消息持续写入时，同一游标窗口无重复/遗漏，换筛选和换 Bot 拒绝旧游标；
- 来源详情可以导航到完整的有界关系视图，逐边显示方向、端点群/作者/时间、贡献来源、证据、状态与图修订；
- 文本、图片和混合查询均使用真实嵌入路径，召回结果明确当前群和来源群；
- forget plan 后新增相关派生写会使计划变化或被 epoch 拦截，无关消息不使计划失效；
- 并发 apply 只有一个推进 epoch，scrub 跨宿主恢复且迟到向量/图/模型结果不复活内容；
- WebUI 与 Shell 使用同一模块 operation，权限范围不同但不维护两套业务实现；
- 页面在普通 PC 视口下默认只展开一个工作区，筛选、结果、详情和关系图按需展开，不依赖超长纵向表单。
