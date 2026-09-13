# Agent 正文载荷与来源闭包（031）

031 将 Agent step 的 `snapshot/output/closed_parts`、模型语义 manifest、精确请求 bytes、模型结果、工具 arguments/result 和旧工具 observation 正文迁入 `PayloadStore`。原列保留为严格 `NULL`，事实行指向永久载荷头，原操作 ID、输入/输出/wire 摘要、状态、费用及 checkpoint 不改。没有旧列读取回退。

新模型步骤的 snapshot 继承当前实际可读 Context 的 `payload_ids`，以及本 Run 尚未发布的 CLOSED 工具组载荷。触发切口、用户及群来源、图文 parts 仍使用原 canonical DTO；不会为了获得正文而重新读取队尾。语义 manifest 再继承该 snapshot 和已经 BOUND 的 EvidenceBundle；请求、输出、工具参数沿父载荷继续传递来源。工具结果继承 snapshot、模型输出、工具参数及宿主提供的其他结果父载荷；closed_parts 保留这些工具结果的闭包。后续 Context RUN_STEP 直接引用 closed_parts 载荷，最终 Reply 由 030 继承最终模型输出。

来源集合由宿主确定，PayloadStore 固定、排序并展开父载荷来源。模型选择的 unit ID、工具参数中的任意 source ID 均不授予读取权限。当前实现保守继承整个 canonical snapshot，并未尝试推断模型究竟使用了哪一句；text-only 输入投影也不删除 canonical 图片依赖。达到现有 256 个直接父载荷或 4096 个来源的限制会明确失败，不静默截断。

`AgentPayloadStore.guard_run_in` 复验该 Run 已接纳的所有步骤和工具载荷，因此裁剪或完成顺序不会丢失已经产生输出的依赖。它由现有 `AgentMemoryStore.guard_run_in` 统一调用，覆盖模型发送许可、Kernel 工具最终状态写入、工具组闭合、Context 发布与最终 Reply。当前 snapshot 恢复和精确请求重放也通过载荷读取；被清理的请求不能换 operation ID 再执行。最终已提交 Reply 的重复读取仍保留回执，正文返回不可用。

锁序沿用原 Binding/路由、Bot runtime/Host、Conversation/Run/业务行之后的 memory clock 和载荷锁；Kernel 内部守卫不反向取得 Binding 或 Conversation 锁。模型和工具正文失效后，原 attempt 的状态、原结果摘要和已知费用仍可作为受限 metadata 结算，不保存返回文本、工具提案或新回复。终止操作不会因失效来源而被迫发布旧 closed_parts。

工具结果用 `result_outcome` 固定该次观察对应的结果状态，区别于可能已进入重试或取消的调度状态；读取和工具组闭合校验原 `[result_outcome,result]` 摘要。已知终态的结果载荷引用、摘要及 metadata 不可改写，UNKNOWN 对账和确认 NOT_SENT 后的新 attempt 仍按受限状态转换执行。每次结果引用、摘要、metadata 或 outcome 改变都追加观察记录。模型 metadata 只允许固定请求身份、既定撤销原因和有界 token 数值；工具 metadata 只允许既定撤销原因，数据库拒绝额外正文或嵌套费用字段。这是对受信任数据库写入路径的防误写约束，不宣称数据库管理员是外部沙箱中的攻击者。

迁移在 SQL 准备、Python handler 和最终约束安装的同一事务内完成。旧 Agent 副本的历史工具/状态来源无法可靠重建，统一带有 Bot 内 `legacy_source_ref` 保守来源标记；这不是完整来源证明。首次遗忘按公共迁移协议显式包含该保守来源，因此旧派生内容及随后继承它的新输出一起失效。旧请求在删除前仍可精确读取原 bytes 和原摘要；迁移重跑不能恢复已经删除的 body。

旧工具的结果 outcome 只在原正文与原结果摘要精确匹配时恢复；无法验证的旧摘要使 031 事务失败并保留原事实，不重算覆盖摘要，也不猜测为 UNKNOWN。

这里完成的是 031 的 Agent 步骤载荷族。升级前遗留 Child 由 032、Delivery 由 033 处理；031 的父快照读取和结果采纳接口当时为旧 Child 提供了兼容与来源传递。产品 Child 现已进入退役设计，032 只保留历史审计、清理和结算价值，不再是 Agent 执行协议，详见[产品子 Agent 退役](p3-children.md)。后续 036–038 已接通 Kernel 状态/结果和代际暴露，042–049 已迁移配置与 `bot_agent_runs.spec`，实际现状分别见[Kernel 正文与代际来源](p4-kernel-payloads.md)和[配置正文与独立替换](p5-configuration-payloads.md)。Job/Shell、数据库旧页、WAL、备份及已经发给外部服务的内容仍属于另外的清理边界，不能将本族在线 body 删除称为全链路物理遗忘。

验收包括真实 PostgreSQL 上的无 RAG 绑定原生输入删除、快照/请求/工具/closed_parts 清理、晚到模型费用结算、下一 Run 不复活旧回答、不可变引用约束，以及从实际 001–028 非空旧库导入精确 wire/历史输出并验证保守清理与重启幂等。原有 AgentStore 与 Runner 测试继续验证同群串行、最后触发切口、工具因果组和效果回执恢复。
