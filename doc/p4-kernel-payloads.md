# Kernel 正文与 Worker 代际来源

本切片实现迁移 036–038：长期 Python Worker 的实际内容暴露账本、Kernel 调用与状态正文分离，以及配置的独立来源域。它延续 [029–035 正文协议](p4-payload-migration-notes.md)，不代表通用管理、Job 正文、备份副本和整套遗忘入口已经完成。

## 1. 为什么依赖按 Worker 代际累计

Plugin 对象会跨调用存活。调用 A 读取的内容可能保存在对象属性中，调用 B 即使不再执行 `state.get`，也可能返回该内容。Host 因此为每个固定 generation 持久保存实际交付过的载荷集合；不要求插件自行报告使用了哪些消息。

`bot_generation_exposure_heads` 固定组件、实例、Worker incarnation 和 Host 所有权，保留 revision/status；`bot_generation_exposures` 保存不可变的交付身份与摘要；`bot_generation_exposure_members` 保存去重后的载荷成员。相同交付身份和相同参数幂等，不同参数冲突；独立交付并发提交后按集合并集合流。

配置、prepare 状态、每个调用输入、状态读取、子调用结果、迁移源和 restore 候选都在事务提交后才交给 Worker。`commit_state` 和 `finish_call` 使用该代际的整个集合构造派生载荷，并在最后提交点复验全部来源。子 Worker 的结果在交给父 Worker 前加入父代际集合，命中已完成回执的路径同样登记。

## 2. 容量与回收

每代最多 256 个直接暴露载荷。第 256 个仍可接纳；下一个新增成员使内容事务回滚，包含尚未提交的 Call/input header。Host 使用这次真实候选的稳定 ID 和预期 exposure revision，在独立事务中证明容量溢出并标记 `EXHAUSTED`。Worker RPC 不能调用这个管理入口或提供其参数。

整代的唯一 SourceRef 并集及晚到回声依赖闭包还必须不超过 4096。多个载荷共享的来源只计一次；不能只逐个检查载荷，否则两个各自合法的 2500 来源输入会让后续结果永远无法发布。新交付导致超限时，Host 在回滚后重新证明 `SOURCE_CAPACITY`，再标记 `EXHAUSTED`；已经交付的集合因晚到关联而超限则标记 `SOURCE_SCOPE_LIMIT/REVOKED`。完整证明仅保存在 Host 私有异常对象中，公共错误只含 generation、revision 和 reason。

`EXHAUSTED` 关闭新 DATA/CONTROL 准入，不推进 route epoch、不取消已接纳调用树。已有调用仍可使用已交付且可读的集合完成提交；新的状态或子结果交付如果会扩大集合，则明确拒绝。监督任务等待实际调用和依赖预留排空，然后回收进程，由原有组件恢复流程重新安装当前 desired。

排空同时检查数据库 `bot_live_calls` 和 Host 的本地调用集合。数据库已提交 admission、但成功确认尚未返回 Host 的调用，也属于必须等待的真实调用；不能因其还未进入本地集合而提前回收。检查持有当前 route/Host 权限，使用完成通知及有界轮询，且仍受原 drain timeout 和撤销中断约束。

删除或来源闭包失效产生 `REVOKED`，此时既有调用也不能继续读写或交付。监督扫描带游标、每页最多 64 代；回收任务与 Host 续租任务独立。排空期间发现撤销会结束等待，转入有界终止；不能等待一个 Worker 而耽误整个 Bot 续租。

组件状态仍支持 512 个有效键。prepare/capture 将实际可读状态按 key 排序，每 128 个键构成一页，最多四页。每页引用对应状态值的真实 payload；代际暴露这些页，保留全部传递来源，不通过截断来源适配容量。

## 3. 永久事实与可移除正文

| 永久事实 | 正文位置与读取规则 |
| --- | --- |
| Call 身份、原 input digest、状态、result digest/ref | 新输入始终有 `input_payload_id`，无业务输入也存真实 JSON null；结果通过 `KernelPayloadStore` hydrate，旧 `result` 列严格 NULL |
| 状态 key/revision/deleted、历史与原步骤摘要 | 当前状态、历史修订和步骤回执引用同一 `(component,key,revision)` 值载荷；旧 value/receipt 列严格 NULL |
| 状态步骤 operation/call/component/revision | `receipt_metadata` 在 SQL 层封闭字段；值只在经过授权的返回 DTO 中组合 |
| 迁移 source revision/schema/route/digest 与候选 digest | source/result payload 保存对应正文；旧 source_rows/result_values 严格 NULL，原 bare SHA-256 摘要语义不变 |
| generation 原 package/config digest、实例、授权及 schema 版本 | 单个 config payload 保存 `{config,state_value_schema}`；原两列严格 NULL，新增 ref/digest 不可变 |

COMPLETED 始终要求真实 result ref/digest，包括合法 JSON null。正文被清理后 Call 仍为 COMPLETED；`read_call` 返回事实 metadata，经过授权的内容读取明确报不可用，不重新运行 Worker，也不将缺失正文当作业务 null。

旧 Call 无法恢复原输入时，只能由迁移标记 `input_legacy_unknown=true`；正常新写入口不能使用这个例外。旧状态、结果和迁移正文使用既有保守 legacy 来源导入，已经存在的 suppression 在导入时生效；不改写 001–035 或冻结的 `migration_v1.py`。

## 4. 删除后的状态恢复

清理状态正文保留当前 revision。干净的新 Worker 的 prepare/restore 只得到可读值；它不能借重启把旧 revision 回退为零。`state.get` 读取已移除的值时返回 `KERNEL_CONTENT_REVOKED`，不返回假业务值。

此错误可以携带经过 IPC 白名单校验的 `current_revision`、`value_digest` 和 `payload_state=UNAVAILABLE`。插件如需重建，必须显式捕获该错误，使用保留的 revision 做 CAS，写入来自当前有效输入的新值。现有插件未处理这一情况时，调用明确失败。

已经看到旧值的候选受 exposure guard 约束；删除后不能继续 migrate、restore、activate 或发布。新的候选可以捕获带不可用标记的当前状态事实，跳过其正文；最终发布仍核对原状态头、route 和迁移摘要。不能采纳旧候选，把旧值重新写成新 revision。

## 5. 配置域与平台来源

`bot_payload_domains` 为 Host 独立输入登记不可变来源身份，包含 `SERVICE_INPUT`、`GENERATION_CONFIG` 和 `STATE_CONTRACT`。这些域参与 suppression 和依赖传播，但不是记忆召回项。MemorySource 允许正式注册的域作为父依赖；未知域、错误 revision/part 仍被拒绝，真实群聊来源的可见性、资格及输入切口不变。

配置属于管理员输入，旧配置迁移也使用独立 generation 配置域，不使用保守聊天 legacy 来源。每次新调用的业务输入有自己的域，同时继承 Host 掌握的实际父载荷；有独立域不意味着可以省略这些父来源。

038 同时替换 Web 配置发布和开发 watch 发布的 SQL 函数，使其读取真实配置载荷，保留原有 desired/effective CAS、包、授权和 Host 校验。读取还验证实际正文摘要、可用状态和当前删除依赖；旧配置列清空后，发布不会再依赖旧列。图来源资格函数也识别精确登记的域，保留原有消息资格、来源版本和 suppression 条件；域自身仍不能成为图节点或召回项。

OneBot 接收路径先验证签名和 Binding，再用 Host 固定解析器确定账号、群和平台消息身份，将原始 bytes 绑定到与 Inbox 相同的 source unit，然后调用可重载 codec。codec 的完整输出必须与 Host 解析的消息投影一致；正式 Inbox 使用这个已验证的平台投影。这样可删除 Worker 曾收到的原始/base64 输入，同时不因 codec 缓存而把其他群消息伪造成当前输入。

发送 prepare 输入继承实际 SendPart payload。`InvocationReceipt` 将业务值与真实 `result_payload_id/result_digest` 一起交回 Host；Delivery permit 核对固定 Call 身份、完成状态和精确返回值，并从数据库中的真实 SendPart 重算完整 OneBot 投影，拒绝 codec 替换正文。实际 wire payload 同时引用 SendPart 与 Kernel result。模板回复和 Agent 工具结果也传递真实 Kernel result 引用。

业务结果进入 Agent 或正式回复前，还须按当前 conversation 和已选择输入切口检查其完整来源。每次 Agent 最终守卫也检查已有 step/tool 载荷，包括未启用记忆检索、没有 BOUND 证据的 Run。群来源 `bot_shared` 仍可跨群使用；其他私聊、未知来源、错误 revision/part 和当前群切口以后的来源不能借 Worker 缓存进入新对话。来源域只认可已登记身份；错误不回显私聊 source ID。

非 Agent 回复拥有真实 Kernel result 父链时，登记完整的 Reply 来源依赖；相同 operation 重放复验原父链，且不改写原 SourceUnit。旧的不完整回复不会仅因重放而被升级成完整来源。

## 6. 事务与外部边界

统一顺序为组件 lifecycle/route → runtime/policy → Host → Call 树及状态头 → 排序后的 generation exposure heads → memory clock → payload。涉及多个代际时先取得完整的 exposure 锁集合，再读取 Agent 或记忆载荷。全新 generation 的 exposure head 在本事务内创建，尚未对其他事务可见。

每次 Worker invoke 在取得真实 IPC 并发槽后、写入请求前调用 `authorize_dispatch`，随后再次检查本地 Worker 状态。prepare/start/migrate/restore/activate 在各自交付前复验 generation。事务不跨 IPC、模型或平台 HTTP。

这些屏障保证删除提交后的新交付和新派生写入被拒绝；它们不能撤回已经交给外部 QQ/模型服务的内容。已在途效果继续使用原有 SENT/NOT_SENT/UNKNOWN 和计费元事实对账。删除 body 行也不代表旧数据库页、WAL 和备份已经完成物理回收。

## 7. 验证入口

- [暴露集合与并发事务](../tests/test_exposures.py)、[Kernel 来源授权](../tests/test_kernel_content_authority.py)。
- [真实 Worker/Kernel 交付与回收](../tests/test_kernel_payload_integration.py)、[真实 IPC 槽位屏障](../tests/test_worker_dispatch.py)。
- [admission 已提交但确认延迟时的排空、撤销与期限](../tests/test_exposure_retirement.py)。
- [037 非空旧库迁移](../tests/test_kernel_payloads.py)、[独立域与 038 迁移](../tests/test_source_domains.py)。
- [配置域的图资格与真实主干发布](../tests/test_graph_source_domains.py)。
- [状态迁移与删除恢复](../tests/test_state_migration.py)、[平台来源和实际发送回执](../tests/test_platform_payloads.py)。
- [来源去重与总量](../tests/test_payload_source_union.py)、[业务结果的会话可见性](../tests/test_payload_visibility.py)。

最终已执行的组合验证、平台差异及镜像证据记录于[实现状态](implementation-status.md)。
