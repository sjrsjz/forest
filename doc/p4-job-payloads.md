# P4 Job 定义载荷与授权撤销

状态：已实现。对应 Bot 迁移 `060_schema_writer_v3.sql`、`061_job_payloads.sql`。

## 1. 目标与边界

会话定时任务会长期保存管理员编写的 `name`、`prompt` 和一份可代替管理员触发会话的 delegation。它们不能继续留在 `bot_job_plans.values`、`bot_job_revisions.values` 或 `bot_job_revisions.delegation` 裸 JSONB 中，否则来源遗忘只能清除消息和 Agent 载荷，却无法清除任务副本。

本切片只改变会话 Job 的定义存储和授权读取。组件持久 Job 是另一种内核工作单元，不包含群聊 prompt，也不创建或调度产品 Agent。

## 2. 数据布局

计划和修订的完整 settings 使用 `control.config` payload，身份分别为：

```text
family   = job
kind     = PLAN | REVISION
resource = job_id
identity = plan_id | revision
```

修订的完整 delegation 使用第二个独立 payload：

```text
family   = job.delegation
kind     = REVISION
resource = job_id
identity = revision
```

三个原 JSONB 列在迁移后必须为 SQL `NULL`。业务行只保留 payload ID、payload digest 和闭合的运行元数据。settings 元数据包含开关、目标会话、日历、补跑策略、并发数、到期时间和总运行次数，不含 `name` 或 `prompt`；delegation 元数据只包含 issuer、key/policy revision、delegation epoch 和到期时间，不含管理员主体或 principal。

合法的 JSON `null` 与正文不可用仍由 payload state 区分，不使用旧列或默认值回退。

## 3. 来源证明链

`job.plan` 在持有 Job head 锁后冻结真实控制输入。PLAN settings 以该输入为父载荷；计划回执以 PLAN settings 为父载荷。延迟约束要求同一事务登记 `bot_control_value_sources`，并核对操作、主体、issuer、计划摘要和完整 settings。

`job.apply` 先读取并证明实际 PLAN settings，再冻结 apply 输入。REVISION settings 和 delegation 都以 PLAN settings 与 apply 输入为父载荷；apply 结果同时以这两个修订载荷为父载荷。修订插入守卫重新读取全部不可变正文并核对：

- Job、计划、预期修订和管理主体一致；
- revision 与 delegation epoch 正好推进一次；
- revision settings 与计划 settings 完全相同；
- delegation ID、能力、算法、到期时间和 authority revisions 来自冻结票据；
- apply 输入、结果以及六条直接父子边完整存在。

因此结果回执不能被当作正文备份，计划也不能引用另一个 Job 或另一个管理员的内容。

## 4. 运行时与并发

调度器在持有 Job head、occurrence 和 Bot authority 锁后，才取得 memory deletion epoch 并 hydrate settings/delegation。正文或任一实际父载荷不可用时，不生成新 occurrence；竞态中已经选中的 occurrence 会被取消。

领取候选后会在行锁内重新检查状态、租约、到期时间、delegation epoch 和 `max_overlap`。两个并发 dispatcher 即使在第一次查询中看见同一个 QUEUED occurrence，也只有一个能推进 lease epoch。

数据库的 occurrence/trigger 授权函数只读取不可变 header、body 存在性、摘要、父依赖和 suppression 快照，不获取 memory clock 行锁。这样不会形成“删除时钟 → Job head”与管理写入“Job head → 删除时钟”的锁序反转。若授权检查先于遗忘提交，则领取线性化在遗忘之前；遗忘完成后，后续检查返回 false，reconcile 取消尚未被会话 Agent 领取的触发。

## 5. 遗忘和管理读取

遗忘 Job 来源会沿不可变父链传播到 PLAN、REVISION settings、delegation 和相应控制回执。`scrub` 删除正文但保留稳定身份、摘要、计划事实、occurrence 状态和审计关系。

Web/Shell 的 Job 读取在正文不可用时继续返回任务与 occurrence 元数据，同时返回：

```json
{
  "values": null,
  "delegation": null,
  "values_payload_state": "UNAVAILABLE",
  "delegation_payload_state": "UNAVAILABLE"
}
```

页面或调用方不得从旧计划回执、审计记录、`definition_digest` 或运行元数据重建 prompt。新的手动运行、定时扩展、领取、重放和待处理 trigger claim 都会拒绝该定义。

## 6. 升级与 writer fence

迁移 060 把 `forest.writer_generation` 提升到 3。升级仍要求旧 HostLease 已停止；gate、DDL、正文导入、约束和迁移账本在同一事务提交。迁移完成后，generation 2 的既有连接和重连都不能再次取得或修改 HostLease。

迁移 061 对旧计划和修订按稳定键分批导入。能证明原 apply 操作时使用其结果 payload 作为父载荷；无法证明现代操作时使用 041 已登记的保守 legacy source。已经撤销或 scrub 的父载荷只产生不可恢复的 header，不会短暂重插旧 prompt 或 delegation。旧行的原值逐字进入 payload，迁移不调用模型、嵌入或外部网络。

## 7. 验证

`tests/test_job_payloads.py` 验证正文列清空、闭合元数据、双 payload 身份、Web hydrate、来源遗忘、领取撤销、物理 scrub、元数据不可变和并发单租约。`tests/test_job_writer_fence.py` 从真实 059 schema 升级并验证 generation 2 连接永久失权。包含原 Job、Scheduler、Web、审计、001–P3 升级、配置依赖、writer fence 和所有既有配置载荷族的 135 项组合回归通过；最终镜像也通过独立 Compose + Edge 的 Job 回复、投递、禁用和重启恢复流程。

备份、恢复与 WAL 保留策略仍需以这些 payload header/body、来源、suppression、删除账本和 writer generation 为一个一致快照实现；本切片本身不提供备份按钮或跨数据库迁移。
