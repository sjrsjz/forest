# P5 组件生命周期 Shell 与重载协议

## 1. 范围

当前 `shell-gate-v8` 提供：

```text
#forest module list
#forest module status <component_id>
#forest module reload <component_id> --expect-revision <desired_revision>
```

`modules` 是 `module` 的确定性别名。新请求固定记录 v8；已经排队的 v2–v7 请求继续按各自 gate 解释，不会被新语法重新解析。`module validate/enable/disable` 和 `core module` 救援入口尚未实现，不能用本协议冒充。

## 2. 查询投影

list/status 从 Bot 事实库关联 `bot_config_heads`、当前路由、当前 generation 和最近 lifecycle operation，只返回：

- `component_id`、desired/effective revision；
- desired/effective package digest；
- generation、route epoch、data/control admission；
- generation/lifecycle phase 和错误码。

查询不 hydrate 组件配置正文，不向群里输出 capability grants、主体、密钥或 operation 详情。状态按持久事实投影为 `UNCONFIGURED/PENDING/ACTIVE/INACTIVE`；进程内 Worker 的瞬时存活信息仍由 Web runtime 面板显示。

## 3. reload 的权威路径

reload 不是“停止旧 Worker 后再试一次”的旁路。Shell 只授权“复制当前指定 desired revision”，随后顺序调用 Web 配置页面使用的同一 `BotControl.plan` 和 `BotControl.apply`：

```text
群命令(module.reload, expected=N)
  -> 在目标 Bot 库复验当前 Binding 管理员、policy、Host 和命令 lease
  -> 锁 config head，要求 desired_revision=N，读取不可变 revision N
  -> config.plan(expected=N, source_revision=N, values=Host 实际读取值)
  -> config.apply(expected=N, plan_id, plan_digest)
  -> config head CAS: N -> N+1
  -> 记录 module.reload 包装回执，并继承实际 apply 回执
  -> Fleet 发现 desired=N+1
  -> bind_activation -> Kernel.install -> lifecycle commit
  -> 发布成功后 effective_revision=N+1
```

接纳回执必须同时显示新的 desired 和仍在生效的 effective，不能把 `config.apply` 成功写入误报为 Worker 已经切换。命令事务不创建 generation、不取得组件 resource lease、不修改 route，也不停止旧 Worker。候选准备、冻结、静默、状态迁移、路由发布、旧代排空和失败保留旧代仍完全由既有生命周期协议负责。

## 4. 幂等、并发与恢复

一条命令派生稳定的三个 operation identity：包装操作、plan 子操作和 apply 子操作。进程内 opaque delegation 只允许这条 v8 命令调用两种固定配置子动作，外部参数不能选择 action、operation identity 或配置值。每个子操作继续写不可变输入/结果载荷和控制回执。

Host 在 plan 前从 revision N 读取值，但 `config.plan` 会再次检查 head；apply 又在目标 head 上执行最终 CAS。因此 Web 保存、另一个群命令和同一命令重试遵循相同裁决：

- 两个入口同时基于 N 时，最多一个把 head 推进到 N+1；
- 失败方得到 `REVISION_CONFLICT`，不会覆盖胜者或产生第二个 revision；
- plan 已提交但 apply 尚未提交时，重试使用相同 operation 读取原回执；
- apply 已提交而包装回执尚未写入时，重试先读取 apply 回执，再补齐包装回执；
- 包装回执完成后再次投递只返回同一结果，不再推进 revision。

配置正文若已经进入遗忘终态，reload 返回 `CONTROL_CONTENT_REVOKED`，不会从行内旧列、effective 值或默认值恢复正文。

## 5. WebUI

组件配置卡片增加“重载当前组件”。页面冻结当前 desired revision 和值，使用稳定的 plan/apply 幂等键提交同一两阶段配置协议；网络确认丢失时可以读取原回执。修订冲突会丢弃过期重载草稿并刷新配置。页面同样只把“接纳”显示为等待生命周期切换，最终状态仍以 effective revision 和 runtime 组件状态为准。

## 6. 版本围栏与验证

Bot 迁移 066 把 writer generation 提升为 6。generation-5 Host 不认识 v8 命令，迁移提交后不能重新取得 Host lease；066 checksum 绑定 `migration_v6.py`、`shell_registry_v8.py` 和 `shell_payloads_v8.py`，同时不改变 064/065 已发布 checksum。

专项验证覆盖 v8 schema/解析、v2–v7 兼容、真实 PostgreSQL reload/幂等重放、Shell 完整 CONTROL 回执、Web 与 Shell 同 revision 竞争、正文不进入群输出，以及 generation-5 writer 永久失权。
