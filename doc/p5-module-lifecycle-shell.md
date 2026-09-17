# P5 组件生命周期 Shell 与重载协议

## 1. 范围

Module Lifecycle 模块向当前 Shell registry 提供：

```text
#forest module list
#forest module status <component_id>
#forest module reload <component_id> --expect-revision <desired_revision>
```

`modules` 是 `module` 的确定性别名。命令接纳时冻结当前 RouteGraph、模块 Generation、参数 schema 和规范参数；恢复时不重新解析原消息。固定 `core reload` 复用这里的同一 reload 权威。

同一 registry 还提供：

```text
#forest module validate <component_id> --expect-config-revision <desired_revision> --expect-state-revision <module_revision>
#forest module enable <component_id> --expect-state-revision <module_revision> --reason "..."
#forest module disable <component_id> --expect-state-revision <module_revision> --reason "..."
```

这里的 `Agent` 仍然只指每个 `(bot_id, conversation_id)` 的唯一会话 Agent。模块命令属于独立 CONTROL 通道，不创建 Agent、子 Agent 或会话运行。

## 2. 查询投影

list/status 从 Bot 事实库关联 `bot_config_heads`、当前路由、当前 generation 和最近 lifecycle operation，只返回：

- `component_id`、desired/effective revision；
- desired/effective package digest；
- generation、route epoch、data/control admission；
- generation/lifecycle phase 和错误码。

查询不 hydrate 组件配置正文，不向群里输出 capability grants、主体、密钥或 operation 详情。状态按持久事实投影为 `UNCONFIGURED/PENDING_ENABLE/ACTIVE/PENDING_DISABLE/DISABLED/DEGRADED`；Bot 维护暂停由 Bot 全局 admission 门执行，不改写健康组件的 route，因而不会把已发布组件误报为故障。进程内 Worker 的瞬时存活信息仍由 Web runtime 面板显示。

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

一条命令派生稳定的三个 operation identity：包装操作、plan 子操作和 apply 子操作。进程内 opaque delegation 只允许这条命令调用两种固定配置子动作，外部参数不能选择 action、operation identity 或配置值。每个子操作继续写不可变输入/结果载荷和控制回执。

Host 在 plan 前从 revision N 读取值，但 `config.plan` 会再次检查 head；apply 又在目标 head 上执行最终 CAS。因此 Web 保存、另一个群命令和同一命令重试遵循相同裁决：

- 两个入口同时基于 N 时，最多一个把 head 推进到 N+1；
- 失败方得到 `REVISION_CONFLICT`，不会覆盖胜者或产生第二个 revision；
- plan 已提交但 apply 尚未提交时，重试使用相同 operation 读取原回执；
- apply 已提交而包装回执尚未写入时，重试先读取 apply 回执，再补齐包装回执；
- 包装回执完成后再次投递只返回同一结果，不再推进 revision。

配置正文若已经进入遗忘终态，reload 返回 `CONTROL_CONTENT_REVOKED`，不会从行内旧列、effective 值或默认值恢复正文。

## 5. WebUI

组件配置卡片提供“验证”“启用”“停用”“重载”。页面冻结当前 config/state revision；重载使用稳定的 plan/apply 幂等键，启停使用独立 state CAS 与稳定幂等键。网络确认丢失时可以读取原回执，修订冲突会丢弃过期草稿并刷新配置。页面只把“接纳”显示为等待生命周期切换，最终状态仍以 effective revision、route 和 runtime 组件状态为准。

## 6. 代际围栏与验证

命令请求保存接纳时的 RouteGraph 摘要、owner component/generation、schema digest 和规范参数。旧 Generation 被撤下后不能领取新命令；已接纳命令只有在原代仍被允许排空时完成，否则以 `GENERATION_RETIRED` 终结。Forest 未发布基线不维护 gate 版本链、writer-generation 数据库升级或版本专用 registry/helper。

专项验证覆盖当前 schema、真实 PostgreSQL reload/validate/enable/disable、双 CAS 幂等重放、Shell 完整 CONTROL 回执、Web 与 Shell 同 revision 竞争、正文不进入群输出、暂停态投影、issuer 最小权限，以及旧 Generation 撤路由后永久失去提交权。

## 7. 配置修订与启停修订分离

组件配置历史的不可变正文固定为 `package_digest/config/grants`。启停状态不能向旧配置 payload 追加字段，也不能借一次 reload 暗示“启用”。Bot 库为每个组件维护独立的模块状态头和不可变修订：

- `desired_state_revision`：管理员最后接纳的启停意图；
- `effective_state_revision`：Kernel 已完成生命周期发布的启停意图；
- 修订正文只有 `enabled`、`reason`、提交主体、策略版本和来源 operation；
- 模块首次配置必须在同一 plan/apply 中创建显式状态修订；不存在“缺行即启用”的兼容默认值。

因此配置和启停形成两个正交的 CAS 轴。禁用期间仍可编辑或 reload 配置，Fleet 不会加载新 Worker；再次启用时，Fleet 使用最新 desired 配置和最新 desired 启停修订共同创建 activation。一次成功的 INSTALL 发布同时推进配置 effective 和模块 effective；一次 DISABLE 发布只推进模块 effective，保留最后成功的配置 effective 作为重新启用失败时的已知可运行版本。

命令接纳只修改 desired facts，不直接创建 generation、冻结 route 或等待 Worker。回执必须明确显示 `ACCEPTED`，并同时返回 desired/effective 配置修订与 desired/effective 启停修订。只有 effective 启停修订追上且 route 已空，状态才是 `DISABLED`。

## 8. validate 的确定性快照

`module validate` 验证目标 Bot 当前的完整 desired 快照，不接收群聊提供的路径、包正文、配置 JSON 或 capability grants。命令在同一事务中冻结并复核：

- 目标 `desired_config_revision` 及其不可变 payload；
- `desired_state_revision` 和 enabled 值；
- package digest 是否在 Host 已验证的 artifact registry 中；
- 配置 schema 与请求能力是否完全匹配；
- 所有 desired-enabled 组件形成的服务提供者、契约版本、必需依赖、环和重复服务；目标即使当前 disabled，也按一次 prospective enable 纳入快照。

验证器返回包摘要、配置/状态修订、`VALID` 或稳定错误码，以及必需依赖组件 ID；不返回配置正文或 grants。读取完成后再次检查两个 head，任一 head 变化均返回 `REVISION_CONFLICT`，不会把混合时刻的结果描述成一个快照。validate 不导入代码、不启动 Worker、不访问网络。

## 9. 禁用生命周期

Fleet 只有在全局 desired 依赖图允许后才调和 DISABLE。仍启用的组件若有必需依赖最终指向目标，目标保持原 effective Worker，错误记录为 `DEPENDENCY_IN_USE`。可选依赖不阻止禁用；依赖者下一次重载后以 degraded dependency 运行。`adapter.onebot` 不能从它承载的群 Shell 禁用，避免在成功回执发出前切断控制通道；WebUI 可在具有其它恢复入口时提交。

DISABLE 使用与 INSTALL 相同的组件 resource lease、route epoch、Host lease 和阶段机，但 lifecycle operation 的 kind 为 `DISABLE` 且没有 candidate generation：

```text
bind module activation(state_revision=S, enabled=false)
  -> begin DISABLE(expected_route_epoch=E)
  -> route data/control admission=false；旧 generation ACTIVE->QUIESCING
  -> 等待直接调用和预留依赖调用全部排空
  -> Worker.quiesce；数据库阶段 FROZEN->QUIESCENT
  -> 单事务发布：旧 generation RETIRED；route generation_id=NULL；route_epoch=E+1
     -> lifecycle COMMITTED
     -> module effective_state_revision=S
  -> 停止并 dispose 已退役 Worker
```

发布事务在改变 route 前复核 module desired head、activation、Host incarnation、operation lease 和无存活调用。空 route 是明确的停用事实，不复用“Worker 崩溃后 admission=false”的含糊状态。组件持久 state、事件订阅、待处理事件和持久任务均保留；调度器在组件停用期间不消费该组件的新投递，重新启用并发布 generation 后继续。

冻结后、提交前失败时，abort 只在 resource token、Host 和 route epoch 仍匹配时恢复旧 generation 的 ACTIVE/admission，并保留旧 effective 启停修订。提交确认丢失时先读 lifecycle operation；若已 COMMITTED，Host 必须按停用完成清理，不能恢复旧 Worker。Host 崩溃后，新 Host 可以接管过期 operation；若 desired 仍为 disabled，它从数据库 route 和 live-call facts 继续禁用，若意图已改变则遗弃旧 operation 并按最新 revision 调和。

## 10. 启用生命周期

enable 只接纳 `false -> true` 的模块状态修订。Fleet 以最新 desired 配置执行完整 package/config/capability/依赖验证，绑定配置 activation 与模块 activation，然后调用普通 `Kernel.install`。候选准备失败、状态迁移失败、健康检查失败或依赖计划变化时，空 route 和旧 effective disabled 状态保持不变。INSTALL 提交时同一事务把 route 指向 READY candidate，并同时推进两个 effective head；随后才开放 Worker transport admission。

已处于相同 desired 状态的重复 enable/disable 返回 `MODULE_STATE_UNCHANGED`，要求调用者先刷新，而同一个 operation identity 的重投只读取原回执。两个群、WebUI 与后台自动化基于相同 state revision 竞争时最多一个修订成功，其他请求返回 `REVISION_CONFLICT`。

## 11. 调和顺序与可观察状态

Fleet 每轮先读取全部配置和模块状态，构建一次不可变 desired 快照，再按依赖拓扑执行：启用/安装按 provider 到 caller，禁用按 caller 到 provider。读取方先对全部配置 head 取得共享锁，再读取模块状态 head；启停写方必须先取得对应配置 head 的排他锁，因此不能在快照中途插入或改写模块状态。若轮次执行期间任一相关 head 改变，activation 绑定或最终发布会以 `CONFIGURATION_SUPERSEDED` / `MODULE_STATE_SUPERSEDED` 失败，下一轮从新快照开始。任何 desired-enabled 组件的包、配置或依赖无法验证时，本轮依赖图标记为不完整：仍可恢复或尝试启用已知组件，但所有尚未生效的停用保持原 Worker 与 route，记录 `DEPENDENCY_GRAPH_INCOMPLETE`，直到完整图可重建。已经 effective disabled 的幂等收敛不受此限制；待停用组件的旧包工件若已经丢失，Kernel 仍以当前加载依赖和 route 作为最终安全屏障完成停用。

模块状态投影扩展为：

- `PENDING_ENABLE`：desired enabled，effective disabled 或 route 尚未发布；
- `ACTIVE`：两个 desired/effective head 一致，route 指向当前 ACTIVE generation 且 admission 符合 Bot maintenance；
- `PENDING_DISABLE`：desired disabled，effective 仍 enabled；
- `DISABLED`：desired/effective disabled 且 route generation 为空；
- `DEGRADED`：事实与预期不一致，例如 effective enabled 但 generation 已故障；
- `UNCONFIGURED`：没有配置修订。

Shell status、WebUI 和审计均显示两个 revision 轴、最近 lifecycle kind/phase/error。进程内 `worker_state` 只是附加观测值，不能覆盖持久状态结论。

## 12. WebUI 与验证矩阵

WebUI 的组件卡片提供“验证”“启用”“停用”“重载”。按钮冻结当前 config/state revision；停用必须填写原因并显示依赖影响。提交后页面显示“已接纳，等待生命周期发布”，轮询到 effective state、route 和 generation 一致才显示完成。冲突会清除旧草稿并重新读取；网络确认丢失使用同一幂等键读取原回执。

实现验证至少覆盖：双 CAS 竞争、禁用中的迟到父调用、冻结后的依赖调用、依赖者阻止 provider 禁用、可选依赖、禁用期间编辑配置、启用同时推进两个 effective head、发布确认丢失、冻结后 abort、Host 接管、Shell/Web 竞争、冻结 Generation 恢复与失权、正文遗忘、停用期间事件/任务不丢失，以及浏览器端完整启停往返。
