# P5 群聊 Core 救援入口

## 1. 当前命令

当前 Host 内固定以下最小救援语法：

```text
#forest core status
#forest core cancel <run_id> --expect-checkpoint <checkpoint_seq> --reason "原因"
#forest core disable --expect-maintenance <maintenance_epoch> --reason "原因"
#forest core reload <component_id> --expect-revision <desired_revision>
```

`core status/cancel/disable` 声明 P0；`core reload` 是 P1。只有可信发言主体命中当前 Binding 管理员名单时才保留声明的 P0，否则请求拒绝或按普通 P1 查询处理。命令随当前 Kernel baseline 一次创建，不保留开发期 gate grammar、writer generation 或旧请求升级分派器。

Core 不是服务器终端、交互式 Shell 或第二套管理框架。它不接受自由 argv、脚本、路径、环境变量、密钥、跨 Bot 目标或插件注册命令。解析只依赖发行版内的冻结 token、封闭参数 schema 和固定动作映射；业务插件及其 Worker 不能覆盖这些 form。

## 2. 与普通命令共用权威

| Core form | 持久 command key / action | 实际提交点 |
|---|---|---|
| `core status` | `core.status` / 无写动作 | 当前 Binding、Bot runtime 与会话积压的有界元数据读取 |
| `core cancel` | `run.cancel` / `agent.cancel` | 原 Agent Run、checkpoint 与会话所有权守卫 |
| `core disable` | `bot.pause` / `bot.maintenance` | maintenance epoch CAS；只关闭 data admission |
| `core reload` | `module.reload` / `module.reload` | 指定 desired revision 的 config head CAS，再由 Fleet/Kernel 发布 |

别名在首次接收时规范化为上述既有 command key 和类型化参数。后续领取直接按持久 RouteGraph 摘要、Kernel Generation、command key、参数载荷和 priority 执行，不重新解析原群消息。Core 因此复用普通入口的幂等回执、来源父链、最终授权和业务冲突码，也不会生成并行的“救援状态”或绕过原资源锁。

`core cancel` 只终结指定 Run，不创建产品子 Agent，不清空后续触发。取消与模型最终提交仍由同一个 checkpoint/Run 事务裁决。`core disable` 是 Bot 级紧急停接：把 `data_admission` 设为 false 并递增 maintenance epoch，保留 `control_admission`，所以管理员仍能查询和恢复；它不是普通组件的持久 disable。`core reload` 与 `module reload` 完全相同，只复制已存在的不可变配置正文形成新 desired revision，回执不会冒充 effective 已发布。

## 3. P0 容量与权限

控制调度器按持久 priority 排序，并在并发上限大于一时为 priority 0 保留至少一个执行槽。普通 P1 查询占满其份额时，通过接纳资格预检的 `core cancel`、`core disable` 和 `core status` 仍可被领取；它们不能抢断已经进入提交阶段的事务，也不能撤销已经提交的外部效果。`core reload` 需要候选准备和排空，保持 P1，不能借 Core 前缀占用紧急槽。

所有 Core form 仍要求 Adapter 验证的真实发言主体位于当前 Binding 的 `admin_user_ids`。签名验证后，固定宿主解析器依据在线 Connection 冻结的 Binding epoch、生效配置修订与配置摘要生成短期资格证明；接纳事务锁定 Binding 后逐字段复核证明与作者、命令和当前修订，再把声明优先级、实际优先级、资格结果、策略名、Binding epoch 和 effective config revision 一起写成不可变事实。没有证明的 MessageStore 直调仍在事务内读取有效配置正文。每个执行事务再复核命令 claim/epoch/期限、Binding epoch、当前配置正文、Host incarnation、Bot policy 和目标业务版本；平台群主标记、昵称、引用消息、匿名身份和 Bot echo 都不能授权。`core status` 也完成相同最终检查，未授权只返回固定拒绝。接纳后撤权会拒绝操作；接纳后加权可以让请求执行，但不会追溯提升已经持久的 P1。

P0 表示调度容量，不表示更高权限或数据库锁优先级。两条 `core disable` 基于相同 maintenance epoch 时只有一条成功；cancel 与最终回答提交按 Run 守卫决胜；Core 与普通 reload 基于相同 config revision 时只有一个能生成下一 revision。失败方收到现有的 `MAINTENANCE_CONFLICT`、Run 终态或 `REVISION_CONFLICT`，不能以到达顺序覆盖胜者。

当前预检在取得 BotStore 连接前把普通消息、合格 P0 与明显无权 Shell 分入互不借位的 4/2/1 条并发通道。无权主体另受每桶突发 4 条、每 5 秒恢复 1 条的令牌预算；耗尽时在 raw payload 与 Inbox 前返回 429。资格缓存只决定入口容量，执行时仍重新鉴权。该隔离保护单个 Bot 进程主动发起的数据库工作，不能在数据库整体失效时提供救援；已获授权但失陷的管理员仍可能提交大量 P0，请求主体治理仍需后续策略。详见 [P5 Shell 接入容量](p5-shell-ingress-admission.md)。

## 4. 正文、恢复与验证

Core 参数从首次接收起进入 `shell.arguments` payload，不进入 Agent Context 正文；类型化管理输入、结果和 CONTROL delivery 继续沿用当前来源链和遗忘闭包。群回执只显示稳定目标、版本和状态，不复述 `reason`，也不输出配置正文、能力 grant、操作者主体或密钥。

相同 command ID 重试读取原业务回执，不能重复取消、递增 maintenance epoch 或创建配置 revision。`core reload` 还继承 module reload 的两阶段恢复：apply 已提交而外层回执尚未写入时，Host 核对实际 apply 输入、结果、来源和 revision 后补齐回执。

验证覆盖当前 Core grammar 与 priority-admission manifest、非法参数不保留、匿名/伪造主体降级、接纳后加权与撤权、优先级依据不可变、真实 PostgreSQL P0 status/disable、普通控制占槽时的 Core cancel、Core reload 完整 CONTROL 回执、Web/config CAS，以及旧 Kernel Generation 失去路由后不能提交。Core 当前不提供主机进程、部署数据库或存储灾难恢复能力；这些必须由单独的受控本机恢复与 ops profile 协议完成。
