# v35 隔离候选：明确拒绝的终态与 Delivery 热重载（2026-10-17）

以已提交源码 `46a23592` 构建镜像 `forest:runtime-46a23592`，镜像 ID 为 `sha256:7a0b307a42805f9b2348ac00d42544eab63216c45f6b8b121e4927077e2e68ca`。新建 `forest-accept-v35` 专用 PostgreSQL、包卷、管理端口 `<MANAGEMENT_BIND_ADDRESS>` 和 Bot `bot/accept-v35-offline-20261017`；没有复用 v33/v34 的数据库。正式管理端口的存储申请、引导配置和供应任务均结算，Bot 起初为 `READY / OFFLINE`。其独立 Bot 库 `forest_bot_afc1a14b5e62d45eddda81e5` 中 22 个 schema bundle 全为 `READY`，Adapter 为 SQL 2.2.0、Delivery 为 SQL 3.1.2、Memory 为 SQL 3.99.0。

在只允许读取账户状态、对所有发送动作明确返回失败的本地假 OneBot 上，将 Bot 置为 ONLINE、22/22 组件 ACTIVE。通过正式配置端口仅将嵌入 endpoint 改为固定向量假端点（冻结 VL profile 不变）、Provider 改为假聊天端点，并将第二个假群纳入触发许可。向假群注入一条显式 `@`：持久入站和触发各 1 条，Agent Run `COMPLETED`，假模型记录 1 次请求。假 OneBot 对 `send_group_msg` 返回失败后，Delivery intent 和 part 都是 `FAILED`，transport attempt 是 `REJECTED / PLATFORM_REJECTED`，attempt_count 为 1；假端点日志也只有 1 次发送拒绝动作。

随后通过 `component.worker-host` 的正式 `module.reload` 动作重载目标 `delivery`。请求的内核结果为 `SUCCEEDED`，运行图 r4→r5，22 个成员仍在 ACTIVE 图中；变化的 Generation 仅为依赖闭包内的 `adapter.onebot`、`agent.runtime`、`conversation.scheduler`、`delivery`、`media.capture`、`shell.command`。重载后原失败意图仍是 `FAILED / REJECTED`，transport attempt 总数仍为 1，没有重放发送。

验收结束后正式运行意图和观测都回到 OFFLINE；假 OneBot、v35 Supervisor、PostgreSQL 容器已停止，卷和账本保留。v34 隔离候选亦已停止。现役 v33 保持 healthy，树莓派旧 `alicebot.service` 已按此前授权停用，NapCat 仍为 READY，8080 只保留一条来自 Forest 的连接。本试验没有真实 QQ 出站、付费模型调用或真实 VL 嵌入；v35 含固定假向量和假配置，不能改绑真实 QQ。v33 仍运行旧工件，正式交接须在新的真实配置 Bot 与独立库上进行。

在上述镜像验收之后，源码 Delivery 进一步加入 `retry_ready_at`：可重试的确定未发送按 attempt 次数做 1、2、4……最多 60 秒的持久退避，claim 只取到期的 part；明确拒绝的终态不变。新源码版本为 Delivery 0.4.6 / SQL 3.1.3，单独在 v35 隔离 PostgreSQL 中用干净测试库验证了“未到期不能再 claim”。该版本尚未进入 v35 镜像或运行图，不能把先前的端到端与热重载结果写成 3.1.3 的验证。

后续源码 Delivery 0.4.7 / SQL 3.1.4 接通 `delivery_observations`：观察参与者消费同一 UnitOfWork 的 Effect 回执，拒绝不匹配的 attempt/operation/状态/证据；把接受结果和与已终结 attempt 冲突的晚到结果分别写成不可变事实，只有接受且需要转换状态时才推进 part/intent。v35 隔离 PostgreSQL 的新干净测试库验证了拒绝观察 `DISPATCHING→REJECTED`、晚到冲突 `REJECTED→REJECTED` 且 intent 仍 FAILED，以及可重试路径的退避。该版本也未装进先前 v35 镜像；尚未通过完整 UnitOfWork 与真实晚到调和入口验收。
