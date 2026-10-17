# v36 隔离候选：Delivery 观察账本的完整运行链（2026-10-17）

从提交 `6f3c16c7` 构建 `forest:runtime-6f3c16c7`（镜像 ID `sha256:1c08bf8909b0239bc92e1429e5bc5bf168abf842b52eaf45652edab105cc1905`），以独立 `forest-accept-v36` Compose 和 Bot 库 `forest_bot_d2121ba1a59336e1e93b2d72` 供应 `bot/accept-v36-offline-20261017`。正式存储申请和供应任务均为 `SUCCEEDED`；Bot 经本地假 OneBot 上线后运行图有 22 个成员，Delivery 私有 SQL 3.1.4 和 Kernel Effect 私有 SQL 1.0.0 的 schema bundle 均为 READY。管理端口仅绑定本机 `127.0.0.1:18108`。

假 OneBot 仅允许账户状态探针，对一切发送返回明确失败；聊天模型和 4096 维嵌入端点也只在候选网络内返回假响应，嵌入 profile 保持冻结。向第二假群注入一条显式 `@` 后，假模型收到 1 次请求，假 OneBot 收到并拒绝 1 次 `send_group_msg`。同一个正式 `delivery.observe` UnitOfWork 在两个独立模块的账本留下配对观察：Kernel Effect 为 `NOT_APPLIED / DISPATCHING→NOT_APPLIED / accepted=true`，Delivery 为 `REJECTED_NOT_APPLIED / DISPATCHING→REJECTED / accepted=true`；两行通过 effect observation operation ID 精确关联。Delivery intent 和 part 为 `FAILED / PLATFORM_REJECTED`，transport attempt 为 `REJECTED / PLATFORM_REJECTED`，attempt_count 为 1。

随后通过 `component.worker-host` 的正式 `module.reload` 动作重载 `delivery`，内核回执 `SUCCEEDED`，图 r4→r5，22 个成员仍在 ACTIVE 图中。变化的 Generation 仅为 `adapter.onebot`、`agent.runtime`、`conversation.scheduler`、`delivery`、`media.capture`、`shell.command`。重载后 Delivery 观察行仍为 1 条且 accepted，part 仍 FAILED，transport attempt 总数及假端点拒绝动作数仍为 1，没有重发。

Bot 已通过正式运行意图退回 OFFLINE，假 OneBot、Supervisor 和 PostgreSQL 均已停止，专用数据卷保留。现役 v33 保持 healthy；树莓派旧 AliceBot inactive，NapCat active，8080 仅保留来自 Forest 的一条连接。本次未发送真实 QQ 消息、未调用付费模型，也没有把含假向量的 v36 Bot 绑定真实 QQ。晚到冲突在隔离 PostgreSQL 的 SQL 参与者中已验证，真正提交晚到证据的调和入口尚未做整链验收；真实配置 Bot 的交接仍待完成。
