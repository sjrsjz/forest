# v34 隔离候选：假 OneBot 在线与 Worker Host 自热重载（2026-10-17）

基于 `forest:runtime-95474cba` 的独立 `forest-accept-v34` 部署，沿用已成功离线供应的 `bot/accept-v34-offline-20261017`。在忽略目录 `.cache/accept-v34` 中增加临时假 OneBot 侧车，与候选 Supervisor 共享**候选容器自己的**网络命名空间，只监听 `127.0.0.1:3001`。该端点仅对 `get_login_info` 和 `get_status` 返回成功；其他 OneBot 动作明确失败。Bot 的冻结 Bootstrap Profile 原先就指向这个地址。侧车未连接树莓派 QQ，未提供消息注入或发送能力。

经正式 `deployment.bot-catalog` 管理动作将 Bot 期望态从 OFFLINE 设为 ONLINE，Supervisor 观察到 STARTING 后约 50 秒进入 ONLINE。Bot 管理作用域报告 RuntimeGraph r1 ACTIVE、22 个组件；独立 Bot PostgreSQL 库也记录 r1 ACTIVE、22 个成员。Provider 和 Embedding 仍使用占位凭据，过程中没有模型调用。

通过在线 Bot 的 `component.worker-host` 包管理动作 `module.reload`，以 r1 为期望修订重载 `component.worker-host`。持久请求 `reload:464cd41bc329adaf213b70db454c243b` 依次观察到 QUEUED、RUNNING、SUBMITTED，内核结算为 SUCCEEDED；Bot 管理作用域报告 RuntimeGraph r2 ACTIVE、22 个组件。Worker Host 的 Generation 已变化；SQL 比对显示 21 个组件换代，`kernel.payload` 保留原 Generation。请求状态能在新代 Worker Host 上继续查询，说明旧代退役没有丢失结果。

随后经正式 Bot 意图端口改回 OFFLINE，观察态从 ONLINE 收敛到 OFFLINE，r1、r2 两张图在独立库中均为 STOPPED；假 OneBot 日志只记录连接开启/关闭，没有发送动作请求。侧车现已停止，候选 Supervisor 和 PostgreSQL 仍运行，现役 v33 Supervisor 在整个过程保持 healthy。一次简单 TCP 端口探测曾让假 WebSocket 服务记录无效握手，这是探测本身造成的侧车日志，不是 Bot 连接错误。

这次验收覆盖了最新源码镜像的独立 Supervisor、真实 PostgreSQL、Bot ONLINE/OFFLINE 生命周期和 Worker Host 自热重载；仍不代表真实 QQ 回复、长时间运行、多模态记忆成本或生产切换验收。
