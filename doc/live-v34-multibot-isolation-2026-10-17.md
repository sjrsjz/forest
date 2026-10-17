# v34 隔离候选：双 Bot 并存与独立存储（2026-10-17）

在同一个 `forest-accept-v34` 生产 Supervisor 中，经正式 Secret、Host 存储请求、Bootstrap Profile 和供应 Job 链路，新增 `bot/accept-v34-second-20261017`。其供应 Job 结算 `SUCCEEDED`，Bot 为 `READY/OFFLINE`。原 Bot 为 `bot/accept-v34-offline-20261017`。两个 Bot 使用不同的冻结 OneBot 身份 `10001`、`10002`，分别指向候选容器本机的假端点 `127.0.0.1:3001`、`:3004`；假端点只允许登录状态读取，拒绝所有发送动作。

Host 实际创建的两个物理 PostgreSQL 库分别为 `forest_bot_b19cb4e64b9d91ca6d14b7c6` 和 `forest_bot_8265c94d06e5f434d084113d`。两库各有 22/22 个 READY schema bundle。经部署 Bot Catalog 的正式意图动作，先将原 Bot 重新 ONLINE，再在原 Bot 仍运行时启动第二个。管理作用域同时报告原 Bot Graph r4 ACTIVE、22 个模块，以及第二个 Bot Graph r1 ACTIVE、22 个模块；假 OneBot 同时记录账号 `10001` 与 `10002` 的连接。

先将原 Bot 改回 OFFLINE 并等待观察态收敛，第二个 Bot 仍为 `READY/ONLINE`、Graph r1 ACTIVE、22 个模块。随后第二个 Bot 也经正式意图退役到 `READY/OFFLINE`，停止假端点。最终两库分别有 3 张和 1 张 STOPPED 图，均无 ACTIVE 图；假端点日志中发送动作请求为 0。候选 Supervisor 与现役 v33 Supervisor 保持 healthy。两 Bot 并存期间候选 Supervisor 约占 1.0 GiB，退役后约 435 MiB。

这证明生产组合根能同时托管两个独立 Bot、为各自分配物理存储与管理作用域，并独立调整运行意图。尚未注入消息或建立记忆内容，因此**不以此证明**各 Bot 的多主干记忆内容隔离、跨群联想、真实 QQ 回复或长时负载。
