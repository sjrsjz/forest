# OneBot CQ 与账号识别验收（2026-10-18）

设计见 [22 OneBot CQ 呈现](22-onebot-cq-presentation.md)。账号识别修正提交 `545e78f8` 已通过官方包发布、预检和热重载进入 v40：Agent 5.1.0、Adapter 2.9.0，图修订 3。每条消息携带接收账号和明确的 `targets_assistant`，账号不是人格中的硬编码。该修改保留了原 Bot 和数据库。

CQ 候选包为 Agent 5.2.0、Adapter 3.0.0、Codec 2.0.0、Delivery 0.7.0。显式平台正文支持 at/reply/face/受管 image；普通 text 保持字面语义。解析在实体解码前进行，解码仅一次，最后发送原生消息数组。图片像素仅在宿主 Payload 物化，不穿越 Worker IPC；发送账本绑定准确的 wire Payload ID 与摘要，删除来源会撤销派生 wire。

本地定向检查 68 passed，覆盖单次转义、普通文本、畸形 CQ、受管图片引用、512 KiB 图片仅返回引用、来源撤回、相同摘要但不同 wire ID 拒绝以及文本模型无像素投影。Ruff 与差异检查通过。

隔离 PostgreSQL 的正式 BotHost 正常 CQ 场景通过：模型最终正文包含 reply、at、face、受管 image 和转义后的 CQ 字面文本；模拟 OneBot 收到有序数组及准确图片字节。缺失图片的场景最初暴露 Delivery 状态机漏列 `CLAIMED → FAILED`；补齐基线后单独复验通过（1 passed in 86.14s），意图 FAILED/ONEBOT_MEDIA_UNAVAILABLE、没有传输 attempt、没有平台发送。正常原生工具与跨 Run 推理回放场景此前也通过。以上集成使用模拟模型和 OneBot，不能替代真实 QQ 呈现验收。

最终镜像 `forest:runtime-cq-presentation-candidate` 的 ID 为 `sha256:d9334b600d8bcc4eab9a6b30829ef77bb937670521b12646f4f0fe3b914d8b18`。Delivery 新 SQL 基线 3.4.0 的准确 SHA-256 为 `e3a4c5ddc33a63d727ffa55790c076e2423ff1dcc8dc14379c189a4f38fd658c`。开发期通过新库供应匹配包图，无旧 wire DTO 分支或数据库迁移。

CQ 实现提交为 `50cccc56`。20:55 CST 已完成 v42 交接：先通过正式意图令 v40 OFFLINE/OFFLINE，经 SSH 确认树莓派 8080 连接归零，再停止旧 Supervisor（退出码 0）、接回原管理端口并令 `forest-main-v42` ONLINE。现役容器 `forest-v42-cq` healthy、图修订 1、22/22 ACTIVE、22/22 schema bundle READY，树莓派仅一条来自 Forest 主机的连接。管理地址仍为 `http://127.0.0.1:18131`，管理员设置保持不变。

本轮使用新 Deployment/Host/Bot 数据库及独立包卷，保留 v40 历史库，未迁移旧记忆。冻结 VL 模型身份、DeepSeek thinking/high、账号 `3605196653`、许可群 `933179609` 与三份凭据沿用原配置。v41 是从未连接 QQ 的旧候选，已停止。

v40 只读观察停止于 20:52 CST、第 26 次采样，最后样本零违规，约持续 2.1 小时；其中已观察到许可群 1 次 ANSWERED/COMPLETED/SENT 闭环，但属于旧版，不能用来证明 CQ 呈现。新观察容器 `forest-v42-soak-20261018` 每 300 秒只读采样、限制 0.25 CPU / 256 MiB，20:55:30 CST 首样本 HTTP 200、22/22 ACTIVE、零违规。新版真实 QQ CQ 回复与完整 24 小时观察尚未验收；未额外发送群消息来代替用户测试。
