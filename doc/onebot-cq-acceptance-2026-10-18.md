# OneBot CQ 与账号识别验收（2026-10-18）

设计见 [22 OneBot CQ 呈现](22-onebot-cq-presentation.md)。账号识别修正提交 `545e78f8` 已通过官方包发布、预检和热重载进入 v40：Agent 5.1.0、Adapter 2.9.0，图修订 3。每条消息携带接收账号和明确的 `targets_assistant`，账号不是人格中的硬编码。该修改保留了原 Bot 和数据库。

CQ 候选包为 Agent 5.2.0、Adapter 3.0.0、Codec 2.0.0、Delivery 0.7.0。显式平台正文支持 at/reply/face/受管 image；普通 text 保持字面语义。解析在实体解码前进行，解码仅一次，最后发送原生消息数组。图片像素仅在宿主 Payload 物化，不穿越 Worker IPC；发送账本绑定准确的 wire Payload ID 与摘要，删除来源会撤销派生 wire。

本地定向检查 68 passed，覆盖单次转义、普通文本、畸形 CQ、受管图片引用、512 KiB 图片仅返回引用、来源撤回、相同摘要但不同 wire ID 拒绝以及文本模型无像素投影。Ruff 与差异检查通过。

隔离 PostgreSQL 的正式 BotHost 正常 CQ 场景通过：模型最终正文包含 reply、at、face、受管 image 和转义后的 CQ 字面文本；模拟 OneBot 收到有序数组及准确图片字节。缺失图片的场景最初暴露 Delivery 状态机漏列 `CLAIMED → FAILED`；补齐基线后单独复验通过（1 passed in 86.14s），意图 FAILED/ONEBOT_MEDIA_UNAVAILABLE、没有传输 attempt、没有平台发送。正常原生工具与跨 Run 推理回放场景此前也通过。以上集成使用模拟模型和 OneBot，不能替代真实 QQ 呈现验收。

最终镜像 `forest:runtime-cq-presentation-candidate` 的 ID 为 `sha256:d9334b600d8bcc4eab9a6b30829ef77bb937670521b12646f4f0fe3b914d8b18`。Delivery 新 SQL 基线 3.4.0 的准确 SHA-256 为 `e3a4c5ddc33a63d727ffa55790c076e2423ff1dcc8dc14379c189a4f38fd658c`。开发期通过新库供应匹配包图，无旧 wire DTO 分支或数据库迁移。

最终候选 v42 尚在离线供应；v41 是从未连接 QQ 的旧候选，已停止。现役 v40 仍在线，管理地址 `http://127.0.0.1:18131`。切换结果、真实群消息与长期观察另行追加，不能由上述模拟检查推出。
