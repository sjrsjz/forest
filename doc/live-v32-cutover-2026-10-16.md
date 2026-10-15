# v32 独立记忆基线在线切换（2026-10-16）

当前唯一在线实例是 `forest-main-v32`；`forest-main-v31` 已通过正式部署管理动作转为 OFFLINE，旧库留存供核对与回退。v32 使用独立的 deployment、Host、Bot 数据库与包卷，管理入口为 `127.0.0.1:18103`。运行镜像 `forest:runtime-candidate-284cc17` 来自源码提交 `284cc17`，镜像摘要为 `sha256:a478091ee7ea0084e3902657210c3212b98bd9866f3c15be324df5fc615a5c63`。v32 的 Memory 包为 1.96.0，私有 SQL 为 3.94.0；v31 的 Memory 包仍为 1.94.0，不能把新 SQL 原地装进旧 Bot 数据库。

切换前，v31 为 ONLINE，22 个成员 ACTIVE、运行图修订号 4。只读观察到 11 条群主干、2137 条来源、1618 个 `DOCUMENT.READY` facet、40 个 `DOCUMENT.UNKNOWN` facet；存在 5 次 UNKNOWN 嵌入尝试，冻结预留 163840 tokens。当天已结算 255 次请求、195583 tokens。无活动 Agent Run、任务或待发送消息，获准群 `933179609` 有 78 条入站记录，最新一次判定为 `NO_TRIGGER/mention_required`。这些历史与费用归属 v31，不会自动并入 v32。

先令 v31 OFFLINE，SSH 核对树莓派 NapCat 8080 的 OneBot 已建立连接数为 0，然后才令 v32 ONLINE。切换后 v32 的 Supervisor healthy，部署期望与观测均为 ONLINE，22/22 成员 ACTIVE、22/22 schema bundle READY、运行图修订号 1。只读观察脚本两次采样均无违规项，树莓派 8080 只有一条来自 v32 的持续连接；没有让两个 Bot 同时接管 QQ。NapCat WebUI 登录状态为 READY、已登录且未离线；OneBot `get_status` 返回 `online=true, good=true`，`get_login_info` 返回已登录账号，针对获准群的 `get_group_info(no_cache=true)` 返回成功。2026-10-16 04:35 左右收到“QQ 版本低无法登录”的反馈时，远端 QQ 为 3.2.34-53644，NapCat 为 4.18.38；本次检查未复现登录失败，未重启 QQ、清理登录态或重新发消息。

v32 的新 Bot 数据库起初为空：群主干、记忆来源和 facet 均为 0。通过正式管理端口向 `memory.graph` 提交一张 64×64 PNG 的图片试运行，状态 `CLAIMED → PENDING → READY`，召回结果 0 条（空库预期）。实际嵌入账本结算 1 次、22 tokens，UNKNOWN 与在途预留均为 0。它验证图片输入、嵌入调用、费用结算和空库召回通路，尚未验证已有图文内容时的召回质量。期间未发送 QQ 测试消息，也未调用聊天模型。

当前 v32 对获准群尚无入站记录，因此群聊 `@` → Agent → Provider → Delivery 的端到端回复仍未在这套新库上验收。后续只在获准群进行低频真实触发并观察，不以连接数或空库图片试运行代替回复验收。若需回退，先令 v32 OFFLINE、确认树莓派 8080 已无该连接，再令 v31 ONLINE；两套记忆数据库分离，不能默认合并。
