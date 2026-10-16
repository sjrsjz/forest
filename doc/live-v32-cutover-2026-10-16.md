# v32 独立记忆基线在线切换（2026-10-16）

当前唯一在线实例是 `forest-main-v32`；`forest-main-v31` 已通过正式部署管理动作转为 OFFLINE，旧库留存供核对与回退。v32 使用独立的 deployment、Host、Bot 数据库与包卷，管理入口为 `127.0.0.1:18103`。运行镜像 `forest:runtime-candidate-284cc17` 来自源码提交 `284cc17`，镜像摘要为 `sha256:a478091ee7ea0084e3902657210c3212b98bd9866f3c15be324df5fc615a5c63`。v32 的 Memory 包为 1.96.0，私有 SQL 为 3.94.0；v31 的 Memory 包仍为 1.94.0，不能把新 SQL 原地装进旧 Bot 数据库。

切换前，v31 为 ONLINE，22 个成员 ACTIVE、运行图修订号 4。只读观察到 11 条群主干、2137 条来源、1618 个 `DOCUMENT.READY` facet、40 个 `DOCUMENT.UNKNOWN` facet；存在 5 次 UNKNOWN 嵌入尝试，冻结预留 163840 tokens。当天已结算 255 次请求、195583 tokens。无活动 Agent Run、任务或待发送消息，获准群 `933179609` 有 78 条入站记录，最新一次判定为 `NO_TRIGGER/mention_required`。这些历史与费用归属 v31，不会自动并入 v32。

先令 v31 OFFLINE，SSH 核对树莓派 NapCat 8080 的 OneBot 已建立连接数为 0，然后才令 v32 ONLINE。切换后 v32 的 Supervisor healthy，部署期望与观测均为 ONLINE，22/22 成员 ACTIVE、22/22 schema bundle READY、运行图修订号 1。只读观察脚本两次采样均无违规项，树莓派 8080 只有一条来自 v32 的持续连接；没有让两个 Bot 同时接管 QQ。NapCat WebUI 登录状态为 READY、已登录且未离线；OneBot `get_status` 返回 `online=true, good=true`，`get_login_info` 返回已登录账号，针对获准群的 `get_group_info(no_cache=true)` 返回成功。2026-10-16 04:35 左右收到“QQ 版本低无法登录”的反馈时，远端 QQ 为 3.2.34-53644，NapCat 为 4.18.38；本次检查未复现登录失败，未重启 QQ、清理登录态或重新发消息。

v32 的新 Bot 数据库起初为空：群主干、记忆来源和 facet 均为 0。通过正式管理端口向 `memory.graph` 提交一张 64×64 PNG 的图片试运行，状态 `CLAIMED → PENDING → READY`，召回结果 0 条（空库预期）。实际嵌入账本结算 1 次、22 tokens，UNKNOWN 与在途预留均为 0。它验证图片输入、嵌入调用、费用结算和空库召回通路，尚未验证已有图文内容时的召回质量。期间未发送 QQ 测试消息，也未调用聊天模型。

当前 v32 对获准群尚无入站记录，因此群聊 `@` → Agent → Provider → Delivery 的端到端回复仍未在这套新库上验收。后续只在获准群进行低频真实触发并观察，不以连接数或空库图片试运行代替回复验收。若需回退，先令 v32 OFFLINE、确认树莓派 8080 已无该连接，再令 v31 ONLINE；两套记忆数据库分离，不能默认合并。

同一当前源码另在隔离 PostgreSQL 与测试专用 Milvus 集合中，用真实 QiYu VL 重跑 PNG/GIF 跨群非空召回整链，`1 passed in 155.44s`；测试后 `forest_cross_vl_%` 临时数据库为 0。该用例验证非空图文召回、原群/作者归因与管理试验，但 OneBot 和聊天模型是替身，不证明 v32 在线群聊端到端回复。

v32 在线记忆预算已通过模块配置计划与应用动作从修订 1 调整到期望/生效修订 2：按 UTC 日最多 300 次嵌入请求、300000 tokens，单次预留仍为 32768 tokens。该设置仅约束后续嵌入准入，不修改已结算账本。配置热重建后运行图修订号 2、22/22 ACTIVE，观察脚本两次采样零违规；NapCat 登录 READY，树莓派 OneBot 仍只有一条连接。预算账本保持已结算 1 次、22 tokens，UNKNOWN/预留为 0。

随后将官方 `memory.graph` 1.96.1 的精确 ZIP 工件发布到 v32 专用包目录：ZIP SHA-256 为 `b440f596d30741956f9fc749297094e52849cee8dd06190bd5b495f19e05145c`，包摘要为 `sha256:903222f7b5e575824f1e0ff2e6fdae377c3d8d288e8cc6cea81dd2f5f4f3dca5`，Manifest 摘要为 `sha256:af15b9108063f0ab78b1fd45b88d319f624bc39d2d7e30989385a3c689d02cbc`。当前 Supervisor 进程没有重启；正式官方工件选择请求 `selection:26fb63e86202180c7162375b1a272850` 的内核账本一次尝试 `SUCCEEDED`，Graph 2→3。预检保留 Memory 配置修订 2 和存储摘要；Bot 数据库权威图账本显示 22 个成员中实际换代 Memory、Agent、Media Capture、Conversation Scheduler、Shell、Adapter 和 Delivery 共 7 个，另 15 个 Generation ID 保留。旧 7 个状态均为 `DISPOSED`，Graph 2 为 `RETIRED`，Graph 3 为 `ACTIVE`。换代后 22/22 ACTIVE，观察器两次采样零违规，预算仍已结算 1 次／22 tokens 且无 UNKNOWN 或预留，QQ 登录 READY、OneBot 单连接。当前在线 Memory 为 1.96.1 / SQL 3.94.0；选择账本已固定该官方工件摘要；后续冷启动恢复见下段。

树莓派 NapCat 对获准群 `933179609` 的只读 `get_group_msg_history` 返回最近消息时间 `2026-10-15T17:05:44Z`（北京时间 10 月 16 日 01:05:44），早于 v32 的在线切换。因此该群在本次观察窗口没有切换后的消息可供入站验收；这项核对不证明其他群的流量或真实 `@` 回复。

在确认 0 活动 Agent Run、任务和待发送后，对同一个 v32 Supervisor 容器执行一次受控重启。重启前 Graph 3 的 Memory 为 1.96.1；重启后同一镜像与包卷恢复为 Graph 5 ACTIVE、22/22 成员 ACTIVE，Memory 仍为官方选择的精确包摘要 `sha256:903222f7b5e575824f1e0ff2e6fdae377c3d8d288e8cc6cea81dd2f5f4f3dca5` / 1.96.1，配置期望/生效修订仍为 2，嵌入账本仍为已结算 1 次／22 tokens、UNKNOWN 和预留为 0。Graph 3 为 STOPPED，数据库没有 Graph 4 行；对照 `kernel.runtime` 的 `retire_graph` 实现，退役先占用修订 4 并把图头置空，不创建实体图，随后冷启动发布 Graph 5。该代码路径解释了观察到的修订间隙，不能把缺失的图行写成一次失败的图发布。观察器两次采样零违规、无活动工作；NapCat 登录 READY，OneBot 连接先降为 0 再收敛为 1。本次未发送 QQ 消息，获准群真实 `@` 回复仍未验收。

2026-10-16 09:08–09:14（北京时间）再次只读验收：v32 Graph 5、22/22 ACTIVE、22/22 schema bundle READY，连续两次观察零违规；无活动 Agent Run/Job、待发送、嵌入预留或 UNKNOWN。Memory 当前有 83 个来源、5 个群主干、0 个 facet；许可群 `933179609` 的 Conversation lane 仍未观察到入站。随后完整扫描群 Inbox 的独立审计返回 86 条群消息、86 条 source occurrence、0 条不可用、0 条超过五分钟仍无捕获、0 条重叠结算。来源数与 occurrence 数属于不同实体，不能互相代换；零 facet 不能证明非空召回。审计不读取正文或媒体，不向 QQ 发消息，也未调用模型。
