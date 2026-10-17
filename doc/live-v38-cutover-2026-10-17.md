# v38 新库切换与真实 VL 图片试验（2026-10-17）

`forest:runtime-delivery-reconcile`（镜像 ID `sha256:dbe0042b51c317f1ef9564abcc64a64f4f82013b8cf59257c7eccff3b538b5bf`）在独立部署 `forest/accept-v38`、独立 PostgreSQL/包/密钥卷上启动。Bot `forest-main-v38` 从全新数据库 `forest_bot_08cb8d6baa06448079479261` 供应为 READY/OFFLINE；22/22 个 Bot schema bundle 为 READY，其中 Conversation SQL 4.32.0、Memory SQL 3.99.0、Delivery SQL 3.1.5。其管理入口只绑定 `127.0.0.1:18110`。

一次性准备进程以只读事务从现役 v33 部署库取出冻结 Bootstrap 配置和三份加密凭据，用旧主密钥验证摘要，在 v38 经正式 `secret.put` 端口用新部署主密钥重新加密。只读复核确认嵌入模型身份和端点、Provider 端点、OneBot 账号/地址、许可群触发策略、Agent 核心提示配置及三份凭据的明文摘要均与 v33 相同；没有把凭据输出到日志或提交到 Git。v38 Supervisor 重启后，正式管理查询仍显示 Bot READY/OFFLINE。

受控切换先把 v33 Bot 经管理意图设为 OFFLINE 并确认树莓派 OneBot 不再有旧的 ESTABLISHED 连接，再把 v38 设为 ONLINE。v38 Bot 的图为修订 1、22/22 ACTIVE；v33 复核为 OFFLINE/OFFLINE，树莓派 OneBot 8080 有且仅有一条来自 Forest 主机的 ESTABLISHED 连接。v38 的生效配置仍仅允许群 `933179609` 触发，真实 OneBot Binding 指向 QQ `3605196653`。首批 20 条其他群的真实入站全部分类为 `NO_TRIGGER/group_not_allowed`，0 条误触发；未读取或记录消息正文。

通过 `memory.graph` 官方管理试验上传一张本地生成的 64×64 PNG（186 字节），以真实 VL endpoint 发起 IMAGE 查询，状态 `CLAIMED → PENDING → READY`。试验结果为 0 条来源，符合新 Bot 空记忆库；本次只证明真实图片请求、Embedding Effect、向量/查询链可结算，不证明非空图片召回质量。成本账本为 1 次已结算、22 charged tokens、0 UNKNOWN、0 预留。没有发送 QQ 测试消息。许可群在观察窗口内尚无新入站，因此真实 `@ → Agent → Provider → Delivery → QQ` 回复仍待低频验收。

随后在 v38 ONLINE 时通过官方 `module.reload` 先后热替换 Delivery 和 Memory：两次请求均 SUCCEEDED，Bot 图修订 1→2→3，每次仍有 22/22 ACTIVE 成员。依赖链的 Adapter、Agent、Conversation、Media、Shell 等 Generation 随图换代，但树莓派始终只有一条 OneBot ESTABLISHED 连接，旧 v33 保持 OFFLINE，Delivery 发送意图保持 0。Memory 旧试验的 Generation 绑定输入在换代后按契约返回 `410 MEMORY_TRIAL_INPUT_UNAVAILABLE`；用新试验 ID 再发一次同样的真实 PNG 查询获得 READY。成本账本跨代保留为 2 次已结算、44 charged tokens、0 UNKNOWN、0 预留。这个验证覆盖运行时包换代和永久预算账本，不意味着限时管理试验输入可跨代继续读取。

现役 v38 PostgreSQL 与 Supervisor 容器均设为 `unless-stopped`，已核对在线容器的实际重启策略和健康状态。回退入口仍是 v33 部署的管理意图：须先令 v38 OFFLINE、确认 OneBot 连接消失，再令 v33 ONLINE。两个 Bot 数据库独立，v33 记忆未迁入 v38；当前 v38 配置与临时运维脚本保存在被 Git 忽略的 `.cache/accept-v38`，数据库卷保留。

同日追加真实 VL 的非空记忆验收：在 v38 部署中经正式供应端口创建独立数据库的临时 Bot `bot/accept-v38-real-vl-fixture-20261017`，只接本机模拟 OneBot `10001`，沿用 v38 受控 VL 密钥引用和 Qwen3-VL-Embedding-8B 模型身份。正式配置端口把此 Bot 的空闲上下文窗口设为 2→1。向两个模拟群各注入 3 条不含 @ 的被动消息后，静默裁剪形成两个主干，各有 2 条 `ELIGIBLE/COMPLETE` 来源；作者准确区分 `qq:20001`、`qq:20002`、`qq:20003`、`qq:20004`。以文本“紫色极光照亮北方森林”及本地合成的紫色极光/森林 PNG 分别发起正式管理试验，两次均 `READY`，各召回群 `933179609` 的 2 条文本来源，群号、作者和主干来源一致，另一群没有被误归因。临时 Bot 的嵌入账本为 4 次已结算、202 charged tokens、0 UNKNOWN、0 预留；v38 主 Bot 账本仍为 2 次、44 tokens。这个试验证明真实 VL 的 TEXT 与 IMAGE 查询都能命中非空归档文本；没有测试真实 QQ 图片入站或长期语义质量。

为加入本机模拟服务，Compose 一次 `up` 意外重建了 v38 PostgreSQL 和 Supervisor 容器，原数据卷保留；主 Bot 在冷启动约数分钟后恢复 `READY/ONLINE`、图修订 4、22/22 ACTIVE。临时 Bot 已经正式退到 `OFFLINE/OFFLINE`，模拟服务已停止。最后核对 v38 PostgreSQL 和 Supervisor 均 healthy、树莓派 NapCat 服务 active，OneBot 8080 恰有 1 条来自 Forest 主机的 ESTABLISHED 连接。获准群真实新 `@ → Agent → Provider → Delivery → QQ` 回复仍没有直接验收样本。

随后只重启本机模拟 OneBot 并重新上线同一隔离 Bot，未重建 v38 主容器。向模拟群 `933179609` 注入一条公开 100×100 PNG 的被动 OneBot 图片消息和两条被动文本；正式媒体抓取把图片来源归档为 `IMAGE/ELIGIBLE/COMPLETE`，带 1 个 READY 文档 facet，作者为 `qq:20005`。用相同 PNG 经管理资产上传和真实 VL `IMAGE` 试验查询，状态 READY，结果中该图片来源以 `is_seed=true`、`image_count=1` 直接命中，另有同群 3 条文本扩散结果；查询及来源均保留准确群号/作者。隔离 Bot 的累计嵌入账本为 7 次已结算、279 charged tokens、0 UNKNOWN、0 预留；主 Bot 仍为 2 次、44 tokens。重上线后上一 Generation 的管理试验输入按契约返回 410，未将旧试验当成可跨代证据。隔离 Bot 再次退为 `OFFLINE/OFFLINE`，模拟服务已停止；v38 主 Bot 保持图修订 4、22/22 ACTIVE。树莓派独立登录态观察返回 `is_login=true`、`core_ready=true`、`is_offline=false`、无登录错误。此项证明当前部署结构下真实图片来源归档和图片直接种子召回；不替代数月负载或获准群的真实回复验收。
