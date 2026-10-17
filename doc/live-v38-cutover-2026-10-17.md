# v38 新库切换与真实 VL 图片试验（2026-10-17）

`forest:runtime-delivery-reconcile`（镜像 ID `sha256:dbe0042b51c317f1ef9564abcc64a64f4f82013b8cf59257c7eccff3b538b5bf`）在独立部署 `forest/accept-v38`、独立 PostgreSQL/包/密钥卷上启动。Bot `forest-main-v38` 从全新数据库 `forest_bot_08cb8d6baa06448079479261` 供应为 READY/OFFLINE；22/22 个 Bot schema bundle 为 READY，其中 Conversation SQL 4.32.0、Memory SQL 3.99.0、Delivery SQL 3.1.5。其管理入口只绑定 `127.0.0.1:18110`。

一次性准备进程以只读事务从现役 v33 部署库取出冻结 Bootstrap 配置和三份加密凭据，用旧主密钥验证摘要，在 v38 经正式 `secret.put` 端口用新部署主密钥重新加密。只读复核确认嵌入模型身份和端点、Provider 端点、OneBot 账号/地址、许可群触发策略、Agent 核心提示配置及三份凭据的明文摘要均与 v33 相同；没有把凭据输出到日志或提交到 Git。v38 Supervisor 重启后，正式管理查询仍显示 Bot READY/OFFLINE。

受控切换先把 v33 Bot 经管理意图设为 OFFLINE 并确认树莓派 OneBot 不再有旧的 ESTABLISHED 连接，再把 v38 设为 ONLINE。v38 Bot 的图为修订 1、22/22 ACTIVE；v33 复核为 OFFLINE/OFFLINE，树莓派 OneBot 8080 有且仅有一条来自 Forest 主机的 ESTABLISHED 连接。v38 的生效配置仍仅允许群 `933179609` 触发，真实 OneBot Binding 指向 QQ `3605196653`。首批 20 条其他群的真实入站全部分类为 `NO_TRIGGER/group_not_allowed`，0 条误触发；未读取或记录消息正文。

通过 `memory.graph` 官方管理试验上传一张本地生成的 64×64 PNG（186 字节），以真实 VL endpoint 发起 IMAGE 查询，状态 `CLAIMED → PENDING → READY`。试验结果为 0 条来源，符合新 Bot 空记忆库；本次只证明真实图片请求、Embedding Effect、向量/查询链可结算，不证明非空图片召回质量。成本账本为 1 次已结算、22 charged tokens、0 UNKNOWN、0 预留。没有发送 QQ 测试消息。许可群在观察窗口内尚无新入站，因此真实 `@ → Agent → Provider → Delivery → QQ` 回复仍待低频验收。

现役 v38 PostgreSQL 与 Supervisor 容器均设为 `unless-stopped`，已核对在线容器的实际重启策略和健康状态。回退入口仍是 v33 部署的管理意图：须先令 v38 OFFLINE、确认 OneBot 连接消失，再令 v33 ONLINE。两个 Bot 数据库独立，v33 记忆未迁入 v38；当前 v38 配置与临时运维脚本保存在被 Git 忽略的 `.cache/accept-v38`，数据库卷保留。
