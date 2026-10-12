# v27 隔离实例与真实连接切换（2026-10-12）

从提交 `b669fde` 构建的 `forest:runtime-candidate-b669fde` 镜像摘要为 `sha256:17cc89ef86adb0077bec3e3a7f52b9265728c41294642b398d1e64ace5a454ab`。镜像离线构建了 8 个 deployment、2 个 Host、16 个 Bot 官方包。先前在独立 PostgreSQL 17.10 上以同一源码完成模拟 OneBot、模型、嵌入及向量端口的完整 BotHost 装配、热替换、停机及冷启动恢复用例，`1 passed in 669.33s`。后续提交 `8e95b26` 只修改文档。

新建 `forest_deployment_v27`、`forest_host_v27`、独立 Bot 物理库和 `forest-candidate-v27_packages` 包卷。部署身份为 `forest/candidate-v27-20261012`，Bot 为 `forest-main-v27`，管理入口为 `http://127.0.0.1:18098`。Supervisor 容器 `forest-candidate-v27-supervisor` 使用旧密钥卷的只读挂载、新签发的 Web 初始化凭据和独立 Bot 数据库角色；PostgreSQL 与 Milvus 物理服务复用。管理员测试身份沿用本机受保护的 v26 凭据，新凭据与引导文件只保存在忽略的 `.local/candidate-v27`，文档不记录其值。

v27 通过正式 Web 管理动作先完成 Host 存储结算、冻结 bootstrap profile 和 Bot 供应任务。供应任务为 `SUCCEEDED`，Bot 达到 `READY/OFFLINE`。Bot 物理库的身份为 `forest-main-v27`，22/22 schema bundle READY；Agent 私有 SQL 为 2.33.0、Conversation 为 4.32.0、Memory 为 3.91.0、Vector 为 2.22.0。触发配置仅许可群 `933179609`，要求显式 @，OneBot 账号为 `3605196653`。v27 不复制 v26 消息或记忆。

切换时先通过旧管理面将 `forest-main-v26` 的期望与观测状态都收敛为 `OFFLINE`，再通过新管理面把 `forest-main-v27` 置为 `ONLINE`；后者约一分钟内完成 `STARTING`→`ONLINE`。2026-10-12 08:00（北京时间）的 15 秒四次只读采样中，v27 管理 HTTP 为 200，Bot 图 22/22 ACTIVE、生命周期账本 22、schema bundle 22/22 READY，Agent 4.51.0、Memory 1.93.2、Vector 2.23.0；无观察器违规、活动或逾期 Run、并发会话、待发送消息和活动 Job。再次查询 v26 仍为 `OFFLINE/OFFLINE`。树莓派 `192.168.31.117:8080` 仅见一条来自本机的远端已建立连接，没有双远端连接迹象。

本轮未主动发送 QQ 消息，也没有在 v27 观察窗口内看到许可群入站记录。因此上述证据证明实例供应、模块装配、运行意图和实际 OneBot TCP 连接，不证明真实 @→Agent→Provider→发送链、图文记忆质量或长期费用。后续真实验证应在许可群低频进行，并观察入站分类、Run、模型回执及出站回执；避免重复触发造成费用或消息干扰。

08:28（北京时间）复查时发现树莓派旧 `alicebot.service` 因自启再次处于 `active`，与先前停用旧机器人服务的要求冲突。已执行 `systemctl disable --now alicebot.service`，随后确认它为 `inactive/disabled`；OneBot `192.168.31.117:8080` 仍有且仅有一条来自本机的远端连接。v27 继续 READY/ONLINE、22/22 ACTIVE、观察器零违规；自然入站使 Memory 来源达到 12、群主干达到 5，未观察到活动 Run 或待发送。许可群 `933179609` 仍未出现入站记录，因此未把其他群的 `group_not_allowed` 分类视为真实许可群回复验收。

需要回退时，先通过 v27 管理面将 `forest-main-v27` 置为 `OFFLINE`，确认观测状态和 OneBot 连接退出，再将 v26 置为 `ONLINE`。两套数据库和包卷均保留；回退不会自动合并 v27 新产生的消息和记忆。

2026-10-12 12:12（北京时间）只读一致快照：v27 Supervisor 仍 healthy；Bot 私有库共有 304 条 Inbox 和 304 条 Context，活动 Context 为 298 条，已有 1 次静默裁剪，6 个来源单元升为 ELIGIBLE、298 个仍为 REGISTERED，记忆嵌入 facet 共 19 个，Agent Run 为 0。许可群 `933179609` 的 Inbox 仍为 0；自然入站来自其他群，均不构成许可群显式 @→回复验收。此观察证明无 Agent 工作时 Context 与长期记忆至少发生了一次实际归档，不代表长期负载下活动 Context 的上界或所有来源的嵌入都已结算。

同一只读账本的 19 个 DOCUMENT facet 全为 READY（13 个 TEXT、6 个 IMAGE）；5 笔嵌入批次均为 SUCCEEDED，当日预算已结算 1177 token、UNKNOWN 请求和 token 均为 0。该样本确认当前自然流量的图文文档嵌入与成本结算，不证明新群图片查询命中或更大规模下的成本上界。

2026-10-12 12:28（北京时间）对在线非空索引进行一次认证管理召回试验：上传一条短文本查询，`trial-v27-2e58c07301678255e675190c` 经真实 VL 端点到达 READY。冻结结果含 8 条 GROUP 来源，均明确给出群 `864533524` 与同一主干；`content_truncated=false`，图扩散因配置的 `NODE_LIMIT` 截断，`association_sampled=false`。此文本 QUERY facet 为 READY，沿 claim→attempt→reservation 核对该次独立结算 21 token；同期 358 条自然入站使文档来源 ELIGIBLE 增至 134、日累计 30 笔嵌入和 21157 token，不能把这笔自然流量费用算作试验成本。整个试验没有 QQ 发送和聊天模型调用；它证明当前实例的非空文本语义召回与来源归因，不证明跨群联想、图片查询命中或获准群 @ 回复。

随后从上述试验的授权结果中取回一张 462×462 JPEG（31987 字节），以原字节发起一次图片 QUERY；VL 输入到 READY，未重复该请求。旧版 `memory.trial.result` 对全页返回 `RUNTIME_SERVICE_OUTPUT_INVALID`；逐项只读定位第 3 项是 `image/gif`，其排序分数有限且其他 7 项可单独读取。`media.capture`、Agent 模型投影和 Embedding Provider 已接受 GIF，旧 Memory 管理结果/图片分块契约及通用管理资产白名单却只列 PNG/JPEG/WebP，造成已合法归档的 GIF 证据令整页失败。当前源码在 Memory 包 1.93.3 与通用资产校验中对齐 GIF；既有分页/分块回归加入 GIF 实例，官方包构建摘要为 `sha256:0fad6851dcbd45f74d15ecde387d5b3be028c19f10cc849c58de0bd371ef8cc8`，相邻回归 114 passed，Ruff 通过。旧图片试验短期证据随后按保留策略清理，未在在线 v27 旧镜像上验证修正，也不把 READY 当作图片召回结果验收。此过程没有 QQ 发送或聊天模型调用。
