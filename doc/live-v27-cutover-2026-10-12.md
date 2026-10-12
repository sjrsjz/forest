# v27 隔离实例与真实连接切换（2026-10-12）

从提交 `b669fde` 构建的 `forest:runtime-candidate-b669fde` 镜像摘要为 `sha256:17cc89ef86adb0077bec3e3a7f52b9265728c41294642b398d1e64ace5a454ab`。镜像离线构建了 8 个 deployment、2 个 Host、16 个 Bot 官方包。先前在独立 PostgreSQL 17.10 上以同一源码完成模拟 OneBot、模型、嵌入及向量端口的完整 BotHost 装配、热替换、停机及冷启动恢复用例，`1 passed in 669.33s`。后续提交 `8e95b26` 只修改文档。

新建 `forest_deployment_v27`、`forest_host_v27`、独立 Bot 物理库和 `forest-candidate-v27_packages` 包卷。部署身份为 `forest/candidate-v27-20261012`，Bot 为 `forest-main-v27`，管理入口为 `http://127.0.0.1:18098`。Supervisor 容器 `forest-candidate-v27-supervisor` 使用旧密钥卷的只读挂载、新签发的 Web 初始化凭据和独立 Bot 数据库角色；PostgreSQL 与 Milvus 物理服务复用。管理员测试身份沿用本机受保护的 v26 凭据，新凭据与引导文件只保存在忽略的 `.local/candidate-v27`，文档不记录其值。

v27 通过正式 Web 管理动作先完成 Host 存储结算、冻结 bootstrap profile 和 Bot 供应任务。供应任务为 `SUCCEEDED`，Bot 达到 `READY/OFFLINE`。Bot 物理库的身份为 `forest-main-v27`，22/22 schema bundle READY；Agent 私有 SQL 为 2.33.0、Conversation 为 4.32.0、Memory 为 3.91.0、Vector 为 2.22.0。触发配置仅许可群 `933179609`，要求显式 @，OneBot 账号为 `3605196653`。v27 不复制 v26 消息或记忆。

切换时先通过旧管理面将 `forest-main-v26` 的期望与观测状态都收敛为 `OFFLINE`，再通过新管理面把 `forest-main-v27` 置为 `ONLINE`；后者约一分钟内完成 `STARTING`→`ONLINE`。2026-10-12 08:00（北京时间）的 15 秒四次只读采样中，v27 管理 HTTP 为 200，Bot 图 22/22 ACTIVE、生命周期账本 22、schema bundle 22/22 READY，Agent 4.51.0、Memory 1.93.2、Vector 2.23.0；无观察器违规、活动或逾期 Run、并发会话、待发送消息和活动 Job。再次查询 v26 仍为 `OFFLINE/OFFLINE`。树莓派 `192.168.31.117:8080` 仅见一条来自本机的远端已建立连接，没有双远端连接迹象。

本轮未主动发送 QQ 消息，也没有在 v27 观察窗口内看到许可群入站记录。因此上述证据证明实例供应、模块装配、运行意图和实际 OneBot TCP 连接，不证明真实 @→Agent→Provider→发送链、图文记忆质量或长期费用。后续真实验证应在许可群低频进行，并观察入站分类、Run、模型回执及出站回执；避免重复触发造成费用或消息干扰。

需要回退时，先通过 v27 管理面将 `forest-main-v27` 置为 `OFFLINE`，确认观测状态和 OneBot 连接退出，再将 v26 置为 `ONLINE`。两套数据库和包卷均保留；回退不会自动合并 v27 新产生的消息和记忆。
