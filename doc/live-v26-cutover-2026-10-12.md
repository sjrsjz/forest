# v26 隔离实例与真实连接切换（2026-10-12）

提交 `f37b4f8` 构建为 `forest:runtime-candidate-v26-f37b4f8`，镜像清单摘要 `sha256:fe73cae84ae7284a9e033290a1cebb3dae79c33545f8a1b7551e862d888e73aa`。此前当前源码在一次性 PostgreSQL Bot 库运行完整 BotHost 图装配、热替换、停机与冷启动恢复，`1 passed in 641.90s`；Conversation 4.39.0 / 私有 SQL 4.32.0 的失败退避包级数据库用例及相邻调度回归 2 passed。镜像内 SQL 字节 SHA-256 为 `42936c89032ea0183d1f52d0b0cd1f190525f38dd3ec1c422f19d791d575c352`，与包清单一致。

v26 使用新建的 `forest_deployment_v26`、`forest_host_v26`、独立 Bot 物理库和 `forest-candidate-v26_packages` 包卷；部署身份 `forest/candidate-v26-20261012`，Bot `forest-main-v26`，管理入口 `http://127.0.0.1:18097`。它复用已有只读部署密钥卷及 PostgreSQL/Milvus 物理服务，但不复用 v25 的部署、Host、Bot 数据库或包卷。Web 配对凭据重新签发，管理员账户沿用现有测试身份；Bot 数据库角色使用新的独立凭据。Bot 配置从在线 v25 的冻结配置和密文按准确源部署 AAD 解密、重加密后供应，许可群仍只含 `933179609`、需显式 @，OneBot 登录账号仍为 `3605196653`。新 Bot 没有复制 v25 的消息或记忆。v25 数据库和包卷保留作回退点。

首次启动 v26 Supervisor 因新建包卷由 root 持有触发 `DATA_OWNER_MISMATCH`；仅对这只空 v26 包卷设置运行 UID 10001 的属主后重启，服务稳定 `healthy`、重启计数 0、管理 HTTP 200。供给辅助脚本最初缺少新 Bot 角色凭据挂载，并误将源密文的部署 AAD 替换成 v26 日期，AES-GCM 认证拒绝；修正辅助脚本的源部署身份与只读凭据路径后，存储请求 `SETTLED`、供给任务 `SUCCEEDED`，Bot `READY/OFFLINE`。这些失败没有导致 QQ 连接，也没有修改 v25 的数据。

切换前 v25 为 `ONLINE`、图修订 21、22/22 ACTIVE，Agent Run、待发送、活跃 Job 均为 0；既有一笔嵌入 `UNKNOWN` 涉及 8 个 DOCUMENT facet，且没有过期 Attempt 或 Reservation。先通过管理动作把 v25 置为 `OFFLINE` 并等到观察状态 `OFFLINE`；此时 v25/v26 到 OneBot 端点的已建立连接均为 0。随后把 v26 置为 `ONLINE`，约 45 秒后观察到图修订 1、22/22 ACTIVE、22/22 schema bundle READY，Conversation 4.39.0、Agent 4.50.0、Memory 1.93.2；Agent Run、待发送与活跃 Job 均为 0，观察器无违约。连续一分钟四次采样均保持上述状态。六个运行中的 Forest 候选 Supervisor 到树莓派 OneBot 的连接数依次为 v19/v22/v23/v24/v25=0、v26=1，没有双连接。

认证 Web 会话下，v26 的向量索引查询返回 `READY`、binding revision 1、ref 水位 0、无活动重建；索引包页面 15,905 字节与清单 SHA-256 相符。Memory 包页面 81,413 字节与清单 SHA-256 相符；许可群主干和来源均为 0，符合新 Bot 空库。此轮没有主动发 QQ 消息或调用付费模型。尚未对 v26 的许可群真实 @ 回复、自然消息归档与长期成本进行新一轮验收；v25 的旧嵌入 UNKNOWN 留在旧库，不随 Bot 身份转移。

需要回退时先把 v26 Bot 置 `OFFLINE` 并确认 OneBot 连接退出，再把 v25 Bot 置 `ONLINE`；两个 Bot 的数据库均保留。回退不会自动合并切换后新 Bot 产生的消息和记忆。
