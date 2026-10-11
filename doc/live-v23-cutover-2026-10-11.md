# 当前源码 v23 实例切换（2026-10-11）

本次把提交 `8bc2192` 构建为 `forest:runtime-candidate-8bc2192`，在无网络容器中验证了自动安装的 8 个部署包、2 个 Host 包和 16 个 Bot 包。由于 v22 已安装的 SQL bundle 与当前源码不同，本次没有对活动 schema 原地执行 DDL。v23 复用 `forest-candidate-v22-postgres-1` 这一个 PostgreSQL 服务和现有 Milvus 服务，但使用独立的 `forest_deployment_v23`、`forest_host_v23`、新 Bot 物理库及 `forest-candidate-v23_packages` 包目录卷。部署身份为 `forest/candidate-v23-20261011`，Supervisor 容器为 `forest-candidate-v23-supervisor`，管理入口为本机 `127.0.0.1:18094`。底层 PostgreSQL 凭据卷与部署主密钥沿用 v22；部署事实库和 Bot 身份相互隔离。

通过部署包的管理 API 供应 `forest-main-v23`，先保持 `READY/OFFLINE`，再用只读 SQL 核对空 Inbox、Context、Memory source 和 Agent Run。其 22 个 schema bundle 均为 READY；`memory.graph` SQL 为 `3.90.0`，`vector.index` SQL 为 `2.19.0`。冻结配置从 v22 的已验证 Bot 复制，仅允许群 `933179609` 明确 @ 触发，OneBot 连接仍指向树莓派 `192.168.31.117:8080`，VL 嵌入模型仍为 `Qwen/Qwen3-VL-Embedding-8B`。本次未迁移旧聊天或记忆数据，也未发送 QQ 测试消息。

先通过 v22 管理端把 `forest-main-v22` 设为 `OFFLINE`，并观察到期望和实际状态均为 OFFLINE；随后把 `forest-main-v23` 设为 `ONLINE`。v23 起初只有 PostgreSQL Docker 网络，缺少通往 Milvus 的 `forest-clean_default` 网络，导致前两次 Bot 图启动报告 `VECTOR_INDEX_UNAVAILABLE`，两次图均 STOPPED。将 v23 Supervisor 接入与 v22 相同的 Milvus 网络后，第三次图启动成功：22 个组件 Generation ACTIVE，Bot 观察状态 ONLINE，Supervisor `running/healthy` 且重启策略为 `unless-stopped`。容器网络表可见到树莓派 `192.168.31.117:8080` 的一条 ESTABLISHED TCP 连接。切换后只读核对仍为 0 条 Inbox、Context、Memory source 和 Agent Run；这不能代替获准群的真实 @、模型回复、发送或长期图文负载验收。

回退时必须先通过 `127.0.0.1:18094` 把 `forest-main-v23` 设为 `OFFLINE` 并确认实际 OFFLINE、OneBot 连接关闭，再通过 `127.0.0.1:18093` 把 `forest-main-v22` 设为 `ONLINE`。两个 Bot 不得同时接收同一 QQ 账号事件。v22 的部署库、Bot 库、容器和包卷均保留，v23 的独立库也应保留供故障分析；是否迁移两边历史需要另立明确的数据合并协议，不能简单拼接来源和记忆 ID。
