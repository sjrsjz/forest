# 当前源码新基线在线切换（2026-10-11）

旧 `forest-main` 的部署内核和 Host 内核数据库缺少当前源码新增的组件状态账本；Bot 包的 `memory.graph`、`vector.index` 私有 SQL 也已改变。因此不能把完整新镜像直接套到旧部署或旧 Bot 库。旧环境保持原数据库和卷，先将旧 Bot 的期望状态改为 `OFFLINE`；旧库在切换前保有 3883 条 Inbox、3858 条 Memory source、1 条已完成 Agent Run 和 1 条已发送 Delivery，没有待执行回复。旧 Supervisor 保留可回退。

新部署 `forest/candidate-v22-20261011` 使用独立的 PostgreSQL、部署密钥和包目录，Supervisor 镜像为 `forest:runtime-candidate-9e8a28d`，Web 管理入口为本机 `127.0.0.1:18093`。它复用已有 Milvus 服务，没有启动第二套 Milvus/etcd/MinIO。部署控制面安装 8 个 ACTIVE 模块；通过模块管理 API 供应新身份 `forest-main-v22`，其 Bot 库安装 22 个 READY schema bundle，包含 `memory.graph` SQL 3.88.0 与 `vector.index` SQL 2.19.0。冻结配置只允许群 `933179609` 的明确 @ 触发，OneBot 为树莓派 `192.168.31.117:8080`，VL 嵌入模型为 `Qwen/Qwen3-VL-Embedding-8B`，向量端点为现有 Milvus。新 Bot 在供应和核对期间一直是 `READY/OFFLINE`。

切换顺序为：确认旧 Agent Run 已完成且无待发送 Delivery；将旧 Bot 提交为 `OFFLINE` 并确认旧进程释放 OneBot 连接；再将新 Bot 提交为 `ONLINE`。新部署观察最终为 `ONLINE`、容器 `running/healthy` 且重启计数 0；新 Bot 图修订 1 的 22 个组件均为 ACTIVE，只有新进程持有到树莓派 `8080` 的已建立 TCP 连接。初次只读观测中新 Bot Inbox、Context、Memory source、Agent Run 与 Delivery 均为空。整个切换没有发送 QQ 测试消息。

这是从空 Bot 库开始的新记忆域，旧 Bot 的历史没有迁入新库，但旧数据库和容器均保留。若新实例失败，应先将 `forest-main-v22` 设为 `OFFLINE` 并确认其 OneBot 连接关闭，再将旧 `forest-main` 设为 `ONLINE`；不能让两者同时接收同一 QQ 账号事件。此次证据证明当前源码的真实控制面、供应与整图启动，不证明获准群的下一条 @ 已触发、真实模型回复或长期混合图文负载；这些仍需低频入站观察。
