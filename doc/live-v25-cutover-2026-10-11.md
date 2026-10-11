# v25 隔离实例切换（2026-10-11）

提交 `f08ceac` 构建为 `forest:runtime-candidate-f08ceac`。内核 `runtime.effect-executor-outcome/2` 只按冻结路由与通用服务账本查询回执；Agent 从冻结 Model Request 取得模型 wire 摘要，Provider 负责模型载荷校验。`agent.runtime` 4.49.0 / SQL 2.32.0。Agent 纯逻辑 15 项、Agent 包隔离 PostgreSQL 1 项、BotHost 两种效果恢复各 1 项通过；Ruff 与编译检查通过。

v25 使用独立的 `forest_deployment_v25`、`forest_host_v25`、Bot 物理库和 `forest-candidate-v25_packages` 包卷。部署身份 `forest/candidate-v25-20261011`，Supervisor `forest-candidate-v25-supervisor`，管理入口 `http://127.0.0.1:18096`。Bot `forest-main-v25` 从已验证的 v22 配置重新供应，默认仅许可群 `933179609` 显式 @ 触发；未复制旧消息或记忆。首次供应为 READY/OFFLINE，22 个 schema bundle 为 READY。

首次启动遗漏 Milvus 所在 `forest-clean_default` 网络，导致 `VECTOR_INDEX_UNAVAILABLE`。期间撤回 v25 意图并恢复 v24，确认 OneBot 连接为 v24=1/v25=0。补齐网络后，按 v24 OFFLINE → 连接数 0 → v25 ONLINE 重新切换。当前 v25 Supervisor healthy、图 8 为 22/22 ACTIVE，向量绑定修订 1 为 READY，无未完成 claim；v24/v25 到树莓派 OneBot 的 ESTABLISHED 连接为 0/1。失败图 1–7 均 STOPPED。首条非许可群入站已完整进入 Context/Memory，Agent Run 为 0。

许可群真实 @→Agent→Provider→Delivery 回执尚待低频人工验收，不能用单连接或非许可群入站代替。回退时先把 v25 Bot 设为 OFFLINE 并确认连接退出，再将 v24 Bot 设为 ONLINE；两边物理库和包卷均保留。
