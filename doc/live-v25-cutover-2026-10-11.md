# v25 隔离实例切换（2026-10-11）

提交 `f08ceac` 构建为 `forest:runtime-candidate-f08ceac`。内核 `runtime.effect-executor-outcome/2` 只按冻结路由与通用服务账本查询回执；Agent 从冻结 Model Request 取得模型 wire 摘要，Provider 负责模型载荷校验。`agent.runtime` 4.49.0 / SQL 2.32.0。Agent 纯逻辑 15 项、Agent 包隔离 PostgreSQL 1 项、BotHost 两种效果恢复各 1 项通过；Ruff 与编译检查通过。

v25 使用独立的 `forest_deployment_v25`、`forest_host_v25`、Bot 物理库和 `forest-candidate-v25_packages` 包卷。部署身份 `forest/candidate-v25-20261011`，Supervisor `forest-candidate-v25-supervisor`，管理入口 `http://127.0.0.1:18096`。Bot `forest-main-v25` 从已验证的 v22 配置重新供应，默认仅许可群 `933179609` 显式 @ 触发；未复制旧消息或记忆。首次供应为 READY/OFFLINE，22 个 schema bundle 为 READY。

首次启动遗漏 Milvus 所在 `forest-clean_default` 网络，导致 `VECTOR_INDEX_UNAVAILABLE`。期间撤回 v25 意图并恢复 v24，确认 OneBot 连接为 v24=1/v25=0。补齐网络后，按 v24 OFFLINE → 连接数 0 → v25 ONLINE 重新切换。当前 v25 Supervisor healthy、图 8 为 22/22 ACTIVE，向量绑定修订 1 为 READY，无未完成 claim；v24/v25 到树莓派 OneBot 的 ESTABLISHED 连接为 0/1。失败图 1–7 均 STOPPED。首条非许可群入站已完整进入 Context/Memory，Agent Run 为 0。

许可群真实 @→Agent→Provider→Delivery 回执尚待低频人工验收，不能用单连接或非许可群入站代替。回退时先把 v25 Bot 设为 OFFLINE 并确认连接退出，再将 v24 Bot 设为 ONLINE；两边物理库和包卷均保留。

## 2026-10-11 Supervisor 镜像更新

`3577fc2` 修复失败启动后撤回意图的生命周期收敛。v25 Supervisor 停机后保留旧容器 `forest-candidate-v25-supervisor-backup-f08ceac`，用 `forest:runtime-candidate-3577fc2` 重新创建同名容器，并沿用原部署、Host、Bot 数据库、包卷、密钥、管理端口；PostgreSQL 与 Milvus 网络均在启动前接通。

新容器 healthy、重启次数 0，图 10 为 22/22 ACTIVE，v24/v25 到树莓派 OneBot 的 ESTABLISHED 连接为 0/1。只读核查为 inbox/context 各 35 条、Agent Run 0；许可群两条消息都判定为 `mention_required`。真实显式 @→Agent→Provider→Delivery 尚待许可群低频验收；本次未主动发送 QQ 消息。失败启动后撤回的 OFFLINE 分支已在隔离 PostgreSQL 回归验证，未在在线 Bot 上人为制造失败。

## 2026-10-11 许可群静默诊断

只读检查截至 `2026-10-11T05:00:22Z` 的入站元数据：49 条消息全部进入 Context，48 条建立 Memory 来源；唯一缺失来源是非许可群图片像素超限，已以 `MEDIA_CAPTURE_UNAVAILABLE` 记账。许可群 10 条消息均为 `mention_required`，Agent Run 仍为 0。逐条对照 OneBot 原始段和规范化段后确认 @ 未在适配器丢失：v25 登录账号 `3605196653`，许可群现有 @ 段指向其他账号，另有仅含回复引用而无 @ 的消息。当前 MENTION 策略要求显式 @ 登录账号；等待一次明确 @3605196653 的低频真人验收。

## 2026-10-11 Web 管理入口实调

使用 v25 管理员会话经 `/management/v1/scopes/resolve` 解析 Bot 管理 scope，再按 scope 路由调用 `vector.index.summary`：索引健康 `READY`、引用水位 0、绑定修订 1、无活动重建。经同一 Bot scope 下载 `vector.index.workspace` 的包自带管理页，15,728 字节与运行时贡献清单的 SHA-256 相符。直接把 `forest-main-v25` 作为 URL 中的 scope ID 会得到 `BOT_MANAGEMENT_SCOPE_NOT_FOUND`，因为 Bot 身份与管理 scope ID 是不同字段；本次未发起重建或其他写动作。
