# v32 独立记忆基线的离线候选（2026-10-16）

当前已提交源码 `284cc17` 构建为 `forest:runtime-candidate-284cc17`，镜像摘要为 `sha256:a478091ee7ea0084e3902657210c3212b98bd9866f3c15be324df5fc615a5c63`。源码 `memory.graph` 为 1.96.0 / SQL 3.94.0；在线 v31 仍为 1.94.0 / SQL 3.93.0，不能把新 SQL 包直接装入旧 Bot 数据库。

切换前只读观察：v31 Bot 图修订 4 为 ACTIVE，Memory 管理页的主干分页返回 11 条且无后续页。来源分页分别证明至少两条 ELIGIBLE、至少两条 REGISTERED，`only_unknown_facets` 也有至少两条匹配；每类只请求一条并观察到 `has_more=true`，因此这里没有推断总数。许可群 `933179609` 的来源查询为空。v31 不提供当前源码中的 `memory.embedding-budget.today` 管理查询，返回 `MODULE_MANAGEMENT_OPERATION_NOT_FOUND`；这反映在线包版本差异，不是当前源码查询失败。

已建立与 v31 分离的 deployment `forest/candidate-v32-20261016`、数据库 `forest_deployment_v32`、`forest_host_v32`、Bot 数据库 `forest_bot_93a61f45287aca0ada4109d6` 和包卷 `forest-candidate-v32_packages`；管理端口为 `127.0.0.1:18103`。准备凭据和脚本只在被 Git 忽略的 `.local/candidate-v32`。首次启动因新 Host 数据库误归 `forest` 而报 `HOST_BASELINE_INSTALL_FAILED`；核对 v31 同类数据库后将 v32 Host 库属主改为 `forest_host`，同一容器重启后 healthy，未动 v31。

正式管理供应得到存储请求 SETTLED、冻结引导配置和供应任务 SUCCEEDED；v32 Bot 为 READY/OFFLINE。数据库只读核对：deployment 作用域 8/8 schema bundle READY；v32 Bot 数据库 22/22 schema bundle READY，其中 `memory.graph` 为 SQL 3.94.0。v31 与 v32 Supervisor 同时 healthy，树莓派 NapCat 8080 仍仅有一条来自在线 v31 的 WebSocket 连接。

此阶段没有切换运行意图，没有通过 v32 连接 QQ，也没有调用聊天模型或发送群消息。离线 READY 只证明包与存储可供应；尚需验证当前 Memory 1.96.0 的管理查询、真实图文召回与低频许可群 Agent 回复后，才能记录为在线验收。切换若进行，必须先令 v31 OFFLINE 并确认 OneBot 连接降为零，再令 v32 ONLINE；两套 Bot 记忆数据库不会自动合并。
