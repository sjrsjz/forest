# v39 离线候选供应（2026-10-18）

以提交 `97165332` 构建的 `forest:runtime-memory-query-headroom-97165332`（镜像 ID `sha256:35afca452126bc92459bf976ed2ded08fce1e6847e5d5b5fecfa2911204dc3e0`）供应了独立部署 `forest/accept-v39`。Deployment 数据库 `forest_deployment_v39`、Host 数据库 `forest_host_v39`、Bot 数据库 `forest_bot_9607fec8da7cc9ef10c77e63` 及包卷 `forest-v39-packages` 均独立于现役 v38。候选复用现有 PostgreSQL 服务，部署密钥文件从 v38 secrets 卷只读挂载；新的引导文件和准备脚本位于被 Git 忽略的 `.cache/accept-v39`。管理入口仅绑定 `127.0.0.1:18130`。

通过正式部署管理动作，候选完成存储申请 `PENDING → SETTLED`、冻结 Bootstrap 配置、供应任务 `RUNNING → SUCCEEDED`。Bot `forest-main-v39` 为 `READY/OFFLINE`，没有 ONLINE 意图；Bot 库 22/22 个 schema bundle 为 READY，`memory.graph` 私有 schema 为 4.0.1。管理 API 返回 Deployment 图 ACTIVE、8 个组件和 Bot READY/OFFLINE。只读对照确认 v38 与 v39 的嵌入模型身份及端点、OneBot 账号与地址、仅允许群 `933179609` 的触发策略、Agent 核心提示配置及 Provider/Embedding/OneBot 三份凭据的明文摘要一致；验证没有输出或提交凭据明文。

供应期间现役 v38 保持 ONLINE，树莓派 NapCat 的 OneBot 8080 只有一条已建立连接。v39 没有运行 BotHost，也没有发送 QQ 消息或调用外部模型。候选接入了现有 Milvus 网络但未发起向量请求；正常 SIGTERM 停机退出码为 0，日志无租约关闭错误。当前 `forest-v39-offline` 容器已停止以减少常驻负载，数据库与包卷保留。

切换前须完成 v38 的 24 小时观察并复核样本；之后启动 v39，复核包选择、管理图和 Bot 仍为 OFFLINE。只在 v38 的 Bot 通过正式管理意图转为 OFFLINE、树莓派确认旧 OneBot 连接归零后，才允许将 v39 设为 ONLINE。随后核对 22/22 ACTIVE、准确 QQ Binding、单连接和许可群的一条低频真实 @ 回复。当前没有执行这些切换步骤，不能将离线供应当作真实 QQ 回复、活动 Bot 停机或长期负载验收。
