# v34 独立离线候选（2026-10-17）

以提交 `95474cba` 的源码构建生产镜像 `forest:runtime-95474cba`。候选 Compose 项目为 `forest-accept-v34`，使用独立的 Secret、PostgreSQL、包目录卷和 `forest/accept-v34` 部署身份；仅借用现有 `forest-clean_default` 网络访问 Milvus，不启动第二套 Milvus。管理端口仅绑定 `<MANAGEMENT_BIND_ADDRESS>`。候选的配置和一次性验收脚本保存在忽略目录 `.cache/accept-v34`，未并入正式 Compose。

初始化器生成新的数据库口令及 Host ID，独立随机主密钥与 Web 首次配对凭据经受保护的 Secret 容器写入新卷。首次管理员设置完成后，管理首页返回 200；部署 RuntimeGraph 修订 1 为 ACTIVE，含 8 个部署 builtin；Host RuntimeGraph 修订 1 为 ACTIVE，含 `host.audit` 与 `host.storage`。此时 Bot 目录为空。

通过正式模块管理端口保存专用 PostgreSQL role Secret，`deployment.storage-orchestrator` 请求达到 `SETTLED`；再从 `provisioning.bootstrap-catalog` 的七个包贡献冻结 Bootstrap Profile，并由 `deployment.provisioning` Job 首次结算 `SUCCEEDED`。新 Bot `bot/accept-v34-offline-20261017` 为 `READY/OFFLINE`，独立物理库为 `forest_bot_b19cb4e64b9d91ca6d14b7c6`。只读 SQL 核验 22/22 个 schema bundle 为 READY，其中 Worker Host 为 `1.11.0`、Memory 为 `3.99.0`、Adapter 为 `2.2.0`、Shell 为 `2.2.5`；Bot RuntimeGraph 数为 0。创建期 Provider、Embedding 与 OneBot 使用占位凭据，OneBot URL 指向候选容器内无服务的 `127.0.0.1:3001`；没有连接树莓派、发群消息或调用付费模型。

一次 `docker compose restart supervisor` 后，启动窗口曾报 `DEPLOYMENT_RUNTIME_UNAVAILABLE`（旧租约尚未释放），同一容器随后自行恢复 healthy。部署与 Host 图均恢复为修订 2 ACTIVE，供应 Job 和 Bot 仍为 `SUCCEEDED`、`READY/OFFLINE`，Bot 库保持 22/22 READY 和 0 张运行图。现役 v33 Supervisor 在整个操作期间保持 healthy，未装载新 SQL 基线。候选 Supervisor 与 PostgreSQL 当前保留运行，额外内存约 380 MiB。

本次证明最新源码的真实 Supervisor 进程可在独立存储上完成 Host 建库、Bot 离线供应和重启恢复；仍未证明候选 Bot 的 ONLINE 图、实际 Worker Host 自身在线升级、真实 QQ 回复或长期图文记忆成本。
