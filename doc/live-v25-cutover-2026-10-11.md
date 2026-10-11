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

Memory 管理入口同样经真实会话验证：按许可群号筛选主干得到 1 条，模块自带页面 81,413 字节与运行时 SHA-256 一致。来源查询默认只看 `ELIGIBLE`，此时返回 0；明确筛选 `REGISTERED` 后首个 2 条分页存在且 `has_more=true`，两条均为文本、`NO_FACETS`。这说明这些来源尚未取得归档资格：Memory 在归档资格提交后才把来源升为 `ELIGIBLE` 并入列文档 facet 工作；不是适配器丢失入站。管理页已提供“待归档”筛选。该在线检查未人为推动归档或调用嵌入服务。
## 2026-10-11 部署包 1.6.3 在线替换

提交 `12dbd5c` 构建 `forest:runtime-candidate-12dbd5c`。停下原 Supervisor 后将其保留为 `forest-candidate-v25-supervisor-backup-3577fc2`，新容器沿用独立 deployment/host/Bot 库、包卷、密钥和本机管理端口，并在启动前接入 PostgreSQL 与 Milvus 网络。新 Supervisor 为 `running/healthy`、重启次数 0；v24/v25 到树莓派 OneBot 的 ESTABLISHED 连接数为 0/1。

重启后部署图仍选中 `deployment.package-registry` 1.6.2；镜像升级不会擅自覆盖已发布的包选择。目录已包含官方 1.6.3 工件 `sha256:277c38df2e8eaacbb9f074782ecea395fd2da54bcfb1745a7faa5bcf3164f701`。通过认证管理端读取图修订 4，预览只替换该模块、保留其他 7 个部署模块，再提交带修订和计划摘要的 `module.replace`。操作 `web:sha256:94dcabcf01f9e99c4a2d89f9b269b70b05e15084b4c72bef5818fc3e72dd5c3d` 结算 `SUCCEEDED`；部署图修订 5 的该模块 `ACTIVE`，版本 1.6.3。此步骤验证了部署模块的真实在线代际热替换，而不是仅验证进程镜像更新。

替换后 Web 管理端的向量索引查询为 READY、绑定修订 1；向量与记忆包自带页面字节均匹配运行时声明的 SHA-256。Bot 图 12 为 22/22 ACTIVE，Supervisor 仍 healthy 且连接数仍为 0/1。只读 Bot 库核对 inbox/context 各 203 条、Memory 来源 202 条、Agent Run 0；203 条全部为 NO_TRIGGER（非许可群 189、许可群未明确 @ 当前账号 14）。唯一未建来源的消息由图片像素超限明确记为 `MEDIA_CAPTURE_UNAVAILABLE`。本次未主动发送 QQ 消息；许可群明确 @ 当前登录账号后的 Agent→Provider→Delivery 回执仍待低频真人验收。
## 2026-10-11 媒体采集传输宿主更新

提交 `0968bfb` 修复原生图片采集端的 DNS 验证/连接地址替换窗口，构建 `forest:runtime-candidate-0968bfb`。停机后保留原容器为 `forest-candidate-v25-supervisor-backup-12dbd5c`，新容器沿用 v25 的隔离数据库、包卷、只读密钥与 bootstrap、本机管理端口和资源/安全限制，并在启动前连接 PostgreSQL 与 Milvus 网络。旧容器停下后才启动新容器，避免同时连接 OneBot。

恢复期间管理端先可用，Bot scope 曾短暂返回 `BOT_MANAGEMENT_UNAVAILABLE`，此时没有重启进程；随后 Bot 图 14 的 22/22 成员 ACTIVE，Supervisor `running/healthy`、重启次数 0。v24/v25 到树莓派 OneBot 的 ESTABLISHED 连接恢复为 0/1。认证管理端的向量与记忆查询、两份包自带页面 SHA-256 核对通过；部署图仍选中 `deployment.package-registry` 1.6.3。只读 Bot 库核对 inbox/context 各 234、Memory 来源 233、Agent Run 0；唯一缺失来源仍是已明确结算 `MEDIA_CAPTURE_UNAVAILABLE` 的超像素图片。最近入站已到 `2026-10-11T07:33:04Z`，许可群 14 条现有消息均为 `mention_required`。本次未主动发送 QQ 消息，明确 @ 当前 Bot 后的真实回复仍待低频验收。
## 2026-10-11 真实 VL 管理召回试验

在 v25 认证 Bot 管理 scope 中分别上传一段短文本和一张本地生成的 64×64 PNG，以 `memory.trial.start` 各发起一次查询。两个试验均经过 CLAIMED/PENDING 到达 READY；`memory.trial.result` 各返回 0 项、`truncated=false`、无截断原因。只读 Bot 库进一步核对：两个查询分别有一个 `TEXT` 和 `IMAGE` 的 READY QUERY facet，预算账本累计 2 次已结算嵌入请求、48 个计费 token、0 次 UNKNOWN。当前库的群聊来源尚未归档为 ELIGIBLE，故空结果符合资格状态；这验证真实 QiYu VL 端点接受文本与图片并完成查询冻结，不证明非空图的跨模态命中。本次没有向 QQ 发送消息。
