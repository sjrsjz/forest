# 在线 `forest-main` 的 v21 镜像切换（2026-10-09）

本记录是当前在线实例的运行快照。早期候选切换记录保留为历史证据，不代表当前状态。Bot 基础身份、供应状态与期望运行意图由部署内核持有；`deployment.bot-catalog` 是可热重载的 builtin 包，只通过受限内核端口提供分页、重命名和启停管理操作，不复制这些权威事实。

## 切换与回退边界

- 源码提交为 `5cea0e5`，新镜像 `forest:runtime-candidate-v21` 的 ID 为 `sha256:a7599fcad969c7592e72d39ea84f373334b8134f221e6ec4d3793b7e3cf6f2d4`。切换前比较了 v19 与 v21 的 26 个官方组件；全部私有 SQL 基线版本和 SHA-256 摘要相同。在线 Memory 先前已选择 1.86.1。本次新镜像的主要包变化为 Embedding 2.8.4、Vector 2.4.2、Provider 4.4.1。
- 停机前旧 Graph 2 有 22/22 ACTIVE 成员，Agent Run 为 0，Delivery 无待发送意图。先关闭旧 Supervisor 的自动重启，再正常停止它；确认旧进程退出后才启动新进程，因此没有两个真实 QQ 客户端同时连接。
- 三个 PostgreSQL 数据库在 Supervisor 停止后做 `pg_dump -Fc`，归档经 `pg_restore -l` 验证。备份保存在忽略版本控制的 `.local/backups/`：`forest_deployment` 415678 字节，SHA-256 `2E95BD6E89BEDA143BE906EFF3C408AB86EDD5A4A771FADE200DEFFEC678AFEC`；`forest_host` 208229 字节，`EF6A249826DFE1CE3C1FFEF5251132704527D0AD52AB2A5258D5D55E29F97505`；`forest_bot_cf635632a5f9f2d6555ac780` 213906098 字节，`7030EE2EB615C3B1E8B5149DF184DBA4C4400B8A8568EB72E54CD2B33CA14EBE`。这些是 PostgreSQL 备份，不包含 Milvus 向量集和 Secret 卷，不能单独宣称完整环境恢复点。
- 旧容器保留为 `forest-live-candidate-v19-supervisor-rollback`，处于 `exited` 且重启策略为 `no`；新容器沿用 `forest-live-candidate-v19-supervisor` 名称、原数据库/Secret/包卷及本机 `127.0.0.1:18092` 管理入口。需要回退代码时，先停止新容器并确认 QQ 连接退出，再移走新容器名称，恢复旧容器名称和重启策略，然后启动旧容器。不要让两者同时运行。回退前核对新旧 SQL、包工件和向量索引状态；本次切换本身未更改 SQL 基线。

## 切换后只读验收

- 新容器 `healthy`，重启次数 0；WebUI HTTP 200，部署内核显示 `forest-main` 为 `READY / ONLINE / ONLINE`，无错误码。Bot Graph 4 的 22 个成员全部 ACTIVE，22 个 READY schema bundle 均匹配当前图。
- ACTIVE 图中的实际包版本为 Embedding 2.8.4、Vector 2.4.2、Memory 1.86.1、Provider 4.4.1；`deployment.bot-catalog` 所在的部署 Graph 2 也是 ACTIVE。在线消息继续进入 Inbox 与 Context；切换后只读采样时二者均为 1644，Memory source unit 为 1635，随后仍继续增长。
- Memory 批次检查得到 1183 个 READY 的 DOCUMENT facet（TEXT 1030、IMAGE 153），188 个 Embedding 批次 SUCCEEDED，188 个 Vector 发布批次 SETTLED；当日预算为 188 次已结算、122549 charged tokens，预留与 UNKNOWN 均为 0。`scripts/observe_live_bot.py` 在 35 秒内采集 4 个样本，违反项为 0；最终样本显示 11 个记忆主干、1183 个 READY facet、无逾期嵌入、无活动 Agent Run、无待发送意图、无并发 Session 或错位群 claim。
- 许可群 `933179609` 的最新入站记录在该次采样中为 `NO_TRIGGER / mention_required`，群 lane 已应用到对应序号；因此本次切换**尚未**验证真实 @ 触发后的 Agent、Provider 与 QQ 回复。完整的跨群、图片及长时负载验收仍以既有隔离验证和后续受控实网观察为准，不由这次无触发采样推断。

## 群消息到记忆来源的全量合流审计

一次性只读脚本 [audit_group_memory_capture.py](../scripts/audit_group_memory_capture.py) 按准确 Bot 存储 Profile 连接 Bot 库，并在同一只读快照里把每条 group.message Inbox 与 source_occurrences、unavailable_source_events 对齐。它只输出计数、不可用错误码和最早未解释缺口时间；超过宽限期仍既无来源也无不可用事实，或同一消息同时有两种终态时，退出码为 1。它扫描完整群消息历史，不应当作为高频健康检查。运行时使用与 Supervisor 相同的 FOREST_* 环境引用、只读 Secret 卷和部署网络，在候选镜像中执行 python /tmp/audit.py --bot-id forest-main --grace-seconds 300。

2026-10-10 本地时间对应的在线 v21 只读运行结果：1810 条群消息 = 1798 条来源记录 + 12 条显式不可用，重叠、宽限期内待捕获、过期未解释缺口均为 0；12 条不可用的底层媒体记录均为图片尺寸或像素上限拒绝。这证明该采样点没有静默漏捕获，不证明未来所有入站和真实 @ 回复链。

## v21 连续在线观察（2026-10-10 本地时间）

在 v21 接管约一小时后，使用同一只读观察器从 2026-10-09 16:42:35 UTC 至 16:47:35 UTC 每 30 秒采样，共 11 次、300.024 秒，所有样本违反项均为空。Bot Graph 4 始终为 22/22 ACTIVE，部署管理 HTTP 为 200；Memory 来源从 1802 增至 1815，末样本有 1341 个 READY facet。嵌入当日账本为 212 次已结算、143533 charged tokens，预留与 UNKNOWN 均为 0；无活动 Agent Run、Job 或待发送意图。许可群 lane 已分类并应用至序号 37，最新消息仍为 NO_TRIGGER / mention_required。紧接着的一次性全量合流审计得到 1827 条群 Inbox = 1815 条来源 + 12 条明确不可用，宽限期内待捕获、过期未解释缺口和结果重叠均为 0。本观察只证明该运行窗口内的静默处理，不覆盖真实 @ 回复或数月负载。
