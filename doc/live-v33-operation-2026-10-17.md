# v33 运行与候选镜像核验（2026-10-17）

18:46 CST SSH 复查树莓派：`nc_3605196653.service` 与 `alicebot.service` 仍运行，NapCat 本地登录诊断为 `READY / is_login=true / is_offline=false / core_ready=true`；日志 18:44 仍有实时群入站，8080 与 Forest 主机有已建立的 TCP 连接。日志中的“QQ 版本过低”登录失败发生于 9 月 17 日，最近 12000 行没有同类错误。实际 QQ 为 3.2.34-53644，仍与 NapCat 当前安装器指定版本一致。未重启、重装或主动发送消息。

10 月 17 日 18:32 CST 对现役 v33 的真实 VL 嵌入账本作只读成本核查：当前 UTC 日已有 188 次成功结算、136015 charged tokens，另有 4 次 UNKNOWN 各冻结 32768 tokens，合计未知暴露 131072 tokens；0 在途预留。按当前每日 300000 token 阈值，账面余量仅 32913 tokens，单批预留为 32768 tokens，因此当时最多只能再准入一批。4 次 UNKNOWN 分别为 1 次 `EMBEDDING_HTTP_UNCERTAIN`（6 facets）与 3 次 `EMBEDDING_TIMEOUT`（4、1、1 facets），均为已派发后结算的不确定外部效果；它们不能因超时就当作未计费或擅自释放预算。当天另有 188 次 SUCCEEDED，最后一次在 18:09 CST 结算；这说明超时并非整个端点持续不可用。为了保留自然群消息的预算，没有在现役 v33 发起额外 IMAGE 管理试验，也没有重试 UNKNOWN、发 QQ 消息或修改配置。快照会随后台流量变化，不代表日终值。

10 月 17 日 17:28 CST 复核：树莓派 NapCat 的本地只读登录诊断返回 `READY / is_login=true / is_offline=false / core_ready=true`，没有当前登录错误；QQ 群消息日志持续更新，v33 Supervisor 仍为 healthy。QQ 为 `3.2.34-53644`，实际注入的 NapCat 为 `4.18.38`。此时 `alicebot.service` 已于 16:41 恢复运行，NapCat 8080 同时有树莓派本地 AliceBot 与 PC 上 Forest 两条 WebSocket 连接；因此本页下方较早的“唯一/一条连接”只描述当时采样，不能代表此刻。当前未为诊断重启服务、替换客户端或发送消息；旧 AliceBot 的运行意图需在正式切换前重新核对。

同日最新源码镜像 `forest:test-worker-host-release-95474cba` 在另一套测试 PostgreSQL 中创建独立 Bot 库，通过完整假 Bot 的启动、Shell/Web、Memory、Graph 换代及冷恢复回归（`1 passed in 581.72s`）；测试库在用例结束时删除，容器退出。它没有连接 QQ，也没有把新 SQL 基线装入 v33 库。

10 月 17 日 16:00 CST 再次通过 SSH 核查：`nc_3605196653.service` 仍为 active，QQ 实际版本 `3.2.34-53644`，NapCat 持续接收实时群消息，树莓派 8080 端口保有一条来自 Forest 的 ESTABLISHED 连接；现役 Supervisor `forest-candidate-v33-supervisor` 为 healthy。日志里的“下载最新版 QQ”是 10 月 13 日的失败登录尝试，10 月 16 日 23:08 已自动快速登录成功。此次检查未重启 QQ、未重置登录态、未发送群消息。隔离 PostgreSQL 的官方包选择并发初始化用例通过（1/1），完整 Bot Host 生命周期用例通过（1/1，560.56 秒）；未把新基线部署到 v33 旧库。

树莓派上的 `nc_3605196653.service` 仍在运行，实际 Linux QQ 为 3.2.34-53644、NapCat 为 4.18.38。NapCat 日志记录 10 月 16 日 23:07 账号离线、约一分钟后自动快速登录成功；10 月 17 日通过本机 OneBot `get_status` 只读调用得到 `retcode=0`、`online=true`、`good=true`。没有重启 QQ、清理登录态或主动发送群消息。

源码 `75beacc` 的 `forest:runtime-candidate-75beacc` 镜像与 Adapter 2.6.0 官方包已构建，Adapter 工件登记在 v33 专用包卷。首次准备的替代 Supervisor 使用旧容器**名称**作 `--volumes-from` 来源；旧容器改名、新容器继承旧名称后，`docker start` 一直等待且新容器保持 `Created`。移除未启动候选并恢复旧容器后，运行重新健康。隔离探针证明同一镜像、挂载、双网络和独立端口均可启动；固定以旧容器**ID**作挂载来源的探针在相同名称交换后也能启动。名称重用导致第一次等待是现有证据支持的推断，未取得 Docker daemon 的直接错误码。

随后用旧容器固定 ID 准备候选，核对环境键、用户、挂载、端口、双网络、命令与健康检查一致；在旧连接降为 0 后启动新容器成功。新 Supervisor HTTP 健康，但 v33 Bot 观测为 `FAILED / SCHEMA_BUNDLE_MISMATCH`、运行图修订 18 无成员，未建立 QQ 连接。源码 Adapter 的私有 SQL 已从 2.1.0 变为 2.2.0，Shell 私有 SQL 也变更；v33 旧库不能被新基线直接覆盖。此试验没有宣称成功热重载或在线 SQL 升级。候选已停止并保留为 `forest-candidate-v33-supervisor-schema-failed-75beacc`，旧镜像 `forest:runtime-candidate-c732bad` 恢复为唯一活动 Supervisor。

之后 Docker Desktop 进程曾退出，Engine API 消失，树莓派 OneBot 连接随之归零。重新启动 Docker Desktop 后，旧 Supervisor 按 `unless-stopped` 自动运行；PostgreSQL 启动期间它先遇到连接池超时，数据库健康后自动重试恢复。最终只读观察为 v33 `READY / ONLINE`、运行图修订 20、22/22 成员活跃、0 活动 Agent Run/任务/待发送，树莓派 8080 只有一条来自 Forest 的已建立连接。Memory 先前的 3 笔 UNKNOWN 嵌入尝试仍冻结，未知暴露额为 98304 tokens；本 UTC 日的正常后台嵌入另已结算 31 次、9386 tokens，不能把它们说成零新增费用。本轮操作没有手动重试 UNKNOWN 或主动调用模型。

当前源码已改正自动安装官方包的初始选择语义：首次物化由 `component.worker-host` 包自有端口将准确工件写入修订 1 的选择账本。其自身则从内核最后一张已发布运行图的准确包摘要冷恢复，忽略未发布候选；跨宿主版本隔离用例通过。v33 旧库不会被就地改写；它继续使用已验证的旧镜像。新基线须在独立 Bot 库上物化，完成包选择、热替换、群聊和记忆链路验收后才能交接 QQ。
