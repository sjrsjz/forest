# v33 运行与候选镜像核验（2026-10-17）

10 月 17 日 16:00 CST 再次通过 SSH 核查：`nc_3605196653.service` 仍为 active，QQ 实际版本 `3.2.34-53644`，NapCat 持续接收实时群消息，树莓派 8080 端口保有一条来自 Forest 的 ESTABLISHED 连接；现役 Supervisor `forest-candidate-v33-supervisor` 为 healthy。日志里的“下载最新版 QQ”是 10 月 13 日的失败登录尝试，10 月 16 日 23:08 已自动快速登录成功。此次检查未重启 QQ、未重置登录态、未发送群消息。隔离 PostgreSQL 的官方包选择并发初始化用例通过（1/1），完整 Bot Host 生命周期用例通过（1/1，560.56 秒）；未把新基线部署到 v33 旧库。

树莓派上的 `nc_3605196653.service` 仍在运行，实际 Linux QQ 为 3.2.34-53644、NapCat 为 4.18.38。NapCat 日志记录 10 月 16 日 23:07 账号离线、约一分钟后自动快速登录成功；10 月 17 日通过本机 OneBot `get_status` 只读调用得到 `retcode=0`、`online=true`、`good=true`。没有重启 QQ、清理登录态或主动发送群消息。

源码 `75beacc` 的 `forest:runtime-candidate-75beacc` 镜像与 Adapter 2.6.0 官方包已构建，Adapter 工件登记在 v33 专用包卷。首次准备的替代 Supervisor 使用旧容器**名称**作 `--volumes-from` 来源；旧容器改名、新容器继承旧名称后，`docker start` 一直等待且新容器保持 `Created`。移除未启动候选并恢复旧容器后，运行重新健康。隔离探针证明同一镜像、挂载、双网络和独立端口均可启动；固定以旧容器**ID**作挂载来源的探针在相同名称交换后也能启动。名称重用导致第一次等待是现有证据支持的推断，未取得 Docker daemon 的直接错误码。

随后用旧容器固定 ID 准备候选，核对环境键、用户、挂载、端口、双网络、命令与健康检查一致；在旧连接降为 0 后启动新容器成功。新 Supervisor HTTP 健康，但 v33 Bot 观测为 `FAILED / SCHEMA_BUNDLE_MISMATCH`、运行图修订 18 无成员，未建立 QQ 连接。源码 Adapter 的私有 SQL 已从 2.1.0 变为 2.2.0，Shell 私有 SQL 也变更；v33 旧库不能被新基线直接覆盖。此试验没有宣称成功热重载或在线 SQL 升级。候选已停止并保留为 `forest-candidate-v33-supervisor-schema-failed-75beacc`，旧镜像 `forest:runtime-candidate-c732bad` 恢复为唯一活动 Supervisor。

之后 Docker Desktop 进程曾退出，Engine API 消失，树莓派 OneBot 连接随之归零。重新启动 Docker Desktop 后，旧 Supervisor 按 `unless-stopped` 自动运行；PostgreSQL 启动期间它先遇到连接池超时，数据库健康后自动重试恢复。最终只读观察为 v33 `READY / ONLINE`、运行图修订 20、22/22 成员活跃、0 活动 Agent Run/任务/待发送，树莓派 8080 只有一条来自 Forest 的已建立连接。Memory 先前的 3 笔 UNKNOWN 嵌入尝试仍冻结，未知暴露额为 98304 tokens；本 UTC 日的正常后台嵌入另已结算 31 次、9386 tokens，不能把它们说成零新增费用。本轮操作没有手动重试 UNKNOWN 或主动调用模型。

当前源码已改正自动安装官方包的初始选择语义：首次物化由 `component.worker-host` 包自有端口将准确工件写入修订 1 的选择账本。其自身则从内核最后一张已发布运行图的准确包摘要冷恢复，忽略未发布候选；跨宿主版本隔离用例通过。v33 旧库不会被就地改写；它继续使用已验证的旧镜像。新基线须在独立 Bot 库上物化，完成包选择、热替换、群聊和记忆链路验收后才能交接 QQ。
