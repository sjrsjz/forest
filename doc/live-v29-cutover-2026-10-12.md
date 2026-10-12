# v29 GIF 契约修复候选与在线切换（2026-10-12）

源码提交 `97bf3ff` 修正内核管理资产表和 `memory.graph` 试验动作对 GIF 的两处 SQL 白名单缺项；Memory 包为 1.93.4 / SQL 3.92.0。隔离 PostgreSQL 中，真实 GIF 资产上传/发布、认证资产交接到 Memory action、混合输入登记共 3 passed，Ruff 通过。内核基线内容摘要改变，因此新建独立 `forest_deployment_v29`（owner `forest`）、`forest_host_v29`（owner `forest_host`）、Bot 数据库及 `forest-candidate-v29_packages` 包卷，没有将新镜像套到 v28 旧库。

运行镜像 `forest:runtime-candidate-97bf3ff` 的 ID 为 `sha256:711d8478642efc8296173d20d8528e9b6befd069624649615e764151d784784c`。部署身份 `forest/candidate-v29-20261012`，Bot 身份 `forest-main-v29`，管理入口 `http://127.0.0.1:18100`。沿用只读密钥卷，v29 的配对码、角色密码和脚本仅存在忽略的 `.local/candidate-v29`，本文不记录其值。v29 Supervisor 启动 healthy、重启 0 次；正式管理动作使存储请求 SETTLED、供应 Job SUCCEEDED、Bot READY/OFFLINE。配置从 v28 冻结值复制，Secret 重新签发为 v29 专属引用；显式 @ 策略仍仅许可群 `933179609`。

切换时先把 v28 Bot 的期望/观测状态均收敛到 OFFLINE，核验 v28/v29 到 OneBot 8080 的已建立连接均为 0，再将 v29 设为 ONLINE。v29 图修订 1 ACTIVE，22 个 Bot 模块 ACTIVE，Memory 为 1.93.4 / SQL 3.92.0，Agent 为 4.51.0。切换后 v28/v29 的 OneBot 已建立连接数为 0/1，两套 Supervisor 均 healthy、重启 0 次。v28 的独立库与包卷保留。

正式 v29 管理入口对从 v28 已归档 Payload 只读提取、SHA-256 核对的 1,352,134 字节 GIF 完成 6 分块上传；`memory.trial.start` 成功，试验从 CLAIMED/PENDING 进入 READY，`memory.trial.result` 返回 0 项。v29 从空记忆开始，0 项只能证明 GIF 输入的资产、Memory 登记、VL 嵌入及查询链没有旧版 500；尚不能证明 GIF 来源的结果展示。此轮没有主动向 QQ 发消息，许可群真实显式 @→Agent→Provider→发送及长期负载仍待验收。

部署控制图热重载复验：经正式 Web 管理查询预览当前 `deployment.audit` 工件的 REPLACE，影响集仅准备/排空该组件，另 7 个部署组件保持原代。提交同一包摘要的管理动作后，请求 `QUEUED → RUNNING → SUCCEEDED`，部署图修订 1→2；`deployment.audit` Generation 更新，另 7 个 Generation ID 不变。v29 Bot 保持 READY/ONLINE，Supervisor healthy、重启 0，OneBot 连接为 1。此试验证明当前在线实例的定点热重载，不证明不同包版本或存储基线的在线升级。

随后从当前发行物的 `deployment.audit` 源目录复制出仅提升 manifest 版本的官方候选 1.0.2，代码和业务 SQL 未变；用插件编译器构建、导出 ZIP，经包目录所有者本地命令 `publish-official-deployment` 核对 ZIP/包/manifest 三层摘要后，在 **v29 私有包卷**登记官方来源。包摘要为 `sha256:46465cc07027428bab7ce9dd1202f721085e3e4097afcfb0630bf06e5c427457`，ZIP 摘要为 `sha256:37146ef73eb6fbf112d263725136fdd2d3a8dd22467a4e6676f4e269258fe40d`。正式候选预览仍只影响 `deployment.audit`；管理动作结算 SUCCEEDED，部署图修订 2→3，活动包版本由 1.0.1→1.0.2，准确 Generation 改变，另 7 个 Generation 不变，Audit 存储基线摘要不变。Bot 保持 READY/ONLINE，Supervisor healthy、重启 0，OneBot 连接 1。临时 ZIP 中转卷已清理，正式包目录保留不可变工件。此证据覆盖官方控制包的真实跨版本热替换；普通第三方领域 SQL 与存储基线在线升级仍未由这次操作证明。

跨版本选择恢复：在 Agent Run 总数为 0 时，对 v29 Supervisor 做一次受控重启。新进程从持久包目录恢复 `deployment.audit` 1.0.2，部署图修订 4、8/8 ACTIVE；Bot 从 STARTING 收敛至 READY/ONLINE，Bot 图修订 3 的 22/22 模块 ACTIVE，Memory 仍为 1.93.4 / SQL 3.92.0，OneBot 已建立连接重新收敛为 1。Supervisor healthy、Docker 重启计数 0（此次为人工 `docker restart`，不是崩溃重试）。这证明准确版本选择和独立 Bot 运行意图跨 Supervisor 进程恢复，不证明带活动 Agent Run 的重启窗口。

回退顺序：先通过 v29 管理面将 `forest-main-v29` 设为 OFFLINE 并确认观测状态及 OneBot 连接释放，再通过 v28 管理面将 `forest-main-v28` 设为 ONLINE。两套库独立，回退不会合并 v29 新产生的消息和记忆。
