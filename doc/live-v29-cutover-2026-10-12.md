# v29 GIF 契约修复候选与在线切换（2026-10-12）

源码提交 `97bf3ff` 修正内核管理资产表和 `memory.graph` 试验动作对 GIF 的两处 SQL 白名单缺项；Memory 包为 1.93.4 / SQL 3.92.0。隔离 PostgreSQL 中，真实 GIF 资产上传/发布、认证资产交接到 Memory action、混合输入登记共 3 passed，Ruff 通过。内核基线内容摘要改变，因此新建独立 `forest_deployment_v29`（owner `forest`）、`forest_host_v29`（owner `forest_host`）、Bot 数据库及 `forest-candidate-v29_packages` 包卷，没有将新镜像套到 v28 旧库。

运行镜像 `forest:runtime-candidate-97bf3ff` 的 ID 为 `sha256:711d8478642efc8296173d20d8528e9b6befd069624649615e764151d784784c`。部署身份 `forest/candidate-v29-20261012`，Bot 身份 `forest-main-v29`，管理入口 `<MANAGEMENT_URL>`。沿用只读密钥卷，v29 的配对码、角色密码和脚本仅存在忽略的 `.local/candidate-v29`，本文不记录其值。v29 Supervisor 启动 healthy、重启 0 次；正式管理动作使存储请求 SETTLED、供应 Job SUCCEEDED、Bot READY/OFFLINE。配置从 v28 冻结值复制，Secret 重新签发为 v29 专属引用；显式 @ 策略仍仅许可群 `<GROUP_ID>`。

切换时先把 v28 Bot 的期望/观测状态均收敛到 OFFLINE，核验 v28/v29 到 OneBot 8080 的已建立连接均为 0，再将 v29 设为 ONLINE。v29 图修订 1 ACTIVE，22 个 Bot 模块 ACTIVE，Memory 为 1.93.4 / SQL 3.92.0，Agent 为 4.51.0。切换后 v28/v29 的 OneBot 已建立连接数为 0/1，两套 Supervisor 均 healthy、重启 0 次。v28 的独立库与包卷保留。

正式 v29 管理入口对从 v28 已归档 Payload 只读提取、SHA-256 核对的 1,352,134 字节 GIF 完成 6 分块上传；`memory.trial.start` 成功，试验从 CLAIMED/PENDING 进入 READY，`memory.trial.result` 返回 0 项。v29 从空记忆开始，0 项只能证明 GIF 输入的资产、Memory 登记、VL 嵌入及查询链没有旧版 500；尚不能证明 GIF 来源的结果展示。此轮没有主动向 QQ 发消息，许可群真实显式 @→Agent→Provider→发送及长期负载仍待验收。

部署控制图热重载复验：经正式 Web 管理查询预览当前 `deployment.audit` 工件的 REPLACE，影响集仅准备/排空该组件，另 7 个部署组件保持原代。提交同一包摘要的管理动作后，请求 `QUEUED → RUNNING → SUCCEEDED`，部署图修订 1→2；`deployment.audit` Generation 更新，另 7 个 Generation ID 不变。v29 Bot 保持 READY/ONLINE，Supervisor healthy、重启 0，OneBot 连接为 1。此试验证明当前在线实例的定点热重载，不证明不同包版本或存储基线的在线升级。

随后从当前发行物的 `deployment.audit` 源目录复制出仅提升 manifest 版本的官方候选 1.0.2，代码和业务 SQL 未变；用插件编译器构建、导出 ZIP，经包目录所有者本地命令 `publish-official-deployment` 核对 ZIP/包/manifest 三层摘要后，在 **v29 私有包卷**登记官方来源。包摘要为 `sha256:46465cc07027428bab7ce9dd1202f721085e3e4097afcfb0630bf06e5c427457`，ZIP 摘要为 `sha256:37146ef73eb6fbf112d263725136fdd2d3a8dd22467a4e6676f4e269258fe40d`。正式候选预览仍只影响 `deployment.audit`；管理动作结算 SUCCEEDED，部署图修订 2→3，活动包版本由 1.0.1→1.0.2，准确 Generation 改变，另 7 个 Generation 不变，Audit 存储基线摘要不变。Bot 保持 READY/ONLINE，Supervisor healthy、重启 0，OneBot 连接 1。临时 ZIP 中转卷已清理，正式包目录保留不可变工件。仓库 builtin manifest 同步为 1.0.2；从跟踪源码在全新包缓存重新构建的包摘要与 manifest 摘要均与线上工件逐字节一致。此证据覆盖官方控制包的真实跨版本热替换；普通第三方领域 SQL 与存储基线在线升级仍未由这次操作证明。

跨版本选择恢复：在 Agent Run 总数为 0 时，对 v29 Supervisor 做一次受控重启。新进程从持久包目录恢复 `deployment.audit` 1.0.2，部署图修订 4、8/8 ACTIVE；Bot 从 STARTING 收敛至 READY/ONLINE，Bot 图修订 3 的 22/22 模块 ACTIVE，Memory 仍为 1.93.4 / SQL 3.92.0，OneBot 已建立连接重新收敛为 1。Supervisor healthy、Docker 重启计数 0（此次为人工 `docker restart`，不是崩溃重试）。这证明准确版本选择和独立 Bot 运行意图跨 Supervisor 进程恢复，不证明带活动 Agent Run 的重启窗口。

回退顺序：先通过 v29 管理面将 `forest-main-v29` 设为 OFFLINE 并确认观测状态及 OneBot 连接释放，再通过 v28 管理面将 `forest-main-v28` 设为 ONLINE。两套库独立，回退不会合并 v29 新产生的消息和记忆。

## 2026-10-13 在线只读复核与 GIF 来源召回

13:00 UTC 使用跟踪源码的只读观察器采样两次：Supervisor healthy，Bot READY/ONLINE、图修订 3 的 22/22 模块 ACTIVE，许可群 `<GROUP_ID>` 的入站、分类和 Context 应用水位均为 109，无活动 Agent Run、待发送，该群无已记录的聊天发送。Memory 为 3,985 条来源、4,575 个 READY 文档 facet。观察器退出码 1 的唯一违反项是 `DOCUMENT_EMBEDDING_UNKNOWN`；不把 Supervisor 健康误报为记忆全健康。

通过正式管理资产入口对原 1,352,134 字节 GIF 仅发起一次新试验 `trial:live-v28-gif-239a2916527e4a1d81642c6ca228b67a`。试验从 CLAIMED/PENDING 进入 READY，结果共 8 条；首 4 条为直接向量种子，含一条来源群 `<SOURCE_GROUP_ID_A>` 的 GIF。只读复取冻结结果确认该 GIF 的 `media_ref.digest` 与输入文件 SHA-256 相同，结果包含非空来源主干和作者引用；其余证据分别标出各自群号与主干。它证明 v29 在已有自然 ELIGIBLE 来源后可用图片跨群召回并正确归因，不证明 LLM 已消费这些证据或真实 QQ 回复。本轮没有向 QQ 发送消息。

随后以只读事务汇总 UNKNOWN：28 个文档 facet（6 个 IMAGE、22 个 TEXT）归属于 4 笔 2026-10-12 11:17–11:18 UTC 的 `EMBEDDING_TIMEOUT` 请求；对应 attempt 与预算均为 UNKNOWN，冻结预算总计 131,072 token，实际供应商计费未知。包管理动作允许审阅后明确承认可能重复计费再逐笔重排；目前未执行重试。观察器现输出 `attempt_failures_scope=all_time` 和 `unknown_attempt_exposure`，避免把累计超时数误读为最近一小时事件，也不把冻结预算当成实耗。

## 2026-10-13 OneBot 外部故障与 Agent 官方候选

13:51 UTC v29 仍为 READY/ONLINE、Bot 图修订 3 的 22/22 成员 ACTIVE，Agent Run 与待发送均为 0。随后从当前源码构建 Agent 4.52.0，包摘要 `sha256:c3c51fd49058b3c95d253236b70e2614c05bf696b5911e4c588fc0518b09427b`、manifest 摘要 `sha256:32fef60476d36cb935f17c8ef6fd57949c82b87cb09f83f4c0283f3fdf047f52`、ZIP 摘要 `sha256:0cb768ddcecaf2d336b48457b4613be9dfccda6641155c99c7a42d6fb246883a`。候选的完整存储基线和存储包摘要与 v29 现有 Agent 绑定逐字段一致；清单中的业务 SQL 文件摘要不能与完整安装基线直接比较。官方目录命令已按三层摘要发布该包，尚未通过 `component.worker-host` 选择或加载。

14:19 UTC 起 Bot 启动失败。14:50 UTC 只读账本显示期望 ONLINE、观测 STARTING，活动图头修订 4、graph_id 为空，最近多次启动图均 STOPPED，失败点为 `adapter.onebot/ONEBOT_API_UNAVAILABLE`，图替换请求为空。树莓派的冻结 OneBot 端点 `ws://<ONEBOT_HOST>:<ONEBOT_PORT>` 无监听；NapCat 服务虽 active，QQ 日志持续报告 `Login Error (ErrType 1, ErrCode 3)`。未向 QQ 发消息，也未提交 Agent 包选择。Bot 管理入口在启动期间返回 `BOT_MANAGEMENT_UNAVAILABLE`，待 QQ 重新登录且 8080 恢复后再做正式预览和热替换。

故障还暴露 v29 旧 Supervisor 每次进入 STARTING 都清零失败次数，导致重复首次间隔重建运行图。当前源码已修正为持久失败串与最多 3600 秒的指数退避；隔离 PostgreSQL 定向回归和 Ruff 通过。v29 镜像尚未装入修复。


## 2026-10-14 QQ 恢复、包预检与 Agent 热替换

树莓派上的 Linux QQ 从 3.2.33-52892 升至腾讯官方 ARM64 包 3.2.34-53644，并将会话配置中仍为 3.2.19-39038 的 baseVersion、curVersion、buildId 同步为新版本。原 QQ 程序目录保留在 `<NAPCAT_INSTALL_DIR>`/opt/QQ.backup-20261014；手机 QQ 重新扫码后，NapCat 的 8080 OneBot 端口恢复，v29 Bot 自动收敛为 ONLINE。旧 alicebot.service 保持停用。为避免资源争用和重复连接，已停止 v19、v22–v28 的旧候选 Supervisor，保留共享 PostgreSQL、Milvus 和各持久卷。

Agent 4.52.0 的正式管理预检曾因 45 秒 Worker 截止而返回 504。定位到 PackageCatalog.get 对同一捕获内容重复完整校验，官方包解析还串行验证全部包。源码改为一次捕获完成 manifest 与 digest 校验，官方包在保持输入顺序及逐包来源验证的前提下至多 4 路并发；包测试 111 passed、组件选择测试 2 passed、Ruff 通过。对 v29 私有包卷 18 个已注册官方包的只读验证耗时从约 12.0 秒降到 6.0 秒。运行镜像仅覆盖这两处 Host 源文件，保留 v29 原 builtin 基线；首次重建遗漏 forest-clean_default 网络，导致 vector.index/VECTOR_INDEX_UNAVAILABLE，补接 Milvus 网络后 Graph 恢复 ONLINE。一次直接用当前完整源码镜像冷启动触发 SCHEMA_BUNDLE_MISMATCH，已回退；随后比对包声明确认真正的存储基线差异是当前源码 memory.graph 1.93.5 / SQL 3.92.1 与 v29 已安装的 1.93.4 / SQL 3.92.0，Agent 4.52.0 的存储仍为 2.33.0。完整镜像需独立新库或正式存储升级，不能直接覆盖 v29；未把该试验镜像留作活动实例。

修复镜像中的正式 Web 管理预检耗时 31.73 秒，返回的影响集为替换 agent.runtime、media.capture、conversation.scheduler、shell.command、adapter.onebot、delivery，保留其余 16 个成员。提交同一 plan_digest 后，选择修订 0→1，请求 selection:ff11560b4e91147d0de8caf16ee4ac6e 的内核状态为 SUCCEEDED，Bot Graph 183→184；管理运行态显示 22/22 ACTIVE，Agent 4.51.0→4.52.0，六个受影响组件均为新 Generation。提交前无活跃 Run、Job 或待发送。尚未以许可群的新显式 @ 消息验证 4.52.0 的真实 Agent→Provider→QQ 回复；28 个旧 UNKNOWN 文档 facet 仍冻结，未发起可能重复计费的嵌入重试。

受控重启持久恢复：在 0 个活动 Run/Job/待发送时重启同一 v29 Supervisor，原图结束后新 Graph 修订 186 达到 READY/ONLINE、22/22 ACTIVE；Bot 库活动图明确记录 agent.runtime 4.52.0 的新 Generation，树莓派 8080 的 TCP 连接重新建立。此验证覆盖官方包选择的进程恢复，不覆盖正在执行的 Agent Run 中断，也未主动发送 QQ 消息。
