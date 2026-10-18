# v31 独立基线接管（2026-10-15）

源码提交 `6c9a36f` 构建为 `forest:runtime-candidate-6c9a36f`。因 `memory.graph` 私有 SQL 从 v30 的 `3.92.1` 变为 `3.93.0`，建立独立 deployment `forest/candidate-v31-20261015`、`forest_deployment_v31`、`forest_host_v31`、Bot 库 `forest_bot_aaf50bbf73f355f8b3acb9ec` 和包卷 `forest-candidate-v31_packages`；没有在 v30 的库上替换 SQL。管理入口为 `<MANAGEMENT_URL>`。准备凭据和脚本仅在忽略的 `.local/candidate-v31`，不进入 Git。

正式管理 API 从 v30 复制冻结的模型、VL 嵌入、OneBot 与仅许可群 `<GROUP_ID>` 的显式 @ 配置，并重新签发新 deployment 的 Secret。v31 先达到 `READY/OFFLINE`，22/22 schema bundle READY；Memory 安装 `3.93.0` 组合基线。新包卷初始属主不符合服务账号约束，修正新卷后 Supervisor healthy；复制来的 Web 配对码已过期，保留 setup 身份、刷新 v31 独立配对码并重启后完成供应。两次准备错误均发生在 Bot 正式上线前，没有触碰 v30 库或 QQ 连接。

切换前 v30 为 READY/ONLINE、图修订 40、22/22 ACTIVE、自然来源 241 条、9 个主干、5 个 DOCUMENT.READY facet，活动 Run、Job、待发送和观察违规均为 0。先通过 v30 正式管理动作令其 OFFLINE，确认树莓派 NapCat 8080 已建立连接为 0；再通过 v31 正式管理动作令新 Bot ONLINE。切换后 v30 期望与观测均 OFFLINE，v31 为 READY/ONLINE、图修订 1、22/22 ACTIVE，NapCat 8080 恰好一条连接，脱敏登录观察器返回 READY、`is_login=true`、`core_ready=true`、`is_offline=false`。v31 在线 Generation 为 Adapter `2.5.0`、Media Capture `2.4.0`、Memory `1.94.0`、Agent `4.53.0`；30 秒四次只读采样无违规、活动工作或待发送。

同存储基线的 Memory 热重载复验：v31 无活动 Run、Job、待发送时，经正式 `component.worker-host` 管理动作提交 `module.reload(memory.graph)`；包请求 `reload:33712d140fa0fdacf350c6d7662ca36d` 的状态为 `SUBMITTED`，内核 `graph_replacement_requests` 已明确 `SUCCEEDED`，结果图修订 2。图 1→2 换代 Memory、Adapter、Agent、Conversation、Delivery、Media Capture、Shell 七个组件，其他 15 个 Generation 保留；全部 22 个包摘要不变，新图 22/22 ACTIVE，观察器无违规或活动工作。树莓派 NapCat 登录仍 READY、8080 恰好一条连接。管理请求的 `SUBMITTED` 是已转交内核的状态，不可误读为热替换尚未完成。

05:04（北京时间）通过 v31 正式管理资产端口上传一张现场生成的 64×64 PNG（716 字节），以固定 query_id 发起一次图片召回试验。查询结算 READY，结果 0 项，因为 v31 新库尚无自然来源；只读账本显示 1 个 QUERY.READY facet、当日 1 次请求已结算 22 个嵌入 token，UNKNOWN 与保留 token 均为 0，活动 Run/Job/待发送和观察违规为 0。此证据覆盖 v31 的图片资产、VL 嵌入及空库查询链，不证明非空图片命中或真实 Agent 模型输入。试验未发送 QQ 消息，也没有调用聊天模型。

05:57（北京时间）树莓派的系统 QQ 安装从 3.2.19-39038 升至腾讯官方 3.2.34-53644 ARM64 包，NapCat 独立 QQ 安装原已为 3.2.34-53644。升级后 NapCat 登录观察器 READY，OneBot 本地 `get_status` 返回 online、`get_login_info` 成功，8080 到 Forest 保持一条已建立 WebSocket；旧 `alicebot.service` 为 inactive。v31 只读采样 3 次无违规，图修订 2、22/22 ACTIVE、0 活动 Run/Job/待发送；自然来源 4 条、群主干 1 个，最近入站决策为非许可群的 `NO_TRIGGER/group_not_allowed`，许可群 `<GROUP_ID>` 尚无入站。此检查未主动发送群消息。

06:30（北京时间）以提交 `352eb5b` 构建新宿主镜像 `forest:runtime-candidate-352eb5b`（`sha256:c8ba1d3d37de1cf965f7421496d7579d339ba9306cf8ceb5507ccf08a8c50d53`）。新旧镜像 29 个官方 builtin 包的摘要指纹同为 `97d638fa5c3d3cb7a526a2a72f52f905ba88cf6107ec66a986a8c95bda345d40`，没有改变包存储基线；切换前 v31 无活动 Run、Job、待发送。停旧容器并重命名为 `forest-candidate-v31-supervisor-prev-6c9a36f` 保留回退，按原 env、Secret 卷、包卷、端口和双网络创建并启动新容器。健康探针在启动期短暂失败，约 47 秒后进入 healthy，重启次数 0；Bot 复归 READY/ONLINE、图修订 4、22/22 ACTIVE。连续 3 次只读采样零违规、0 活动 Run/Job/待发送，Memory 自然来源 9 条、群主干 1 个，许可群仍未观察到入站；NapCat READY，8080 仅一条 Forest 连接。本轮没有主动发送 QQ 消息或调用聊天模型。

07:40（北京时间）一小时只读在线观察自然结束：`forest-candidate-v31-soak-20261015` 退出码 0，持续 3600.036 秒，从 2026-10-14 22:40:49 UTC 到 23:40:49 UTC 采集 121 个样本；逐条管理 HTTP/违约检查均为零异常，汇总违约为空。Memory 自然来源从 9 增至 35、群主干从 1 增至 2；终点图修订 4、22/22 ACTIVE、活动 Run/待发送均为 0、嵌入 UNKNOWN 请求 0，Supervisor healthy 且重启次数 0。树莓派 8080 终点只有一条已建立连接，NapCat 自带运行中 QQ 包版本为 3.2.34-53644。观察器不含 QQ 登录 API 检查；此前专用登录检查为 READY。许可群 `<GROUP_ID>` 整段未观察到新入站，观察期间没有主动发送 QQ 消息或调用聊天模型；这次通过的是运行稳定性抽样，不证明真实 @ 回复或数月规模负载。

v31 是独立空记忆库，上线时来源、主干和 facet 均为 0；v30 的 241 条来源没有复制，也没有自动重复嵌入计费。切换操作没有主动发送 QQ 消息或调用模型；后续单次图片管理试验调用了 VL 嵌入，未调用聊天模型。许可群新显式 @→Agent→Provider→QQ 回复、自然图片归档后的非空图片召回、跨群长期负载及成本仍待验收；运行图健康不能替代这些证据。

回退顺序：先经 v31 正式管理面令 `forest-main-v31` OFFLINE，确认 OneBot 连接降为 0；再经 v30 管理面令 `forest-main-v30` ONLINE，并核对图、登录态和单连接。两库独立，回退不会自动合并 v31 期间的新消息或记忆。
