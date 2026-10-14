# v31 独立基线接管（2026-10-15）

源码提交 `6c9a36f` 构建为 `forest:runtime-candidate-6c9a36f`。因 `memory.graph` 私有 SQL 从 v30 的 `3.92.1` 变为 `3.93.0`，建立独立 deployment `forest/candidate-v31-20261015`、`forest_deployment_v31`、`forest_host_v31`、Bot 库 `forest_bot_aaf50bbf73f355f8b3acb9ec` 和包卷 `forest-candidate-v31_packages`；没有在 v30 的库上替换 SQL。管理入口为 `http://127.0.0.1:18102`。准备凭据和脚本仅在忽略的 `.local/candidate-v31`，不进入 Git。

正式管理 API 从 v30 复制冻结的模型、VL 嵌入、OneBot 与仅许可群 `933179609` 的显式 @ 配置，并重新签发新 deployment 的 Secret。v31 先达到 `READY/OFFLINE`，22/22 schema bundle READY；Memory 安装 `3.93.0` 组合基线。新包卷初始属主不符合服务账号约束，修正新卷后 Supervisor healthy；复制来的 Web 配对码已过期，保留 setup 身份、刷新 v31 独立配对码并重启后完成供应。两次准备错误均发生在 Bot 正式上线前，没有触碰 v30 库或 QQ 连接。

切换前 v30 为 READY/ONLINE、图修订 40、22/22 ACTIVE、自然来源 241 条、9 个主干、5 个 DOCUMENT.READY facet，活动 Run、Job、待发送和观察违规均为 0。先通过 v30 正式管理动作令其 OFFLINE，确认树莓派 NapCat 8080 已建立连接为 0；再通过 v31 正式管理动作令新 Bot ONLINE。切换后 v30 期望与观测均 OFFLINE，v31 为 READY/ONLINE、图修订 1、22/22 ACTIVE，NapCat 8080 恰好一条连接，脱敏登录观察器返回 READY、`is_login=true`、`core_ready=true`、`is_offline=false`。v31 在线 Generation 为 Adapter `2.5.0`、Media Capture `2.4.0`、Memory `1.94.0`、Agent `4.53.0`；30 秒四次只读采样无违规、活动工作或待发送。

同存储基线的 Memory 热重载复验：v31 无活动 Run、Job、待发送时，经正式 `component.worker-host` 管理动作提交 `module.reload(memory.graph)`；包请求 `reload:33712d140fa0fdacf350c6d7662ca36d` 的状态为 `SUBMITTED`，内核 `graph_replacement_requests` 已明确 `SUCCEEDED`，结果图修订 2。图 1→2 换代 Memory、Adapter、Agent、Conversation、Delivery、Media Capture、Shell 七个组件，其他 15 个 Generation 保留；全部 22 个包摘要不变，新图 22/22 ACTIVE，观察器无违规或活动工作。树莓派 NapCat 登录仍 READY、8080 恰好一条连接。管理请求的 `SUBMITTED` 是已转交内核的状态，不可误读为热替换尚未完成。

05:04（北京时间）通过 v31 正式管理资产端口上传一张现场生成的 64×64 PNG（716 字节），以固定 query_id 发起一次图片召回试验。查询结算 READY，结果 0 项，因为 v31 新库尚无自然来源；只读账本显示 1 个 QUERY.READY facet、当日 1 次请求已结算 22 个嵌入 token，UNKNOWN 与保留 token 均为 0，活动 Run/Job/待发送和观察违规为 0。此证据覆盖 v31 的图片资产、VL 嵌入及空库查询链，不证明非空图片命中或真实 Agent 模型输入。试验未发送 QQ 消息，也没有调用聊天模型。

v31 是独立空记忆库，上线时来源、主干和 facet 均为 0；v30 的 241 条来源没有复制，也没有自动重复嵌入计费。切换操作没有主动发送 QQ 消息或调用模型；后续单次图片管理试验调用了 VL 嵌入，未调用聊天模型。许可群新显式 @→Agent→Provider→QQ 回复、自然图片归档后的非空图片召回、跨群长期负载及成本仍待验收；运行图健康不能替代这些证据。

回退顺序：先经 v31 正式管理面令 `forest-main-v31` OFFLINE，确认 OneBot 连接降为 0；再经 v30 管理面令 `forest-main-v30` ONLINE，并核对图、登录态和单连接。两库独立，回退不会自动合并 v31 期间的新消息或记忆。
