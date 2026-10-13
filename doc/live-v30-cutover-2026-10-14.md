# v30 完整源码候选与单实例切换（2026-10-14）

当前 `memory.graph` 1.93.5 / SQL 3.92.1 与 v29 的 1.93.4 / SQL 3.92.0 基线不同。使用提交 `fb8199b` 的完整源码镜像 `forest:runtime-candidate-fb8199b`，建立独立部署 `forest/candidate-v30-20261014`、`forest_deployment_v30`、`forest_host_v30`、Bot 库 `forest_bot_467cdffde2fd7c8d4328a89e` 和私有包卷 `forest-candidate-v30_packages`。Supervisor 接入原部署网络和 Milvus 网络；密钥卷只读共享。管理入口 `http://127.0.0.1:18101`。

正式管理 API 复制 v29 冻结的模型、嵌入、OneBot 与许可群配置，并重新签发 Secret 引用。供应请求 SETTLED、供应 Job SUCCEEDED，Bot READY/OFFLINE，22 个 schema bundle 均 READY。Agent SQL 2.33.0、Memory SQL 3.92.1。凭证和本地切换脚本仅在忽略的 `.local/candidate-v30` 中。

切换前 v29 READY/ONLINE，活动 Agent Run、Job、待发送均为 0。先令 v29 OFFLINE，并确认树莓派 OneBot 8080 的已建立连接为 0；再令 v30 ONLINE。v30 达到 READY/ONLINE、图修订 1、22/22 组件 ACTIVE，观察器违规项为空，活动 Run、Job、待发送均为 0。运行图装载 `agent.runtime` 4.52.0 / SQL 2.33.0、`memory.graph` 1.93.5 / SQL 3.92.1；树莓派仅有 1 条 Bot 主机的 OneBot 连接。旧 `alicebot.service` 仍停用。NapCat 实际使用 QQ 3.2.34-53644；系统级 `linuxqq` 3.2.19 不参与该服务。

v30 是空的独立记忆库，切换时 0 条来源和 0 个 facet。v29 历史记忆及 28 个 `DOCUMENT.UNKNOWN` facet 保留于旧库，未复制或重试，避免对未知结果的嵌入请求重复计费。没有主动向 QQ 发消息；许可群 `933179609` 的新显式 @→Agent→Provider→QQ 回复、图片召回及长期负载尚待验收。组件健康不能代替端到端验收。

回退顺序：先通过 v30 管理面令 `forest-main-v30` OFFLINE，确认观测状态和 OneBot 连接释放；再通过 v29 管理面令 `forest-main-v29` ONLINE。两库独立，回退不会自动合并 v30 期间的新消息或记忆。

正式 v30 管理入口对 SHA-256 已核对的 1,352,134 字节 GIF 进行 6 分块上传，发起单次试验 `trial:live-v30-gif-2605cf6edc8945a7955315f284ed3211`。试验从 CLAIMED/PENDING 到 READY，结果 0 项（新库没有来源）。只读账本显示 1 个 `QUERY.READY` facet、当日已结算 1 次请求与 82 个嵌入 token，UNKNOWN 请求/保留 token 均为 0；无活动工作及观察器违规。该证据覆盖 v30 的 GIF 资产、VL 嵌入与空库查询链，不证明非空图片命中或真实 QQ 回复；此轮没有主动发送 QQ 消息。

02:38:49 树莓派 NapCat 日志曾记录一次账号离线，虽然进程和 OneBot TCP 始终存在。随后在 02:45 左右用 NapCat 本机 `CheckLoginStatus` 验证 `isLogin=true`、`isOffline=false`、`coreReady=true`、`loginPhase=ready`、无登录错误；这说明该次离线已恢复，不能仅凭 TCP 判定 QQ 登录态。该时段许可群没有新入站，v30 来源数仍为 0，真实 @ 端到端验收继续待定。

同存储基线的在线 Memory 热重载复验：v30 无活动 Run、Job、待发送时，经正式 `component.worker-host` 管理动作提交 `module.reload(memory.graph)`。包内请求 `reload:1689ab7af2aa7b2d4d20296106890968` 的提交态为 `SUBMITTED`，对应内核 `graph_replacement_requests` 明确 `SUCCEEDED`，结果图修订 2；`SUBMITTED` 不是内核替换未完成。Graph 1→2 恰好换代 `adapter.onebot`、`agent.runtime`、`conversation.scheduler`、`delivery`、`media.capture`、`memory.graph`、`shell.command` 这 7 个成员，其他 15 个 Generation 保留，换代成员包版本与存储基线均不变。Memory 的 1 条 REGISTERED 来源数和非内容指纹 `6c30905ed19ae4f9bd71aaf8feeebbd3` 前后一致；新图 22/22 ACTIVE、观察器无违规、活动工作为 0，OneBot 连接重新收敛为 1，QQ 登录态 READY。此轮无主动 QQ 消息，未覆盖活动 Agent Run 排空、不同工件或 schema 基线升级。

03:11 复查树莓派时，实际运行的 QQ 包是 3.2.34-53644，系统中较旧的 `linuxqq` 3.2.19 未被该服务使用。NapCat 本机 `CheckLoginStatus` 返回 `READY`，只读 OneBot WebSocket `get_status` 返回 `online=true`、`good=true`；8080 端口有一条来自 v30 主机的已建立连接。此前 02:38 的离线告警已恢复，因此未对已在线服务强行升级或重启。v30 Bot 库此时有 2 条其他群入站，均为 `FINAL/NO_TRIGGER`，Memory 有 2 条 REGISTERED 来源，Agent Run、许可群入站均为 0；没有主动发送 QQ 消息。该复查只证明当前协议在线，不代替许可群触发和真实回复验收。

04:10 用已提交源码 `f631613` 构建 `forest:runtime-candidate-f631613` 并更新同一 v30 Supervisor：更新前只读观察器确认 Graph 2、22/22 ACTIVE、无活动 Run/Job/待发送和违规，6 条自然来源、2 个主干、82 token 已结算。旧容器优雅退出码 0，树莓派 OneBot 连接先降为 0；旧容器保留为 `forest-candidate-v30-supervisor-prev-f631613`。新容器沿用原 deployment/Host/Bot 数据库、包卷、只读 Secret、bootstrap、管理端口和双网络；启动后健康，v30 READY/ONLINE、Graph 4、22/22 ACTIVE，6 条来源均 REGISTERED、0 个 facet、82 token 成本账本不变，活动工作与观察违规均为 0。真实 NapCat `get_status` 启动门禁通过，QQ 本机登录状态 READY，OneBot 连接恰好 1 条。此轮没有主动向 QQ 发消息；许可群仍无新入站。门禁只检查 Generation 启动瞬间，运行期离线仍须独立监测。
