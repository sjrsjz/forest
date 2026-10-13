# v30 完整源码候选与单实例切换（2026-10-14）

当前 `memory.graph` 1.93.5 / SQL 3.92.1 与 v29 的 1.93.4 / SQL 3.92.0 基线不同。使用提交 `fb8199b` 的完整源码镜像 `forest:runtime-candidate-fb8199b`，建立独立部署 `forest/candidate-v30-20261014`、`forest_deployment_v30`、`forest_host_v30`、Bot 库 `forest_bot_467cdffde2fd7c8d4328a89e` 和私有包卷 `forest-candidate-v30_packages`。Supervisor 接入原部署网络和 Milvus 网络；密钥卷只读共享。管理入口 `http://127.0.0.1:18101`。

正式管理 API 复制 v29 冻结的模型、嵌入、OneBot 与许可群配置，并重新签发 Secret 引用。供应请求 SETTLED、供应 Job SUCCEEDED，Bot READY/OFFLINE，22 个 schema bundle 均 READY。Agent SQL 2.33.0、Memory SQL 3.92.1。凭证和本地切换脚本仅在忽略的 `.local/candidate-v30` 中。

切换前 v29 READY/ONLINE，活动 Agent Run、Job、待发送均为 0。先令 v29 OFFLINE，并确认树莓派 OneBot 8080 的已建立连接为 0；再令 v30 ONLINE。v30 达到 READY/ONLINE、图修订 1、22/22 组件 ACTIVE，观察器违规项为空，活动 Run、Job、待发送均为 0。运行图装载 `agent.runtime` 4.52.0 / SQL 2.33.0、`memory.graph` 1.93.5 / SQL 3.92.1；树莓派仅有 1 条 Bot 主机的 OneBot 连接。旧 `alicebot.service` 仍停用。NapCat 实际使用 QQ 3.2.34-53644；系统级 `linuxqq` 3.2.19 不参与该服务。

v30 是空的独立记忆库，切换时 0 条来源和 0 个 facet。v29 历史记忆及 28 个 `DOCUMENT.UNKNOWN` facet 保留于旧库，未复制或重试，避免对未知结果的嵌入请求重复计费。没有主动向 QQ 发消息；许可群 `933179609` 的新显式 @→Agent→Provider→QQ 回复、图片召回及长期负载尚待验收。组件健康不能代替端到端验收。

回退顺序：先通过 v30 管理面令 `forest-main-v30` OFFLINE，确认观测状态和 OneBot 连接释放；再通过 v29 管理面令 `forest-main-v29` ONLINE。两库独立，回退不会自动合并 v30 期间的新消息或记忆。

正式 v30 管理入口对 SHA-256 已核对的 1,352,134 字节 GIF 进行 6 分块上传，发起单次试验 `trial:live-v30-gif-2605cf6edc8945a7955315f284ed3211`。试验从 CLAIMED/PENDING 到 READY，结果 0 项（新库没有来源）。只读账本显示 1 个 `QUERY.READY` facet、当日已结算 1 次请求与 82 个嵌入 token，UNKNOWN 请求/保留 token 均为 0；无活动工作及观察器违规。该证据覆盖 v30 的 GIF 资产、VL 嵌入与空库查询链，不证明非空图片命中或真实 QQ 回复；此轮没有主动发送 QQ 消息。
