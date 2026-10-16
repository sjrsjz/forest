# v33 当前源码独立实例切换与验收（2026-10-16）

截至北京时间 18:31，forest-main-v33 是唯一 ONLINE Bot；forest-main-v32 已通过正式部署管理动作转为 OFFLINE。v33 使用独立的部署 forest/candidate-v33-20261016、部署数据库 forest_deployment_v33、宿主数据库 forest_host_v33、Bot 数据库 forest_bot_9291fa239554ca15299270df 和专用包卷 forest-candidate-v33_packages。管理端口为 127.0.0.1:18104，私有引导文件位于被 Git 忽略的 .local/candidate-v33。v32 数据库与包卷保留供回退，记忆数据未迁移到 v33。

运行镜像 forest:runtime-candidate-c732bad 从已提交源码 c732bad 构建，镜像摘要为 sha256:4b2684f573d96e69cf18867017ddea15beaf8c3ff05d419349d674844cbc6890。v33 的 Memory 包为 2.1.0／私有 SQL 3.99.0，Agent 包为 4.53.1／SQL 2.33.1，Delivery 包为 0.4.4／SQL 3.1.1。离线供应通过正式存储、引导配置与供应任务完成，Bot 达到 READY/OFFLINE；Bot 数据库 22/22 schema bundle READY。

首次启动离线 Supervisor 时，新包卷仍属 root，存储所有权门禁以 DATA_OWNER_MISMATCH 拒绝。仅将 v33 专用包卷根目录交给服务 UID 10001 后，同一容器健康运行。首次 ONLINE 尝试又以 VECTOR_INDEX_UNAVAILABLE 失败：v33 只连接 PostgreSQL 网络，缺少 v32 已连接的 forest-clean_default Milvus 网络。随即将 v33 退回 OFFLINE、确认树莓派 OneBot 连接数为 0，恢复 v32 ONLINE 与单连接。为 v33 增加同一 Milvus 网络并从容器内确认 milvus:19530 可达后，重新执行交接。旧本地切换脚本的固定幂等键在恢复 v32 时产生 WEB_AUTHORITY_RECEIPT_CONFLICT，私有脚本现按 Bot 意图修订生成键；管理端拒绝复用键的行为正确。

第二次交接前，v32 的活动 Agent Run、任务、待发送及未结算嵌入均为空。正式管理动作使 v32 OFFLINE 后，树莓派 OneBot 连接数降为 0；随后 v33 ONLINE，Bot 运行图修订 5 为 ACTIVE，22 个模块加载，树莓派保持唯一一条连接。只读 OneBot get_status 返回 online=true、good=true、retcode=0。获准群仍仅为 933179609；本轮没有向 QQ 发送测试消息。

在真实 v33 实例中，Memory 管理端先用空代际 ID 查询最近图重建，得到预期的 NOT_FOUND；发起一次包声明的图代际重建后，任务经历 CLAIMED → SUCCEEDED，发布到修订 0，按管理员最近任务查询与按代际 ID 精确查询一致。数据库的图重建工作项为 SUCCEEDED，发布修订 0。这个验收不调用聊天或嵌入模型，也不证明非空图扩散性能。

v33 Supervisor 的 Docker 重启策略已设为 unless-stopped，容器仍健康运行；PostgreSQL 与 Milvus 亦为 unless-stopped。v32 Bot 的期望状态保持 OFFLINE，避免引擎重启后双 Bot 接管 QQ。这个设置仅证明自动启动配置已安装，尚未通过实际重启 Docker 引擎验收恢复链路。

截至 18:31，待验收项仍包括许可群真实用户的低频 @ → Agent → Provider → Delivery 回复、v33 新库内非空图文召回与长时间流式负载。没有把 QQ 登录、单连接或空图重建当作端到端 Agent 回复证明。若需回退，先确认 v33 没有活动 Agent/任务/待发送，令 v33 OFFLINE 并核对 OneBot 连接数为 0，再令 v32 ONLINE；两套记忆数据库仍独立。

2026-10-16 当前源码非空多模态复验：在与在线 v33 隔离的随机 PostgreSQL Bot 库和临时 Milvus 集合中运行 test_real_vl_png_and_gif_archive_cross_group_recall_and_management_trial。测试用 QiYu .env 声明的真实 VL 嵌入端点处理 PNG/GIF 文档与图片查询，验证 TEXT/IMAGE DOCUMENT facet READY、跨群 IMAGE→IMAGE 直接种子、原群/作者归因、跨主干联想和管理图文试验；OneBot、媒体抓取和聊天模型仍为测试替身。原测试只等待 GIF facet READY 就立即断言 PNG 也 READY，第一次可达供应商的运行在 PNG 尚为 DISPATCHED 时错误失败；改为同时等待两个独立来源结算后，1 passed in 151.33s。首次失败运行与复验均可能产生供应商计费，未获得供应商费用回执。测试 finally 删除独立 Milvus 集合与随机数据库，事后 forest_cross_vl_% 数据库为 0，测试容器已移除。此结果证明当前源码的非空图文链路，但不证明在线 v33 新库已有可召回 facet，也不证明真实 QQ @ 回复。

2026-10-16 19:16（北京时间）真实 QQ 静默图片接入：树莓派 NapCat 记录许可群 933179609 的一条图片消息。只读核对 v33 Bot 数据库：Adapter `platform_events` 与 Conversation `inbox_messages` 各新增 1 条，同一入站序号为 1；分类已结算为 `FINAL / NO_TRIGGER / mention_required`，作者为 `qq:2061865630`。Conversation `context_units` 中该 INPUT 单元仍位于 `active_context_units`，作者与 `NO_TRIGGER` 标记一致。媒体抓取和图片 part 均为 READY、尝试 1 次；Memory 来源单元为 `REGISTERED / IMAGE`，尚未因上下文裁剪而成为 ELIGIBLE，也没有据此宣称 READY facet。只读 OneBot `get_status` 返回 `online=true / good=true`，树莓派 8080 端口与 Forest 主机保持一条 ESTABLISHED WebSocket。此次未发送 QQ 消息或调用模型；真实 @ → Agent → Provider → Delivery 仍未验收。

2026-10-16 23:22（北京时间）真实回复链路复核：仅对在线 v33 的 Bot 库执行只读、可重复读查询，不读取消息正文，也没有新发 QQ 消息或调用模型。许可群 933179609 在 21:18、21:35、21:55 的三条 `bot_mentioned` 入站消息，各自形成 `TRIGGER → ANSWERED`、唯一 `claimed_run_ref` 和 `FINAL_REPLY` 归档；三个 Agent Run 均为 `COMPLETED`。四条 `provider.chat-completions` 模型请求及其传输尝试均为 `SUCCEEDED`：其中 21:35 的首次输出触发 `AGENT_PROTOCOL_XML`，协议失败被记录为 `REPAIR_READY`，第二次模型调用 `REPAIR` 成功，没有由架构层猜测或吞掉错误输出。三条 `answered_reply_ref` 均与 Delivery `CHAT` 意图的 `reply_ref/cause_id` 一致；对应单段 `send_group_msg` 传输尝试都有回执，段和意图均为 `SENT`。最近一条从 21:55:09 UTC+8 触发，到 21:56:01 UTC+8 发送结算。故上述三次真实 @ → Agent → Provider → Delivery 已获数据库因果链与 OneBot 发送回执证明；仍不能据此证明长时间流式负载、同群并发触发排队、跨群异步，或在线 v33 新库的非空图文召回。在线容器仍是从 c732bad 构建的旧镜像，本地后续提交未部署到 v33。
