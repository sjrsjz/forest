# v25 隔离实例切换（2026-10-11）

提交 `f08ceac` 构建为 `forest:runtime-candidate-f08ceac`。内核 `runtime.effect-executor-outcome/2` 只按冻结路由与通用服务账本查询回执；Agent 从冻结 Model Request 取得模型 wire 摘要，Provider 负责模型载荷校验。`agent.runtime` 4.49.0 / SQL 2.32.0。Agent 纯逻辑 15 项、Agent 包隔离 PostgreSQL 1 项、BotHost 两种效果恢复各 1 项通过；Ruff 与编译检查通过。

v25 使用独立的 `forest_deployment_v25`、`forest_host_v25`、Bot 物理库和 `forest-candidate-v25_packages` 包卷。部署身份 `forest/candidate-v25-20261011`，Supervisor `forest-candidate-v25-supervisor`，管理入口 `http://127.0.0.1:18096`。Bot `forest-main-v25` 从已验证的 v22 配置重新供应，默认仅许可群 `933179609` 显式 @ 触发；未复制旧消息或记忆。首次供应为 READY/OFFLINE，22 个 schema bundle 为 READY。

首次启动遗漏 Milvus 所在 `forest-clean_default` 网络，导致 `VECTOR_INDEX_UNAVAILABLE`。期间撤回 v25 意图并恢复 v24，确认 OneBot 连接为 v24=1/v25=0。补齐网络后，按 v24 OFFLINE → 连接数 0 → v25 ONLINE 重新切换。当前 v25 Supervisor healthy、图 8 为 22/22 ACTIVE，向量绑定修订 1 为 READY，无未完成 claim；v24/v25 到树莓派 OneBot 的 ESTABLISHED 连接为 0/1。失败图 1–7 均 STOPPED。首条非许可群入站已完整进入 Context/Memory，Agent Run 为 0。

许可群真实 @→Agent→Provider→Delivery 回执尚待低频人工验收，不能用单连接或非许可群入站代替。回退时先把 v25 Bot 设为 OFFLINE 并确认连接退出，再将 v24 Bot 设为 ONLINE；两边物理库和包卷均保留。

## 2026-10-11 Supervisor 镜像更新

`3577fc2` 修复失败启动后撤回意图的生命周期收敛。v25 Supervisor 停机后保留旧容器 `forest-candidate-v25-supervisor-backup-f08ceac`，用 `forest:runtime-candidate-3577fc2` 重新创建同名容器，并沿用原部署、Host、Bot 数据库、包卷、密钥、管理端口；PostgreSQL 与 Milvus 网络均在启动前接通。

新容器 healthy、重启次数 0，图 10 为 22/22 ACTIVE，v24/v25 到树莓派 OneBot 的 ESTABLISHED 连接为 0/1。只读核查为 inbox/context 各 35 条、Agent Run 0；许可群两条消息都判定为 `mention_required`。真实显式 @→Agent→Provider→Delivery 尚待许可群低频验收；本次未主动发送 QQ 消息。失败启动后撤回的 OFFLINE 分支已在隔离 PostgreSQL 回归验证，未在在线 Bot 上人为制造失败。

## 2026-10-11 许可群静默诊断

只读检查截至 `2026-10-11T05:00:22Z` 的入站元数据：49 条消息全部进入 Context，48 条建立 Memory 来源；唯一缺失来源是非许可群图片像素超限，已以 `MEDIA_CAPTURE_UNAVAILABLE` 记账。许可群 10 条消息均为 `mention_required`，Agent Run 仍为 0。逐条对照 OneBot 原始段和规范化段后确认 @ 未在适配器丢失：v25 登录账号 `3605196653`，许可群现有 @ 段指向其他账号，另有仅含回复引用而无 @ 的消息。当前 MENTION 策略要求显式 @ 登录账号；等待一次明确 @3605196653 的低频真人验收。

## 2026-10-11 Web 管理入口实调

使用 v25 管理员会话经 `/management/v1/scopes/resolve` 解析 Bot 管理 scope，再按 scope 路由调用 `vector.index.summary`：索引健康 `READY`、引用水位 0、绑定修订 1、无活动重建。经同一 Bot scope 下载 `vector.index.workspace` 的包自带管理页，15,728 字节与运行时贡献清单的 SHA-256 相符。直接把 `forest-main-v25` 作为 URL 中的 scope ID 会得到 `BOT_MANAGEMENT_SCOPE_NOT_FOUND`，因为 Bot 身份与管理 scope ID 是不同字段；本次未发起重建或其他写动作。

Memory 管理入口同样经真实会话验证：按许可群号筛选主干得到 1 条，模块自带页面 81,413 字节与运行时 SHA-256 一致。来源查询默认只看 `ELIGIBLE`，此时返回 0；明确筛选 `REGISTERED` 后首个 2 条分页存在且 `has_more=true`，两条均为文本、`NO_FACETS`。这说明这些来源尚未取得归档资格：Memory 在归档资格提交后才把来源升为 `ELIGIBLE` 并入列文档 facet 工作；不是适配器丢失入站。管理页已提供“待归档”筛选。该在线检查未人为推动归档或调用嵌入服务。
## 2026-10-11 部署包 1.6.3 在线替换

提交 `12dbd5c` 构建 `forest:runtime-candidate-12dbd5c`。停下原 Supervisor 后将其保留为 `forest-candidate-v25-supervisor-backup-3577fc2`，新容器沿用独立 deployment/host/Bot 库、包卷、密钥和本机管理端口，并在启动前接入 PostgreSQL 与 Milvus 网络。新 Supervisor 为 `running/healthy`、重启次数 0；v24/v25 到树莓派 OneBot 的 ESTABLISHED 连接数为 0/1。

重启后部署图仍选中 `deployment.package-registry` 1.6.2；镜像升级不会擅自覆盖已发布的包选择。目录已包含官方 1.6.3 工件 `sha256:277c38df2e8eaacbb9f074782ecea395fd2da54bcfb1745a7faa5bcf3164f701`。通过认证管理端读取图修订 4，预览只替换该模块、保留其他 7 个部署模块，再提交带修订和计划摘要的 `module.replace`。操作 `web:sha256:94dcabcf01f9e99c4a2d89f9b269b70b05e15084b4c72bef5818fc3e72dd5c3d` 结算 `SUCCEEDED`；部署图修订 5 的该模块 `ACTIVE`，版本 1.6.3。此步骤验证了部署模块的真实在线代际热替换，而不是仅验证进程镜像更新。

替换后 Web 管理端的向量索引查询为 READY、绑定修订 1；向量与记忆包自带页面字节均匹配运行时声明的 SHA-256。Bot 图 12 为 22/22 ACTIVE，Supervisor 仍 healthy 且连接数仍为 0/1。只读 Bot 库核对 inbox/context 各 203 条、Memory 来源 202 条、Agent Run 0；203 条全部为 NO_TRIGGER（非许可群 189、许可群未明确 @ 当前账号 14）。唯一未建来源的消息由图片像素超限明确记为 `MEDIA_CAPTURE_UNAVAILABLE`。本次未主动发送 QQ 消息；许可群明确 @ 当前登录账号后的 Agent→Provider→Delivery 回执仍待低频真人验收。
## 2026-10-11 媒体采集传输宿主更新

提交 `0968bfb` 修复原生图片采集端的 DNS 验证/连接地址替换窗口，构建 `forest:runtime-candidate-0968bfb`。停机后保留原容器为 `forest-candidate-v25-supervisor-backup-12dbd5c`，新容器沿用 v25 的隔离数据库、包卷、只读密钥与 bootstrap、本机管理端口和资源/安全限制，并在启动前连接 PostgreSQL 与 Milvus 网络。旧容器停下后才启动新容器，避免同时连接 OneBot。

恢复期间管理端先可用，Bot scope 曾短暂返回 `BOT_MANAGEMENT_UNAVAILABLE`，此时没有重启进程；随后 Bot 图 14 的 22/22 成员 ACTIVE，Supervisor `running/healthy`、重启次数 0。v24/v25 到树莓派 OneBot 的 ESTABLISHED 连接恢复为 0/1。认证管理端的向量与记忆查询、两份包自带页面 SHA-256 核对通过；部署图仍选中 `deployment.package-registry` 1.6.3。只读 Bot 库核对 inbox/context 各 234、Memory 来源 233、Agent Run 0；唯一缺失来源仍是已明确结算 `MEDIA_CAPTURE_UNAVAILABLE` 的超像素图片。最近入站已到 `2026-10-11T07:33:04Z`，许可群 14 条现有消息均为 `mention_required`。本次未主动发送 QQ 消息，明确 @ 当前 Bot 后的真实回复仍待低频验收。
## 2026-10-11 真实 VL 管理召回试验

在 v25 认证 Bot 管理 scope 中分别上传一段短文本和一张本地生成的 64×64 PNG，以 `memory.trial.start` 各发起一次查询。两个试验均经过 CLAIMED/PENDING 到达 READY；`memory.trial.result` 各返回 0 项、`truncated=false`、无截断原因。只读 Bot 库进一步核对：两个查询分别有一个 `TEXT` 和 `IMAGE` 的 READY QUERY facet，预算账本累计 2 次已结算嵌入请求、48 个计费 token、0 次 UNKNOWN。当前库的群聊来源尚未归档为 ELIGIBLE，故空结果符合资格状态；这验证真实 QiYu VL 端点接受文本与图片并完成查询冻结，不证明非空图的跨模态命中。本次没有向 QQ 发送消息。


## 2026-10-11 已响应模型结果的阻塞诊断

许可群 933179609 的一个 Run 自 10:25:50 UTC 起保持 BLOCKED_UNKNOWN，10:35:50 UTC 截止时间已过。只读账本确认 Provider 在 10:26:46 UTC 保存了 model.result 和 model.usage；结果为 RESPONDED、finish_reason=length、text 为空、error_code=MODEL_OUTPUT_INCOMPLETE。旧版 Provider 4.4.1 的派发输出 schema 只接受 RESPONDED.error_code=null，故通用服务账本把已经完成的调用记为 RUNTIME_SERVICE_OUTPUT_INVALID；旧版只读 lookup 又拒绝该真实结果，Agent/Effect 无法对账。不能把原 Attempt 当作未发送重试。

提交 85c7d34 将 Provider 升至 4.4.2、Agent 升至 4.49.1，保留已响应质量错误并经原 Attempt 转成协议修复或终结事实；过期 Run 不再发起修复调用。57 项模型/Agent 定向离线测试通过，两包在本地和候选镜像内部的工件摘要一致。仅覆盖这八个修复文件的镜像 forest:runtime-candidate-quality-85c7d34 已构建，基础镜像仍是在线使用的 0968bfb；预创建容器的环境、挂载、网络、端口与资源限制均与线上一致，但未启动，随后已清理。在线 v25 仍使用原镜像和原包。

12:43 UTC 更新后的只读观察器确认 Supervisor healthy、22/22 ACTIVE，群分类水位 22、Context 水位 17、活动 Run 1 且已过期，明确报 AGENT_RUN_DEADLINE_EXPIRED。当前 Agent 的 replacement guard 对绑定被替换 Provider 的 UNKNOWN 请求给出 blocker，因此不能直接热换 Provider；镜像更新本身也不会覆盖已发布包选择。需先通过受限、可验证的原 Attempt 对账端口收敛旧 Run，或在全新隔离 Bot 中完成切换。没有停止在线容器、修改线上库或发送 QQ 消息。


## 2026-10-11 故障实例暂时下线

只读服务账本确认旧 Provider lookup 每次都将真实已响应结果判为 PROVIDER_RESULT_CORRUPT，Conversation Agent owned-work 随后以约一秒间隔重领，累计 6415 条失败操作。通过现有认证管理端用新的幂等键把 forest-main-v25 运行意图设为 OFFLINE，观察状态也已收敛为 OFFLINE；Supervisor、数据库、Provider 回执及原容器均保留。之后只读复查失败操作仍为 6415 条，最新时间固定在 12:55:00 UTC，没有继续增长。该动作没有删除消息/记忆、修改模型账本或发送 QQ 消息。重新设为 ONLINE 前必须先解决 UNKNOWN 对账或换用已验证的新隔离 Bot。


## 2026-10-11 原结果对账与在线恢复

确认 Bot 处于 OFFLINE 且 component.worker-host 选择账本没有 Agent/Provider 的显式固定摘要后，使用已验证约束的 forest:runtime-candidate-quality-85c7d34 替换 Supervisor 容器；旧容器停止并保留为 forest-candidate-v25-supervisor-backup-0968bfb-quality。新 Supervisor healthy、重启 0，沿用原部署/Host/Bot 库、密钥、包卷、管理端口及两个网络。通过原管理契约把 Bot 设回 ONLINE 后，冷启动图修订 16 的 22/22 成员 ACTIVE：Provider 4.4.2 摘要 sha256:cc22d87f233cc3cdcaaf734db54a54e46701faaefa5b49c6c97684bce1dbee1e，Agent 4.49.1 摘要 sha256:d0e2061d94285a0c4467f27ca2bff0e5ade9fc5612dbe382e90511f55bb0191d。由于旧活动图已停止，这是正常冷启动装配，未绕过活动图的 UNKNOWN 热替换围栏。

旧 Run run:9b104319e5bd04f994e7bbd7d069887b9bb094f742bb81d224c018518e1eeca2 在 13:05:08 UTC 终结为 FAILED/AGENT_PROTOCOL_FAILED，协议失败事实为 AGENT_PROTOCOL_OUTPUT_INCOMPLETE。原 Request 只有 1 次 Attempt，原 Effect 只有 1 次；两者经已保存的 Provider 结果回查收敛为 SUCCEEDED，没有第二次模型派发。许可群 933179609 的分类与 Context 水位均为 25，执行槽已释放。20 秒三次只读观察均为零违约、活动 Run 0、过期 Run 0、待发送 0，最近 QQ 发送仍为 08:46:52 UTC 的旧回执。本次恢复没有向 QQ 主动发送新消息，也没有删除历史数据库。


## 2026-10-11 召回成本上限调整

对已终结的两次许可群模型请求只读统计：两次 PRIMARY 的输出上限均为 8192 token；首次补全使用 3016 token，故障补全恰好使用 8192 token 且 finish_reason=length。故障补全输入计费 36717 token；冻结 wire 有 35 条消息，其中 32 条是 Memory 证据、10 张图片，序列化体积约 1.7 MB、文本字段共约 7 万字符；CorePrompt 约 4.8 千字符，当前 Context 仅 1 条。Conversation 发布的 ModelView 预算为估算 32768 token，Memory 等固定成本占 32495，最近群聊 Context 的 17 条可选记录均未进入这次模型请求。估算量与供应商实际计费量不同，不能让 Provider 以估算超限拒绝派发；但这揭示 Memory 先占固定预算会挤掉正常群聊上下文。

通过 v25 的正式 Web 配置计划/应用契约，将 memory.graph 检索上限从 32 节点、24000 字符、12 MiB 图片调整为 8 节点、8000 字符、2 MiB 图片，保留图文召回能力。desired/effective 均已收敛至修订 2，图从 16 热替换到 17，22/22 ACTIVE；Memory 1.93.0、Agent 4.49.1、Provider 4.4.2，许可群分类/Context 水位均为 29、无活动 Run 或待发送。当前没有为验证新上限而主动触发模型 API 或 QQ 消息，实际下次模型计费仍需低频观察。源码默认值另在 memory.graph 1.93.1 更新；源码已新增 Conversation 所有的 Memory/Context 联合选择与 Agent 按计划投影（conversation.scheduler 4.38.0 / agent.runtime 4.50.0），当时线上仅有上述 Memory 检索限额，后续切换见下。

联合 ModelView 包切换（14:51 UTC 后）：提交 `0b434e4` 只覆盖 Agent 与 Conversation 的八个源码/清单文件，构建 `forest:runtime-candidate-joint-0b434e4`（镜像清单摘要 `sha256:79fb84a67514d0e5137412ecb08385d27da4fe71e1c5b60276c775645b4862a0`）。离线容器中构建出 Agent 4.50.0 工件 `sha256:d9c2115cc039977f74f99c42ce33099586a2441fe598e6d0381f2151f988f7c5` 和 Conversation 4.38.0 工件 `sha256:088329f2d59402987a066e88fe51f04cb865a98bb0506786a2cdf34eda6548c6`，与最终本机构建一致。切换前无活动 Agent Run、待发送、嵌入 DISPATCHED 或未结算预留；一笔更早的嵌入 UNKNOWN 已持久化，涉及 8 个 DOCUMENT facet 和 32768 token 不确定占额，未对其重试。旧 Supervisor 停机保留为 `forest-candidate-v25-supervisor-backup-joint-0b434e4`；新容器复用三个原挂载、2 GiB/3 CPU 约束和管理端口，并在启动前接通 PostgreSQL 与 Milvus 网络。新图修订 19 为 22/22 ACTIVE，Agent/Conversation 实际版本分别为 4.50.0/4.38.0，Memory/Provider 仍为 1.93.0/4.4.2。三次只读采样均无活动 Run 或待发送；认证管理端核对 Memory desired/effective 修订 2，检索上限仍为 8 节点、8000 字符和 2 MiB 图片。部署观察 ONLINE、HTTP 200，唯一观察器违约仍是切换前已有的 `DOCUMENT_EMBEDDING_UNKNOWN`（1 个请求、8 个 facet），未变成新派发或新未知。此次没有主动发送 QQ 消息或调用聊天模型；联合视图的真实供应商 token 降幅尚未测量。

切换后对嵌入 UNKNOWN 的只读追查：唯一批次于 14:49:30 UTC 派发、14:49:31 UTC 结算，含 8 个文档 facet。Kernel 服务操作 `embedding.encode` 本身成功持久化了 Provider 受限回执；回执为 `status=UNKNOWN`、`error_code=EMBEDDING_HTTP_UNCERTAIN`、`http_status=503`。Memory attempt 与预算 reservation 均为 UNKNOWN，原 32768 token 占额保留，未执行 `memory.embedding-unknown.retry`。近期另外统计到本地 `OVERLOADED` 工作错误 10 次，但现有证据不能将其等同于本次上游 HTTP 503。不得以 503 猜测供应商是否计费或无条件再次派发。
