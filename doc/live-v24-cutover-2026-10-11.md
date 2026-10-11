# v24 隔离实例切换（2026-10-11）

源码提交 `735fbb4` 构建为 `forest:runtime-candidate-735fbb4`。本轮新增检索区域截断原因，`memory.graph` 1.93.0 / SQL 3.91.0、`agent.runtime` 4.48.0 与 `conversation.scheduler` 4.37.0 协同发布；记忆证据协议升为 `forest.memory-evidence/3`。隔离 PostgreSQL 中的高扇出冻结链路和管理/模型投影相关 57 个用例通过，文本群聊触发至 Provider 请求的端到端用例通过，另外两个 Memory/Conversation 包用例通过。

v24 复用现有 PostgreSQL 和 Milvus 服务，但使用独立的 `forest_deployment_v24`、`forest_host_v24`、新 Bot 物理库及 `forest-candidate-v24_packages` 包卷。部署身份为 `forest/candidate-v24-20261011`，Supervisor 为 `forest-candidate-v24-supervisor`，管理入口为 `http://127.0.0.1:18095`；管理员账号沿用上一候选的本地私有配置，首次配对凭据已单独更新。Bot `forest-main-v24` 从已验证的 v22 配置重建，默认仅许可群 `933179609` 显式 @ 触发，OneBot 指向 `192.168.31.117:8080`，VL 嵌入模型仍为 `Qwen/Qwen3-VL-Embedding-8B`。本次没有复制旧群聊、记忆或运行记录。

新 Bot 先供应到 `READY/OFFLINE`：22 个 schema bundle 为 READY，其中 `memory.graph` 3.91.0、`vector.index` 2.19.0；初始 Inbox、Context、Agent Run 和 Memory source 均为空。然后通过 v23 管理端将 `forest-main-v23` 设为 `OFFLINE`，确认实际状态 OFFLINE，再通过 v24 管理端将 `forest-main-v24` 设为 `ONLINE`。切换后 v24 图 1 的 22 个 Generation ACTIVE，Supervisor healthy；到树莓派 OneBot 的 ESTABLISHED TCP 连接数为 v23 0、v24 1，未出现双 Bot 同时接收同一 QQ 账号事件。

切换后只读探针看到 v24 的 2 条新入站均来自非许可群，均为 `FINAL/NO_TRIGGER group_not_allowed`；对应 Context 与 Memory 来源各 2 条，无缺失来源，Agent Run 为 0。许可群尚无真实 @ 入站，本次不能据此认定最新版本已完成模型回复、发送与长期图文负载验收。

如需回退，必须先在 `127.0.0.1:18095` 将 `forest-main-v24` 设为 `OFFLINE` 并确认实际离线及 OneBot 连接关闭，再在 `127.0.0.1:18094` 将 `forest-main-v23` 设为 `ONLINE`。两边物理库和包卷暂时保留，历史数据没有自动合并路径。

后续验收（2026-10-11）：当前源码的多 Bot Supervisor 生命周期与假 OneBot 跨群 Agent 端到端场景合计 4 passed（107.43 秒），覆盖多实例监督、跨群独立推进及来源归因。v24 管理面分别提交一次纯文本和一次 64×64 PNG 图片召回试验，二者均到 READY、结果为 0 项且 `truncation_reason=null`；只读账本确认各有一个 READY 的 QUERY facet，累计两次已结算嵌入请求、48 个计费 token、0 次 UNKNOWN。该结果证明真实 VL 嵌入端点接受两种输入并完成冻结，不证明非空图中的跨模态命中；许可群 @→回复仍待独立验收。
