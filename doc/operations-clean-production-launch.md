# 干净生产启动链

本文记录当前可执行的单 Bot 启动切片。它是最小 bootstrap 与冻结 RuntimeGraph 的生产入口，不是新的业务组合根。宿主只持有数据库连接池、schema bundle installer、HostLease 和 `RuntimeModuleRegistry`；Adapter、触发策略、Conversation、Agent、Memory、Provider 与 Delivery 均以平级 Generation 成员运行。

## 启动图

`forest bot` 在一个空的专用 PostgreSQL 数据库内安装并发布以下 13 个成员：

- `kernel.runtime`、`kernel.payload`、`kernel.authorization`、`kernel.effect`、`kernel.audit`；
- `provider.chat-completions`、`embedding.multimodal`、`memory.graph`、`agent.runtime`；
- `conversation.trigger-policy`、`conversation.scheduler`；
- `adapter.onebot`、`delivery`。

OneBot 消息进入后，Adapter 只做协议校验、身份归一化和载荷落库。`conversation.trigger-policy` 根据冻结配置给出 `TRIGGER` 或 `NO_TRIGGER`；内核 OneBot ingress UoW 在同一事务中提交平台事件、Conversation Inbox、群主干 Memory 来源与分类结果。Scheduler 因此不再依赖测试或管理端手工补写触发记录。

群策略当前支持 `MENTION`、`ALL` 和 `DISABLED`。`MENTION` 只在许可群内且消息明确 `@` 当前 Bot 身份时触发 Agent；未触发消息仍进入该群 Conversation 的有序 Inbox 和短期上下文流水。每个群继续由单 Actor 排队，不同群可以并行。

## 部署环境契约

直接启动至少需要：

```text
FOREST_BOT_DSN=postgresql://...
FOREST_MODEL_BASE_URL=https://provider.example/v1
FOREST_MODEL_API_KEY=...
FOREST_MODEL_NAME=...
FOREST_EMBEDDING_ENDPOINT=https://api.siliconflow.cn/v1/embeddings
FOREST_EMBEDDING_API_KEY=...
FOREST_ONEBOT_SELF_ID=123456789
FOREST_ONEBOT_WEBSOCKET_URL=ws://127.0.0.1:3001/
FOREST_ONEBOT_ACCESS_TOKEN=...
FOREST_ALLOWED_GROUP_IDS=933179609
```

模型、Embedding 和 OneBot 凭据也可分别通过 `FOREST_MODEL_API_KEY_FILE`、`FOREST_EMBEDDING_API_KEY_FILE` 与 `FOREST_ONEBOT_ACCESS_TOKEN_FILE` 注入；同一种凭据不得同时设置文本值和文件。Compose 使用受保护文件构造 Bot DSN，因此不要求显式提供 `FOREST_BOT_DSN`。可选项包括：

```text
FOREST_BOT_ID=forest-main
FOREST_MODEL_DEPLOYMENT_ID=provider.chat-completions/main
FOREST_MODEL_CONTEXT_LIMIT=32768
FOREST_MODEL_MAX_OUTPUT_TOKENS=4096
FOREST_MODEL_INPUT_MODALITIES=text
FOREST_EMBEDDING_DEPLOYMENT_ID=embedding.multimodal/qiyu-vl/default
FOREST_EMBEDDING_MODEL_REVISION=operator-declared/1
FOREST_EMBEDDING_MODEL=Qwen/Qwen3-VL-Embedding-8B
FOREST_EMBEDDING_DIMENSIONS=4096
FOREST_GROUP_TRIGGER_MODE=MENTION
FOREST_PRIVATE_ENABLED=true
FOREST_AGENT_SYSTEM_PROMPT=...
```

`FOREST_AGENT_SYSTEM_PROMPT` 是 Agent 模块拥有的 PresetContext。不可修改的核心协议仍来自 `agent_protocol.CORE_PROMPT`，包含单 Agent 身份、作者隔离、工具调用和输出语法约束。

`embedding.multimodal` 独占 VL 请求投影、凭据、HTTP 客户端、传输尝试和用量回执；`memory.graph` 只通过冻结的 `embedding.encode/1` 路由提交已获准的文本或图片 facet。两者的发送边界在同一数据库事务中把 Provider 尝试标为 `SENT`、把 Memory 尝试标为 `DISPATCHED`。Embedding 的 deployment、模型版本、模型种类、维度与派生 space 在 Bot 首次初始化时冻结；配置 plan/apply 可以替换 endpoint、凭据引用、超时和连接数，新 Generation 会采用新传输配置。改变冻结 profile 会以 `EMBEDDING_PROFILE_IMMUTABLE` 拒绝启动。

当前环境变量只承担首次 bootstrap 与无 WebUI 时的真实链路验收。最终管理入口必须通过模块贡献式 WebUI 读写同一配置协议，不能把这些字段重新固化成壳层专用 Bot 表单。

## 生命周期与恢复

启动顺序为 `install bundle → prepare → bind → prewarm → atomic publish → local activate`。OneBot WebSocket 在 prewarm 阶段缓冲事件，只有整个图发布后才开放接收。正常关闭先冻结图准入、逆序 quiesce/close、退休图，再显式释放 HostLease，因此同一数据库可立即由新的宿主 incarnation 接管。

新宿主取得租约后会把数据库中遗留的 OneBot 当前客户端结算为 `FAILED/HOST_RESTARTED`，再建立新客户端。该行为只用于进程已失去的本地连接资源；发送结果不确定性仍按 Delivery 的 `UNKNOWN` 规则处理，不能把未知发送自动重试成重复消息。

Compose 默认命令已经切到 `forest bot`，PostgreSQL 默认数据库为 `forest_bot`。容器在 `/data/runtime-status.json` 发布不含凭据的当前图身份，Docker 健康检查只接受已成功发布的图。开发期从旧 schema 切换时直接重建卷，不运行旧迁移链。

## 尚未接入本图的能力

- OneBot 图片抓取与受管媒体生命周期；
- `shell.command` 的完整 Generation 装配及群管理员授权；
- 模块贡献式管理协议与紧凑 WebUI；
- 多 Bot provisioning 与每 Bot 独立数据库/全局 Memory 域；
- OneBot 热替换排空、长连接抖动和真实群低频验收。

上述缺口不能由组合根中的特殊判断补齐，必须继续作为平级模块和内核协调 UoW 接入。
