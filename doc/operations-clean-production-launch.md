# 干净生产启动链

本文记录当前可执行的单 Bot 启动切片。它是最小 bootstrap 与冻结 RuntimeGraph 的生产入口，不是新的业务组合根。宿主只持有数据库连接池、schema bundle installer、HostLease 和 `RuntimeModuleRegistry`；Adapter、触发策略、Conversation、Agent、Memory、Provider 与 Delivery 均以平级 Generation 成员运行。

## 启动图

`forest bot` 在一个空的专用 PostgreSQL 数据库内安装并发布以下 15 个成员：

- `kernel.runtime`、`kernel.payload`、`kernel.authorization`、`kernel.effect`、`kernel.audit`；
- `provider.chat-completions`、`embedding.multimodal`、`memory.graph`、`media.capture`、`agent.runtime`；
- `conversation.trigger-policy`、`conversation.scheduler`；
- `adapter.onebot`、`delivery`、`shell.command`。

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

模型、Embedding 和 OneBot 凭据也可分别通过 `FOREST_MODEL_API_KEY_FILE`、`FOREST_EMBEDDING_API_KEY_FILE` 与 `FOREST_ONEBOT_ACCESS_TOKEN_FILE` 注入；同一种凭据不得同时设置文本值和文件。Compose 强制使用专用 secret volume，不再把三类凭据放进容器环境。先启动只具本地卷权限、无网络的初始化器，再从标准输入逐项安装凭据：

```sh
docker compose run --rm initialize
printf '%s' "$MODEL_API_KEY" | docker compose run --rm -T -e FOREST_SECRET_NAME=model_api_key secret
printf '%s' "$EMBEDDING_API_KEY" | docker compose run --rm -T -e FOREST_SECRET_NAME=embedding_api_key secret
printf '%s' "$ONEBOT_ACCESS_TOKEN" | docker compose run --rm -T -e FOREST_SECRET_NAME=onebot_access_token secret
printf '%s' "$MANAGEMENT_TOKEN" | docker compose run --rm -T -e FOREST_SECRET_NAME=management_token secret
```

安装器只接受这四个固定名称和单行可打印 ASCII 值；它会移除 Windows PowerShell 管道可能添加的单个 UTF-8 BOM，随后执行原子替换并把文件收敛为应用 UID 独占的 `0400`。它不会打印凭据。Compose 同样使用该受保护卷构造 Bot DSN，因此不要求显式提供 `FOREST_BOT_DSN`。可选项包括：

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

`media.capture` 独占 OneBot 临时图片定位符、下载客户端、重试租约和受管图片资产。Adapter 在 ingress 事务里只向 Conversation 发布稳定 `media_ref`，并登记异步捕获任务；签名 URL 不进入规范 Context。Worker 完成有界下载和图片解码验证后，通过 `memory.source.capture/1` 发布保持原段顺序的图文来源。Agent 在下一次模型调用前从 `media.context.resolve/1` 取得当前 Context 的不可变像素快照；text-only Provider 仍只在该次投影中省略图片。

当前环境变量承担首次 bootstrap 与无 WebUI 时的真实链路验收。设置 `FOREST_MANAGEMENT_TOKEN`（至少 16 字符）后，宿主同时启动首个干净管理面切片；默认监听 `127.0.0.1:8080`，容器内监听地址由 Compose 固定为 `0.0.0.0`，宿主端口仍只绑定 loopback。`FOREST_MANAGEMENT_HOST` 与 `FOREST_MANAGEMENT_PORT` 可调整监听。当前页面从准确 RuntimeGraph Registry 加载模块贡献，支持模块配置 plan/apply、热替换、Memory 的主干/来源/关系查询、Agent PresetContext/CorePrompt，以及 OneBot Binding 状态、fence 与端点变更；token 只保存在浏览器 `sessionStorage`。Binding 密钥不回显，当前页面只沿用 bootstrap secret 引用，不能更换访问令牌或 transport。该切片还不是最终独立 Supervisor/Web 身份控制面。环境变量不能演变为壳层专用 Bot 表单。

## 生命周期与恢复

启动顺序为 `install bundle → prepare → bind → prewarm → atomic publish → local activate`。OneBot WebSocket 在 prewarm 阶段缓冲事件，只有整个图发布后才开放接收。正常关闭先冻结图准入、逆序 quiesce/close、退休图，再显式释放 HostLease，因此同一数据库可立即由新的宿主 incarnation 接管。

新宿主取得租约后会把数据库中遗留的 OneBot 当前客户端结算为 `FAILED/HOST_RESTARTED`，再建立新客户端。该行为只用于进程已失去的本地连接资源；发送结果不确定性仍按 Delivery 的 `UNKNOWN` 规则处理，不能把未知发送自动重试成重复消息。

Compose 默认命令已经切到 `forest bot`，PostgreSQL 默认数据库为 `forest_bot`。容器在 `/data/runtime-status.json` 发布不含凭据的当前图身份，Docker 健康检查只接受已成功发布的图。开发期从旧 schema 切换时直接重建卷，不运行旧迁移链。

## 尚未接入本图的能力

- `shell.command` 的取消、重载、任务处理与审计查询命令；
- 模块贡献式管理协议的独立监督、Secret authority UI 与完整 Web 身份/授权；
- 多 Bot provisioning 与每 Bot 独立数据库/全局 Memory 域；
- OneBot 热替换排空、长连接抖动和真实群低频验收。

上述缺口不能由组合根中的特殊判断补齐，必须继续作为平级模块和内核协调 UoW 接入。
