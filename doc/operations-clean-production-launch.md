# 干净生产启动链

本文记录当前可执行的单 Bot 启动切片。它是最小 bootstrap 与冻结 RuntimeGraph 的生产入口，不是新的业务组合根。宿主只持有数据库连接池、schema bundle installer、HostLease 和 `RuntimeModuleRegistry`；Adapter、触发策略、Conversation、Agent、Memory、Provider 与 Delivery 均以平级 Generation 成员运行。

## 启动图

`forest bot` 在一个空的专用 PostgreSQL 数据库内安装并发布以下 16 个成员：

- `kernel.runtime`、`kernel.payload`、`kernel.authorization`、`kernel.effect`、`kernel.audit`；
- `provider.chat-completions`、`embedding.multimodal`、`memory.graph`、`media.capture`、`agent.runtime`；
- `conversation.trigger-policy`、`conversation.scheduler`；
- `adapter.onebot`、`delivery`、`job.scheduler`、`shell.command`。

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

直接运行 `forest bot` 时，模型、Embedding 和 OneBot 凭据也可分别通过 `FOREST_MODEL_API_KEY_FILE`、`FOREST_EMBEDDING_API_KEY_FILE` 与 `FOREST_ONEBOT_ACCESS_TOKEN_FILE` 注入；同一种凭据不得同时设置文本值和文件。这是显式诊断路径，当前 Compose 不再包含单 Bot 服务。

Compose 先启动只具本地卷权限、无网络的初始化器，再从标准输入安装两个部署根凭据：

```sh
docker compose run --rm initialize
python -c 'import base64,secrets,sys;sys.stdout.write(base64.b64encode(secrets.token_bytes(32)).decode())' | docker compose run --rm -T -e FOREST_SECRET_NAME=deployment_master_key secret
printf '%s' "$DEPLOYMENT_MANAGEMENT_TOKEN" | docker compose run --rm -T -e FOREST_SECRET_NAME=deployment_management_token secret
```

安装器只接受上述两个 allowlist 名称和单行可打印 ASCII 值；它会移除 Windows PowerShell 管道可能添加的单个 UTF-8 BOM，随后执行原子替换并把文件收敛为应用 UID 独占的 `0400`。它不会打印凭据。Compose 使用同一受保护卷中的 PostgreSQL 口令构造部署 DSN；模型、Embedding 与 OneBot 凭据通过 deployment WebUI/API 写入加密 Secret authority，只在准确 Bot 供应与启动边界解析。显式诊断入口的可选项包括：

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

直接 `forest bot` 时，环境变量只承担无 WebUI 的显式链路诊断。设置 `FOREST_MANAGEMENT_TOKEN`（至少 16 字符）后，该 Bot 宿主可启动自己的模块管理面；页面从准确 RuntimeGraph Registry 加载模块贡献，支持模块配置 plan/apply、热替换、Memory 的主干/来源/关系查询、Agent PresetContext/CorePrompt、Run/Tool/Job 控制以及 OneBot Binding 管理。生产 Compose 的部署控制面监听 `127.0.0.1:8080`（容器内 `8081`），token 只保存在浏览器 `sessionStorage`。

## 生命周期与恢复

启动顺序为 `install bundle → prepare → bind → prewarm → atomic publish → local activate`。OneBot WebSocket 在 prewarm 阶段缓冲事件，只有整个图发布后才开放接收。正常关闭先冻结图准入、逆序 quiesce/close、退休图，再显式释放 HostLease，因此同一数据库可立即由新的宿主 incarnation 接管。

新宿主取得租约后会把数据库中遗留的 OneBot 当前客户端结算为 `FAILED/HOST_RESTARTED`，再建立新客户端。该行为只用于进程已失去的本地连接资源；发送结果不确定性仍按 Delivery 的 `UNKNOWN` 规则处理，不能把未知发送自动重试成重复消息。

Compose 只编排 `initialize`、`postgres` 和 `supervisor`，镜像默认命令也是 `forest supervisor`。PostgreSQL 首次初始化只创建 `forest_deployment`；供应器为每个 Bot 按 Bot ID 派生稳定库名和无特权 login role，为该 role 生成随机口令，只允许在同一批准集群创建，并复核 role flags 与数据库 owner。Bot 运行时不持有 deployment 超级用户凭据。Supervisor 的健康检查只探测 loopback 管理监听，不读取管理 token。开发期从旧 schema 切换时直接重建卷，不运行旧迁移链。

## 多 Bot Supervisor 入口

`forest supervisor` 使用独立部署数据库运行持久供应 Worker 与多 Bot 生命周期协调器。最小进程配置为：

```text
FOREST_DEPLOYMENT_DSN=postgresql://...
FOREST_DEPLOYMENT_ID=forest/default
FOREST_DEPLOYMENT_ACTIVE_KEY_ID=master/default/1
FOREST_DEPLOYMENT_MASTER_KEY_FILE=/run/secrets/deployment_master_key
FOREST_DEPLOYMENT_MANAGEMENT_TOKEN_FILE=/run/secrets/deployment_management_token
FOREST_DEPLOYMENT_MANAGEMENT_HOST=127.0.0.1
FOREST_DEPLOYMENT_MANAGEMENT_PORT=8081
FOREST_SUPERVISOR_POLL_SECONDS=1
FOREST_SUPERVISOR_MAX_STARTING=2
```

主密钥文件只包含一个 Base64 值，解码后必须恰好为 32 字节。当前入口使用该 key ID 解密部署 Secret authority，并从 READY Bot 固化的 storage/bootstrap profile 构造各自 `CleanBotHost`；profile 中只保存 Secret 引用，凭据不会写入 Bot catalog。供应时只安装模块 authority 和 revision 1，不启动 Agent、Provider 或 OneBot 网络资源；供应 Job 成功发布 READY 后，Supervisor 才依据 runtime intent 启停 Bot。

Compose 中无需设置 `FOREST_DEPLOYMENT_DSN`：Supervisor 从 `/run/forest-secrets/postgres_password` 构造只指向 `forest_deployment` 的 DSN。显式 DSN 仅供非 Compose 部署。默认 `docker compose up -d` 启动 `initialize`、`postgres` 和 `supervisor`；Compose 中不存在绕过部署控制面的单 Bot 服务。

Supervisor 同时启动多作用域 WebUI。页面从 deployment provider 的三个控制面组件动态发现 Bot 目录、Secret 和 Provisioning 工作区；可以写入不回显的版本化 Secret、创建只暴露引用的本地 PostgreSQL storage profile，并从 Bootstrap Contributor 目录逐模块设置 Provider、VL Embedding、触发策略、Shell、Agent 和 OneBot 后冻结 profile、加入供应队列。页面不再接收整份组件 JSON 或客户端 schema digest。Bot 进入 ONLINE 后，壳层作用域选择器会通过注册的 Bot provider 打开该 Bot 的准确 RuntimeGraph 模块工作区；壳层不把 Agent、Memory、OneBot 或 Job 页面拷贝到 deployment 后台。下线先禁止新管理调用，已接纳调用排空后再关闭 Bot Host。管理 token 仍是部署级共享 token，完整 Web 身份/细粒度授权和主密钥多版本 keyring 仍是发布前缺口。

## 尚未接入本图的能力

- 受控原生 `ExecutionProfile` 的隔离执行后端；模块贡献式持久任务、群聊命令和 Web 工作区已经进入当前图；
- 完整 Web 身份/细粒度授权、主密钥多版本 keyring 和跨 Bot 模块工作区路由；Supervisor 默认 Compose 编排、deployment scope 管理贡献、Secret UI、本地 PostgreSQL 动态分配、正式 provisioner、`forest supervisor` 入口、独立部署目录、不可变组件级 bootstrap profile、持久供应 Job 与带 fence 的多宿主协调器已经落地，每个 Bot 仍使用独立存储引用与全局 Memory 域；
- OneBot 热替换排空、长连接抖动和真实群低频验收。

上述缺口不能由组合根中的特殊判断补齐，必须继续作为平级模块和内核协调 UoW 接入。
