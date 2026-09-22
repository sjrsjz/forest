# 干净生产启动链

本文记录当前可执行的单 Bot 启动切片。它是最小 bootstrap 与冻结 RuntimeGraph 的生产入口，不是新的业务组合根。宿主只持有数据库连接池、schema bundle installer、HostLease 和 `RuntimeModuleRegistry`；Adapter、触发策略、Conversation、Agent、Memory、Provider 与 Delivery 均以平级 Generation 成员运行。

## 启动图

`forest bot` 在一个空的专用 PostgreSQL 数据库内安装并发布以下 18 个成员：

- `kernel.runtime`、`kernel.payload`、`kernel.authorization`、`kernel.effect`、`kernel.audit`；
- `provider.chat-completions`、`embedding.multimodal`、`vector.index`、`memory.graph`、`media.capture`、`agent.runtime`；
- `conversation.trigger-policy`、`conversation.scheduler`；
- `adapter.onebot`、`delivery`、`process.executor`、`job.scheduler`、`shell.command`。

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
python -c 'import base64,json,secrets,sys; kid="master/default/1"; sys.stdout.write(json.dumps({"protocol":"forest.deployment-master-keyring/1","active_key_id":kid,"keys":{kid:base64.b64encode(secrets.token_bytes(32)).decode()}},separators=(",",":")))' | docker compose run --rm -T -e FOREST_SECRET_NAME=deployment_master_keyring secret
SETUP_ID="setup/$(python -c 'import secrets; print(secrets.token_urlsafe(18))')"
PAIRING_CODE="$(python -c 'import secrets; print(secrets.token_urlsafe(48))')"
python -c 'from datetime import datetime,timedelta,timezone; import json,sys; now=datetime.now(timezone.utc); sys.stdout.write(json.dumps({"protocol":"forest.deployment-web-bootstrap/2","setup_id":sys.argv[1],"pairing_code":sys.argv[2],"issued_at":now.isoformat(),"expires_at":(now+timedelta(hours=1)).isoformat()},separators=(",",":")))' "$SETUP_ID" "$PAIRING_CODE" | docker compose run --rm -T -e FOREST_SECRET_NAME=deployment_web_bootstrap secret
printf 'Forest WebUI 一次性配对码（1 小时有效）：%s\n' "$PAIRING_CODE"
```

安装器只接受上述两个 allowlist 名称和单行可打印 ASCII 值；它会移除 Windows PowerShell 管道可能添加的单个 UTF-8 BOM，随后执行原子替换并把文件收敛为应用 UID 独占的 `0400`。Secret 容器不回显内容；上面的本地命令只把新生成的配对码交付给当前操作员。启动 Supervisor 后访问 WebUI，输入该配对码并创建首位管理员。账号密码不写入 bootstrap 文件，配对码不放入 URL 或服务日志，成功后数据库会清除其摘要并永久关闭首次设置入口。若配对码在完成前过期，保留原 `SETUP_ID`、生成新的配对码和时间窗、重新安装该 secret 并重启 Supervisor；未过期凭据不能被替换，完成状态也不能被轮换重新打开。Compose 使用同一受保护卷中的 PostgreSQL 口令构造部署 DSN；模型、Embedding 与 OneBot 凭据通过 deployment WebUI/API 写入加密 Secret authority，只在准确 Bot 供应与启动边界解析。显式诊断入口的可选项包括：

```text
FOREST_BOT_ID=forest-main
FOREST_MODEL_DEPLOYMENT_ID=provider.chat-completions/main
FOREST_MODEL_MAX_OUTPUT_TOKENS=4096
FOREST_MODEL_INPUT_MODALITIES=text
FOREST_EMBEDDING_DEPLOYMENT_ID=embedding.multimodal/qiyu-vl/default
FOREST_EMBEDDING_MODEL_REVISION=operator-declared/1
FOREST_EMBEDDING_MODEL=Qwen/Qwen3-VL-Embedding-8B
FOREST_EMBEDDING_DIMENSIONS=4096
FOREST_VECTOR_INDEX_ENDPOINT=http://milvus:19530
FOREST_VECTOR_INDEX_CREDENTIAL_REF=
FOREST_VECTOR_INDEX_TIMEOUT_SECONDS=10
FOREST_GROUP_TRIGGER_MODE=MENTION
FOREST_PRIVATE_ENABLED=true
FOREST_AGENT_SYSTEM_PROMPT=...
```

`FOREST_AGENT_SYSTEM_PROMPT` 是 Agent 模块拥有的 PresetContext。不可修改的核心协议仍来自 `agent_protocol.CORE_PROMPT`，包含单 Agent 身份、作者隔离、工具调用和输出语法约束。

`embedding.multimodal` 独占 VL 请求投影、凭据、HTTP 客户端、传输尝试和用量回执；`memory.graph` 只通过冻结的 `embedding.encode/1` 路由提交已获准的文本或图片 facet。两者的发送边界在同一数据库事务中把 Provider 尝试标为 `SENT`、把 Memory 尝试标为 `DISPATCHED`。Embedding 的 deployment、模型版本、模型种类、维度与派生 space 在 Bot 首次初始化时冻结；配置 plan/apply 可以替换 endpoint、凭据引用、超时和连接数，新 Generation 会采用新传输配置。改变冻结 profile 会以 `EMBEDDING_PROFILE_IMMUTABLE` 拒绝启动。

`vector.index` 独占 Milvus client 和 Bot collection，提供 `upsert/search/delete` 三个冻结服务。PostgreSQL 不再保存可查询向量列或执行全量余弦扫描，只保存稳定 `index_ref_id` 与权威内容、来源和关系；Milvus 命中必须回 PostgreSQL 复核后才能进入 EvidenceBundle。`space_id`、维度和度量与 Bot 生命周期绑定，配置只允许替换兼容 endpoint、凭据引用和超时。

`media.capture` 独占 OneBot 临时图片定位符、下载客户端、重试租约和受管图片资产。Adapter 在 ingress 事务里只向 Conversation 发布稳定 `media_ref`，并登记异步捕获任务；签名 URL 不进入规范 Context。Worker 完成有界下载和图片解码验证后，通过 `memory.source.capture/1` 发布保持原段顺序的图文来源。Agent 在下一次模型调用前从 `media.context.resolve/1` 取得当前 Context 的不可变像素快照；text-only Provider 仍只在该次投影中省略图片。

直接 `forest bot` 时，环境变量只承担无部署控制面的显式链路诊断。生产 Compose 的部署控制面监听 `127.0.0.1:8080`（容器内 `8081`）；浏览器使用持久管理账号和服务端可撤销会话，不保存共享管理 token。页面从准确 RuntimeGraph Registry 加载模块贡献，支持模块配置 plan/apply、热替换、Memory 的主干/来源/关系查询、Agent PresetContext/CorePrompt、Run/Tool/Job 控制以及 OneBot Binding 管理。

## 生命周期与恢复

启动顺序为 `install bundle → prepare → bind → prewarm → atomic publish → local activate`。OneBot WebSocket 在 prewarm 阶段缓冲事件，只有整个图发布后才开放接收。正常关闭先冻结图准入、逆序 quiesce/close、退休图，再显式释放 HostLease，因此同一数据库可立即由新的宿主 incarnation 接管。

新宿主取得租约后会把数据库中遗留的 OneBot 当前客户端结算为 `FAILED/HOST_RESTARTED`，再建立新客户端。该行为只用于进程已失去的本地连接资源；发送结果不确定性仍按 Delivery 的 `UNKNOWN` 规则处理，不能把未知发送自动重试成重复消息。

若 Delivery 工作区出现 UNKNOWN，先从平台或 Provider 查询原发送 attempt，再进入 Bot 作用域的“发送结果核对”。选中记录后核对 Intent、Part、Attempt 与 Effect 引用，提交结论、证据类型和外部查询引用。确认未发送并重新排队会唤醒 Delivery Worker；确认已发送或最终未发送会终结原链。不要通过新建消息、重启 Bot 或修改数据库解除阻塞。

Compose 编排 `initialize`、`postgres`、`etcd`、`minio`、`milvus` 和 `supervisor`，镜像默认命令也是 `forest supervisor`。PostgreSQL 首次初始化只创建 `forest_deployment`；供应器为每个 Bot 按 Bot ID 派生稳定库名和无特权 login role，为该 role 生成随机口令，只允许在同一批准集群创建，并复核 role flags 与数据库 owner。Bot 运行时不持有 deployment 超级用户凭据。Supervisor 的健康检查只探测 loopback 管理监听，不读取 Web 凭据。开发期从旧 schema 切换时直接重建卷，不运行旧迁移链。

若 deployment catalog、加密 Secret 与冻结 Bootstrap profile 已经有效，只需要重建某个尚未发布的 Bot 库，则先正常停止 Supervisor 并等待其 lease 过期，再在候选镜像中挂载同一只读 Secret 卷和 `scripts/rebuild_bot_database.py`。脚本默认只输出派生库名、role、profile 摘要和 lease 状态；真正执行必须同时给出 `--execute` 和与派生结果逐字相同的 `--confirm-database`。它只允许删除由 Bot ID 派生、owner 也准确匹配的数据库，然后直接从现有 authority 安装当前 18 个 baseline；不修改 READY catalog、已完成 Provisioning Job、Secret 或 Bootstrap profile，不包含列迁移、双写或兼容读取。重建后必须得到 18 个 schema bundle 且没有 RuntimeGraph head，之后才可重启 Supervisor 发布新图。

长时实网验收使用 `scripts/observe_live_bot.py`，并在候选镜像、同一 deployment 网络和 Supervisor 的只读 Secret mount 中运行。该探针只输出 RuntimeGraph、Binding、Client、工作项和 Memory 终态计数，不读取或输出凭据、Payload 正文、群消息、作者身份或媒体内容。每个样本使用 Bot 数据库的只读 repeatable-read 事务，检查 18 个 schema bundle、18 个当前图成员及最新生命周期、OneBot desired/effective revision、超过期限的 Embedding attempt/预算预留，以及同一 Agent session 是否出现多个未终结 Run；deployment 状态和管理端 HTTP 同期采样。任一样本出现违反项时最终退出码为 1，完整时段无违反项才退出 0。sidecar 不提供管理 HTTP，运行时应加 `--no-healthcheck`，避免继承 runtime 镜像的 Supervisor healthcheck：

```powershell
docker run --rm --no-healthcheck --network forest-clean_default `
  --volumes-from forest-clean-supervisor-1:ro `
  --mount "type=bind,source=$PWD\scripts\observe_live_bot.py,target=/tmp/observe_live_bot.py,readonly" `
  -e FOREST_DEPLOYMENT_ID=forest/default `
  -e FOREST_DEPLOYMENT_DATABASE_PASSWORD_FILE=/run/forest-secrets/postgres_password `
  -e FOREST_DEPLOYMENT_DATABASE_HOST=postgres `
  -e FOREST_DEPLOYMENT_DATABASE_NAME=forest_deployment `
  -e FOREST_DEPLOYMENT_MASTER_KEYRING_FILE=/run/forest-secrets/deployment_master_keyring `
  -e FOREST_DEPLOYMENT_WEB_BOOTSTRAP_FILE=/run/forest-secrets/deployment_web_bootstrap `
  -e FOREST_DEPLOYMENT_MANAGEMENT_ORIGIN=http://127.0.0.1:8080 `
  forest:local python /tmp/observe_live_bot.py --bot-id forest-main `
  --duration-seconds 3600 --interval-seconds 30
```

## 受控进程现场验收

`process.executor` 初始配置没有任何 ExecutionProfile。管理员应在 Bot 作用域选择该模块的“受控进程执行”配置贡献，通过通用 plan/apply 表单提交 [diagnostic.echo.json](../examples/process_executor/diagnostic.echo.json)；不要直接修改模块表或 Payload。该示例只适用于当前 `forest:local` 镜像：它冻结 `/usr/local/bin/python3.11` 的 SHA-256、`10001:10001` 身份、`/app` 工作目录、无网络策略和资源上限。镜像变化后必须先在候选镜像中重新计算并核对可执行文件摘要，再发布新配置 revision。

配置热替换完成后，在 `job.scheduler` 工作区创建 `native.execute` Job，输入为：

```json
{
  "profile_id": "diagnostic.echo",
  "arguments": {
    "value": "forest-live-profile"
  }
}
```

验收应看到 Job 进入 `SUCCEEDED`、退出码为 0，并产生受管 stdout Payload；管理页面只展示摘要、字节数、截断状态和 Payload 引用。再用 `$(touch should-not-run)` 作为 `value` 时，它必须作为普通 argv 原样输出，不能产生 shell 副作用。这个 profile 的网络策略为 `deny`，Generation 若不能安装 seccomp 或不能以声明身份和 POSIX limits 运行，应拒绝激活，不能降级为有网络或无限制执行。

## 多 Bot Supervisor 入口

`forest supervisor` 使用独立部署数据库运行持久供应 Worker 与多 Bot 生命周期协调器。最小进程配置为：

```text
FOREST_DEPLOYMENT_DSN=postgresql://...
FOREST_DEPLOYMENT_ID=forest/default
FOREST_DEPLOYMENT_MASTER_KEYRING_FILE=/run/secrets/deployment_master_keyring
FOREST_DEPLOYMENT_WEB_BOOTSTRAP_FILE=/run/secrets/deployment_web_bootstrap
FOREST_DEPLOYMENT_MANAGEMENT_HOST=127.0.0.1
FOREST_DEPLOYMENT_MANAGEMENT_PORT=8081
FOREST_DEPLOYMENT_MANAGEMENT_ORIGIN=http://127.0.0.1:8081
FOREST_SUPERVISOR_POLL_SECONDS=1
FOREST_SUPERVISOR_MAX_STARTING=2
```

主密钥 keyring 是单行、可打印 ASCII JSON，协议固定为 `forest.deployment-master-keyring/1`；包含 `active_key_id` 和最多 32 个 `key_id → Base64(32 bytes)` 条目。active key 只加密新 Secret 版本，历史密文和账号操作凭据指纹按自身保存的 key ID 复核，因此轮换时必须提交“全部仍被引用的旧 key + 新 key”，再把 active 指向新 key 并重启 Supervisor。不得在确认没有引用前删除旧 key。keyring、数据库口令和首次管理员密码都不进入环境变量、日志或对象 repr。Supervisor 从 READY Bot 固化的 storage/bootstrap profile 构造各自 `CleanBotHost`；profile 中只保存 Secret 引用，凭据不会写入 Bot catalog。供应时只安装模块 authority 和 revision 1，不启动 Agent、Provider 或 OneBot 网络资源；供应 Job 成功发布 READY 后，Supervisor 才依据 runtime intent 启停 Bot。

Compose 中无需设置 `FOREST_DEPLOYMENT_DSN`：Supervisor 从 `/run/forest-secrets/postgres_password` 构造只指向 `forest_deployment` 的 DSN。显式 DSN 仅供非 Compose 部署。默认 `docker compose up -d` 启动 `initialize`、`postgres` 和 `supervisor`；Compose 中不存在绕过部署控制面的单 Bot 服务。

Supervisor 同时启动多作用域 WebUI。页面从 deployment provider 动态发现 Bot 目录、Secret、Provisioning 与管理账号工作区；可以写入不回显的版本化 Secret、管理 ADMIN/AUDITOR 账号及会话、创建只暴露引用的本地 PostgreSQL storage profile，并从 Bootstrap Contributor 目录逐模块设置 Provider、VL Embedding、触发策略、Shell、Agent 和 OneBot 后冻结 profile、加入供应队列。登录使用 HttpOnly、SameSite 服务端会话；POST 同时校验精确 Origin 和 CSRF，角色在 HTTP 入口与目标作用域网关复核。Bot 进入 ONLINE 后，壳层作用域选择器通过注册的 Bot provider 打开该 Bot 的准确 RuntimeGraph 模块工作区；壳层不复制 Agent、Memory、OneBot 或 Job 页面。下线先禁止新管理调用，已接纳调用排空后再关闭 Bot Host。

## 仍需补齐的能力

- 树莓派与 PC 之间的长时实网抖动和持续真实流量验证；loopback 断连、action 不确定性、1000 条有界顺序流、过载显式失败、候选发布失败回滚和 owned task 崩溃后的 Supervisor 隔离已经自动化覆盖；
- 许可群中的低频消息、图片和排队行为需由真人触发完成最终验收，自动验收不得主动向群发送消息。

上述缺口不能由组合根中的特殊判断补齐，必须继续作为平级模块和内核协调 UoW 接入。
