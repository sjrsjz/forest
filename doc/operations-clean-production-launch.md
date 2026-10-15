# 干净生产启动链

2026-10-16 更新：v31 仍是唯一连接 QQ 的在线 Bot；[v32 离线候选](live-v32-offline-candidate-2026-10-16.md)已供应为 READY/OFFLINE，使用独立数据库与 Memory SQL 3.94.0，尚未切换或合并旧记忆。

截至 2026-10-15，当前在线候选为 `forest-main-v31`，Supervisor 使用已提交源码镜像 `forest:runtime-candidate-352eb5b`；独立数据库、组件版本与实时证据见 [v31 在线记录](live-v31-cutover-2026-10-15.md)。v30 及更早版本是保留的历史或回退实例。Supervisor HTTP healthy、Bot ONLINE 与 OneBot TCP 已连接分别只证明各自层级存活，不能单独证明 QQ 账号已登录或群聊回复链可用。

本文记录多 Bot Supervisor 的生产启动入口，以及独立 `forest bot` 诊断路径。宿主持有进程、数据库连接、HostLease 和 RuntimeGraph 生命周期；Adapter、触发策略、Conversation、Agent、Memory、Provider 与 Delivery 作为 Bot 作用域的平级 Generation 成员运行。Bot 基础身份和期望运行意图归部署内核，管理 builtin 通过受限端口操作。

## 启动图

`forest bot` 在一个空的专用 PostgreSQL 数据库内安装并发布以下 22 个成员：

- `kernel.runtime`、`kernel.payload`、`kernel.authorization`、`kernel.effect`、`kernel.audit`、`component.worker-host`、`audit.console`；
- `routing.graph`, `provider.chat-completions`、`embedding.multimodal`、`vector.index`、`memory.graph`、`media.capture`、`agent.runtime`；
- `conversation.trigger-policy`、`conversation.scheduler`、`codec.onebot`；
- `adapter.onebot`、`delivery`、`process.executor`、`job.scheduler`、`shell.command`。

OneBot 消息进入后，Adapter 复核 Binding、连接世代与传输围栏，通过冻结到准确 Generation 的 `codec.onebot` 契约取得规范消息，再提交载荷和平台观察。Codec 不接触凭据、Socket、Repository 或 Conversation。`conversation.trigger-policy` 根据冻结配置给出 `TRIGGER` 或 `NO_TRIGGER`；内核 OneBot ingress UoW 在同一事务中提交平台事件、Conversation Inbox、群主干 Memory 来源与分类结果。Scheduler 因此不再依赖测试或管理端手工补写触发记录。

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

`memory.graph` 拥有 VL facet、来源投影、claim 和批次 handoff；`embedding.multimodal` 是带 manifest、schema、隔离 Worker 与同代 Effect Driver 的 builtin 包，只公开 `embedding.profile.bind/2` 和 `embedding.encode/2`。绑定契约还声明可准确归因的最大批次项数；Memory 在 Bot 域保存该上限，SQL claim 与 Embedding 发送前都复核它。Memory 在一个事务中消费准确授权票据、提交 `kernel.effect` transport handoff 与自身 batch handoff；驱动复核当前 RuntimeGraph、Embedding Generation、wire 摘要和 profile 后才读取凭据并执行 HTTP。外部 attempt 与观察由通用 Effect authority 保存，不再存在第二套 Embedding transport 表。Embedding 的 deployment、模型版本、模型种类、维度与派生 space 在 Bot 首次初始化时冻结；配置 plan/apply 可以替换 endpoint、凭据引用、超时和连接数，新 Generation 会采用新传输配置。候选代会重新验证实际 desired revision，改变冻结 profile 会以 `EMBEDDING_PROFILE_IMMUTABLE` 拒绝启动。

`vector.index` 独占 Milvus client 和 Bot collection，提供 `upsert/search/delete` 三个冻结服务。PostgreSQL 不再保存可查询向量列或执行全量余弦扫描，只保存稳定 `index_ref_id` 与权威内容、来源和关系；Milvus 命中必须回 PostgreSQL 复核后才能进入 EvidenceBundle。`space_id`、维度和度量与 Bot 生命周期绑定，配置只允许替换兼容 endpoint、凭据引用和超时。

`media.capture` 独占 OneBot 临时图片定位符、下载客户端、重试租约和受管图片资产。Adapter 在 ingress 事务里只向 Conversation 发布稳定 `media_ref`，并登记异步捕获任务；签名 URL 不进入规范 Context。Worker 完成有界下载和图片解码验证后，通过 `memory.source.capture/2` 发布保持原段顺序的图文来源。Agent 在下一次模型调用前从 `media.context.resolve/1` 取得当前 Context 的不可变像素快照；text-only Provider 仍只在该次投影中省略图片。

直接 `forest bot` 时，环境变量只承担无部署控制面的显式链路诊断。生产 Compose 的部署控制面监听 `127.0.0.1:8080`（容器内 `8081`）；浏览器使用持久管理账号和服务端可撤销会话，不保存共享管理 token。页面从准确 RuntimeGraph Registry 加载模块贡献，支持模块配置 plan/apply、热替换、Memory 的主干/来源/关系查询、Agent PresetContext/CorePrompt、Run/Tool/Job 控制以及 OneBot Binding 管理。

## 生命周期与恢复

启动顺序为 `install bundle → prepare → bind → prewarm → atomic publish → local activate`。OneBot WebSocket 在 prewarm 阶段缓冲事件，只有整个图发布后才开放接收。正常关闭先冻结图准入、逆序 quiesce/close、退休图，再显式释放 HostLease，因此同一数据库可立即由新的宿主 incarnation 接管。

新宿主取得租约后会把数据库中遗留的 OneBot 当前客户端结算为 `FAILED/HOST_RESTARTED`，再建立新客户端。该行为只用于进程已失去的本地连接资源；发送结果不确定性仍按 Delivery 的 `UNKNOWN` 规则处理，不能把未知发送自动重试成重复消息。

Supervisor 被 SIGKILL 后，立即重启可能因旧 Host incarnation 的 30 秒租约尚有效而以 `HOST_SUPERVISOR_BUSY` 退出；这是拒绝双主的 fencing，不要手工清空租约或更换 `host_id`。Compose 的 Supervisor 使用 `restart: unless-stopped`，会在租约到期后重新尝试；人工以 `--restart no` 运行时，应等租约到期再启动。新进程 HTTP 健康后仍需等待 Bot 的 `observed_state=ONLINE` 和新 Bot Graph `ACTIVE`，再核对原 Inbox、Context、Memory 与发送账本，不能只用进程存活判定恢复成功。

若 Delivery 工作区出现 UNKNOWN，先从平台或 Provider 查询原发送 attempt，再进入 Bot 作用域的“发送结果核对”。选中记录后核对 Intent、Part、Attempt 与 Effect 引用，提交结论、证据类型和外部查询引用。确认未发送并重新排队会唤醒 Delivery Worker；确认已发送或最终未发送会终结原链。不要通过新建消息、重启 Bot 或修改数据库解除阻塞。

Compose 编排 `initialize`、`postgres`、`etcd`、`minio`、`milvus` 和 `supervisor`，镜像默认命令也是 `forest supervisor`。PostgreSQL 首次初始化创建 `forest_deployment`，以及由无超级用户 `forest_host` role 持有的独立 `forest_host`；Host role 不能连接 deployment 库。管理员先在 Secret 模块登记专属 PostgreSQL role 口令，再由 `deployment.storage-orchestrator` 冻结请求；`host.storage` 按其资源账本分配物理数据库名、无特权 login role 与 owner marker，并在 Host 物化及 pin 后原子发布 deployment DSN Secret/profile。Bot 运行时不持有 deployment 超级用户凭据。Supervisor 的健康检查只探测 loopback 管理监听，不读取 Web 凭据。开发期从旧 schema 切换时直接重建卷，不运行旧迁移链。

旧的单 Bot 数据库重建脚本已经退役。它按 Bot ID 推导数据库名并直接删除数据库，无法核对 `host.storage` 的资源身份、owner marker、pin 和引用水位，不能用于当前 Host 管理的 profile。对经当前 Host 物化并持有准确 pin 的 Bot，管理员可提交 `bot.retire`：部署退役包等待 Bot 静止、撤销 profile、释放 pin，Host 包在物理锁和 owner marker 复核下回收数据库及 role，最后同一部署事务将 Bot 与退役 Job 结算为 `DELETED`/`SUCCEEDED`。外部结果不明时 Job 保留 `BLOCKED_UNKNOWN`，只能按准确 Job 修订号重新准入；不得手工删库或推断已成功。退役后用新的 Bot 身份走正常供应流程，当前仍不提供同一 Bot 身份的原位重建。旧预发布资源若没有可证明的 Host marker/pin，不能直接使用自动退役。预发布阶段若决定整体重置环境，应先停止 Supervisor，备份需要保留的 PostgreSQL、Milvus 和 Secret，再把 deployment、Host、Bot 物理库及向量集合视作同一批环境重建，重新经 WebUI 创建 Secret、存储请求与 Bot 供应事实。不能只删 Bot 库而保留指向它的 READY catalog/profile 与 Host pin。

`pg_dump` 只保存 PostgreSQL 权威事实，不包含 Milvus 向量。整体重置前如需可回滚的完整记忆，应另行保存同一向量集合或重新嵌入，并明确处理外部调用费用。

若向量驱动报告 `VECTOR_INDEX_REBUILD_REQUIRED`，表示 Milvus collection 缺失且 PostgreSQL 仍有活动 ref、同名 collection 的 `collection_id` 已变化，或一个既有 collection 没有包私有身份绑定。Bot 可继续接收入站并保存权威记忆，语义检索和新向量发布会明确失败；删除 ref 仍结算 tombstone。当前候选尚无经过验收的索引重放/原子切换操作，应恢复与绑定身份一致的 Milvus 备份或在隔离环境完成重建流程后再切换，不能手工创建一个空的同名 collection 并宣称召回已恢复。

在线 v21 镜像使用的 `forest-main` deployment 库保留旧基线，缺少当前候选源码要求的 `forest_deployment.storage_profile_revocations`。候选探针会以 `OBSERVATION_SCHEMA_INCOMPATIBLE` 退出，尚未读到 Bot 库，不能据此判断在线 Bot 是否健康，也不能宣称完成候选长时实网验收。应先在隔离环境验证候选基线，再有计划地切换在线实例，不对在线库临时补表。

长时实网验收使用 `scripts/observe_live_bot.py`，并在候选镜像、同一 deployment 网络和 Supervisor 的只读 Secret mount 中运行。该探针只输出 RuntimeGraph、工作项和 Memory 状态计数及错误代码，不读取或输出凭据、Payload 正文、群消息、作者身份或媒体内容。每个样本使用 Bot 数据库的只读 repeatable-read 事务，检查 READY schema bundle 是否属于当前图、当前图全部成员是否都有最新且 ACTIVE 的生命周期事实、超过 deadline 且再宽限指定秒数仍未结算的 Embedding batch attempt/预算预留，以及同一 Agent session 是否出现多个未终结 Run；`conversation_claims` 仅按活动领取组与群 lane 汇总，单个无 Run 的预填组是合法状态，多个活动组或与 lane 的 Session/Run 不对齐则报 `CONVERSATION_CLAIM_ALIGNMENT_INVALID`，该只读检查不执行自动重排；`memory.facet_states` 区分用途和状态，`memory.attempt_failures` 汇总失败代码，`DOCUMENT.UNKNOWN/FAILED` 作为长期记忆覆盖缺口报告违反项。 `memory.attempt_failures` 配有 `attempt_failures_scope=all_time`；`unknown_attempt_exposure` 将当前 UNKNOWN 文档 facet 按 attempt 去重，报告批次数与原请求冻结预算，不把预算误报为已计费 token。`latest_ingress_decisions` 按每个会话最新一条消息汇总触发决策及原因码，利用 `(bot_id,conversation_id,ingress_seq)` 索引定位，不扫描全量历史；它是排障快照，不是累计触发计数。deployment 状态和管理端 HTTP 同期采样。OneBot 连接是模块运行时资源，当前持久化 schema 不提供 Binding/Client 在线事实；该数据库探针不能单独证明 WebSocket 在线，实网验收还需由适配器运行状态和低频端到端消息确认。若 Bot 库缺少当前候选基线的 `embedding_batch_attempts`，探针会以 `BOT_SCHEMA_INCOMPATIBLE` 失败，应先在隔离库安装候选基线，不能把旧在线库的结果当作新版本验收。任一样本出现违反项时最终退出码为 1，完整时段无违反项才退出 0。sidecar 不提供管理 HTTP，运行时应加 `--no-healthcheck`，避免继承 runtime 镜像的 Supervisor healthcheck：

```powershell
docker run --rm --no-healthcheck --network forest-clean_default `
  --mount type=volume,source=forest-clean_forest_secrets,target=/run/forest-secrets,readonly `
  --mount "type=bind,source=$PWD\scripts\observe_live_bot.py,target=/tmp/observe_live_bot.py,readonly" `
  -e FOREST_DEPLOYMENT_ID=forest/default `
  -e FOREST_DEPLOYMENT_DATABASE_PASSWORD_FILE=/run/forest-secrets/postgres_password `
  -e FOREST_DEPLOYMENT_DATABASE_HOST=postgres `
  -e FOREST_DEPLOYMENT_DATABASE_NAME=forest_deployment `
  -e FOREST_DEPLOYMENT_MASTER_KEYRING_FILE=/run/forest-secrets/deployment_master_keyring `
  forest:local python /tmp/observe_live_bot.py --bot-id forest-main `
  --duration-seconds 3600 --interval-seconds 30
```

NapCat 运行在另一台树莓派时，还需独立查询实际 QQ 登录态。当前树莓派的 WebUI 状态端口只在本机读取；`scripts/observe_napcat_login.py` 从本机 `webui.json` 读取令牌并取得短时认证，仅输出 `is_login`、`is_offline`、`core_ready`、登录阶段和是否有登录错误，不输出令牌、短时凭证或二维码。退出码 0 表示 `READY`，2 表示明确未就绪，1 表示无法判断；它是运维诊断，不改变微内核 Bot 运行意图。当前树莓派可从工作区执行：

```powershell
Get-Content -Raw -Encoding UTF8 scripts/observe_napcat_login.py |
  ssh sjz1@192.168.31.117 python3 - --config /home/sjz1/Napcat/opt/QQ/resources/app/app_launcher/napcat/config/webui.json
```

QQ 登录态 `READY` 仍须结合许可群一次低频真实 @→Agent→发送验收；不以心跳或管理页状态替代回复证据。

验收部署级包时追加 `--deployment-component-id deployment.provisioning`。探针会在部署库的只读 repeatable-read 快照中报告当前 Graph、该组件的 Generation 状态与控制入口；Graph 非 ACTIVE，或指定组件缺席、非 ACTIVE、无控制入口，均产生违反项。该选项默认不启用，以便先观察旧镜像和旧部署图；它只能证明包已发布并允许控制调用，不能单独证明供应 Job 已改由包编排。

要核对模块级热重载的实际影响，可重复传入 `--component-id audit.console --component-id kernel.audit`（最多八个不同组件）。探针在同一只读 repeatable-read 快照内返回当前 Graph 中这些组件的 Generation ID、包版本和工件摘要；当前 Graph 不含某组件时明确返回 `null`。在管理员经 WebUI 或获权群 Shell 提交重载前后比较目标及其依赖闭包与无关组件的身份，并结合持久重载回执判断完成状态；这个观测选项本身不提交重载。

指定 `--group-id 933179609` 时，探针还从该群的 CHAT 投递 lane 读取最新一条发送意图的状态、错误码、序号及完成时间。查询使用 `(bot_id,binding_ref,lane_kind,ordering_key,lane_sequence)` 唯一索引，群消息增加后仍只读取末条意图；没有意图时返回 `latest_chat_send: null`。它不会读取回复正文，且 `SENT` 只证明 Delivery 已记录传输结果，群内实际呈现仍以低频现场观察为准。

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
FOREST_HOST_DSN=postgresql://forest_host:...@.../forest_host
FOREST_DEPLOYMENT_ID=forest/default
FOREST_DEPLOYMENT_MASTER_KEYRING_FILE=/run/secrets/deployment_master_keyring
FOREST_DEPLOYMENT_WEB_BOOTSTRAP_FILE=/run/secrets/deployment_web_bootstrap
FOREST_DEPLOYMENT_MANAGEMENT_HOST=127.0.0.1
FOREST_DEPLOYMENT_MANAGEMENT_PORT=8081
FOREST_DEPLOYMENT_MANAGEMENT_ORIGIN=http://127.0.0.1:8081
FOREST_SUPERVISOR_POLL_SECONDS=1
FOREST_SUPERVISOR_MAX_STARTING=2
```

Compose 初始化器会在受保护卷首次生成稳定 `host_id`，Supervisor 通过 `FOREST_HOST_ID_FILE=/run/forest-secrets/host_id` 读取；重启只更换 incarnation，不会更换宿主身份。非 Compose 启动必须设置且只能设置 `FOREST_HOST_ID`（`host/` 后接 32 位十六进制随机值）或 `FOREST_HOST_ID_FILE` 之一，并持久保存同一身份文件；不得用 deployment ID 或容器进程 ID 代替。

主密钥 keyring 是单行、可打印 ASCII JSON，协议固定为 `forest.deployment-master-keyring/1`；包含 `active_key_id` 和最多 32 个 `key_id → Base64(32 bytes)` 条目。active key 只加密新 Secret 版本，历史密文和账号操作凭据指纹按自身保存的 key ID 复核，因此轮换时必须提交“全部仍被引用的旧 key + 新 key”，再把 active 指向新 key 并重启 Supervisor。不得在确认没有引用前删除旧 key。keyring、数据库口令和首次管理员密码都不进入环境变量、日志或对象 repr。Supervisor 从 READY Bot 固化的 storage/bootstrap profile 构造各自 `BotHost`；profile 中只保存 Secret 引用，凭据不会写入 Bot catalog。供应时只安装模块 authority 和 revision 1，不启动 Agent、Provider 或 OneBot 网络资源；供应 Job 成功发布 READY 后，Supervisor 才依据 runtime intent 启停 Bot。

Compose 中无需设置 `FOREST_DEPLOYMENT_DSN` 或 `FOREST_HOST_DSN`：Supervisor 分别从 `/run/forest-secrets/postgres_password` 和 `/run/forest-secrets/host_postgres_password` 构造指向两个独立库的 DSN。显式 DSN 仅供非 Compose 部署。旧预发布 PostgreSQL 卷缺少 Host role、库或受保护口令时不会自动补迁移；须先备份并按预发布重建流程重建该卷，再启动此版本 Supervisor。默认 `docker compose up -d` 启动 `initialize`、`postgres` 和 `supervisor`；Compose 中不存在绕过部署控制面的单 Bot 服务。

Supervisor 同时启动多作用域 WebUI，并把 Host Graph 作为独立作用域注册；Host 包自带资源清单与模块配置页经 Host Web 回执授权。页面从 deployment provider 动态发现 Bot 目录、Secret、Provisioning 与管理账号工作区；可以写入不回显的版本化 Secret、管理 ADMIN/AUDITOR 账号及会话、在 Host 存储模块提交建库请求，待结算后使用对应 storage profile，并从 Bootstrap Contributor 目录逐模块设置 Provider、VL Embedding、触发策略、Shell、Agent 和 OneBot 后冻结 profile、加入供应队列。登录使用 HttpOnly、SameSite 服务端会话；POST 同时校验精确 Origin 和 CSRF，角色在 HTTP 入口与目标作用域网关复核。Bot 进入 ONLINE 后，壳层作用域选择器通过注册的 Bot provider 打开该 Bot 的准确 RuntimeGraph 模块工作区；壳层不复制 Agent、Memory、OneBot 或 Job 页面。下线先禁止新管理调用，已接纳调用排空后再关闭 Bot Host。

## 当前 v25 验收与回退边界（2026-10-11）

`forest-candidate-v25-supervisor` 的 WebUI 监听本机 `127.0.0.1:18096`；部署、Host、Bot 数据库与包卷均独立。只读复核显示 Supervisor healthy、Bot 图 22/22 ACTIVE，v24/v25 到树莓派 OneBot 的已建立连接为 0/1。当前 Bot 的 OneBot `self_id` 为 `3605196653`，触发策略只允许群 `933179609` 显式 @ 该账号；此前许可群入站均未满足这一条件，Agent Run 为 0。真实 @→Agent→Provider→Delivery 尚未验收，不能用连接数或其他群的入站代替。

回退前重新读取运行意图、运行观察与 OneBot 连接：先将 v25 Bot 改为 OFFLINE，确认其连接退出，再将 v24 Bot 改为 ONLINE；两者不可同时持有同一 QQ 身份。切换操作应使用新的幂等键与当前意图修订，不重放旧切换请求。任何 `UNKNOWN` 发送或模型效果按所属模块的证据协议恢复，不能靠重复群消息探测。实际步骤与保存的旧容器见 [v25 在线验收记录](live-v25-cutover-2026-10-11.md)。

## 历史 v15/v16 候选（2026-10-09）

当时 v16 复用 v15 的隔离 Secret、PostgreSQL 与包卷；mock Bot 在 Docker 内部假 OneBot 完成冷恢复和 Shell 混合内容验证，真实 Bot 保持离线。随后已通过新的独立候选和多次受控切换推进到 v25。原“旧 Supervisor 停机未获授权、候选必须离线”的判断只描述 2026-10-09 的旧现场，现已失效。

## 仍需补齐的能力

- 树莓派与 PC 之间的长时实网抖动和持续真实流量验证；loopback 断连、action 不确定性、1000 条有界顺序流、过载显式失败、候选发布失败回滚和 owned task 崩溃后的 Supervisor 隔离已经自动化覆盖；
- 许可群中的低频消息、图片和排队行为需由真人触发完成最终验收，自动验收不得主动向群发送消息。

真实验收应记录 Bot Graph、Agent Run、Provider Effect、Delivery 回执和 Memory 状态；只读采样不能替代实际群回复检查。
