# Agent 实现与启动

Agent、Memory、工具网关是三个独立 Wasm 包，分别位于 `plugins/agent`、`plugins/memory`、`plugins/agent-tools`。Context、Provider、Embedding 继续使用原有独立包。宿主新增的只有模块私有 SQLite 操作；其余实现、SQL、提示词和接口类型映射均在插件内。

## 1. 包与接口

| 对象 | 入口 | 职责 |
|---|---|---|
| `/module/agent` | `/group_message`、`/status` | 接收全部允许群消息、每群一个工作循环、持续点、窗口和诊断 |
| `/module/memory` | `/accept`、`/archive`、`/link`、`/delivery` | 不可变原文、群内位置、归档集合、实际种子联想和发送回执关联 |
| `/module/memory` | `/read`、`/recall`、`/project`、`/media/read` | 已归档原文读取、全局召回、受管媒体和格式化用投影 |
| `/module/memory` | `/index/advance`、`/index/rebuild`、`/status` | 持久索引待办、显式重建和索引状态 |
| `/module/agent-tools` | `/describe`、`/invoke` | 原生工具声明与显式模块路由 |
| `/module/deepseek-web` | `/search`、`/fetch` | 默认网页搜索与 Markdown 抓取 |
| `/module/context` | `/format` | 人格提示词、账号身份、来源标记、CQ 和图片格式化 |
| `/module/openai-provider` | `/complete`、`/capabilities` | 模型协议和多模态能力声明 |
| `/module/qwen-vl-embedding` | `/embed` | 固定模型、2048 维文本或图片向量 |

公共契约见各包中的 `manifest.json` 和 `*.schema`。Rust 调用方持有本地类型映射，不链接其他插件实现；其他语言使用同一 Schema/Value 和固定 Wasm ABI。

## 2. 实际会话流程

`/group_message` 可重入。处理器同步把消息放入对应群的待接入队列；已有循环时立即返回，没有循环时取得本群处理权。所有共享状态修改都在无 await 的短操作中完成。接纳、模型、工具或媒体等待期间，其他群和同群新消息仍能推进。

工作循环先通过 Memory 固定群内位置。群消息去重键为 `(group_id, self_id, message_id)`；生成回合使用首次触发 MemoryId 和本次工作内的回合序号，工具结果再附加调用位置。平台自回显不制造第二份模型发言，实际发送回执通过 `/delivery` 关联到原 ModelTurn。

每轮选取截至最后一条已接纳 @ 的前缀，保存在会话的 staged 队列中，然后逐条 @ 查询。等待期间的新消息不扩张该批次。召回分别放在各自 @ 之前；所有工具结果闭合后，才将下一批群消息接入标准 API 请求。请求头、工具声明和已发送 messages 在同一次工作内保持原值，仅追加新内容。

窗口按单元数计量，默认高水位 256、低水位 128。静默与首次请求冻结前可以归档并移除旧前缀；开始模型工作后只追加。原生 Assistant 与其工具结果是一个不可拆分块。Recall 只保存引用，不再次归档或嵌入。裁剪不调用摘要模型，没有估算 token 超限硬阻断。

Provider 失败只记录一次；有新的 @ 则接入新输入、清除旧待报告错误并继续。无持续点时排空普通尾部并静默；仍有错误则用 `send` 提交错误报告。正常正文在工具执行前发送，支持文本、@、回复、表情和受管图片 CQ。图片引用由 Memory 解析为发送用数据，不让模型直接指定服务器文件。

正常 destroy 将窗口、staged、pending 和诊断写入自身配置项。新 create 清除旧触发意图并恢复静默；不重放 Provider 或工具副作用。不完整的旧工具块保留原始记录，但不作为下一次请求的顶层原生工具链。Force/trap 仍只能恢复此前成功保存的状态。

## 3. 存储与派生索引

Memory 的表结构位于 `plugins/memory/src/schema.sql`，包含 `trunks`、`units`、`links`、`media`、`vectors` 和 `deliveries`。原始结构用 JSON 保存，可直接用 SQLite 工具查看；CQ 是可重建的投影。图片原始字节进入受管文件，数据 URI 仅在模型或发送调用需要时生成。

接纳事务保存原文、位置和媒体来源，随后用 self-send 推进媒体获取。归档事务加入归档资格及每个表示的持久待办，然后返回；嵌入和 Qdrant 不在此事务中。后台处理登记明确的 published/failed 状态；思考和空节点不产生向量表示。非空工具调用、完整工具结果和纯图片均进入处理。

未完成待办在下一次 Memory 接纳/归档或显式 `/index/advance` 时继续推进。重载不重放 Agent 工作，也不凭空创建模块后台线程。已失败项保持失败；管理员可显式 `/index/rebuild` 重建当前索引。换模型或投影配方应建立新的索引空间，再从同一事实库重建；不能把不同空间混写到一个 collection。

Qdrant 保存向量和原群、序号引用。查询采用 [Query API](https://api.qdrant.tech/api-reference/search/query-points)；发布使用 [upsert 的 `wait=true`](https://api.qdrant.tech/api-reference/points/upsert-points)，确认 completed 后才标记本地发布成功。命中回表检查 Archived，随后在 SQLite 内查询有界邻接。种子相似度门槛与图激活分开；时序按实际位置差衰减，联想按端点去重，超出度数限制时随机抽样。

## 4. 构建与配置

在仓库根目录运行：

```powershell
./scripts/build-agent.ps1
```

该脚本构建七个独立 Wasm 包及其外部 Manifest/Schema，编译并行数为 2。

配置项如下，修改后热重载所属模块生效：

| 模块配置 | 内容 |
|---|---|
| Agent `/settings` | `allowed_groups`、`bot_name`、`high_water`、`low_water` |
| Context `/prompt/agent` | 此 Bot 的人格与行动提示词 |
| Provider `/settings` | endpoint、model、api_key、multimodal、output_limit、reasoning_effort |
| Embedding `/connection` | 同一 Qwen VL 模型的 endpoint、api_key |
| Memory `/settings` | Qdrant endpoint、api_key、collection、检索参数 JSON |
| 工具网关 `/routes` | 工具声明及其目标对象、接口 |
| DeepSeek Web `/connection` | DeepSeek API 前缀和独立凭据 |

初次部署可用配置辅助程序。先在当前终端设置 `FOREST_CHAT_ENDPOINT`、`FOREST_CHAT_MODEL`、`FOREST_CHAT_KEY`、`FOREST_EMBED_KEY`、`FOREST_GROUPS`，再执行：

```powershell
cargo run --locked --offline -j 2 -p forest-agent-module --example configure
```

启用默认网页工具时还需设置 `FOREST_DEEPSEEK_KEY`；API 前缀可用 `FOREST_DEEPSEEK_ENDPOINT` 修改。`FOREST_GROUPS` 是逗号分隔的群号；默认不启用任何群。可选环境变量包括 `FOREST_EMBED_ENDPOINT`、`FOREST_QDRANT_ENDPOINT`、`FOREST_QDRANT_KEY`、`FOREST_BOT_NAME`、`FOREST_AGENT_PROMPT`、`FOREST_CHAT_VISION=true`、`FOREST_MAX_OUTPUT`、`FOREST_REASONING_EFFORT` 和 `FOREST_CONFIG_DB`。凭据只写入本地配置库，不输出到日志。已有运行实例需要重载相应模块才会采用新值。

启动向量服务与宿主：

```powershell
docker compose up -d qdrant
cargo run --locked --offline -j 2 -p forest-cli -- --package packages/agent --events-ws ws://127.0.0.1:8080
```

网关地址是示例。正常运行不传 `--allow-group`，群启用只由 Agent 配置表 `/settings` 中的 `allowed_groups` 决定；在管理端保存并重载 Agent 后生效，不需要重启宿主。`--allow-group` 仅用于显式测试限制：一旦提供，宿主会额外过滤入站事件和原生发送目标，不随 Agent 热重载改变。网关凭据仍通过 `FOREST_ONEBOT_TOKEN` 环境变量提供。CLI 从 `--packages-directory` 的 Manifest 规划依赖，只加载根包实际使用的包。Qdrant Compose 使用命名卷，避免把服务数据库直接挂载到 Windows 工作目录；固定镜像版本 [v1.19.2](https://github.com/qdrant/qdrant/releases/tag/v1.19.2)。

## 5. 工具扩展

默认网关提供 `web_search`、`web_fetch`、`http_request` 和 `memory_read`。网页搜索及阅读优先使用独立 [DeepSeek Web 插件](23-deepseek-web.md)，返回搜索摘要和 Markdown；原始 HTTP 保留用于明确的 HTTP/API 操作。记忆工具按原群和序号读取已归档资料，保留来源，不输出历史思考。

外部工具接口接受 `Invocation { group_id, bot_id, name, arguments_json }`，返回 `Result<String, Error>`。路由按工具名选择明确的 `(对象, 接口)`，调用仍经过 Manifest uses 和目标 ACL；参数不参与选择任意接口。把自定义路由列表交给打包程序，可同时生成静态依赖：

```powershell
cargo run --locked --offline -j 2 -p forest-agent-tools --bin package-agent-tools -- routes.json
```

路由项形状见 `plugins/agent-tools/src/lib.rs` 的 `Route` 与生成的配置 Schema。目标模块自行解释参数、完成操作和报告结果。切换到 Manifest 尚未声明的目标时，需要重新打包网关；单纯改配置不会绕过静态依赖和 ACL。

## 6. 验证范围

`plugins/agent/tests/system.rs` 加载实际 Wasm Agent、Memory、Context、Provider、Embedding 和工具网关，模型、向量 HTTP 和 OneBot 回执由本地可控服务提供。覆盖排队边界、并发群会话、原生角色顺序与缓存前缀、正文先发送、静默裁剪、错误报告、热重载、原文保留、媒体投影、失败索引和有界图扩散。

本轮还通过了运行时的 5 项检查，包括 SQLite 原子性与隔离、OneBot 实际 Wasm 链路和调度/热重载；新增包及受影响宿主通过 Clippy。CLI 常规集成命令因现有进程占用 `target/debug/forest.exe` 无法覆盖可执行文件，随后用本次构建的 `target/debug/deps/forest.exe`、临时配置库和本地 WebSocket 完成独立包回复与六包 Agent 依赖启动验证。现有进程未停止，配置辅助程序也在临时库中验证。

```powershell
cargo test --locked --offline -j 2 -p forest-agent-module --test system
cargo test --locked --offline -j 2 -p forest-runtime --lib sqlite_tests
```

这组模拟服务测试不衡量真实模型相关性、供应商延迟或十万/百万向量容量。首次实现验证时 Docker 引擎未运行，未连接真实 Qdrant 或发送真实群消息。现有 Vue 控制台提供模块观测和加载/重载；专门的 Agent 配置表单与记忆浏览页面尚未增加。

2026-10-22 的真实启动观察已连接 Qdrant v1.19.2、固定 Qwen VL 模型、配置的 Chat Completions 模型与 OneBot 网关。用户在授权测试群发送 @ 后，实际完成了图片能力查询、文本向量化、Qdrant 查询、模型响应和发送回执；原始群消息与带 reasoning_content 的模型回合各保存一条，随后回到静默，六个模块仍为 Serving。新库尚无归档单元，此次召回为空；未声称验证了真实记忆命中质量或工具执行。原有 AliceBot 服务保留运行。
