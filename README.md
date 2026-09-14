# Forest

Forest 正在实现一个采用原生 Python 微内核、支持多 Bot 和完整图文记忆的重型机器人框架。目标架构见[设计文档](doc/README.md)。

**当前已接通多 Bot 管理、原生插件升级、OneBot HTTP、持久单 Agent、定时任务、跨群图文召回、来源遗忘，以及加密多数据库备份、外部工件导入、独立校验、隔离恢复、托管 Bot 恢复激活和旧存储代际回收；首版框架仍在实现中。**Agent 使用宿主内置且不可覆盖的 CorePrompt、Bot 级 PresetContext 和原子 XML ModelTurn；一次模型轮至多执行一个工具，模型或工具步骤后的下一次模型调用可以接入排队触发。旧产品子 Agent 已从运行时、配置和页面删除，迁移 050 只保留被封存的历史事实。后台可配置模型、人格、图文能力、工具授权、嵌入服务与预算，并管理插件、账号、运行审批、任务、发送结果、跨群记忆来源和备份工件。消息持久入库，同群串行、跨群并行；支持模型/工具安全点接入与零摘要裁剪。内核支持自动重载、状态迁移、锁定依赖环境及跨 Worker 服务调用。自动跨主干语义联想已复用既有图文向量接通；会话 Job 与群聊 Shell 的正文已进入可遗忘 payload，并分别由 writer v3/v4 隔离旧写者。剩余部署正文族、部署控制面整库恢复、完整管理 Shell 及容量治理仍需继续实现，边界见[实现状态](doc/implementation-status.md)。

## 使用 Docker 启动

需要正在运行的 Docker Engine，以及支持 `--wait` 的 Docker Compose。在项目根目录执行：

```sh
docker compose up --build -d --wait
docker compose exec forest forest setup-token
```

打开 [http://127.0.0.1:8080](http://127.0.0.1:8080)，填写终端给出的一次性配对码，创建管理员账号和密码，然后登录后台。配对码默认 15 分钟有效，不放入 URL；初始化完成后不能再次创建首管理员。尚未提交初始化计划时，可以再次运行 `setup-token` 轮换配对码。

Compose 自动创建私网 PostgreSQL 和随机数据库密码，数据库不向宿主机公开端口，页面使用托管数据库，无需填写数据库连接。初始化容器只准备项目专用卷；管理应用以非 root 用户运行。普通启动日志不会打印数据库密码或配对码。

进入配置页后，修改显示名称或会话时长，点击预览差异，再应用。显示名称在后台读取新配置时生效，会话时长影响后续登录；已有会话保留原到期时间。两个窗口基于同一旧版本提交时，后提交者收到 `REVISION_CONFLICT`，原草稿保留，需要读取新版本后重新预览。「部署审计」按稳定游标查询初始化、配置、账号、Bot 目录、Binding 预留与插件导入的脱敏元数据。

进入「Bot 与组件」，填写名称并创建 Bot。托管模式为每个 Bot 创建独立数据库与受限账号，页面展示创建进度；服务中断后按同一持久计划恢复。存储就绪后，配置内置计数器的显示名称和倍率，授权状态读写，预览并应用。期望修订先保存，组件成功启动或完成排空切换后才推进生效修订。「调用组件」可验证各 Bot 状态独立，修改倍率后计数继续保留。

「插件包管理」支持导入可信 Python 插件的完整 ZIP，并按版本为 Bot 配置和启用。常见 schema 自动生成字段，复杂配置在页面内编辑 JSON；配置历史可载入草稿并作为新修订应用。坏版本不会替换当前有效实例，重启也优先恢复当前生效版本。详见 [Web 插件管理](doc/p2-web-plugin-management.md)。

在 Bot 页面「QQ 账号连接」登记 OneBot 11 账号，填写 HTTP API 地址、访问令牌、上报签名密钥，预览并应用。将页面提供的上报地址和相同密钥填入所用 OneBot 实现。系统先调用 `get_login_info` 校验账号，再启用连接；密钥不回显。默认仅记录消息，选择「模板回复」后可通过 @、`/chat` 或私聊触发。群聊管理的管理员 QQ 号在同一表单配置，不自动信任平台群管理员身份。

使用模型对话时，在「Agent 与模型」设置 Chat Completions 兼容基础地址、模型和密钥，预览并启用，再将账号回复模式设为「Agent 对话」。工具审批、运行取消和未知工具结果核实也在此页面处理。图片来源需明确允许；文本模型调用时只投影文本，原始多模态记录保留。详细语义与能力边界见 [P3 Agent 说明](doc/p3-agent.md)。

在同一 Bot 的记忆面板配置图文嵌入服务、密钥、允许的图片来源，以及索引、下载和查询预算，预览后启用。后台独立捕获原图并索引已接纳的全群消息；Agent 可根据触发消息的文字和图片召回其他群的记忆，结果逐条标注群及作者。文本模型会在调用时移除图片，图片索引与召回仍可工作。来源列表按群和作者展示完整图文来源；遗忘先生成影响预览，再以删除版本 CAS 提交，召回立即抑制，正文由后台分批清理。当前支持 `jina-clip-v2` 原生图文协议；配置、费用语义与未完成边界见[记忆运行说明](doc/p4-memory-runtime.md)和[来源管理说明](doc/p4-memory-administration.md)。

「计划任务」可配置 interval/cron、时区、补跑规则、并发限制和委托期限，预览后保存，也可以立即执行、停用或取消待处理任务。任务按同一群聊触发队列运行，进入该会话唯一的 Agent Session，不另开模型 Agent 或群响应链。群内管理员可用 `#forest status` 读取维护版本，使用 `#forest bot pause --expect-maintenance <版本> --reason "原因"` 或对应的 `bot resume` 命令紧急暂停、恢复本 Bot，也可通过 `run list`、`jobs list` 和 `memory list` 查询并按显示版本取消、审批或遗忘本群来源。`event` 管理组件事件，`component-job` 单独管理插件持久任务；`audit list`（别名 `log/logs list`）按稳定游标读取当前 Bot 的脱敏审计元数据，不打开宿主终端或进入 Agent。实际语法和授权边界见[群聊 Shell 详细设计](doc/09-group-shell-command-system.md)。

Docker 内连接宿主机 OneBot 时，API 地址可使用 `http://host.docker.internal:5700`；Linux Engine 可按本机网络配置显式映射 host gateway，或让 OneBot 加入同一 Docker 网络。OneBot 上报必须能访问 Forest 的 HTTP 入口。协议与恢复边界、群聊 Shell 和发送核对详见[接入说明](doc/p1-integration.md)。

查看状态或停止服务：

```sh
docker compose ps
docker compose logs --tail 100 forest
docker compose down
```

普通 `down` 保留命名卷。`forest_data` 包含本地初始化状态、加密主密钥、包快照和加密备份，`forest_secrets` 包含数据库凭据，`postgres_data` 包含部署、托管 Bot 数据库及尚未清理的隔离恢复副本。管理员可在「存储与备份」创建部署及全部 READY Bot 的加密逻辑备份，保存恢复口令并下载工件；每个数据库是独立一致切片，目录变化会标为 `INCONSISTENT`。建立隔离副本前必须先完成独立只读校验：后台认证解密精确密文、检查 ZIP、清单和切片证明，生成 24 小时有效且绑定密文与清单摘要的凭据，不创建或连接恢复数据库。隔离恢复只接受有效凭据，再将整套工件恢复为随机命名的数据库，精确核对 schema、行数、外键和身份，并封存 Bot 副本。托管 Bot 副本可在页面生成五分钟激活预览并经当前密码提交；服务端要求当前库与恢复库的删除代际、Binding 和关键副作用账本一致，然后先封存旧库、原子推进目录代际 head、最后启用新库。中断保持 Bot 离线，同一持久任务可人工重试。备份、未采用的隔离副本和激活后旧存储代际分别默认保留 30 天、7 天和 7 天；当前 head 永远不能进入旧代回收，物理删除前还会复核受管集群、数据库 owner 与 Forest marker。部署控制面整库激活仍需本机恢复流程，不能通过删除初始化状态或手工登记数据库绕过。管理员也可在 WebUI 创建后台账号，并通过带预览、当前密码复核和会话撤销的账号计划重置密码、调整固定角色或停用账号。

## 使用 Python 开发

需要 Python 3.11、uv 和一个允许创建表、索引及函数的独立 PostgreSQL 数据库。

```sh
uv sync --frozen
uv run --frozen forest serve --data-dir .forest
```

在另一个终端、同一项目目录取得配对码：

```sh
uv run --frozen forest setup-token --data-dir .forest
```

打开同一后台地址，在初始化页面填写配对码、PostgreSQL URI 和管理员信息。URI 必须明确指定单个目标的主机、数据库和用户，例如 `postgresql://forest:your-password@127.0.0.1:5432/forest`。数据库定位及凭据在首次初始化中冻结并加密保存；日常配置在 WebUI 中修改。

初始化使用受限本地 SQLite 仅保存安装状态，业务配置、账号和会话使用 PostgreSQL。冻结目标后只能恢复原计划，数据库暂时不可达时会后台重试；已完成的部署会先核对数据库初始化身份，不会对空库重新建立管理员。

原生模式没有托管 PostgreSQL 配置时，可在创建 Bot 页面填写已准备好的空数据库连接，使用该数据库的专用所有者账号。Bot 运行账号不能有超级用户、建库或建角色权限；既有业务库、部署库及其他 Bot 库会被拒绝。存储连接加密保存在后端，普通配置仍通过页面完成。

## 原生插件开发切片

当前可运行示例是 [examples/counter](examples/counter/python/counter.py)：继承 `Plugin`，使用 `@service` 声明异步服务，通过 `ctx.state` 读取和提交版本化状态，通过 `ctx.operations.stable()` 派生重试不变的步骤 ID。示例随包提供 `module.toml` 和输入、输出、配置 schema；开发 watch 已实现，自动生成 manifest 与 `@plugin/@command` 尚未实现。

以下命令不需要 Bot 数据库：

```sh
uv run --frozen forest plugin build examples/counter
uv run --frozen forest plugin check examples/counter
```

`build` 静态校验完整包并保存内容寻址快照，不执行插件代码。`check` 额外启动独立候选 Worker，验证导入、prepare/start/health 后回收；候选没有生产 state RPC 权限。`check/dev` 可通过 `--config-json` 提供经包 schema 校验的配置，默认 `{}`；`build` 不要求这份默认配置满足运行时必填字段。默认缓存目录为 `.forest/packages`，可通过 `--cache-dir` 修改。普通构建修改包内容后需提升 `module.toml` 的版本，同版本不同内容会被拒绝；开发 `--watch` 则按内容自动派生 `+dev.<digest>` 版本。

`forest dev` 在一个**专用开发 Bot 数据库**中执行一次服务调用，随后关闭 Worker 和宿主租约。首次使用的数据库必须为空，不能复用后台部署库、其他 Bot 库或已有业务库；该库一旦初始化就固定绑定 `--bot`，后续可复用以检查状态持久化。

PowerShell 示例；先将 URI 替换为已经准备好的空开发数据库。通过标准输入传递 JSON，可避免原生命令参数的引号转义问题：

```powershell
$env:FOREST_BOT_DSN = 'postgresql://forest_dev:your-password@127.0.0.1:5432/forest_dev_counter'
'{"increment":1}' | uv run --frozen forest dev --bot dev-counter --package examples/counter --service counter.increment --input-json - --operation-id demo-increment-1 --grant state.read --grant state.write
```

POSIX shell 对应命令：

```sh
export FOREST_BOT_DSN='postgresql://forest_dev:your-password@127.0.0.1:5432/forest_dev_counter'
uv run --frozen forest dev --bot dev-counter --package examples/counter --service counter.increment --input-json '{"increment":1}' --operation-id demo-increment-1 --grant state.read --grant state.write
```

空库的第一次调用应返回 `{"status":"completed","result":{"value":1,"revision":1}}`。同一个 `operation-id` 和输入重试读取原结果；新的加一操作使用新 ID，省略该参数会自动生成新 ID。一次状态步骤已提交而整次调用随后失败时，状态提交仍然保留，失败调用不会自动重新执行。

`FOREST_BOT_DSN` 与 `dev` 是开发验证入口。日常 Bot 创建与组件配置直接通过 WebUI 完成。持续开发可使用以下命令；首次配置和 grants 固定，后续源码保存不会扩大权限。修改 helper、schema 或资源也会自动重载，失败保留旧实例，重启从持久 last-good 工件恢复。

```sh
forest dev --bot example --watch examples/counter --grant state.read --grant state.write
```

外部依赖通过离线 wheelhouse 与完整锁定清单装入独立不可变环境；插件不继承宿主的第三方库。包导入导出和锁文件命令见 [插件开发说明](doc/p2-plugin-packages.md)，持久状态格式升级见 [迁移说明](doc/p2-state-migration.md)。当前不迁移 Python 调用栈；Agent 使用宿主数据库中的持久 checkpoint 恢复，不能把插件 KV 迁移等同于运行栈迁移。

跨组件调用使用 `ctx.services.call(name, payload, contract_major=1, operation_id=ctx.operations.stable(step))`。调用方必须同时声明 manifest `requires` 与 `service.call:<name>` 能力，宿主按固定依赖和当前路由生成子调用权限，Worker 不提供目标 Bot 或 CallGrant。`Kernel.install_many()` 在执行插件前验证服务唯一性、契约和依赖 DAG，按依赖顺序安装、逆序回收。它是有逐项回执的批量操作；失败不撤销已经成功发布的组件。当前可选依赖在安装时固定，后续依赖变更需要显式重载调用方。

插件创建的连接或客户端应使用 `ctx.resources.register(...)` 登记。服务调用中的资源随该调用在返回前逆序关闭；`prepare` 根 Context 中的资源在代际停止时、且在 `Plugin.stop` 之后逆序关闭。manifest 的 `max_owned_resources` 提供有界登记额度，详细错误与取消语义见 [P0 Worker 资源监督 SDK](doc/p0-resource-sdk.md)。

组件事实通过 manifest `publishes/subscribes` 与代码 `@subscribe(...)` 显式声明。`ctx.events.publish(...)` 在宿主核验当前调用、能力、契约和 schema 后持久写入事件正文引用及每订阅 delivery；dispatcher 按稳定 subscriber 身份独立确认、重试和死信，热重载只切换执行代际。Bot 页的「组件事件」面板可以筛选投递、显式回放 DEAD、预览并提交订阅退役；页面只显示元数据，不读取事件正文。详见 [P0 持久组件事件](doc/p0-component-events.md)和[组件事件管理](doc/p0-component-event-management.md)。

插件自有的延迟或长工作通过 manifest `jobs`、代码 `@job(...)` 和调用内 `ctx.jobs.schedule(...)` 提交。宿主持久化正文引用、固定组件主体与尝试/超时/并发预算，使用调用账本处理丢失 ACK，并在兼容热重载后交给新代际。Bot 页的「组件持久任务」面板可筛选元数据、显式回放 DEAD，并以 preview/apply 退役处理器；退役会立即封住旧代的新调度和认领。它与上面的群会话「计划任务」分属不同协议，不创建新的模型 Agent；详见[组件持久任务详细设计](doc/p0-component-jobs.md)和[组件持久任务管理](doc/p0-component-job-management.md)。

## 远程入口与端口

默认只绑定宿主机 loopback。远程管理需要在宿主机反向代理终止 HTTPS，再转发到 `127.0.0.1:8080`，并保留外部 `Host`。代理应保留浏览器实际 `Origin`，页面与 API 使用同一来源。

Compose 支持启动环境变量 `FOREST_PUBLIC_ORIGIN` 和 `FOREST_HTTP_PORT`。以下是 PowerShell 示例，域名、TLS 证书与反向代理需由部署者配置：

```powershell
$env:FOREST_PUBLIC_ORIGIN = 'https://bot.example.com'
docker compose up --build -d --wait
```

POSIX shell 可用 `FOREST_PUBLIC_ORIGIN=https://bot.example.com docker compose up --build -d --wait`。原生启动可指定 `forest serve --public-origin https://bot.example.com`。来源必须与浏览器一致且不带路径；远程 HTTP 会被启动校验拒绝，HTTPS 来源使用 Secure cookie。

更换本机映射端口时同时设置来源，例如 `FOREST_HTTP_PORT=8090` 与 `FOREST_PUBLIC_ORIGIN=http://127.0.0.1:8090`。这些变量配置的是启动入口，不替代 WebUI 的日常配置。

## 运行测试

不提供 PostgreSQL 时，可运行安全、bootstrap、包校验及真实本地 Worker 等测试，数据库相关测试会明确跳过：

```sh
uv run --frozen pytest -q
uv run --frozen ruff check .
```

完整数据库测试需要 `FOREST_TEST_DSN` 指向**专门的测试 PostgreSQL**。测试账号需要超级用户权限，以验证建库、受限角色、SCRAM 归属与跨库隔离；各测试分配随机 schema/数据库/角色，结束后只清理自己创建的资源。不要指向真实部署或共享业务数据库。

可单独创建临时测试实例，以下凭据仅用于这个 loopback 测试容器：

```sh
docker run --rm -d --name forest-test-postgres -p 127.0.0.1:55432:5432 -e POSTGRES_DB=forest_test -e POSTGRES_USER=forest_test -e POSTGRES_PASSWORD=forest-test-only postgres:17.10-alpine3.23
docker exec forest-test-postgres pg_isready -U forest_test -d forest_test
```

确认数据库就绪后，在 PowerShell 中执行：

```powershell
$env:FOREST_TEST_DSN = 'postgresql://forest_test:forest-test-only@127.0.0.1:55432/forest_test'
uv run --frozen pytest -q
```

POSIX shell 中改用 `export FOREST_TEST_DSN='postgresql://forest_test:forest-test-only@127.0.0.1:55432/forest_test'`，再执行测试。完成后可用 `docker stop forest-test-postgres` 停止并移除上述临时容器。

真实浏览器验证使用独立临时 Compose project 和随机端口，默认使用 Playwright Chromium：

```sh
docker compose build
uv run --frozen playwright install chromium
uv run --frozen python scripts/smoke_browser.py
```

Windows 已有 Edge 时可直接指定浏览器：

```powershell
uv run --frozen python scripts/smoke_browser.py --browser 'C:\Program Files (x86)\Microsoft\Edge\Application\msedge.exe'
```

脚本验证首次初始化、登录、后台账号创建与查看者读写边界、部署配置、双 Bot 创建与状态隔离、组件热切换、双窗口冲突、OneBot 账号配置与真实 HTTP 收发、UNKNOWN 核对、服务重启恢复和注销撤销；`finally` 清理本次随机 project 的容器及卷，保留 `.local/browser-artifacts/` 下的截图。它不会操作普通 `forest` Compose project。浏览器、Linux 与外部服务的实际验证结果以[实现状态](doc/implementation-status.md)为准，测试脚本存在不代表已经通过。
