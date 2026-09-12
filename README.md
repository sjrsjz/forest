# Forest

Forest 正在实现一个采用原生 Python 微内核、支持多 Bot 和完整图文记忆的重型机器人框架。目标架构见[设计文档](doc/README.md)。

**当前可运行的是 P0 的后台管理闭环与原生插件内核切片，整个 P0 尚未完成。**后台支持浏览器初始化、登录和部署配置；开发入口支持不可变 Python 包、独立 Worker、受监督 SDK 调用、单 Bot PostgreSQL 状态及同 schema 的排空替换。WebUI 尚不能创建或接入 Bot，QQ、Agent、记忆和群聊 Shell 仍未实现。完整能力与验证边界见[实现状态](doc/implementation-status.md)。

## 使用 Docker 启动

需要正在运行的 Docker Engine，以及支持 `--wait` 的 Docker Compose。在项目根目录执行：

```sh
docker compose up --build -d --wait
docker compose exec forest forest setup-token
```

打开 [http://127.0.0.1:8080](http://127.0.0.1:8080)，填写终端给出的一次性配对码，创建管理员账号和密码，然后登录后台。配对码默认 15 分钟有效，不放入 URL；初始化完成后不能再次创建首管理员。尚未提交初始化计划时，可以再次运行 `setup-token` 轮换配对码。

Compose 自动创建私网 PostgreSQL 和随机数据库密码，数据库不向宿主机公开端口，页面使用托管数据库，无需填写数据库连接。初始化容器只准备项目专用卷；管理应用以非 root 用户运行。普通启动日志不会打印数据库密码或配对码。

进入配置页后，修改显示名称或会话时长，点击预览差异，再应用。显示名称在后台读取新配置时生效，会话时长影响后续登录；已有会话保留原到期时间。两个窗口基于同一旧版本提交时，后提交者收到 `REVISION_CONFLICT`，原草稿保留，需要读取新版本后重新预览。

查看状态或停止服务：

```sh
docker compose ps
docker compose logs --tail 100 forest
docker compose down
```

普通 `down` 保留命名卷。`forest_data` 包含本地初始化状态和加密主密钥，`forest_secrets` 包含数据库凭据，`postgres_data` 包含部署事实；恢复部署需要保留三者。当前还没有 Web 备份、密码重置或存储迁移功能，不能通过删除初始化状态绕过恢复流程。

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

## 原生插件开发切片

当前可运行示例是 [examples/counter](examples/counter/python/counter.py)：继承 `Plugin`，使用 `@service` 声明异步服务，通过 `ctx.state` 读取和提交版本化状态，通过 `ctx.operations.stable()` 派生重试不变的步骤 ID。示例随包提供 `module.toml` 和输入、输出、配置 schema；自动生成 manifest、`@plugin/@command` 和自动 watch 尚未实现。

以下命令不需要 Bot 数据库：

```sh
uv run --frozen forest plugin build examples/counter
uv run --frozen forest plugin check examples/counter
```

`build` 静态校验完整包并保存内容寻址快照，不执行插件代码。`check` 额外启动独立候选 Worker，验证导入、prepare/start/health 后回收；候选没有生产 state RPC 权限。`check/dev` 可通过 `--config-json` 提供经包 schema 校验的配置，默认 `{}`；`build` 不要求这份默认配置满足运行时必填字段。默认缓存目录为 `.forest/packages`，可通过 `--cache-dir` 修改。修改包内容后需提升 `module.toml` 的版本，同版本不同内容会被拒绝；当前不会自动生成开发版本。

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

`FOREST_BOT_DSN` 与 `dev` 是开发验证入口，不是普通管理员的配置方式。日常配置仍以 WebUI 为目标入口，当前后台仅开放显示名称与新会话有效期。内核已有排空替换 API，但上述 CLI 不持续运行、不监视文件，也不提供 QQ 接入或完整热重载恢复。

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

完整数据库测试需要 `FOREST_TEST_DSN` 指向**专门的测试 PostgreSQL**。测试账户需要创建数据库和 schema 的权限：repository 测试创建随机 `forest_test_*` schema，Web、BotStore、Kernel 测试分别创建随机 `forest_web_*`、`forest_bot_*`、`forest_kernel_*` 数据库，结束后只清理自己创建的资源。不要指向真实部署或共享业务数据库。

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

脚本验证首次初始化、登录、预览应用、双窗口冲突、服务重启后持久配置和注销撤销；`finally` 清理本次随机 project 的容器及卷，保留 `.local/browser-artifacts/` 下的截图。它不会操作普通 `forest` Compose project。浏览器、Linux 与外部服务的实际验证结果以[实现状态](doc/implementation-status.md)为准，测试脚本存在不代表已经通过。
