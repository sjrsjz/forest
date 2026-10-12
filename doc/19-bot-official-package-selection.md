# Bot 官方包的选择与发布边界

本章规定 Bot 作用域官方包的版本选择。它是 `17-component-worker-host.md` 的补充。官方来源登记、按 Bot 精确选择和宿主本地发布命令已经实现；真实生产目录中的新版发布与在线切换仍须验收。

## 权威与作用域

部署内核独占 Bot 基础身份、供应状态、期望运行状态、运行观察和相应幂等操作账本。`deployment.bot-catalog` 是可热替换的管理包；它只通过内核授予的有界 Query/Command 提供目录与运行意图管理，不能拥有另一份 Bot 表、直接跨 schema 写内核账本，或以包配置决定 Bot 是否存在。Bot 私有的 `component.worker-host` 则拥有该 Bot 的组件选择账本和控制工作；它不能据此改写部署内核的 Bot 身份或运行意图。两种选择事实使用不同作用域和授权端口。

`builtin` 表示随发行物提供的真实插件包，不是 Python import 特权。包仍以内容摘要进入目录，取得准确的 Generation、配置仓库、端口能力和热替换生命周期。包的 manifest 自报 `official` 存储执行权限不构成来源证明；只有宿主受信任入口在独立的 Bot 官方来源登记中写入该摘要，Bot 才可按官方权限装配它。普通 ZIP 导入或普通本地构建不得获得这种登记。

## 选择事实

每个 Bot 的 Worker Host 私有账本按 `component_id` 保存一个期望条目。目标协议明确区分：

| 来源 | 含义 | 摘要 | 能力批准 |
| --- | --- | --- | --- |
| `builtin` | 使用本次宿主发行物中自动发现的该组件工件 | 空 | 空 |
| `official` | 使用目录中已登记为 Bot 官方来源的准确工件 | 包与 manifest 摘要必填 | 空 |
| `installed` | 使用经外部包规则检查的准确工件 | 包与 manifest 摘要必填 | 按普通包规则批准 |

同一条目还保存 `desired_enabled`、账本修订和操作来源。`official` 必须与发行物自动发现的**同一个**组件 ID、Bot scope、包运行时和官方存储权限相符；不得凭官方来源登记把任意新组件注入默认官方集合，也不得用 `installed` 抢占官方组件 ID。所有来源的停用条目仍需校验所指工件身份，以免失效摘要在下一次启用时突然恢复。Kernel 组件不能进入选择账本。

候选选择的唯一输入是 Worker Host 在同一个 Bot 选择围栏下提供的规范快照。只读预检由该包投影拟议修订；宿主据快照解析精确工件、读目标配置和 Payload 事实、编译**不可执行**的正式描述符并使用 RuntimeGraph 规划器求替换闭包。预检不安装 SQL、不准备 Worker、不写选择。提交在授权事务内重新读取选择、Graph、工件、配置、容量和计划摘要；选择写入后由持久工作请求驱动候选装配。装配必须先解析并验证每个工件，随后才安装其 schema 或建立 Generation；发布前持有选择共享围栏重查修订与摘要。冷启动从同一快照恢复，缺失或失配的已选摘要使 Bot 明确失败，不能退回最新 builtin。不同 Bot 可以选择同一个官方组件的不同摘要，且各自 Graph 和配置事实独立。

### Worker Host 的引导环

`component.worker-host` 自己保存选择账本，不能先读取自己尚未引导的版本选择。当前它由宿主发行物提供的官方源包先引导，读取器再在热替换时比较旧代与候选代对**同一私有账本**的投影；schema 身份不得悄悄变化。其自身跨版本热替换仍走正常 `module.reload`、Generation 排空与发布协议。精确摘要选择的首批范围应排除这个自举组件；如果以后要求每个 Bot 固定 Worker Host 摘要，需要先把它的引导摘要移入独立、可冷读的内核 bootstrap 事实，且保证它不复制组件选择账本。不得靠“先用最新版读，再换成任意旧版”的猜测解决引导环。

## 官方新版的发布入口

目录的 `build_official(..., scope="bot")` 只登记宿主发行物自带的源码；新版使用宿主本地命令 `forest plugin publish-official-bot`。命令必须由能够以包目录所有者身份在主机或 Supervisor 容器内执行的操作员调用，并显式指定已存在的绝对包目录、完整 ZIP、ZIP 字节摘要、内容寻址包摘要、manifest 摘要、包 ID 与组件 ID。此处的等价宿主授权证据是本地进程对私有生产包目录的 OS 所有权与写权限，不是 ZIP 自报字段。命令仅允许发行物当前自动安装的 Bot 官方组件，核对其包 ID 与运行时，并排除自举的 `component.worker-host`。Web、群聊 Shell 和 Worker 没有授予官方来源的端口；普通 ZIP 导入仍不能标记成官方。目录成功只增加可选工件，不自动改变任何 Bot 的选择。每个 Bot 的切换须再走自己的只读预检、授权提交和热替换；相同版本号不同字节不能覆盖旧工件。

现有宿主源码登记的工件索引、包版本绑定和官方来源事实必须在同一 SQLite 写事务内提交；其中任何一步失败都不能留下可查询的半注册官方工件。文件捕获早于事务完成时留下的未登记目录也不能成为 Bot 的选择来源。这是未来可信发布入口复用的目录原子性边界，并不授权普通 ZIP 获得官方身份。

目录的 `publish_official_archive` 在写事务前验证完整 ZIP 的字节摘要、内容寻址包摘要、解析后的 manifest 摘要、包 ID、组件 ID、scope、运行时和官方存储声明，再在同一事务发布索引、版本绑定与官方来源。它不独立判断调用者授权；本地命令先核对发行物组件白名单，随后只以当前目录所有者的进程权限调用它。包捕获目录若早于事务写完而留下，未登记前也不能被选择。普通 `plugin import` 和 Web 包上传仍只登记普通工件，不调用该入口。Python Worker 当前不是恶意代码 OS 沙箱，因此同一服务 UID 下只运行已受信任的代码；不可信插件须另有可验证的 OS/Wasm 隔离后才能安装。

可先在隔离目录运行普通 `forest plugin import routing-2.1.0.zip --cache-dir <隔离目录>`，从其 JSON 回执取得并复核 ZIP、内容和 manifest 摘要及包/组件 ID；此步仍不授予官方来源。随后在拥有生产目录的宿主环境中执行：

```text
forest plugin publish-official-bot routing-2.1.0.zip --catalog-dir /var/lib/forest-packages --archive-sha256 sha256:<ZIP摘要> --package-digest sha256:<内容摘要> --manifest-digest sha256:<manifest摘要> --package-id forest.routing-graph --component-id routing.graph
```

### Deployment 与 Host builtin 的本地发布

`forest plugin publish-official-deployment` 与 `forest plugin publish-official-host` 复用上述 ZIP、三层摘要、包 ID、组件 ID 和绝对私有目录参数。命令从当前发行物的 `auto_install` 清单核对目标作用域、组件 ID、包 ID 与运行时，再通过同一个原子目录入口把工件登记到**对应作用域**的官方来源表；部署与 Host 来源不能互相借用。它们只在持有目录写权限的本地进程中发布；Web、Shell、Worker 管理契约不暴露官方来源写端口，普通 ZIP 导入也不授予该来源。发布只增加准确可选工件，不改变运行图、配置或目标组件的 Generation；替换仍需各作用域现有的预检、授权 Graph 提交和 drain 生命周期。这个入口仅覆盖随当前发行物登记的 builtin 身份，不是第三方包自报官方 SQL 的通道。

例如，准备并检查 Host 审计包 ZIP 后可执行：

```text
forest plugin publish-official-host host-audit-next.zip --catalog-dir /var/lib/forest-packages --archive-sha256 sha256:<ZIP摘要> --package-digest sha256:<内容摘要> --manifest-digest sha256:<manifest摘要> --package-id forest.host-audit --component-id host.audit
```

## 当前实现与验收缺口

Linux 运行镜像已在 UID 10001、无网络、只读根文件系统和独立包卷中实际运行本地发布命令，新旧 `routing.graph` 官方工件同时可读；该隔离验收没有修改在线包卷或 Bot 选择。真实生产目录发布、在线热换代和长期负载仍是独立验收项。

2026-10-09 的候选 Supervisor 进一步使用自己的持久包卷和真实 Bot 数据库验证发布与选择分离：本地命令发布官方 `routing.graph` 2.0.3 时，目录保留 2.0.2，两个 Bot 均未改变运行意图；随后仅让连接 Docker 内网 OneBot mock 的 Bot 上线，通过 Bot Web 管理端预检和 `module.enable` 提交选择。Graph 修订 3→4，只有 `routing.graph` Generation 改变；该 Bot 停机后，两代 Graph 均 `STOPPED`，新图全部 Generation 与被替换旧代均 `DISPOSED`。另一个持有真实 QQ 凭据的候选 `forest-main` 始终 `OFFLINE`，旧在线实例及其包卷未改动。这证明隔离候选部署的完整发布/热替换路径，不替代旧在线目录、实网切换或长时负载验收。

工件详情 `/2` 投影该 scope 的 `official_registered`，由目录独立登记计算而不信任 manifest。Worker Host 选择账本、WebUI 和 Shell 输入现接受 `official` 条目；包内 SQL 与各层 schema 要求准确包/manifest 摘要和空批准能力，拒绝对 Worker Host 自身固定摘要。宿主在准备组件前核对官方登记与发行物组件/包身份；预检从所选摘要编译不可执行描述符，提交在同一 Graph/选择/配置围栏内重算计划，发布和冷启动读取同一持久选择事实。

隔离 PostgreSQL 完整 BotHost 用例通过 Web 管理契约验证并提交 `routing.graph` 的另一官方工件，实际 Graph 发布后准确包摘要改变、内核 Generation 保留；Worker Host 随后自热重载，关闭整个 BotHost 再从同一 Bot 数据库和持久包目录冷启动，仍恢复所选摘要。包内选择账本用例验证官方条目的投影与写入相等，并拒绝额外能力和自举组件。上述验收没有发送 QQ，也没有证明官方新版如何进入生产目录。

同一隔离 BotHost 用例还将固定的 routing 工件目录临时移走，再从原数据库冷启动，得到 `PACKAGE_CHANGED` 且没有发布运行图，证明缺失工件不会回退到发行物默认版本。双 Bot 验收使两个 Bot 同时在线并共享官方目录、各用独立数据库固定同一 `routing.graph` 的不同官方摘要；第二个 Bot 提交选择并发布后，第一个 Bot 的 Graph 身份和所选摘要保持不变。另一个已登记的官方工件故意在 Worker `prepare` 失败：选择工作记录 FAILED，旧 Graph 身份与管理查询保持可用；将该 Bot 的选择修回先前摘要后，替换工作成功，冷启动也恢复该摘要。故障候选未发布，其清理只排空 ACTIVE 成员，不能把尚未构造或已经停止的候选成员当作在线持久任务。本地发布命令已在隔离目录中验证不同版本的 `routing.graph` 并保留默认版本的官方登记；真实生产目录中的发布、在线 Bot 切换与长期负载仍需独立验收。
