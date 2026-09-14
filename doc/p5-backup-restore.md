# P5 多数据库备份、校验与隔离恢复

状态：详细设计与恢复/清理纵向切片；加密工件、持久备份任务、每库切片、目录前后锚点、管理员 Web 创建/下载、外部工件流式导入、独立只读校验回执、托管 PostgreSQL 的整套隔离恢复，以及备份/隔离副本的保留期清理已实现。恢复激活继续按本文推进。

## 1. 范围与不变量

Forest 的部署目录、每个 Bot 事实库和宿主工件目录是三个不同权威域。首版备份必须覆盖部署 PostgreSQL、创建时已冻结的 Bot 清单、每个可达 Bot PostgreSQL、插件内容寻址工件，以及恢复加密凭据所需的受保护主密钥。向量和图当前同属 Bot 事实库；若以后迁到外部索引，manifest 必须明确标记为可重建派生物或独立快照，不能默认为已经备份。

备份不制造跨库事务。每个数据库各自在 `REPEATABLE READ READ ONLY` 快照中导出，manifest 保存数据库身份、schema 迁移集合、事务快照、WAL LSN、捕获起止时间、Bot 删除 epoch、表/列清单、行数和内容摘要。部署快照先冻结本次 roster，全部 Bot 捕获后再读取部署目录锚点；两次目录 revision 不一致时工件仍可用于取证，但状态为 `INCONSISTENT`，不能直接进入恢复计划。

以下规则始终成立：

1. 活动数据库不会被备份恢复原地覆盖；恢复目标必须是经过验证的空库并保持 `ISOLATED`。
2. 旧备份缺少不早于恢复点的删除日志或 suppression 屏障时，不允许开放 Bot 读取、派生任务、Adapter 或 Agent。
3. 已发送、UNKNOWN、已许可工具效果和控制操作账本作为事实恢复；恢复器不把 outbox 重置为未发送，也不重做外部效果。
4. 普通下载不含明文密钥。整个工件由随机数据密钥流式认证加密，数据密钥由管理员提供的恢复口令包装；口令只在请求内存中存在。
5. 数据密钥另由当前部署 SecretVault 包装并保存到持久 Job，使进程重启可以接续同一工件。下载工件不依赖原部署主密钥解包。
6. 备份、校验、恢复和切换都是有界持久操作。浏览器断开不取消任务，相同幂等键只能恢复同一输入摘要。
7. 部署与 Bot schema 必须精确兼容当前恢复器；未知迁移、缺表、额外列、约束环或摘要不一致一律保持隔离。

## 2. 工件格式

`ForestBackupV1` 是单文件容器：固定 magic、长度受限的 canonical JSON header、加密正文和末尾认证标签。header 只含格式版本、KDF 参数、salt、数据密钥包装 nonce/ciphertext、正文 nonce、正文长度和工件 ID，不含数据库名、Bot 名、账号、DSN 或内容摘要。

解密正文为一个 ZIP64 容器，但所有成员名来自固定生成器，不接受调用方路径。它包含：

```text
manifest.json
recovery/master.key
deployment/tables/<ordinal>-<table>.bin
bots/<bot_id>/tables/<ordinal>-<table>.bin
artifacts/<package_digest>.zip
```

数据库表使用 PostgreSQL `COPY ... TO STDOUT (FORMAT BINARY)` 的原始字节。manifest 固定数据库 server major、encoding、collation、schema migration checksum、表名、按 `attnum` 排列的列、列类型 OID/名称、生成属性、主键和外键依赖。恢复只对由当前迁移建立且 manifest 完全一致的表执行 `COPY FROM STDIN`；不拼接备份提供的 SQL，也不执行工件脚本。

`master.key` 只在已加密正文中出现，用于恢复部署库中的 DSN、账号计划和 Bot 内 SecretVault ciphertext。Bootstrap SQLite 不直接复制，因为它固定原部署 DSN；恢复器从已验证的部署身份和目标 DSN 创建新的本地冻结计划，避免旧地址重新成为启动权威。

插件工件按部署表中的已注册 digest 求闭包，只复制内容寻址归档；内置插件由当前镜像提供。每个成员在 manifest 中记录 SHA-256 与字节数。临时文件、浏览器截图、Python 环境、日志、缓存索引和锁文件不进入工件。

## 3. 持久任务和并发

部署库增加 `backup_jobs`、`backup_database_cuts`、`backup_artifacts` 和不可变事件表。Job 状态为：

```text
QUEUED -> CAPTURING -> SEALING -> READY
             |             |         |
             +-----------> FAILED     +-> EXPIRED
                           |
                           +-> INCONSISTENT -> EXPIRED
```

独立校验使用 `backup_verification_jobs` 和不可变事件表。管理员通过恢复口令解包随机数据密钥后，接纳事务冻结来源备份、精确密文摘要、SecretVault 再包装的数据密钥和数据库数量。后台任务只在私有临时目录认证解密并检查 ZIP、清单、成员摘要、roster 和每库 cut 证明；它不创建、不登记也不连接恢复数据库。成功后发布 24 小时有效的 `VERIFIED` 回执，绑定 `backup_id + artifact_digest + manifest_digest`。任务使用 `QUEUED → VERIFYING → VERIFIED|FAILED`，回执到期进入 `EXPIRED`；领取、续租、失败和完成均带 owner epoch。

隔离恢复使用独立的 `restore_stage_jobs`、`restore_stage_targets` 和不可变事件表。一个恢复任务只接受尚未过期的 `VERIFIED` 回执，在接纳事务再次核对来源仍为 READY、未删除、未过期，并要求当前密文与清单摘要都和回执一致。它冻结回执 ID、来源密文摘要、来源备份、目标数据库清单和每库 cut 摘要；数据密钥从回执的 SecretVault 密文复制到恢复任务，Web 请求不再提交恢复口令。状态为：

```text
QUEUED -> VERIFYING -> RESTORING -> VALIDATING -> STAGED
             |             |             |
             +-------------+-------------+-> FAILED
```

恢复副本在 `STAGED/FAILED` 后可以进入 `EXPIRED`。备份和恢复副本共用 `storage_cleanup_plans`、`storage_cleanup_jobs`、幂等请求与不可变事件，但各自保留原生命周期事实。显式清理必须先生成五分钟有效的预览计划，应用时复核管理员当前密码、计划摘要、资源摘要和活动恢复/校验依赖。自动保留期使用相同 tombstone/Job 事务，不伪造浏览器计划。`QUEUED/VERIFYING` 校验任务和活动恢复任务暂时固定来源工件；已完成的回执不阻止显式删除，删除后即使回执尚在有效期也不能建立恢复任务。

清理 Job 为 `QUEUED/RETRY → CLEANING → COMPLETE|BLOCKED`，每次领取递增 owner epoch 并持有 lease。`BLOCKED` 表示路径、目录登记或数据库 marker 冲突，或普通 I/O 在五次有界退避后仍失败；管理员排除外部冲突后可重新认证并将同一 Job 放回队列。重试沿用原资源身份，不会通过换目标继续删除。

外部工件使用独立的 `backup_import_jobs` 与不可变事件表，不能把浏览器上传直接登记成 `backup_artifacts`。协议分为三个边界：管理员以当前密码和精确字节数建立 24 小时预留；浏览器用 `application/vnd.forest.backup` 原始 `PUT` 流式上传；上传完成后再次提交当前密码和恢复口令，把任务推进到后台校验。状态为：

```text
RESERVED -> UPLOADING -> UPLOADED -> QUEUED -> IMPORTING -> IMPORTED
    |            |           |                     |
    +-> EXPIRED  +-> RESERVED+-> EXPIRED           +-> FAILED
```

上传正文不经过 JSON/框架内存缓冲。执行者逐块计算 SHA-256，并同时执行声明大小、64 GiB 工件上限、owner epoch 和可续租 lease；连接中断、进程退出或续租失败使当前候选失去发布权，新 epoch 使用不同文件名并清除更老的同任务候选。错恢复口令不消耗 `UPLOADED` 状态，管理员可重新提交。服务端只保存口令 HMAC 和经 SecretVault 包装的数据密钥，不保存口令明文。

`IMPORTING` 在私有目录独立认证解密，复用恢复器的 ZIP、manifest、roster、成员和 cut 证明校验，但不创建或连接数据库。全部验证通过后才在一个发布事务中把密文原子移动到标准备份目录，并插入 `backup_jobs`、`backup_database_cuts`、`backup_artifacts`、事件和审计；此后下载、只读校验、隔离恢复和清理只走标准备份协议。外部 ID 必须是 canonical UUID。同一 ID 已存在时只允许 `READY`、未删除且密文与 manifest 摘要都完全一致的去重；任何差异固定失败为 `BACKUP_ID_CONFLICT`。发布前崩溃留下的目标文件没有账本权威，接管者按任务摘要收敛；失败路径只删除该导入任务精确拥有且未被标准 artifact 引用的文件。

执行者用 owner epoch 和 lease 领取任务，同一 deployment 同时只有一个活动恢复任务。独立续租任务覆盖解密和单个大库 COPY；续租失败会取消正在运行的数据库任务并回滚其当前事务。每个目标数据库在迁移、COPY、身份核对和封存期间持有 session advisory lock，取得锁后再次回部署库复核 owner epoch；陈旧 Worker 与接管者不能同时写同一个副本。每个目标状态提交也核对 job owner epoch 和未过期租约。租约过期时新执行者从 `VERIFYING` 重新核验工件；状态已经是 `STAGED` 的目标按不可变回执跳过，其他目标先清空后重做。数据库创建、文件解密和跨库 COPY 不持有部署库事务，目标状态与任务进度分别用受围栏的短事务提交。崩溃最坏留下带本任务 marker 的未注册隔离库，不能留下一个被目录误认成在线 Bot 的半完成目标。

单个目标严格按 `PLANNED → CREATING → CREATED → VALIDATED → STAGED` 推进；`VALIDATED` 重试可以回到 `CREATED`，任一非终态可以进入 `FAILED`。`CREATING` 在 DDL 前持久提交，数据库以 `ALLOW_CONNECTIONS=false` 创建；只有写入任务 marker 并撤销 `PUBLIC` 全部数据库权限后才允许恢复连接。这样 CREATE 与后续配置之间没有公开连接窗口，接管者也能区分本任务的未完成 DDL 和同名外部数据库。

接纳事务验证当前 admin 会话、恢复口令强度、范围和容量，生成稳定 `backup_id`、随机数据密钥、口令包装和 SecretVault 包装，保存输入摘要后提交。后台执行者通过 owner epoch/lease 领取；同一 deployment 同时最多一个 CAPTURING，每个 Bot 按稳定 `bot_id` 顺序捕获。独立续租任务覆盖单库 COPY、包导出和整包加密；续租失败取消主捕获任务。不可取消的线程 I/O 必须先收束，随后才能删除该 owner epoch 的临时目录。任务不持部署事务等待 Bot 数据库或长文件 I/O。

每库捕获步骤先读取身份和迁移账本，在只读快照内导出所有表并生成临时数据库 manifest；文件 fsync 完成后才提交 cut 回执。接管者使用新的 owner epoch 从头建立自己的临时工件，不能复用仍可能被旧线程写入的路径。最终 SEALING 对 ZIP 成员逐项摘要、流式加密到 owner epoch 专属候选文件。发布事务锁定 Job 并复核 owner/lease，执行原子 rename 与目录 fsync 后再次核对租约，再一并提交 cut、`backup_artifacts` 和 READY/INCONSISTENT 状态。每个 epoch 的最终候选名不同，陈旧线程即使迟到也不能覆盖当前权威工件；未进入 `backup_artifacts` 的候选没有下载权威，交给后续孤儿清理。日志与审计只记录已发布的标量。

遗忘、消息写入和备份读快照按 PostgreSQL 正常 MVCC 先后线性化。manifest 中的 deletion epoch 是该数据库 cut 的一部分。备份读取不会阻塞长事务之外的业务写，也不添加 retention pin；正在 scrub 的正文可能在该快照内仍有 body，但对应 header/suppression 已使其在恢复时不可读。捕获后发生的删除不在旧工件中，因此开放恢复前必须提供后续删除屏障。

容量默认限制：同时一个部署备份、最多四个数据库导出连接、单工件 64 GiB、单成员 16 GiB、最多 1024 个数据库和 100 万个表成员。空间预检只作早期拒绝，写入过程中仍逐块执行硬限制。失败任务保留有界元数据，临时正文立即回收；READY 工件按保留期限和显式删除计划回收。

## 4. Web 和 Shell 管理协议

WebUI 的“存储与备份”页提供：备份范围、是否包含插件工件、恢复口令、预计数据库数、创建按钮、进度、每库切口、最终大小、下载、外部文件上传与导入状态、只读校验、校验凭据状态、隔离恢复创建、每个随机目标库和失败码。口令输入使用 password 控件，不进入浏览器持久草稿；页面刷新只恢复任务标识和持久状态。校验接纳再次验证管理员当前密码和来源恢复口令，相同幂等键只能指向同一来源与口令摘要。只有当前有效凭据存在时页面才显示隔离恢复按钮；恢复请求只提交凭据 ID 和管理员当前密码。

当前已经实现创建、下载、外部导入、独立重新校验、整套隔离恢复，以及备份/隔离副本删除的 plan/apply；后续的取消及激活继续使用同一管理模式。创建备份、预留/完成导入、校验和隔离恢复要求当前密码复核、幂等键及部署 admin 权限；流式上传本身要求同一 admin 会话、Origin 与 CSRF。应用清理计划和重试阻塞任务也要求当前密码。下载会重新核对当前会话与工件状态，响应禁止缓存；查看者不具备备份、导入、校验、恢复或清理权限。页面没有激活按钮，`IMPORTED` 只表示外部密文已成为标准备份，`VERIFIED` 只证明某一精确工件通过只读检查，`STAGED` 只表示物理副本通过验证。

群聊 Shell 首版只提供 `backup status <id>` 和 `backup cancel <id>` 的远程救援入口，不接收恢复口令、不上传或下载工件。创建和恢复需要 Web/CLI 的重新认证通道。Shell 命令仍是 CONTROL 路由，不创建产品 Agent，也不占用群会话执行权。

## 5. 校验、恢复与切换

校验顺序固定为：容器/header 限额 → 口令解包 → AEAD 标签 → ZIP 路径和压缩比 → manifest schema → 每成员大小/摘要 → 数据库身份与迁移兼容 → 表/列/依赖 DAG → 删除屏障 → SecretVault key 与密文抽样解密 → 包工件摘要。任何失败只产生固定错误码，不能回显成员正文、DSN、用户名、表值或底层解密异常。

当前 `verify` 先按持久来源密文摘要重新读取本地工件，认证解密到任务私有临时目录，再要求 manifest 顶层、数据库、表、列、成员和包路径都是精确键集。数据库 roster 必须与接纳时冻结的 cut 一一对应，目录起止锚点必须相同；完成只发布有期限的验证事实和脱敏审计，不产生 PostgreSQL 目标。`restore-stage` 只接受与当前工件密文/清单摘要仍一致的有效回执，随后重复认证解密和清单检查，防止校验与使用之间发生替换。`INCONSISTENT`、过期、已删除或被替换的工件不能进入恢复。每个目标使用 `forest_restore_<job-prefix>_<ordinal>` 随机任务名，创建后写入数据库 owner marker 并撤销 `PUBLIC` 权限，且永远不写入 `bot_storage_plans`。

恢复器只运行当前镜像的迁移，不执行备份中的 SQL。它要求 server version、encoding、collation、迁移 checksum、表列、约束和序列定义精确相同，随后在一个目标库事务中关闭触发式约束、清空表、逐表 binary COPY、核对行数、恢复序列并显式扫描所有公共 schema 外键；任一步失败会回滚该库的数据事务。提交后再次核对部署/Bot 身份。Bot 副本在独立事务中提升 storage generation 和 maintenance epoch、撤销 host lease，并设置 `SEALED`、`data_admission=false`、`control_admission=false`。部署副本也保持未注册且不可被 Web 启动计划发现。全部目标成功后才原子发布 Job `STAGED` 与脱敏完成审计。

恢复分为三个不同操作：

1. `verify` 只读工件并生成有期限的验证回执。
2. `restore-stage` 当前把来源工件的全部数据库恢复到系统新建的未注册空库，使用当前迁移代码建立 schema，再按稳定表名执行 binary COPY；序列按捕获值恢复并扫描外键。全部目标保持网络隔离和 `SEALED/ISOLATED`。选择部分数据库和显式外部目标属于后续扩展。
3. `restore-activate` 在预览中逐项核对当前目录、Binding 账号归属、包工件、凭据、最新 deletion barrier 与效果账本。Bot 迁移使用旧库 SEALED、新库确认和 deployment storage generation CAS；部署控制面整库恢复由本机恢复命令重建 BootstrapState 后切换，不能由正在被替换的 Web 请求自我覆盖。

同 Bot 恢复不创建第二个 Bot 身份。目录切换前旧库保持权威；切换后新库才可 ACTIVE，旧库永久 SEALED 且运行凭据撤销。部分 Bot 恢复逐项保存结果，不声明全局原子成功。恢复包中已有待发送意图按原状态处理：确定未发送可由原 dispatcher 接续，SENT 不重发，UNKNOWN 保持人工对账；控制 outbox 不因新部署未见其消费者而改写状态。

删除屏障有三种合法来源：同一备份链中不早于 base 的增量删除日志、仍在线当前权威库导出的签名 suppression 集，或灾难恢复策略中明确保存的外部 WORM 删除日志。缺少任一来源时可以完成物理 staging 与取证查询，但禁止开放正常读取和派生。恢复器把屏障先于正文可见性应用，随后运行 scrub 和投影重建；不得先启用 Agent/Embedding 再补删除。

## 6. 备份保留与 WAL

逻辑工件不能清除 PostgreSQL 自身 WAL、基础备份或 Docker 卷快照。Forest 只管理自己创建且有完整 manifest/owner 的工件；外部 WAL/对象存储由 StorageProfile 插件声明能力、保留期限、加密和删除回执。未配置该能力时页面明确显示“无时间点恢复”，不能把逻辑导出宣传为 PITR。

备份默认保留 30 天，隔离恢复副本默认保留 7 天。到期扫描与显式操作都先在一个事务中建立清理 Job、把原资源推进到 `EXPIRED`、写事件和审计；备份同时设置不可逆 `deleted_at`。从该提交点起不再签发下载或恢复能力，物理删除可以稍后重试。

备份密文删除只接受 `backup_id.owner_epoch.forest-backup` 的直属普通文件，并在删除前复核权威 `relative_path`、密文 SHA-256 和字节数。隔离数据库删除逐个复核固定随机名称、`forest:restore-stage:<restore_id>:<content_digest>` owner marker、非模板属性，以及它没有出现在 `bot_storage_plans`；缺失目标按已删除收敛，任一身份冲突转为 `BLOCKED`。因此崩溃可以留下已失效但尚未物理回收的资源，不能把不明数据库误删成成功。

未进入 `backup_artifacts` 的 epoch 候选和 `.job-<backup_id>-<epoch>` 临时目录没有下载权威。孤儿扫描只清理超过两小时宽限、存在对应 Job 且路径 epoch 不大于 Job owner epoch、未被 artifact 引用、也不属于有效活动租约的直属普通路径；拒绝符号链接、reparse point 和特殊文件，并保存不可变孤儿清理回执。普通磁盘无法证明物理不可恢复，界面只报告 Forest 托管文件或数据库已删除。备份中可能含已删除前的用户正文，因此恢复激活仍受删除屏障约束。

## 7. 验收

- 在持续接收两个群消息时创建全部署备份；每个 Bot cut 自洽，群作者、触发切口、Agent checkpoint 和多主干来源均可复核。
- 备份期间来源遗忘与正文 scrub 并发；恢复 staging 后该来源仍不可读，派生任务不能复活正文。
- 在任一表 COPY、Bot 连接、fsync、加密、rename 和最终回执前杀进程；恢复器只接续同一 Job，不发布半个工件。
- 错口令、篡改 header/tag/member、ZIP 路径穿越、压缩炸弹、未知迁移、列类型变化和额外数据库均固定拒绝。
- 浏览器断线、账号禁用和任务领取过期按持久权限边界处理；已经封口的密文事实保留，新的下载 capability 不再签发。
- 普通 API、审计、日志、截图和下载文件名不出现 DSN、数据库密码、SCRAM verifier、主密钥、恢复口令或正文片段。
- staging 不启动 Adapter、Agent、Job、发送或 Embedding；激活缺少当前删除屏障时明确失败。
- Compose 中可从全新部署创建加密备份、下载、独立校验并恢复到隔离数据库；重启后任务和下载元数据可恢复。
- 下载工件可在另一部署按精确大小流式导入；错口令保持可重试，传输中断或 lease 丢失不发布备份，同 ID 不同内容被拒绝，成功工件继续走标准校验/恢复链路。
- 清理计划应用后即使 Worker 尚未执行，旧下载和恢复入口也已失效；崩溃后继续同一 Job，marker 或目录登记不符时保留数据库并显示 `BLOCKED`。
