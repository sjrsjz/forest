# P5 部署控制面离线恢复

状态：详细设计；本切片实现宿主数据目录仍可用时，将部署控制面切换到 Forest 已隔离恢复的部署数据库。宿主数据目录与主密钥同时丢失的裸机恢复需要后续增加部署身份恢复胶囊。

## 1. 问题与边界

部署 PostgreSQL 保存账号、会话、Bot 目录、加密存储凭据、备份和恢复账本。正在使用该数据库的 Web 请求不能可靠地替换自己的权威存储：连接池、后台任务和 Bot Fleet 可能继续从旧库取得工作，HTTP 成功也无法证明进程已经退出。因此部署库切换只允许本机 CLI 在 Forest 服务完全停止时执行，不提供 Web 或群聊 Shell 写入口。

本切片要求以下材料同时存在：

- 原宿主数据目录及其中的 `master.key`、`bootstrap.sqlite3`；
- 状态为 `COMPLETE` 且完整性校验通过的原始初始化计划；
- Forest 加密备份、恢复口令文件，以及由该备份建立的部署数据库暂存副本；
- 指向暂存副本的 PostgreSQL URI 文件。URI 和恢复口令均不允许出现在命令行参数、JSON 输出或异常文本中。

保留本地数据目录使恢复过程能够沿用原 `deployment_id`、`setup_id`、数据库初始化 `plan_digest` 和 SecretVault 主密钥。Bot 数据库中已经登记的 Web issuer、持久 Job 委托和部署库中的密文因此仍能被同一签发身份解释。只凭恢复库生成一个新的 deployment ID 会割裂这些授权事实，所以不接受这种做法。

本切片不承诺 PostgreSQL 时间点恢复，不开放恢复后的 Bot 数据，也不替代每个 Bot 的删除屏障和恢复激活协议。备份是逐库一致切片；部署库切换完成后，各 Bot 仍按目录中原有的有效存储代际启动。

## 2. 停服互斥

`forest serve` 启动前必须取得 `<data-dir>/deployment.lock` 的进程级排他锁，并持有到 Uvicorn 退出。第二个服务进程和离线恢复命令都使用同一把锁，获取失败固定返回 `DEPLOYMENT_ALREADY_RUNNING`。锁文件是受保护的普通文件，符号链接、硬链接和 reparse point 均按现有安全路径规则拒绝。

恢复命令持锁覆盖归档校验、目标库校验和本地指针提交。锁只证明使用同一宿主数据目录的 Forest 进程已经停止；复制数据目录到另一台主机后同时运行属于首版单节点部署契约之外的运维错误。数据库端仍会在最终校验事务中对所有 manifest 表取得 `SHARE` 锁，阻止切换窗口内的普通写入。

`setup-token` 和纯插件开发命令不取得部署运行锁；它们不连接或切换已完成的部署权威。

## 3. 两阶段命令

命令固定为：

```text
forest deployment-recovery plan \
  --data-dir /data \
  --artifact /recovery/backup.forest-backup \
  --passphrase-file /run/secrets/recovery-passphrase \
  --target-dsn-file /run/secrets/recovered-deployment-dsn

forest deployment-recovery apply \
  --data-dir /data \
  --recovery-id <plan 输出> \
  --plan-digest <plan 输出> \
  --passphrase-file /run/secrets/recovery-passphrase
```

`plan` 先完成全部只读校验，再在本地 SQLite 写入不可变、SecretVault 加密的恢复计划。公开输出只包含 recovery ID、计划摘要、备份 ID、归档/manifest/部署切片摘要、目标 host/port/dbname/user 和当前恢复 head revision。目标 URI、口令、主密钥、表内容和账号正文不输出。

Compose 部署可以用 `--target-database forest_restore_...` 代替 `--target-dsn-file`；CLI 从现有只读 secret mount 取得托管 PostgreSQL 凭据，不输出密码。该选项只接受恢复器生成的数据库名前缀。原生或外部 PostgreSQL 使用 URI 文件。

`apply` 不接受新的 URI；它从加密计划读取冻结目标和归档路径，要求调用者回传精确 recovery ID 与计划摘要，并重新读取口令文件。它重新认证整个归档、重新计算目标库内容，在持有目标表锁时用 SQLite `BEGIN IMMEDIATE` 对 recovery head 做 CAS。重复应用同一个已经成为 head 的计划返回原结果；同一 base revision 的其他计划固定返回 `RECOVERY_PLAN_STALE`。

恢复口令文件和 DSN 文件必须是有界的普通非链接文件。口令去掉一个末尾换行后仍需满足备份口令规则；DSN 使用初始化相同的单 host PostgreSQL URI 规范化。文件内容从不拼接到 shell 命令。

## 4. 工件与目标校验

归档校验依次执行：

1. 认证解开数据密钥并流式 AES-GCM 解密到数据目录中的私有临时文件；
2. 校验 ZIP 条目数量、路径、压缩方式、大小和每个成员 SHA-256；
3. 要求顶层精确格式、`ISOLATED_RESTORE_REQUIRED` 激活标志、一致的 roster 和唯一部署数据库切片；
4. 复算 manifest SHA-256、部署切片 `content_digest`、schema-bundle roster digest 和 identity digest；
5. 核对已验证归档中的 `recovery/master.key` 摘要与当前宿主主密钥一致；
6. 冻结密文 SHA-256、备份 ID、manifest digest、部署内容摘要与预期 restore marker。

目标 PostgreSQL 在 `REPEATABLE READ` 事务中校验：

- 实际连接的 host/dbname/user 与冻结 locator 一致，数据库注释精确为 `forest:restore-stage:<restore_id>:<content_digest>`；
- server version、encoding、collation、ctype、内容寻址 schema-bundle roster 和 checksum 与 manifest 精确相同；
- 公共表、列顺序、类型、not-null、identity/generated 属性、约束和序列定义精确相同；
- 每张业务表重新执行 binary `COPY TO STDOUT`，字节摘要与备份成员相同，行数相同；序列当前值与 manifest 相同；
- `web_setup_state` 的 `setup_id + plan_digest` 与原本地初始化计划及备份 identity 三方相同。

因此一个仅复制行数、手工添加 marker 或指向其他部署身份的库不能被切换。校验不运行迁移、不修改恢复库，也不把备份内 SQL 当作代码执行。

## 5. 本地权威状态

原 `bootstrap_state` 保持不变，继续保存最初的部署身份和数据库初始化证明。新增本地表：

- `deployment_recovery_head(singleton, revision, recovery_id)`：唯一有效恢复指针；
- `deployment_recovery_plans`：恢复 ID、base revision、前后目标身份、归档/manifest/内容摘要、marker、加密计划、状态和时间。

恢复计划摘要覆盖 base revision、原初始化 plan digest、前后目标身份、目标 URI 摘要、归档绝对路径及摘要、备份/manifest/内容摘要和 marker。完整计划再次由 SecretVault 认证加密。读取有效启动计划时先验证原 bootstrap，再验证 head、计划摘要和行列冗余字段，最后只替换返回值中的 `dsn` 与 `target_identity`；`deployment_id`、`setup_id` 和数据库 `plan_digest` 始终来自原初始化计划。

应用事务只做两件事：把 PLANNED 计划改为 APPLIED，并将 head 从预期 revision 推进一位。SQLite 的 FULL synchronous 和单事务提交保证崩溃前看到旧指针、崩溃后看到新指针，不存在半条 DSN。旧 URI 保存在加密计划链中，旧数据库本身不删除、不改 marker；“封存”在本切片表示它不再是该数据目录的有效 head。物理禁止旧库连接需要数据库管理员权限且无法与 SQLite 跨域原子提交，不能伪装成已完成。

## 6. 启动与失败恢复

`ManagementRuntime.restore` 无需新增旁路。`BootstrapState.load_plan()` 返回当前有效目标后，启动仍先连接该库，核对原 `setup_id + plan_digest`，再运行当前迁移、恢复 WebAuthority 和 Bot Fleet。目标不可达或身份漂移时服务保持未就绪并按现有恢复循环重试，不会初始化空库。

| 失败点 | 可见结果与处理 |
|---|---|
| 无法取得部署锁 | 不读取秘密、不连接目标，停止并要求先关闭同 data-dir 的服务 |
| 归档、口令或 manifest 无效 | 不写恢复计划；删除临时明文 |
| `plan` 目标校验失败 | 本地 head 不变，恢复库继续隔离 |
| `plan` 成功后目标被改写 | `apply` 的完整复验失败，head 不变 |
| `apply` 在 SQLite 提交前崩溃 | 原 head 仍有效；同一计划可重试 |
| `apply` 提交后输出丢失 | 同 recovery ID/digest 重试得到 APPLIED；启动使用新目标 |
| 新目标随后不可达 | 服务保持未就绪；修复目标后自动恢复。需要退回旧库时必须基于当前 head 创建并验证新的恢复计划 |

切换不是“删除 bootstrap 文件再初始化”。恢复计划链、归档证据和目标数据库身份共同构成审计依据；任何完整性不明的状态都应停止启动，而不是猜测一个可用 DSN。

## 7. 后续边界

裸机恢复还需要在加密备份中加入独立的部署身份恢复胶囊，并定义安全安装 `master.key`、重建本地 bootstrap、证明旧宿主已停机的协议。PostgreSQL base backup/WAL、跨主机 fencing 和对象存储保留策略仍属于 StorageProfile/PITR 扩展。
