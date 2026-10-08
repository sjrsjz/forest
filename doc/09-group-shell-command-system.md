# 群聊 Shell、管理命令与优先级详细设计

## 1. 目标与边界

管理员可以在群聊中查询、维护和修复 Forest，不要求身处服务器旁。群聊 Shell、WebUI 和 CLI 调用同一组类型化 management contracts；Shell 只提供适合短消息的入口和 CONTROL 回执，不成为另一套业务实现。

管理命令由确定性解析器执行，不经过 LLM，不等待当前群 Agent 回答完毕，也不创建第二条 Agent 执行链。命令消息仍以原 `ingress_seq`、作者和群来源进入安全历史投影，但 `CONTROL_COMMAND` 不产生 conversation trigger，不扩大正在运行的 Agent 输入切口。

Shell 不是任意系统终端。默认保留前缀为 `#forest`；内核只实现很小的 `core` 救援集合，其余命令由当前模块 Generation 注册。原生进程只能通过预先批准的执行 profile 或不可变 runbook 运行，群消息不能提供任意可执行文件、工作目录、环境或重定向。

## 2. 接入顺序

```text
可信 Adapter 帧与有界平台解析
  -> Core ShellGate 前缀与接入容量判断
  -> Inbox/作者/来源/ingress_seq 持久接纳
  -> shell.command 复核作者策略并冻结 management route
  -> CONTROL durable request 或普通分类工作
  -> P0/P1 Control Scheduler 或 P2/P3/P4 Conversation Scheduler
  -> 类型化 operation / 模块 management contract
  -> CONTROL Delivery 回原会话
```

ShellGate 位于稳定内核边界，只做有界本地工作：确认完整前缀 token、已认证 Binding/principal、输入大小、Core grammar 形状和接入容量，并形成不可持久复用的 ingress proof。它不解释管理员名单、不拥有 Shell 策略或命令请求，也不载入模块 Python、不联网、不访问 LLM。`shell.command` 在同一 UnitOfWork 中用自己的当前策略与 RouteGraph 复核 proof，写自己的接纳事实；`conversation.scheduler` 同时写 Inbox。任一 participant 失败时两边都不提交。

一条消息在接纳事务中冻结：

- `bot_id/binding_id/conversation_id/event_id/ingress_seq`；
- Adapter 已认证的原作者 principal，以及 `shell.command` 当前管理员策略修订与资格结果；
- `graph_digest`、命令 owner component/generation、operation ID 和 schema digest；
- 规范化参数载荷引用、service class、目标作用域与所需能力；
- idempotency key、接纳时限与输出目标。

已认领的 `#forest` 消息无论语法错误、权限不足、模块故障或超时，都以 Shell 结果终结，不能 fallback 到 Agent 或普通插件。未命中保留前缀的消息才进入业务命令和普通消息 Handler 分类。

带完整 `#forest` 前缀的消息即使尾部混入图片、额外 @ 或其他非文本片段，或前缀后直接换行，也由 Shell 接管并返回 `SHELL_SYNTAX_INVALID`；不得把它重新送入普通 Agent 触发链。只有整个命令区均为直接文本片段、且前缀后使用空格或制表符分隔时才解析和执行。

## 3. Core Gate 与模块命令

内核保留以下救援语义：

```text
#forest core status
#forest core cancel <operation-or-run-id> --expect-revision <n>
#forest core disable --expect-maintenance <n>
#forest core reload --expect-graph <graph-revision>
```

`status/cancel/disable` 可以声明 P0，`reload` 为 P1。`core.status/pause/resume/reload` 由 `component.worker-host` 包贡献，包内 Worker 通过受限 `kernel.runtime` 服务端口查询状态、以 maintenance epoch CAS 更改 DATA admission，或以 graph revision CAS 提交整图 replacement request；内核负责 HostLease 围栏与状态写入，不解析 Shell 命令。单组件 reload 由同包的 `module.reload` 生命周期动作提供，Shell 不把整图换代假装成局部替换。命令只写持久 replacement request 并返回，宿主监督循环在该 Shell operation 完成后领取请求，避免执行者同步排空自身造成死锁。`disable` 关闭当前 Bot 的 data admission 并保留 CONTROL，不等于卸载某个模块；模块停用使用模块自己的 lifecycle action。只有接纳时命中 `shell.command` 对当前 Binding/群生效的管理员策略，并通过 `kernel.authorization` 精确能力检查的主体才能保留 P0；否则命令拒绝或降为普通 P1 查询，不能仅凭文本或平台群角色取得保留槽。执行和回执前再次查询当前授权、Binding epoch、Host lease、目标 revision 和资源所有权。

Core Gate 只规范化到既有 operation、Conversation Run 和 module lifecycle 权威。它不复制 Agent/Memory/Scheduler 状态机，不直接停止线程或改数据库行。正常模块不可用时，Core 命令仍能撤下路由、请求取消或读取最小健康信息。

其他命令由模块描述符注册：

```text
RuntimeShellCommandContribution {
  command_id, command_version,
  service_id, service_version,
  argument_schema, management_operation_id,
  required_capabilities, target_scope,
  service_class, option_aliases, output_policy, timeout
}
```

示例：Agent 模块注册 `run list/inspect/cancel`，Memory 模块注册 `memory sources/query/forget-*`，Conversation 模块注册 `context inspect/compact/reset`，Job 模块注册 `jobs list/run/cancel`。控制台和 Shell 都从同一 management operation catalog 发现这些动作；控制台不会因此出现硬编码 Agent/Memory 顶层页面。

当前 `memory.graph` 已作为首个非内核命令贡献者提供 `memory trunks`、`memory sources` 和 `memory relations`。群聊输出只给最多十项的短投影；来源项明确标注作者、模态和来源群，关系项明确标注证据来自哪些群。需要稳定游标、组合筛选或展开完整证据时，回执引导管理员进入同一模块的 WebUI 查询面，不能在群里倾倒大页数据。

当前 `job.scheduler` 提供 `jobs list/inspect/run/cancel`，但不拥有具体任务语义。执行模块通过 `RuntimeJobTypeContribution` 声明稳定任务类型、版本、输入 schema、服务、超时、最大尝试和并发上限；Scheduler 在 claim 时冻结准确 provider Generation，并以持久 lease/token 拒绝旧 Worker 的迟到结果。热替换先排空旧 Scheduler 已领取任务，新 Generation 才继续领取排队任务。`jobs cancel` 使用调用者看到的 job revision：排队任务直接终结，运行中任务只登记可观察的取消意图，由执行器在安全点响应；已经产生的真实结果不能被改写成“从未执行”。首个内置类型为只读 `runtime.health-snapshot`。

官方 `process.executor` 是普通执行模块，通过同一目录贡献 `native.execute`，不向 Shell 暴露命令行解释器。其 Generation 配置保存部署方预登记的 ExecutionProfile：固定可执行文件绝对路径与 SHA-256、参数 JSON Schema 到 argv 的逐项映射、固定工作目录与环境、执行身份、网络策略及资源上限。调用方只能给出 profile ID 和 schema 参数；路径、环境和 argv 结构不能随请求覆盖。执行器启动前复验常规文件、权限和摘要，使用无 shell 的 `execve`；请求取消或墙钟超时时对整个进程组先 TERM 后 KILL。stdout/stderr 持续排水并按 profile 上限保存为 `kernel.payload`，群回执只展示任务状态和 artifact 引用。Linux 的网络拒绝策略使用 seccomp；平台不支持请求的隔离时拒绝激活该配置，绝不回退为不受控执行。

`audit.console` builtin Worker 包贡献并执行 `audit.tail`；它经 `audit.page.snapshot` 受限服务端口读取当前 `kernel.audit` Generation 的审计页，不持有审计表或宿主处理器。命令允许按 `action`、`producer_component_id`、`origin_kind`、资源二元组、操作二元组和 `before` 游标筛选，单页最多十项。回执只投影时间、级别、动作、生产者、主体、资源、操作和稳定 audit ID，不读取详情载荷正文。Shell 结果、CONTROL SendIntent 与对应不可变审计 envelope 在同一 UnitOfWork 中提交，任一参与者失败时整体回滚。

`RuntimeModuleDescriptor.shell_commands` 是唯一命令目录。命令词由 `command_id` 的点分段确定，参数名和类型来自完整 Draft 2020-12 schema，CLI 风格别名只能显式映射到 schema 中已声明的属性。RuntimeGraph 构建时拒绝重复 `(command_id, command_version)`；Contract Router 只向 `shell.command` 暴露贡献目录，并为每项解析由贡献模块准确 Generation 提供的类型化服务句柄。Shell 不按命令 ID 导入模块，也不含模块命令分支。

模块命令 registry 是 RouteGraph 的一部分。发布事务把命令 schema 摘要、能力摘要、operation、服务等级和准确 owner Generation 写入权威 route。新 Generation 发布时一次切换，之后接纳的命令使用新 schema；已接纳请求继续绑定原 Generation 和参数载荷，执行时同时复核冻结 owner 和服务句柄 provider，除非该模块明确声明、宿主验证且管理员允许兼容 takeover。Forest 未发布基线不保留开发期 `shell-gate-v2…v12`、版本专用 Python registry 或旧 writer-generation 分派器。

### 3.1 Shell 与 Web 的同一控制动作

`module.enable/disable` 是 `component.worker-host` 的管理 action，Shell 命令只能把经过解析的参数交给该 action；Shell 服务不直接调用选择写端口，也不复制选择账本 SQL。控制入口使用同一不可变 action 声明、输入 schema、actor 字段、operation ID、计划摘要和 `runtime.selection-commit` 事务预检。两种入口的凭据不同：Web 使用 Web authority ticket；Shell 使用当前 `shell.command` 已授权的执行 claim。不能把 Shell principal 填进 Web ticket 字段，或把 `management_action` 写端口降成普通 Domain Command 来迁就 Shell。

Shell claim 由 Shell 组件提供事务内验证契约，证明 `bot_id`、原群、`request_id`、`claim_token`、principal、命令及版本、冻结的目标 component/Generation、已消费的 `kernel.authorization` ticket、有效 lease 和当前管理员资格。Shell Worker 给控制入口的是这个 claim 的引用与规范化 action 输入，不是数据库句柄或私表名。控制入口从已冻结的 Shell 路由与 `management_operation_id` 核对 action，不能由 Worker 任意指定另一个管理操作；action 的 `requested_by` 始终由验证后的 principal 注入，群文本不能覆盖。

提交顺序为：取得 RuntimeGraph 操作锁 → 在目标 Bot 数据库开启事务 → 经 Shell 包验证 claim（Web 则验 Web ticket、Bot policy 与审计）→ 运行同一个 `runtime.selection-commit` 预检，按组件 ID 顺序锁定选择/配置头并核对准确 action 输入、目标 Generation、选择修订、Graph 修订、配置修订与计划摘要 → 调用包内 `management_action` 写端口 → 同事务记录来源明确的审计与操作回执。Shell 的管理员资格与 lease 必须在这笔事务里重验；预检不得只在 Shell 接纳时进行。任一步失败整笔回滚，不能出现“Shell 已获准、选择账本却采用过期图”的部分提交。

Shell 回执只报告持久 `request_id` 和状态；Graph 替换仍由 Worker Host 的独立任务提出并由宿主执行，不在群命令栈内同步排空自身。准确重放沿用原 operation ID 与输入摘要；同一 ID 改输入冲突。Web 与 Shell 对同一选择修订竞争时，最多一笔提交成功。命令执行结果未知时按 operation ID 查询，而不是重新生成选择请求。Shell claim 过期、管理员撤销、路由换代或计划变更都拒绝新写入，已经提交的请求保持真实状态。验证器属于 `shell.command` 包边界，内核只编排版本化验证契约与事务，不读取 Shell 私表。

当前实现中，`component.worker-host` 贡献 `module.validate/enable/disable`，Shell 包解析命令并把 claim token 及其自身 Generation 送至冻结的目录路由。启停命令的 manifest 显式标记 `management_action`，通用包适配器据此直接调用同一管理 action，而不调用一个拥有选择写权限的 Shell 服务；未标记的命令不能获得这条入口。`shell.command` 的私有 UnitOfWork 验证器在 action 写事务内检查 claim、已消费的授权 ticket、租约、发言者与群、原命令/参数、目标服务及 Generation，并复核当前管理员资格。目标 Graph 控制准入、选择计划预检、Worker Host 选择写入和来源明确的审计在同一事务完成。Shell `module.validate` 只返回短预览，启停命令只返回持久请求 ID；完整事实仍由模块管理查询查看。

命令贡献还会形成生命周期依赖：`shell.command` 必须在所有命令提供者之后启动，并在它们之前停止接纳和排空；Agent 工具目录同理使 `agent.runtime` 依赖工具提供者。该依赖进入 Generation 描述事实与拓扑排序，贡献者反向依赖 Shell 时会作为环在图构建期拒绝。这样旧图热替换时，Shell 已领取的命令不会遇到先被 quiesce 的提供者。

## 4. 解析规则

只允许去除 Adapter 已验证的开头 Bot mention 和前导空白，再匹配大小写敏感的完整 `#forest` token。引用、转发、代码块、图片 OCR、LLM 输出和附件说明不能成为可执行入口；`#forestX` 不是前缀。

解析器支持空白分词、单双引号、`--key value` 与 `--key=value`。布尔参数可用 `--flag` 或 `--flag=true|false`。单引号逐字保留；双引号只解码 `\\` 与 `\"`。未闭合引号、重复单值选项、未知选项、位置参数和超长字段均拒绝。首版不提供 `--` 结束选项，因为命令协议没有自由位置参数。

`;`、`|`、`&&`、重定向、`$VAR`、反引号和 `$()` 没有 shell 语义，不展开也不连接命令。跨消息续行、交互 stdin 和从聊天上传任意脚本不属于协议。帮助文本由实际参数 schema 生成，不能维护另一套手写 grammar。

参数在写入请求前转成规范类型；Worker 不重新解析原消息。敏感参数进入 Shell 模块拥有的受管 payload，不复制到 Context、audit、operation metadata 或回执。正文与来源规则见[14 干净数据库基线](14-clean-schema-and-payload-provenance.md)。

## 5. 服务等级与资源优先级

| 等级 | 用途 | 约束 |
|---|---|---|
| P0 | 取消、拒绝、紧急 disable、审批/拒绝、最小健康检查 | 独立小容量；不可运行长任务或任意模块代码 |
| P1 | 配置、查询、reload、context/memory 管理 | 有界控制池；不能抢占已发生外部效果 |
| P2 | 确定性业务命令与普通消息 Handler | 按 Conversation 顺序与组件预算执行 |
| P3 | Agent 模型/工具推进 | 同群唯一执行链，不同群并发 |
| P4 | 索引、图构建、备份、scrub 等后台工作 | 可暂停、限速、租约化 |

优先级只决定可运行工作领取顺序，不改变历史顺序、权限、锁序或目标资源所有权。P0 可以登记取消意图，不能在另一个事务持锁或网络调用中间抢夺对象。最终效果仍由目标模块在安全点检查 cancellation/revision/fence。

Agent Run 取消采用持久两阶段协议。`run cancel` 只以 `run_id + expected_checkpoint_seq` 向 `agent.runtime` 登记不可变取消意图；并发请求由 Run 行锁和唯一目标约束确定唯一胜者，旧 checkpoint 请求直接冲突。Agent 在模型或工具交接事务内再次检查该意图：若取消先提交，则不得把新外部调用推进到 `DISPATCHING`；若外部交接先提交，则保留真实结果并在观察结果后的下一个安全边界取消，不能伪称请求从未发生。`MODEL_PENDING`、已 `DISPATCHING` 的工具以及 `BLOCKED_UNKNOWN` 不能直接改写为已取消。Agent 写入 `CANCELLED` checkpoint 后，Conversation Worker 以独立幂等终态操作把该 Run 已领取的 trigger 标记为 `CANCELLED` 并释放 lane；两步间崩溃时，恢复 Worker 从持久 Agent 终态继续完成 Conversation 收敛。

每个 Bot、Binding、主体和命令定义都有待处理上限与令牌桶；P0、普通 CONTROL 和拒绝回执使用相互隔离的极小容量，防止普通查询耗尽救援入口。超限得到固定、脱敏、可去重结果，不转给 Agent。资格缓存只用于接入保护，最终授权始终读权威状态。

## 6. 持久请求状态机

```text
RECEIVED -> QUALIFIED -> QUEUED -> CLAIMED -> EXECUTING
                                      -> WAITING_APPROVAL
           -> REJECTED               -> SUCCEEDED
                                      -> FAILED
                                      -> CANCELLED
                                      -> UNKNOWN
```

请求只允许一个有效 claim，带 owner、lease epoch 和截止时间。宿主崩溃后，新 owner 根据 operation receipt 恢复；已接纳的外部效果不能因 ACK 丢失而换 operation 重放。目标命令调用返回 `CALL_TIMEOUT`、`SERVICE_OPERATION_IN_PROGRESS` 或 `SERVICE_OPERATION_UNKNOWN` 时，Shell 只能登记并回执 `UNKNOWN`，不能把可能已经发生的效果报告为 `FAILED`。UNKNOWN 必须经查询或人工 resolution 收敛。

执行者从冻结 RouteGraph 解析原 Generation，验证该代仍可完成已入场工作，再调用准确 management contract。新路由发布不会重新解释旧参数；旧代撤下后若不能安全完成，请求以明确 `GENERATION_RETIRED` 终结，不能偷偷交给不兼容的新 handler。

业务结果、operation、audit 和 CONTROL SendIntent 按 participant contract 在同一短事务提交。网络发送由 Delivery 异步完成。群回执只含允许公开的状态、稳定 ID、必要 revision 和下一步；异常堆栈、密钥、命令正文、其他群身份和任意载荷不进入群。

## 7. 授权与作用域

`#forest` 本身不授予权限。Shell 使用可信 Adapter 身份建立 principal，按命令的目标作用域检查：

- `conversation`：固定为当前会话，不能提交其他群 ID；
- `binding`：固定为当前账号绑定；
- `bot`：允许管理同一 Bot 的全局事实，但输出仍回原会话并按可见性脱敏；
- `deployment`：默认不从群聊开放，只有显式授予、强认证和双确认的少量操作可用。

跨 Bot 操作必须提供稳定 bot ID 并拥有目标权限；不按昵称模糊匹配，不广播。模块 capability 声明只是请求，实际权限是 package trust、deployment grant、当前主体、目标 scope、policy revision 和具体 operation 的交集。

权限在接纳、claim、调用 management handler、写入结果和外发回执前分别复验。接纳后的授权收缩可以阻止尚未开始的副作用；已经发生的外部效果仍保留真实结果。批准操作绑定准确 request/plan digest、主体、期限和一次性 nonce，不能批准一个可变查询。

## 8. 与群聊 Agent 和 Context 的关系

CONTROL 请求由独立调度器执行，不占用 Conversation Agent lane。命令的安全历史投影仍按原 `ingress_seq` 由 Conversation Actor 合并，保留作者和来源，但没有敏感参数。

`context compact/reset` 只能登记带 `expected_context_view_revision` 的请求。Conversation Actor 在 IDLE 或下一次模型调用安全边界应用：

- `compact` 按确定性 token 预算归档/选择，不调用摘要模型；
- `reset` 发布新的不可变视图，保留活动 Run 已接入的完整 InputBatch 和未闭合模型/工具因果组；
- 两者都不删除 Inbox、触发、长期记忆或回退输入游标；
- 需要终止活动模型/工具时先调用 `run cancel`，不能让 P1 Context 命令隐含取得 P0 取消权。

Agent 工作期间到达的新普通消息照常入队。Shell 执行不会把它们插入当前 ModelTurn，也不会越过连续分类水位。下一次模型调用仍只推进到最后一个待触发记录；Agent 结束且没有新触发时，普通尾部静默排空。

## 9. 业务命令与普通消息策略

管理 Shell、业务命令和普通 Handler 是三类路由：

1. Core/management 前缀优先认领 CONTROL；
2. 已启用的业务命令按冻结 RouteGraph 选择唯一 owner，执行其参数 schema、策略和 P2 handler；
3. 未认领消息进入普通 Handler 链，再按冻结 trigger decision 回落 Agent 或静默 Context。

业务命令策略绑定 `(bot_id, binding/group scope, command_id, component_generation, principal_class)`，默认禁用。命中后的拒绝或失败不能 fallback Agent 以绕过策略。多个 handler 的 priority 只在同一命令 owner 内排序，不能覆盖 Core 前缀或提高服务等级。完整顺序以本节、[03 的冻结路由](03-communication-scheduling.md#3-路由与执行)和[08 的会话接纳](08-group-context-interaction.md)为准。

## 10. 受控原生进程

首版只允许部署者预注册的 `ExecutionProfile`：固定 executable digest/路径、允许的 argv schema、工作目录根、环境白名单、身份、CPU/内存/时间/输出上限和网络策略。群消息选择 profile 和 schema 参数，不能指定程序路径或添加环境变量。

任务由受监督 Job Worker 在隔离执行环境中运行。stdout/stderr 分块进入受限 artifact，群回执只给摘要和 artifact ID；敏感输出默认仅 WebUI 按权限读取。取消先协作终止，再按期限升级到进程组终止。无法提供声明隔离级别时拒绝 profile，而不是在宿主 shell 中退化执行。

## 11. WebUI 与模块管理 UI

WebUI 的模块页从描述符展示命令目录、参数 schema、权限和最近请求。模块自带 UI 可以生成草稿或调用已声明 management operation，但不能把 URL、HTTP 方法或权限名传给宿主。群 Shell、WebUI 和 CLI 使用相同 plan/apply、revision、idempotency 和 audit 规则；它们只在主体与输出通道上不同。

浏览器断开不取消已接纳请求。页面显示 desired/effective/observed 和 operation phase，不把 HTTP 202 或 Shell“已排队”回执展示为生命周期完成。

## 12. 验收

- 忙碌 Agent、普通 Handler 故障和模型容量耗尽时，受权 Core P0 仍能有界接纳和执行；
- 未授权高优先级文本不能占用 P0，权限在执行和外发前收缩会阻止新效果；
- 同一群的 CONTROL 与 CHAT 分开发送 lane，控制优先不改变 Context 的 ingress 顺序；
- 热重载期间已接纳命令绑定原 Generation，新命令绑定新 Generation，不出现混代参数或重复 operation；
- 两个管理员从 WebUI/Shell 基于同一 revision 竞争，最多一个 apply 成功；
- `context compact/reset` 只在 Actor 安全点应用，无摘要 API 调用，不丢未答触发和作者；
- 业务命令拒绝不 fallback Agent，普通 Handler 全部 Pass 才采用冻结 trigger decision；
- 原生执行 profile 的路径、环境、资源、输出和取消边界均有实际隔离测试；
- 源码、数据库和测试不包含开发期 gate registry 兼容链或 writer-generation 升级夹具。

## 13. 健康 Job 的组件归属

`runtime.health-snapshot` 是由 `component.worker-host` builtin 包声明、执行和热替换的只读 JobType，不再由 `kernel.runtime` 直接注册 Job handler。内核只拥有 `runtime.status.snapshot/1` 只读端口，返回当前 Graph、接纳状态与成员生命周期的有界状态投影；包通过冻结的服务路由消费该端口，并发布 `forest.runtime-health-snapshot/1` 结果。JobScheduler 仍只拥有排队、领取、取消和结算，不知道健康检查字段。

取消意图在 Scheduler 派发前以及目标包 Job handler 入队前复核；已开始的只读状态读取可完成，不能把已产生的结果重写成“未执行”。换代时 JobType 的 provider Generation 随 `component.worker-host` 一起冻结或替换，旧队列项必须按原 Generation 身份结算或获得明确失败，不能临时改路由到新包。
