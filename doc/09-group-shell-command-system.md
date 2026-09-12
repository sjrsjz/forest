# 群聊 Shell、管理命令与优先级详细设计

> 状态：待实现的框架契约。本文命令、DTO 和配置名称为拟定接口，当前仓库不提供可执行实现。

## 1. 目标与边界

管理员可以通过群消息查询、维护和修复 Forest，也可以处理运行任务、上下文与记忆；不需要守在服务器旁边。群聊 Shell 与首版必需的 [WebUI 后台](12-webui-administration.md)、CLI/API 共同调用同一组类型化管理服务。WebUI 提供日常表单配置、差异预览和操作中心，Shell 保留群内快速维护入口。**管理命令由确定性解析器执行，不经过 LLM，不等待当前群 Agent 回答完毕。**

默认入口为 `#forest`，保留 `#forest core` 作为稳定内核救援命令。框架 Shell 支持子命令、引号、选项、帮助、持久 Job 与受控原生进程扩展；业务插件可以增加自己的命令。它不改变[08](08-group-context-interaction.md)的单群 Agent 独占与输入切口规则。

三件事分别建模：解析时谁拥有命令；调度时哪个服务等级先取得执行槽；相交资源最终按什么事务顺序提交。命中高优先级命令不会自动提升权限，也不能撤销已经发生的网络发送或其他外部效果。

WebUI 不把点击转成伪造群消息或拼接 `#forest` 字符串：它以自己的可信主体和 `management_request_id` 提交相同 typed action，Shell 保留真实 platform event、`command_id` 和来源会话。两者在目标库关联同类 `operation_ref={kind,id}`，共同遵守当前授权、配置 CAS、维护/代际 fence、Job/生命周期/效果与批准账本；页面、HTTP 长连接和群消息都不拥有实际操作的执行权。

## 2. 用户交互与命令树

### 2.1 典型操作

```text
#forest help
#forest bot status
#forest run list --conversation here
#forest run cancel run-42 --reason "停止这次回答"
#forest bot pause --mode maintenance
#forest module reload weather --expect-revision 18
#forest context inspect --conversation here --limit 20
#forest memory inspect assertion-17
#forest config set agent.max_steps 12 --expect-revision 8 --preview
#forest confirm approval-23 <一次性确认码>
#forest jobs status job-83
#forest logs tail --job job-83 --lines 40
#forest ops runbook restart-provider --arg provider=qiyu
#forest ops exec --profile maintenance -- python --version
#forest --bot bot-b module status
#forest core status
```

未写 `--bot` 时目标固定为接收到该事件的 Bot。跨 Bot 必须显式指定稳定目标 ID，执行前验证目标管理权；不按昵称模糊匹配，不对同群全部 Bot 广播。回执默认回原命令会话，目标 Bot 的可读权限与该群的输出可见性分别检查。

| 命令树 | 首版动作与行为 | 默认服务等级 |
|---|---|---|
| `help`、`status` | 授权后可见的命令帮助、版本与运行摘要 | P1 |
| `bot` | list/status/pause/resume；明确 Bot 与暂停模式 | P1；紧急 maintenance pause 为 P0 |
| `module` | list/status/validate/enable/disable/reload；返回影响集合与生命周期 operation | P1；救援 disable 只走 core |
| `run` | list/inspect/cancel；取消指定 Run，不自动取消后续用户触发 | P1；cancel 为 P0 |
| `context` | inspect/compact/reset；读取历史、申请裁剪、建立新上下文视图 | P1 |
| `memory` | inspect/search/assert/retract/resolve/forget/rebuild；沿用全局 Bot 存储与 06 契约 | P1 |
| `config` | get/diff/set/validate；类型校验、revision、preview 与 reload_policy | P1 |
| `jobs`、`logs` | list/status/cancel；受限分页、tail、取消订阅 | P1；cancel 为 P0 |
| `confirm`、`deny` | 对固定 approval 做单次批准/拒绝，解除既有等待 | P0 |
| `ops` | runbook/exec/shell/reconcile；持久工作、外部效果对账 | P1；重计算在 P4 执行 |
| `core` | status/cancel/disable/reload；最小稳定救援集合 | status/cancel/disable 为 P0；reload 为 P1 |
| `ext <plugin> ...` | 部署批准的扩展管理命令；仍使用 CONTROL 与管理参数保护 | P1，不允许插件自行升为 P0 |
| 独立业务前缀 | 普通插件命令，例如 `/hello`；无隐式管理能力 | P2 |

`context reset` 默认以 `--mode safe-point` 登记持久意图，由会话所有者在模型/工具因果组闭合后，以预期 `context_view_revision` 发布新视图；忙时立即返回操作引用。若已有 FINAL_READY，先按原输入完成合法最终提交，再执行重置；若还需继续推理，先 checkpoint 并按新视图修订重新构造后续请求。旧视图的迟到最终候选不得跨过重置提交。工具等待尚未闭合时可继续等或显式选择 `--mode cancel-run`，后者先通过 Run 取消守卫终结旧执行权，再按同一状态所有者协议发布视图。

重置不删除 Inbox、触发账本、旧工具事实或回退输入游标；仍保留/重新关联所有未终结触发的必要原文与主体，不把“已从活动视图折叠”当作已回答。需要真正遗忘使用 `memory forget` 删除屏障。Shell 不直接改活跃 Prompt，也不凭管理服务等级取得并行 Context 写权。

### 2.2 参数与注册契约

一条平台原始消息对应一条 Shell 请求。只允许去除由 Adapter 验证的开头 Bot mention 和前导空白，再匹配大小写敏感的完整 `#forest` token；引用、转发、代码块、附件文字和 OCR 结果均不作为可执行入口。`#forestX` 不是该前缀。

解析器输出结构化 `argv`：空白分词、单双引号聚合、明确的反斜杠转义规则，支持 `--key value`、`--key=value` 与 `--` 结束选项。单引号内容逐字保留；双引号仅解码 `\\` 与 `\"`；未闭合引号、重复单值选项、未知选项、额外参数均报错。未经引号保护的换行、`;`、`|`、`&&`、重定向不构成框架命令连接语法；首版拒绝多命令。`$VAR`、反引号、`$(...)` 在框架 argv 中不展开，作为普通参数由 schema 验证。

每个 `CommandSpec` 声明 `command_key, command_version, schema_digest, args_schema, required_capabilities, resource_scope, service_class, execution_kind, confirmation_policy, output_visibility, timeout`。参数 schema 声明类型、必填、枚举、范围、默认值及 `sensitive`；帮助直接由同一 schema 生成。路径参数由目标服务在其配置根目录解析，`--` 后参数也不能跳过授权和范围检查。

未加引号的反斜杠按字面保留；双引号内非上述两种转义的反斜杠同样保留，避免 Windows 路径被隐式改写。需要字面分号、竖线或换行的参数必须整体引号包围并符合 schema，仍不会生成第二条命令。跨消息续行、交互 stdin 和自动命令替换不属于首版协议；复杂脚本通过不可变 script-ref 显式提交。

注册表是不可变快照。使用精确 token 树匹配，别名先归一为唯一 command_key；同一命令键/别名重复默认拒绝启用。插件只能注册获分配的 namespace，不能覆盖 `core`、管理树或全局选项。需要替换业务命令时由部署配置选择确定所有者。处理器的 `priority` 高值优先只决定同阶段策略顺序，不能抢占保留前缀或改变已解析命令。

**`#forest` 一旦被认领，未知命令、权限不足、语法错误、超时和模块故障都以 Shell 结果终结，不能 fallback 到 Agent。** 只有未匹配任何保留入口的正常消息继续业务命令/Agent 分类；业务 namespace 被认领后的错误同样不能换一个处理器解释。

## 3. 接入分流与三层优先级

### 3.1 ShellGate 不依赖 Agent 或连续分类头

```text
可信 Adapter → Inbox 去重/分配 ingress_seq + ShellGate 快速预分类
  ├─ CONTROL → durable command_requests → P0/P1 控制调度 → 管理服务
  ├─ BUSINESS → 持久业务分类/命令 → P2 调度
  └─ NONE → 普通分类/触发 → P3 Agent 或 P4 静默维护
全部入站 → 按 ingress_seq 连续推进 Context → 08 输入切口/裁剪
管理结果 → send_lane=CONTROL → Delivery → 原群回执
```

ShellGate 位于稳定内核，只执行有长度上限的本地前缀判断、可信来源检查、注册快照绑定与 durable 领取；不运行用户插件、不联网、不调用模型。Inbox 同事务保存 `command_route=CONTROL|BUSINESS|NONE`、`command_id?`、快照版本及待调度记录。`#forest` 整个保留为 CONTROL，包括扩展管理命令和未知子命令；BUSINESS 只用于 `/hello` 等独立业务前缀。此分流不依赖可热替换插件，完整 argv/schema 解析在事务外进行；`core` 使用独立的最小内核解析器。

识别到 CONTROL 即记录 `trigger_decision=CONTROL_COMMAND` 与安全上下文投影。无效管理命令仍有这个终态，原文保护存储且不阻塞连续分类头。其命令执行与回执**不等待** `classified_through_seq`、`context_applied_seq`、Actor lease、输入切口或模型/工具结束；事务只与必要的窄会话计数器、目标资源守卫短暂竞争。不能把“控制队列独立”实现为先排在普通异步分类后面。

例如消息 40 的普通分类卡住，41 是 `/hello`，42 是 `#forest run cancel run-42`：42 可先登记并执行取消、发控制回执；Context 仍等待 40 分类终结后依次推进 40、41、42。控制执行早于上下文物化不改变来源顺序。

### 3.2 调度服务等级

| service_class | 内容 | 配额与抢先边界 |
|---|---|---|
| P0 | 取消、已建 approval 的批准/拒绝、紧急停接业务、core 救援 | 独立小队列与保留控制执行槽；只做有界状态变更 |
| P1 | 常规查询、配置、生命周期请求、管理 Job 接纳 | 控制面预留槽；长工作提交 Job 后释放槽 |
| P2 | 普通插件命令 | 独立命令预算；涉及 Agent 的工作仍取得同群 Session |
| P3 | 前台 Agent 模型/工具调度 | 按 Bot 和会话公平排队，不占满控制资源 |
| P4 | 确定性裁剪、归档、批量索引及图重建 | 保证最低进展配额；必需视图准备继承有界deadline，裁剪不申请模型槽/摘要API |

P0 数值最小、优先最高，与 handler.priority 的数值方向不同。调度先满足保留容量，再按等级的加权公平选择、等级内 Bot 权重与会话/调用者轮转；有限 aging 只能在分配份额内推进，不能升级管理权限。不能用无限严格优先队列让 P3/P4 永久饥饿。

初始建议每宿主至少预留 2 个 P0、4 个 P1 轻量执行槽及独立数据库连接份额；每 Bot 控制待办最多 128 条、每主体同时 2 个长管理 Job。所有等级受宿主总资源约束，具体数值经压测调整。P0 仅接受有资格主体的控制请求；其余命令尝试走受限拒绝预算，重复限速通知合并，防止伪造大量 cancel 占满救援容量。无法接纳返回可查询的 OVERLOADED，不丢弃已承诺接收的请求。

预留必须从接入开始：在 Inbox 提交前，根据可信 sender、来源绑定及内核维护的资格索引进行有界预检，为核心候选分配独立入站/数据库份额；未知或明显无权请求不能占用这些待办槽。资格索引仅影响排队资格，实际操作仍按目标库当前授权执行；索引未就绪时由受限核心核验路径处理，不将任意请求当管理员放行。普通、拒绝和控制待办分别计额，前两者满载不能耗尽 P0 保留额度。已承诺接收的事件保留历史与最终拒绝状态；未接纳事件按协议背压/拒绝并记录容量缺口，不能伪造为已可靠接收。平台自身若在 Forest 之前丢弃或限制消息，框架不能保证从该通道及时救援。

保留控制任务、连接池与外发额度；模型槽饱和不能影响 core status。CPU 阻塞插件由独立 Worker 隔离。数据库/平台彻底不可用时不能承诺即时回执；在正常可用基线下测量 control 排队时延、最老等待与拒绝率。

### 3.3 最终提交顺序与并发合流

同资源命令以 `operation_id`、预期业务 revision、当前授权与 owner epoch 进入 CommitGuard。生命周期沿用 `lifecycle_resource_leases`；其他单资源操作在既有业务行 CAS，多资源修改按 [05](05-storage-security-operations.md)统一锁序取齐所需资源。数据库行锁和事务内不等待模型或用户；生命周期逻辑资源租约可以跨排空等异步阶段保留，通过续期、deadline 和 fencing 恢复。等待用户批准时尚不占有执行阶段的生命周期资源租约。

高等级仅能先领取尚未执行的工作，或在显式策略下把**尚未提交/许可的低等级意图**标记 CANCELLED/SUPERSEDED。标记与低等级许可/提交竞争同一守卫；赢家形成审计事实。已经提交的配置、记忆事实、ModelTurn 和 DISPATCHING 外部请求保留结果，不能声称 priority 能回滚它们。

P0 紧急取消/暂停只独立提交 `cancel_requested` 或 `bot_runtime_state.data_admission` 与递增的 `maintenance_epoch`，不夺取仍有效的生命周期资源 lease。实际排空/停用由持原 lease 的操作协作执行或由其过期后的受控接管完成。所有重载、resume 与恢复动作在开放业务前检查最新 maintenance_epoch/admission 期望，不能用旧计划重新打开管理员刚关闭的业务入口。

| 并发场景 | 裁决 |
|---|---|
| 两名管理员均基于 config revision 8 改同字段 | 首个提交生成 9；另一个 REVISION_CONFLICT，展示新差异，不能盲写覆盖 |
| WebUI 标签页和 Shell 基于相同 config revision 并发提交 | 同一目标 head CAS 裁决；Web 返回 HTTP 409 并保留草稿，Shell 回 REVISION_CONFLICT；显式重基后才新提交，不能因入口不同绕过冲突 |
| reload 与 disable 相交组件 | 资源 lease 互斥；紧急请求可要求取消尚未切换的 reload，不能解锁另一 operation 的资源 |
| cancel 与 Agent 最终提交 | 同一 Run 守卫裁决；cancel 先成功则旧回答不能提交，提交先成功则报告已完成 |
| memory forget 与晚到摘要/索引 | 删除屏障拒绝旧派生产物；沿用 06 防复活，不只取消一个后台协程 |
| 两群管理同一 Bot 的记忆 Resolve | 固定候选集摘要与 revision；过期仲裁失败，不能取最后完成的结果 |

管理命令通常是顺序相关的状态迁移，不能当作可交换 reducer。只对 06 已定义的可合流事实保留交换/结合/幂等保证；重放复用已记录的命令解析、裁决与 operation，不能按新到达顺序重新解释历史。

## 4. 身份、授权与确认

WebUI 与 Shell 的相同动作使用相同 P0–P4 服务等级和有界配额；HTTP 登录/事件流另有入口容量，不会使网页用户天然比群管理员优先。Web 紧急取消仍须当前目标授权，不能抢占另一个有效生命周期 lease；页面按钮是否可见只用于交互提示，最终提交守卫不可省略。操作中心可查询来源为 Web、Shell 或 Agent 工具的已获授权操作，群内查询也可查看 Web 创建的操作，但读取结果与向群公开分别授权。

群管理员角色只说明平台群管理身份。部署政策可授予其本群 run.cancel、受限 context.inspect 等能力；Bot 管理员可获得指定 Bot 的配置/模块管理权；部署管理员才可被授予主机资源或跨 Bot 管理能力。角色本身不自动等价服务器 root。

每条命令绑定真实平台 sender、source_event_id、origin_bot_id、origin_binding_id、origin_conversation_id 与宿主签发的 origin_principal_ref/origin_auth_ref；目标库独立解析 target_principal_ref/target_auth_ref。原命令是唯一请求 authority，不从上下文中最近管理员、引用作者、同批最高权限者或 Shell 会话继承授权。有效权为主体、部署/Bot 策略、执行组件申请能力及目标范围的交集，在执行/提交/外发前重验；机器人 echo、模型生成文本、转发及内容重放不执行命令。

读取权限与群输出权限分别判定。日志、配置、记忆读取默认只返回脱敏摘要及 operation ID；敏感内容可生成绑定主体且短期有效的受保护结果引用，不能因为发令者有权就把全量私聊或凭据发群。日志中的 `#forest` 文本只是输出数据。

普通查询与已授权可逆动作直接执行。清空视图、大范围遗忘、停用关键基础设施、破坏性 runbook 等少量高影响动作先生成 preview：目标、影响集合、差异、是否重启/断连、预期版本与可恢复边界；再创建唯一的 `approval_requests`。Shell 的 `confirm/deny` 与 Agent 工具批准复用这个账本和执行许可，不建立第二套批准状态。

确认绑定 `command_id, argv_digest, command_version, schema_digest, plan_digest, expected_revisions, generation_digest, principal, origin_bot_id, target_bot_id, conversation_id, expires_at`；这里 approval 的 conversation_id 固定为原命令会话。一次性码只保存摘要，消费与决定在同一事务；过期、改参数、换群/换人、目标版本变化均失效。确认前后均重验授权，确认只是批准固定计划，不是登录管理员身份或授予新增能力。需要重新规划时生成新 preview，不能复用旧批准。

以上是批准的 Shell 来源分支；Web 分支在同一 `approval_requests` 中使用 `origin_ref={kind:web,management_request_id,web_subject_ref,session_ref}`，不要求伪造 `conversation_id`。在另一个入口查看同一计划不等于可跨入口消费批准：Web 账户与 QQ sender 只有通过明确验证、可撤销的身份映射才可能等价，不按昵称、自报 QQ 号或浏览器填写 user_id 合并。跨入口确认还必须由固定计划允许该确认渠道，并满足原来源绑定及当前映射修订；不满足时以当前入口主体重新形成需要的批准，原批准不会被复制令牌绕过。普通已授权可逆配置不因此增加强制人工确认。

可选 `shell_sessions` 仅保存输出偏好、显式选择的目标建议等，默认 TTL 15 分钟，绑定 `(sender, origin_bot, conversation)`。每条消息仍写 `#forest` 前缀、每个命令重新验证权限；跨 Bot 执行仍要求该命令显式 `--bot`，目标建议只用于展示和补全。退出/过期清除偏好；这不是 08 的 ConversationSession，也不占用群 Agent 所有权。

## 5. 群上下文与回执发送

### 5.1 所有来源保留，控制数据不进入指令角色

Shell 原消息保留自己的 ingress_seq、作者快照、当时昵称和引用关系。进入 Context 的是 `context_role=control_data` 安全投影，例如“用户 u-17 请求取消 run-42，command c-9”；其 `trigger_decision=CONTROL_COMMAND`，不产生 conversation_trigger，也不推进最后 Agent 触发切口。`control_data` 是数据分类，Provider 渲染为带来源的普通用户数据，不渲染为 system/tool 授权。

投影仅保留允许在该会话出现的命令类别、非敏感目标和结果引用；敏感参数从首次收集起不进 LLM、摘要/嵌入、普通日志。原载荷与 argv 保存受限引用并设置保留期；secret 参数使用预存 `secret_ref`。未知/坏语法命令无法可靠识别字段时，只投影“用户提交了一条未成功解析的管理命令”，不回显原文本。

控制消息仍按 08 的输入批次进入短期视图和裁剪：当前 Run 忙且它位于最后触发之后，则先留队；下一次 Agent 触发切口覆盖它或静默排空时再进入 Context。控制动作无需等到这一刻。异步操作结果是带 command_id 的独立控制事实，不能回写原消息冒充当时已知结果；允许的结果摘要经会话协调器按提交因果顺序物化。

### 5.2 CONTROL 与 CHAT 两条发送通道

`send_intents` 增加 `send_lane=CONTROL|CHAT`。CHAT 沿用 `(conversation_id, response_seq, part_index)`；CONTROL 使用 `(command_id, control_seq, part_index)`，同命令先接纳回执、后进度、后最终结果，重试保持同一子项 ID。普通 P2 命令回复属于 CHAT；管理回执不分配 response_seq、不创建 Agent ModelTurn、不把待触发标成 ANSWERED。

CHAT 的早期 DISPATCHING/UNKNOWN 只阻塞同 CHAT 有序链，不能阻塞 CONTROL 的取消/批准/错误回执；不同管理命令无需按原消息顺序完成，回执必须引用各自 command_id 与发令消息。CONTROL 自身某命令的 UNKNOWN 默认阻塞该命令后续分片，另一个 status 命令可以独立查询它，不能用新 ID 偷偷重发未知内容。

两条通道共享平台账号速率限制，Delivery 预留少量控制令牌并做有界公平分配。高优先只能影响尚未发出的意图；不能抢走已发网络请求、突破平台风控或承诺实际展示的全群总顺序。平台只能串行请求时仍受当前有界请求/连接超时约束。

CONTROL 回执标记 `origin=control_output`，先入控制输出账本；平台 echo 只关联既有输出/来源占位，不重复追加，不触发 Agent，更不能重新交给 Shell 解析器。跨 Bot echo 按平台可信作者、因果 ID 与 hop 策略处理，不能成为另一个 Bot 的管理员命令。

## 6. 长任务、内容处理与原生进程

命令接纳后尽快回复 `command_id / operation_id / job_id / state`，不能在控制槽里等待编译、索引重建、模型、长文件任务完成。长工作使用现有 `jobs`，记录固定参数、身份、目标、deadline、lease、取消和恢复点；`jobs status` 可查询，即使发令者暂时离线也能继续。`logs tail` 是有速率、行数、字节与 TTL 上限的订阅，支持分页 cursor 和停止；进度可合并，终态必持久。

配置修改先 schema 校验再生成差异，提交必须携带 expected_revision；generation/restart/migration 字段转交对应生命周期工作流。上传配置附件只形成受限工件，命令显式引用该工件摘要并 preview，附件不自动执行。记忆内容修改沿用 Assert/Retract/Resolve/Forget，不把一段 LLM 生成 JSON 覆盖成整库状态。

配置 `get/status` 和控制回执区分 desired 与 effective，显示待生效资源、operation 和失败原因；`set` 接纳成功不能宣称客户端/模块已经切换。Web 表单和 Shell 修改同一 `ConfigRevision/config_heads` 权威，generation 在路由/状态 CAS 时同步发布 effective，失败保留旧生效值；回滚是新修订，跨 Bot 批量修改逐目标显示部分成功。部署模板变更不会在下一次读取时悄悄改变 Bot 已固定快照。凭据在 Web write-only 表单写入 Secret Resolver 后，Shell 只引用 `secret_ref`，不要求管理员将 API key 直接发群。

`context compact` 只执行08定义的本地预算裁剪，不生成记忆摘要或隐式调用模型。`memory inspect/search` 返回的每项内容明确来源群、主干与作者；`--source-group <稳定ID>` 仅是Bot全局域内的筛选条件，不切换群数据库。跨主干联想查询和重建遵守[10](10-multi-trunk-memory-graph.md)，群名仅作显示，不用于目标身份解析。

原生运维分为三种明确能力，由可选 `ops.process` Worker 提供：

| 模式 | 适用需求 | 执行约束 |
|---|---|---|
| `ops runbook <name>` | 常见诊断、重建、重启外部服务 | 部署登记的版本化步骤和参数 schema；固定脚本摘要/能力，按步骤账本恢复 |
| `ops exec --profile <id> -- <argv...>` | 管理员临时调用部署允许的程序 | 明确 `host.exec` 授权；argv 直接交给进程 API，默认不启用 shell；执行文件、参数、cwd、环境与输出由 profile 限定 |
| `ops shell --profile <id> --script-ref <artifact>` | 确实需要管道、条件语句或复杂系统维护 | 额外 `host.shell` 授权；固定解释器及不可变脚本文本摘要，preview/确认绑定整个脚本和 profile；在受管进程中执行 |

profile 声明 OS 身份、允许目录/程序、网络与密钥用途、CPU/内存/时长/输出限额；路径解析检查符号链接后的真实范围。高权限主机维护可以配置专门的显式授权 profile，不能从“群管理员”隐式得到。`ops shell` 的解释语义只在该能力边界内启用；框架命令解析器本身从不 eval 参数。变更可执行文件/解释器/profile 使未执行的批准失效。

进程 stdout/stderr 保存受限工件，群中只发脱敏限长摘要、退出码与结果引用。取消先记录意图，再终止受管进程树并限时回收；杀进程不证明其已产生的文件/网络修改被回滚。对可能非幂等的脚本或进程在启动前写 effect_operations，进程失联进入 UNKNOWN，恢复不自动再次运行；状态检查/人工对账沿用同一 operation。

## 7. 持久 DTO、状态机与执行步骤

```json
{
  "command_id": "cmd-42",
  "source_event_id": "evt-42",
  "origin_bot_id": "bot-a",
  "target_bot_id": "bot-a",
  "origin_binding_id": "binding-a",
  "origin_conversation_id": "qq-group-a",
  "origin_principal_ref": "host-issued-origin-principal",
  "origin_auth_ref": "host-issued-origin-authority",
  "target_resource": {"kind": "module", "id": "weather"},
  "target_principal_ref": "host-issued-target-principal",
  "target_auth_ref": "host-issued-target-authority",
  "command_key": "module.reload",
  "command_version": "1.0",
  "schema_digest": "sha256:reload-schema",
  "registry_snapshot_id": "commands-a-18",
  "argv_ref": "protected:cmd-42-argv",
  "argv_digest": "sha256:canonical-argv",
  "input_digest": "sha256:canonical-command",
  "service_class": "P1",
  "state": "RUNNING",
  "operation_ref": {"kind": "lifecycle_operations", "id": "op-42"},
  "approval_ref": null,
  "reply_route": "original-command-reply-route",
  "deadline_at": "2026-09-12T13:01:00Z"
}
```

DTO 示例展示已解析并启动的 module.reload 请求；argv_ref 对应固定的规范参数，敏感字段只在获权执行边界解封。身份、路由、服务等级与 operation ID 由宿主构造，插件返回的同名字段不构成授权。完整字段与条件约束以[05 数据模型](05-storage-security-operations.md#21-控制与所有权)为准。

| 表/扩展 | 字段与约束 |
|---|---|
| `command_requests` | 沿用05完整schema：原始来源/身份与目标授权分开，registry_snapshot_id 固定命令解释，argv_ref/argv_digest/input_digest 固定参数；源事件唯一；operation_ref/approval_ref 尚未建立时可空，批准引用含权威 bot_id/approval_id |
| `command_registry_snapshots` | snapshot_id、scope、revision、digest、entries_ref、created_at；不可变，引用中的版本保留到所有 claim 终结 |
| `approval_requests` 扩展 | command_id、token_digest、plan/argv/schema/generation 摘要、expected_revisions、origin/target_bot_id、conversation_id、consumed_at；沿用 principal、scope、expires_at、decision，为唯一批准权威 |
| `shell_sessions` | shell_session_id、sender_principal_id、origin_bot_id、conversation_id、default_target_bot_id、output_preferences、expires_at、revision；只保存交互偏好 |
| `inbox_messages` 扩展 | command_route、command_id?、registry_snapshot_id?、context_role、safe_projection_ref；原文与模型投影分开 |
| `send_intents` 扩展 | send_lane、command_id?、control_seq?；CHAT 的 response_seq 与 CONTROL 的 command/control_seq 各自唯一 |
| 复用的执行账本 | lifecycle_operations、jobs、effect_operations、相关资源事实与 domain_outbox；command_requests 不另造一套生命周期/效果终态 |

```text
RECEIVED → PARSED → AUTHORIZED → QUEUED → RUNNING → SUCCEEDED
    └──────────────→ REJECTED
              │         └─→ WAIT_APPROVAL ─→ QUEUED
              └───────────────────────────────→ REJECTED
QUEUED / WAIT_APPROVAL → CANCELLED / EXPIRED / SUPERSEDED
RUNNING → FAILED / CANCEL_REQUESTED / UNKNOWN
CANCEL_REQUESTED → CANCELLED / SUCCEEDED / UNKNOWN
UNKNOWN → SUCCEEDED / FAILED / MANUAL_RESOLUTION
```

REJECTED 含语法/权限拒绝；已执行效果不因命令 deadline 到期直接记为 EXPIRED。RUNNING 是其所引用操作的查询投影，底层已提交而回执失败时仍报告业务成功；重启通过 operation_ref 对账，不能重复操作。

RECEIVED 阶段只固定来源、可信主体、入口路由与注册快照引用；`command_key/command_version/schema_digest/target_bot_id` 等尚未确定字段可空，原始事件 digest 与解析后 `input_digest` 分别记录。PARSED 短事务冻结规范参数和命令契约，AUTHORIZED 后才能取得相应服务类别的执行权；语法失败从 RECEIVED/PARSED 进入 REJECTED。命令快照的 `revision` 不能用 Bot 配置修订或路由 epoch 代替；请求使用 `registry_snapshot_id` 精确定位。core 命令使用内核内置版本化 schema，即使扩展注册快照无法加载也能受理。

```python
async def accept_message(event):
    trusted = adapter.verify_and_normalize(event)
    with bot_store.transaction() as tx:
        msg = tx.inbox.insert_once_and_allocate_seq(trusted)
        if not msg.inserted:
            return msg.existing_receipt
        route = shell_gate.classify(msg, tx.command_registry_snapshot())
        tx.inbox.set_route_and_safe_projection(msg, route)
        if route.kind == "CONTROL":
            tx.commands.insert_once(msg, route, state="RECEIVED")
            tx.outbox.add_control_ready(msg.command_id)
        else:
            tx.jobs.enqueue_classification(msg.event_id)
        tx.conversations.mark_pending(msg.conversation_id)


async def execute_command(command_id):
    request = await commands.claim(command_id)  # 有限槽/lease/固定快照
    spec, argv = parse_with_snapshot(request)
    request = await commands.freeze_parsed_once(request, spec, argv)
    authority = await authorize_exact_request(request, spec, argv)
    plan = await load_or_make_frozen_plan(request, spec, argv, authority)
    if plan.requires_confirmation and not await approvals.matches(plan):
        return await persist_preview_and_request_approval(request, plan)
    operation = await submit_idempotent_operation(request, plan, authority)
    return await persist_control_receipt(request, operation)
```

完整实现中错误也持久终结并发安全回执；批准后的恢复使用原 plan，不重新执行可改变参数的 parser。尚未创建目标操作时，approval 绑定 command_id 与计划摘要，不伪造 operation_ref。submit 在对应目标存储内重验权限/版本/lease 和批准的有效范围，产生真实线性化决定；`approvals.matches` 的早期查询不能替代最终许可检查。

跨 Bot 请求由原 Bot 保存 command_requests 和回执；目标 Bot 用稳定派生 operation_id 幂等接纳目标子操作。通过 durable outbox、查询与受权重试推进，原库与目标库不做伪跨库事务。目标提交后原库失联，恢复必须查原 operation；不得生成新 ID。多 Bot 批量命令必须显式列目标并分别展示成功/失败，不承诺整体原子性。

需要批准时，`approval_requests` 的权威记录与目标操作放在同一目标 Bot 库，原命令仅保存 `approval_ref={bot_id, approval_id}`。来源群的 confirm/deny 通过同样的受权幂等子请求提交到目标库，目标事务核验批准主体、原群、计划、期限与目标授权，再记录决定；执行许可在该库内原子复核批准、资源版本及单次执行身份。不能先读来源库的“已批准”缓存再声称与目标副作用原子提交。来源 Bot 停止转发后的撤销结果可能尚未到达目标，必须查询目标权威状态后报告；批准回执和受限结果仍通过原 Bot 的受控输出链返回。

## 8. 热重载、暂停与故障救援

命令请求在入站时绑定 registry_snapshot_id，解析事务再固定 command_version、schema_digest 与输入摘要；排队期间不使用新注册表重解释旧文本。路由与命令表快照在受影响 Bot 的受控切换中发布；已经 claim 的旧请求由兼容旧执行器排空，或由声明兼容的新代接管。无法读取旧契约则阻止切换/保留兼容执行器，不能静默换 argv 语义。

shell.command 的解析、帮助、丰富输出与扩展命令可以热重载。其 durable 请求、批准、Job、操作和回执不保存在插件对象里；shell 重载不取消已经批准的合法操作，也不产生重复回执。旧 Worker 的迟到提交仍受 epoch 守卫。

稳定内核保留 ShellGate、最小 core parser、授权执行、控制请求/回执账本访问与救援注册。`#forest core status/cancel/disable/reload` 无需调用故障 shell 或业务插件；救援 reload 只接纳现有受控包/配置引用，复杂构建转为 Job，P0 不运行任意脚本。内核救援也要鉴权、幂等、资源 lease 和审计。

`bot pause --mode conversation` 停止新的 Agent/业务执行，按配置排空或明确取消当前工作，继续保存接纳消息并允许控制；`--mode maintenance` 优先关闭 data_admission 并提升 maintenance_epoch，保留 control_admission、Adapter 接收与必要恢复/静默记录。data_admission/control_admission 是独立持久门；重载结束不覆盖暂停期望。需要停止 Adapter、迁移事实库或宿主重启时必须给出可用通道与断连结果，不能承诺维护期间仍能通过该群控制。

若仅 shell/业务模块崩溃，core 救援应可用；若对应 Adapter、Bot 事实库、宿主网络或整个主机失效，单一 QQ 通道无从自救。部署 CLI/独立管理 API 共享相同鉴权和操作协议，作为实际可访问时的另一管理入口。

## 9. 验收场景

以下为实现阶段的测试要求，使用真实事务、假平台/Provider 与 barrier 控制交错，不表示当前文档已运行这些测试。

| 编号 | 场景 | 必须观察到的结果 |
|---|---|---|
| GS-01 | 慢模型与工具等待期间发送 core status/cancel | 不等待 Agent 安全点；正常基线内控制请求与回执获得保留容量 |
| GS-02 | 普通 ingress 40 分类阻塞，42 为管理命令 | 42 控制执行/回执先完成；Context 仍严格按 40..42 消费 |
| GS-03 | 未知 #forest 命令、坏引号、未知选项、权限不足 | 唯一 Shell 错误终态，无 Agent fallback，Context 有作者与安全投影 |
| GS-04 | 普通成员伪造管理员昵称、转发批准、Bot echo 含命令 | 无管理能力和命令执行；只保留允许的历史来源 |
| GS-05 | 同批普通成员与管理员消息混合 | 管理命令只使用该原事件 sender 的 authority，权限不合并 |
| GS-06 | 两 Bot/两群同时配置相同资源名称 | 明确目标与隔离；跨 Bot 未授权拒绝，回执回原发令群 |
| GS-07 | 同 revision 并发配置写、Resolve 与 reload/disable/pause | 明确 CAS/lease 胜负，无最后写覆盖或盗用他人 operation lease，重载不重新打开暂停业务 |
| GS-08 | 高等级 cancel 与低等级提交/外部许可交错 | 胜出顺序唯一；已提交/在途效果不被宣称回滚 |
| GS-09 | Agent CHAT 出现 UNKNOWN，管理员发 cancel/status | CONTROL 可发送；CHAT 顺序和 UNKNOWN 对账规则继续有效 |
| GS-10 | 同命令接纳/进度/完成分片乱序准备 | control_seq/part_index 有序；其他命令可独立发回执 |
| GS-11 | 全 Bot 普通流量与恶意控制请求洪峰 | 内存/连接/待办有界，P0 授权请求可进展，P3/P4 不永久饥饿 |
| GS-12 | 管理命令在最后 Agent 触发之后到达 | 即刻控制执行，原消息按 08 留尾或静默推入，正常裁剪、不多开 Agent |
| GS-13 | 敏感参数、错误参数、日志中出现指令样式文本 | 不进入 LLM/普通日志/群回显，输出不被二次解析执行 |
| GS-14 | 同一确认重复、过期、换群/换人、改配置版本 | 至多一个有效批准；其余拒绝，原固定计划不能被换参 |
| GS-15 | 普通 status/reload 与危险 forget/config 预览 | 前者按授权直接执行，后者按明确策略绑定 preview；无全局重复确认 |
| GS-16 | 长命令接纳后重启，或目标已提交但原 Bot 回执未存 | 查询原 operation/job 恢复，重试不重复副作用 |
| GS-17 | shell/业务插件崩溃与 shell 自身 reload | core 救援可用，旧 claim 不被新 schema 解释，批准/作业/回执延续 |
| GS-18 | exec 超时/取消、原生脚本启动后失联 | 受管进程树回收，限长输出，非幂等结果 UNKNOWN，不自动重跑 |
| GS-19 | 群管理员尝试原生 shell，部署管理员用显式 profile | 前者无隐式主机权；后者只获授予的 OS 身份/范围/脚本计划能力 |
| GS-20 | 不断 tail 日志、多命令进度与正常对话竞争平台限额 | 有界订阅/分页、控制预留与公平限流，不突破平台限额 |
| GS-21 | conversation pause、maintenance、Adapter/DB 停止 | 两种 pause 保留声明的控制路径；基础依赖停机如实报告不可用边界 |
| GS-22 | context reset/forget 与旧 Agent、摘要回调交错 | 预期视图/取消/删除屏障拒绝迟到覆盖，原来源与未取消触发不丢失 |
| GS-23 | Inbox接纳或PARSED冻结前后崩溃，再投递同一平台事件 | 原来源与控制请求同事务存在或均未提交；恢复沿用同command_id和注册快照，错误命令也不重复解析成另一个操作 |
| GS-24 | 原群跨Bot确认后断线，或目标撤权与执行许可竞争 | 批准与执行由目标库裁决，原库按同approval_ref/operation_ref恢复；不把来源缓存批准当目标库原子许可 |
| GS-25 | Web 与 Shell 同版本改配置并查询另一入口的操作 | 同目标 CAS 和权限裁决，冲突不丢失更新；共用 operation_ref，不建立重复执行或重复批准账本 |
| GS-26 | Web 账户与 QQ 同昵称，或复制另一入口的批准令牌 | 不自动等价；只有已验证映射、原渠道/来源绑定和当前权限均满足才可确认，否则重新批准 |
| GS-27 | Web 修改 Bot 配置后群 Agent 运行，页面刷新/掉线 | 不生成伪造 Inbox/trigger/ModelTurn；已接纳操作可恢复，配置按声明边界生效，页面断连不取消或重做 |
