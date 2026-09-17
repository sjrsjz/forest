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

## 3. Core Gate 与模块命令

内核保留以下救援语义：

```text
#forest core status
#forest core cancel <operation-or-run-id> --expect-revision <n>
#forest core disable --expect-maintenance <n>
#forest core reload <component-id> --expect-revision <n>
```

`status/cancel/disable` 可以声明 P0，`reload` 为 P1。`disable` 关闭当前 Bot 的 data admission 并保留 CONTROL，不等于卸载某个模块；模块停用使用模块自己的 lifecycle action。只有接纳时命中 `shell.command` 对当前 Binding/群生效的管理员策略，并通过 `kernel.authorization` 精确能力检查的主体才能保留 P0；否则命令拒绝或降为普通 P1 查询，不能仅凭文本或平台群角色取得保留槽。执行和回执前再次查询当前授权、Binding epoch、Host lease、目标 revision 和资源所有权。

Core Gate 只规范化到既有 operation、Conversation Run 和 module lifecycle 权威。它不复制 Agent/Memory/Scheduler 状态机，不直接停止线程或改数据库行。正常模块不可用时，Core 命令仍能撤下路由、请求取消或读取最小健康信息。

其他命令由模块描述符注册：

```text
CommandSpec {
  command_id, command_version, tokens, aliases,
  argument_schema_digest, management_operation_id,
  required_capabilities, target_scope,
  service_class, output_policy, timeout
}
```

示例：Agent 模块注册 `run list/inspect/cancel`，Memory 模块注册 `memory sources/query/forget-*`，Conversation 模块注册 `context inspect/compact/reset`，Job 模块注册 `jobs list/run/cancel`。控制台和 Shell 都从同一 management operation catalog 发现这些动作；控制台不会因此出现硬编码 Agent/Memory 顶层页面。

模块命令 registry 是 RouteGraph 的一部分。新 Generation 发布时一次切换，之后接纳的命令使用新 schema；已接纳请求继续绑定原 Generation 和参数载荷，除非该模块明确声明、宿主验证且管理员允许兼容 takeover。Forest 未发布基线不保留开发期 `shell-gate-v2…v12`、版本专用 Python registry 或旧 writer-generation 分派器。

## 4. 解析规则

只允许去除 Adapter 已验证的开头 Bot mention 和前导空白，再匹配大小写敏感的完整 `#forest` token。引用、转发、代码块、图片 OCR、LLM 输出和附件说明不能成为可执行入口；`#forestX` 不是前缀。

解析器支持空白分词、单双引号、`--key value`、`--key=value` 和 `--` 结束选项。单引号逐字保留；双引号只解码 `\\` 与 `\"`。未闭合引号、重复单值选项、未知选项、额外位置参数和超长字段均拒绝。

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

请求只允许一个有效 claim，带 owner、lease epoch 和截止时间。宿主崩溃后，新 owner 根据 operation receipt 恢复；已接纳的外部效果不能因 ACK 丢失而换 operation 重放。UNKNOWN 必须经查询或人工 resolution 收敛。

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

业务命令策略绑定 `(bot_id, binding/group scope, command_id, component_generation, principal_class)`，默认禁用。命中后的拒绝或失败不能 fallback Agent 以绕过策略。多个 handler 的 priority 只在同一命令 owner 内排序，不能覆盖 Core 前缀或提高服务等级。完整 Handler 顺序见[P2 业务命令](p2-business-commands.md)和[P2 普通消息链](p2-message-handlers.md)。

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
