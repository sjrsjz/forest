# P2 业务命令与普通消息插件链详细设计

状态：协议已冻结，实施切片 1–2 已完成。`@command` 可静态生成正式 manifest/schema，纯命令包可被接纳，候选 Worker 会逐字段核验运行时声明；PostgreSQL effective registry、同名冲突、路由代际原子切换、disable/re-enable 和永久 retirement 原语也已接通。Web 用户策略、消息分类、Actor 执行和回复提交仍待实现。本文补全 03、07、08、09 中的业务命令路径。它不改变每个 `(bot_id, conversation_id)` 只有一个 Agent 的设计，也不创建产品子 Agent。

## 1. 目标与边界

业务插件可以声明确定性群聊命令，例如 `/hello` 和 `/记住`。命令在普通消息接入后由宿主匹配，使用当前平台作者身份授权，在本群唯一 Conversation Actor 内执行，返回受校验的回复提案。无论结果是 Reply、Consumed、Pass、拒绝或失败，原消息都已经进入 Inbox，并按作者和来源顺序进入 Context。

`#forest` 永远先由固定 ShellGate 识别。业务插件不能覆盖、提前观察或消费这个前缀。业务命令是 P2 数据面工作；Shell P0/P1、Agent P3 和后台 P4 的优先级只影响有界资源调度，不改变群消息历史顺序或授权结果。

首版命令匹配是确定性的 token 匹配，不提供正则表达式、任意 Python predicate 或模型分类。通用非命令 message handler、模糊自然语言 intent 和模块级函数适配层在业务命令闭环后单独增加，不能借 `@command` 偷渡。

## 2. 包声明

`module.toml` 的单个 Component 增加 `commands`：

```toml
[[components.commands]]
command_id = "hello"
name = "/hello"
handler = "handle_hello"
permission = "command.hello"
argument_mode = "tail_text"
input_schema = "schemas/hello-command.json"
priority = 0
```

字段语义：

| 字段 | 约束 |
|---|---|
| `command_id` | 包内稳定标识，1–128 字符，版本切换不能无声明复用为不同语义 |
| `name` | NFC 后的单个非空 token，以 `/` 开头，不含空白、控制字符、Bidi override 或 `#forest` 保留形式，最长 64 个 Unicode scalar |
| `handler` | 入口类上的 async 方法名；Worker 验证签名固定为 `(event, ctx)` |
| `permission` | 稳定用户动作名，必须由部署策略显式解释；空值和未知动作均拒绝启用 |
| `argument_mode` | 首版仅 `none` 或 `tail_text`；原 segments 仍随 Event 提供，tail 只是确定性便利投影 |
| `input_schema` | 宿主构造的 CommandEvent schema；不能让插件自行声明可信作者字段 |
| `priority` | 同一已选 owner 内的 handler 顺序，范围 -100–100；不映射 P0–P4 服务类别 |

`@command(name, permission, *, id=None, args="none", schema=None, priority=0)` 只接受可静态求值字面量。自动声明生成器产生同一正式 `commands` 数组和内联 schema；显式 manifest 继续是显式包的权威。候选 Worker 报告装饰器运行时声明，宿主逐字段对照工件，不能只对照方法名称。

首版只允许类方法。模块级函数需要生成并审计适配入口，在该能力实际实现前构建器明确拒绝，不生成不可执行清单。

## 3. 生效注册表与冲突

命令可执行集合来自目标 Bot 当前 effective Component routes，不从磁盘目录、期望配置或正在 prepare 的候选推断。每次组件发布在同一事务写入不可变 `bot_command_generations`，字段包含 package digest、component/generation、route epoch 和完整 CommandSpec digest；`bot_command_heads` 只指向当前 effective generation。

同一规范化 `name` 同时只能有一个 owner。安装或启用候选时锁定命令 head，完整验证 prospective enabled graph：

1. 保留 `#forest` 及其别名固定冲突；
2. 两个 effective Component 声明同名命令时拒绝发布；
3. 组件升级可以用相同 `command_id + name` 原子替换自己的旧代；
4. 删除命令必须经过与 durable handler 相同的显式 retirement 计划，先处理已接纳工作，再撤销 head；
5. 不按包扫描顺序、加载时间或 priority 选择赢家。

管理页面展示 desired/effective owner、package digest、权限动作及冲突原因。管理员不编辑生成的 manifest 文件。

## 4. Web 管理的用户策略

命令声明能力与用户动作授权分开。新增版本化 `bot_command_policies`，以 `(binding_id, command_id)` 为作用域，支持：

- `DISABLED`：默认值，命令不会认领消息；
- `EVERYONE`：可信平台用户均可执行，机器人自身、匿名/缺失身份和回放内容仍拒绝；
- `ADMINS`：发言者必须在 Binding 当前 `admin_user_ids`；
- `USER_SET`：发言者的规范平台 user ID 必须在策略的有界集合中。

策略保存使用 Web 账号重新鉴权、preview/apply、expected revision 和 Bot 权限票据。策略正文进入受来源管理的配置 payload；读取只返回安全配置，不回显其他身份资料。应用后新消息使用新 revision，已接纳命令继续绑定其接纳时的 policy snapshot，并在执行和回复提交前重验“该 revision 仍可终结该工作”；撤销时未开始工作被取消，已产生外部意图按账本对账。

Shell 管理命令策略属于后续显式 CommandSpec，不让业务插件注册管理命令或自行把策略改为 EVERYONE。

## 5. 接入和持久分类

固定顺序为：

```text
HMAC/WebSocket connection proof
  → bounded OneBot parse
  → ShellGate reserved-prefix classification
  → durable Inbox/context admission
  → effective business-command snapshot match
  → author policy check
  → durable COMMAND / TRIGGER / NO_TRIGGER decision
```

Shell 命中时仍走现有 CONTROL 路径，业务注册表不可见该正文。普通路径在读取当前 Binding execution config 时同时取得命令 registry revision 和 policy revision。匹配只使用首个文本 token；mention、reply、forward 和图片不会被串接成权限 token。引用或转发中的 `/hello` 是数据。

`bot_message_inbox.trigger_decision` 增加 `BUSINESS_COMMAND`。同一事务写入不可变 `bot_business_command_requests`：event/conversation/ingress、作者 principal、command identity、component generation、route epoch、registry/policy revision、规范参数和输入 payload 引用。正文摘要不复制到表列。

如果 token 命中已注册名称但策略拒绝，写 `BUSINESS_COMMAND_DENIED` 终态及安全回复意图；它不能改判 Agent trigger。若名称未注册或策略为 DISABLED，则继续现有 Agent trigger 规则。命令注册表读取失败时写可重试分类状态，不能默认为 Agent trigger；到固定 deadline 后写 FAILED 占位以推进连续分类游标。

Inbox 重放要求原事件摘要、命令选择和策略证明一致。分类完成后 registry 变化不重解释旧消息。来源遗忘会 scrub 参数 payload，并使尚未执行的请求终结为 `CONTROL_CONTENT_REVOKED`；迟到同源事件继续使用 metadata-only suppression ACK，不能重建参数。

## 6. 同群执行与优先级

命令和 Agent 共用现有 Conversation Actor lease。每群任何时刻最多运行一个命令 handler 或一个 Agent model/tool quantum，不增加第二条响应链。跨群仍可并行。

Actor 选择顺序：

1. 先把已连续分类的消息按 ingress 顺序应用进 Context；
2. 若存在本群已接纳命令，按 ingress sequence 选择最早一条；
3. 命令完成后再选择后续命令或 Agent trigger；
4. 正在运行的 Agent 不被新命令中断；新命令在下一个模型调用安全边界前不会插入当前 ModelInput，待当前 quantum 结束后由同一 Actor 取得；
5. P2 全局执行槽可以优先于尚未开始的 P3，但必须给 P3 保留最低服务份额，不能让命令洪泛永久饿死 Agent。

命令等待组件重载、依赖或短暂存储故障时保留持久 request 和 Actor 的有界工作资格。它不能持有 Python 协程跨进程重启；重试依赖 operation ID 和调用账本。长期等待释放物理执行槽，但不允许同群另一个响应工作越过它提交 reply sequence。

## 7. Worker 调用与 Context

宿主构造只读 CommandEvent：

```json
{
  "command_id": "hello",
  "name": "/hello",
  "argument_text": "Alice",
  "source_event_id": "event-id",
  "conversation": {"platform": "onebot", "channel_kind": "group", "channel_id": "123"},
  "author": {"principal_ref": "principal:...", "external_user_id": "456", "display_name_at_event": "..."},
  "segments": [{"type": "text", "text": "/hello Alice"}]
}
```

`ctx.principal`、`ctx.reply_route`、`ctx.input_batch` 均由宿主调用帧固定，插件不能传入或改写。命令调用经 Kernel 的 DATA admission，绑定 expected generation/route/policy、deadline、输入 payload parents 和 `command.execute:<permission>` 调用依据；普通组件 grant 仍必须覆盖 handler 请求的 state/service/event/job 能力。

热重载 freeze 后不再向旧代接纳命令。已经取得旧代调用 grant 的 handler 可以在 deadline 内完成，但最终提交再次验证 command request 所绑定的 generation exposure；无法安全提交时保留调用回执并由新代按稳定 operation 对账，不能重复外部 effect。

## 8. 返回值与原子提交

SDK 提供不可变值对象：

- `Reply(parts)`：一到多个规范消息，交由宿主验证大小、媒体引用和平台能力；
- `Consumed`：命令成功但不产生聊天回复；
- `Pass`：只允许交给同一 CommandSpec 内下一 handler；首版每命令一个 handler，因此等同安全终结 `COMMAND_NOT_HANDLED`，不回落 Agent。

Worker 不能直接发送 OneBot action。宿主在一个 Bot 事务内完成：handler 调用结果引用、command request 终态、Context 中的 command result 单元、conversation reply sequence，以及可选 CHAT SendIntent。Delivery 随后执行。崩溃发生在事务前则按同一 operation 重试；事务后丢 ACK 读取既有结果；交给平台后的不确定性继续记录 UNKNOWN。

拒绝、输入 schema 错误和插件故障只产生有界、安全的错误回复，不回显异常正文、权限策略或内部 component ID。消息本身仍保持用户作者；Bot 回复使用 Bot principal，不能把命令发言者标成回复作者。

## 9. 生命周期和容量

每个 Bot 设置命令注册上限、每 Binding 策略上限、参数字节上限、待执行请求上限、每主体令牌桶、全局 P2 槽和单调用 deadline。容量拒绝发生在 raw payload 已经由现有接入预算允许后，仍要写 metadata 分类结果以免制造 sequence 空洞。

组件 disable/reload 顺序为：冻结命令 head → 停止新命令 admission → 排空或持久终结已接纳调用 → 处理 reply/outbox 对账 → 切换 generation → 恢复 head。Core/Shell 救援容量独立，不等待业务命令排空才接纳取消和状态查询。

## 10. 实施切片与验收

实现按以下顺序提交，每一步必须可执行：

1. **已完成：** manifest/AST/SDK 声明与 Worker 全字段对照，尚不在平台公开命令；
2. **已完成：** PostgreSQL registry、冲突检查、generation 切换和宿主内部 retirement 原语；
3. Web policy preview/apply、表单和来源 payload；
4. OneBot 接入分类、持久 request、遗忘终结与重放合流；
5. Actor P2 调度、CommandEvent、Reply/Consumed/Pass 和原子 Delivery；
6. Web 诊断、限额、恢复、升级和端到端验收。

最低验证包括：

- 静态构建不导入插件，动态参数有行列，manifest/runtime 任一字段不一致拒绝；
- 同名 owner 并发发布只有一个成功，旧代在 route 切换后不能提交；
- Shell 保留前缀永不进入业务 Worker；引用、转发、机器人 echo 和模型文本不执行命令；
- 普通、拒绝、Consumed、Pass、异常和重载中的命令消息都进入 Context 且保留真实作者；
- 同群命令与 Agent 不并发响应，跨群能并行，持续 P2 流量不能饿死 P3；
- policy 保存/撤销和 handler 执行竞争按 revision 收敛，未知权限默认拒绝；
- reply 事务前后故障、发送 UNKNOWN、重复事件和重启恢复不产生重复已确认回复；
- 遗忘与迟到重放不能恢复参数正文；
- 真实 PostgreSQL 多进程竞争、fake Worker/Adapter 故障注入、浏览器策略闭环及 OneBot 低频许可群验收分别记录，不能互相替代。
