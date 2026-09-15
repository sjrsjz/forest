# 原生 Python 插件与开发体验详细设计

> 状态：目标 SDK 与工具链设计，部分已实现。当前可运行 `Plugin`、`@plugin`、`@service`、`@subscribe`、`@job`、state/tasks/resources/operations/events SDK、不可变单组件包、独立 Worker，以及 `forest plugin build/check`、单次 `forest dev` 和自动 watch。缺少显式 `module.toml` 时，构建器已从字面量装饰器和 `pyproject.toml` 静态生成清单、内联 schema 与 `checksums.json`。类方法 `@command` 的静态生成、纯命令包接纳和候选 Worker 全字段核验已经实现，但尚未接入持久 registry、平台分类、同群 Actor 或回复提交。DTO schema/适配入口生成及记忆/回复/通用服务代理仍是目标接口；示例见 [counter](../examples/counter/python/counter.py) 与 [auto_echo](../examples/auto_echo/src/auto_echo/__init__.py)，用法与边界见[实现状态](implementation-status.md)及[项目 README](../README.md#原生插件开发切片)。以下目标契约不因当前实现范围缩减。

## 1. 插件作者看到的接口

Forest 默认直接运行普通 Python 包。作者编写模块、类和 `async handle`，通过 Context 调用记忆、回复、任务等服务；框架生成声明、启动 Worker、处理 IPC 与代际替换。无需把 Python 编译成 Wasm，也无需编写 Rust、二进制 ABI 或 Worker 协议。

这一体验参考 AliceBot 的“放入插件、声明处理器、修改代码后重载”，可靠性按[生命周期设计](02-kernel-lifecycle.md)执行。Wasm 仅是未来可选 RuntimeDriver，不是 SDK 使用条件或首版核心里程碑。

每个组件默认 `scope="bot"`，同一包可以被多个 Bot 启用。一个 Bot 的同一组件实例服务多个会话；类实例不是群级单例，`self` 与模块全局变量不能存放“当前群”“当前用户”或可变 Prompt。

## 2. 最小插件

```text
plugins/hello/
  pyproject.toml
  src/hello/__init__.py
  src/hello/helpers.py
  tests/
```

以下示例展示拟定作者接口，`Reply` 为值对象，返回它不会在 Worker 内直接发送网络消息：

```python
from forest.sdk import PluginContext, Reply, command, plugin
from .helpers import format_greeting


@plugin(id="example.hello", version="0.1.0", scope="bot",
        requests=["reply.propose"])
class Hello:
    async def on_load(self, ctx: PluginContext):
        self.prefix = ctx.config.get("prefix", "你好")

    @command("/hello", permission="command.hello")
    async def handle(self, event, ctx):
        text = format_greeting(self.prefix, event.sender.display_name)
        return Reply.text(text)

    async def on_unload(self, ctx):
        ctx.logger.info("hello component released")
```

`/hello` 注册为终结处理器，按[通信与调度设计](03-communication-scheduling.md)获得当前会话的执行上下文。SDK 把返回值规范化为 `ReplyProposal`，在轮次提交事务中写入回复意图；Delivery 再负责实际发送。

handler 可返回 `Reply`、`Pass` 或 `Consumed`。`Pass` 表示不匹配，`Consumed` 表示已完成且无需默认回复；两者都不能阻止这条正常群消息进入历史。抛出的异常进入统一错误契约。命令路由冲突在启用时检查，不按文件枚举顺序偶然挑选一个处理器。

以上是 P2 业务命令接口；持久注册、用户策略、分类、同群 Actor 与原子回复的实现协议见 [P2 业务命令与普通消息插件链](p2-business-commands.md)。`#forest` 由核心 ShellGate 保留，普通 handler 无论 priority 多高都不能覆盖或提前消费它；命中保留命名空间后的错误、拒绝和 `Pass` 均不能回落到 Agent。业务命令的 `handler.priority` 只决定允许阶段内的匹配顺序，不等于宿主 `service_class`。管理扩展通过[09 的版本化 CommandSpec](09-group-shell-command-system.md)登记到显式扩展命名空间，接受部署授权和固定参数 schema，默认 P1，不能自行申请成为核心 P0 救援命令。

群消息上下文收集由框架统一完成，不要求每个插件重复 push 用户消息。触发分类只产生结构化决策与原始事件引用，不能直接 `Agent.run()` 或自行 `create_task` 开启回复；待触发由唯一 ConversationSession 在模型安全点按最后触发记录选择前缀。没有触发也照常静默保存/裁剪，详见[08](08-group-context-interaction.md)。命令插件若需要长期模型对话，同样申请该会话的调度资格。

当前首版实现只接纳 `@plugin` 类中的 `@command` 方法；只定义 handler 的类无需继承 `Plugin` 或实现生命周期方法，装饰器会提供默认生命周期。模块级函数适配层仍待实现，在此之前构建器不会把模块级装饰器生成成不可执行清单。

## 3. 声明生成与授权

装饰器使用可静态解析的字面量元数据。当前打包器在源码没有显式 `module.toml` 时读取 AST 与 `pyproject.toml` 的 `tool.forest.module-root`，生成 `module.toml`、装饰器内联的契约 schema 及覆盖工件全部文件的 `checksums.json`；字符串 schema 路径继续引用包内文件。生成只发生在捕获快照中，不修改源码，也不导入插件。DTO schema 与 SDK 适配入口生成仍是目标能力。

| 简易声明 | 生成或补全的内容 |
|---|---|
| `@plugin(id, version, scope)` | package/component 身份、`runtime=python-worker`、作用域 |
| `@command(name, permission)` | 命令匹配、输入 schema、终结处理器、用户动作权限 |
| handler 的 DTO 类型 | 契约 major/minor、输入输出 schema 与摘要 |
| `requests=[...]` | 组件申请的能力；不自动授予 |
| `@service(name, contract, input_schema, output_schema)` | 服务身份、精确契约及包内路径或内联 JSON Schema；缺省 schema 为开放 schema `{}` |
| `@subscribe(subscriber_id, topic, contract, schema, max_attempts)` | 稳定 subscriber ID、精确事件契约、包内路径或内联 schema 及重试次数 |
| `@job(handler_id, input_schema, max_attempts, timeout_s, max_concurrency)` | 持久任务身份、输入 schema、尝试预算、单次期限与并发上限 |
| SDK/项目配置默认值 | API 兼容范围、资源配额、生命周期超时与 `reload_mode=drain` |

动态拼接的装饰器参数无法静态求值时，构建失败并指出位置；作者可改用显式 manifest。正式产物必须包含 02 中要求的完整声明，便利接口不能省略状态 schema、必需依赖或权限边界。

权限分两层：包声明自己需要调用 `reply.propose`；命令还检查发言者是否获准 `command.hello`。部署政策可以将该命令开放给普通成员，但未知动作默认拒绝。角色由可信平台身份/管理配置解析，不能由消息里的“我是管理员”或模型输出生成。

候选导入、装饰器注册和 `on_load` 不得申请更高权限。运行时实际能力始终是声明、部署授予、Bot 策略和当前请求身份的交集；SDK 的本地快速检查不能替代宿主最终执行检查。

### 3.1 插件配置与后台表单

插件随包提供版本化配置schema，SDK可从静态配置声明生成它。字段说明类型、默认值、范围/枚举、是否可继承、`reload_policy`和敏感性；可附受限的标题、帮助文本、分组和控件提示。WebUI据同一schema提供表单与继承来源展示，服务端完成最终验证，插件作者无需另写一套后台。复杂字段可使用经schema校验的高级结构编辑，不能退回要求管理员登录服务器改文件。

普通设置值以Bot/组件作用域的配置修订保存；密码、API Key等通过专用写入接口转为用途受限的secret_ref，表单读取只显示已配置状态和可见版本，不回显秘密。schema和帮助内容不得包含可执行脚本、任意HTML或读取本地文件的钩子；缺失/不兼容schema时展示诊断并阻止不安全编辑，不能默默丢弃未知字段后覆盖。

默认需要在on_load读取或缓存的字段声明generation，例如最小插件中的prefix；标记dynamic的字段必须按框架提供的请求快照读取，默认对后续Run/任务生效。包代码watch只提交新包候选，不监视导出配置文件后覆盖后台已保存修订；配置文件导入也经同一差异预览、expected_revision与管理API。开发自动重载策略继续有效，普通设置保存不因此增加一律人工审批。完整配置流程见[12](12-webui-administration.md)。

## 4. Context 服务与数据边界

| 作者接口 | 契约语义 |
|---|---|
| `ctx.config`、`ctx.principal`、`ctx.reply_route` | 单事件命令的只读配置、可信请求主体和固定回复目标；多人Agent批次不使用一个principal代表所有人 |
| `ctx.input_batch`、`ctx.triggers` | 模型安全点冻结的输入版本、逐消息作者及逐触发授权引用；只读，不能自行推入尾部 |
| `ctx.services.call(...)` | 版本化 request/reply，经 Router 绑定代际、权限与 deadline |
| `ctx.memory.recall(query_parts=...)` | 接受合法text/image parts和受管媒体引用，读取Bot全局图文记忆，返回保留图片与逐项来源的EvidenceBundle；文本query简写生成text part |
| `event.parts`、`ctx.media` | 按来源顺序访问ContentPart，申请获权媒体状态/读取或物化；SDK不把临时URL当长期图片身份 |
| `ctx.memory.append(...)` | 提交显式断言/关系操作，幂等接纳并返回持久回执 |
| `ctx.turn.commit(...)` | 提交 Context、最终回复与可选记忆操作的同 Bot 原子事务 |
| `ctx.events.publish(topic, payload, contract=..., operation_id=...)` | 当前调用内持久发布领域事实与逐订阅 delivery；临时进度使用另行声明的通道 |
| `ctx.tasks.spawn_owned(...)` | 登记受监督任务，继承取消、deadline 和资源额度 |
| `ctx.jobs.schedule(...)` | 持久长任务/定时任务，具有固定主体、预算和目标范围 |
| `ctx.resources`、`ctx.state` | 登记调用/代际临时资源并按 LIFO 关闭；受版本与 scope 约束的小型状态接口 |
| `ctx.operations.stable(step)` | 从持久逻辑执行身份派生幂等操作 ID，重试不改变 |

这些接口是宿主服务的类型化代理，`ctx.memory` 不是共享 dict。作者不能从代理取得原始 WebSocket、数据库连接、凭据或另一个 Bot 的 Context。消息与媒体通过 DTO/MediaRef 编码，禁止 pickle 或传入可执行 Python 对象。

Context 只在当前调用及其受监督子调用中有效，不能存入 `self` 后交给下一条消息使用。跨服务只声明契约，不 `import other_plugin`；同包 `helpers.py` 可正常相对 import，因为它随整个包一起形成同一不可变版本。

持久事件的 payload 正文进入受来源与删除 epoch 约束的内容存储。消费 envelope 明确携带发布组件、原契约与 event ID；同一订阅串行、不同订阅可并行。发布幂等、ACK 丢失恢复、热重载接续和死信边界见 [P0 持久组件事件](p0-component-events.md)。

组件持久任务通过 manifest `jobs` 和代码 `@job(handler_id)` 成对声明；调用内使用 `ctx.jobs.schedule(handler_id, payload, operation_id=..., delay_seconds=...)`。它只调用同一组件当前代际的处理器，不创建 Agent、不读取群上下文，也不继承用户或管理员权限。处理器可用稳定 operation 调度下一周期，完整 claim、预算、重试与热重载协议见[组件持久任务详细设计](p0-component-jobs.md)。面向群会话唯一 Agent 的 interval/cron 任务仍使用 P3 会话任务协议。

Agent 的工具提议必须引用其服务的 trigger/request ID，宿主分别检查对应主体；同批多个用户的权限不能取并集。`event.sender` 来自平台认证后规范化的作者快照，回复引用、转发作者、正文中的名字与管理员称谓都不能替换它。业务插件返回回复只生成 CHAT 意图，最终外发按该群 response_seq 排序。管理扩展通过绑定 command_id 的控制回执接口产生 CONTROL 意图，宿主验证命令授权；普通 `Reply` 不能自行选择控制通道来跳过聊天顺序或配额。

handler 中直接 `asyncio.create_task` 后遗弃引用、跨请求修改模块级列表、创建未登记定时器都违反运行契约。必须跨热重载存活的工作提交为 Job/checkpoint，不能依靠后台协程自然留存。

## 5. 全局记忆与合流接口

下面的拟定命令明确请求把用户自述保存为本 Bot 可跨群召回的事实。发布者必须获得相应共享权限；没有共享权限时拒绝命令，不静默放宽来源可见性。

```python
from forest.sdk import Assert, Reply, command, plugin


@plugin(id="example.notes", version="0.1.0", scope="bot",
        requests=["memory.assert", "memory.share", "reply.propose"])
class Notes:
    @command("/记住", permission="command.notes.share", args="tail_text")
    async def handle(self, event, ctx):
        operation = Assert(
            subject_id=ctx.principal.subject_id,
            predicate="self_report.note",
            value=event.args.text,
            visibility="bot_shared",
        )
        receipt = await ctx.memory.append(
            operation,
            operation_id=ctx.operations.stable("remember:v1"),
            source=ctx.evidence.current_input(),
        )
        return Reply.text(f"已记录，操作编号：{receipt.operation_id}")
```

`bot_id`、来源事件、实际权限和执行代际由宿主绑定。`memory.share` 仍检查当前用户与来源是否允许提升为 `bot_shared`；仅修改 `visibility` 字符串不能绕过检查。两个群调用同一 Notes 组件时，操作写入同一个 Bot 事实域，Bot B 使用另一域。

上述示例先独立持久化记忆，再产生确认回复：若回复提交失败，记忆仍已记录；重试使用同一 operation ID 取得原回执。需要“记忆与回复意图同时成功”时使用 `ctx.turn.commit(memory_operations=..., reply=...)`，由服务在同一 Bot 事务执行并返回 `Committed(receipt)`；handler 透传该结果，SDK 不再次提交。作者不能自行跨两个 RPC 假装原子提交。

稳定 ID 绑定原始事件/持久 Run 与逻辑 step，不能包含重试次数、当前时间或 generation。算法改变 step 版本属于新逻辑操作，必须显式安排 replay/替代关系；热重载不能自动为历史事件生成另一组记忆。

| 操作/扩展 | 合流约束 |
|---|---|
| `Assert` | 稳定断言身份；不同值同时保留为候选/冲突，禁止最后写入覆盖 |
| `Retract(target_assertion_id)` | 追加针对固定 ID 的关系，撤回先到时也不丢弃 |
| `Resolve(expected_candidate_digest, ...)` | 受权命令与版本检查，明确承认顺序相关的仲裁 |
| `ctx.memory.recall` | 返回 candidates/conflict/evidence，不把冲突隐式压成单值 |
| 自定义 reducer | 固定版本、规范输入、纯计算；实现并验证交换、结合、幂等要求 |
| 显式非裁剪任务的LLM派生/抽取 | 独立授权/预算，固定source manifest与artifact slot，选择冻结后复核发布；不得由裁剪隐式触发 |

合流语义遵循[06 并发合流设计](06-concurrent-convergence.md)，记忆域与可见性遵循[04 Bot 与记忆设计](04-bot-agent-memory.md)。添加 `confluent=True` 标签、使用 CAS 或串行锁都不能证明 reducer 合流；SDK 必须提供排列、分批、重复与撤回先到的性质测试夹具。

Memory SDK 从可信事件/Context关联确定group_source_id、trunk_id与source_unit_id，插件不填写“当前群”覆盖证据原来源；跨群派生显式引用全部sources。Provider适配器必须调用统一Evidence渲染入口，实际输入保留来源群稳定ID/可见名称、作者及逐项证据引用，不能只取item.text或把来源放进随后丢弃的metadata。跨主干联想通过类型化操作提交，TRUNK_NEXT由框架按稳定位置维护，详见[10](10-multi-trunk-memory-graph.md)。

业务插件可以把本次固定InputBatch中获准使用的image part直接传给recall，不先调用LLM转写图片。工具含图片的返回值也声明typed ContentPart与来源，不把二进制或媒体URL藏进任意JSON/text字段。Embedding插件声明独立的原生图文输入/查询和对齐space契约；Provider插件声明实际部署及各角色的模态能力，不能用一个supports_images开关控制两者。

每次LLM调用由统一投影器根据真实能力处理完整历史、工具结果和Evidence：文本模型去图但保留合法文本/来源/工具配对，记录实际包含与省略清单；这些操作不回写event.parts或持久Context。未经声明的图片说明/OCR不是默认补偿钩子；插件也不能从原始媒体存在推断当前模型实际看到了它。详细SDK与请求记录契约见[11](11-multimodal-memory-and-model-projection.md)。

Context裁剪使用框架的确定性预算与Archive/ViewPlan接口，不存在默认`summarize_on_trim`回调；插件不能在裁剪钩子暗中发LLM请求。高流量群聊合并本地维护、批量写索引，活动视图移出原文不等待Embedding或模型服务。

## 6. 简单钩子与严格生命周期的映射

| 内核阶段 | SDK 默认处理 | 作者可选扩展 |
|---|---|---|
| `prepare` | 创建实例、注入只读组件 Context、验证配置 | 调 `on_load(ctx)`；只允许受监督资源和只读/隔离预热 |
| `start` | 注册待命 handler/订阅并执行健康检查 | `on_ready(ctx)`，仍无生产 admission |
| route commit / ACTIVE | 宿主签发 admission 后开放业务调用 | 无必须成功的业务激活回调 |
| `quiesce` | 停止接新工作与周期触发，排空现有调用树 | `on_quiesce(reason, deadline)`，配合安全 checkpoint |
| `snapshot` / restore | 使用版本化状态 DTO，验证 revision/checksum | 显式 `snapshot`/纯 `migrate`，不序列化 Python 栈 |
| 切换前 `resume` | 恢复旧代 admission 与受监督触发器 | `on_resume(ctx)`，失败则由监督器按 02 恢复 |
| `stop` | 保持无新 admission，停止后台任务，清理后退出 Worker | 调 `on_unload(ctx)`；高级接口可显式实现 `stop(reason)` 替代 |
| `dispose` | Worker 已退出，由宿主回收剩余资源并清空账本 | 不依赖 Worker 内钩子；外部资源由登记的宿主关闭器处理 |

简单插件仅实现 `on_load/on_unload` 即可；高级插件选择完整钩子时，SDK 拒绝同时声明同阶段的冲突方法。方法与默认行为的调用次序必须固定，不依赖继承链偶然覆盖。

`on_unload` 须幂等、限时且不能发业务副作用，返回不代表实例已经 DISPOSED。异常或进程被杀时它甚至可能没有执行；宿主仍依据资源所有者清理。导入模块顶层只定义类型、常量和声明，禁止开连接、启动任务、写正式状态或调用模型。

## 7. 原生 Python 加载与开发 watch

默认加载单位是不可变 Python 包快照，替换单位是受影响 Bot 的组件代际。首版同一包/Bot 放在一个 Worker 时，包内全部组件属于共同进程故障与替换集合；不能声称重载其中一类而保留同进程另一类的执行栈。

Python 的 reload 不会自动重绑定其他模块持有的旧对象引用，既有类实例也继续使用旧类定义。因此这里的代际替换需要新装载环境和路由交接，不能只调用 reload。相关行为见[Python importlib 官方文档](https://docs.python.org/3.11/library/importlib.html#importlib.reload)。

```text
文件变更 → 合并事件 → 稳定快照 → 完整摘要/manifest/compile 校验
        → 独立候选 Worker 导入/prepare/start/health
        → freeze/drain/snapshot/迁移验证 → 路由 CAS → publish → retire
```

1. watch 默认 debounce 300 ms，连续编辑设 2 秒候选调度上限；仅触发一次构建请求。写入尚未稳定时重试快照，不加载半个文件。
2. 捕获包内全部源码、helpers、模板、schema、资源与锁文件，规范路径并形成清单；读取前后校验变化，摘要不稳定就重新捕获。禁止从快照逃逸的符号链接和未声明资源路径。
3. AST/语法编译验证不执行插件代码；装饰器静态声明、契约、启动依赖、状态版本与权限计划一并检查。语法通过不能替代候选运行验证。
4. 外部依赖采用锁定版本及工件摘要，记录 Python 版本、平台和依赖环境摘要。复用经过校验的只读环境；依赖变化创建新环境，不能在活跃环境里 `pip install` 覆盖文件。
5. 候选从不可变快照启动全新解释器，限定搜索路径，不从正在编辑的工作区补 import。包内 helpers 和其传递依赖自然随新进程加载；不维护脆弱的 `sys.modules` 子串清理规则。
6. 候选仍无生产写权，在限时 prepare/health 期间验证导入与必需服务。随后按 02 完成 drain 和迁移；watch 本身无权发布路由。
7. 构建、导入、校验或迁移失败均保留旧版本服务，输出文件/行号与失败阶段。冻结后失败走受控 resume；不得先杀旧代来腾出候选内存。
8. 每次构建绑定 `build_request_id + package_digest`；切换前核对最新期望构建。排队旧候选可取消，已经提交路由的升级不擅自回退，后续编辑形成下一次操作。

同版本开发内容可以生成仅用于本地的派生版本，例如 `0.1.0+dev.<digest>`；正式发布继续拒绝同版本不同内容的静默替换。代码、配置和依赖变化分别记录，但都通过可审计变更计划进入运行状态。

## 8. 开发工具与运行模式

当前原生 Worker 工作流：

```text
forest plugin init ./plugins/hello --id example.hello
forest plugin check ./plugins/hello
forest dev --bot dev-alice --watch ./plugins/hello
forest plugin build ./plugins/hello
forest plugin lock ./plugins/hello --require "example-lib==1.2.3"
forest plugin export sha256:<digest> --output hello.zip
forest plugin import hello.zip
```

`check` 检查声明/编译/契约并在独立候选中验证；`dev` 单次模式可以调用合成输入，watch 模式持续管理 Worker 生命周期；`build` 输出工件路径和内容摘要，工件内包含完整 manifest、schema 与已声明的依赖锁。锁定依赖环境在 Worker 启动前校验并物化。CLI 隐藏 IPC 与 Worker 启动细节，不隐藏失败、权限拒绝或被降级的保证。

`dev --watch` 持续持有开发 Bot 的宿主租约，使用 `FOREST_BOT_DSN` 指定该 Bot 的独立 PostgreSQL 数据库；数据库不得同时交给另一开发进程或 Web 托管宿主。`--config-json` 默认 `{}`，`--grant` 可重复指定，例如 counter 示例需要 `--grant state.read --grant state.write`。第一次注册 watch 时冻结绝对源码路径、配置、初始 grants 和策略 revision；保存源码不会扩权，同一 watch 重启也不能偷偷改写这些授权。源码可以删掉或损坏，恢复仍先加载数据库记录的 last-good 工件并校验摘要，不能从工作区补文件；指定的 `--cache-dir` 必须保有该工件。

watch 按与构建器相同的有界全包捕获规则轮询，默认每 100 ms 检查一次；使用 300 ms debounce，持续编辑的候选调度等待上限为 2 s。运行中至多一个候选和一个被合并的待处理请求；已有候选尚在执行限时生命周期时，后续构建等待它退出，不能为满足调度间隔无限增开 Worker。每次观察到不同内容（包括无法捕获的坏路径）先在 Bot 数据库接受新的 `build_request_id`，之后才构建。新的期望请求立即使旧候选失去提交资格，语法错误不会恢复旧候选的资格。

开发控制使用当前本机宿主权限，独立记录 `bot_dev_watch_heads / requests / builds / activations / results`，不伪造 Web ticket。完整快照摘要与操作 ID 冻结后进入普通 `Kernel.install`；watch 无权写路由。`Kernel.publish` 同一事务中的持久触发器锁定 watch head，核对最新请求、组件身份、工件、配置、grants 和宿主，再更新 last-good。拿到最后的业务锁后复查宿主及操作到期时间，过期的候选只能在仍有当前宿主权威时终结并恢复旧代。请求、候选操作、提交结果均持久化；重启先恢复 last-good，再为尚未完成的工作区意图创建新请求，避免复用已 FAILED 或 ABANDONED 的生命周期操作。

`plugin init` 只接受新的目标路径，先在同父目录的临时树中生成并执行与 build 共用的双捕获静态校验，再原子发布源码目录；任何阶段失败或目标已存在都不会覆盖用户文件。CLI 输出逐行 JSON 的 accepted/building/validating/published/failed/restored 状态；语法失败提供文件和行列，插件 hook 失败不回显任意异常正文。Ctrl+C 会停止观察，等待被持有的文件捕获线程与候选收尾，再关闭 Worker、撤销宿主租约和数据库池。原有单次模式仍可用：`forest dev --bot dev-alice --package ./plugins/hello --service hello.echo --input-json '{"text":"hi"}'`。`inprocess-dev` 仍是预留工作流，当前 CLI 不提供。

`plugin lock` 只读取包内已提供的 wheel，验证当前 Python/平台对应的完整依赖闭包，然后原子写入 `dependencies.lock.json`；不会解析联网源或执行安装脚本。作者需在 manifest 顶层显式设置 `dependency_lock = "dependencies.lock.json"`。`plugin export/import` 按完整 ZIP 和内容摘要搬运工件，导入先完成边界及静态验证，同版本不同内容仍被拒绝。

预留的 `runtime=inprocess-dev` 仅面向可信开发代码，不具备独立内存/崩溃隔离，不能可靠强杀阻塞 handler，卸载只做尽力清理；复杂修改需受控重启整个开发宿主。仍禁止 `importlib.reload()` 原地改写活跃模块，不能将此模式标为生产热重载验收通过。

普通 Python 子进程不是恶意代码沙箱。默认插件信任来源；SDK/包检查提供契约约束，安全强制还需操作系统网络/文件/进程隔离。平台 Adapter、Provider 与存储网关由宿主管理必要特权，业务插件仍不获得这些原始连接。

## 9. 作者与框架各自的验收责任

- 作者验证 handler 输入/输出、权限需求、状态迁移、清理幂等及领域 reducer 性质；测试使用合成消息和假服务。
- SDK 夹具固定 Bot、Conversation、主体和事件 ID；同一处理逻辑可模拟跨群并发、重复投递、取消和 `STALE_EPOCH`，不能绕过服务端验证伪造成功。
- 框架验证修改 helpers 后切换完整包、新文件语法错误时旧代继续服务、候选导入无生产副作用、重复 watch 不发布过期候选。
- 框架验证跨 Bot 独立状态、旧代迟到写被拒、已提交回复意图继续对账、资源回到基线，以及长任务的 checkpoint 恢复。
- 框架验证普通群消息未被Pass/Consumed丢弃、多个触发在下一模型调用前只推进到最后触发记录、无触发静默裁剪且LLM调用为零、WAIT/RELOAD保持同群独占，并在归档/召回中正确保存逐条作者及来源群。
- SDK 的简单写法与显式完整 Component 必须通过同一份 02/03/04/06 契约测试；便利层不能形成另一条不受监督的运行路径。
