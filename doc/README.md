# Forest 设计文档

要操作当前 WebUI，请先看[中文操作指南](user-guide.md)；本页主要面向设计和开发。

文档中的账号、群号、管理入口、SSH 身份与机器路径使用占位符，示例必须按实际部署填写。验收记录中的不同群使用可区分的来源代号，保留跨群关系和验证结论；不提交真实平台身份、管理员凭据或私有主机地址。

状态：目标设计与激进重构进行中。更新日期：2026-10-19。[00 架构总契约](00-architecture-contract.md)是当前实现的唯一入口和最高权威；01–23 只展开目标领域契约，禁止夹带“已落地”式完成度叙述。[实现状态与验证记录](implementation-status.md)是代码完成度、测试证据和暂时缺口的唯一权威。旧 P0–P5 纵向切片已经从工作树删除，避免旧 Store、组合根、固定后台和开发期迁移设计继续充当实现输入；历史只通过 Git 追查。项目根 [README](../README.md) 只提供当前架构入口和开发检查；当前干净单 Bot 的实际部署方式见[生产启动运行手册](operations-clean-production-launch.md)。

Forest 是面向多 Bot 实体、长期记忆和自主 Agent 的重型机器人框架。采用稳定微内核与原生 Python 插件，运行实例按代际管理；一个 Bot 拥有一个全局记忆域，不按群建立记忆库，群聊之间可以异步并行。Wasm 为可选扩展，不是开发或运行前提。

## 文档权威顺序

后续实现必须先读本节。文档冲突时按表中从上到下裁决；代码现状和历史迁移记录不能反向修改目标语义。

| 优先级 | 文档 | 约束范围 |
|---|---|---|
| 0 | [00 架构总契约](00-architecture-contract.md) | 当前对象模型、微内核边界、权威执行链、模块控制台、激进重构规则和实现门禁；直接否决冲突设计 |
| 1 | [21 原生 assistant 与 thinking](21-native-assistant-and-thinking.md) | 替换旧输出 XML 线协议；原生 tools、厂商推理能力与私有续轮回放 |
| 1 | [22 OneBot CQ 呈现](22-onebot-cq-presentation.md) | 平台正文格式、单次转义、受管图片引用与冻结发送字节；保留普通 text 的字面语义 |
| 1 | [13 微内核运行时与控制面重构](13-microkernel-control-plane-redesign.md)、[14 干净数据库基线](14-clean-schema-and-payload-provenance.md)、[15 单 Agent 生产执行](15-single-agent-production-execution.md)、[16 持久语义召回](16-semantic-recall-execution.md)、[17 组件 Worker Host](17-component-worker-host.md)、[18 Host 存储](18-host-storage.md)、[19 Bot 官方包选择](19-bot-official-package-selection.md)、[20 跨范围驱动联合换代](20-cross-scope-driver-hot-reload.md) | 00 的运行时、控制面、模块数据所有权、schema bundle、生产 Agent、Host 存储、官方包选择与跨图替换边界详细化 |
| 2 | [01 架构设计](01-architecture.md) | 系统边界、模块职责、部署与总体验收不变量 |
| 3 | [02 生命周期](02-kernel-lifecycle.md)、[03 通信与调度](03-communication-scheduling.md)、[05 存储与鉴权](05-storage-security-operations.md)、[06 并发合流](06-concurrent-convergence.md) | 通用协议、所有权、事务、授权与收敛规则 |
| 4 | [08 群聊上下文](08-group-context-interaction.md)、[09 群聊 Shell](09-group-shell-command-system.md)、[10 多主干记忆图](10-multi-trunk-memory-graph.md)、[11 多模态记忆](11-multimodal-memory-and-model-projection.md)、[12 WebUI](12-webui-administration.md) | 交互、管理和数据域专项契约；不得扩大微内核职责 |
| 5 | [04 多 Bot、Agent 与记忆](04-bot-agent-memory.md)、[07 Python 插件](07-python-plugin-development.md) | 可替换模块的详细行为；04 包含 CorePrompt 规范正文，Agent 仅是模块，不能成为框架主循环 |
| 状态证据 | [实现状态与验证记录](implementation-status.md) | 唯一的当前完成度来源；不定义目标架构 |

任何实现若需要新增顶层 Agent/Memory/Bot 业务页面、恢复已废弃的 `BindingRuntime` 组合根、在通用 Supervisor 上持有业务对象、引入产品子 Agent、按旧未发布表增加兼容迁移，或绕过注册表直接调用模块，均应视为设计错误并先修改方案。

本轮语义纠偏已经把早期的“Bot/Agent 中心后台”改为模块平级的微内核工作台。后续任务不得只凭类名或现有页面继续补功能：先在 00 的废止清单中排除旧方案，再以对应模块的 `owner / contract / lifecycle / management contribution / scale query / convergence` 设计门建立实现。尤其禁止把旧页面换皮后保留固定业务 API，或把无筛选消息表包装成“记忆管理”。

## 当前实现输入

Agent 工具扩展采用 [24 工具网关与外部插件](24-agent-tool-gateway-and-external-plugins.md)：工具通过冻结目录接入，执行与只读查询分离，DeepSeek、HTTP 和 OneBot 工具属于外部包。安装和各包的设置入口见 [plugins 使用说明](../plugins/README.md)。

编写任何模块前，必须先从 00、13、14 确定 `owner / namespace / Repository contract / provides / requires / UnitOfWork participants / Generation fence / management contribution / deletion behavior`；涉及会话 Agent 执行时还必须读取 15。缺少任一项就先补设计，不能从旧实现反推边界。

00–23 定义目标契约，`research-*` 提供只读调研证据，`implementation-status.md` 记录完成度；操作指南、运行手册与验收记录说明具体操作和证据，不定义目标架构。新增的[23 触发策略控制页与群发现](23-trigger-policy-group-selection.md)约束模块自己的群选择 UI 和 Adapter 只读发现服务。旧 P0–P5 切片不再保留副本；其中仍有价值的算法必须先重新写入对应目标文档，才能进入实现。撤销的产品子 Agent、开发期 payload 迁移、旧库升级、固定业务后台和组合根不得从 Git 历史恢复。

## 需求对应

| 用户要求 | 设计落点 |
|---|---|
| 松散微内核与严格生命周期 | 01 的内核边界、02 的状态机/资源归属/代际切换 |
| 严格通信策略 | 03 的信封、调用、事件、流、背压与外部效果账本 |
| 多 Bot 与各 Bot 全局记忆 | 04 的实体/绑定/会话划分及 Bot 存储域，05 的独立存储配置 |
| 不同群异步对话 | 03 的接收序号和公平调度，04 的独立 Actor/Run 与 continuation |
| 异步并发写的合流性 | 06 的事实集合与确定性归约；不可合流操作显式仲裁 |
| 参考 AliceBot 的开发便利性，无需绑定 Wasm | 07 的 Python 插件/装饰器/异步钩子/watch，框架托管 IPC 与资源 |
| Agentlib 的单 Agent 核心协议 | 04 的内置 CorePrompt 规范正文、独立 PresetContext、原子 ModelTurn、单工具循环和跨重启 checkpoint；08 定义消息安全边界 |
| 所有群消息进入上下文，按最后触发记录推进下一轮 | 08 的持久输入流水、触发记录、模型安全边界与静默排空；03–05 同步该契约 |
| 历史正确区分不同用户 | 08 的结构化作者快照、引用来源与归档证据归属，禁止多人批次混合授权 |
| 管理员通过群聊管理框架并处理指令优先级 | 09 的 ShellGate、命令注册与授权、P0–P4 调度、CONTROL 回执；02–05 同步生命周期与事务边界 |
| 全局图中每个群聊一条主干，联想跨主干 | 10 的群来源注册、稳定来源单元、TRUNK_NEXT/ASSOCIATES_WITH；04–06同步图模型与并发协议 |
| 召回明确指明来自哪个群聊 | 10 的逐项Evidence来源与Provider实际渲染；原始来源群和当前使用群分别标明 |
| 移除裁剪时的记忆摘要以控制成本 | 08 的确定性token预算、可靠归档、原文/来源引用视图；裁剪LLM调用为零，索引独立批量执行 |
| 记忆与嵌入完整支持文本和图片，并能按图召回 | 11 的有序ContentPart、受管媒体、对齐嵌入空间与text/image/mixed query；04–06、10同步来源与索引协议 |
| LLM不支持多模态时调用API前去掉图片 | 11 的模型能力快照及逐请求ModelInputProjection；08区分持久裁剪和请求投影，保留原图及图片检索能力 |
| 通过WebUI后台管理，无需手改日常配置文件 | 12 的首次初始化、作用域选择、模块贡献 UI、schema表单、配置变更与运维闭环；05定义存储权威，WebUI/Shell/CLI共用控制协议 |
| 激进重构且不保留未发布兼容层 | 13 的重构拒绝清单与 14 的最小 bootstrap、独立模块 schema bundle、统一载荷来源闭包 |

## 源码调研依据

| 项目 | 调研记录 | 主要价值 |
|---|---|---|
| `../synthase` | [synthase 调研](research-synthase.md) | Wasm 执行、effect/ability 边界、包元数据、热加载与 principal 的实际完成度 |
| `../RAG`，QiYu | [QiYu 调研](research-qiyu.md)、[Agentlib 专项审计](research-qiyu-agentlib.md) | 单 Agent 状态机、CorePrompt/ModelTurn、上下文原子提交、复杂记忆、会话 Actor、工具确认与持久任务 |
| `../AliceBot Plus` | [AliceBot 调研](research-alicebot.md) | QQ 接入、插件分发、上下文、工具、渲染与动态加载的完整链路 |

源码调研以当前本地代码为证据，文件路径和行号用于回查。未启动源项目或连接真实平台；Agentlib 专项审计运行了 68 个不依赖缺失可选包的本地核心测试，其余调研仍是静态核对。“代码中存在”不等同于通过运行验证。源项目变化后应重新确认行号。新设计不是对三个项目代码的直接拼接。

## 约束与术语

本设计中的“必须”“禁止”是实现和验收要求；“默认”是可调整的初始配置；“后续”是不纳入首个实现阶段的扩展。容量和超时数字是待压测的设计初值，不是已测得的能力。

| 名称 | 定义 |
|---|---|
| Bot 作用域 | 独立身份、资源、授权、预算与全局记忆的数据作用域，以 `bot_id` 标识；不是持有模块对象的运行时聚合 |
| Binding | Bot 与平台账号/连接的绑定；重连不产生新 Bot |
| Conversation | 一个 Bot 在某平台频道/群/私聊/线程中的会话执行域 |
| ModulePackage | 有版本和内容摘要的不可变分发包 |
| Component | 包声明的服务、订阅处理器或受监督后台工作组件 |
| Instance | 某组件在确定作用域内的一次运行实例 |
| Generation | 同一组件运行版本的唯一代际，旧代退出后不可复用 |
| Epoch | 持久化单调递增的所有权编号，用于拒绝失效执行者的写入 |
| ConversationSession | 一个群当前唯一的 Agent 交互执行所有权，可跨多个串行 Run；等待不释放该所有权 |
| Run | 一次 Agent 执行，可跨多个模型/工具步骤并在安全边界接收新 InputBatch；输入批次分别冻结 |
| InputBatch | 在安全边界选定的消息前缀与触发集合；推进上下文不等于触发已被回应 |
| Effect | 模块请求宿主执行的受控操作，包括通信、存储与外部副作用 |
| 全局记忆 | 同一 Bot 在多个会话积累的统一长期记忆；全局不是全体 Bot 共享 |
| 群来源 / 主干 | Bot域内稳定群身份及其原始来源序列；群内线程/连接变化不新建主干，群主干不构成独立存储库 |

## 优先保证的系统不变量

1. `bot_id` 由受信绑定和执行上下文确定，插件与模型不能自行选择另一个 Bot 的身份。
2. 每个 Bot 的长期记忆归属固定的 `MemoryStoreProfile`；`conversation_id` 只描述来源、上下文与访问条件，不决定记忆库。
3. 同群只有一个活跃 ConversationSession 和一条前台 Agent 调用链；等待工具、确认或重载仍保留逻辑所有权，不同群并发且不共用 Bot 级推理锁。
4. Agent CorePrompt 是宿主内置、版本化且不可配置的协议；Bot 人格只进入不可裁剪、不可记忆化的 PresetContext。模型使用原生 assistant/tool_calls，每个响应构造一个原子 ModelTurn，至多包含一个 FunctionCall；推理续轮字段按 [21 原生协议](21-native-assistant-and-thinking.md)保存与回传。Forest 不创建产品子 Agent。
5. 模块之间只通过版本化契约通信，不持有彼此实例、数据库连接或平台连接。
6. 路由切换、取消和租约失效后，旧执行者不能提交新的业务事实；已提交的外部意图按账本完成或对账。
7. 热重载以新实例替换旧实例，失败保留可用版本；不尝试迁移 Python 栈、协程或 Wasm 调用栈。
8. 业务事实与 outbox 在同一事务提交；外部平台结果不确定时明确记录 `UNKNOWN`，不承诺端到端 exactly-once。
9. 每个任务、订阅、计时器、连接和临时文件有所有者、限额及终止路径。
10. 授权在每次 effect 执行和持久提交时校验，模块声明能力不等于获得能力。
11. 群聊正文和模型输出不能自行授予宿主权限；确定性 Shell 入口只把可信发言者的显式命令转换为经当前权限校验的控制请求。
12. 相同有效操作集与相同归约版本产生相同事实投影语义；LLM 工件的字节结果、配置竞争和外部副作用不冒充可合流状态。
13. 所有已接纳群消息都有上下文归属与作者信息，非触发消息同样参与裁剪；每个模型安全边界有新触发时，只推进到已连续完成分类的队列中最后一个触发记录（包含该条），保留其后普通尾部。
14. 没有新触发且 Agent 已结束时，剩余消息静默排空并裁剪，不额外生成回复；输入游标与触发处理状态分别持久化，结束/入站竞态不能丢失唤醒。

15. 管理 Shell 不等待 Agent 或普通消息分类缺口；控制执行优先级不改变历史顺序、不绕过资源租约与最终授权。CONTROL 回执与 CHAT 回复分开排序，正常暂停保留受权控制入口。

16. 一个Bot全局图内每群一条主干；顺序边只在同主干，联想边可跨主干。召回必须在实际模型输入中区分来源群、当前群及逐条作者，多源内容不能覆盖成最后一个群来源。
17. 裁剪只进行可靠归档、确定性预算选择与来源引用维护，不生成记忆摘要、不调用LLM；完整持久历史与有界模型视图分别记录，未答触发及工具因果组不可被静默遗漏。

18. 完整上下文、归档和记忆保留有序文本/图片及各自来源；嵌入配置必须支持原生图片输入和查询，不用图片说明代替图像向量。
19. 每次LLM请求按实际模型能力生成独立投影视图；文本模型请求移除所有图片载荷并标记未见图片，不删除持久媒体、不改变输入切口，也不隐式调用OCR/描述模型。
20. WebUI是首版正式管理入口，日常配置持久化为受版本控制的事实，通过界面完成；页面、群聊Shell和CLI不直接改文件或运行对象，提交均走同一授权、版本冲突和生命周期协议。
21. 配置期望、生效版本与实际运行状态分别可见；保存成功不冒充已生效，断开浏览器不取消持久操作，冲突不以最后保存覆盖，密钥不通过配置读取接口回显。
22. 不可绕过的 `forest_kernel` bootstrap 只包含 Bot 身份与 schema bundle 账本；Runtime、Payload、Authorization、Effect、Audit 是拥有独立 bundle/Repository/管理贡献的 kernel 模块，不能把它们和 Agent、Memory、Adapter、Shell、Job、Delivery 压成巨型内核表。开发期旧库只重建、不迁移。
23. WebUI 壳层不固定 Bot/Agent/Memory/安全/存储产品栏目；当前作用域内的导航、配置、查询与动作由准确模块 Generation 的管理贡献生成，内核自身也遵循该协议。

出现跨文档冲突时，先维护这些不变量，再修改具体契约；不得由实现自行弱化并仍宣称满足设计。
