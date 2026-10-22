# Forest 设计文档

本目录记录 Rust 新主线设计与实现边界。当前已实现调度核心、基础 SDK、静态 ACL、配置及 OneBot 接入。实际启动、ABI 和未覆盖范围见[运行与 SDK](13-runtime-and-sdk.md)。

| 文档 | 状态 |
|---|---|
| [序列化](serialization.md) | 已实现 |
| [模块运行时架构](01-module-runtime.md) | 核心已实现 |
| [状态、访问与重载](02-state-access-and-reload.md) | 已实现，声明变更策略仍受限 |
| [Token 与通配符](03-token.md) | 路径及宿主来源绑定已实现 |
| [OneBot API 与宿主接入](04-onebot-v11.md) | HTTP/WS 和群消息已接入 |
| [配置表](05-config-table.md) | SQLite 与受限入口已实现 |
| [静态 ACL](06-config-access.md) | 声明及宿主检查已接入 |
| [外部 Manifest](07-module-manifest.md) | 类型及加载器已实现 |
| [宿主接口](08-host-interfaces.md) | 基础 SDK 与导入已实现 |
| [模块调度](09-scheduling.md) | 独占推进、两种让出及重入已实现 |
| [生命周期](10-module-lifecycle.md) | 三种卸载与资源交接已实现 |
| [notifier 与依赖](11-notifier-and-dependencies.md) | 静态转发及装卸计划已实现 |
| [状态机与所有权](12-scheduler-state-machine.md) | 已实现，关键交错经真实 Wasm 验证 |
| [运行与 SDK](13-runtime-and-sdk.md) | 实际命令、ABI、范围及验证 |
| [宿主调试台与模块管理](14-inspection-webui.md) | Vue/TypeScript、Inspector、SSE、依赖图、配置编辑、滚动日志与受限本地包管理已实现 |
| [多主干多模态记忆模型](15-memory-model.md) | Memory 独立包已实现；多主干、归档集合、媒体引用、跨主干召回与联想 |
| [Qwen VL 与记忆设施](16-memory-infrastructure.md) | 模块私有 SQLite 与 Qdrant 协议已接入；原文与派生向量分离 |
| [向量化 Wasm 模块](17-embedding-module.md) | 已实现；JSON 浮点载荷、热重载、真实图文批量调用已验证 |
| [OpenAI 兼容 Provider](18-openai-provider.md) | 已实现；模块配置模型与连接、图片能力声明、原生 function 工具及完整响应 |
| [上下文格式化模块](19-context-module.md) | 已实现；单一 format 入口，可配置 Agent 提示词，原生角色、CQ/图片及热重载经实际 Wasm 验证 |
| [Agent 状态与窗口裁剪](20-agent-state-and-window.md) | 独立 Agent 包已实现；每群串行工作、持续点、高低水位裁剪及静默恢复 |
| [Agent、Memory 与数据库边界](21-agent-storage-boundary.md) | 已实现；Wasm 持有数据模型，宿主按模块身份执行通用原子 SQLite 操作 |
| [Agent 实现与启动](22-agent-implementation.md) | 包构建、配置、工具路由、启动命令及实际验证范围 |
| [DeepSeek 网页工具](23-deepseek-web.md) | 独立 Wasm 搜索/抓取插件，默认联网工具与原始 HTTP 分工 |
| [Wolfram Alpha 工具](24-wolfram-alpha.md) | 独立 Wasm 计算插件、连接配置与工具网关路由 |
| [文档渲染与受管媒体](25-document-rendering-proposal.md) | 独立 Typst/Media Wasm 包、PNG 引用、CQ 发送及媒体持久化 |
| [群聊 Shell 与学校帮助](26-group-shell-and-school-help.md) | 授权管理 Wasm 包、共享宿主管理端口与 NPUCraft 帮助回复 |

文档中的“待定”表示尚未形成决定。编写对应实现前应继续讨论，不能把示例名称或先前提案直接固定为接口。

当前采用宿主托管状态表，以及“提供方在配置或接口上挂静态 AccessPolicy＋调度器绑定真实来源＋宿主检查执行”的方向。配置读写使用 readers/writers，接口 send/call 使用 callers，复用同一套 ACL 查询，不建立动态 SQL ACL 表。此前讨论的强制 `snapshot` 交接、完整委派令牌树及自动重载超时均未采用。

当前范围收敛到单个 Bot 账号，同时处理多个群聊。OneBot 事件沿用 v11 原始结构，省略无关字段，不为多账号预设实体目录、路由与配置挂载。

模块配置接口先采用独立、原子的单项 `get/set/reset`，冲突处理由模块决定；不引入事务内模块回调、批量操作、版本、CAS 或自动重试。现有原生配置库的额外 API 不自动成为 Wasm 接口。

同一 Wasm 实例一次只推进一个任务。时间片被动让出保留执行权，模块主动返回 Pending 才释放；接口明确声明 reentrant，不设置数值并发上限。不可重入接口在任务等待期间仍保持占用，主动让出也不代表热重载排空完成。

状态机以所有权表达约束：当前推进、接口许可、请求额度和返回出口只移动不复制；ReadyQueue 合并重复唤醒，结束任务不保留可变终态。poll 返回之后的 ABI 收尾仍属于独占推进；原生本地操作终结后才完成旧实例的资源交接。

外部卸载/重载可明确选择 Drain、AtYield 或 Force，不自动超时升级。create/destroy 均禁止 send/call，包括宿主原生服务对象；destroy 另限于配置与基础清理。Force/trap 跳过全部来宾析构，由宿主回收资源。宿主定义事件，Manifest 只声明接收入口，无运行时取消订阅。

群消息等事件由宿主接入能力产生，调度器 notifier 查静态注册表投递普通请求，逐目标接纳结果由宿主处理。模块没有 events 字段或 notify 发布接口。模块加载依赖由 uses 的模块目标确定，subscriptions 只核对宿主事件支持，不产生插件生产者依赖。

旧 Python 实现及其设计文档保存在 `archive/python-2026-10-19` 等归档分支中，不作为新主线的设计依据。
