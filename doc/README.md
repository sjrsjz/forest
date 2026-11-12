# Forest 文档

本目录说明 Forest 的使用方式、模块契约和架构。构建入口见[项目 README](../README.md)；模块的机器可读契约位于各包的 `manifest.toml` 及其引用的 Forest `*.schema`、`*.value` 文本中。

## 使用与部署

| 文档 | 内容 |
| --- | --- |
| [运行时、SDK 与 Wasm ABI](13-runtime-and-sdk.md) | 启动方式、OneBot 接入、插件开发、热重载及 ABI |
| [WebUI 与模块管理](14-inspection-webui.md) | 登录、运行状态、声明依赖图、配置编辑、日志和模块装卸 |
| [Agent 使用指南](22-agent-guide.md) | 插件构建、模型连接、群启用、工具配置与运行流程 |
| [群聊 Shell](26-group-shell.md) | 群内管理命令、授权、配置读写与示例回复插件 |
| [Linux ARM64 部署](27-linux-deployment.md) | 交叉构建、编译预热、systemd 和运行目录 |

## 内核与模块开发

| 文档 | 内容 |
| --- | --- |
| [模块开发手册](module-development/README.md) | 完整示例、类型化接口、生命周期、并发、存储、宿主 API、打包与热重载 |
| [序列化](serialization.md) | Schema、Value、结构子类型、转换和编码 |
| [模块运行时架构](01-module-runtime.md) | 单例模块、消息通信、隔离及职责 |
| [状态、访问与重载](02-state-access-and-reload.md) | 托管状态、配置、ACL 与实例交接 |
| [来源令牌与路径匹配](03-token.md) | 身份路径、可信来源和通配符 |
| [OneBot v11](04-onebot-v11.md) | API 库、Wasm 接入包和消息契约 |
| [配置表](05-config-table.md) | 默认值、原子操作、SQLite 存储与导入导出 |
| [访问声明](06-config-access.md) | 配置读写、接口调用和静态 ACL |
| [模块 Manifest](07-module-manifest.md) | TOML 包格式、元信息、Forest 文件引用、SHA-256 及契约声明 |
| [宿主接口](08-host-interfaces.md) | 通信、配置、网络、文件、SQLite、日志和时间 |
| [模块调度](09-scheduling.md) | 执行权、时间片、异步等待、实例内宿主锁与队列 |
| [模块生命周期](10-module-lifecycle.md) | create/destroy、三种停止方式和故障结算 |
| [事件与依赖](11-notifier-and-dependencies.md) | 模块事件、实例 I/O 通知、静态接收声明和依赖规划 |
| [通用宿主与接入模块](36-generic-host-and-adapters.md) | 平台无关的内核、激活、I/O 期限、资源通知与 OneBot Wasm 包 |
| [调度器状态机](12-scheduler-state-machine.md) | 状态转换、所有权、不变量和资源交接 |
| [调度系统形式化模型](34-scheduler-formal-model.md) | 状态转换、调用祖先、可取消重载请求、可观测等待及循环依赖反例 |
| [调度器 Lean 验证](35-scheduler-lean-verification.md) | Store/ABI 协议的机器证明、反例、源码对应与可信边界 |

## Agent 与记忆

| 文档 | 内容 |
| --- | --- |
| [多主干多模态记忆](15-memory-model.md) | 原始单元、群主干、归档资格、联想与图扩散 |
| [记忆存储与索引设施](16-memory-infrastructure.md) | SQLite、受管媒体、Qdrant 和向量空间 |
| [多模态向量化](17-embedding-module.md) | 嵌入接口、固定模型、连接配置和浮点载荷 |
| [聊天模型 Provider](18-openai-provider.md) | Chat Completions、原生工具调用、思考字段和能力声明 |
| [上下文格式化](19-context-module.md) | 完整输入到标准 API 上下文、人格和来源标记 |
| [群会话与裁剪](20-agent-state-and-window.md) | 每群串行工作、持续点、高低水位和静默维护 |
| [Agent 与数据库边界](21-agent-storage-boundary.md) | 插件数据所有权、原子提交与恢复 |
| [模型输入图片投影](31-image-projection.md) | 聊天图片缩放、重排缩略图与派生缓存 |
| [多模态召回重排](32-multimodal-reranking.md) | 候选召回、文本/图片评分和种子选择 |

## 工具与消息输出

| 文档 | 内容 |
| --- | --- |
| [搜索与网页抓取](23-deepseek-web.md) | DeepSeek 搜索、抓取与原始 HTTP 工具的分工 |
| [计算工具](24-wolfram-alpha.md) | Wolfram Alpha LLM API 与连接配置 |
| [文档渲染与受管媒体](25-document-rendering.md) | Typst、Media、图片引用和群内 `/typst` 命令 |
| [Agent OneBot 工具](28-onebot-agent-tools.md) | 账号、好友、群成员查询及独立消息发送 |
| [AI 绘画](29-ai-paint.md) | 绘图后端、参数、受管图片和工具结果 |
| [群消息输出](30-message-output.md) | 分片发送、逐群长消息转发和实际回执 |
| [Markdown 消息与富文本](33-markdown-messages.md) | 专用链接、选择性排版、原文保留及协议投影 |

## 阅读约定

架构文档说明模块职责和运行时保证，使用指南说明构建、配置和操作。模块章节中的离线构建命令以完成根 README 的工具链安装和 `cargo fetch --locked` 为前提。未提供的能力在各章的范围说明中列出。

所有地址、账号、群号和凭据均使用示例或占位符。模型 API、协议标准及构建依赖的链接用于说明公开契约，阅读文档不需要访问其他项目的源码或本地目录。
