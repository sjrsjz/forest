# Agent 工具网关与外部工具插件

## 1. 所有权与接入

复用现有 `agent-tools` 贡献目录、冻结 ToolCall、Authorization/Effect 交接与原生 tool 消息协议。工具业务不能进入 Agent、Provider 或内核。外部包位于 `plugins/`，通过 `source_kind=installed` 选择；需要宿主 I/O 的包使用已有 reviewed Bot 工件信任，仍独立安装、配置、升级、停用和热重载，不并入默认 builtin 成员。

本轮补齐 SDK 中面向工具提供者的执行适配：检查接收者身份，把业务结果物化为本模块的 `tool.result`，持久记录准确回执。开始记录与重复执行拦截由既有 at-most-once 服务账本负责；SDK 不另建第二套开始状态机。查询原调用只能读记录；没有准确结果时返回 UNKNOWN，不能重发。网关与适配器不解释搜索、HTTP 或 QQ 业务。

工具目录中的服务/version/Generation、Schema 摘要与 binding 在模型回合冻结。执行中不得按名称重新绑定到新版插件。插件排空、资源释放、失败恢复继续采用同一 Worker 协议。查询函数不创建执行记录，不以“查询”名义重新执行副作用。

模型请求最多声明 128 项工具，与响应解析的调用数量上限分开；Agent 的单次模型回合仍最多接纳一个工具调用。不能用响应调用上限截断目录，也不能因为安装更多工具而改成并行 Agent 或并行工具执行。

贡献事实采用 `forest.agent-tool-contribution/2`，显式声明独立只读 `query_service_id` 与执行超时；执行服务采用 at-most-once 接纳，查询服务采用 read 接纳。执行器仅把提供者明确证明的拒绝/终态失败转为 FAILED，交接后无准确回执的异常保持 UNKNOWN。30 秒授权票据与 Effect permit 的 deadline 只约束交接，服务请求的执行 deadline 单独按工具声明计算；不能为了长请求延长票据，也不能用票据有效期截断已交接的合法执行。

## 2. 通用 HTTP 传输能力

新增宿主通用 `network.http` I/O 驱动种类，外部 reviewed 包通过 effect service 声明使用。宿主仅处理有界请求、连接生命周期、URL/目标约束、凭据注入、响应 Payload 与明确的传输结果；DeepSeek 路径、参数和响应含义均由外部插件处理。新增一种工具不应再次修改 BotHost 组合根或增加专用宿主客户端。

传输配置由管理员提交到插件自己的配置文档：允许的 origin、公网 URL 开关、明确允许的私网主机、超时、响应字节上限与可选凭据引用/用途。请求只可选择声明允许的目标；DNS 解析后的地址在连接时固定，保留原 Host 与 TLS SNI。禁止 URL 内凭据、环境代理、自动重定向，以及模型覆盖 Host/管理凭据等传输头。重定向作为响应返回，下一次请求重新进行目标检查。

凭据仅由宿主按配置引用和用途解析、注入到精确允许的 origin，不返回 Worker 或模型。完整响应正文保存为所属插件的 `http.response` Payload，通过准确引用和分块读取跨 IPC；不能把大响应或二进制 base64 塞入工具消息。传输要求 identity 编码并对原始响应字节限额；服务端仍返回压缩内容时，HTTP 插件保留编码信息和原始 Payload，不在工具消息内自动解压。插件决定可见投影和截断标记。每次传输只尝试一次，写请求结果不确定时必须 UNKNOWN。

## 3. DeepSeek 搜索与抓取插件

`plugins/deepseek_web` 提供 `web_search` 与 `web_fetch`，分别调用官方 [Search API](https://api-docs.deepseek.com/zh-cn/api/search/) 和 [Fetch API](https://api-docs.deepseek.com/zh-cn/api/fetch/)。按[官方指南](https://api-docs.deepseek.com/zh-cn/guides/search/)使用独立的 `/v1/tools/search`、`/v1/tools/fetch` 端点及 Bearer 鉴权，不能拼接聊天端点 `/chat/completions`。

Search 参数遵循 query/count/ori_question/localization/need_content，保留结果标题、URL、来源日期和相关性信息。默认只读搜索摘要，正文按需 fetch；Fetch 保留来源 URL、标题及 Markdown。结果字数与返回条数由插件设置约束，裁切明确标记，不增加摘要模型调用。HTTP 错误作为结构化工具结果交回同一 Agent Run；不在 Provider 添加搜索或抓取业务。

## 4. HTTP 操作插件

`plugins/http_tools` 提供 HTTP(S) 查询与写操作的独立工具。GET/HEAD 的读接口与 POST/PUT/PATCH/DELETE 的写接口分别声明，写权限、目标和主体由本插件配置决定，不依据模型自报身份。请求支持有界文本/JSON body 与业务头，结果保留状态码、允许的响应头、Content-Type、正文投影和截断信息；二进制结果返回受管 Payload 引用。网页内容始终是工具证据，不进入系统提示词或取得工具授权。

## 5. OneBot 工具插件

`plugins/onebot_tools` 贡献账号/群/成员查询、群/私聊发送、成员禁言与解除、全员禁言、踢人、群名片与群名等独立工具。模型不取得任意 `action/params` 执行入口。参数按 [OneBot v11 公开 API](https://github.com/botuniverse/onebot-11/blob/master/api/public.md)声明；列表分页输出，群和成员来源明确。

Adapter 提供版本化查询与命令端口，并复用自身 Generation 已持有的 WebSocket。凭据、连接和平台 API 回执仍归 Adapter；外部插件不建立第二条 OneBot 连接。查询与管理命令分别声明服务能力。发消息通过 Delivery 的提交接口，支持既有正文 CQ、表情和受管图片；工具返回“已入队”与 intent_id，不把它谎称为 SENT。

工具范围由插件配置中的账号绑定、允许群、操作开关和可信 QQ 操作者决定；群管理员权限按冻结 ToolCall 中的真实 principal 向 Adapter 查询目标群角色，普通文本“我是管理员”没有授权意义。不同管理操作分别配置允许角色；平台还会按 Bot 自身角色拒绝越权操作。发送和群管理不因安装插件自动向任意群开放。真实平台验收只读查询；禁言、踢人和批量发送仅在模拟 OneBot 中验证。

## 6. Delivery 主动发送

增加独立 OUTBOUND lane，承载模块主动发送。它与 CHAT 使用相同普通发送优先级，不冒用 Shell/Core 的 CONTROL 优先级，也不伪造 `context.reply` 来源。Delivery 自己物化发送正文并提交持久 intent；外部插件只接收准确队列回执。原 Chat 回复、Shell/Core 控制回执的来源约束保持明确。

`delivery.submit` 接收有序文本 parts，是否使用 `format=platform-markup` 由调用模块明确选择。OneBot 工具按 CQ 输出契约设置该标记；Delivery 不把其他模块的普通文本自动解释为 CQ。工具交接的未知异常仍以 UNKNOWN 收敛，同时保留安全的结构化错误码，不能丢掉原因或猜测为 FAILED。

提交重放使用来自工具 effect 的稳定 operation identity，相同输入返回同一 intent，不因 Worker 重启再次入队。状态查询只读 Delivery 自有记录。现有发送 Worker、Codec、受管媒体解析、Effect 发送/UNKNOWN 对账链继续作为唯一实际发送路径。

跨模块调用的来源组件由 Contract Router 经 Worker 调用元数据注入 `ctx.caller_component_id`，不接受正文自报来源；Delivery 以此记录主动发送的来源模块。

## 7. 配置与验收

每个插件携带自己的设置 UI，使用通用配置计划/应用协议；群选择消费 Adapter 的群发现端口。安装与能力授予通过模块管理；API Key 使用 Secret 引用。配置、帮助和验收记录不提交真实账号、群号、凭据或私有地址。

验证重点为：插件构建与安装、模型可见目录、准确参数与来源、跨 Bot 隔离、并发重放合流、读取无写入、副作用交接后的 UNKNOWN、插件热替换和资源归属。外部 API 用受控模拟服务验证全部请求和错误；真实 DeepSeek 使用低频探针，真实 OneBot 管理操作不执行。
