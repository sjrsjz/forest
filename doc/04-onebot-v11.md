# OneBot v11 API 库

## 职责与边界

`forest-onebot` 提供 OneBot v11 的类型化请求、响应、消息段与异步客户端。协议层依赖 Serde，原生 HTTP 传输通过可选 feature 引入 Reqwest；模块调度、鉴权和网关连接管理由宿主负责。

## 协议与执行分离

- `Action` 用关联类型确定请求对应的响应，标准 action 名由实现声明，不让调用方配对任意请求与任意响应类型。
- `Client<T>` 负责一次请求的编码、异步提交及结果解码。`Transport` 只负责向指定 action 传递参数 JSON 字节并取得响应 JSON 字节。
- 默认不启用网络依赖，协议和泛型客户端可以编译为 Wasm。`http` feature 提供原生 HTTP(S) 传输，使用 Tokio 运行时，但库不创建运行时或隐式后台任务。
- 覆盖 v11 的全部 38 个公开 API，包含消息、群管理、请求处理、查询、凭证、媒体和实现管理。不把 NapCat 等实现的扩展混成标准。
- 本库提供 v11 数据类型和异步 API 客户端；WebSocket 连接、echo 分派与事件监听由宿主适配器负责，见[运行说明](13-runtime-and-sdk.md)。模块生命周期由调度器管理。
- 请求结构与消息段是数据，构造和序列化不发送请求。只有显式调用异步方法才产生 I/O。

## 结果语义

依据 [HTTP 标准](https://github.com/botuniverse/onebot-11/blob/master/communication/http.md)，区分完成（ok/0）、提交异步执行（async/1）和远端失败（failed/其他返回码）。提交异步执行不等于执行成功，也不产生伪造的 message_id。

传输错误、JSON 编解码错误、协议状态矛盾和远端 API 错误分别表达。失败响应保留返回码及实现提供的 message/wording/data，不用错误字符串决定逻辑。JSON 动态值只用于协议边界上的未知数据或标准允许的实现扩展。

Rust 的异步 I/O 不等于 OneBot 的 `_async` action：普通 `call` 异步等待一次完整响应；显式提交接口可选择标准 `_async` 或 `_rate_limited` 模式，其结果仍受远端实际响应约束。

HTTP 请求使用 JSON Content-Type 和可选 Bearer 认证；目标可带路径前缀。仅 HTTP 200 解码为标准 API 响应，其他状态保留 HTTP 错误。默认传输不自动重试、不跟随重定向，不设置框架级超时；并发调用各自拥有请求 Future，不经过全局串行后台队列。请求失败或 Future 被丢弃都不证明远端没有执行操作。

默认客户端沿用环境代理；允许通过 `with_client` 注入明确配置的连接池。注入时由调用方决定代理、TLS、重试及重定向等传输策略，不由 SDK 暗中覆盖。本地 HTTP 测试使用直连环回地址。

## 消息与数据契约

依据 [API 概述](https://github.com/botuniverse/onebot-11/blob/master/api/README.md)和[消息段标准](https://github.com/botuniverse/onebot-11/blob/master/message/segment.md)，消息支持 CQ 字符串、消息段数组及单段对象。标准消息段使用枚举与结构化字段，保留图片/语音/视频等文件引用，不自动读取文件、下载媒体、转换 Base64 或解释受管媒体引用。

字符串形式由调用方明确决定是原始 CQ 还是普通文本（`auto_escape`）；段数组的文本不会被再次解析为 CQ。SDK 不负责执行 Agent 输出策略。未知消息段或非法标准字段返回解码错误；不通过降级为普通文本掩盖协议问题。扩展支持需要显式增加类型。

已显式支持 NapCat 的 `Markdown { content: String }` 消息段。原始 Markdown 保留在结构化消息中，经 CQ 投影时使用正确转义的 `[CQ:markdown,content=...]`，读取时可还原。含该段的消息可通过 notifier 进入记忆，不因缺失一种消息段而整条拒收。声明依据为 [NapCat 消息类型](https://github.com/NapNeko/NapCatQQ/blob/main/packages/napcat-onebot/types/message.ts)。

`get_msg` 在顶层按 message_type 区分 sender；成员列表允许标准说明可能缺失的字段；状态 online 可以为 null。普通可选请求参数不传时省略，由 OneBot 实现应用标准默认值。

## 与 Forest 接入

该 crate 不持有 Forest 管理员身份，不决定哪个用户可以禁言或读取凭证。Forest 暴露的 OneBot 原生接口在静态声明上挂 AccessPolicy，通过 callers 检查可信调用主体；具体群目标等约束由接口处理。群消息等宿主事件由接入层解析后交给调度器 notifier，再按 Manifest 接收声明投递到处理模块，不要求插件产生事件。该协议库本身不创建 notifier 或定义事件监听入口。

当前接入只考虑一个 Bot 账号，仍支持该账号参与的多个群聊。不加入账号池、Bot 实体目录、跨账号选择参数或每 Bot 配置挂载；事件里的 self_id 保留为平台账号信息。

### 事件转发约定

采用 OneBot v11 原始事件结构，保留字段名称、数据含义和消息内容，不另造统一聊天事件。接入层将协议 JSON 解码为共享的 OneBot 类型，经 Forest 序列化传给模块；notifier 只做投递。这里的“原始结构”指协议结构，不要求逐字节保留 JSON 空白和字段顺序，也不要求模块反复解析一段 JSON 字符串。

群消息保留 time、self_id、post_type/message_type/sub_type、message_id、group_id、user_id、anonymous、message、raw_message 和 sender。message 的 CQ 字符串或消息段结构保持原意，不降级为裸文本，不主动下载或内联媒体。sender 的可选信息按标准缺省，不伪造昵称、角色等字段。字段含义与可选性依据 [v11 消息事件](https://github.com/botuniverse/onebot-11/blob/master/event/message.md)。

共享事件类型省略 font 等与接入无关的字段，保留消息来源、发送者、回复关系及媒体引用。省略由接入协议的类型定义确定，调度器不任意裁剪字段。

WebSocket 连接与事件监听由宿主接入层负责，不把连接转交给接收插件。事件源身份由宿主绑定；平台用户信息和群角色属于事件上下文，不自动授予框架管理权限。

模块主动取得的网络句柄按其执行实例生命周期管理，不迁移 Rust Future 或连接对象；宿主事件接入自身的资源归该宿主能力，不归某个消息接收插件所有。具体通知契约见[notifier 设计](11-notifier-and-dependencies.md)。

标准依据：[公开 API](https://github.com/botuniverse/onebot-11/blob/master/api/public.md)、[HTTP](https://github.com/botuniverse/onebot-11/blob/master/communication/http.md)、[鉴权](https://github.com/botuniverse/onebot-11/blob/master/communication/authorization.md)。
