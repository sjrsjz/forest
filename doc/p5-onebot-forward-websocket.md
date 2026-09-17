# P5 OneBot 正向 WebSocket 传输

## 1. 目标与边界

Forest 的 Binding 继续表示一个固定 OneBot 账号与一个 Bot 实体的绑定。本切片在 HTTP API + HTTP POST 事件上报之外增加 OneBot v11 正向 Universal WebSocket：同一条由 Forest 主动建立的 `/` 连接既接收事件，也执行获得许可的 API action。它用于 AliceBot/NapCat 常见的本机部署方式，不改变 Conversation、Shell、Agent、Delivery 或多 Bot 隔离语义。

首版不实现反向 WebSocket 服务端，也不把 WebSocket 放进插件 Worker。握手凭据、socket、echo waiter 和入站队列都由宿主持有；`adapter.onebot` Worker 仍只做无凭据的纯规范化和出站编码。OneBot v11 正向 WebSocket 的 `/api`、`/event` 与 Universal `/` 语义来自[标准通信说明](https://github.com/botuniverse/onebot-11/blob/master/communication/ws.md)。

## 2. 配置与凭据

Binding 配置新增：

| 字段 | 取值 | 约束 |
| --- | --- | --- |
| `transport` | `http-post` / `forward-websocket` | 旧配置缺省按 `http-post` 解释 |
| `api_base_url` | HTTP(S) origin | 仅 HTTP 模式使用；禁止 userinfo、query、fragment 和非根 path |
| `websocket_url` | WS(S) URL | 仅正向 WebSocket 模式使用；首版只接受 Universal 根路径 `/`，禁止 userinfo、query 和 fragment |
| `access_token` | 宿主秘密 | 两种模式均必须存在；只进入握手头或固定 API origin |
| `webhook_secret` | 宿主秘密 | 仅 HTTP POST 模式必须存在；WebSocket 事件没有 `X-Signature` |

页面、审计、配置正文和回执不回显凭据。`ws://` 适用于本机或受信网络；跨不可信网络应使用 `wss://`。客户端关闭代理自动发现、HTTP 重定向和压缩协商，避免凭据离开固定端点，并限制解压放大面。

## 3. 单连接所有权

一个已启用的 WebSocket Binding 在一个 `client_incarnation` 下恰有一个宿主客户端。客户端状态为：

```text
NEW -> CONNECTING -> BUFFERING -> ACTIVE -> RETIRING -> CLOSED
                    |             |
                    +-----------> FAILED
```

`BUFFERING` 表示握手已经认证，但 Binding 尚未发布新 `binding_epoch`。此时接收器可以把事件放入有界内存队列，不得调用业务存储。身份探针 `get_login_info` 必须在同一条候选连接上完成；返回账号与 Binding 的 `self_id` 不一致时立即关闭候选。

发布顺序固定为：

1. 建立候选连接并启动唯一读取循环；
2. 在候选连接上执行只读身份探针；
3. 将旧 Binding 置为 `DRAINING`，停止旧连接的新业务接纳并关闭旧传输；
4. 以 CAS 推进 `binding_epoch`、`connection_epoch` 和 `client_incarnation`；
5. 本地登记新 `Connection`，再原子打开候选的 ACTIVE gate；
6. 候选队列按接收顺序逐条提交 Adapter 入站观察，再经 UnitOfWork 进入 `conversation.scheduler` 接纳端口。

NapCat 会把事件广播给同一账号的多个正向连接，因此切换重叠窗口可能出现重复事件。Forest 依赖既有 `(binding, platform_message_id)` 幂等事实去重；不得用“只让一条连接接收”换取无法证明的无缝切换。若发布 ACK 丢失，候选保持 BUFFERING，先读取数据库确认新 incarnation 是否已成为 head：是则采用并打开 gate，否则关闭并丢弃。未成为 head 的客户端永远不能写 Bot 数据。

## 4. 单读取者与消息分类

每条 socket 只有一个读取协程。它先执行有界 UTF-8 JSON 解码并拒绝重复键，然后按下列顺序分类：

1. `echo` 是当前 pending 表中的精确字符串：作为 action response 交付给对应 waiter；
2. 存在 `post_type` 且没有受 Forest 管理的 echo：作为原始事件帧进入 FIFO；
3. 其它帧：记录协议错误并终止连接。

发送协程绝不直接读取 socket。它在有界 pending 表中登记随机、不可复用的 echo，冻结 action 与 params 字节，然后在发送锁内写入一个完整文本帧。读取协程负责匹配响应，因此多个不同群的发送可以并发等待，返回顺序不必等于请求顺序。

事件 FIFO 默认最多 256 帧、每帧最多 128 KiB；pending action 默认最多 64 个、响应最多 64 KiB。FIFO 满、帧过大、二进制帧、重复 echo、未知响应或读取循环异常都使连接失败并触发 Binding 监督器重建。首版不宣称 OneBot 具有事件 replay；断线后尚未进入 Forest 持久 Inbox 的事件可能无法恢复，页面必须暴露断线错误。

## 5. 入站顺序与现有 Agent 语义

ACTIVE 后只有一个事件消费协程，严格按 socket 接收顺序调用接入协调器。Adapter 只完成宿主预解析、原始平台帧与规范化事件 Payload 接纳、纯 Worker 投影一致性核对、回声识别和平台事件幂等。协调器随后在有界 UnitOfWork 中调用 Shell 优先级端口与 `conversation.scheduler` Inbox 接纳端口；触发分类由路由模块完成。任何一步都通过稳定引用交换，不能由 Adapter 直接写其他模块的表。

WebSocket 握手已经用 Binding 的 access token 认证，因此不再伪造 HTTP `X-Signature`。代码必须调用单独的“已认证传输帧”入口，不能传入虚构签名绕过校验。该入口仍校验当前 `Connection` 对象、`client_incarnation` 和 `binding_epoch`，候选或退役连接不能借旧回调写入。

进入 Inbox 后，所有并发规则完全复用现有实现：同群唯一 ConversationSession 串行消费，不同群并行；运行中的新触发只在下一次模型调用安全点接入到最后触发记录；普通尾部在新一轮或静默排空时进入短期上下文。传输层不得创建第二个 Agent、不得按 WebSocket 帧启动模型任务，也不得改变历史作者标记。

## 6. 出站不确定性

WebSocket action 使用 Delivery 通过传输端口交付的冻结 `wire_payload` 作为 `params`，仅在外层增加 `action` 与 echo。一次 Delivery attempt 至多调用一次 Adapter 端口、一次 socket write：

- 在取得当前连接前失败，或在任何字节写入前已知连接关闭：`NOT_SENT`；
- 完整响应为 `status=ok, retcode=0` 且含合法 `message_id`：`SENT`；
- 明确平台拒绝码：`NOT_SENT`；
- 写入开始后的取消、超时、断线、未知/重复 echo、畸形响应：`UNKNOWN`。

客户端禁止自动重发 action。连接监督器可以重建传输，但 Adapter 只返回类型化传输观察；Delivery 与 `kernel.effect` 负责持久化 `UNKNOWN` 和显式对账。关闭连接时，所有已写入的 pending waiter 返回 `UNKNOWN`，未发送的新调用返回 `NOT_APPLIED`。

## 7. 关闭、失败与热切换

`Connection.close()` 是幂等、可屏蔽调用方取消的所有权收口：关闭新发送接纳，终止 socket，结算 pending，停止入站读取和消费，最后释放网络资源。`adapter.onebot` Generation 持有所有候选与已发布客户端；本地关闭未完成时不得清空 `_clients` 所有权集合。`BotRuntimeSupervisor` 只观察模块健康和生命周期终态，不取得客户端所有权。

读取循环意外结束后，`client.is_closed` 必须立即可见。`adapter.onebot` 的连接监督器在下一次扫描发现 owner 不再健康，按同一配置建立新 incarnation；不在客户端内部悄悄重连，以免绕过数据库代际与身份探针。配置热切换同样建立候选、探针、DRAIN、CAS、采用，不修改单 Agent 或插件代际。

## 8. 验收

实现至少证明：

- 真实 WebSocket socket 上的 Bearer 握手、同连接身份探针、事件接收与 action echo；
- 单读取者可以处理响应乱序和事件穿插，事件仍按到达顺序提交；
- 候选激活前不写业务库，激活后排空；错误账号、队列溢出和畸形帧失败关闭；
- action 发送一次，断线/超时为 `UNKNOWN`，明确拒绝为 `NOT_SENT`；
- 从空 namespace 安装的 `adapter.onebot` baseline 可明确选择 HTTP 或正向 WebSocket transport；不读取旧配置、不运行兼容迁移，WebUI 不回显凭据；
- 重载、丢 ACK、取消和 runtime close 不泄漏 socket、reader、consumer 或 pending waiter；
- 使用线上 AliceBot/NapCat 配置完成只读 `get_status`、`get_login_info`、获准群 `get_group_info`；真实发送若执行，只允许群 `933179609` 且保持单次低频。

2026-09-15 的线上验收已完成最后一项：当前 Forest 客户端在树莓派隔离临时目录连接运行中的 NapCat，Bearer 握手、账号身份、状态和许可群查询通过；随后仅发送一条明确标记的测试文本，结果为 `SENT`，message ID 可由 `get_msg` 读回。客户端未重试，临时源码和依赖已清理，在线 AliceBot/NapCat 未重启。真实用户入站和完整 Bot Actor/Agent/Handler 链仍需单独触发验收。
