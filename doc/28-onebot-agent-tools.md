# Agent OneBot 工具

`/module/onebot-tools` 是独立、可热重载的 Wasm 包。Agent 仍通过工具网关的 `/describe` 与 `/invoke` 调用它，不直接持有 OneBot 连接或凭据。

| 工具 | 内容 |
|---|---|
| `onebot_get_login_info` | Bot 账号与昵称 |
| `onebot_get_friend_list` | 好友账号、昵称与备注 |
| `onebot_get_group_list` | 已加入群的账号、名称、人数 |
| `onebot_get_group_info` | 指定群的信息 |
| `onebot_get_group_member_list` | 群成员列表、名片与角色 |
| `onebot_get_group_member_info` | 指定成员的群资料与角色 |
| `onebot_get_stranger_info` | 指定用户的公开资料 |
| `onebot_send_group_message` | 立即发送独立群消息，返回各次发送的实际 message_ids |
| `onebot_send_private_message` | 立即发送独立私聊消息，返回各次发送的实际 message_ids |

群查询与群发送省略 `group_id` 时使用当前会话群；显式指定时可操作其他已加入群。用户参数使用平台账号，不能把昵称当作账号。列表使用 `offset`、`limit`，默认每页 50 条、最多 100 条；结果包含 `items`、`total`、`next_offset`，不把第一页表示成完整列表。`no_cache` 默认 false，按 OneBot 标准传递给支持它的查询。

发送参数 `message` 使用 [Forest Markdown](33-markdown-messages.md)，通过专用链接表达 @、回复、表情和受管图片。图片只能引用 `media:<sha256>`；只在发往网关时产生 Base64。工具结果保留目标、实际 `message_ids`、终止 `error`（成功时为 null）和原始 Markdown，不把 Base64 放回模型。分隔符节点拆分消息，失败后保留成功回执并停止发送。OneBot 失败或仅接受异步执行都不会返回成功回执；不自动重试发送。

普通 assistant 正文仍自动发送到当前群。工具用于显式的独立发送，成功后不应再在 assistant 正文重复同一条消息。工具调用及结果沿用现有 Agent 归档流程，不伪造模型回合，也不向另一群的 Agent 队列注入用户输入。

宿主 `/core/onebot` 的 `/query` 接收固定的只读 `Query` 枚举并返回对应 v11 数据的 JSON；`/send_private_message` 与 `/send_group_message` 接收类型化发送请求。它们共用 HTTP/WebSocket 连接与错误处理，不接受任意 action 字符串。工具包仅允许 `/module/agent-tools` 与受信 `/user/admin` 调用，跨模块调用仍检查 Manifest uses 与目标 ACL。

正常运行由 Agent 配置决定哪些群能触发工作。显式 `--allow-group` 测试限制还会过滤群查询目标及群列表，拒绝范围外群发送和全部私聊发送。

构建：

```powershell
cargo run --locked --offline -j 2 -p forest-onebot-tools --bin package-onebot-tools
cargo run --locked --offline -j 2 -p forest-agent-tools --bin package-agent-tools
```

网关默认路由包含这些工具。已有实例保留管理员设置的 `/routes` 当前值，添加工具时通过配置 `set` 修改路由并重载网关。Manifest 默认值更新不覆盖当前值；原生端口随宿主启动安装，工具包可独立热重载。
