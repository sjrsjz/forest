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
| `onebot_send_group_message` | 立即发送独立群消息，返回实际 message_id |
| `onebot_send_private_message` | 立即发送独立私聊消息，返回实际 message_id |

群查询与群发送省略 `group_id` 时使用当前会话群；显式指定时可操作其他已加入群。用户参数使用平台账号，不能把昵称当作账号。列表使用 `offset`、`limit`，默认每页 50 条、最多 100 条；结果包含 `items`、`total`、`next_offset`，不把第一页表示成完整列表。`no_cache` 默认 false，按 OneBot 标准传递给支持它的查询。

发送参数 `message` 使用 CQ 文本，支持文本、@、回复、表情和受管图片。图片只能引用 `media:<sha256>`；插件通过 Media 的 `/get` 获取字节，只在发往网关时产生 Base64。工具结果保留目标、实际 message_id 和原 CQ 文本，不把 Base64 放回模型。OneBot 失败或仅接受异步执行都不会返回成功回执；不自动重试发送。

普通 assistant 正文仍自动发送到当前群。工具用于显式的独立发送，成功后不应再在 assistant 正文重复同一条消息。工具调用及结果沿用现有 Agent 归档流程，不伪造模型回合，也不向另一群的 Agent 队列注入用户输入。

宿主 `/core/onebot` 增加两个入口：`/query` 接收固定的只读 `Query` 枚举并返回对应 v11 数据的 JSON，`/send_private_message` 接收类型化私聊请求。原有 `/send_group_message` 保持不变。它们共用当前 HTTP/WebSocket 连接与错误处理，不接收模块提交的任意 action 字符串，不公开凭据、重启等管理 API。工具包只允许 `/module/agent-tools` 与受信 `/user/admin` 调用；后续模块调用继续检查 Manifest uses 与宿主 ACL。

正常运行由 Agent 配置决定哪些群能触发工作。显式 `--allow-group` 测试限制还会过滤群查询目标及群列表，拒绝范围外群发送和全部私聊发送。

构建：

```powershell
cargo run --locked --offline -j 2 -p forest-onebot-tools --bin package-onebot-tools
cargo run --locked --offline -j 2 -p forest-agent-tools --bin package-agent-tools
```

新安装的默认路由包含这些工具。现有实例仍保留管理员的 `/routes` 当前值，需通过配置 `set` 追加路由再重载网关；更新 Manifest 默认值不会覆盖当前值。首次升级宿主需要重启以注册新的原生接口，之后工具包可独立热重载。
