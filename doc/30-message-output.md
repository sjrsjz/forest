# 群消息输出与长消息转发

`/module/message-output` 是独立 Wasm 包，源码位于 `plugins/message-output`。它通过静态 `/send` 接口完成 CQ 解析、受管图片解析、按群选择长消息展示方式与 OneBot 发送。Agent 持有包自己的契约副本，不链接其实现；宿主仍只提供 OneBot 原生传输。

## 调用与回执

```text
Agent 模型正文 / 错误报告 ─┐
                         ├─ message-output /send ─ core/onebot /send_group_message
OneBot 工具的群发送 ──────┘
```

请求为 `GroupOutput { group_id, bot_id, bot_name, message }`，其中 message 是 OneBot 的类型化 Message：模型正文使用 CQ 字符串，框架错误报告使用明确的 Text 消息段。返回 `Result<SentMessage, Error>`；调度失败仍属于外层 DispatchError。静态 callers 允许 Agent、OneBot 工具包和受信管理员。

发送前只接纳文本、@、回复、表情和受管图片。图片由 Media 取得字节，转换为本次发送使用的 Base64；引用或地址本身不计入文本长度。模型不能通过输出 `node` 伪造其他人的发言。合并转发节点由输出模块构造，发送者始终为调用请求中的 Bot 身份。

启用群中，解码后的文本超过字符阈值或行数阈值，就把整条消息转换为合并转发。字符使用 Unicode 标量计数，换行按 `\n` 计数。阈值只决定展示方式；一条原始输出对应一个完整的转发节点，文本与非文本段保持原样和顺序，通过一次发送形成一张转发卡片。

NapCat 的 `send_group_msg` 能直接接收全部为 `node` 的消息数组，因此复用现有原生发送入口；实现依据为官方 [SendMsgBase](https://github.com/NapNeko/NapCatQQ/blob/main/packages/napcat-onebot/action/msg/SendMsg.ts)。不需要给调度器增加长文本或转发策略。

实际发送成功后，Agent 将卡片外层的 message_id 关联到原 ModelTurn。数据库、模型上下文与向量化来源继续使用完整模型原文；转发节点仅是发送投影，不制造新的用户或记忆节点。转发接口失败后返回错误，记录诊断，不重试或降级为普通长文本。

## 配置

在 WebUI 中选择 `message-output` → 配置表 → `/settings` → 编辑。

| 字段 | 默认值 | 含义 |
|---|---|---|
| `enabled_by_default` | true | 没有逐群覆盖时是否转换长消息 |
| `groups` | 空列表 | 每条包含 `group_id` 与 `enabled`，每个群最多一条 |
| `character_threshold` | 1000 | 超过此 Unicode 字符数转换，保留完整正文 |
| `line_threshold` | 20 | 超过此行数转换 |

所有群默认启用。要关闭某群，在 groups 添加一条该群的配置，将 enabled 取消勾选；将其勾选可覆盖默认关闭策略。字符与行数阈值须为正整数。短消息继续普通发送。

每次输出开始时读取并验证一份完整设置，保存后下一条输出直接采用，无需重载。已有发送保留其开始时的配置快照。配置仅由模块自己读取、由管理员通过既有 ACL set 路径修改。

包可按现有 Drain、AtYield、Force 语义热重载。无待迁移的后台工作或消息副本，设置持久化在宿主配置表中。
