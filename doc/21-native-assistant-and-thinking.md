# 原生 assistant、工具调用与推理上下文

本设计取代旧版要求模型输出 ModelTurn XML 的线协议。Agent 的内部 ModelTurn、工具鉴权、效果账本和安全边界继续由模块协议约束；模型通过 Chat Completions 原生 `assistant.content`、`tool_calls` 输出。宿主编排的群消息与来源数据使用正确转义的 XML；图片保留原生图片内容块。

## 输出与执行

正文与工具调用可以并存。存在工具调用时正文是阶段候选，Run 不得完成；工具结算后才能进入下一次模型调用。当前执行器明确采用单工具步骤：每次请求声明 `parallel_tool_calls=false`，核心提示要求最多一个调用，Agent 对多调用响应整体申请一次有界修复，禁止只取第一项执行。此约束不创建子 Agent，也不允许 Provider 决定工具权限。

Provider 保存完整的受支持原生 assistant 字段，含未改写的工具调用 ID、arguments 字符串和可选 `reasoning_content`。Agent 从原生响应构造内部语义块；原生参数错误、未知工具和不满足执行约束的候选进入现有唯一修复步骤。修复也使用原生 tools；删除输出 XML 解析与 XML 修复提示，不设置双协议兼容分支。

工具执行仍使用宿主生成的持久 ToolCall 身份和幂等 Effect 身份。给模型回传工具结果时，`tool_call_id` 必须使用对应原生 assistant 返回的调用 ID；厂商调用 ID 不授予权限。

工具贡献只声明业务参数。Agent 在原生 tools schema 外包裹 `authority_trigger_id` 与 `arguments`，并保持内部 JSON Schema 引用的语义；模型可从带作者信息的触发消息中读取准确引用。Agent 校验包装，受限的只读工具目录按冻结贡献校验业务参数，校验不调用工具、不修改账本。权限判断与效果提交仍走独立的受权执行链。

## Thinking 能力

Provider 配置显式选择 `reasoning_mode=none|deepseek|openai`，并可设置支持的 effort。默认 none，不能根据 endpoint 名字猜测能力。deepseek 模式发送 `thinking.type=enabled`，openai Chat Completions 模式发送 `reasoning_effort`；不发送另一厂商的字段。配置不支持的组合应拒绝，不能在失败后偷偷换协议重试。

DeepSeek 当前官方说明：携带 tools 时，所有保留历史 assistant 的 reasoning_content 均需回传，包括没有工具调用的 assistant；不携带 tools 时无需回传。保留该字段作为原生 assistant 的续轮状态，不能拼进可见正文、转成群消息或归档为长期记忆。上下文估算包含保存的推理字段；裁剪以完整 assistant/tool 因果组为单位。OpenAI Chat Completions 不返回相同的明文 reasoning_content，不伪造该字段；Responses API 的 opaque reasoning items 属于另一传输契约，不能用 Chat Completions 假装支持。

Run 内从已提交 Provider 结果回放原生 assistant；Run 结束后，Context 只保存可见回复，Agent 用回复 Payload 身份与会话范围在自己的索引账本中查询原生结果引用。查询不扫描聊天正文、不访问其他模块的私有表。Context 接收准确回复对应的有界字节成本，用于自己的选择与裁剪；只在选定回复后读取原生内容。实际响应允许空推理字段或零推理 token，不补写或捏造推理。

## 回放与缓存

每条 Adapter 消息的 Context Payload 保留独立的 `receiving_account`：`bot_id`、`binding_ref`、`platform`、`external_user_id`。它来自已绑定平台账号，不能从用户正文、昵称或人格配置推断。Agent 验证其 Bot、Binding 与平台归属，在宿主 XML 消息头中明确说明这是该消息的机器人接收账号，并按该账号给 mention 标记 `targets_assistant`。旧记录缺少账号时保留未知，不补造身份。来源群、发言者与机器人接收账号分别表达；账号事实不授予任何权限，也不作为用户人格文本编辑。

历史 assistant 不再插入宿主元数据或改写工具 arguments。用户与召回数据携带可信来源，assistant 的内部归因保存在账本和 manifest。召回材料放在稳定历史之后，减少每次召回改动对前部历史的影响；不承诺整个请求始终是上一请求的严格追加，因为上下文选择、裁剪和召回仍可能改变视图。

DeepSeek 缓存匹配已落盘的完整前缀单元，输入结束和输出结束可产生单元，公共前缀另行检测持久化；缓存构建与命中尽力而为。用量保留 hit/miss、cached_tokens 和 reasoning_tokens，以实际数字评价效果，不从 XML 或 JSON 格式推断命中比例。

依据：[DeepSeek thinking](https://api-docs.deepseek.com/guides/thinking_mode/)、[DeepSeek 缓存](https://api-docs.deepseek.com/guides/kv_cache/)、[OpenAI Function calling](https://developers.openai.com/api/docs/guides/function-calling)、[OpenAI reasoning](https://developers.openai.com/api/docs/guides/reasoning)。
