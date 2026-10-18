# 原生 assistant 与 thinking 验收（2026-10-18）

设计依据见 [21 原生协议](21-native-assistant-and-thinking.md)。模型输出 XML 解析已删除；Provider 结果使用 `forest.provider-model-result/2`，Agent 解释原生 assistant 消息，工具结果回放保留厂商调用 ID 和 arguments 原文。Agent、Conversation、Provider 官方包升级为 5.0.0，Agent 私有 SQL 基线为 3.0.0；开发期使用新库供应，不添加旧协议兼容或表迁移。

本地检查覆盖原生消息解析、流式 reasoning/content/tool_calls 分离、原样回放、厂商参数隔离、工具参数包装与局部 Schema 引用、只读参数校验和 Context 预算选择。最后一组本地检查为 67 passed、3 skipped；跳过项要求 PostgreSQL。此前相关检查为 75 passed、1 skipped。Ruff 与 diff whitespace 检查通过，以上组之间有重叠，不相加统计。

独立 Docker/PostgreSQL 集成的两个正式 BotHost 场景通过（2 passed）：工具调用续轮沿用同一 Run，准确回传原始调用 ID 与私有推理，同时吸收截至最后触发的新消息、保留普通尾部；前次回复结算后，新触发进入不同 Run，从 Agent 索引账本恢复原生 assistant 和推理。两者均断言 QQ 模拟发送正文不含私有推理，使用独立测试数据库与模拟 OneBot。

使用现役配置的 `deepseek-v4-flash-vision-exp` 执行了四次真实 API 请求，工具仅为进程内纯 echo，无平台或存储副作用，也未连接 QQ。最后一组请求中，首步原生工具调用含 100 个字符的独立推理，第二步携带原始 assistant、reasoning_content 和工具结果后正常返回最终正文；第二步为零 reasoning token，属于有效响应。四次请求均 RESPONDED、无 Provider 错误，共 3992 prompt token、218 completion token，其中 2432 prompt token 被报告为缓存命中。不据此推算长期命中率。

18:42 CST，独立候选 `forest/accept-v40` 已通过正式管理动作供应主 Bot `forest-main-v40` 为 READY/OFFLINE；Deployment 图 8 个模块 ACTIVE。候选使用新 Deployment、Host、Bot 数据库及独立包卷，复制现役冻结的 VL 模型身份、真实 Provider/OneBot 配置与许可群策略，显式配置 `reasoning_mode=deepseek`、`reasoning_effort=high`。现役 v39 此时继续在线，候选尚未连接 QQ。切换与后续真实群聊证据另行记录。
