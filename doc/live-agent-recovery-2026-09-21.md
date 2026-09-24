# 许可群 Agent 触发恢复记录（2026-09-21）

许可群 `933179609` 的一次低频 mention 验收暴露了模型传输重试与协议修复投影中的两个生产缺陷。本记录只保留状态和计数，不记录消息正文、作者标识或 Provider 凭据。

## 现象与持久状态

- 新消息已经完成 Inbox 接纳与 trigger 分类，但同群旧 Run 长期停留在 `PREPARING_MODEL`。同群串行规则正确阻止了第二个并发 Agent Run，因此后续触发保持排队。
- 旧 ModelRequest 已产生 834 个 `NOT_APPLIED` TransportAttempt。`max_steps` 只限制语义模型轮数，未限制同一语义轮的传输次数；`NOT_APPLIED` 分支也没有退避或终止条件。
- 发现后先优雅停止 `forest-clean-supervisor-1`，阻断继续调用和费用增长，再部署修复。

## 修正后的收敛规则

- 同一 ModelRequest 最多允许 3 次传输尝试，计数来自持久化 `attempt_count`，进程重启不会重置额度。
- 重试使用异步退避，只暂停当前群的 Agent 执行协程，不阻塞其他群的 Conversation actor。
- 明确 HTTP 4xx 属于不可重试的 `NOT_APPLIED`，单次观察后立即终止；错误码包含 HTTP 状态，例如 `MODEL_HTTP_REJECTED_422`。
- 达到上限或遇到不可重试错误时，Agent 原子写入 `FAILED/AGENT_MODEL_TRANSPORT_FAILED` 检查点并关闭 Session；Conversation Scheduler 结算该 Run 已认领的 trigger 并释放 lane，使后续排队触发能够建立新 Run。
- Provider 错误码随 `effect_observation_ref` 持久化，故障诊断不再依赖进程日志。

部署后，旧 834 次请求没有产生第 835 次调用，原 Run 直接失败并释放 lane。排队触发随后建立了独立 Run；其一次 PRIMARY 调用得到 `finish_reason=length`，1024 个 completion token 全部被模型推理消耗且可见正文为 0 字节。Agent 按设计进入唯一一次 REPAIR 调用，但该调用暴露了第二个问题：host repair 记录曾以单个 `{type,text}` 对象作为 system content，OpenAI 兼容端点返回 HTTP 422。

## 协议修复与运行配置

- repair 记录现在编码成有界 JSON 字符串，system content 满足 Chat Completions 的字符串契约；其来源仍是 host control，不会冒充用户消息。
- `agent.runtime` 的 `output_tokens` 已通过模块自己的 Web 管理 contract 从 1024 调整为 4096，desired revision 2 经 plan/apply 生效，并触发 RuntimeGraph revision 9 热替换。
- 模型种类、Provider endpoint、VL Embedding profile、许可群和触发策略均未改变。

## 验证

- 对应回归现由 `tests/test_agent_runtime_package.py` 覆盖包内模型重试与持久 Run 收敛。
- `tests/test_conversation_scheduler_package.py` 覆盖包内 lane 释放与后继调度。
- `tests/test_models.py` 明确断言 repair system content 为字符串并能还原 host control 记录。
- 相关回归：`63 passed`；完整回归：`598 passed, 7 skipped`；Ruff、Python 编译与 diff whitespace 检查通过。
- 部署镜像 `forest:local` 对应 manifest `sha256:98cd18bc39b6782d8f9fa4e8ec6a03d770ec7751fa90602c083d635161335d48`，Supervisor 为 `running/healthy`。

本次自动诊断和修复没有主动向 QQ 发送测试消息。失败的历史触发保留为可审计终态，不在数据库中篡改为待处理；下一次真人 mention 用于验证 4096 token 下的完整 PRIMARY/REPAIR/Delivery 链。

## 真人触发验收结果

修复部署后，真人再次在许可群 mention Bot。新消息以 ingress sequence 19 接纳并创建独立 Session/Run；最终状态如下：

- Conversation trigger：`ANSWERED`；lane 的活动 Session/Run 均已清空，`context_applied_seq=19`。
- Agent Run：`COMPLETED`，`next_step_seq=3`，执行一次 PRIMARY 和唯一一次 REPAIR；两次 TransportAttempt 均为一次成功，没有重试。
- PRIMARY：16.680 秒，prompt 13,211 token，completion 4,096 token，`finish_reason=length`，可见正文 0 字节。
- REPAIR：14.998 秒，prompt 13,306 token，completion 2,845 token，`finish_reason=stop`，候选正文 885 字节。
- Delivery SendIntent：`SENT`，单 part；真人在 QQ 群确认收到回复。

端到端结果正确，但总 Run 耗时 118.769 秒，其中 Memory query 从注册到冻结占 86.299 秒。QUERY embedding 的两个 facet 实际在约 1.34 秒内已经完成，因此延迟定位为旧 baseline 的 PostgreSQL `real[]` + `memory_vector_dot` 全量排序；约 663 个 facet 已耗时约 85 秒。

该实现已被删除：当前基线新增平级 `vector.index` Generation，以 Milvus 保存派生向量并执行 ANN；PostgreSQL 只保存内容、来源、关系、facet 状态和稳定 `index_ref_id`。Embedding 成功后先进入 `EMBEDDED`，Milvus 以相同 ref 幂等发布成功才进入 `READY`；召回只接收 Milvus 返回的 ref，随后回 PostgreSQL 复核可见性与来源并物化证据。真实 Milvus 的 upsert/search/delete 往返测试已通过。旧 118.769 秒记录只用于说明事故原因，不能代表新路径的性能验收；仍需在许可群完成新镜像的真实端到端时延采样。
