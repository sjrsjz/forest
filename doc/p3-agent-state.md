# P3 持久 Agent 状态与并发边界

状态：持久执行底座已存在，原子 ModelTurn/协议修复正按[P3 单 Agent 设计](p3-agent.md)接入。实现入口为 `storage/agent.py`、`storage/messages.py` 和 Bot 数据库迁移 `013_agent.sql`。宿主 `AgentRunner` 调用这些接口；插件没有数据库连接，也不能自行推进会话边界。数据库事务中不执行模型 HTTP、插件或平台发送。

## Run、Actor 与安全点

每个 Conversation 保留一个持久 Session 和活动 Run。Actor 是可释放、可重新取得的执行租约。`ensure_run` 首次冻结配置与截止时间，后续接管仅递增 owner epoch，不重新解释旧 Run 配置。宿主身份、Actor epoch、Binding epoch、策略版本、存储世代及维护世代共同限制写入。事务开始与最终提交前均检查权限和数据库时钟。

`boundary` 在同一会话事务检查上一模型步骤与整组工具结果已经闭合。如果出现已分类触发，先按 step 序写入未发布的非最终 `RUN_STEP`，再调用事务内输入选择器，把队列推入截至最后一个可纳入触发的位置，同时推进 Run 的 `input_revision`。其后的普通消息留在队列中。分类缺口尚未消失时进入 `WAIT_INPUT`，不能创建模型快照；只有分类水位推进或已知续体超时才重新调度。

没有新触发时，已闭合的 ModelTurn/FunctionResponse 保留为 Run 私有上下文，用于下一模型调用，不重复写入公共历史。一个含最终 FunctionCall 的 ModelTurn、该调用的 FunctionResponse 与下一 ModelTurn 构成有序因果链；每个模型轮次至多一个工具，不存在并行工具组。当前 Run 因果单元和所有已领取触发的源单元都固定保留；超过模型预算时明确失败，不能删除触发或拆开调用链来腾出空间。已完成 Run 的工具因果链只能按完整单元边界裁剪。裁剪只调整可见性，不调用摘要模型；归档返回准确 unit/event/source 引用，首尾序号仅供说明。

注册为持久 Agent 的 Run 不能调用旧版公共 `select_input(SAFE_POINT)`、`finalize_reply` 或 `end_run` 绕过状态机。事务内辅助方法仍验证持久状态；最终回复从保存的 `FINAL_READY` 输出读取，而不是相信调用者提供新输出。

## 模型许可与预算

`snapshot` 创建不可变逻辑 step、输入切口、触发 ID、输入 batch 引用、上下文版本及来源信息。同一准备步骤恢复时复用原快照。`prepare_model` 验证 Provider manifest 的快照摘要与所有输入版本，并持久化精确 HTTP body 字节、wire digest、序列化版本和 attempt epoch。API key 不进入 Run、manifest 或 body 快照；宿主使用独立加密配置解析凭据。

每次模型许可先在 Bot 级 UTC 日账本预留输入上界与输出上限。不同群聊通过同一预算行合流，不能各自检查余额后并发超支。`check_model_permit` 在网络发送前原子地把许可改为 `DISPATCHING`，复检当前配置与最终租约时间。

`NOT_SENT` 可以有限重试，保持语义 request ID、完整 body 和输入切口，仅改变 attempt ID 与 epoch；最多三次。`UNKNOWN` 保留日预算预留与原 Session，禁止自动重试。明确成功结果按 usage 结算；无 usage 时保守计入完整预留。晚到但仍属于当前 Actor 的模型结果保留为观察并结算已知费用，Run 明确失败，不能发布文本或执行其工具。失去 Actor 或当前权限的迟到结果不能修改 Run；接管时以 UNKNOWN 保留不确定预留。

已知续体（包括等待审批）到达 Run 截止时间后可持久失败；正在发出的请求或工具先转 UNKNOWN。UNKNOWN 不会因时间过去就被当作没有执行，它需要可证明的宿主执行回执或管理员明确处置。

## 工具权限、观察与管理

一个 ModelTurn 最多产生一个工具提案并原子持久化。工具固定操作 ID、参数摘要和一个 `authority_trigger_id`；该触发必须属于当前 Run 切口。不能组合不同用户的权限。`binding_admin` 同时要求当前管理员列表，并明确拒绝匿名和 Bot 自身消息。

工具授权取冻结定义与当前配置的交集。审批必须对应同一工具、参数摘要、触发主体和策略版本，且在有效期内。管理端使用签名 Bot ticket 接受决定，审批决定不可覆盖，操作回执支持丢失确认后的重放。界面数据包含原始用户身份、平台、群聊、触发事件和 ingress 位置，不能只显示不透明哈希供人批准。

`guard_tool_call_in` 在 Kernel 已持有的 runtime 锁内重新检查工具、Actor、Session、Binding、当前配置与审批。它不逆序取得 Binding 锁；root admission 还比对真实服务目标与参数摘要。Kernel 的后续 state 写入、子调用及完成操作再次使用该持久授权。工具 NOT_SENT 最多三次，操作 ID 保持稳定，`attempt_count` 防止旧尝试覆盖新尝试的观察。

UNKNOWN 工具只允许从相同操作 ID 的已完成 Kernel 回执恢复，或通过签票人工确认结果。每次状态/结果变化追加不可变观察账本，人工解决不会抹除先前 UNKNOWN 事实。当前工具未知时阻塞该 Agent 因果链。结构化结果以 typed parts 或 `type=json` 保留，交给 Provider 投影处理嵌套媒体；不提前把媒体伪装为文本 JSON。

`inspect` 提供 Run、审批、UNKNOWN 工具和预算；`decide_approval` 接受精确参数摘要；`resolve` 还比较显示时的 checkpoint 序号。`cancel` 同样使用 checkpoint CAS，撤销 Actor 并终结当前 Run 的 claims，不取消后续排队触发；不确定模型预算继续保留。

## 最终回复与恢复

经过协议验证的终止 ModelTurn 形成 `FINAL_READY`，固定 `final_step_id`、`final_input_revision` 与 Assistant 文本；Thinking 不进入群回复。模型槽可先释放。随后 `commit_final` 在一个事务中刷新更早非最终闭合步骤、插入最终回复与 CHAT SendIntent、把本轮触发置为 ANSWERED、完成 Run 并记录 checkpoint。最终步骤不走通用 flush。

若事务失败，旧 FINAL_READY 和 claims 完整保留；重启后先提交这个原候选，再处理新触发。若确认丢失，已提交回复 ID 与 SendIntent 保持唯一，重复提交返回原回执。最终回复、输入快照、模型观察、审批决定与工具观察的数据库约束防止历史事实被覆盖。

## 验证范围

`tests/test_agent_store.py` 使用真实 PostgreSQL 独立数据库，覆盖切口与工具闭合、公共边界绕过、FINAL_READY 接管/确认丢失/事务回滚、跨群并发预算、精确 NOT_SENT 重放、UNKNOWN 与过期策略、真实签票审批/取消/解决、来源展示、低预算触发固定保留及旧工具尝试隔离。Runner、Web、真实 Native Worker 与模型 HTTP 的组合验证分别位于其他 P3 测试文件。该状态机不声称能回滚已经发生的平台或外部工具效果；未知效果依赖持久回执和明确处置。
