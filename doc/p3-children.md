# P3 产品子 Agent 退役与兼容迁移

状态：**旧设计已撤销。此文只记录升级兼容，不再定义可用能力。**新的 Agent 语义见[单 Agent 协议与持久运行](p3-agent.md)。

Forest 曾实现 `forest_analyze`、`ChildScheduler`、`bot_agent_children` 和 `bot_child_model_attempts`，让群 Agent 再发起独立模型请求。该实现误解了 QiYu Agentlib：Agentlib 的“下一轮”是同一个 Agent 在工具或 Provider 安全边界后的下一次模型调用，pending branch 也是同一条对话执行链；它没有群聊子 Agent。

产品子 Agent 会额外引入第二份 Prompt、模型预算、恢复分支、候选接纳和来源授权，却没有解决本项目的群聊排队、原子 ModelTurn 或 Agent 核心提示词问题。因此该能力退役：

- 删除内置工具 `forest_analyze`；
- 不启动 `ChildScheduler`，不创建 Child 模型请求；
- Agent 配置和 WebUI 删除全部 `children_*` 字段；
- 后台不再提供 Child 状态、派发、接纳或恢复入口；
- 计划任务需要对话时向目标 Conversation 写入 trigger，进入唯一 Agent Session；
- 组件内部的嵌套 Kernel RPC 继续存在，但称为 component call tree，不称为 Agent。

已经发布的数据库迁移文件保持不可变。`050_single_agent_protocol.sql` 已执行以下收敛：

1. 锁定旧 Child 表并把非终态记录转为 `CANCELLED` 或明确的遗留失败状态；
2. 结算已知模型用量，`UNKNOWN` 费用事实继续保留，不推断为未发送；
3. 对旧 request/result payload 保持既有来源、删除屏障和保留期规则；
4. 安装拒绝 INSERT/业务状态推进的数据库 fence，防止旧 Worker 重新创建任务；
5. 不删除历史事实表，便于审计和旧备份恢复；
6. 新运行时不 hydrate 或调度任何遗留 Child 记录。

升级测试需要覆盖旧 019/032 数据库、活动/等待/未知/终态 Child 记录、载荷已清理记录以及旧 Worker 持有连接的情形。迁移完成后，只有显式的只读历史诊断可以看见遗留记录。
