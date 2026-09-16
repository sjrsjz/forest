# P3 产品子 Agent 删除

状态：**旧设计已撤销，并从未发布基线直接删除。**此文只记录禁止重新引入的语义。新的 Agent 语义见[单 Agent 协议与持久运行](p3-agent.md)。

Forest 曾实现 `forest_analyze`、`ChildScheduler`、`bot_agent_children` 和 `bot_child_model_attempts`，让群 Agent 再发起独立模型请求。该实现误解了 QiYu Agentlib：Agentlib 的“下一轮”是同一个 Agent 在工具或 Provider 安全边界后的下一次模型调用，pending branch 也是同一条对话执行链；它没有群聊子 Agent。

产品子 Agent 会额外引入第二份 Prompt、模型预算、恢复分支、候选接纳和来源授权，却没有解决本项目的群聊排队、原子 ModelTurn 或 Agent 核心提示词问题。因此该能力退役：

- 删除内置工具 `forest_analyze`；
- 不启动 `ChildScheduler`，不创建 Child 模型请求；
- Agent 配置和 WebUI 删除全部 `children_*` 字段；
- 后台不再提供 Child 状态、派发、接纳或恢复入口；
- 计划任务需要对话时向目标 Conversation 写入 trigger，进入唯一 Agent Session；
- 组件内部的嵌套 Kernel RPC 继续存在，但称为 component call tree，不称为 Agent。

Forest 尚未上线，不为这项错误抽象保留兼容层。重建数据库基线时直接删除 Child 相关表、索引、触发器、权限、配置字段、DTO、API、WebUI、调度器和测试夹具；开发数据库由干净基线重建。历史迁移文件若尚未形成公开发布契约，可以压平或替换，不要求旧 019/032 数据库升级，不提供只读 Child 诊断，也不允许旧备份把 Child 运行状态恢复进新基线。

需要保留的只有独立于 Child 抽象的真实外部效果账本和已确认费用事实；保留它们时去除 Child 业务身份，按通用模型调用/效果协议归档。测试只需证明代码、最终 schema、管理路由和运行注册表中不存在产品子 Agent，并证明计划任务和同群新触发都进入唯一 Conversation Agent 队列。
