# CQ 上下文与 Shell 控制输入分离

## 故障证据

两次 `#forest module list` 入站均失败于 `SHELL_GATE_PROOF_INVALID`；发送者都在当前生效的 Shell 管理员名单中，Shell 请求表没有记录。原因是 CQ 上下文已包含账号、输出格式及平台消息元数据，而入站证明仍要求旧的精确 `{"parts": ...}` 结构；事务在权限校验前回滚。

## 修正

Adapter 3.4.0 只为保留前缀消息建立 `onebot.shell-input`，保存 `forest.shell-input/1` 接收账号和原始规范化控制分段，父来源为准确原始平台帧。四种普通/救援、文本/媒体入站计划显式要求完整的 `gate.input` PayloadRef。Payload 写入权限只增加这个明确的 owner kind。

Kernel ShellGate 继续验证原始传输因果根、原帧、事件、作者和群围栏，以及模型可见上下文的 owner、digest、长度；控制识别改从独立 Payload 重算，并严格验证组件、owner kind、事件 ID、内容 digest、协议和接收账号。CQ 上下文保持原格式，不放宽旧 JSON 检查或在内核解释 CQ。OneBot 原始身份、Shell 管理员授权和 Delivery 结算仍执行原有契约。

## 验证

Shell 语法、原帧与伪造拒绝检查 16 项通过；隔离 PostgreSQL 的接纳、优先级和救援围栏 3 项通过。完整 BotHost、真实 Worker 与隔离数据库验证了普通和 @ 前置 `module list` 均为 SUCCEEDED，并产生模拟 Delivery 发送，模型调用为零：1 passed in 80.45s。运行工件使用相同源码构建；业务 SQL 基线没有变化。

内核证明实现需要随 Supervisor 更新，Adapter 工件通过正式 Worker Host 预览与选择安装；保留数据库、连接配置和管理员名单。历史失败事务没有 Shell 执行记录，不自动补发旧命令。

运行实例已完成 Adapter 正式包替换及 Supervisor 正常 OFFLINE → 镜像更新 → ONLINE。最终 Graph 51、25/25 ACTIVE，活动 Adapter 摘要与发布工件完全一致；人格、Provider、Conversation、Shell、Adapter、触发策略六份生效配置逐字段比对不变。切换前 15 条 SENT 与 1692 条记忆来源保留，恢复后记忆正常增长到 1693、未结算 Run 为 0。原唯一观察器已恢复，首样本零违约。真实新命令的发送回执需用户再次发送后核对，不以模拟回归代替该结论。
