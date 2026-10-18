# Agent 工具网关与外部插件验收（2026-10-19）

设计见 [24 工具网关](24-agent-tool-gateway-and-external-plugins.md)，操作见 [外部插件说明](../plugins/README.md)。账号、群、管理入口与私有地址均使用占位符；本记录不保存凭据或真实平台身份。

## 实现边界

- 复用冻结目录、Contract Router、Authorization 与 Effect 交接，不建立第二套 Agent 或调度器。贡献协议为 `forest.agent-tool-contribution/2`；执行和原结果查询分别使用 at-most-once 与 read 端口。
- SDK 执行器显式绑定自身组件，代际来自真实 Worker 身份；已交接的恢复查询使用持久 attempt ID。UNKNOWN 的错误原因进入 Effect 证据，Agent 不把不确定性写成失败终态。未处理异常只报告类型及代码位置，不输出异常文本、局部变量或完整路径。
- `network.http` 是通用宿主 I/O；DeepSeek 与 HTTP 业务位于独立外部包。正文经受管 Payload 传输；凭据只由宿主按用途解析并向精确 origin 注入。
- OneBot 工具借用 Adapter 的连接，管理操作逐项授权；角色查询明确要求 `no_cache=true`。发送提交有序 parts，CQ 格式由 OneBot 插件选择，Delivery 使用普通 OUTBOUND 队列。
- Agent 请求目录最多 128 项，与响应调用数量限制分离；单轮仍只接纳一个工具调用。Provider 不承担上下文裁剪。

## 已完成验证

| 范围 | 证据 |
|---|---|
| 包与静态约束 | 16 个自动供应官方包、3 个外部包构建校验通过；外部工具分别为 2、2、13 项，各有独立设置 UI 和只读查询端口；Ruff 与差异检查通过。 |
| Worker / SDK | 定向 38 passed，包含真实隔离 Worker；执行器回执重放及只读查询不执行、不写状态。 |
| 既有包目录 | 114 passed、1 skipped；一个既有重复 ZIP 名称警告。 |
| OneBot 协议 | 12 passed，验证成员分页、拒绝任意 action、成功/失败/不确定回执、交接后断连不重发及角色查询绕过缓存。 |
| 设置页 | 三个包均通过真实浏览器的草稿 Schema、计划生成检查；OneBot 覆盖分页保留选择、查询失败保留选择、逐工具角色与安全文本渲染。 |
| 官方 API | 真实 DeepSeek Search、Fetch 各一次，均 HTTP 200 且结构匹配；原生 HTTP 驱动对公开文档 HTTPS 读取 HTTP 200，验证固定 DNS 后的 Host / SNI 与字节限额。 |
| 完整隔离实例 | PostgreSQL、真实 Worker、模拟模型/HTTP/OneBot，1 passed in 610.59s。17 个工具 SUCCEEDED；普通成员禁言得到预期 `ONEBOT_MANAGEMENT_DENIED`，两个 Agent Run 均 COMPLETED。4 次 HTTP、5 次管理命令、3 次发送均只落在模拟服务；CQ 的 @ 与表情分段经过实际 wire 核对。 |
| 插件热重载 | 同一完整实例将 OneBot 工件从 1.0.0 换为 1.0.1，Generation 改变，原调用计数不增加，无重复外部效果。 |

## 现役切换

现役实例已切入 `forest:external-agent-tools`，RuntimeGraph 修订 41，25/25 模块 ACTIVE，25/25 schema bundle READY。真实控制台核验三个包的独立设置页、配置计划生成与丢弃均通过；OneBot 发现 24 个已加入群。原人格、Provider、Adapter、触发策略与 Shell 配置逐项比对不变，三个插件配置均已生效。

正式停机前后确认无未结束 Agent Run、无 QUEUED/SENDING/UNKNOWN 发送；仅重建 Delivery 与 Worker Host 的开发记录，删除原 62 条 SENT、4 条 FAILED 的发送历史及旧模块选择记录。原 925 条记忆来源仍在，未重建 Agent、Conversation、Memory、媒体或向量数据。三个包按 reviewed 来源独立登记，不冒用 official/builtin 身份。

此次修正了既有 Agent SQL 及公共/内核存储 SQL 的 Git 行尾失真：Git 保存实际受封印的原始字节，Agent 和公共/内核存储摘要保持不变，避免把代码更新变成数据库重建。新 Delivery 使用 3.5.0 基线，未添加开发期兼容迁移。

旧观察器已停止，新版唯一只读观察器已启动；首个样本为 ONLINE/ONLINE、Graph 41、25 个活动成员、25 个 READY bundle、925 条记忆来源，`violations=[]`。

真实群管理动作不执行。长期运行观察及真实 QQ 发起的工具对话不能由上述模拟验收替代，24 小时稳定性尚未验收。
