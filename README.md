# Forest

Forest 是一个正在重构中的重型多 Bot 微内核框架。它以不可变模块代际、严格生命周期、类型化契约和持久因果事实组织平台接入、对话调度、Agent、记忆、Shell、发送与管理能力。

**使用控制台从[中文操作指南](doc/user-guide.md)开始**：进入 Bot 工作区、查看对话、筛选记忆、修改配置和热重载的具体步骤。无需先阅读架构文档。

> **当前状态：开发中，尚未发布。** 当前完成度、运行实例与可信验证见[实现状态](doc/implementation-status.md)；目标语义见[架构总契约](doc/00-architecture-contract.md)。操作指南描述当前界面，不代表全部目标能力已完成。

## 架构入口

后续设计和实现必须按以下顺序读取：

1. [架构总契约](doc/00-architecture-contract.md)：最高权威，固定产品语义、模块边界、执行链和重构禁区。
2. [设计索引](doc/README.md)：列出 01–22 的裁决顺序和各领域详细设计。
3. [微内核运行时与控制面](doc/13-microkernel-control-plane-redesign.md)：模块代际、运行图、管理贡献和宿主职责。
4. [干净数据库基线](doc/14-clean-schema-and-payload-provenance.md)：模块数据所有权、schema bundle、载荷来源和跨模块事务。
5. [当前实现状态](doc/implementation-status.md)：只记录已实现能力、阻断项与实际运行证据，不定义目标架构。

旧 `doc/p0-*` 至 `doc/p5-*` 纵向实现文档已删除，只能通过 Git 回查；不能据此恢复旧表、编号迁移、固定业务页面、专用宿主 API、巨型 Store 或业务组合根。

## 不可改变的语义

- Agent 是可替换的 `agent.runtime` 模块，是 QiYu Agentlib 风格的单状态机；不存在产品子 Agent。
- 同群只有一条活动响应链，不同群可以并行。所有已接纳群消息进入 Context，并保留作者、群、Binding、时间和引用来源。
- Agent 忙碌时，下一次模型调用只接入截至最后一个新触发记录的连续前缀；其后的普通尾部留队。工作结束且没有新触发时，尾部静默排空。
- Context 裁剪是确定性的，不调用 LLM，也不生成记忆摘要。
- 每个 Bot 有一个全局多模态记忆域，每个群有独立主干；跨主干联想允许存在，召回必须逐条标记来源群、作者和证据。
- 文本、图片和混合输入都进入完整记忆与嵌入链。text-only LLM 只在单次请求的 ModelView 中移除图片，不改变规范历史。
- VL embedding 空间在 Bot 创建时冻结；之后只能更换兼容 endpoint 或凭据。
- 群聊 Shell、WebUI 和 CLI 调用同一类型化管理操作、授权、幂等、冲突和审计协议。
- WebUI 是模块控制台。壳层不固定 Agent、Memory、OneBot、Bot 管理、备份或存储页面；准确模块 Generation 贡献 schema、查询、动作和可选沙箱 UI。

## 微内核边界

平台 Adapter、Conversation Scheduler、Agent、Memory、Shell、Delivery、Job 和路由器是平级模块。模块只经版本化 Contract、稳定引用和受管 `PayloadRef` 通信；需要原子提交时由内核主持各模块 participant，模块仍只能写自己的 namespace。

`BotRuntimeSupervisor` 只监督冻结的 RuntimeGraph、宿主 lease、资源和健康。它不得构造或保存具体业务模块。`BindingRuntime`、巨型 `BotStore`/`MessageStore`、Adapter 内嵌 Shell/Delivery、Agent 直接发送消息和固定业务后台都是禁止恢复的旧设计。

每个实现切片必须先在设计中写清：

- owner 与独占 namespace；
- Repository contract 与持久事实；
- provides/requires 及准确 Generation；
- UnitOfWork participants 和锁序；
- lease、epoch、CAS、删除与异步发布 fence；
- management contribution 与 UI 沙箱边界。

## 开发与验证

项目需要 Python 3.11。开发依赖可用 `uv sync --frozen` 安装。当前常用检查为：

```sh
uv run --frozen ruff check src tests
uv run --frozen pytest
```

PostgreSQL 测试要求专用的可重建测试库；具体 DSN 和选择的测试组以当前开发环境为准。不要把旧 `forest serve`、旧 Docker Compose 页面可访问或旧迁移链成功视为目标架构已经接通。

Forest 尚未发布。开发期 schema 改动直接更新模块 baseline 并重建数据库，不增加兼容迁移、fallback、双写或旧 API 转发。真实 OneBot 与 Docker 验收只有在生产启动链完成干净 RuntimeGraph 切换后才有发布意义。
