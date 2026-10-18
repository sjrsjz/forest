# 外部 Agent 工具

这些目录各自构建为不可变 Python Worker 包，通过 Bot 工作区的 `component.worker-host` 选择安装工件。它们不在默认 builtin 列表中，也不会被宿主解释器 import。工具网关、授权、Effect 交接和恢复协议见 [24 设计](../doc/24-agent-tool-gateway-and-external-plugins.md)。

| 包目录 / 组件 | 工具 |
|---|---|
| `deepseek_web` / `tools.deepseek-web` | `web_search`、`web_fetch` |
| `http_tools` / `tools.http` | `http_get`（GET/HEAD）、`http_request`（POST/PUT/PATCH/DELETE/OPTIONS） |
| `onebot_tools` / `tools.onebot` | `onebot_account_info`、`onebot_groups`、`onebot_group_info`、`onebot_member_info`、`onebot_members`、`onebot_send_group`、`onebot_send_private`、`onebot_delivery_status`、`onebot_ban_member`、`onebot_ban_all`、`onebot_kick_member`、`onebot_set_card`、`onebot_set_group_name` |

## 安装与配置

发行负责人用 `forest plugin build <目录> --cache-dir <暂存目录>` 捕获包，并按 `forest plugin publish-reviewed-bot --help` 核对 ZIP、包、manifest 和组件摘要后发布到运行实例的目录。`publish-reviewed-bot` 是本地目录所有者操作，普通 ZIP 导入不会取得宿主 I/O 信任。首次安装由发行供应步骤调用 `bootstrap_installed_package`，以包的 `defaults.json` 或显式初始配置建立独立配置文档；批准范围必须与包声明的能力逐项匹配。供应只建立配置和存储，不启用模块。

随后在 Bot 的 `component.worker-host` 中选择准确安装工件，检查候选图并提交启用。启用后选择对应组件自己的设置页，生成配置计划，再点击外层的“应用计划”。首次配置供应目前是发行步骤，WebUI 尚未提供通用的第三方包初始化向导。

- **DeepSeek**：根端点默认 `https://api.deepseek.com`；在 Secret 管理中登记 API Key，在插件设置填 Secret 引用及匹配的用途。这里使用 `/v1/tools/search`、`/v1/tools/fetch`，不复用聊天 URL。可以限制结果条数、模型可见字符、响应字节及超时；不调用额外摘要模型。官方契约见 [Search](https://api-docs.deepseek.com/zh-cn/api/search/)、[Fetch](https://api-docs.deepseek.com/zh-cn/api/fetch/)。
- **HTTP**：配置公网开关、精确 origin、显式允许的私网主机及写操作主体。主体使用可信引用，例如 `qq:<QQ_ID>`；默认没有写操作者。可选 Bearer Secret 仅注入精确允许的 origin。模型不能覆盖凭据、Host、Cookie 等传输头。重定向作为响应返回；二进制或压缩正文只返回受管 Payload 引用。
- **OneBot**：选择当前 Bot 的账号绑定，从已加入群列表中勾选允许群，不必手填群号。逐项开启工具并为群发送、禁言、踢人、名片、群名分别勾选允许的目标群角色；全局操作者另行配置。默认群范围为空，未配置时不会向任何群开放。私聊发送只允许全局操作者。禁言时长为秒，`0` 表示解除；全员禁言的 `enable=false` 表示解除。

OneBot 的工具群范围独立于自动对话触发范围。群角色来自冻结调用中的真实触发者，经 Adapter 查询目标群核验；模型参数和网页正文不能自称管理员。平台还会按机器人自身权限拒绝操作。

包内工具开关限制执行；停用整个包会从后续模型轮次的目录移除该包贡献。角色查询显式绕过平台缓存。结果不确定时保留原操作，不重新计算 attempt 或重发命令。

## 结果与生命周期

发送工具支持已有 CQ 文本、@、回复、表情和受管图片。图片必须携带 `[CQ:image,file=<media_ref>,revision=<media_revision>,source_unit_id=<source_unit_id>,part_id=<part_id>]` 的完整来源；引用格式及可用媒体见 [CQ 契约](../doc/22-onebot-cq-presentation.md)，禁止任意文件路径、外部 URL 或 base64 图片。发送成功回执表示 Delivery **已入队**；使用 `onebot_delivery_status` 核对 `SENT` / `FAILED` / `UNKNOWN`，不要把入队当成已发送。

每个工具有独立的执行端口和只读原结果查询端口。查询不会执行工具，也不创建状态记录；没有准确回执就保留 `UNKNOWN`。HTTP 写请求和 OneBot 管理请求在交接后失去结果时不自动重发。插件热重载使用普通候选图、排空和 Generation 切换；在途调用不会改投新版插件。HTTP 客户端归插件 Generation；OneBot 工具借用 Adapter 已有连接，停用工具不会建立或残留第二条 QQ 连接。

修改包后重新构建、发布新工件，在 `component.worker-host` 选择新版提交更换。相同存储基线的插件升级可热重载；宿主新增驱动种类及 Delivery SQL 基线变更属于发行更新，不能谎称为任意存储格式的在线热迁移。
