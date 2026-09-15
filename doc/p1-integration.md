# OneBot、群聊调度与可靠发送实现说明

本文件记录 P1 接入切片，不替代 01–12 的完整设计。当前接通 OneBot 11 HTTP API + HTTP POST 上报；未实现反向 WebSocket、平台事件重放或原生幂等发送。平台实现和登录态由部署者提供，Forest 不内置 QQ 客户端。

## 配置与连接归属

从 WebUI 创建 Bot，再登记 QQ 账号。部署库唯一约束 `(platform,self_id)`，不允许两个 Bot 同时宣称拥有同一账号。登记先留下 `RESERVED` 事实，然后以新鲜 Web 票据向目标 Bot 提交固定 Binding ID/账号；目标成功才记 `REGISTERED`。跨库中断后页面仍展示待完成登记，原管理员点击该项即可继续固定操作。当前不支持转移或删除账号归属。

Binding 配置包含启用状态、HTTP API origin、触发方式、模板及管理员账号列表；API token 和上报签名密钥只加密存储在目标库。随机密文先按操作冻结，再把密文摘要加入签名计划。同一 HTTP 幂等键重试不重复加密或生成不同配置。配置采用预览计划、CAS 接纳 desired revision、运行时推进 effective revision 的过程。不同旧版本的并发配置不会相互覆盖。

新连接先核对 API 真实账号，旧连接停止接纳新发送并排空/取消已持有资源，随后发布新 Binding/connection epoch。关闭或失权的连接不能偷偷借用新客户端完成旧发送。后台的在线状态同时校验当前宿主实际持有的连接，数据库里上次运行留下的 `ONLINE` 不代表重启恢复完成。

HTTP 凭据由宿主持有。`adapter.onebot` 是受生命周期管理的 Python Worker，只处理无凭据协议 DTO 与准确发送字节。纯 codec 有明确 CONTROL 调用通道，不能调用 state 或创建子服务调用；普通暂停不阻止接收及控制回执，代际冻结、撤权、宿主租约失效仍会阻止它。

## 群消息与触发

HMAC-SHA1 校验原始请求体，核对固定 `self_id`，规范化完成后才写 Inbox。HTTP 成功响应在目标事务提交之后返回。去重范围固定账号/会话和平台消息 ID；重试不另占 ingress 序号。平台时间只作为来源信息，内部顺序以持久接收序号为准。作者保存平台用户 ID、显示名、群名片和来源，匿名用户不合并为同一共享用户。

所有普通群消息均进入 Inbox 和短期上下文候选，即使不触发也会静默排空并裁剪。裁剪按预算移除可见单元，保留不可变 Inbox 和归档引用，不生成记忆摘要、不调用语言模型。一个 Bot 的多个群由共享槽和独立 Actor 租约调度；每个群只执行一个前台响应，其他群可并行。

新的触发在当前响应期间排队。下一次安全输入选择只推进到已分类连续前缀中的最后一个触发，后续非触发消息暂不进入该轮。执行结束后若无新触发，普通尾部静默排空。最终回复、已覆盖触发的 ANSWERED、回复序号和 SendIntent 在同一事务提交。取消、预算不足或失效不会把未回答触发标记为成功。

当前回复链由每个 `(bot_id, conversation_id)` 唯一持久 Agent 执行。宿主在下一次模型调用安全点推进消息切口到当时最后一个触发记录，模型—工具链继续期间不创建第二条并发响应；Agent 结束后若只剩未触发尾部，则静默排入短期上下文。CorePrompt、PresetContext、原子 ModelTurn、工具因果组和 checkpoint 已接通；未配置模型时不会退回产品子 Agent。

当前协议转换和模板计算都是纯计算，失败后可用新 attempt 重新执行。业务去重落在 Inbox、最终 Reply/SendIntent 与发送尝试账本，不宣称 Worker 代码只执行一次。模型计费、工具副作用和长期记忆写入不能直接沿用这种重试方式。

图片目前规范化为有类型的引用并避免把临时下载 URL 带进 Context。后续还需完成受权下载与媒体内容持久化；P4 实现图文 embedding、图多主干、跨群来源召回和文本模型投影。当前引用不能解释为图片已长期保存或已建立多模态记忆。

## 发送顺序与不确定结果

CHAT 按 Conversation/response_seq/part 排序，CONTROL 按独立 command/control_seq 排序。二者共享账号限速与容量，并预留控制处理槽。一个群的 UNKNOWN 会阻止后续 CHAT 越过它，但不会阻止其他群或独立管理回执。

发送前先冻结 Worker 产生的具体 wire bytes，目标事务核验 Bot、Binding、连接、插件代际、原 policy、取消状态和期限后记录 permit。网络 I/O 在事务外，只使用许可对应的字节与客户端。记录 permit 后丢失响应、超时、取消或崩溃均保留可能已发送的事实；不能通过重连、代际更换或重启自动重发。

后台逐项展示原消息、目标、分段 ID、状态和时间。UNKNOWN 可由当前管理员以预期 revision 核对为：平台已收到（SENT）、有依据确认未发送并重试（NOT_SENT）、跳过此条（SKIP）。SKIP 不抹掉旧尝试的不确定事实。确认未发送才允许同一语义分段的新尝试；确定失败的 NOT_SENT 本身不产生隐式重试循环。

当前后台队列和发送列表各取最近 100 条，不提供全历史分页或按状态检索。长期运行中旧 UNKNOWN 可能移出这个有限视图；账本保留事实，完整历史定位与容量治理属于 P5 必须补齐的管理能力。

## 群聊管理入口

固定完整前缀 `#forest` 在 Inbox 接纳时进入独立 CONTROL 路径。识别直接文本及可信的开头 Bot mention，不把引用、转发或 self echo 当命令。未知命令、坏语法和无权限请求均不回落普通 Agent。当前支持状态、Run/Job 管理和本群记忆来源遗忘；管理员来自有效 Binding 配置的显式 QQ ID，群 owner/admin 角色不自动授予框架权限。`shell-gate-v2` 的已接纳命令保持冻结，新消息使用增加记忆命令的 v3，Dispatcher 按持久 gate 版本恢复。

当前不执行任意系统命令、不开放 OS shell。敏感原始参数不写入普通 Context 或日志，未知命令只保存摘要和固定说明；已接入命令使用类型化参数、会话 scope 和 P0–P2 优先级，动态注册、运维 runbook 与完整授权委托仍按 09 文档实现。管理回执不占普通群 Actor，也不改正在执行的提示上下文。

## 验证入口

`test_web_bindings.py` 使用真实 PostgreSQL、受限 Bot 数据库、实际 Python Worker 与真实本地 TCP OneBot 服务器验证接收、静默排空、模板回复、UNKNOWN 对账、重启与跨 Bot 账号隔离。`test_messages.py`、`test_group_scheduler.py`、`test_delivery.py` 分别覆盖事务合流、Actor fencing、并发和恢复。`scripts/smoke_browser.py` 在独立 Compose 部署中用真实浏览器操作连接表单并走 HTTP 收发。

这些本地验证不需要真实 QQ/LLM 密钥。2026-09-15 另使用当前 Forest 正向 WebSocket 客户端连接线上 AliceBot/NapCat：账号身份、服务状态、许可群查询、向群 `933179609` 的单次发送及 `get_msg` 读回均通过，且没有自动重试。该外部证据仍不证明 QQ 用户入站、业务命令、普通 Handler 或完整 Agent 回复链已在真实群中触发；其余边界以[实现状态](implementation-status.md)开头的发布候选快照为准。
