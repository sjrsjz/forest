# v39 真实 QQ 切换与登录状态（2026-10-18）

运行镜像为 `forest:runtime-memory-query-headroom-97165332`（`sha256:35afca452126bc92459bf976ed2ded08fce1e6847e5d5b5fecfa2911204dc3e0`），管理入口绑定 `127.0.0.1:18130`。v39 使用独立的 Deployment、Host、Bot PostgreSQL 数据库及 `forest-v39-packages` 包卷；未复用 v38 的 Bot 记忆库。

切换先将 v38 主 Bot `forest-main-v38` 通过正式管理意图设为 `OFFLINE/OFFLINE`，确认树莓派 NapCat 的 OneBot 8080 已建立连接数归零，再将 v39 主 Bot `forest-main-v39` 设为 `ONLINE/ONLINE`。v39 Bot 图 22/22 ACTIVE，Deployment 图 8/8 ACTIVE；树莓派随后仅出现一条来自 Forest 主机的 OneBot 连接。v38 Supervisor 仍健康运行，但其主 Bot 已离线，隔离测试 Bot 已正式退役。

原 v38 只读 24 小时观察在第 74 次零违规采样后停止，累计约 6 小时，不能称为通过 24 小时验收。切换后从 2026-10-18 03:12 CST 启动独立只读容器 `forest-v39-soak-20261018`，每 300 秒观察一次、计划持续 24 小时。第 1、2 次样本分别在 03:12、03:17 CST：管理 HTTP 200、主 Bot ONLINE、22/22 ACTIVE、无重叠会话、过期 Run、待发消息或违规。第二次样本已见入站消息和 `group_not_allowed` 拒绝记录，说明 OneBot 消息在进入 v39；允许群 `933179609` 尚无样本。真实允许群的一次低频 `@ → Agent → Provider → Delivery → QQ` 回复以及完整 24 小时观察仍待验证。

03:17 CST 左右，经 SSH 对树莓派 NapCat WebUI 的只读 `CheckLoginStatus` 查询返回 `isLogin=true`、`isOffline=false`、`loginPhase=ready`、`coreReady=true`、空 `loginError`。树莓派运行 Linux QQ `3.2.34-53644` 和 NapCat Core `4.18.38`；旧 `alicebot.service` 保持 disabled/inactive。NapCat 历史日志中“请下载最新版 QQ”的快速登录失败发生于 10 月 13 日 22:19 CST；10 月 16 日 23:07 CST 记录了自动快速登录成功。当前状态和唯一连接表明本次检查时账号已经在线，因此未重启或升级 QQ/NapCat；这不等于解释了此前每次掉线的根因。

只读状态查询通过树莓派本机配置在进程内完成，未在文档记录或输出登录凭据。持续观察脚本不发送 QQ 消息或调用模型。

## 允许群真实 `@` 验收

10 月 18 日 13:58:23、14:00:16、14:04:49 CST，允许群 `933179609` 的 3 条真实 `@` 消息分别进入会话序号 10、11、12，均被判定为触发并最终标记 `ANSWERED`。3 个 Agent Run 均 `COMPLETED` 且无终止错误，选择的输入终点分别为相应消息的序号。对应的 3 条 CHAT 发送意图和触发回复引用逐条匹配，OneBot 发送账本均为 `SENT`、无错误，完成时间分别为 13:58:39、14:00:38、14:05:11 CST。这是现役 v39 的真实 `@ → Agent → Provider → Delivery → OneBot` 账本闭环，不是模拟 OneBot 用例；尚未由群成员人工确认 QQ 客户端中的消息展示。

第 1 次 Run 使用 1 次 PRIMARY 模型调用；第 2、3 次各使用 1 次 PRIMARY 加 1 次 REPAIR，共 5 次模型调用。5 次传输尝试均 `SUCCEEDED`、模型观测被接受，且均有用量载荷。两次修复原因均为 `AGENT_PROTOCOL_XML`：模型首轮输出没有满足协议，Agent 通过新的模型请求重写后才完成回复。这额外消耗了 2 次模型调用；尚未从用量载荷核算这 2 次的具体 token 数或金额，不能把它们说成零成本。

截至 17:42 CST，v39 24 小时只读观察容器仍运行；第 175 次样本显示主 Bot `ONLINE/ONLINE`、22/22 ACTIVE、允许群 `context_applied_seq=classified_through_seq=12`、无活动 Run 或待发消息、该样本零违规。完整 24 小时观察尚未结束。
