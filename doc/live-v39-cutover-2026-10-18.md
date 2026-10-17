# v39 真实 QQ 切换与登录状态（2026-10-18）

运行镜像为 `forest:runtime-memory-query-headroom-97165332`（`sha256:35afca452126bc92459bf976ed2ded08fce1e6847e5d5b5fecfa2911204dc3e0`），管理入口绑定 `127.0.0.1:18130`。v39 使用独立的 Deployment、Host、Bot PostgreSQL 数据库及 `forest-v39-packages` 包卷；未复用 v38 的 Bot 记忆库。

切换先将 v38 主 Bot `forest-main-v38` 通过正式管理意图设为 `OFFLINE/OFFLINE`，确认树莓派 NapCat 的 OneBot 8080 已建立连接数归零，再将 v39 主 Bot `forest-main-v39` 设为 `ONLINE/ONLINE`。v39 Bot 图 22/22 ACTIVE，Deployment 图 8/8 ACTIVE；树莓派随后仅出现一条来自 Forest 主机的 OneBot 连接。v38 Supervisor 仍健康运行，但其主 Bot 已离线，隔离测试 Bot 已正式退役。

原 v38 只读 24 小时观察在第 74 次零违规采样后停止，累计约 6 小时，不能称为通过 24 小时验收。切换后从 2026-10-18 03:12 CST 启动独立只读容器 `forest-v39-soak-20261018`，每 300 秒观察一次、计划持续 24 小时。第 1、2 次样本分别在 03:12、03:17 CST：管理 HTTP 200、主 Bot ONLINE、22/22 ACTIVE、无重叠会话、过期 Run、待发消息或违规。第二次样本已见入站消息和 `group_not_allowed` 拒绝记录，说明 OneBot 消息在进入 v39；允许群 `933179609` 尚无样本。真实允许群的一次低频 `@ → Agent → Provider → Delivery → QQ` 回复以及完整 24 小时观察仍待验证。

03:17 CST 左右，经 SSH 对树莓派 NapCat WebUI 的只读 `CheckLoginStatus` 查询返回 `isLogin=true`、`isOffline=false`、`loginPhase=ready`、`coreReady=true`、空 `loginError`。树莓派运行 Linux QQ `3.2.34-53644` 和 NapCat Core `4.18.38`；旧 `alicebot.service` 保持 disabled/inactive。NapCat 历史日志中“请下载最新版 QQ”的快速登录失败发生于 10 月 13 日 22:19 CST；10 月 16 日 23:07 CST 记录了自动快速登录成功。当前状态和唯一连接表明本次检查时账号已经在线，因此未重启或升级 QQ/NapCat；这不等于解释了此前每次掉线的根因。

只读状态查询通过树莓派本机配置在进程内完成，未在文档记录或输出登录凭据。持续观察脚本不发送 QQ 消息或调用模型。
