# v37 Delivery UNKNOWN 调和验收（2026-10-17）

本轮验证新包 `forest.delivery` 0.4.8 / SQL 3.1.5 的管理入口。`delivery.unknown.list` 和 `delivery.unknown.get` 是模块声明的只读查询；`delivery.unknown.resolve` 是模块声明的 P0 管理动作，须经管理主体授权，再由 UnitOfWork 在一个数据库事务中调用 `kernel.effect.observe` 和 `delivery.record-observe`。浏览器表单不直接读写账本。

在独立 `forest-accept-v37` 部署中，从空 PostgreSQL 卷供应独立 Bot `bot/accept-v37-offline-20261017`，22 个组件在线。模型、嵌入和 OneBot 均指向该部署本地假端点，Bot QQ 自身 ID 为 10001。仅向假 OneBot 注入一次群 933179609 的提及消息；假模型返回固定 XML，假 OneBot 收到 `send_group_msg` 后断开而不返回回执。管理查询随后找到一个 `UNKNOWN` intent 和 attempt，详情包含对应 Effect 引用与原始 `EXECUTOR` 不确定观察。

管理员通过正式管理动作提交 `REJECTED_NOT_APPLIED` 与 `isolated-fixture` 证据，动作返回 `accepted=true`、intent 状态 `FAILED`。再次读取详情可见 attempt `REJECTED`、`resolved_at` 已填、两条观察中有 `RECONCILIATION` 证据。直接只读检查 Bot PostgreSQL：该发送的 Effect operation 为 `NOT_APPLIED`，Delivery intent 为 `FAILED`，原始执行观察和调和观察均被接受。再提交相反的 `SENT` 裁决，返回 `accepted=false`、`EFFECT_EVIDENCE_CONFLICT`；Effect 和 Delivery 终态不改变，冲突观察可审计。这证明管理路径实际跨组件执行，并非仅在 UI 改状态。

本次选择“确定未发送”的依据仅来自可控假端点的实现；真实 QQ 平台发生 UNKNOWN 时，管理员必须先取得平台侧证据，不能凭传输断线推断未发送。现役 v33 没有升级，新包也没有对真实 QQ 发送消息。树莓派 SSH 只读检查确认 NapCat 使用 Linux QQ 3.2.34-53644，10 月 17 日 19:45 仍接收群消息；历史“版本过低”记录在 9 月 17 日，未对树莓派客户端做替换或重启。
