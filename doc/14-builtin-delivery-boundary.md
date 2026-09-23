# Builtin Delivery 模块边界

本文固定 `delivery` 从宿主内置 Python 对象迁移为官方 builtin 包时的所有权，防止迁移期间再次把 Repository 包装成“插件”。目标组件 ID 保持 `delivery`，包 ID 为 `forest.delivery`。

## 所有权

`delivery` 包独占以下内容：

- SendIntent、SendPart、TransportAttempt、Observation、Claim 与 reconciliation 的状态机和表；
- 队列选择、同 lane 串行、租约、恢复、重试和 UNKNOWN 收敛策略；
- 创建发送意图、取消、查询 UNKNOWN、读取详情和人工 reconciliation 的业务契约；
- owned-work handler、配置 schema、管理 query/action 和管理 UI；
- 所有 Delivery DTO、幂等 operation 派生和业务错误码。

上述状态必须位于包内 `schema/`，变更函数必须由 manifest 声明为 Domain Command 或 UnitOfWork participant。Worker 只能通过 SDK 的 Query、Command、UnitOfWork 和 Service 端口工作，不能得到数据库连接、宿主 Repository 或其他组件实例。

## 宿主保留的窄端口

宿主只保留下列与 Delivery 语义无关的能力：

| 端口 | 所有者 | 约束 |
| --- | --- | --- |
| `kernel.payload.json-read` | `kernel.payload` effect driver | 读取并复核完整 Payload 引用，只返回有限 JSON |
| `kernel.effect.json-materialize` | `kernel.payload` effect driver | 只可创建 `kernel.effect` 所有的 `effect.wire` 或 `effect.evidence` JSON Payload |
| `authorization.issue` | `kernel.authorization` UnitOfWork participant | 从调用方精确 Generation 固化的 grant digest 派生授权集合，调用方不能提交 grant ID |
| `effect.prepare/handoff/observe/abandon` | `kernel.effect` UnitOfWork participant | 只处理通用 Effect 状态与 permit，不解释 Delivery 状态 |
| `adapter.transport-fence` | `adapter.onebot` UnitOfWork participant | 在调用方事务内复核精确连接与 Generation fence |
| `adapter.onebot.delivery.prepare` | `adapter.onebot` JSON service | 把规范消息段投影为确定 wire bytes，不写 Delivery 状态 |
| `adapter.onebot.delivery.dispatch` | `adapter.onebot` JSON service | 在精确 fence 下执行一次外部发送并返回 SENT/NOT_SENT/UNKNOWN 证据 |

任何端口都不得读写 Delivery 表。Payload driver 只拥有字节落盘规则；Authorization 只拥有票据；Effect 只拥有外部副作用账本；Adapter 只拥有连接与协议投影。

## 原子边界

包在 manifest 中声明固定顺序的 UnitOfWork plan。协调器从同一 RuntimeGraph 解析每个 participant 的精确 Generation，在一个 PostgreSQL 事务内逐步调用；任一步 typed rejection 都回滚全部数据库写入。

准备发送至少包含：

1. `adapter.onebot/adapter.transport-fence`；
2. `delivery/delivery.attempt-prepare`；
3. `kernel.authorization/authorization.issue`；
4. `kernel.effect/effect.prepare`。

交接、观察、放弃和人工 reconciliation 各自使用独立 plan。plan 是不可变协议，不是可编辑工作流或 WebUI “执行链”。外部 dispatch 位于数据库准备与交接之后；一旦交接，取消或进程丢失只能写入 UNKNOWN/显式 evidence，不能伪装成未发送。

## 生命周期和热重载

Delivery 队列只能由包声明的 owned-work supervisor 领取。Generation 替换先关闭领取入口，等待已经领取的 handler 到达 settlement 边界，再排空 Worker 调用和 effect driver，最后退休旧 Generation。新 Generation 只有在整个 RuntimeGraph 原子发布后才能领取工作。

未领取意图属于组件的持久业务状态，可由契约相同的新 Generation 继续领取。已领取项绑定 claim token、attempt 和原 Generation；旧 Generation 必须完成、明确失败或进入 UNKNOWN，禁止新 Generation 偷接一个可能已经产生外部副作用的 attempt。

## 删除旧实现的完成条件

只有同时满足以下条件，才能删除宿主 `DeliveryRepository`、`CleanDeliveryWorker`、`DeliveryAuthorizationPort` 和旧静态管理页面：

- final-reply、Shell 和 ingress 协调器只调用 `delivery` 包的 participant ABI；
- Bot 组合根只通过 `bootstrap_builtin_package()` 装入 Delivery；
- 生产 RuntimeGraph 中 `delivery` 的 driver 为 `forest.python-worker/1`，并带包摘要、存储摘要和 management UI 摘要；
- prepare/handoff/observe/reconciliation 的跨组件回滚与崩溃恢复在真实 PostgreSQL 上通过；
- 热重载证明旧 Worker、owned-work claim 和 effect driver 全部排空，失败候选不会改变当前 graph；
- 源码中不存在宿主对 Delivery 表的直接读写，也不存在双写、fallback 或旧 schema 迁移。
