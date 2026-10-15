# 跨范围驱动联合换代

本设计约束 Host/Deployment 控制包导出的 Effect 驱动与 Bot 包消费方之间的运行时热替换。当前实现已经冻结来源、核对契约并在活动 Bot 仍消费旧来源时拒绝 Host 换代；下述联合换代协议已接入 Supervisor 的候选换代路径，但多 Bot PostgreSQL、进程恢复和真实实例验收仍未完成。这里的 Agent、Memory 等业务包只通过 `effect_services.kind` 声明消费，不参与调度本协议。

## 权威与身份

- Host 控制图持有提供方包、公开服务、准确 Generation 与 `source_ref`。来源摘要包含 scope、scope ID、包摘要、Generation、服务契约、语义和双 schema 摘要；无关 Graph revision 不改变来源。
- Bot 图描述符持有服务名、kind、`source_ref` 的冻结绑定。Bot-owned 代理只转发到这一来源，不自行寻找“最新”提供方。Host 路由和 Host 持久 Generation 权威再次核对目标。
- Supervisor 拥有跨图协调端口和进程内准入围栏。Bot 图与 Host 图仍分别由各自存储权威发布；不能伪装成一个跨数据库事务。可恢复操作身份必须写入 Host 图变更账本，并派生每个 Bot 图的稳定 activation ID。
- Bot 内核负责排空自身 Generation 与 Effect 调用；Host 内核负责排空提供方 Generation。协调器不得直接触碰包内消息队列或业务状态。

## 变更影响

Host 候选图与旧图计算提供方影响闭包。只对准确旧 `source_ref` 在活动 Bot 描述符中出现的 Bot 建立消费方集合。每个 Bot 候选图用 Host 候选导出事实重新编译；描述符摘要变化确定该 Bot 的换代闭包。未消费变化来源的 Bot 和闭包外 Generation 保留身份。

Host 图变更在持有跨图围栏时读取活动 Bot 绑定。Bot 启动、换代、关闭也持有同一围栏，直到绑定快照发布或解除，防止影响集检查与新消费方进入交错。长时间持锁会暂时串行化不同 Bot 的图管理；群聊消息和已活动组件的数据调用不通过这把锁。

## 发布顺序

1. Host 图验证目录来源、授权、存储基线和 Graph revision；装配候选并 `prewarm()`。此时 Host 候选 Generation 身份已分配，Worker/驱动已准备，但路由尚未发布。由候选 Registry 和候选包快照生成未来导出事实，不能从当前控制器的活动导出表猜测。
2. 对每个受影响 Bot 固定选择账本与当前 Graph revision，用未来 Host 导出事实装配候选；调用 `prepare()` 而不是 `prewarm()`。准备阶段仅产生候选 Generation 和替换事实，不启动需要活动 Host 来源的代理。候选必须核对 kind、契约、语义、双 schema 摘要和来源身份。
3. 依 Bot ID 稳定排序冻结消费方入口，排空其换代闭包中已接纳的调用与 owned work，执行现有 `begin_replacement`、`validate_replacement` 和 quiesce。任何一个 Bot 排空失败时，不准排空或发布 Host。
4. Host 图按原有逻辑排空其影响闭包并发布候选。持久 Host Graph 提交是不可逆边界；控制器活动 Registry 和导出快照必须立即指向新 Host Generation。
5. 逐个 Bot `prewarm()` 后 `start()`。此时代理的来源就绪检查必须命中新 Host Generation。Bot 图发布后更新进程内绑定快照，转交保留实例所有权，关闭旧图。最后关闭旧 Host 图，并结算 Host 变更账本。
6. 全程不改变消费方 Bot 的基础身份、记忆主干或群聊串行规则；只换代描述符和依赖闭包明确要求的组件。

## 失败与恢复

| 边界 | 允许的动作 |
| --- | --- |
| Host 发布之前 | 关闭所有已准备候选；逆序恢复已冻结 Bot 图和 Host 图。任一恢复或清理失败必须使相应宿主进入致命状态，不能偷偷开放旧入口。 |
| Host 已发布、某 Bot 尚未发布 | 绝不恢复该 Bot 的旧来源。继续尝试其已准备候选；失败时冻结并停用该 Bot，记录可审计的错误，使冷恢复按新 Host 来源重新装配。不得把仍运行的旧 Bot 报为 ONLINE。 |
| Bot 已发布、旧图关闭失败 | 新图保持权威；标记该 Bot 运行失败并阻止旧实例继续工作，不能回滚 Host。 |
| 进程中断 | 重启先恢复 Host 控制图，再启动 Bot。每个 Bot 从当前 Host 导出重建候选来源；已提交的 Host 变更以持久 Graph 证据结算，未提交的请求按原有重放规则处理。 |
| 无关 Host 图修订 | 提供方 Generation 与来源未变，Bot 不换代。 |

控制包的正式变更回执不能把“Host 图已发布但部分 Bot 未接管”压成无条件成功；管理面须显示 Host 持久发布结果和每个受影响 Bot 的结算/失败。由可验证的 Host Graph 证据决定是否进入“发布后”路径，不根据内存布尔值猜测。

## 必需验证

隔离 PostgreSQL 用例至少覆盖两个 Bot 共用一个 Host 驱动、一个无关 Bot、在途 Effect 调用、Host 候选预热失败、Bot 准备失败、Host 发布后 Bot 激活失败与进程冷恢复。证明受影响 Bot 及其依赖闭包换代、无关 Generation 保留、旧来源没有新调用、所有在途调用只结算一次；再用低频许可群实例验证真实 Bot 图不中断群聊串行语义。当前单图围栏的回归只能证明拒绝不安全发布，不能算作这些验收。

## 实现状态（2026-10-15）

Supervisor 已按精确旧 `source_ref` 选取 Bot 消费者，在共享门闩下准备 Bot 候选、排空旧图，随后发布 Host 并激活 Bot；Bot activation ID 由 Host activation ID 与 Bot ID 稳定派生。Host 发布后的接管失败会使受影响 Bot 和 Host 进入致命状态，Host 图变更回执以 `PARTIAL` 同时保留图证据与错误码，WebUI 将其显示为终态。Host `PARTIAL` 约束目前只存在于新的 Host 数据库基线，不能直接加载到现行 v31 数据库。

尚未完成：受影响 Bot 的逐项持久结果与中断后联合操作恢复；两个 Bot 共享驱动及无关 Bot 的真实 PostgreSQL 换代用例；在途 Effect 调用、失败注入和许可群低频实测。单元顺序验证与 Host PostgreSQL 回执验证不能代替这些验收。
