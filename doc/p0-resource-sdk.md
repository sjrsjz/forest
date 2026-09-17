# P0 Worker 资源监督 SDK

> **历史快照｜禁止作为当前实现规格。** 本文只记录旧纵向切片及其当时的局部验证。它服从 [00 架构总契约](00-architecture-contract.md) 与 01–14 号详细设计；冲突内容已经失效。不得据此新增或恢复旧表、编号迁移、兼容读取、固定业务页面、专用宿主 API、巨型 Store、业务组合根或产品子 Agent。需要复用算法时，必须先把 owner、namespace、Repository contract、Generation、UnitOfWork participant、并发 fence 和 management contribution 映射到当前设计。

历史快照当时状态：本页描述已实现的首版 `ctx.resources` 纵向切片。它管理 Python Worker 进程内可关闭对象，不宣称提供操作系统级沙箱、持久 Job 或跨进程句柄迁移。

## 1. 所有权与接口

插件通过 `ctx.resources.register(resource, closer=None, name=None)` 把已经创建的连接、客户端、临时会话或其他可关闭对象交给当前 Context 的资源所有者。未显式提供 `closer` 时，对象必须提供 `aclose()` 或 `close()`；关闭函数可以是同步函数或 async 函数。注册返回原对象，便于在构造表达式中使用。`await ctx.resources.release(resource)` 会立即关闭并解除登记；重复释放、把同一对象登记两次或用另一作用域的 handle 释放均明确拒绝。

资源身份按对象 identity 判断，不调用插件对象的 `__eq__` 或 `__hash__`。登记数量受 manifest `resources.max_owned_resources` 约束，默认 64、范围 1–256；达到上限时拒绝新登记，并由插件负责关闭尚未成功交给框架的对象。

## 2. 调用资源与代际资源

服务调用取得的 `ctx` 登记 **call-scoped** 资源。宿主先等待该调用的受监督子 task，再以登记的逆序关闭全部调用资源，最后才允许 Worker 返回调用结果。handler 抛错、取消、deadline 到期或子 task 失败时仍执行同一清理；清理错误不会跳过其余资源，正常业务结果则被 `OWNED_RESOURCE_FAILED` 取代，避免把泄漏后的调用报告为成功。

`prepare` 保存的根 `ctx` 登记 **generation-scoped** 资源。它们可由 `start/health/quiesce/snapshot/restore/resume/stop` 使用；业务调用期间不能借保存的根 handle 登记或释放资源。正常停止顺序为：停止接纳并取消/等待调用 → 执行 `Plugin.stop(reason)` → 关闭根 task → 逆序关闭根资源 → Worker 报告 STOPPED。即使 stop hook 失败，也继续清理资源；Host 超时或 Worker 崩溃时终止整个进程，OS 回收进程资源，框架不伪称任意远端效果已经撤销。

call-scoped 资源不会进入 snapshot，也不会交给新 Generation。generation-scoped 资源同样不迁移为 Python 对象；热替换的候选自行创建新资源，路由切换并排空旧代后旧 Worker 关闭自己的资源。必须跨重启继续的工作应写入状态、效果账本或后续持久 Job，而不是把活连接放进 `Plugin.snapshot()`。

## 3. 错误与可观测性

关闭器必须可重复调用或由资源监督器保证只调用一次；Forest 对一次成功登记只主动关闭一次。关闭所有目标后，错误以有界的类型名称记录，不回显对象内容、连接串或异常正文。显式 release 失败返回 `OWNED_RESOURCE_FAILED`；批量清理继续处理其余条目后返回相同稳定错误码。

资源监督发生在 Worker 内，Host 仍以进程、IPC、调用 grant、Generation 和 route epoch 为最终边界。一个恶意可信 Python 插件仍可绕开 SDK 直接创建资源；首版只对正常 SDK 路径和进程回收给出可执行保证。

## 4. 验收

- 同一调用内多个同步/异步资源严格 LIFO 关闭，主动 release 后不重复关闭；
- handler 正常返回、抛错、取消和受监督 task 失败均清空 call-scoped 资源；
- 根资源在 stop hook 之后关闭，stop hook 失败也不跳过；
- 保存根 handle 不能在业务调用里越权登记，调用 handle 在调用结束后失效；
- 资源上限、重复对象、无关闭器和关闭失败返回稳定错误，其他资源仍获得清理；
- 真实子进程 Worker 执行服务并停止后，由文件记录观察到调用资源和代际资源的既定关闭顺序。
