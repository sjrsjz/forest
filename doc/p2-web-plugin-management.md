# P2 Web 插件与配置管理

> **历史快照｜禁止作为当前实现规格。** 本文只记录旧纵向切片及其当时的局部验证。它服从 [00 架构总契约](00-architecture-contract.md) 与 01–14 号详细设计；冲突内容已经失效。不得据此新增或恢复旧表、编号迁移、兼容读取、固定业务页面、专用宿主 API、巨型 Store、业务组合根或产品子 Agent。需要复用算法时，必须先把 owner、namespace、Repository contract、Generation、UnitOfWork participant、并发 fence 和 management contribution 映射到当前设计。

> **历史文档原标签：实现切片。** 本页只描述包工件与可安装组件的管理协议。控制台结构、作用域和 UI 所有权以 [12](12-webui-administration.md) 与 [13](13-microkernel-control-plane-redesign.md) 为准。不存在一个拥有包仓库、Bot 目录、运行维护和全部组件配置的“模块管理后台”。旧专用包 API 和固定壳层页面已经删除，后续实现不得恢复。

插件包与依赖格式见[插件包说明](p2-plugin-packages.md)。旧“状态迁移”切片已撤销；当前 Generation 切换与可选 state handoff 只按[生命周期设计](02-kernel-lifecycle.md)执行，开发数据库 schema 直接重建。

## 操作流程

1. 管理员先进入 deployment scope 的 `deployment.package-registry` 贡献，通过它的 `package.import` artifact operation 上传完整 ZIP。导入只把内容寻址工件登记到部署包仓库，不选择任何 Bot 的运行版本。
2. 管理员再通过通用 scope opener 进入目标 Bot。该 Bot 的 `component.worker-host` 贡献从受限包目录端口读取可选工件，用 `packages`、`configuration` 和 `configuration-history` query 展示候选与当前状态。
3. `configuration.plan` 固定包摘要、配置、能力、handoff 声明和预期修订；`configuration.apply` 只提交这一不可变计划。候选准备、在途调用排空、必要状态交接与健康检查成功后，目标 Bot 数据库在受围栏事务中发布路由与 effective。
4. `module.validate` 只做无持久副作用的验证；`module.enable` 与 `module.disable` 是两个权限固定的 action，不能用一个布尔字段在 handler 内动态选择 authority。`service.invoke` 依据已发布服务 schema 调用实际 Worker。
5. 配置历史按稳定游标分页。载入历史只生成新草稿，重新 plan/apply 后创建新修订；不会倒退 revision。历史包必须仍可按摘要取得；运行状态格式不同时只允许 02 定义的显式 handoff，持久 schema 不兼容时首发前直接拒绝并重建开发数据库。

常见的闭合对象 schema 可以由宿主兜底表单编辑。包也可以携带与摘要、Generation 和 schema revision 绑定的管理 UI；该 UI 在受限 iframe 中只交换草稿补丁、校验和计划请求，不能持有管理 Cookie、拼接 API URL 或直接调用任意管理动作。最终始终由服务端 schema 和模块契约校验。日常配置保存在权威存储中，管理员无需编辑服务器配置文件。

配置读取期间的新增输入、应用期间的新草稿、组件选择，以及调用期间切换服务或修改参数，都有独立的界面版本检查。切换 scope、组件或 Generation 时旧控制器必须销毁其状态和未完成响应，避免旧表单参与新的请求。壳层只挂载 `ManagementContribution`，不识别包字段、组件业务语义或上述 operation 的页面布局。

## 登记与启用的边界

上传使用通用 artifact 路由：

```text
POST /management/v1/scopes/deployment/{deployment_id}/modules/deployment.package-registry/uploads/package.import
```

`deployment.package-registry` 的描述符固定 `application/zip`、最大字节数、管理员主体和幂等要求。通用网关在读取正文前完成 scope、ACTIVE Generation、会话、Origin、CSRF、`Content-Length` 和并发门禁，再把有界流交给模块。JSON action 仍受独立的小正文上限约束。旧 `POST /management/v1/packages` 不存在，也不提供兼容别名。

保留命名空间 `forest.*`、`adapter.*`、`core.*` 在任何工件发布前拒绝。完整内容可以先写入本机内容寻址缓存，随后部署数据库在同一事务复检当前会话、固定 `(package_id, version) → digest` 并记录审计。拒绝或取消的请求可能留下孤儿缓存，但不能占用版本名，也不会进入 Fleet 可用包列表。

当前部署最多登记 256 个外部版本。没有自动删除历史包或缓存 GC；这部分属于后续容量管理。插件版本一经登记不可替换，修复插件须发布新版本。包导出通过同一模块声明的 download artifact operation 完成，只对已登记的可用摘要开放并重新校验当前管理主体。

原生 Python 插件需要可信来源。独立 Worker、SDK 权限和锁定依赖管理运行协议与生命周期，不是对恶意本机 Python 代码的操作系统沙箱。

## 重启与失败恢复

部署重启从登记摘要重新校验工件；单个外部包缺失或损坏会记录该包错误，不妨碍管理后台与其他包恢复。启用的具体实例由每个 Bot 的配置权威与宿主租约决定。

若 desired 是坏版本、effective 仍为上一可用版本，Fleet 重启会先恢复**当前 effective**，再重试 desired。恢复操作绑定当前 effective 修订、包摘要、配置、能力、当前 policy 与宿主；发布时再次检查，不能借恢复把任意旧修订标为 effective。状态从当前 Bot 数据库读取，不覆盖为历史值。

升级失败保留当前生效实例。宿主死亡后，新的有效宿主可终结原宿主遗留的操作；同一有效宿主也可以受限地终结自己已过期、仍精确持有资源与 op epoch 的失败操作，恢复旧入口。过期权限仍不能发布或续租。

`component.worker-host` 的贡献展示期望与生效修订、运行错误和有界的持久组件切换记录。该投影不是完整日志系统；Agent、Memory、Routing、审计、备份和恢复由各自模块/控制面服务贡献，不得汇总回一个固定组件后台。

包自带配置界面通过 Bot scope 的只读 `package.management-ui` download artifact 取得。它是 `component.worker-host` 的工件传输操作，不是 deployment 包仓库的浏览器旁路，也不是可以修改状态的管理 action。连同该 artifact，当前契约共有十一项稳定操作；前十项 query/action 的业务语义仍以上述列表为准。
