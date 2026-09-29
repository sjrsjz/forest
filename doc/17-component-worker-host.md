# `component.worker-host` 详细设计

状态：目标契约；实现完成度只看[实现状态](implementation-status.md)。本设计细化 [13 微内核运行时与控制面重构](13-microkernel-control-plane-redesign.md)，不把现有 Web gateway 或 Bot 组合根改名当成模块化。

## 所有权与启动悖论

`component.worker-host` 是 Bot scope 的官方包，具有自己的不可变工件、Generation、能力集合、私有 schema bundle、管理契约和 UI。它拥有**组件控制意图**：某 Bot 选择哪些包、启用状态、待应用配置计划、验证结果、组件操作回执，以及向内核申请代际重建。它不拥有其他组件的业务数据、包工件目录或 Web 登录会话。目标组件的配置值和配置修订由该组件私有 repository 保存；Worker Host 只通过该组件的版本化配置端口规划和提交，不复制一份可写配置真相。

Worker Host 不能负责物理启动或停止自己，也不能握住其他 Worker 对象。内核的 RuntimeGraph 执行器仅按冻结候选图执行通用的 prepare、prewarm、drain、start、publish、stop、rollback；这些动作不理解组件选择政策或配置内容。Worker Host 经受限的 `runtime.graph-replacement.request/1` 端口提交冻结控制意图，Bot Host 的租约监督循环领取请求并驱动内核执行器。Worker Host 热重载时，它自己的旧 Generation 与其他组件一样被排空；原操作的持久回执和替换请求不依赖旧 Python 对象存活。Worker Host 失效时，已有业务 Generation 可以继续运行，但组件控制写入拒绝；不得回退到 Web gateway 的直接写路径。

`kernel.runtime` 继续拥有 Bot 整体 admission、Graph/Generation 事实、Host lease 和通用图替换队列。Shell 的 `core reload` 是整个 RuntimeGraph 的维护请求，属于内核；`component reload/enable/disable/validate` 是 Worker Host 的独立操作，不能把 `core reload` 伪称为单组件热重载。若候选图暂时只能整体重新实例化，必须在结果中明示影响的所有 Generation；在能保留未变化 Generation 之前，不宣称已实现局部重载。

首个包的边界必须按下表落地，不能借用当前 gateway 的对象引用作为依赖注入：

| 项 | 目标声明 |
|---|---|
| owner / scope | `component.worker-host` / `bot`，官方 `python-worker`，`reload_mode = drain` |
| namespace / repository | `schema_id = forest.component.worker-host`，宿主派生 SQL namespace，`forest.component-worker-host.repository/1`；只写组件选择、启停意图、控制计划及回执 |
| provides | `component.host.packages/1`、`component.host.module/1`、`component.host.validate/1` 读端口；`component.host.configuration.plan/1`、`component.host.configuration.apply/1`、`component.host.enable/1`、`component.host.disable/1` 控制端口 |
| requires | 内核受限 `runtime.graph-replacement.request/1`、冻结 Graph 查询、部署包目录只读工件快照、目标组件配置端口；不直接持有 Host、registry、其他 Worker 或其 repository |
| UnitOfWork | `component.host.commit-control-intent/1` 与目标组件标准配置 apply participant 原子提交 desired 和 `PENDING_REBUILD`；内核替换请求由包内持久任务幂等提交，不通过 Web 打开跨模块事务 |
| 管理贡献 | 随本包 Generation 发布组件目录、配置、历史、验证与启停 UI；包内 UI 使用受限 artifact 操作读取 |
| 并发围栏 | Bot ID、Host lease、目标 Graph revision、目标 Generation、目标配置 revision、控制 desired revision、工件 digest 和 operation ID |
| 删除行为 | 停用保留模块存储与回执；删除工件前跨 Bot 检查选择、活动图及未结算引用；本包不可自停用 |

这些端口是待实现的版本化 ABI 名称。目标配置端口必须先作为通用包运行时能力建立，Worker Host 才能依赖；不能把 `CleanModuleConfigurationContract` 对象直接传进 Worker 进程。

## 工件、配置和持久事实

`deployment.package-registry` 向 Worker Host 提供只读、内容寻址的可安装包目录与工件校验快照。Worker Host 只保存选中的 `package_digest`、`component_id`、manifest digest 与所需能力，不读取 `web_packages` 或任意 ZIP 路径。包选择和显式启停的 Bot 级控制事实存入 `component.worker-host` 私有 schema，键为 `(bot_id, component_id)`；状态记录包含单调 `desired_revision`、当前选择、启用意图、最后接受的操作 ID、目标 graph revision。每次控制写入必须比较 expected revision，幂等重放返回原回执。

组件私有配置 repository 仍拥有其 `desired/effective` 值、配置计划、修订与不可变路径约束。每个包的标准配置管理端口读取和规划本包配置；预览时还需校验候选工件 schema、能力申请、接管声明和配置来源摘要。配置 apply 不能先写目标 repository 再调用 Worker Host，也不能把同步 `replace_runtime_graph()` 包进 Web 请求。目标包清单声明 `configuration_apply = "worker-host/1"`，其 Generation 管理契约据此发起**描述符冻结的两方事务**：本包标准配置步骤在调用方持有的事务中校验 Web ticket、plan、desired/effective CAS、不可变路径与代际围栏并写入 desired；固定的 Worker Host UnitOfWork participant 在同一事务记录 `PENDING_REBUILD` 和精确的目标配置 revision。标准配置步骤使用 `ModuleConfigurationRepository.apply_in`，不是另造一个包私有 SQL participant。事务失败时两边都不写。成功后，Worker Host owned-work 用稳定 operation ID 请求内核图替换；该内核请求不伪装成跨模块 SQL 原子提交，丢失回执时可从持久控制事实重放。effective 只由内核发布新图后的目标组件配置激活推进，管理界面必须分开展示“desired 已提交、图替换已请求、effective 已生效”。

### 配置切换的路由与恢复围栏

标准配置端口以 `(bot_id, component_id, generation_id, contract_major)` 作为完整身份。内核按冻结 Graph 和**调用包自身的描述符**绑定目标配置仓库及固定的 Worker Host participant；Worker Host 不持有目标 `CleanModuleConfigurationContract`、repository、数据库连接或私表。配置计划保留在目标私有 schema；Worker Host 只保存 plan ID、目标组件/Generation、期望配置与 Graph revision、操作 ID、授权主体和控制状态，不复制配置正文。Web ticket 在目标包的标准配置步骤中、同一事务内消费；Worker Host participant 仅接受调用包身份及目标 apply 的类型化回执，不自行信任任意浏览器字段。

每个目标包的标准配置 apply 计划只有两个静态步骤：`component_id = 本包` 的标准配置仓库步骤，以及 `component_id = component.worker-host` 的控制 participant。目标由调用包身份决定，不从请求正文读取，更不扩展成可指向任意组件的动态 UnitOfWork 槽位。协调器在同一事务检查 Bot、Graph、两方 Generation、目标配置 plan、desired/effective CAS 与控制操作幂等键；Worker Host participant 从第一步的类型化回执读取新 desired revision。这个固定协议随包快照进入 Generation 描述符，不是宿主按组件名拼接的临时事务。

控制状态按 `PENDING_REBUILD -> REBUILD_REQUESTED -> EFFECTIVE` 推进。配置 apply 的永久拒绝使两方事务一起回滚；提交以后图替换失败为 `REBUILD_FAILED`，不能把尚未激活的 desired 当成 effective，也不能回滚已提交的配置修订。任务 claim 过期后重领并重放同一内核 operation ID；旧 claim 不得结算新结果。若其他图替换先发生，Worker Host 重新读取当前 Graph 并确认目标组件仍存在、当前候选会采纳该 desired revision，再以当前 Graph revision 提交内核请求；无法满足时停在 `REBUILD_FAILED` 并暴露原因。重复 Web/Shell 操作只返回原请求，新的配置操作必须重新规划和比较 revision。跨 Generation 的配置重放须验证当前 Web 票据与 Bot 授权策略，但只读回原配置修订及控制意图，不得把新票据当作原写操作再次消费，也不得提交第二次图替换。

Worker Host 的私有表只存选择、启停、验证与控制回执，不存其他模块的配置正文、业务消息或可执行 Python 对象。Graph/Generation 和物理替换请求保留在 `kernel.runtime`；包 ZIP、安装校验和下载保留在 `deployment.package-registry`。唯一跨 schema 的原子动作必须声明 UnitOfWork participant，不能由 Web gateway 打开多个 repository 事务凑合。

## 类型化操作

所有入口调用同一组 Worker Host 管理操作；WebUI、Shell 和 CLI 不各自实现组件逻辑。下表的目标 `component_id` 是参数，不改变 operation 的拥有者；每个操作携带 Bot scope、目标 Generation/Graph revision、操作 ID 和授权主体。

| 操作 | 种类 | 返回或效果 |
|---|---|---|
| `packages` | query | 可选择的工件摘要、兼容性与安装验证状态；分页，且不返回 ZIP 内容 |
| `module` | query | 一个组件的所选工件、desired/effective 状态、Generation 与可用操作 |
| `configuration` / `configuration-history` | query | 经目标组件配置端口读取当前值或键集分页历史，密钥始终脱敏 |
| `module.validate` | query | 针对不可变工件和输入配置做无持久效果的 schema、能力、依赖、接管和资源检查 |
| `configuration.plan` | action | 冻结目标包、配置输入摘要、影响集、期望修订与有效期；不发布新图 |
| `configuration.apply` | action | 提交已授权计划，生成 desired 修订与持久重建请求，返回等待/已生效的分离状态 |
| `module.enable` / `module.disable` | 两个 action | 分别要求启用/停用 authority，保存意图并请求重建；禁止用布尔值在签票后换权限 |
| `service.invoke` | action | 仅调用 manifest 明确声明可管理的幂等服务；使用目标 Generation 和效果账本 |
| `package.management-ui` | download artifact | 只读传输当前所选摘要对应的包内 UI；按内容摘要和 Generation 校验 |

组件自身的业务管理 query/action 仍归组件自己，Worker Host 不成为 Agent、Memory、OneBot 或 Routing 的总管理代理。`service.invoke` 只给受限的通用服务调用提供入口，不能替代那些组件的管理契约。Worker Host 自带的组件目录、配置表单、历史和生命周期页面随它的 Generation 发布；包内 UI 的字节和来源证明仍由所选包快照提供。壳层只处理通用 scope、授权票据和受限 artifact 传输。

## 并发、失败和删除

控制操作的线性化点是 Bot 库中的 desired revision CAS。计划包含选中工件摘要、目标配置 revision、Graph revision、可见影响集和 plan digest；apply 重新验证这些事实，任何变化都返回冲突，不能沿用旧计划。单进程锁只用于减少重复候选装配，不能作为跨重启正确性前提。Graph 替换申请以 `(bot_id, operation_id)` 幂等；Host lease 与 activation ID 围住执行权；旧 Generation 在新图发布后不能再接受控制效果。替换失败保留旧图，desired 标记为待重试或失败，effective 不前进。

停用一个组件前必须从冻结候选图计算依赖和 Handler 接管影响。仍有必需依赖时拒绝停用；已接纳事件、Job、Session、外部效果按各拥有模块的 drain/replay 协议处理。停用不是删除数据，相关私有 schema、配置历史和 Payload 引用保持可审计；删除包工件须由部署包目录在确认无 Bot 选择、活动图、待处理重建和保留回执引用后执行。Worker Host 自身不可经其 `module.disable` 停用；它的替换走内核通用 Graph 机制。

## 落地顺序与当前差距

当前 `component.worker-host` 的包内 `modules`/`module` 只读端口可以分页列出已发布 RuntimeGraph 成员并检查单个 Generation；分页游标绑定 Graph 身份，换代后拒绝旧页。它不等于可选择的包工件目录。包内 UI 已提供持久 `module.reload` 控制入口，但尚未提供配置 apply 或显式启停。`CleanManagementGateway` 仍直接解析 `registry.get(component_id).management_contract`，调用组件配置 repository，然后持有 Bot Host 触发 `replace_runtime_graph()`；`CleanBotHost._assemble_registry()` 仍静态拼装官方组件。两者都不是上述 Worker Host，现有 Web 配置入口的可用性不证明组件控制权已经模块化。

群 Shell 的 `module.reload` 现由 Worker Host 包提供，例如 `#forest module reload --component-id agent.runtime --expect-graph 7`。它先按冻结 Graph 核对目标组件，再向 Worker Host 私有控制账本提交带 expected Graph revision 的持久请求。包内 owned-work 领取请求后，通过声明的 `runtime.graph-replacement.request/1` 内核服务提交带 Host lease 围栏的图替换请求。Web 管理 action `module.reload` 也写入同一账本；`reload` query 将包内状态与内核替换结算状态分别展示。命令回执明确列出当前实现会替换整图所有 Generation；同一 Shell 操作跨新代重放只返回原持久请求，不再排队第二次。这是组件控制入口与通用执行器之间的第一条写路径，不代表已实现局部热重载。配置 apply、enable/disable、可选择工件目录及跨模块配置提交的原子边界仍按本设计后续落地。

当前已有 Worker Host 私有 schema、真实包 manifest、Graph 成员查询与持久重载账本；可选择工件的 `packages`/`module.validate` 端口仍需通过内核受限目录快照建立。下一步建立目标组件配置端口与带 revision/plan digest 的 desired 控制账本，把配置 plan/apply 与显式启停切换到 Worker Host；最后将 Web/Shell/CLI 指向它、移除 gateway 的直接配置和图替换逻辑。每一步都要证明该包热替换后控制回执、请求和 UI 所有权不丢失；旧路由应直接删除，不建兼容别名。未完成这些步骤前，实现状态必须标记此控制面为未完成。
