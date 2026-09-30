# `component.worker-host` 详细设计

状态：目标契约；实现完成度只看[实现状态](implementation-status.md)。本设计细化 [13 微内核运行时与控制面重构](13-microkernel-control-plane-redesign.md)，不把现有 Web gateway 或 Bot 组合根改名当成模块化。

## 所有权与启动悖论

`component.worker-host` 是 Bot scope 的官方包，具有自己的不可变工件、Generation、能力集合、私有 schema bundle、管理契约和 UI。它拥有**组件控制意图**：某 Bot 选择哪些包、启用状态、待应用配置计划、验证结果、组件操作回执，以及向内核申请代际重建。它不拥有其他组件的业务数据、包工件目录或 Web 登录会话。目标组件的配置值和配置修订由该组件私有 repository 保存；Worker Host 只通过该组件的版本化配置端口规划和提交，不复制一份可写配置真相。

Worker Host 不能负责物理启动或停止自己，也不能握住其他 Worker 对象。内核的 RuntimeGraph 执行器仅按冻结候选图执行通用的 prepare、prewarm、drain、start、publish、stop、rollback；这些动作不理解组件选择政策或配置内容。Worker Host 经受限的 `runtime.graph-replacement.request/2` 端口提交冻结控制意图，Bot Host 的租约监督循环领取请求并驱动内核执行器。Worker Host 热重载时，它自己的旧 Generation 与其他组件一样被排空；原操作的持久回执和替换请求不依赖旧 Python 对象存活。Worker Host 失效时，已有业务 Generation 可以继续运行，但组件控制写入拒绝；不得回退到 Web gateway 的直接写路径。

`kernel.runtime` 继续拥有 Bot 整体 admission、Graph/Generation 事实、Host lease 和通用图替换队列。Shell 的 `core reload` 是整个 RuntimeGraph 的维护请求，属于内核；`component reload/enable/disable/validate` 是 Worker Host 的独立操作，不能把 `core reload` 伪称为单组件热重载。若候选图暂时只能整体重新实例化，必须在结果中明示影响的所有 Generation；在能保留未变化 Generation 之前，不宣称已实现局部重载。

首个包的边界必须按下表落地，不能借用当前 gateway 的对象引用作为依赖注入：

| 项 | 目标声明 |
|---|---|
| owner / scope | `component.worker-host` / `bot`，官方 `python-worker`，`reload_mode = drain` |
| namespace / repository | `schema_id = forest.component.worker-host`，宿主派生 SQL namespace，`forest.component-worker-host.repository/1`；只写组件选择、启停意图、控制计划及回执 |
| provides | `component.host.packages/1`、`component.host.module/1`、`component.host.validate/1` 读端口；`component.host.configuration.plan/1`、`component.host.configuration.apply/1`、`component.host.enable/1`、`component.host.disable/1` 控制端口 |
| requires | 内核受限 `runtime.graph-replacement.request/2`、冻结 Graph 查询、部署包目录只读工件快照、目标组件配置端口；不直接持有 Host、registry、其他 Worker 或其 repository |
| UnitOfWork | `component.host.commit-control-intent/1` 与目标组件标准配置 apply participant 原子提交 desired 和 `PENDING_REBUILD`；内核替换请求由包内持久任务幂等提交，不通过 Web 打开跨模块事务 |
| 管理贡献 | 随本包 Generation 发布组件目录、配置、历史、验证与启停 UI；包内 UI 使用受限 artifact 操作读取 |
| 并发围栏 | Bot ID、Host lease、目标 Graph revision、目标 Generation、目标配置 revision、控制 desired revision、工件 digest 和 operation ID |
| 删除行为 | 停用保留模块存储与回执；删除工件前跨 Bot 检查选择、活动图及未结算引用；本包不可自停用 |

这些端口是待实现的版本化 ABI 名称。目标配置端口必须先作为通用包运行时能力建立，Worker Host 才能依赖；不能把 `CleanModuleConfigurationContract` 对象直接传进 Worker 进程。

## 工件、配置和持久事实

`deployment.package-registry` 向 Worker Host 提供只读、内容寻址的可安装包目录与工件校验快照。Worker Host 只保存选中的 `package_digest`、`component_id`、manifest digest 与所需能力，不读取 `web_packages` 或任意 ZIP 路径。包选择和显式启停的 Bot 级控制事实存入 `component.worker-host` 私有 schema，键为 `(bot_id, component_id)`；状态记录包含单调 `desired_revision`、当前选择、启用意图、最后接受的操作 ID、目标 graph revision。每次控制写入必须比较 expected revision，幂等重放返回原回执。

宿主从目录按精确 `package_digest` 装配已安装包时，必须核对 Bot scope、Python Worker runtime、静态配置 schema 和批准的能力集合。框架提供的标准配置账本在宿主派生的独立 namespace 以可信 baseline 安装；外部包自带的 SQL bundle 只能单独按 `declarative` 配置安装并绑定其 schema 内容与执行配置摘要，不能把两者拼接后使用 `official` 执行权限。存储摘要独立于 Python 工件摘要，使代码换代而 SQL 契约不变时可复用已安装 schema；RuntimeGraph 仍以工件摘要区分代码 Generation。没有自有 SQL bundle 的包不声明 Generation storage binding，仍使用独立配置账本。现有装配入口已落实这个边界，并在隔离 PostgreSQL 中通过同一外部包两个不同摘要的热重载；它尚未接入 Worker Host 的选择事实，也未建立 Web 安装权威。需要 SQL 函数、事务 participant 或 owned-work SQL 的外部包仍须专门的受审查 authority，不能把这些声明悄悄降级为普通表。

组件私有配置 repository 仍拥有其 `desired/effective` 值、配置计划、修订与不可变路径约束。每个包的标准配置管理端口读取和规划本包配置；预览时还需校验候选工件 schema、能力申请、接管声明和配置来源摘要。配置 apply 不能先写目标 repository 再调用 Worker Host，也不能把同步 `replace_runtime_graph()` 包进 Web 请求。目标包清单声明 `configuration_apply = "worker-host/1"`，其 Generation 管理契约据此发起**描述符冻结的两方事务**：本包标准配置步骤在调用方持有的事务中校验 Web ticket、plan、desired/effective CAS、不可变路径与代际围栏并写入 desired；固定的 Worker Host UnitOfWork participant 在同一事务记录 `PENDING_REBUILD` 和精确的目标配置 revision。标准配置步骤使用 `ModuleConfigurationRepository.apply_in`，不是另造一个包私有 SQL participant。事务失败时两边都不写。成功后，Worker Host owned-work 用稳定 operation ID 请求内核图替换；该内核请求不伪装成跨模块 SQL 原子提交，丢失回执时可从持久控制事实重放。effective 只由内核发布新图后的目标组件配置激活推进，管理界面必须分开展示“desired 已提交、图替换已请求、effective 已生效”。

### 配置切换的路由与恢复围栏

标准配置端口以 `(bot_id, component_id, generation_id, contract_major)` 作为完整身份。内核按冻结 Graph 和**调用包自身的描述符**绑定目标配置仓库及固定的 Worker Host participant；Worker Host 不持有目标 `CleanModuleConfigurationContract`、repository、数据库连接或私表。配置计划保留在目标私有 schema；Worker Host 只保存 plan ID、目标组件/Generation、期望配置与 Graph revision、操作 ID、授权主体和控制状态，不复制配置正文。Web ticket 在目标包的标准配置步骤中、同一事务内消费；Worker Host participant 仅接受调用包身份及目标 apply 的类型化回执，不自行信任任意浏览器字段。

每个目标包的标准配置 apply 计划只有两个静态步骤：`component_id = 本包` 的标准配置仓库步骤，以及 `component_id = component.worker-host` 的控制 participant。目标由调用包身份决定，不从请求正文读取，更不扩展成可指向任意组件的动态 UnitOfWork 槽位。协调器在同一事务检查 Bot、Graph、两方 Generation、目标配置 plan、desired/effective CAS 与控制操作幂等键；Worker Host participant 从第一步的类型化回执读取新 desired revision。这个固定协议随包快照进入 Generation 描述符，不是宿主按组件名拼接的临时事务。

包与不可卸载的内核 authority 均以 `ComponentConfigurationApplyScope` 调用标准配置协调器；这个作用域只承载组件、Generation、调用 ID 与管理 operation ID。`PackageCallScope` 只用于包 Worker 的普通服务、状态和通用 UnitOfWork，不伪装内核 authority。协调器固定生成 `management.configuration` 因果根，不能接受调用方提供的另一种根来记录重建意图。

`component.configuration-rebuild.record` 是标准管理事务专用端口。普通包的通用 UnitOfWork 计划不得引用它；包清单在发布前拒绝该引用，Host participant 还核对 `management.configuration` 因果根。普通插件可以声明自己的 UnitOfWork 计划，但不能用自造的“配置已提交”前序回执驱动组件重建。

控制状态按 `PENDING_REBUILD -> REBUILD_REQUESTED -> EFFECTIVE` 推进。配置 apply 的永久拒绝使两方事务一起回滚；提交以后图替换失败为 `REBUILD_FAILED`，不能把尚未激活的 desired 当成 effective，也不能回滚已提交的配置修订。任务 claim 过期后重领并重放同一内核 operation ID；旧 claim 不得结算新结果。若其他图替换先发生，Worker Host 重新读取当前 Graph 并确认目标组件仍存在、当前候选会采纳该 desired revision，再以当前 Graph revision 提交内核请求；无法满足时停在 `REBUILD_FAILED` 并暴露原因。重复 Web/Shell 操作只返回原请求，新的配置操作必须重新规划和比较 revision。跨 Generation 的配置重放须验证当前 Web 票据与 Bot 授权策略，但只读回原配置修订及控制意图，不得把新票据当作原写操作再次消费，也不得提交第二次图替换。

Worker Host 的私有表只存选择、启停、验证与控制回执，不存其他模块的配置正文、业务消息或可执行 Python 对象。Graph/Generation 和物理替换请求保留在 `kernel.runtime`；包 ZIP、安装校验和下载保留在 `deployment.package-registry`。唯一跨 schema 的原子动作必须声明 UnitOfWork participant，不能由 Web gateway 打开多个 repository 事务凑合。

通用 Web 配置栏在 apply 返回时只能显示 desired 已提交及 Worker Host 重建请求 ID。它应通过 Worker Host 的持久 `configuration-rebuild` 查询观察 `EFFECTIVE` 或 `REBUILD_FAILED`，并在旧 Generation 消失、查询暂时冲突时重试；只有查到 `EFFECTIVE` 后才能显示“已生效”。模块自带 UI 收到的提交事件也只代表 desired 提交，不得自行把该事件解释为 effective。页面离开当前 Bot/工作区时停止轮询，后续仍可按请求 ID 在 Worker Host 页面查询。

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

`module.validate` 的依赖、接管和资源判断必须使用与实际候选图装配相同的冻结描述符及 `RuntimeModuleRegistry` 规划规则。启动依赖参与有向无环启动顺序；普通服务句柄、UnitOfWork 与配置控制属于保留依赖，允许成环但影响换代闭包。旧 `module_validation.py` 把所有服务要求合成单一 DAG，生产路径也未使用它，现已删除；不得将它的规则复制进 Worker Host 查询以制造与实际发布不同的第二套规划器。

## 并发、失败和删除

控制操作的线性化点是 Bot 库中的 desired revision CAS。计划包含选中工件摘要、目标配置 revision、Graph revision、可见影响集和 plan digest；apply 重新验证这些事实，任何变化都返回冲突，不能沿用旧计划。单进程锁只用于减少重复候选装配，不能作为跨重启正确性前提。Graph 替换申请以 `(bot_id, operation_id)` 幂等；Host lease 与 activation ID 围住执行权；旧 Generation 在新图发布后不能再接受控制效果。发布前替换失败保留旧图，desired 标记为待重试或失败，effective 不前进。发布事务已提交而本地激活失败时，内核撤下候选图并封闭路由；旧图已经退役，不得假称回滚成功或恢复旧图 admission。替换请求记录失败，Bot 宿主进入失败态并释放两代资源，等待新的宿主重新构造图。发布成功但旧图排空失败也使宿主失败，不能将资源泄漏当成成功替换。

### 包选择快照与发布线性化

选择账本以每 Bot 单调修订为头，以组件 ID 为键记录工件来源、启用意图、不可变工件/manifest 摘要和显式批准的能力。`module.enable` 与 `module.disable` 使用不同管理 action 和授权键；两者在 Worker Host 私有事务内取得同一 Bot 的独占选择围栏，先核对全局选择修订 CAS，再写组件事实与幂等操作回执。部署包目录负责校验精确工件，目标组件配置仓库负责初始配置与后续 desired/effective；选择账本不存配置正文。对于首次启用的外部包，候选预热前必须已经有经授权、按该工件配置 schema 初始化的目标配置账本；未满足时拒绝选择申请，不得用空对象悄悄初始化。

Worker Host 提交内核替换请求时，冻结所接受选择操作的全局修订、规范化快照摘要、目标组件和期望 Graph revision；内核只持久转运这些不透明控制凭据，不解释包配置或启停政策。旧操作若在提交前已被较新选择修订覆盖，应结算为被取代，不能读取新选择并冒充旧请求。宿主领取请求后按冻结修订读取 Worker Host 的只读选择快照，按精确摘要装配候选；成员增删与依赖闭包仍由同一个 RuntimeModuleRegistry 规则规划。对新增成员，内核不能套用“目标必须已在当前图”的普通重载检查。

预热前读取和比较快照不足以阻止随后写入。宿主在候选发布期间必须持有与 Worker Host 选择写入互斥的 Bot 级共享数据库围栏，重新比较修订和摘要，并在该围栏释放前完成内核 Graph 发布；选择写入持有独占围栏，且不得在同一事务内反向等待 Graph 发布锁。这样旧候选发布与较新选择写入有明确的先后顺序：先写入则旧候选拒绝发布；先发布则新选择操作的持久任务必须重新核对当前 Graph revision，过时请求不能沿用旧 revision 发布。进程锁、UI 游标和轮询结果均不能代替该数据库顺序。冷启动同样使用选择快照；若已接受的 desired 无法验证或装配，宿主应停在明确失败态，不能静默恢复静态全量 builtin 图。

停用一个组件前必须从冻结候选图计算依赖和 Handler 接管影响。仍有必需依赖时拒绝停用；已接纳事件、Job、Session、外部效果按各拥有模块的 drain/replay 协议处理。停用不是删除数据，相关私有 schema、配置历史和 Payload 引用保持可审计；删除包工件须由部署包目录在确认无 Bot 选择、活动图、待处理重建和保留回执引用后执行。Worker Host 自身不可经其 `module.disable` 停用；它的替换走内核通用 Graph 机制。

## 落地顺序与当前差距

当前 `component.worker-host` 的包内 `modules`/`module` 只读端口可以分页列出已发布 RuntimeGraph 成员并检查单个 Generation；分页游标绑定 Graph 身份，换代后拒绝旧页。`packages` 只读端口经受限服务路由读取部署共用的 `PackageCatalog` 内容寻址目录，逐页校验包字节，返回 manifest 摘要、组件身份和能力申请；目录修订变化拒绝旧页。`artifact` 端口按精确 SHA-256 工件摘要重新校验不可变包，并投影配置 schema 摘要、能力申请、服务依赖/提供声明与存储执行权限；包内 UI 可检查这些声明。两者仅供候选工件浏览，不构成已授权的选择、启停或完整兼容性验收。包内 UI 已提供持久 `module.reload` 和配置重建状态查询。18 个 builtin 包和两项可配置的内核 authority 均从自身 Generation 发起标准配置事务，原子提交 desired 与 Worker Host 控制意图；Web gateway 只路由已授权的管理契约，不直接写配置或同步替换 Graph。`CleanBotHost._assemble_candidate()` 仍逐个 provision 官方包，但最终成员集合已受 Worker Host 选择快照约束。

群 Shell 的 `module.reload` 由 Worker Host 包提供，例如 `#forest module reload --component-id agent.runtime --expect-graph 7`。它先按冻结 Graph 核对目标组件，再向 Worker Host 私有控制账本提交带 expected Graph revision 的持久请求。包内 owned-work 领取请求后，通过声明的 `runtime.graph-replacement.request/2` 内核服务提交带 Host lease 围栏的图替换请求。Web 管理 action `module.reload` 也写入同一账本；`reload` query 将包内状态与内核替换结算状态分别展示。命令回执说明目标及其依赖闭包会换代；实际成员变化以发布后的 Graph 为准；同一 Shell 操作跨新代重放只返回原持久请求，不再排队第二次。配置 apply 现有标准两方事务，但 enable/disable 与工件选择控制仍待完成。

当前已有 Worker Host 私有 schema、真实包 manifest、Graph 成员查询、只读 `packages` 目录与精确摘要 `artifact` 投影、持久重载账本及标准配置控制意图。私有 schema 还具备选择修订头、组件条目、幂等操作回执、内部启用/停用 SQL 端口和只读快照；宿主在构造候选图时读取快照，过滤已停用的 builtin，并从精确工件摘要装配已启用的外部包。外部包必须先有目标私有配置账本；装配时复核当前 desired 值的配置 schema 摘要与内容，未初始化或失配则拒绝。选择操作在同一私有事务内冻结操作后快照并生成独立的持久执行任务；操作事实不可改写，任务由包内 owned-work 领取、对照当前快照与 Graph revision，再向内核提交携带选择修订和摘要的 `/3` 请求。较新选择覆盖旧任务时，旧任务结算为 `SUPERSEDED`；内核请求与目标快照不符时，宿主拒绝发布。宿主在 Graph 发布期间持有与选择写入互斥的数据库围栏，旧候选不能在新的选择修订之后发布。只有冻结选择请求允许目标为旧图之外的新成员，普通重载仍要求目标属于旧图。当前内部选择端口已能自动驱动成员热加入和停用，但尚未发布为 Web/Shell 管理操作。目录由受限内核服务桥接宿主共用的内容寻址工件库；它不接受上传、不决定 Bot 选择，也不将 ZIP 或本地路径交给 Worker。下一步须建立部署工件的安装权威和完整 `module.validate`，再开放经过授权与校验的 Web/Shell/CLI 选择入口；提交前须拒绝不满足目标配置和依赖约束的选择。每一步都要证明该包热替换后控制回执、请求和 UI 所有权不丢失；旧路由应直接删除，不建兼容别名。未完成这些步骤前，组件控制面仍标记为未完成。

### 管理入口的预验证与提交边界

包内管理页现可读取显式选择快照，并分别查询选择任务和内核替换的结算结果；它不把 `SUBMITTED` 当成 Graph 已发布。当前通用 management action ABI 仅能直接绑定 Domain Command，无法把工件检查、目标配置检查和真实候选图规划作为同一授权提交的前置条件。因此不能仅将内部 `component.selection.enable/disable` 登记成 Web action。需要先建立只读 `module.validate`：输入精确工件摘要、批准能力、目标配置 revision、选择 revision 和 Graph revision，借用正式 `RuntimeModuleRegistry` 的描述符、启动依赖与保留依赖规则产生影响集合和计划摘要；验证期间不安装 schema、不创建 grant、不推进配置或选择账本。部署级包安装权威只负责内容寻址工件与版本登记，Bot 级预验证只读取该目录和目标配置账本。

提交时 Web authority 在选择写入事务内核对管理票据、计划摘要、当前选择修订以及预验证冻结的 Graph、工件和目标配置事实，随后由 Worker Host 私有命令写入选择操作与任务。任何事实变化返回冲突并要求重新验证；不同 action 键分别约束 enable 与 disable。群 Shell 使用同一计划与提交端口，但保留 Shell 自身优先级和管理员身份检查。内核只传递冻结凭据，不解释包策略；宿主最终预热与发布继续以数据库围栏核对准确快照。通用管理 ABI 必须先具备这种预验证加事务复核能力，才开放选择写入，避免一个仅有界面校验、实际可绕过的控制入口。

### 局部换代的内核边界

当前 `RuntimeModuleRegistry` 用冻结描述符摘要和启动/保留依赖计算换代闭包，只为闭包及新增成员创建新实例与 Generation；闭包外的原实例、Worker 进程、Generation 身份及配置 effective 均不重绑定。候选 Router 登记保留提供方以接收新图入站调用；保留实例内部的旧句柄继续使用原 Router，并由内核核对调用方和提供方准确 Generation 的当前成员身份。Graph 发布成功后，旧 Registry 只关闭换代或移除的成员，原 Router 随被保留的 Generation 保持可用；新 Registry 负责这些实例的监督和最终关闭。纯新增不排空旧图的任何成员；纯移除允许候选图全部引用保留 Generation，而不强迫虚构一个新代。

Registry 的候选规划现从当前已发布图及冻结描述符计算 `replace | retain | remove | add` 成员集合，宿主对成员变化调用该规划；目标组件、其包摘要/配置生效修订变化者，以及持有受影响组件冻结启动路由或贡献目录的消费者进入替换闭包。闭包外组件只有在原 Generation、描述符摘要、配置生效修订、Host 围栏与路由依赖都不变时才能 `retain`。Worker Host 仍缺少把这一规划与包选择事实、精确影响集合、资源预算一起写入持久控制意图的能力；宿主已按快照增删候选成员，但 Graph 替换请求尚未冻结快照身份。预热只为 `replace/add` 创建新 Worker。`retain` 不执行 `prepare/start/stop`，不重新绑定其不可变 Generation 身份，也不改写其私有配置 effective 或 owned-work 租约。

新图成员事实可以引用仍然 `ACTIVE` 的旧 Generation。内核持久层现按来源 Graph ID、准确 Generation ID、当前 Host lease 和冻结依赖事实验证保留成员：保留者的依赖若指向换代或移除组件，候选准备直接拒绝；发布时来源图必须仍是当前图，新建成员仍须 `READY`。发布事务只把 `replace/remove` 旧代变为 `QUIESCING`，只对这些路由增加 epoch；`retain` 的 Generation 与路由 epoch 保持原值，且不重复推进配置 effective 或发布回调。历史图不能把当前图保留的 Generation 标为停止。Worker Host 的目标重载已使用这些内核事务能力。候选发布前失败时，宿主只清理本次新建 Worker，不关闭借用的旧代。

服务句柄在每次新调用入场时冻结**当前图**的路由快照；已入场调用继续用原调用树的准确 Generation/route epoch 完成，前提是目标未被换代且授权仍有效。持有旧 Graph ID 的句柄不能在发布后自动获得调用新目标的权力；对 `replace/remove` 的旧路由必须由 epoch 拒绝。保留组件若需调用换代目标，必须进入替换闭包并按自身 handoff 协议重建，除非它声明并实现可验证的安全边界重新绑定契约。这样既允许独立组件保持运行，也不把旧 Generation 偷偷伪装为新代。局部发布、失败回滚和宿主关闭都须按实际成员所有权只排空/释放一次；热重载验收须核对未变化 Generation ID、进程、配置生效修订和 owned-work 租约保持不变，而目标及依赖闭包获得新 Generation。

内核路由器已在一次事务内核对当前图中原调用方和原服务提供方的准确 Generation；仅两者都被保留时，旧句柄本次调用的路由快照才改用当前 Graph 身份。服务操作账本以当前图记录新尝试，结算则验证执行 Generation 仍是当前成员，因此保留者跨图完成的持久调用不会因原图 ID 而失败。外部效果的 `runtime.graph` 围栏接受准确原图成员身份，但必须同时证明执行 Generation 仍在当前图；这不允许已被换代的执行者重放旧 permit。标准配置事务等延迟绑定的 participant 依赖现已纳入保留依赖；宿主现按替换闭包排空并在发布成功后转移实例所有权；旧 Router 保留到其最后一个原 Generation 关闭。

### 目标组件与保留依赖的当前落地

Worker Host 已把目标 `component_id` 随 `runtime.graph-replacement.request/2` 写入内核持久请求；内核在期望 Graph revision 上核对目标成员，并把目标纳入幂等摘要。`RuntimeModuleRegistry.replacement_closure()` 按启动依赖和单独的保留依赖计算需要换代的反向闭包。保留依赖包括普通服务句柄、UnitOfWork participant 和配置控制引用；这些引用可以成环，不能并入启动 DAG。冻结 Generation 绑定及内核图事实分别记录保留依赖，候选图保留旧 Generation 时会验证其所依赖的组件也被保留。

宿主现消费持久请求中的目标，比较候选与当前冻结描述符，再只为目标、描述符变化者、新增成员及其依赖消费者创建新 Generation。候选预热借用闭包外实例，发布前不接管关闭所有权；成功发布后接管，旧 Registry 只释放被换代或移除的实例。隔离 PostgreSQL 的最小真实图、Counter 包成员增删及完整官方包宿主用例已验证这一执行边界。Worker Host 持久选择、启停和部署工件安装权威仍待完成；静态宿主装配尚不会自行改变成员集合。

内核 admission 围栏现在接受明确的成员集合，并依据当前图持久的启动依赖与保留依赖拒绝不闭合的集合；只有选中成员的路由 admission 和 epoch 发生变化，失败恢复也只重新打开选中成员。整图维护仍使用同一个端口的全图模式。宿主现在把替换闭包传给此端口；`core reload` 仍使用整图模式。

Registry 的替换排空现接受与内核围栏相同的成员集合：只冻结选中组件的 owned-work gate、排空其任务并执行其 `begin_replacement`/`quiesce`；保留成员的 Router admission 保持开放。失败回滚只恢复选中组件的本地 gate、任务状态和内核路由。整图替换仍可省略成员集合，保持原有全图生命周期。

保留 Worker 的内部服务句柄绑定原 Router 及冻结的 Graph 路由身份。候选 Registry 可以取得实例的最终监督与关闭所有权，但不得把原 WorkerIdentity 改写成新 Graph 身份，也不得在发布后立即销毁原 Router。已发布的新 Registry 接管保留实例的监督与最终关闭；旧 Registry 只清理被换代实例，保留原路由上下文，直到最后一个借用者关闭时再释放。旧路由仍须在调用方和提供方的准确 Generation 都为当前成员时才能跨图调用。
