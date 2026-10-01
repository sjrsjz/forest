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

当前已有 Worker Host 私有 schema、真实包 manifest、Graph 成员查询、只读 `packages` 目录与精确摘要 `artifact` 投影、持久重载账本及标准配置控制意图。私有 schema 还具备选择修订头、组件条目、幂等操作回执、内部启用/停用 SQL 端口和只读快照；宿主在构造候选图时读取快照，过滤已停用的 builtin，并从精确工件摘要装配已启用的外部包。外部包必须先有目标私有配置账本；装配时复核当前 desired 值的配置 schema 摘要与内容，未初始化或失配则拒绝。选择操作在同一私有事务内冻结操作后快照并生成独立的持久执行任务；操作事实不可改写，任务由包内 owned-work 领取、对照当前快照与 Graph revision，再向内核提交携带选择修订和摘要的 `/3` 请求。较新选择覆盖旧任务时，旧任务结算为 `SUPERSEDED`；内核请求与目标快照不符时，宿主拒绝发布。宿主在 Graph 发布期间持有与选择写入互斥的数据库围栏，旧候选不能在新的选择修订之后发布。只有冻结选择请求允许目标为旧图之外的新成员，普通重载仍要求目标属于旧图。当前选择端口已能自动驱动成员热加入和停用，Web 管理 action 已声明并在隔离库验证；包内 UI 已接通预检、启停和状态查询；Shell 启停入口尚未开放，真实实例也未部署此候选版本。目录由受限内核服务桥接宿主共用的内容寻址工件库；它不接受上传、不决定 Bot 选择，也不将 ZIP 或本地路径交给 Worker。下一步须完成 Shell/CLI 选择入口、部署工件安装权威和真实授权会话验收；提交前须拒绝不满足目标配置和依赖约束的选择。每一步都要证明该包热替换后控制回执、请求和 UI 所有权不丢失；旧路由应直接删除，不建兼容别名。未完成这些步骤前，组件控制面仍标记为未完成。

### 管理入口的预验证与提交边界

选择任务重领时，只用当前选择快照判断是否已被较新意图覆盖；它始终用原操作的 `expected_graph_revision` 和稳定 operation ID 请求内核。内核先查不可变请求账本，已有同输入请求直接重放，即使当前 Graph 已发布新修订也如此；尚无旧请求时才检查 Graph revision 并拒绝过期提交。Worker Host 不得在调用内核前用当前 Graph revision 拦截重放，否则“内核已提交、包任务未结算”的崩溃窗口会被误判为永久失败。

包内管理页现可读取显式选择快照，并分别查询选择任务和内核替换的结算结果；它不把 `SUBMITTED` 当成 Graph 已发布。当前通用 management action ABI 仅能直接绑定 Domain Command，无法把工件检查、目标配置检查和真实候选图规划作为同一授权提交的前置条件。因此不能仅将内部 `component.selection.enable/disable` 登记成 Web action。需要先建立只读 `module.validate`：输入精确工件摘要、批准能力、目标配置 revision、选择 revision 和 Graph revision，借用正式 `RuntimeModuleRegistry` 的描述符、启动依赖与保留依赖规则产生影响集合和计划摘要；验证期间不安装 schema、不创建 grant、不推进配置或选择账本。部署级包安装权威只负责内容寻址工件与版本登记，Bot 级预验证只读取该目录和目标配置账本。

提交时 Web authority 在选择写入事务内核对管理票据、计划摘要、当前选择修订以及预验证冻结的 Graph、工件和目标配置事实，随后由 Worker Host 私有命令写入选择操作与任务。任何事实变化返回冲突并要求重新验证；不同 action 键分别约束 enable 与 disable。群 Shell 使用同一计划与提交端口，但保留 Shell 自身优先级和管理员身份检查。内核只传递冻结凭据，不解释包策略；宿主最终预热与发布继续以数据库围栏核对准确快照。通用管理 ABI 必须先具备这种预验证加事务复核能力，才开放选择写入，避免一个仅有界面校验、实际可绕过的控制入口。

正式 Registry 现提供只读 `preview_graph_transition()`，从同一份候选与来源描述符计算 `replace/retain/remove/add`、旧图排空集合与新图预热集合；实际 `plan_graph_transition()` 消费该结果后才绑定保留实例的所有权。预览不准备 Generation、不安装 schema、不更改 Registry 实例表。它只是候选拓扑规划，不会自行读取或验证 Bot 选择账本、精确工件、目标配置 revision 和批准能力，也不产生可授权的计划摘要；`module.validate` 必须把这些冻结事实与预览结果一并核对，不能把这个底层方法单独暴露为管理入口。

已安装包现有共享的无持久写入检查：`inspect_installed_package()` 按精确工件与 manifest 摘要核对 Bot scope、Python Worker、批准能力和受限 SQL/Effect 声明，再从目标包私有配置账本读取指定 desired revision、schema 摘要、实际值及 Bot 生命周期不可变字段。检查结果只保留内容寻址工件/manifest/存储摘要及配置 revision、Payload 身份/摘要，不把配置正文、SQL 字节、工件路径或可写 Repository 交给控制端口。配置头、正文 Payload 和 effective 基线在同一 `REPEATABLE READ` 事务中验证；为了与 Payload 删除纪元围栏协作，该事务保留短暂的 `FOR SHARE`，但不安装 schema、不创建 grant、不写配置或选择。并发配置变更引起的序列化失败按 revision 冲突返回，要求重新规划。生产装配在 `initial_configuration=None` 时先执行同一检查，缺失或不兼容配置不能先留下安装副作用。预检结果可进入只读候选 Graph 编译；选择修订围栏、计划摘要和 Web 授权提交复核已由 Worker Host action 串接；此底层只读检查单独调用时仍不授予启停权限。

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
### 已安装工件的只读候选描述符

`compile_installed_candidate_descriptor()` 按精确工件、manifest 摘要、批准能力及目标配置修订读取候选，并与生产装配共用包描述符编译器。编译结果保留相同的组件依赖、端口、存储绑定和管理贡献，但不含可执行的 Worker factory；它只在宿主可信进程内交给 `RuntimeModuleRegistry.preview_graph_transition()` 计算影响集合，不跨 Worker 或 Web 边界传输。Registry 在准备 Graph 前拒绝含此类描述符的实例，避免预览误触发 schema、grant 或 Generation 写入。

这只解决了精确候选的静态编译和正式 Graph 规划复用。随后已接入完整冻结选择快照的成员编译及 Graph/选择/配置事实摘要；完整 `module.validate` 仍需核对资源预算，并在授权提交事务中复核。预览不是启停授权，也不是对后续发布的无条件保证。
### 冻结选择快照的只读 Graph 预览

生产装配与候选预览现在共用 `select_package_members()`：相同的 builtin 启停、内核目标禁选、官方身份冲突、精确已安装工件及 manifest 核对规则决定候选成员。拟议写入的下一修订由 Worker Host 包内只读 `component.selection.project` SQL 端口生成规范快照，宿主不再另写一份选择投影；该端口可在选择共享围栏和配置快照事务内调用。候选编译为每个启用的外部包读取并记录配置 revision、Payload 身份、schema 与存储摘要；拟启用或换代的目标外部包必须显式指定配置 revision。禁用包仍核对工件身份，但不创建 Worker。

Worker Host 包的选择写端口拒绝停用自身；宿主成员选择不按该组件 ID 再写一份业务规则。Registry 对每个描述符显式声明的 `retention_requires` 检查候选图成员完整性，缺失即拒绝预规划或装配；内核发布时还会核对冻结 Generation 的同一依赖。这使损坏的选择快照不能通过移除配置控制所依赖的 Worker Host 形成残缺图，也不会把包的选择策略复制到宿主。

宿主冷启动的选择读取器从已验证、内容寻址的 Worker Host 包快照取得私有 `schema_id` 和三个只读 Domain Query 的函数绑定，不再写死包内 SQL 函数名。候选热替换继续用当前已发布代的读取绑定持有选择围栏，并在同一围栏内核对新包快照查询得到相同选择事实；新包绑定只在新 Graph 发布成功后成为活动读取器。私有 schema 身份在同一 Bot 的热替换中必须稳定，变更需按未上线部署的显式清库重建处理，不能静默切换到另一份选择账本。

宿主可信进程的 `preview_component_change()` 在当前 Graph 替换锁和选择共享围栏内冻结选择，再交给 `preview_selected_graph()` 使用正式 Registry 依赖和换代规则返回影响集合，不安装 schema、创建 grant、准备 Generation 或启动 Worker。Worker Host 仍拥有选择和授权策略；宿主只负责不可执行描述符编译与图规划。官方包描述符来自最近一次真实装配，计划提交前必须重新装配并比对工件、配置、Graph 与选择身份；`module.validate` 已接入 Web 管理查询，`module.enable/disable` 已接入同事务授权提交；Shell 启停入口仍待接通。

`component.host.validate/1` 现作为包内只读管理查询接入。Worker Host 接收目标组件、精确工件/manifest 摘要、批准能力、启停意图以及预期选择、Graph、目标配置修订，经受限 `runtime.component-candidate.preview/1` 服务请求宿主预览；宿主在图替换锁内取得选择共享围栏、读取当前选择快照，再通过 Worker Host 包内 SQL 端口投影拟议写入，并从同一 `REPEATABLE READ` 事务快照读取候选中全部包的配置事实，编译不可执行候选。选择围栏先于配置快照建立，避免等待锁时冻结过时选择；两者使用两个连接，围栏持续至预览结束。返回选择与来源 Graph 身份、目标、`replace/retain/remove/add/drain/prepare` 影响集合、各包工件与配置事实、`kernel.runtime` 容量准入及 `plan_digest`，不返回配置正文或工件路径。官方包的内容校验与配置读取按最多四个并发只读操作执行，阻塞式目录校验移到线程执行，避免 Worker 调用期限被事件循环阻塞耗尽。此查询本身不写选择账本、安装包、准备 Generation，也不授权启停。授权提交事务中的计划事实复核已接入；启停管理页已接通，Shell 启停入口仍未完成。

候选包事实现在还包含规范化的 `worker_limits`：帧、并发、队列、状态并发、持有任务及持有资源六项都从同一个不可变包声明和 `WorkerLimits` 默认值生成，Worker 启动也使用该构造器。相同声明也固定在通用 RuntimeModuleDescriptor 的 Graph 事实中；Worker SDK 的隔离快照包含该限额模型。正式 Graph 规划器以当前全部活动 Worker 加上候选 `prepare` 集中的 Worker 组成 `worker_capacity.peak`，因此被替换的旧 Worker 与新 Worker 会同时占用容量，保留的 Worker 只计算一次。`kernel.runtime` 的版本化 Bot 配置拥有七个总量阈值：Worker 数以及六项 Worker 声明的合计；只读预览在同一配置快照内核对阈值，返回 `admission` 的配置修订、Payload 摘要、上限与峰值，计划摘要绑定这些事实。Worker Host 的选择提交在同一事务重算准入；宿主冷启动、热替换预热前和发布前再核对当前 desired 策略。运行时配置可通过该内核模块自己的管理页提交并请求 Graph 重建；配置提交先以当前 Graph 的 `kernel.runtime` 换代闭包检查峰值，不能把无法重建的策略写成 desired。此准入约束的是 Worker IPC/并发声明，不是实测内存或 CPU；操作系统级 CPU/内存硬隔离仍未实现。

Graph 规划需要管理契约的 schema 描述，但这些 schema 已在 Generation 构造时从经校验的不可变包快照编译。`PackageManagementContract.describe()` 读取隔离的已编译副本，不在每次描述符比较时重复扫描工件；包工件及当前配置本身仍在预览时按精确摘要和修订检查。

Worker Host 自带的隔离管理页提供折叠式候选预检与启停提交。页面在预检和提交前读取最新选择及 Graph 修订；外部包需要手填精确工件、manifest、批准能力与目标配置修订。结果按换代、保留、移除、新增、排空、预热显示完整组件 ID 列表、Bot 容量上限/峰值及计划摘要；提交前重新预检，调用包自己的 `module.enable/disable` action，并用返回的确定性请求 ID 查询发布状态。预检运行中禁用重复提交，结果失效或失败时不显示旧预览。

### 候选事实摘要与提交复核边界

`SelectionGraphPreview.plan_digest` 使用规范化事实计算：Bot 身份、来源 Graph ID/revision、目标组件、冻结选择 revision/digest、正式 Registry 的全部候选成员描述符摘要、候选包的配置 revision/Payload 身份/schema/存储摘要、`kernel.runtime` 策略 revision/Payload 摘要及容量上限和峰值，以及换代影响集合。官方包与已安装包均读取各自私有配置账本的 desired 值并校验当前工件 schema；官方包按最近一次真实装配的精确工件读取，预览本身不 provision。选择快照类型自身保证排序、条目修订与摘要一致。相同事实重复预览得到相同摘要；工件或配置事实改变必须得到新摘要。

全部配置事实必须来自同一数据库快照。配置头上的短暂共享锁在预览结束前持有，若另一个事务在快照建立后先改写尚未读取的配置头，预览以序列化冲突失败并要求重试，不得合成旧配置与新配置混用的摘要。工件目录按内容摘要校验且不由数据库快照保护；安装包目录需要继续保持内容不可变，提交时还须重新校验精确工件。

这个摘要只证明一次只读预览的输入身份。Worker Host 的授权提交在选择写入事务中复核来源 Graph、当前选择、精确工件、全部配置事实、Bot 容量和管理票据；宿主实际预热和发布还须重新核对 desired 容量策略。`plan_digest` 本身不是已授权启停凭据。

### 启停提交的锁序和原子复核

Web 的 `module.enable` 与 `module.disable` 是两个 action ID、两个独立授权键，但共享 Worker Host 私有选择账本。输入固定包含只读预检返回的 `plan_digest`、目标配置修订、预期 Graph 与选择修订，以及与预检完全相同的目标工件和能力批准；action 的签票正文绑定这些字段。包内 SQL 端口不自行装配 RuntimeGraph，`kernel.runtime` 核对自己的总量策略，Worker Host 保持选择和包授权策略。当前 manifest 已分别声明两个 action 并绑定事务内提交复核，包内管理页已接通预检与公开 action；Shell 凭据入口、真实 Web 会话及 CPU/内存硬隔离仍未验收。

管理 action 的执行顺序是：先由 Worker Host 的 Generation 绑定一个**必需**的提交前置检查，再在进入 Domain Command 事务之前取得宿主 Graph 替换锁；事务内先核对当前 Graph/Generation、Web 票据、Bot 策略和审计，再取得该 Bot 的选择独占 advisory 围栏，读取同一连接上的当前选择快照，并按组件 ID 顺序对候选中全部启用包的目标配置头取得共享锁。配置值、不可变字段和 Payload 身份也从同一连接验证。随后用正式的选择投影、内容寻址工件校验和 Registry 预览重新计算计划摘要；任何来源 Graph、选择、工件、配置或预算事实不符，都在调用选择写入函数前返回冲突，事务回滚。选择 SQL 在同一连接上重入该选择围栏，写不可变操作回执、操作后快照与持久执行任务；授权接受、审计和选择写入一同提交。

宿主锁必须先于 Domain Command 事务取得：当前通用入口在 `before_write` 之前已经验证活动 Generation 并进入内核 Graph 数据库围栏，而图替换按“宿主锁→数据库围栏”执行。反过来在 `before_write` 中才等待宿主锁会制造锁序环。提交前置检查必须由 manifest 明确声明且在缺失绑定时拒绝启动或拒绝 action，不允许退化为普通 Domain Command。它可以使用宿主提供的只读候选编译能力和调用方事务连接，但 Worker Host 仍拥有 action、选择 SQL、幂等和结果语义；通用包运行时只编排声明的检查，不硬编码 `component.worker-host` 的策略。

通用包运行时已接受管理 action 的 `transaction_preflight` 声明：装配 Generation 时从宿主解析对应能力，缺失则拒绝装配；action ID 必须与声明的 Domain Command 端口一致，声明了检查却丢失绑定时拒绝调用。宿主检查作用域先于数据库事务进入并一直保持至提交/回滚，当前 Web 票据验证与检查回调在 Domain Command 写入前于同一连接执行；等待锁、校验和事务执行共用 action 超时。管理 action 的输出 schema 于事务提交前验证，错误输出导致整笔写入回滚。

宿主的 `runtime.selection-commit` Graph 选择检查能力只向当前官方包绑定，Worker Host 的两个公开 action 已声明该检查。启停方向是命令输入的一部分，包内 SQL 验证该值与实际 `enable`/`disable` 端口一致，避免检查能力与写命令错绑。检查作用域先取得 Graph 替换锁，事务内取得选择独占围栏，先用包内操作探针区分准确重放；新操作读取当前选择修订，再复用同一包内选择投影与正式 Registry 规划器，按组件 ID 顺序读取并锁定全部启用包的配置头，最后比较冻结的 `plan_digest`。目标工件与当前配置由同一候选检查路径重新校验。隔离 PostgreSQL 的完整宿主用例已验证只读预览摘要可在同一 Graph 下复核，伪造摘要、超出 Bot 容量策略的提交均被拒绝且选择账本不变。Shell 尚无管理 action 凭据入口；不能从普通 Shell 服务调用选择 SQL 冒充公开启停。

同一事务内输出校验也覆盖普通 Domain Command、启动时的目录同步 Command 和清理时的目录释放 Command；这些写命令返回无效结果时，不提交包内部分写入。只读 Domain Query 保持读后输出校验。

管理 action 的 `port_kind` 由内核标记为 `management_action`，不降格为普通 `command`；Worker Host 的 `component.selection.enable/disable` SQL 只接受该标记。这样即使包持有同名 Domain Command 能力，也不能经普通命令绕过授权凭据和前置检查。包私有 SQL 仍只维护选择账本，不能代替宿主的 Graph 与候选计划复核。

同 operation ID 的准确重放需要在选择独占围栏内先查包私有不可变操作账本。若签票目标 Generation/Graph 仍然有效，输入摘要与启停方向相同就返回旧回执，不再以变化的选择或配置事实否定已经接受的操作；同 ID 不同输入仍冲突。换代后的旧 Web 票据不再有效，调用方通过持久状态查询追踪原操作。新的操作必须完成全部实时复核。事务提交后，现有 owned-work 只依冻结选择 revision/digest 请求内核 `/3`；它不能把预检时的 Graph 当作已发布事实。实际发布继续在选择共享围栏下核对准确快照。目标资源预算必须来自明确的 Bot 策略及包声明，不得把已有 IPC/队列上限伪称为 CPU 或内存保证。

Worker Host 包已提供只读 `component.selection.operation-probe` 端口，从其不可变操作账本按 Bot/operation ID 返回 `ABSENT` 或原始回执 `REPLAY`；同 ID 不同输入摘要或启停方向直接返回冲突。宿主提交检查在持有该 Bot 选择独占围栏的事务中先调用探针，准确重放不重新规划已变化的选择或配置，新操作才进入计划复核。探针不重新提交旧操作，也不绕过实际写端口的幂等复核。
