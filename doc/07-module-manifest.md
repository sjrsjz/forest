# 模块 Manifest 详细设计

本文确定插件包的外部静态声明 `manifest.json`。宿主读取文件即可了解对象令牌、工件、配置及 ACL、提供和使用的接口、宿主事件接收声明，无需执行 Wasm 才能取得这些信息。

本文是设计约定。Manifest 类型、解析器、包加载器、模块运行时和 SDK 尚未实现。现有代码提供 Schema、Value、Token、内存 ACL 查询和 SQLite 配置存储。本阶段不设计自动生成工具、UI 配置、ABI 版本、版本协商或迁移分支。

## 1. 单例模块、对象与工件

一个已登记的模块对象由一个稳定 Token 标识，正常运行时只有一个活动执行实现。Manifest 的 `package.id` 声明这个 Token，宿主校验并登记，不再给同一个包创建多个具名运行实例。

```text
plugin/
├── manifest.json
└── module.wasm
```

Wasm 文件名由 `module` 字段指定，包归档和分发方式仍未确定。包是代码与声明的载体，对象是宿主中可被寻址的逻辑模块，Wasm 执行实例是实现它的执行资源。

热重载期间旧执行实例与候选实例可以暂时共存，它们共同参与同一个逻辑对象的一次交接，不构成两个独立对象，不取得两个 Token，也不复制两套配置表。

当前只考虑单个 Bot 账号；模块仍可处理多个群聊，并按群组织状态与任务。Manifest 不引入多 Bot 实体、账号选择或按 Bot 挂载配置的机制，调度器按单例模块对象派发调用。

| 包内静态声明 | 宿主维护的运行事实 |
|---|---|
| `package.id` 声明的对象 Token、工件位置 | 对象登记、实际执行来源、当前活动实现 |
| 配置键、Schema、默认值、ACL | 当前值、所属实体及挂载关系 |
| 提供的接口路径、契约及 ACL，使用的对象/接口路径对 | 目标查找、调用权限检查、队列和实际执行资源 |
| 宿主事件来源、事件路径与本模块接收接口 | notifier 静态接收注册表 |
| 可选显示名称、说明和发布标记 | Bot 账号、群作用域、运行意图、执行状态 |

模块可以声明自己的名字，不能据此伪造宿主认可的来源；只有宿主实际登记并派发的执行上下文才具有该身份。重复加载同名对象不能静默覆盖当前实现，更新须走显式交接。

## 2. 顶层字段与数据模型

使用 UTF-8 JSON，加载后进入明确的 Rust 结构。顶层固定六个必填字段：

| 字段 | 类型 | 含义 |
|---|---|---|
| `package` | `PackageInfo` | 对象稳定名称与包展示信息 |
| `module` | 包内文件路径 | Wasm 工件位置 |
| `configs` | `ConfigDeclaration[]` | 所属配置声明 |
| `provides` | `InterfaceDeclaration[]` | 当前对象提供的接口 |
| `uses` | `InterfaceUse[]` | 按对象、接口两端路径声明的依赖 |
| `subscriptions` | `EventSubscription[]` | 本模块接收的宿主事件及自己的接收接口 |

无相应声明时数组显式为 `[]`，不以缺失字段或 null 代替。拒绝未知字段和重复 JSON 对象键，避免拼写错误或重复值被静默覆盖。仅明确可选的展示字段允许省略。

概念模型如下，名称不表示 SDK 已实现这些类型：

```rust
struct Manifest {
    package: PackageInfo,
    module: PackagePath,
    configs: Vec<ConfigDeclaration>,
    provides: Vec<InterfaceDeclaration>,
    uses: Vec<InterfaceUse>,
    subscriptions: Vec<EventSubscription>,
}

struct PackageInfo {
    id: ObjectId, // 对 Token 的具名包装，就是模块的稳定对象令牌
    version: String,
    name: Option<String>,
    description: Option<String>,
}

struct ConfigDeclaration {
    key: Token,
    schema: Schema,
    default: Value,
    access: AccessPolicy,
}

struct InterfaceDeclaration {
    path: InterfacePath, // 对合法具体 Token 的具名包装
    input: Schema,
    output: Schema,
    reentrant: bool,
    access: AccessPolicy,
}

struct InterfaceUse {
    object: ObjectId,
    interface: InterfacePath,
    input: Schema,
    output: Schema,
}

struct EventSubscription {
    source: EventSourceId, // 宿主提供的事件源，不是插件生产者
    event: EventPath,
    interface: InterfacePath, // 本模块 provides 中的接收入口
}
```

ObjectId、InterfacePath、EventSourceId 与 EventPath 分别表达对象、接口、宿主事件源和事件路径，避免内部参数混用；均复用 Token，不创造第二套路径语法。JSON 对象不是动态分发脚本，字段在读取边界解析一次。

## 3. 命名与地址

### 3.1 对象 Token

`package.id` 是包作者声明的稳定对象 Token，例如 `/module/text-relay`。宿主负责名称登记、保留地址检查和冲突处理；不另设实例名或独立包令牌来重复表达同一个身份。`package.name` 仅用于展示。

该 Token 同时标识正常调用的目标、模块的可信执行来源和自身配置所属对象。create 收到宿主认可的同一个 Token。热重载保持它不变，工件版本和 Wasm 地址不进入令牌。

### 3.2 接口路径与两端地址

接口路径可以是任意合法具体 Token，包括根路径 `/`。没有固定前缀，不要求以对象、包名或某个全局接口命名空间开头；不包含通配符。合法文本与路径段规则见[令牌设计](03-token.md)。

**完整调用地址只有 `(对象 Token, 接口路径)`。** 一个接口路径对应一个可调用入口及其输入输出契约，不再叠加方法名、MethodId 或分发表序号作为第三层地址。

```text
(/module/search, /web/query)
(/module/search, /images/query)
(/module/archive, /web/query)
(/module/archive, /)
```

同一对象内接口路径唯一，不同对象可以声明相同路径并采用不同的契约。仅凭接口路径不能全局定位服务；需要连同对象 Token 查询。路径层级只组织名称，不产生父接口、继承或访问权。数组位置不构成接口身份。

接口路径是逻辑名称，不是 Rust 函数名、Wasm 导出符号或函数指针。到实际执行入口的映射由后续统一 ABI 确定；宿主建立内部路由，调用方不缓存目标的 Wasm 执行实例、函数地址或工件标识。

### 3.3 工件路径和发布标记

`module` 是使用 `/` 分隔的包内文件路径，不是 Token。它须指向包内文件，不接受绝对路径或 `..` 路径段，不作为外部下载地址或宿主任意文件路径使用。

`package.version` 是非空发布标记，仅用于展示和识别更新。当前不要求版本范围、SemVer 比较、兼容协商或多版本同时运行。构建前后的不同文件仅是同一对象的实现替换，不产生多实例模型。

## 4. 配置、默认值与 ACL

每个配置声明必须包含 `key`、`schema`、`default`、`access`；同一 Manifest 的配置键唯一，重复项不能覆盖已有声明。

配置键使用具体 Token。`/settings` 与 `/settings/prefix` 是两个独立键，不自动展开为嵌套对象；单项 Value 仍可为 Record、Map 等组合类型。

schema 与 default 直接使用现有 Forest Schema、Value 的 Serde JSON 编码，不新建 JSON Schema。默认值必须满足 `default.check(&schema, false)`：

```json
{
  "key": "/settings/prefix",
  "schema": ["String"],
  "default": {"String": ""},
  "access": {
    "readers": ["SelfModule"],
    "writers": ["SelfModule"],
    "callers": []
  }
}
```

Value 保留类型标签，例如 `{"I64": 0}`，不从裸 JSON 数字猜测 I32/I64。可选数据使用 Schema 的 Option 和相应 Value 编码，原始 JSON null 或缺失字段不能替代 Entry 或默认值。

`access.readers`、`access.writers` 与 `access.callers` 均为必填数组。配置声明使用前两项，callers 为 []；接口声明使用 callers，readers/writers 为 []。主体编码为 `"SelfModule"` 或 `{"Path": "/module/*"}`，对应现有 SubjectPattern；不将特殊字符串插入 Token 本身。

SelfModule 绑定资源所属逻辑模块的稳定 Token，与旧/新执行实例无关。空列表拒绝相应操作，读取、写入、调用三种许可独立，没有隐式管理员权限。规则只声明当前资源的访问权，见[访问声明](06-config-access.md)。

宿主将配置声明挂载到所属对象并构造 AclEntry，自身配置的实体与所有者使用已登记的模块 Token；Manifest 不另设可伪造的 owner 字段。当前先挂载模块自身声明；其他实体的挂载按实际需求再设计，不引入多 Bot 配置层次，不能任意填写另一个实体以覆盖其规则。

首次建立配置时用默认值初始化；普通重启和热重载不重新覆盖当前值。新版默认值或 Schema 与已存声明不一致时，落盘声明如何更换和交接仍待设计；Manifest 校验成功不等于完成这些修改，也不允许静默投影旧数据或部分切换声明。

## 5. `provides` 与 `uses`

provides 逐项声明当前对象提供的接口路径、完整输入 Schema 和完整返回 Schema。无参数可用单位类型 Schema；output 表达模块的整个 `Result<T, E>`，包含成功类型 T 和模块错误 E。

每项 provides 还必须明确提供布尔字段 `reentrant`：false 表示该接口已有未完成调用时，新调用排队；true 表示允许多个未完成调用。时间片被动让出保留实例执行权，模块主动让出才允许同一实例切换任务；false 接口的占用在两种让出期间都保留。具体开始、恢复与完成条件见[调度设计](09-scheduling.md)。不引入 N、`*` 或数值并发上限；uses 不重复声明提供方的调度属性。

每项 provides 必须携带静态 access: AccessPolicy，callers 列出允许调用的主体，readers/writers 显式为 []。send 与 call 均检查 Access::Call，不能将二者映射为读和写。缺少接口、主体不匹配或 callers 为空均不授予调用权；create/destroy 的通信禁令独立生效。宿主原生接口也提供同样的静态接口策略。

框架的提交失败、路由失败、Wasm 执行失败由通信协议外层表达，不混入模块 E。同一接口可以用于 call 或 send：前者等待结果，后者只返回提交结果，不另复制 send/call 两套声明。

uses 逐项声明目标对象路径、目标接口路径，以及调用方预期的 input/output。身份由两条路径共同确定；同一 uses 列表内不得重复同一个路径对。不同对象可使用相同接口路径，一个对象也可依赖另一个对象的多个接口。

```text
当前对象：/module/text-relay
依赖地址：(/module/text-transform, /text/transform)
```

这里没有 SlotId、本地别名或部署时的槽位重绑定。目标路径直接写在 uses 中，不用包版本或执行实例地址定位依赖。

宿主核对准确的目标对象与接口是否声明，并分别以双向 subof 检查调用方预期的 input/output 和提供方 Schema 结构等价。比较不依赖 JSON 字节相同，不自动做有损投影、参数转换或版本协商。

声明依赖不自动取得配置、接口调用或平台管理权限。Manifest 解析不隐式安装或启动模块；加载管理根据显式启用集合和 uses 的模块目标计算静态加载闭包与卸载影响范围，不自动安装缺失包。宿主原生接口目标与事件源只核对能力可用，不产生同名 Wasm 插件依赖，见[事件与依赖设计](11-notifier-and-dependencies.md)。依赖环的启动策略另行确定。

生命周期 create/destroy 由宿主控制，不放进 provides；两阶段都禁止 send/call，包括宿主原生服务对象。接口对 Wasm 导出的映射、调用内存交接仍由 ABI 设计确定，本文件不写函数指针或底层符号规则。

### 5.1 subscriptions

事件及其 Schema 由宿主定义。subscriptions 的 source/event 定位宿主已经提供的事件，interface 必须指向当前对象的 provides；目标对象由宿主绑定，不允许声明另一个对象代收。这里没有插件生产者、模块 events 字段或模块发布能力。

宿主检查源事件存在且对应能力已启用，其 Schema 与目标 input 双向 subof，接收接口 callers 允许该宿主事件源主体，并拒绝重复的 (source, event, interface)。模块不复制或覆盖宿主事件 Schema。宿主 notifier 将事件按普通请求的接纳和派发机制送入模块，不等待接收接口 output；subscriptions 不绕过接口 ACL。

注册表在装载和替换声明时建立，不提供运行时注册、取消订阅或 notify/emit/publish 宿主操作。投递结果由宿主处理，不返回给一个模块发布者。群消息接收示例、QueueFull 和热重载语义见[notifier 详细设计](11-notifier-and-dependencies.md)。

## 6. 完整示例

该对象提供 `/text/relay`，依赖对象 `/module/text-transform` 的 `/text/transform`，没有宿主事件接收声明。接口路径不要求与对象名称一致。两个接口的返回类型均为 `Result<String, String>`，仅用于说明结构；群消息等宿主事件的接收片段另见 notifier 文档。

```json
{
  "package": {
    "id": "/module/text-relay",
    "version": "0.1.0",
    "name": "Text Relay",
    "description": "将文本交给另一个模块处理后返回。"
  },
  "module": "module.wasm",
  "configs": [
    {
      "key": "/settings/prefix",
      "schema": ["String"],
      "default": {"String": ""},
      "access": {
        "readers": ["SelfModule", {"Path": "/module/inspector"}],
        "writers": ["SelfModule"],
        "callers": []
      }
    }
  ],
  "provides": [
    {
      "path": "/text/relay",
      "reentrant": false,
      "access": {
        "readers": [],
        "writers": [],
        "callers": [{"Path": "/module/**"}]
      },
      "input": ["String"],
      "output": [{
        "Variant": {
          "Err": {"Unnamed": ["String"]},
          "Ok": {"Unnamed": ["String"]}
        }
      }]
    }
  ],
  "uses": [
    {
      "object": "/module/text-transform",
      "interface": "/text/transform",
      "input": ["String"],
      "output": [{
        "Variant": {
          "Err": {"Unnamed": ["String"]},
          "Ok": {"Unnamed": ["String"]}
        }
      }]
    }
  ],
  "subscriptions": []
}
```

每份 Schema 是独立类型表，索引 0 为根，Ref 只引用本表内索引。无全局 types 注册表，也不跨接口或配置项引用类型编号。组合和递归类型复用现有 Schema 表达能力。

没有普通接收入口时 provides 可设为 `[]`，但不能声明接收接口不存在的 subscriptions，也不能由此推出已经存在后台任务启动机制。没有直接接口依赖时 uses 为 `[]`，无配置时 configs 为 `[]`，无宿主事件接收声明时 subscriptions 为 `[]`。

## 7. 校验与加载

```mermaid
flowchart LR
    F[读取 manifest.json] --> P[解析具体声明类型]
    P --> V[校验对象名、配置、接口与事件契约、默认值和 ACL]
    V --> D[取得不可变包描述]
    D --> R[按对象 Token 登记模块及接口]
    R --> C[按统一 ABI 建立执行资源]
```

| 阶段 | 检查内容 | 结果 |
|---|---|---|
| Manifest 读取 | 必填与未知字段、重复 JSON 键、路径、布尔 reentrant、重复配置/接口/依赖/事件接收声明 | 明确的包声明 |
| 声明校验 | Schema 完整性、默认值匹配、ACL 主体模式、Result 返回契约 | 不可变类型化描述 |
| 对象登记 | 对象路径冲突、保留身份检查、普通加载与更新的区分 | 唯一逻辑对象 |
| 工件加载 | 文件位于包内、工件可读、实际导出满足统一 ABI | 内部执行资源 |
| 依赖核对 | uses 的 input/output，宿主事件 Schema 与接收 input，模块依赖图 | 调用与宿主事件契约、加载闭包及卸载影响范围 |
| 正常操作 | 可信来源、访问控制、入口限制及通信数据类型 | 一次受控操作结果 |

声明读取与校验不写 SQLite，不因出现配置项就重置当前值，也不让插件仅凭名称获得可信来源。全部声明有效后才发布包描述，不发布半个包或建立半个配置表。

JSON 在读取边界解析一次，内部用 ObjectId、InterfacePath、Token、Schema 和 AccessPolicy；运行时通过调度器解析两端地址，不让调用者解析 Manifest 或持有对方执行实例。配置与接口各用同一 AclTable 类型的独立表，避免相同文本路径混淆资源；对未知对象/资源键对均返回 false。

## 8. 稳定地址与热重载

热重载保持原来的 `(对象 Token, 接口路径)`，不要求调用者重新发现或绑定地址。默认 Drain 完成旧实例此前已接纳的请求；AtYield/Force 由外部显式选择，允许中断未完成的旧请求，不能对这两种方式承诺调用结果完全无感。完整语义见[生命周期设计](10-module-lifecycle.md)。

1. 宿主读取候选工件及 Manifest，校验它仍声明同一个对象 Token，不提前替换当前配置规则或接口契约。
2. 候选必须保留当前已发布接口的路径及 input/output 契约；可新增接口。删除或改变既有接口契约不属于对其他包无感的替换，不能直接交接。

   候选接收声明重新核对宿主事件契约，在新实例可调度后一次切换，不能提前影响当前 notifier 注册表。插件更新不修改宿主事件定义。
3. 关闭旧实例的新请求入口并确定旧请求集合。Drain 继续处理旧集合中的队列与任务；AtYield/Force 按选定方式结束旧请求。交接期间新请求由宿主有界队列保留，容量不足返回 QueueFull。
4. 正常退出运行受限 destroy，强制或 trap 跳过来宾清理；宿主回收旧资源后运行新 create。初始化成功后切换活动实现、声明及 ACL，并发布对应静态事件注册关系，派发保留的新请求。
5. 已完成调用不重复执行。Drain 不主动弃置旧 send；非排空方式可中断未完成旧请求，call 返回框架异常，已返回的 send 不能追加失败结果。不自动重放已开始的有副作用操作。

重入规则属于提供接口的调度属性。候选 reentrant 如有变化，仅在旧实例按选定方式结束并切换声明后生效；不改变 uses 的输入输出契约，不提前改变旧实例的调用占用。主动让出仍是未完成调用，详细语义见[调度设计](09-scheduling.md)。

调用地址、逻辑对象身份和配置值保持不变。单例约束针对对外逻辑模块，不能因旧/候选执行资源共存就创建第二个对象；候选也不能提前独立对外服务。

兼容性判断发生在切换前。候选声明不合格就不进入交接，当前声明与路由保持有效。新 create 失败时释放候选资源并保持对象不可调度，不自动恢复已经销毁的旧实例。配置声明更新及外部写入如何排序仍需确定，不靠 Manifest 解析器假定已经解决。

不预设超时、自动强制卸载、版本路由或重试。Force 是外部明确选择，不是等待太久后的自动降级；地址稳定也不表示完全没有延迟或中断。

## 9. 实现分层与范围

声明类型应位于宿主与 Wasm 都能使用的纯类型层，复用 forest-serde 和 forest-util。依赖 SQLite 的 forest-config 消费配置声明，不成为插件声明库。具体 crate、接口到 Wasm 的映射 ABI、包归档和生成工具另行设计。

本文件不放 UI 配置、ABI 版本、数据迁移函数、线程数或调度期限；也不引入多实例命名、多 Bot 配置挂载、依赖槽位或全局接口路径注册。配置默认值/Schema 替换、代用户访问的入口上下文及排空细节继续见[访问声明](06-config-access.md)和[状态与热重载设计](02-state-access-and-reload.md)。
