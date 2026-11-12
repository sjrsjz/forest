# 模块包声明与文件格式

包通过根目录的 `manifest.toml` 声明稳定身份、元信息、Wasm 工件、接口、配置、事件订阅及文件摘要。Schema 与默认 Value 使用 Forest 文本文件，Manifest 中只保存文件引用。

`forest-package` 是宿主侧的统一包读取与打包库。它将磁盘格式解析为 `forest-protocol::Manifest`，后者包含完整的 Schema、Value 和静态访问规则。调度器不读取 TOML，也不根据文件名解释接口。

## 1. 包目录

```text
example/
├── manifest.toml
├── module.wasm
├── README.md                 # 可选
├── LICENSE                   # 可选
├── assets/                   # 可选静态资源
└── generated/                # Rust 打包器生成的声明文件
    ├── lifecycle-error.schema
    ├── configs/
    │   ├── 0.schema
    │   └── 0.value
    ├── provides/
    │   ├── 0.input.schema
    │   └── 0.output.schema
    └── uses/
        ├── 0.input.schema
        └── 0.output.schema
```

引用以包根目录为基准，使用 `/` 分隔的相对文件路径。文件路径与 Token 是不同概念：前者定位包内文件，后者标识对象、接口或配置项。绝对路径、反斜杠、盘符、空段、`.` 和 `..` 不作为包内引用接受；文件访问受包目录约束。

`generated/` 由 Rust 打包器管理并在重新打包时替换。文件序号仅用于生成可移植的文件名，不构成接口或配置项身份。其他语言可以直接编写 TOML、Schema 和 Value 文件，不要求采用这个目录布局或链接 Rust crate。

## 2. TOML 结构

以下示例展示各类声明，摘要由打包器填写实际值：

```toml
[package]
id = "/module/example"
name = "Example"
version = "0.1.0"
description = "An example module"
authors = ["Example Author"]
repository = "https://example.org/source"
license = "MIT"
readme = "README.md"

[module]
file = "module.wasm"
lifecycle-error = "generated/lifecycle-error.schema"

[[configs]]
key = "/settings"
schema = "generated/configs/0.schema"
default = "generated/configs/0.value"
[configs.access]
readers = ["self"]
writers = ["self", "/user/admin"]

[[provides]]
path = "/render"
input = "generated/provides/0.input.schema"
output = "generated/provides/0.output.schema"
[provides.access]
callers = ["/module/**"]

[[uses]]
object = "/module/media"
interface = "/get"
input = "generated/uses/0.input.schema"
output = "generated/uses/0.output.schema"

[[subscriptions]]
source = "/module/onebot"
event = "/group_message"
interface = "/render"

[sha256]
# 每个有效负载文件对应一个实际的 64 位十六进制 SHA-256。
```

`package`、`module` 和 `sha256` 必须存在；空的 configs、provides、uses、subscriptions、events 可以省略。未知字段和重复 TOML 键会被拒绝。订阅的输入类型必须与对应宿主事件兼容，示例中的接口和事件需要按实际用途选择。

## 3. 身份与元信息

| 字段 | 用途 |
|---|---|
| `package.id` | 必填，稳定的单例对象 Token |
| `package.version` | 必填，非空发布标记 |
| `package.name`、`description` | 可选展示名称和简介 |
| `package.authors` | 可选作者名称列表 |
| `package.repository` | 可选源码仓库地址 |
| `package.license`、`license-file` | 可选许可证说明及包内许可证文件 |
| `package.readme` | 可选包内说明文件 |
| `package.homepage`、`documentation` | 可选主页和文档地址 |
| `package.keywords` | 可选检索标签 |

一个对象正常运行时只有一个活动实现。热重载保持 `package.id` 不变，不创建额外实例名或执行实例 Token。`version` 用于展示和识别发布，接口兼容性由 Schema 判断，不由版本号推断。

元信息不参与鉴权。模块来源由宿主绑定，访问权限由静态 ACL 决定。README 和许可证文件也必须列入摘要索引；仓库、主页与文档地址只作为展示信息，不自动下载或执行。

## 4. Schema 与默认 Value

`module.lifecycle-error`、配置 Schema、接口输入输出均引用 Forest Schema 文本。例如：

```text
schema Request = { text: string };
export Request;
```

配置默认值引用 Forest Value 文本，例如：

```text
{ text: "hello" }
```

Schema 文件可以声明辅助类型和递归结构，通过 export 指定根类型。Value 文件保存具体值，不在读取时填补字段或猜测类型。每个配置声明必须同时提供 Schema 和默认值，默认值必须符合 Schema。具体语法和结构子类型见[序列化与类型契约](serialization.md)。

生命周期错误同时用于 create/destroy。SDK 将具体错误转成 Value，宿主按所属实例的 Schema 校验并保存阶段与载荷，见[运行与 ABI](13-runtime-and-sdk.md)。

## 5. 接口、依赖与 ACL

完整调用地址为 `(对象 Token, 接口路径)`。接口路径可以是任意合法具体 Token，包括 `/`，不要求固定前缀；只在所属对象内唯一。文件名、数组序号、Rust 函数名和 Wasm 函数地址都不构成接口身份。

provides 必须声明输入 Schema、完整 `Result<T, E>` 输出 Schema、访问规则。uses 指定目标对象、目标接口及调用方要求的输入输出契约，不使用依赖槽位或版本范围。subscriptions 指定宿主事件及本模块 provides 中的接收接口，不声明模块事件生产能力。

ACL 的主体采用统一文本表示：

- `"self"`：声明所属逻辑模块，对应 SelfModule。
- `"/module/**"` 等绝对路径模式：按已有 Token 通配符规则匹配可信主体。

配置的 access 只使用 readers/writers；接口的 access 只使用 callers。空列表或省略的许可列表表示不授予该项权限。read、write、call 相互独立，不隐含自身权限、父路径权限或管理员旁路。主体列表语法不改变[ACL 查询语义](06-config-access.md)。

## 6. 摘要与读取流程

sha256 是包内相对文件路径到 SHA-256 的映射，摘要直接计算文件原始字节。Wasm、全部引用的 Schema/Value、README、许可证及随包资源都属于有效负载。Manifest 本身不列入这张表；整包归档的摘要由分发记录保存。

读取流程如下：

1. 从包根读取一次 TOML，解析具体声明与摘要索引。
2. 读取声明文件，先校验摘要，再解析 Schema/Value；同一文件在一次读取过程中复用同一份字节。
3. 校验配置默认值、接口 Result 契约、重复声明、订阅和静态 ACL，形成完整 Manifest。
4. 实际装载时校验索引中的全部负载文件，将已验证的同一份 Wasm 字节交给编译器。
5. 校验固定 ABI 后，再进入加载或热重载流程。

管理端目录扫描只读取声明及其引用的说明文件，不读取或编译 Wasm。实际装载时的摘要失败会在旧实例停止之前报告。运行快照展示当前实例所对应 Wasm 的摘要，而非目录中可能已经替换的文件。

Rust 打包器从类型化 Manifest 生成声明文件，计算摘要，最后替换 TOML。它保留包内其他静态资源并将其列入索引。摘要校验用于内容一致性；作者与摘要字段不会自动获得可信身份。

## 7. 热重载约束

候选必须保持对象身份，保留已发布接口，并满足 `old.input <: new.input`、`new.output <: old.output`。依赖按 `uses.input <: provides.input`、`provides.output <: uses.output` 检查，完整 Result 的错误分支也参与判断。

配置键集合及 Schema 保持等价，默认值可以更新并保留全部当前值。生命周期错误 Schema 属于具体执行实例，允许随候选改变；错误始终携带所属实例的契约。元信息和文件摘要不替代这些兼容检查。

普通加载求静态依赖闭包，允许依赖环；宿主在联合候选目录中检查接口，再初始化并统一发布整组成员。生命周期、执行交接、ACL 发布及请求队列仍按[模块生命周期](10-module-lifecycle.md)和[调度器状态机](12-scheduler-state-machine.md)执行。

## 8. 事件与发布后激活

[module] 可声明 activated = "/activated"，指向本包的 Unit 输入普通接口。其 callers 允许 /system/lifecycle，整组发布后以绑定实例的控制请求运行一次。它不改变固定 Wasm 导出，也不建立额外初始化屏障。

[[events]] 声明 path、input（Schema 文件引用）及 access.readers；不允许 writers/callers。生成器把 Schema 放在 generated/events/，同样纳入 SHA-256 索引。subscriptions 可以指向模块事件，形成静态依赖；源 readers 和目标接口 callers 同时检查，事件输入必须是目标输入子类型。来源包替换时事件输入只能向旧契约的子类型收窄。见[事件设计](11-notifier-and-dependencies.md)。
