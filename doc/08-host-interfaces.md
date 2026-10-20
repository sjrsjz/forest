# 模块可用的宿主接口

本文制定 Wasm 模块需要的最小宿主接口。它是接口设计，不表示运行时、Wasm 导入或 SDK 已实现；函数名用于说明职责，底层符号、内存传递和异步恢复方式随后确定。

配置访问先采用单项 `get/set/reset`。数据冲突的处理方式由模块决定，不引入事务内模块回调、跨调用事务、版本、CAS 或自动重试。外部 I/O 采用专用 HTTP、WebSocket 和受管文件接口，不要求模块在 Wasm 内实现底层网络和文件协议。

## 1. 接口边界

| 分组 | 最小操作 | 宿主负责 |
|---|---|---|
| 模块通信 | `send`、`call` | 按对象和接口寻址、来源绑定、派发、结果关联 |
| 配置 | `get`、`set`、`reset` | ACL、类型校验、单次原子操作及持久化 |
| HTTP | `request` | 异步 HTTP(S) 请求 |
| WebSocket | `connect`、`send`、`receive`、`close` | WS(S) 连接和协议处理 |
| 受管文件 | `read`、`write`、`remove` | 模块数据目录内的文件访问 |
| 日志 | `log` | 附加真实模块身份并记录日志 |
| 时间 | `now`、`sleep` | 读取当前时间、异步等待 |

这些是专用、类型明确的宿主函数，不需要先把每项宿主能力包装为一个可安装的模块对象，也不增加一套通用字符串操作码。模块间服务仍然使用 `(对象 Token, 接口路径)` 通信。

Agent、记忆、搜索、消息触发和群聊 shell 等处理由模块实现。宿主不提供 agent.run 或 memory.recall 等处理器。群消息等外部事件由宿主接入能力产生，调度器 notifier 投递到模块声明的接收入口，不经过模块发布接口。平台原生操作可按宿主对象和接口地址提供，与模块调用共享寻址方式；不把 notifier 当成模块可调用的能力。

宿主接口的存在不表示任意生命周期阶段都能执行它。候选实例何时能产生写入或外部 I/O，由生命周期统一决定，不能由每个接口自行创造一套重载规则。

## 2. 身份与调用上下文

`create` 接受宿主登记的自身对象 Token。模块可以保存该值作为目标地址，不需要另外调用“申请令牌”接口。

宿主在派发接口时提供只读调用上下文：当前执行模块、可信发起者以及已有的请求关联。Bot、群和消息等信息属于具体模块的类型化输入，不放进所有模块必需的宿主上下文。

所有宿主操作从当前执行上下文取得来源，不接受模块传入 `source`、管理员标记或其他可覆盖身份的参数。后续 `send/call` 及异步恢复保留来源；资源操作不能把来源变成宿主自身。

模块直接访问内部配置时，以实际执行模块为主体检查 ACL。WebUI 等已认证用户直接访问配置时，以该用户为主体。代用户跨模块执行管理操作的入口契约尚需确定，不能通过增加一个由模块任选的身份参数解决，见[配置 ACL](06-config-access.md)。

## 3. 返回与错误

沿用[模块通信设计](01-module-runtime.md)的区分：调度与执行失败属于外层 `DispatchError`；被调用接口的正常返回数据，包括模块自己的 `Result<T, E>`，属于内层。

```rust
type DispatchResult<R> = Result<R, DispatchError>;

// 对可能返回自身操作错误的宿主服务，概念上表达为：
type HostResult<T, E> = DispatchResult<Result<T, E>>;
```

例如，配置项不存在、值类型不匹配属于配置操作错误，HTTP 网络失败属于 HTTP 操作错误；它们不扩充成调度异常。权限拒绝由宿主授权边界报告。各错误使用明确的枚举及字段，不以字符串内容判断流程；错误枚举的完整代码定义不在本文提前展开。

模块可处理的错误必须作为正常 ABI 数据返回，不能因它们发生就终止 Wasm。异步等待、时间片让出和重载期间的派发等待不是错误。

错误返回不普遍承诺操作未产生副作用。例如 HTTP 对端可能已收到请求。宿主不自动重试，丢弃等待也不表示撤销已经执行的操作。

## 4. 模块通信

```rust
send(object: Token, interface: Token, input: Value) -> DispatchResult<()>
call(object: Token, interface: Token, input: Value) -> DispatchResult<Value>
```

这里的 Value 表达跨 Wasm 边界的数据，SDK 按声明的输入输出类型提供编码和解码，不要求模块作者手工遍历动态字典。接口路径为任意合法具体 Token，不增加固定前缀或第三层方法名。

- `send` 成功只表示宿主接受请求，不等待接收方完成，也不提供随后查询结果的任务句柄。需要结果就使用 `call`。
- `call` 等待接收方返回完整输出；若输出声明为 `Result<T, E>`，该结果整体位于外层 DispatchResult 内。
- 目标接纳容量不足时，send/call 在提交阶段返回 `Err(DispatchError::QueueFull)`；本次请求未入队，目标不会执行它。宿主不另建隐藏等待队列，也不自动丢弃旧消息腾位置。已接受任务的恢复不受这一提交检查阻挡。
- 宿主依据 Manifest 的 `uses/provides` 核对地址与类型，不由调用者提交函数名、Wasm 实例或返回目标。
- 请求接纳前按目标接口静态 access.callers 检查可信直接调用主体，send/call 使用相同的调用许可。复用现有 ACL 查询，uses 不代替权限声明。
- 正常返回就是对当前 call 的响应，不增加 `reply(request_id, ...)` 接口。

create/destroy 都禁止 send/call，包括自身、其他模块和宿主原生服务对象。宿主在接纳前检查生命周期阶段，不因 ACL 允许而放行，也不缓存在初始化成功后发送。

热重载保持对象身份、地址和结果关联，默认 Drain 完成已接受的旧请求，AtYield/Force 可显式中断旧请求；模块不需要重绑定。同一实例的执行权、被动与主动让出、接口重入限制和队列规则见[调度设计](09-scheduling.md)。嵌套等待的死锁处理不属于当前范围。

宿主事件转发向每个接收入口提交请求时沿用相同的接纳语义。某个接收者 QueueFull 不撤回其他接收者已经接受的请求；逐目标提交结果由宿主事件接入和管理诊断处理，不作为模块发布操作的返回值。

### 4.1 宿主事件接收

notifier 是宿主内部机制，不是模块宿主函数。模块在 Manifest 的 subscriptions 中声明接收哪些宿主事件，以及对应的 provides 接口；事件及其 Schema 由宿主定义。接收接口的 access.callers 须允许对应可信宿主事件源。调度器按静态注册表提交普通请求，不直接重入处理器、不等待其完成，也不保存历史待补发，见[notifier 设计](11-notifier-and-dependencies.md)。

模块没有 notify/emit/publish、register/subscribe/unsubscribe 宿主操作。模块之间的主动通信使用 send/call，不扩展为插件事件生产与订阅机制。

## 5. 配置

```rust
get(object: Token, key: Token) -> HostResult<Value, ConfigError>
set(object: Token, key: Token, value: Value) -> HostResult<(), ConfigError>
reset(object: Token, key: Token) -> HostResult<(), ConfigError>
```

`object` 是目标实体，`key` 是准确的配置项路径，不是通配符。读取取得当前值副本，不返回可修改引用，不隐式建立条目。模块知道自己的声明，普通读取不必每次附带 Schema 和默认值；条目元数据由宿主已装载的声明持有。

`get` 检查读取权；`set/reset` 检查写入权。`set` 校验新值符合声明，原子替换这一项的当前值；`reset` 在宿主内部直接恢复声明默认值，不由模块先读默认值再写入。两者都不改变 Schema、默认值或 ACL。成功返回表示 SQLite 已提交。

每次操作独立完成。分开的 `get` 和 `set` 中间允许其他写入，不形成原子读改写；宿主不合并值、不重试，也不保证互相覆盖的修改满足交换律。模块按自身需要组织数据与协调访问。

本阶段不开放批量读取、批量写入、事务回调、`begin/commit`、锁句柄或 CAS。已有 `forest-config` 原生库的 `read/apply/update` 是宿主 API，不是必须移植到 Wasm 的协议。原生库的导出、导入和实体注册同样不属于普通模块接口，见[配置表实现](05-config-table.md)。配置 UI 的列举与元数据接口在 UI 设计时单独确定。

## 6. HTTP

```rust
request(request: HttpRequest) -> HostResult<HttpResponse, HttpError>

struct HttpRequest {
    method: HttpMethod,
    url: String,
    headers: Vec<Header>,
    body: Vec<u8>,
}

struct HttpResponse {
    status: u16,
    headers: Vec<Header>,
    body: Vec<u8>,
}
```

方法、请求和响应是具体类型，HTTP 头用列表保留重复字段。第一版只做完整请求和完整响应，不先引入流式 body、客户端池句柄或可编程中间件。HTTPS 连接和证书验证由宿主实现。

HTTP 4xx/5xx 是有效 HttpResponse，不被调度器改写成失败；如何解释状态和响应正文由模块决定。重定向先作为响应返回，不自动重放请求；重试和响应内容解析同样由模块决定。

搜索与模型供应商模块可用这个接口访问各自服务，具体供应商地址、密钥和模型参数不进入宿主接口。

## 7. WebSocket

```rust
connect(request: WebSocketRequest) -> HostResult<WebSocketHandle, WebSocketError>
send(handle: WebSocketHandle, message: WebSocketMessage) -> HostResult<(), WebSocketError>
receive(handle: WebSocketHandle) -> HostResult<WebSocketEvent, WebSocketError>
close(handle: WebSocketHandle) -> HostResult<(), WebSocketError>
```

连接请求包含 URL 和握手头；消息为文本或二进制的明确枚举。接收结果区分消息与正常关闭，正常关闭不伪装成调度失败。帧拆装、Ping/Pong 和 TLS 由宿主处理，OneBot 等上层协议由模块解释。

句柄由宿主创建并绑定实际执行实例，不能仅凭句柄数值操作其他实例的连接。模块自行安排连接的收发顺序，不对同一连接并发发起多个 receive。宿主只提供操作，不引入自动重连、订阅恢复或消息重放。

这组接口描述模块主动建立连接。OneBot HTTP 上报或反向 WebSocket 的监听属于宿主 OneBot 接入能力，收到协议事件后交给 notifier；不为此要求模块取得通用监听接口。其他模块的服务器监听需求出现时另行设计。

## 8. 受管文件

```rust
read(path: ManagedPath) -> HostResult<Vec<u8>, FileError>
write(path: ManagedPath, data: Vec<u8>) -> HostResult<(), FileError>
remove(path: ManagedPath) -> HostResult<(), FileError>
```

ManagedPath 是模块数据目录内的相对文件路径，不复用表示对象身份的 Token。宿主从执行模块确定数据根目录，普通文件接口没有任意宿主路径或其他模块根目录参数；根目录随稳定模块对象保留，不随 Wasm 重建而清空。

宿主保证访问留在受管目录内，不能只检查字符串前缀而允许 `..` 或符号链接绕出目录。模块不获得原生文件描述符。第一版读取或写入完整文件，write 表示替换文件内容；不提供流式句柄、随机访问、文件锁或跨文件事务，也不承诺文件写入与配置写入组成一个事务。

文件接口用于模块自身的附件和数据文件。配置仍经配置接口读写，不能借文件接口直接打开宿主配置数据库。包内只读资源的读取方式随包加载设计确定，不混入任意宿主文件访问。

## 9. 日志与时间

```rust
log(level: LogLevel, message: String) -> DispatchResult<()>
now() -> DispatchResult<i64>
sleep(duration_ms: i64) -> DispatchResult<()>
```

日志等级使用枚举，模块只提交等级和正文；宿主附加实际模块身份及已有调用关联，不从正文解析可信来源。

now 返回 Unix 毫秒，用于时间标记。sleep 接受非负毫秒数，宿主使用适合计时的单调时钟，不阻塞执行线程，也不把等待时长变成调用超时。sleep 只暂停当前执行，不创建独立后台任务。数值使用现有 forest-serde 支持的 i64。

本阶段没有定时任务注册表、cron、spawn、join 或单独的模块主动 yield 宿主接口。任务通过 SDK 异步推进结果明确交出执行权，不以“调用了宿主函数”自动触发重入；恢复与唤醒语义见[调度设计](09-scheduling.md)。主动工作如何开始仍待生命周期设计确定。

## 10. 生命周期与下一步

配置和受管目录属于稳定逻辑对象；WebSocket 等连接属于具体执行资源。句柄不能持久化到配置表后被新实例继续使用。宿主在执行实例退出时回收其剩余资源，不依赖模块总能正常运行 destroy。

外部卸载/重载可选择 Drain、AtYield 或 Force，不自动迁移网络等待、Future 或连接。Drain 等待旧请求完成，AtYield 在主动返回边界中断剩余旧请求并运行 destroy，Force 跳过来宾清理。具体顺序及 destroy 白名单见[生命周期设计](10-module-lifecycle.md)。

create 禁止 send/call，允许初始化需要的基础宿主操作；请求不会延后至初始化成功再发送。普通入口只在 create 成功返回后开放，已有配置提交和网络副作用不因初始化失败撤销。

destroy 阶段宿主允许配置 get/set/reset、受管文件访问、日志、时钟和释放既有资源；禁止 send/call（包括原生服务对象）、新任务、新网络请求或连接、应用消息收发以及 sleep。限制由宿主执行，配置 ACL 仍有效，不能通过接口包装绕过。

接下来需要确定的是：

1. 按[调度设计](09-scheduling.md)和[状态机](12-scheduler-state-machine.md)落地模块任务推进、I/O 唤醒及 Wasm 内存传递 ABI；执行权、接口许可、结果归属与阶段转换已明确。
2. 静态依赖环的装载规则与主动任务如何开始；create 已禁止 send/call，destroy 白名单及三种卸载方式也已明确。
3. 将宿主事件来源、原始发起者及代用户操作落实为具体调用上下文；OneBot 事件沿用 v11 原始结构，监听属于宿主接入实现。

这些决定完成后再固定底层 ABI 和实现顺序。接口授权采用静态 access.callers，不因此增加 ABI 版本、动态授权表或通用监听协议。
