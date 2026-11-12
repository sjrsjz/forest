# 模块可用的宿主接口

本文记录 Wasm 模块使用的最小宿主能力。当前运行时、导入与 SDK 已实现，实际符号、字节协议和异步恢复见[运行与 SDK](13-runtime-and-sdk.md)；下列概念签名用于说明职责。

配置访问先采用单项 `get/set/reset`。数据冲突的处理方式由模块决定，不引入事务内模块回调、跨调用事务、版本、CAS 或自动重试。外部 I/O 采用专用 HTTP、WebSocket 和受管文件接口，不要求模块在 Wasm 内实现底层网络和文件协议。

## 1. 接口边界

| 分组 | 最小操作 | 宿主负责 |
|---|---|---|
| 模块通信 | `send`、`call`、`emit` | 按对象和接口寻址、来源绑定、派发、结果关联 |
| 配置 | `get`、`set`、`reset` | ACL、类型校验、单次原子操作及持久化 |
| 互斥 | `lock`、`unlock` | 具体 Token 命名、任务持有、FIFO 等待与资源回收 |
| HTTP | `request` | 异步 HTTP(S) 请求 |
| WebSocket | `connect`、`send`、`receive`、`close`、`watch` | WS(S) 连接和协议处理 |
| 受管文件 | `read`、`write`、`remove` | 模块数据目录内的文件访问 |
| 私有 SQLite | `sqlite` | 执行模块专用数据库内的一次原子事务 |
| 日志 | `log` | 附加真实模块身份并记录日志 |
| 时间 | `now`、`monotonic_now`、`sleep`、`timer_after` | 日历时间、单调时间、等待和实例通知 |
| 随机 | `random_bytes` | 操作系统安全随机字节 |

这些是专用、类型明确的宿主函数，不需要先把每项宿主能力包装为一个可安装的模块对象，也不增加一套通用字符串操作码。模块间服务仍然使用 `(对象 Token, 接口路径)` 通信。

Agent、记忆、搜索、消息触发和群聊 shell 等处理由模块实现。宿主不提供 agent.run 或 memory.recall 等处理器。外部协议由接入 Wasm 包解析并通过自身声明的事件发布；宿主只提供通用 I/O。模块事件与原生事件共用静态接收关系。

宿主接口的存在不表示任意生命周期阶段都能执行它。候选实例何时能产生写入或外部 I/O，由生命周期统一决定，不能由每个接口自行创造一套重载规则。

## 2. 身份与调用上下文

`create` 接受宿主登记的自身对象 Token。模块可以保存该值作为目标地址，不需要另外调用“申请令牌”接口。

宿主在派发接口时提供只读调用上下文：当前执行模块、可信发起者以及已有的请求关联。Bot、群和消息等信息属于具体模块的类型化输入，不放进所有模块必需的宿主上下文。

所有宿主操作从当前执行上下文取得可信 caller。origin 是 Principal(Token) 或 External { issuer, subject }；emit 可以声明本地外部 subject，issuer 必须由宿主绑定为实际发布模块，不能生成 Principal 或覆盖可信 caller。后续 `send/call` 及异步恢复保留来源；资源操作不能把来源变成宿主自身。

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

ABI 帧不设固定 16 MiB 上限，长度由 wasm32 的 `u32` 表示，实际分配仍受实例内存配额约束。无法用该长度表示的编码结果属于 `DispatchError::FrameTooLarge`，结束本次调用，不标记模块不可调度。宿主 I/O 的 body 限制独立于模块间帧长度。

HTTP 响应、WebSocket 消息与帧、文件读取的上限来自宿主配置 `/core/runtime` → `/io`，单位 MiB，默认各 64。配置写入由管理员 ACL 授权；模块发起 I/O 时只使用宿主读取到的上限，不能在请求中绕过。具体生效边界见[运行与 SDK](13-runtime-and-sdk.md)。

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

热重载保持对象身份、地址和结果关联，默认 Drain 完成已接受的旧请求，AtYield/Force 可显式中断旧请求；模块不需要重绑定。同一实例的执行权、被动与主动让出、显式宿主锁和队列规则见[调度设计](09-scheduling.md)。嵌套等待的死锁处理不属于当前范围。

宿主事件转发向每个接收入口提交请求时沿用相同的接纳语义。某个接收者 QueueFull 不撤回其他接收者已经接受的请求；逐目标接纳结果由 emit 返回；原生接入也能通过宿主 API 取得相同结果。

### 4.1 宿主锁

```rust
lock(key: Token) -> DispatchResult<()>
unlock(key: Token) -> DispatchResult<()>
```

同一 Wasm 实例中的相同具体 Token 命名同一把独占、非递归锁；不同实例与不同代际的锁表完全独立。锁名不表示授权身份或宿主内部互斥锁。持有者由当前可信实例与任务决定，调用方不能指定；跨接口可以使用相同锁名。lock 按导入提交顺序 FIFO 等待，不阻塞实例或执行线程；未就绪时由 SDK 返回 Pending。

锁授予后先归操作完成槽持有，operation_take 成功复制结果后转给任务。取消未取走的等待或授予会释放许可；取走以后由 unlock 或任务清理释放。非持有者 unlock 返回 Denied。unlock 的结果使用相同 operation 协议，但提交后立即就绪；SDK 可同步取走，支持 LockGuard 的 Drop。

create/destroy 的 lock/unlock 返回 LifecycleRestricted。任务结束时先终结已提交且不可中断的原生工作，再释放其已持有锁。trap/Force 停止整个实例，取消等待，不再推进任何来宾任务。详细所有权见[调度规则](09-scheduling.md#5-显式宿主锁)。

### 4.2 事件与资源通知

emit(event, input, subject) 发布当前模块 Manifest 中声明的事件，按 subscriptions 转交普通请求并逐目标报告接纳结果。静态检查事件输入、源 readers 和接收接口 callers。不提供动态订阅或历史回放，见[事件与依赖](11-notifier-and-dependencies.md)。

ws_watch(handle, interface) 监听当前实例的一条 WS 结果；处理器输入 IoEvent::WebSocketReady(handle)，通过 ws_receive 取走原始结果后才继续接收。timer_after(delay_ms, interface) 投递一次 IoEvent::Timer(handle)。回调输入必须兼容 IoEvent，callers 允许 /system/io。watch/timer_after/emit 在 create/destroy 禁止。

普通模块事件面向对象，可交接未启动请求；资源通知及激活绑定实例，不能交接到新实例。空闲监听不占来宾任务；已接纳回调必须参与 Drain 静止检查。取消和 join 原生监听属于实例资源回收。

## 5. 配置

```rust
get(object: Token, key: Token) -> HostResult<Value, ConfigError>
set(object: Token, key: Token, value: Value) -> HostResult<(), ConfigError>
reset(object: Token, key: Token) -> HostResult<(), ConfigError>
```

`object` 是目标实体，`key` 是准确的配置项路径，不是通配符。读取取得当前值副本，不返回可修改引用，不隐式建立条目。模块知道自己的声明，普通读取不必每次附带 Schema 和默认值；条目元数据由宿主已装载的声明持有。

`get` 检查读取权；`set/reset` 检查写入权。`set` 校验新值符合声明，原子替换这一项的当前值；`reset` 在宿主内部直接恢复声明默认值，不由模块先读默认值再写入。两者都不改变 Schema、默认值或 ACL。成功返回表示 SQLite 已提交。

每次操作独立完成。分开的 `get` 和 `set` 中间允许其他写入，不形成原子读改写；宿主不合并值、不重试，也不保证互相覆盖的修改满足交换律。模块按自身需要组织数据与协调访问。

Wasm 配置接口不提供批量操作、事务回调、`begin/commit`、锁句柄或 CAS。`forest-config` 的 `read/apply/update`、导出、导入与实体注册属于宿主 API，见[配置表](05-config-table.md)。管理端通过受限 Inspector 列举条目和读取元数据，详见[WebUI](14-inspection-webui.md)。

## 6. HTTP

```rust
request(request: HttpRequest) -> HostResult<HttpResponse, HttpError>

struct HttpRequest {
    method: HttpMethod,
    url: String,
    headers: Vec<Header>,
    body: Vec<u8>,
    timeout_ms: Option<u64>,
}

struct HttpResponse {
    status: u16,
    headers: Vec<Header>,
    body: Vec<u8>,
}
```

timeout_ms 为从宿主执行至完整响应 body 的相对期限；None 显式不设期限，超时返回 ServiceError::Timeout，不自动重试或撤销远端效果。WS connect/send 同样支持 timeout_ms。

方法、请求和响应是具体类型，HTTP 头用列表保留重复字段。接口提供完整请求和响应，不提供流式 body、客户端池句柄或中间件。HTTPS 连接和证书验证由宿主实现。

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

这组接口描述模块主动建立连接。HTTP 上报、反向 WS 等服务器功能需要未来的通用监听资源，不在内核保留平台专用接入路径。

## 8. 受管文件

```rust
read(path: ManagedPath) -> HostResult<Vec<u8>, FileError>
write(path: ManagedPath, data: Vec<u8>) -> HostResult<(), FileError>
remove(path: ManagedPath) -> HostResult<(), FileError>
```

ManagedPath 是模块数据目录内的相对文件路径，不复用表示对象身份的 Token。宿主从执行模块确定数据根目录，普通文件接口没有任意宿主路径或其他模块根目录参数；根目录随稳定模块对象保留，不随 Wasm 重建而清空。

宿主将文件访问限制在受管目录内，阻止 `..` 或符号链接越界，模块不获得原生文件描述符。read/write 读写完整文件，write 替换内容；不提供流式句柄、随机访问、文件锁、跨文件事务或跨配置事务。

文件接口用于模块自身的附件和数据文件。配置仍经配置接口读写，不能借文件接口直接打开宿主配置数据库。包内只读资源的读取方式随包加载设计确定，不混入任意宿主文件访问。

## 9. 日志与时间

```rust
log(level: LogLevel, message: String) -> DispatchResult<()>
now() -> DispatchResult<i64>
sleep(duration_ms: i64) -> DispatchResult<()>
```

日志等级使用枚举，模块只提交等级和正文；宿主附加实际模块身份及已有调用关联，不从正文解析可信来源。

now 返回 Unix 毫秒，用于时间标记。sleep 接受非负毫秒数，宿主使用适合计时的单调时钟，不阻塞执行线程，也不把等待时长变成调用超时。sleep 只暂停当前执行，不创建独立后台任务。数值使用现有 forest-serde 支持的 i64。

宿主不提供定时任务注册表、cron、spawn、join 或单独的主动 yield 接口。任务通过 SDK 的推进结果交出执行权，调用宿主函数本身不触发重入。工作由 send/call 或宿主事件接纳，恢复与唤醒见[调度设计](09-scheduling.md)。

## 10. 生命周期限制

配置和受管目录属于稳定逻辑对象；WebSocket 等连接属于具体执行资源。句柄不能持久化到配置表后被新实例继续使用。宿主在执行实例退出时回收其剩余资源，不依赖模块总能正常运行 destroy。

外部卸载/重载可选择 Drain、AtYield 或 Force，不自动迁移网络等待、Future 或连接。Drain 等待旧请求完成，AtYield 在主动返回边界中断剩余旧请求并运行 destroy，Force 跳过来宾清理。具体顺序及 destroy 白名单见[生命周期设计](10-module-lifecycle.md)。

create 禁止 send/call/emit 与 ws_watch/timer_after，允许初始化需要的基础宿主操作；请求不会延后至初始化成功再发送。普通入口只在 create 成功返回后开放，已有配置提交和网络副作用不因初始化失败撤销。

destroy 阶段宿主允许配置 get/set/reset、受管文件访问、私有 SQLite 原子操作、日志、时钟和释放既有资源；禁止 send/call/emit（包括原生服务对象）、ws_watch/timer_after、新任务、新网络请求或连接、应用消息收发以及 sleep。限制由宿主执行，配置 ACL 仍有效，不能通过接口包装绕过。

基础调度、固定 ABI、静态依赖及宿主事件上下文现已实现。接口授权仍采用静态 access.callers，不增加 ABI 版本、动态授权表或通用监听协议。

## 11. 模块私有 SQLite

```rust
sqlite(statements: Vec<SqlStatement>) -> HostResult<Vec<SqlRows>>

struct SqlStatement { sql: String, parameters: Vec<SqlValue> }
enum SqlValue { Null, Integer(i64), Real(f64), Text(String), Blob(Vec<u8>) }
struct SqlRows { columns: Vec<String>, rows: Vec<Vec<SqlValue>>, changed: u64 }
```

这是专用 `forest_host.sqlite` 导入。请求与结果使用现有宿主操作的 bincode 编码；SQL REAL 不扩充可哈希的 Forest Value 类型。SDK 数据定义位于 `forest-protocol::service`，成功结果与输入语句一一对应，SELECT 的 changed 为零。

每个执行模块只能访问其稳定令牌对应的数据库，没有目标令牌或路径参数。每项是单条参数化 SQL；整批在一个事务内依次执行，失败整体回滚，成功编码结果后才提交。宿主禁止来宾自己管理事务、ATTACH/DETACH、PRAGMA 和加载扩展，不能通过 SQL 绕过私有资源范围。表结构和 SQL 的含义完全由模块解释。

连接惰性建立，在专用阻塞工作任务内执行，通过模块 Pending 与恢复返回结果，不阻塞调度线程。同一实例的数据库操作按连接互斥串行执行；一次 SQL 事务提供原子性，分开的调用不会自动合并。当前连接启用 WAL、外键及同步持久化，不自动重试数据库错误。

create、普通处理器和 destroy 都可使用该资源。已提交的 SQL 属于不可中断本地操作；停止或强制丢弃来宾后，宿主仍等它实际结束，再释放连接并启动新实例。数据库文件跨热重载和卸载保留。

## 12. 随机与单调时间

random_bytes(length: u32) 返回操作系统安全随机字节，长度受实例内存额度约束，随机源失败返回 ServiceError::RandomUnavailable。monotonic_now() 返回当前宿主进程内单调毫秒数；now() 仍返回 Unix 毫秒。单调时间只能用于同一进程内的间隔，不用作持久日历时间。
