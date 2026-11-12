# 事件与静态依赖

调度器负责事件的类型校验、来源绑定和投递。外部协议由普通 Wasm 接入包解析；运行时不解释平台数据。原生嵌入方也可以登记类型化事件源。

## 声明与依赖

源包通过 events 声明路径、输入 Schema 和 access.readers。事件的 readers 表示谁能订阅，不使用 writers/callers。

```toml
[[events]]
path = "/message"
input = "events/message.schema"
[events.access]
readers = ["/module/**"]
```

接收包绑定自己的普通接口：

```toml
[[subscriptions]]
source = "/module/adapter"
event = "/message"
interface = "/handle"
```

装载与替换联合检查来源、事件、源 readers、接收接口 callers，以及事件输入是否为接收接口输入的子类型。同一接收声明不得重复。事件和接口路径都是具体 Token。

模块事件订阅形成接收者到源包的依赖边；已登记原生事件只检查宿主能力。uses 和订阅共同形成允许环的依赖图。装载从根包计算闭包，整组检查与发布；卸载集合不能留下组外依赖者。ACL 主体引用不形成依赖。

## 发布与身份

```rust
host::emit(event, &input, subject).await
// DispatchResult<Vec<EventDelivery>>
```

只能发布当前模块自己的事件。宿主绑定 caller 为实际源包，逐目标报告接纳结果，不等待处理完成。一个目标 QueueFull 不撤销其他目标已接受的请求。不自动重试、持久回放或丢弃旧请求，不提供动态订阅接口。

subject 为 None 时继承 origin。指定外部 subject 时，宿主构造 External { issuer: 当前模块, subject }，不能生成 Principal 或冒用其他 issuer。接收者必须同时信任 issuer 并解释 subject；例如 Shell 先确认 OneBot 发行者，再核对授权用户。caller ACL 和用户授权分别检查。

模块事件面向稳定对象地址，使用有界接纳预算和接口 FIFO。未启动事件可以交给 Drain 重载后的新实例；已开始的事件处理器必须完成。发布新 Manifest 时原子切换关系；新增订阅不补历史，移除订阅不撤销已接受事件。新事件输入必须是旧输入子类型。

## 实例 I/O 通知

ws_watch(handle, interface) 和 timer_after(delay_ms, interface) 绑定当前实例。接口输入兼容 IoEvent，callers 允许 /system/io。资源是在实例中建立的，因此不写 subscriptions。

WS 在原生侧等待一条结果，通知只带句柄，处理器用 ws_receive 取得原始文本或二进制。每连接只缓冲一条结果，未取走前不继续读；通知使用独立接纳池（与普通请求相同的数量和字节上限），不会被等待回执的普通调用占满；池满时等待容量变化。close 中止监听，但不会撤销已接纳的通知，模块按句柄存续处理顺序。

空闲监听不占来宾任务或接纳额度，不阻止 Drain。已经接受的实例通知和发布后激活即使未启动，也不能转交下一代。Drain 等它们完成，AtYield/Force 可中断。自然静止检查、关闭投递和通知接纳在同一目录锁下排序。旧监听取消并 join 后才能创建新实例。

完整边界见[通用宿主与接入包](36-generic-host-and-adapters.md)。
