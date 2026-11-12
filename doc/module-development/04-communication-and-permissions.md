# 通信、事件与权限

[返回目录](README.md)

## 1. 对象与接口地址

```rust
let object: Token = "/module/example-hello".parse()?;
let interface: Token = "/greet".parse()?;
```

这是两个 Token，不能把它们拼成一个字符串操作码交给通用执行入口。接口路径可以是任意合法具体路径，包括 `/`；`/module`、`/settings` 等前缀是表达用途的约定。

Token 从 `/` 开始。路径段不允许为空、`.`、`..`、反斜杠、星号、空白或控制字符。具体 Token 不包含通配符；ACL 使用独立的 `TokenPattern`。

## 2. call 与 send

```rust
let returned: Result<String, std::convert::Infallible> = host::call(
    "/module/example-hello".parse().unwrap(),
    "/greet".parse().unwrap(),
    &Greeting { name: "Example".into() },
).await?;
```

这段调用等待结果。Greeting 是调用方自己的类型映射，只需符合公开 Schema，不需要依赖示例模块的 crate。

```rust
host::send(target, interface, &event).await?;
```

send 的成功只证明宿主接纳，不证明目标已完成或副作用成功，也没有供发送者查询执行结果的任务 ID。SDK 返回的 `Operation` 必须被 await 或轮询才会启动；构造后直接丢弃不会提交操作。

需要完成状态、顺序依赖或返回数据时使用 call。对已提交操作停止等待，不意味着撤销下游任务或回滚外部请求。重试可能重复产生副作用，是否重试由协议和模块自身决定。

## 3. uses 与 callers 都要满足

调用方 Manifest 中需要：

```toml
[[uses]]
object = "/module/example-hello"
interface = "/greet"
input = "contracts/greeting.schema"
output = "contracts/greeting-result.schema"
```

目标 `/greet` 的 access.callers 同时需要允许调用方，例如：

```toml
[provides.access]
callers = ["/module/example-consumer"]
```

uses 表达静态依赖和类型要求，callers 表达接纳权限，二者不能互相代替。send 和 call 使用相同的调用许可。静态依赖允许形成环，装载闭包联合校验。

调用自身接口也需要 uses 和 callers；自身引用和跨模块引用使用相同契约校验，不依赖图是否为 DAG。持有宿主锁时等待一个也要获取同名锁的嵌套 call，会形成等待环。当前调度器不自动检测或解开调用死锁。

宿主会校验输入、目标契约及完整输出。SDK 的 Rust 泛型不使错误的 Manifest 自动正确，也不使两个同名 Rust struct 自动兼容。

## 4. 可信来源

普通处理器获得：

```rust
pub struct CallContext {
    pub origin: Origin,
    pub caller: Token,
}
```

`origin` 记录原始发起者，`caller` 记录本次直接调用主体。链路 `用户 → 模块 A → 模块 B` 中，B 的 origin 保留原始来源，caller 为 A。接口 callers 匹配直接调用主体；常规配置操作则以实际执行模块为主体检查。

OneBot 群消息的 origin 由接入包声明用户、宿主绑定发行者生成 `External { issuer: /module/onebot, subject: /user/ACCOUNT }`，caller 为 `/module/onebot`。普通群消息正文中的账号、群管理员标记、模型参数都不能替代宿主绑定的来源。群管理员资格也不会自动授予框架管理员权限。

`CreateContext.object`、操作参数中的目标 Token 和调用上下文是不同用途的数据。知道一个管理员 Token 的文本，不等于获得该身份；宿主 API 不允许模块指定自己的来源。

需要代用户执行管理操作时，使用明确的宿主管理能力及其授权契约。普通 `config_set` 没有“传入 origin 即按用户鉴权”的选项。

## 5. 配置与接口 ACL

许可由资源所属包声明，属于静态 Manifest 元数据：

| 资源 | 使用字段 |
| --- | --- |
| 配置项 | readers、writers |
| 接口 | callers |

`self` 在 TOML 中表示声明所属模块；Rust 中对应 `SubjectPattern::SelfModule`。它不会自动附带读写许可，必须放进具体列表。未声明资源或没有匹配许可时返回拒绝。

| 模式 | 含义 |
| --- | --- |
| `/module/example-consumer` | 仅完整路径相等 |
| `/module/*` | `/module` 后恰好一个路径段 |
| `/module/**` | `/module` 自身及任意层后代 |

路径父子关系不隐含授权；前缀匹配需要明确写 `/**`。当前 ACL 为允许列表，不通过普通配置 set 动态添加规则。

受信 WebUI 的只读 Inspector 可以查看完整配置；编辑仍走 `/user/admin` 的 config set 和 writers。可见不等于可写，`/user/admin` 也不是模块接口的隐式通行证。

## 6. 模块事件

群事件接收声明为：

```toml
[[subscriptions]]
source = "/module/onebot"
event = "/group_message"
interface = "/group_message"
```

接收接口必须出现在本模块 provides 中，输入应能接受源包事件 Schema，callers 必须允许对应事件源。订阅随包加载和重载维护，模块不调用动态 subscribe/unsubscribe。

模块用 events 声明事件及 readers，emit 发布自身事件并逐目标取得接纳结果。subscriptions 形成静态依赖，允许环；接收接口 callers 同时允许源包。不提供动态订阅、回放或自动重试。需要响应时用 call，单目标通知可用 send。

emit 的 subject 为 None 时继承 origin；指定路径时生成 External { issuer: 当前模块, subject }，不能伪造 Principal。完整规则见[事件与依赖](../11-notifier-and-dependencies.md)。

## 7. 接纳与背压

模块的已占用请求数和输入字节受宿主额度约束。容量不足时返回 `QueueFull`，表示本次新请求没有入队，宿主不会创建另一条隐藏的无限等待队列。

队列满与锁等待是不同问题：等待锁的任务已开始，仍占接纳额度。已经接纳的任务恢复时不重新申请接纳额度，因此队列满不阻止它完成。

持续高频输入需要模块明确安排接收和处理。例如短接收接口先完成必要的有界入队，再通过另一接口进行异步处理；模块通过明确的状态提交范围和必要的宿主锁协调队列；允许请求开始本身不提供背压或状态一致性。
