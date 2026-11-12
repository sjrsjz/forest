# 通用宿主、资源通知与接入模块

本章规定通用化重构的目标边界。运行时和 CLI 不依赖 QQ、OneBot 或 Agent；平台接入由普通 Wasm 包提供。实现按本章更新其余章节，未部署的重构不保留旧 ABI 或包描述兼容层。

平台协议库和平台端到端测试放在插件目录。OneBot 协议库位于 `plugins/onebot/protocol`；宿主互操作测试由插件作为外部程序启动 CLI，不作为 CLI 的开发依赖。工作区默认只构建通用 CLI。

## 1. 宿主 I/O

HTTP 请求增加 `timeout_ms: Option<u64>`，从宿主开始执行请求至完整读取响应 body 使用同一相对期限。None 表示调用者明确不设置期限；具体值来自模块配置。超时为类型化 ServiceError，不改变 call/send、锁或管理请求的等待语义，不自动重试或承诺撤销远端副作用。WS 建连和发送同样允许单次期限；接收监听不设隐式空闲期限。

`random_bytes(u32)` 取得操作系统安全随机字节；整数、范围和生成器属于 SDK。`monotonic_now()` 返回当前宿主进程内的单调毫秒数，不能持久化为日历时间；`now()` 保留 Unix 时间语义。

## 2. 发布后激活

Manifest 的 `activated` 可引用一个输入为 Unit 的普通提供接口。整组 create 成功并原子发布后，宿主向每个新实例提交一次激活通知，caller 为 `/system/lifecycle`。该接口使用正常 Result 输出和正常调度；等待期间其他接口可以执行，不建立第二个初始化屏障。返回 Err 是模块结果，trap 终止整实例。

激活有独立的一个控制接纳名额，防止重载交接队列占满普通额度后无法激活。控制名额由唯一接纳凭据持有，参与自然静止，不复制活动任务计数。普通请求不能使用该名额。

create/destroy 仍禁止模块通信和宿主锁。destroy 已承担正常收尾，不新增重复的 before_destroy。Force/trap 不调用任何来宾清理。

## 3. 实例资源通知

模块建立 WS 后，可绑定本实例中一个接收 IoEvent 的接口。宿主等待一条消息，把结果保存在该连接中，投递携带连接句柄的 WebSocketReady 通知；模块通过 ws_receive 取走原始文本或二进制。每个连接最多保存一条待取结果，结果未取走时不继续读取，通知使用独立于普通 call/send 的有界接纳池，数量和字节上限分别沿用 max_requests 与 max_input_bytes，接纳不足时等待容量变化。普通调用等待 I/O 不会耗尽其完成通知的名额。普通拉取与监听不能同时读取同一连接。

一次性计时器投递 Timer 通知。连接、计时器、监听和完成槽均归具体实例，不随稳定对象地址迁移。宿主只处理通用 WS 和计时，不理解平台 JSON、重连或回执。

请求的投递归属用明确的 Object / Instance 类别表示：普通 call/send 与模块事件是 Object；激活和 I/O 通知是 Instance。Drain 重载只扣除可交接的未启动 Object 请求，已接纳的 Instance 通知必须执行并收尾。静止检查、提交和关闭旧投递由目录锁排序。显式 AtYield/Force 中断旧通知；所有原生监听终结后才创建新实例。空闲监听不占来宾任务或接纳凭据。

## 4. 模块事件

Manifest 的 events 声明路径、输入 Schema 和静态 readers；subscriptions 指向 `(source, event, interface)`。模块事件来源由宿主绑定，模块只能发布自身声明的事件。订阅模块事件形成静态依赖，装载和替换统一校验 Schema、源的 readers 及接收接口的 callers。静态图仍允许环。

emit 逐目标报告接纳结果，不等待处理完成，不撤回其他接收者已经接受的请求。事件没有持久回放或动态订阅 API。模块之间需要响应时继续使用 call。

## 5. 外部身份

CallContext.origin 区分 Principal(Token) 和 External { issuer, subject }。直接调用主体 caller 仍由宿主附加，ACL 根据 caller 查询。模块发布事件时可以提供本地外部 subject；宿主自动绑定 issuer 为实际模块身份。模块不能构造 Principal 管理员身份，也不能冒用其他接入模块的 issuer。普通 send/call 原样传播 origin。

接入模块对自己的外部身份声明负责。例如 OneBot 验证并解析连接中的用户字段，Shell 同时检查 OneBot issuer、对应 subject 与授权用户设置。该信任来自所安装接入包，不要求内核重新解析平台协议。

## 6. OneBot 包

`/module/onebot` 使用通用 HTTP/WS、资源通知、计时和事件接口；负责 JSON 编解码、echo 关联、群消息发布、账号查询、重连及丢失回执结算。activated 启动连接；网络回调处理一条输入后返回。断线使未结算回执正常返回错误，不自动重放动作。

首版支持正向 WS，适配现有使用方式。反向 WS 和 HTTP 上报需要通用监听资源，留到对应需求实现。CLI 只装载包目录、配置库、调度器和可选 WebUI，不携带平台账号、网关参数或群白名单。

## 7. 验证义务

验证超时覆盖完整 body、随机和单调时钟、循环事件依赖、发行者隔离、激活与调用交错、通知背压、Drain 不迁移旧句柄、Force/trap 不留下监听，以及完整 Wasm OneBot 回执和事件链。既有 Store/ABI 证明继续成立的对应关系需重新审查；控制接纳与实例通知增加单独的模型性质，不把新路径自动算入旧证明。
