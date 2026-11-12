# 调试与其他语言实现

[返回目录](README.md)

## 1. 按实际失败阶段定位

| 现象 | 优先检查 |
| --- | --- |
| 本地包无效 | TOML、文件路径、摘要、Schema/Value 语法、默认值 |
| `missing ABI function` | 编译目标、export_module、导出名称与签名 |
| 无法完成依赖规划 | uses 的对象/接口、完整 Result 类型、可用声明闭包 |
| create 失败 | LifecycleError 的阶段、Schema 和 Value，配置及本地数据 |
| 收不到群事件 | 包是否 Serving、subscriptions、输入契约、事件源 callers、宿主测试群限制 |
| `Denied` | 调用方 uses、目标 callers、配置 readers/writers，以及实际 caller |
| `LifecycleRestricted` | create/destroy 中执行了禁止的操作 |
| `QueueFull` | 模块请求/输入额度、持锁与等待关系、未完成任务数量 |
| `InputContract` | 调用输入和 uses/目标 Schema 是否一致 |
| `OutputContract` | 实际返回值、调用方解码类型、完整错误分支是否一致 |
| `ExecutionFailed` 或 Failed | trap、panic、借用冲突、错误 ABI，及相关宿主日志 |
| 重载拒绝 | 对象 ID、接口子类型、配置键集合和 Schema |
| 重载一直 Waiting，模块仍 Serving | 目标集合尚未同时静止；可取消请求或明确选择 Force |
| 已提交后停在 Creating / Releasing | 候选初始化、明确的宿主操作等待及必要资源收尾 |

普通模块返回 `Err(E)` 是正常的类型化结果，不等于模块已 Failed。QueueFull 表示没有接纳本次提交；成功 send 也不能当作目标完成的证据。

## 2. 使用已有观察入口

WebUI 可查看模块阶段、请求占用、接口排队、声明依赖、日志、当前实例 SHA-256 和配置。依赖图来自 Manifest，不是完整运行调用追踪。

判断代码是否更新时，比对实例 SHA 与候选工件。判断配置是否生效时，区分持久当前值和 create 时读取的内存快照。判断操作成功时，使用其真实结果或平台回执，而非日志中一个“已提交”的描述。

源码中的 SDK 宿主导入在原生目标会拒绝执行。纯计算逻辑可在原生环境验证，调用宿主的完整处理路径需要实际 Wasm 和受控宿主。示例中的 manifest 工厂可以原生运行，这不意味着其处理器也可以直接在原生测试中访问宿主。

## 3. 日志和错误数据

模块错误通过返回 E 表达；日志补充阶段与诊断。生命周期失败由宿主保留 `InstanceFailure::Lifecycle { phase, schema, error }`，调度失败由 `InstanceFailure::Dispatch` 表达。字符串展示不是新的错误协议。

外层调度异常定义见 [forest-protocol](../../crates/forest-protocol/src/lib.rs)，宿主资源错误见 [service.rs](../../crates/forest-protocol/src/service.rs)。当前没有通用的“错误即自动重试”机制，具体重试还需考虑已经发生的副作用。

## 4. 非 Rust 模块的边界

Forest 使用私有 core Wasm 模块和固定 ABI，不要求插件链接 Rust crate。其他语言需要分别实现：

1. 外部 TOML、Schema/Value 文本和文件摘要。
2. Forest Value 的结构与字节编码。
3. 固定导出、输入输出内存所有权、任务推进。
4. 需要使用的宿主导入和异步操作句柄。

公开 Schema 是结构契约，不能只按 Rust struct 内存布局写入指针。接口名也不是每个单独的 Wasm 导出：普通方法通过 Invocation 的路径进入同一套固定入口。

## 5. 固定 Wasm 导出

| 导出 | Wasm 签名 | 用途 |
| --- | --- | --- |
| `memory` | 私有、非共享 wasm32 内存 | 复制帧数据 |
| `forest_alloc` | `(i32) -> i32` | 分配长度对应的字节范围 |
| `forest_free` | `(i32, i32) -> ()` | 归还原始指针和长度的分配 |
| `forest_create` | `(i64, i32, i32) -> ()` | 为 task 建立初始化任务 |
| `forest_invoke` | `(i64, i32, i32) -> ()` | 为 task 建立接口任务 |
| `forest_destroy` | `(i64) -> ()` | 建立收尾任务 |
| `forest_poll` | `(i64) -> i64` | 推进指定任务，返回输出帧位置 |

逻辑上的 ptr/len 为 u32，task 为 u64；Wasm 函数类型只记录 i32/i64。`forest_poll` 的返回值低 32 位为指针，高 32 位为长度。

create 的输入是 `CreateContext { object }`；invoke 的输入是 `Invocation { interface, input, context }`，context 包含 origin 和 caller。create/invoke 只建立任务，poll 才推进具体工作。

poll 的 Step 为以下分支之一：

| 分支 | 作用 |
| --- | --- |
| `Pending` | 来宾任务挂起，等待唤醒 |
| `Output(Value)` | 普通接口完成，Value 包含完整 Result |
| `Lifecycle(Result<(), Value>)` | create/destroy 完成，错误符合该实例生命周期 Schema |
| `FrameTooLarge` | 输出无法由 wasm32 帧长度表达 |

输入由宿主调用 guest alloc 分配并复制，在入口返回后归还；输出由来宾分配，宿主复制解码后调用 guest free。Force/trap 后不再执行任何来宾辅助函数。

## 6. 宿主导入与挂起操作

导入命名空间固定为 `forest_host`。普通操作如 call、config_get、http_request 均使用 `(ptr: u32, len: u32) -> operation: u64`，输入为对应专用结构。

操作句柄通过以下控制函数查询和回收：

| 导入 | 用途 |
| --- | --- |
| `operation_status(id) -> i64` | -1 表示等待，非负值为结果字节长度 |
| `operation_take(id, ptr, len)` | 将已完成结果复制到来宾，长度必须准确 |
| `operation_release(id)` | 放弃操作结果并执行相应回收 |
| `task_wake(task)` | 通知某来宾任务具备继续推进的条件 |

句柄绑定实际实例及任务，不是持久 ID。异步操作完成由宿主唤醒任务，SDK 在后续 poll 传播 Rust Waker。自建语言绑定同样需要保存挂起状态、处理完成通知并正确回收操作，不能在 Wasm 内忙轮询等待宿主 I/O。

操作必须处于有效执行上下文中。释放结果或中断调用不保证远端没有产生效果；本地已提交文件/SQL 工作的结算由宿主管理。

## 7. 字节编码

控制结构、请求结构、Step 和 Value 均按 [abi.rs](../../crates/forest-protocol/src/abi.rs) 的固定 bincode 规则编码：小端、定长整数，拒绝尾随字节。字段顺序、enum 分支顺序和容器编码必须与当前协议一致；TOML 或 JSON 不能直接替代这层 ABI。

Schema 的公开文本可用于生成语言本地 DTO，参数与返回值仍编码为 Forest Value。原生 f64 只出现在 SQL 等明确的宿主 I/O 结构中；它不是一般 Forest Value 分支。

ABI 没有版本协商。语言绑定应对齐当前协议定义和运行时，实现完整的帧、唤醒及内存规则。帧可表示的最大长度为 u32 上限，实际还受内存和队列额度约束。

FrameTooLarge 表示编码长度无法表达，不是所有内存不足的统一结果。来宾分配触及实例内存配额仍可能导致 trap；不能根据传输长度上限推断一次分配必定成功。

当前仓库提供现成 Rust SDK；其他语言的可用实现需要按以上边界自行编写和验证。[运行时 ABI 说明](../13-runtime-and-sdk.md#4-固定-abi)与 [SDK 源码](../../crates/forest-sdk/src/lib.rs)可作为实现入口。
