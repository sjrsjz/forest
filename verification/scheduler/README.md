# 调度器协议的 Lean 验证

这个独立项目验证调度器与来宾 ABI 的抽象协议。工具链固定为 `leanprover/lean4:v4.19.0`，只依赖随工具链发行的 Std，不下载 Mathlib 或其他证明库。

在本目录执行：

```sh
python check.py
```

检查程序核对 Lean 版本和人工审查过的 Rust 源码指纹，构建全部证明，再读取 `Audit.lean` 中各主定理的 `#print axioms` 报告。允许的基础逻辑公理只有 `propext`、`Classical.choice`、`Quot.sound`；不允许额外公理、未完成证明或依赖本机代码计算结果的证明捷径。源码指纹只负责发现对应关系可能过期，不证明 Rust 与模型语义等价。也可以分别运行：

```sh
lake build
lake env lean Audit.lean
```

## 文件与性质

| 文件 | 内容 |
| --- | --- |
| [Basic.lean](Forest/Basic.lean) | 任意有限长度执行轨迹和归纳不变量 |
| [Abi.lean](Forest/Abi.lean) | ABI 分配、输入读取、输出借用与解码、释放、内存增长、Store 回收 |
| [Store.lean](Forest/Store.lean) | 唯一推进持有者、任务和生命周期阶段、被动暂停、正常析构与异常回收 |
| [Mutex.lean](Forest/Mutex.lean) | 实例隔离、任务持有的宿主锁、FIFO 授予、结果交付、取消、实例停止与原生工作收尾 |
| [Reload.lean](Forest/Reload.lean) | 任意目标集合的提交、未启动请求按接口交接、整组替换与发布、提交前取消 |
| [Counterexamples.lean](Forest/Counterexamples.lean) | 证明错误地在借用期间增长内存、在推进未结束时释放执行权，会破坏不变量 |
| [Audit.lean](Audit.lean) | 主定理的可信基础审计入口 |

主结论按执行轨迹归纳：初始状态满足不变量，每个合法转换保持不变量，因此任意可达状态满足不变量。轨迹长度、模块编号、任务编号、分配次数和重载目标集合没有固定上界。证明不要求程序终止、调度公平或外部 I/O 最终返回。

`Buffer.id` 是一次分配的逻辑身份，包含原始长度；它不是永不复用的数值地址。`released` 是证明中的历史记录，不要求生产环境维护分配历史。证明根据协议位置推导释放前仍然拥有同一块缓冲区，没有把“指针有效”作为释放转换的假设。

## 证明边界

这里证明的是 Lean 模型的协议安全。Rust 实现没有通过翻译器或编译器细化证明自动关联到该模型，不能把结果称为整个 Rust 程序的端到端内存安全证明。

底层约定包括：Wasmtime 实现其安全接口和 Future 取消约定；模块与 SDK 遵守外部 ABI；分配器返回有效、不重叠的活跃分配；输入反序列化返回拥有自身数据的对象；模块内部不自行破坏其语言的内存规则。没有重新验证 Wasmtime、Rust 编译器、分配器、操作系统或插件内部算法。

`Abi.Step.grow` 表示来宾调用可能使线性内存迁址。宿主持有借用切片时不允许转交可变 Store；这是 Rust 借用和完整推进边界提供的约束，不是运行时增加的一张地址检查表。解码失败也结束借用作用域，然后通过异常回收路径释放整个 Store。

`Store.Step.trap/force` 抽象 Wasmtime 调用退出、取消及 Store 丢弃，不能解释为在同步借用仍存活时释放内存。`releasing/staged` 表示来宾 Store 已不再可执行；宿主不可中断工作仍须在完成清理后才进入 staged。

任务编号使用逻辑身份。实例代际单独建模；实际 Rust 的局部任务编号可以在新实例中重新从 1 开始，因为宿主操作表和通知弱引用归属于具体实例。`Store.Step.enqueue` 表示将宿主请求登记到当前实例 FIFO，包括发布新实例时接入已经接纳的交接请求，不表示再次鉴权或再次占用接纳额度。

队列模型保留完整 Request 值和同接口顺序，不证明跨接口的全局 FIFO。组提交和发布表示目录锁中的一个可观察转换；各实例的实际 create/destroy 与宿主 I/O 不属于该原子转换。Manifest Schema/ACL 正确性、死锁自由、最终响应、OOM 不发生、队列容量以及调用者自定义资源使用均不属于本证明的结论。

实际代码的对应和仍需审查的细化义务见[验证说明](../../doc/35-scheduler-lean-verification.md)。
