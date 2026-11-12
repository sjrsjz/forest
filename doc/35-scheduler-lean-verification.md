# 调度器协议的 Lean 验证

验证工程位于 [`verification/scheduler`](../verification/scheduler/README.md)，固定 Lean 4.19.0，只依赖工具链自带的 Std。它把[调度模型](34-scheduler-formal-model.md)中涉及 Store、ABI 和重载交接的部分表达为可由内核检查的定义与定理。

## 1. 检查方法

安装 `lean-toolchain` 中指定版本后，在仓库根目录执行：

```sh
python verification/scheduler/check.py
```

脚本执行四项检查：

1. 实际 Lean 版本与固定工具链一致。
2. 本次人工核对的 Rust 源码指纹保持一致。
3. 全部 Lean 文件通过编译，警告按错误处理。
4. `Audit.lean` 中主定理的公理依赖仅来自 `propext`、`Classical.choice`、`Quot.sound`，不允许未完成证明、额外公理或本机计算捷径。

Lean 工具链是开发依赖，不进入宿主运行路径，Bot、Wasm SDK 和树莓派均不需要安装 Lean。

## 2. 已机械检查的性质

| 范围 | 主定理与含义 |
| --- | --- |
| ABI 不变量 | `Abi.reachable_safe`：任意长度合法轨迹都保持缓冲区所有权、释放历史和借用有效性 |
| 正确释放 | `Abi.free_valid`：释放前拥有同一个完整 Buffer，包括原始长度；该分配尚未释放 |
| 禁止重复释放 | `Abi.no_double_free`：同一分配身份不能在一条合法轨迹中释放两次 |
| 内存迁址 | `Abi.borrowed_reference_valid`：借用期间的内存 epoch 与取得引用时一致 |
| Store 回收 | `Abi.no_exports_after_close`：关闭后的状态不能继续分配、调用、poll 或 free |
| 执行与生命周期 | `Store.reachable_protocol_safe`：任意长度合法轨迹保持推进持有者、ABI、任务和生命周期的对应关系；Staged 中不存在未完成任务或收尾 |
| 被动暂停 | `Store.preemption_keeps_owner`：时间片暂停不释放推进持有者，也不替换 ABI 状态 |
| 重载前提 | `Store.drain_has_no_owner`：已开始任务及收尾结束后，不存在旧普通推进 |
| 未启动请求 | `Store.queued_unbound`：待启动请求不能已经成为某次推进的持有者 |
| 代际隔离 | `Store.stale_completion_rejected`：旧实例的操作完成不能选择新实例任务 |
| 锁的可达状态 | `Mutex.reachable_safe`：任意长度合法轨迹中，等待者和未交付授予均属于可继续运行的任务 |
| 唯一许可与 FIFO | `Mutex.lease_unique` 与 `grant_is_fifo`：每个实例内的锁名至多一个许可，只能从等待队首授予 |
| 实例内作用域 | `Mutex.instance_step_is_local`：一个实例的锁操作不改变其他实例的锁或等待者 |
| 授予与取消 | `Mutex.cancel_removes_undelivered` 与 `cancel_keeps_delivered`：取消移除等待及未交付授予，不释放已交付给任务的许可 |
| 持有者与实例停止 | `Mutex.other_task_cannot_unlock`、`stop_instance_blocks_all`、`stopped_cannot_deliver`：锁属于具体实例与任务；整个实例停止后，不再向其任务交付许可 |
| 原生工作收尾 | `Mutex.cleanup_keeps_held_until_native_finishes`：已停止任务仍有不可中断原生工作时，合法转换不能提前释放其持有锁 |
| 接纳额度 | `Reload.admission_accounting`：在线性接纳额度守恒前提下，仅扣除 Object 排队额度后归零意味着 Instance 排队、执行与收尾均为零 |
| 整组提交 | `Reload.commit_preserves` 与 `commit_has_no_old_owner`：对任意目标集合联合提交，保持 Store 安全，目标中没有旧推进 |
| 请求交接 | `Reload.commit_transfers_exact_requests` 与 `old_requests_precede_new`：完整请求值和同接口 FIFO 顺序保留，提交后请求追加到原队列之后 |
| 替换屏障 | `Reload.all_old_stores_closed` 与 `replacement_preserves`：全部旧 Store 关闭后创建整组候选，保持安全 |
| 发布屏障 | `Reload.publish_preserves` 与 `publish_all_together`：候选满足初始化前提后统一发布，没有逐成员提前开放入口 |
| 取消 | `Reload.cancellation_leaves_world` 与 `no_commit_after_cancel`：取消 Pending 不改动运行对象，取消后的请求不能再提交 |

证明按 `Init → Step → Trace` 归纳，不枚举固定数量的任务或有限测试场景。`Counterexamples.normal_abi_roundtrip` 构造了包含输入分配、invoke、输出读取和两次释放的非空执行，避免只有空轨迹满足模型。

另有两个反例定理验证安全边界的必要性：宿主仍持有借用时允许内存增长，会使引用的 epoch 失效；ABI 推进仍有活跃缓冲区时把 owner 清空，会破坏空闲状态的不变量。这些错误规则均未包含在合法转换中。

## 3. 模型粒度

ABI 子模型区分以下步骤：

```text
idle → allocated → written → invoked → free input
     → poll → output → borrowing → decoded → free output → finished → idle
```

resume 与 destroy 可以从无输入的 poll 准备阶段进入。Wasmtime 的一个完整异步导出调用内部可以反复被动暂停；它继续持有同一次推进，不因此回到 idle。模型中 `decode` 同时表示 Rust 借用作用域结束；解析失败时，后续走 Store 回收，而不是在借用期间回收。

一次 ABI 分配由逻辑身份和长度表示。逻辑身份允许区分数值地址复用前后的两次分配；证明不要求真实分配器永不复用地址。分配器返回有效、不重叠活跃范围的语义属于底层约定。释放合法性由协议位置和此前转换推出，没有在 free 转换中直接假设“该指针有效”。

`released`、任务逻辑身份及内存 epoch 是证明状态，不要求运行时增加这些表或计数。生产代码继续通过独占 Rust 借用、Request/Started 所有权、Admission、实例操作表和弱引用实现对应约束。

锁子模型选取任意一个实例内的具体锁名，区分 FIFO 等待、完成槽中的 Granted 与任务持有的 Held。授予和 take 分成两步，以覆盖已授予但尚未取走结果时的取消。停止操作可以选一个任务，也可以选择同实例全部任务；未交付的授予与等待立即取消，Held 留到该任务原生工作结束。不同锁名和实例分别遵守此协议，`Mutex.instance_step_is_local` 保证一个实例的锁状态转换不改变另一个实例的锁或等待者；模型不要求锁名数量固定，也不证明模块选取的临界区符合其内部算法。

## 4. 与 Rust 实现的对应审查

当前对应关系经过人工审查，并由 [`source-map.json`](../verification/scheduler/source-map.json)记录 UTF-8、统一换行后的源码 SHA-256。指纹变化会让检查脚本退出，要求重新审查对应关系；指纹不构成语义等价证明。

| Rust 位置 | 模型对应与审查义务 |
| --- | --- |
| [`Wasm::advance/advance_inner`](../crates/forest-runtime/src/wasm.rs) | `&mut self` 与 `&mut Store` 覆盖整个推进；输入分配、复制、invoke、释放、poll、输出解码及释放均位于此范围；返回数据拥有自身内容 |
| [`run_guest/lifecycle`](../crates/forest-runtime/src/driver.rs) | 同一驱动器独占 Store；Pending 保存原 GuestTask；普通推进结束前不进入 destroy；异常退出不再调用来宾清理导出 |
| [`Request/Started/Admission`](../crates/forest-runtime/src/core.rs) | FIFO 中未启动请求与已绑定任务由 Rust 所有权区分；额度在 ABI、本地操作及收尾完成后归还；交接不复制或重新创建返回出口 |
| [`turn/complete`](../crates/forest-runtime/src/core/lifecycle.rs) | START 与提交在同一目录锁排序；开始前尝试已准备好的重载；结果只结算一次 |
| [`try_commit_change/create_group`](../crates/forest-runtime/src/core/changes.rs) | 联合检查目标和候选；扣除 FIFO 持有的额度；原子转移未启动队列；全部旧成员进入 Staged 后才创建新成员；全部初始化完成后一起发布 |
| [`Operations`](../crates/forest-runtime/src/operations.rs) | 后台操作持有拥有自身数据的完成结果，不持有来宾内存切片；结果和通知绑定具体实例、任务与完成槽；迟到通知不能转向新实例 |
| [`Locks`](../crates/forest-runtime/src/locks.rs) | 每个实例的 Operations 私有持有一张 Locks 表；同实例相同 Token 共享一个 Tokio FIFO Mutex；拥有许可的 guard 随完成槽或任务移动，不复制所有权 |
| [`Operations::lock/take/release/finish_task`](../crates/forest-runtime/src/operations.rs) | lock 导入内首次 poll 登记 FIFO；take 成功写入后移交许可；取消释放未交付许可；清理先等待不可中断工作，再释放任务已持有许可 |
| [`SDK LockGuard`](../crates/forest-sdk/src/lock.rs) | 所属任务内 Drop 同步 unlock；生命周期退休不调用普通任务的宿主解锁，由宿主负责回收 |
| [`SDK 缓冲区`](../crates/forest-sdk/src/runtime.rs) | `allocate/encode_buffer` 交出 Box；`deallocate` 取回同一地址和长度；`read_input` 返回不借用输入字节的对象 |
| [`SDK 导出`](../crates/forest-sdk/src/lib.rs) | create/invoke 在导出返回前读完输入；poll 的临时 RefCell 借用不跨 Future 推进；destroy 先取出待退休 Future，再在 RefCell 借用之外丢弃 |

以下连接仍是审查义务，没有伪装成已完成的 Lean 证明：

- 实际 Rust 的每个具体操作都能映射为模型的某个转换或停顿。
- 互斥锁、Rust 借用及控制流确实覆盖模型中的原子边界。
- 所有实际 FIFO 项恰好是未启动 Request；字节额度与序列化后的 Invocation 长度一致，收尾没有提前归还 Admission。
- 每次 ABI 导出遵守其 SDK 合约；宿主 import 在返回或排队异步工作前完成来宾数据复制，后台任务不保存裸地址。
- Force/trap 使用 Wasmtime 正常支持的 Future 退出和 Store 丢弃机制，并先结束同步借用作用域。
- Tokio Mutex 与完成槽的锁定范围实现模型中的 FIFO、唯一许可及取消转换；锁等待已属于 Started，不能当作宿主 FIFO 中未启动请求迁移。

目前没有自动生成 Rust 的纯状态转换器，也没有验证 Rust 到 Lean 的编译或细化过程。未来修改这里的内核代码时，需同时检查源码指纹、转换对应及定理是否仍然描述实际实现。

## 5. 结论范围

可声明的结果是：在给定的 Store、ABI 和实例隔离约定下，模型中所有合法调度与重载轨迹保持所列资源安全性质，相关定理由 Lean 内核检查。

可信底层包括 Lean 内核及标准逻辑、Wasmtime 安全接口与异步取消机制、Rust/SDK 内存规则、分配器和操作系统。模型使用普通非共享 Wasm Memory；插件自行破坏内存规则、Wasm 内部线程、用户自定义数据竞争、框架外裸指针使用均不纳入结论。

这里没有证明所有调用最终返回、没有死锁、不会 OOM、任何程序都不会 trap，也没有证明外部 I/O 或数据库的功能正确性。没有新增调度等待超时、死锁检测或运行时地址验证来替代上述协议。

## 6. 通用事件与资源通知的扩展

Reload.Member 分开表示可交接 Object 队列、不可交接 Instance 队列和激活控制凭据。CanCommit 要求后两者为零；独立 I/O 池须全部归还，执行与收尾仍由 Store 任务表示。commit_has_no_instance_callbacks 与 separate_control_admission 检查这一前提；Request 交接定理仅适用于 Object 请求。

Resources 子模型区分原生空闲监听与已接纳回调。step_preserves 保证回调只带当前代际；closed_rejects_admission 与 stale_generation_rejected 排除关闭入口或旧实例向新实例投递。replace 必须在旧监听 join、旧回调结算后执行。模型抽象了目录锁与实例 ID 检查，没有证明网络协议或将 Rust 自动细化为 Lean。

新增对应审查：core/events.rs 的实例与阶段检查在目录锁内；services/notifications.rs 的闭包捕获具体实例弱引用，不查询稳定对象的新实例；driver 在终结可注册监听的操作后取消并 join 监听。随机、网络超时、外部协议不在 Store 内存安全证明范围。
