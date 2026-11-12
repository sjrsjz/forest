# 生命周期与并发

[返回目录](README.md)

## 1. 模块入口

Rust SDK 的核心接口为：

```rust
pub trait Module: Sized + 'static {
    type LifecycleError: AsSchema + AsValue;

    fn create(context: CreateContext)
        -> impl Future<Output = Result<Self, Self::LifecycleError>> + 'static;

    fn handlers() -> Handlers<Self>;

    fn destroy(self: Rc<Self>)
        -> impl Future<Output = Result<(), Self::LifecycleError>> + 'static;
}
```

模块作者可以用 `async fn` 实现 create/destroy。destroy 默认成功且不做操作。处理器接受 `Rc<Self>`、`CallContext` 和具体输入，返回一个 Future；SDK 不要求它是 Send。

## 2. create

宿主先准备声明、配置默认值、模块资源和执行实例，再调用 create。`CreateContext.object` 是稳定的模块对象 Token，可以保存为后续配置访问的目标。

create 适合读取配置、初始化私有数据库、恢复持久状态和建立实例内的数据结构。配置项必须事先在 Manifest 中声明，不能在 create 中临时注册一个未声明的配置键。

create 禁止 `host::send`、`host::call`、`host::lock` 和 `host::unlock`，包括调用自身和宿主原生对象；权限允许也不能绕过这条限制。当前实现允许直接配置操作、文件、私有 SQLite、HTTP/WebSocket、日志、时间及等待。需要调用其他模块的工作，应通过 Serving 后的普通接口触发。

候选代码的解析、ABI 和兼容性检查先于旧实例停止；create 本身则发生在旧实例资源结束之后。create 中已经提交的数据库写入或外部请求不具有通用回滚语义。返回错误不是恢复所有外部状态的事务回滚。

## 3. 普通接口与 destroy

Serving 阶段可以通过普通接口执行工作。停止时，宿主依所选模式完成或中断旧调用；进入 destroy 后不再开始普通处理器。

destroy 用于保存短期状态和完成本地收尾。当前能力边界为：

| 操作 | create | 普通处理器 | destroy |
| --- | --- | --- | --- |
| send/call | 禁止 | 按依赖和 ACL | 禁止 |
| lock/unlock | 禁止 | 当前实例与任务持有 | 禁止 |
| 配置 get/set/reset | 按 ACL | 按 ACL | 按 ACL |
| 受管文件、私有 SQLite | 允许 | 允许 | 允许 |
| HTTP、WS 建连/收发 | 允许 | 允许 | 禁止 |
| WS close | 允许 | 允许 | 允许 |
| sleep | 允许 | 允许 | 禁止 |
| log、now | 允许 | 允许 | 允许 |

destroy 可以依赖本地持久化，不应依赖给另一个模块发消息完成收尾。Force 或 trap 不执行来宾 destroy，也不执行来宾 free 等辅助函数。必须可靠保存的数据应在正常处理时按所需原子边界提交，不能只保存在最终析构中。

生命周期错误的 Schema 来自关联类型并写入 Manifest。宿主保留阶段、Schema 和原始 Value；这类错误不会冒充普通接口的返回 E。正常卸载中的 destroy 返回错误后仍完成资源释放，再向管理端报告。

## 4. 运行阶段

| 阶段 | 模块作者看到的行为 |
| --- | --- |
| Creating | create 正在推进，或已成功但等待整组发布；普通请求不能开始 |
| Staged | 旧实例已完全释放，稳定对象等待整组创建步骤 |
| Serving | 当前实例接受并处理请求 |
| Stopping | 已确定停止边界，按停止模式处理旧集合及交接请求 |
| Destroying | 仅推进来宾收尾，能力受限 |
| Releasing | 宿主结束资源，不再允许来宾操作 |
| Failed | 模块不可调度，等待外部管理恢复或卸载 |

管理请求与模块阶段分开：Preparing / Waiting 可取消，旧实例继续 Serving；默认 Drain 重载等整组已开始任务及收尾结束后原子提交，未启动请求由新实例首次执行。Drain 卸载则须完成全部已接纳请求。正常替换在整组旧实例回收完成后创建新实例，全部初始化成功后统一发布。trap 等失败经资源回收进入 Failed；运行时不自动重启。操作细节见[打包与热重载](07-packaging-and-reload.md)。

## 5. 同一个实例共享内存

同一实例的处理器共享模块对象、线性内存和静态数据。多个接口不是多个独立对象，也不会因为调用一次接口就复制一份存储。

但共享内存不等于同时执行来宾指令：**一个实例一次只推进一个任务**。不同实例可以在宿主多个执行线程上并行运行。

存在两种不同的暂停：

| 情况 | 实例执行权 | 同实例的其他任务 |
| --- | --- | --- |
| 时间片耗尽，被运行时暂停 | 保留给当前推进 | 不能插入 |
| 来宾 Future 返回 Pending，完成 ABI 收尾 | 归还调度器 | 可以开始或恢复其他就绪任务 |

所以，一个一直计算且从不主动挂起的死循环，不会让同一实例再开始第二个死循环。时间片允许其他实例获得 CPU；它不把当前实例的共享数据开放给其他调用。`async fn` 中没有实际返回 Pending 的同步计算，也不会自动获得重入机会。

## 6. 任务持有的宿主锁

`Handlers::with(path, access, handler)` 只声明接口路径、ACL 与函数。多个未完成任务可在主动让出点交错。共享临界区由模块代码显式表达：

```rust
let _guard = host::lock("/state".parse().unwrap()).await?;
let old = self.counter.get();
host::sleep(100).await?;
self.counter.set(old + 1);
```

`LockGuard` 离开作用域时同步解锁，也可以调用 `guard.unlock()?`。同一 Wasm 实例中的相同具体 Token 表示同一把独占、非递归锁；本实例的不同接口可以共用它。不同实例及热重载后的新实例使用各自独立的锁表。等待按 lock 提交顺序 FIFO，不阻塞本实例其他任务。所有权绑定宿主确定的实例与根来宾任务，移动 guard 不转移所有权；其他任务 unlock 返回 Denied。

取消 lock 的等待 Future 会退出队列；已授予但未取走的许可也会释放。已交付许可由 guard 或任务回收释放。任务回收先等待已提交的不可中断本地操作，trap/Force 则停止整个实例、不再推进任何来宾代码，再由宿主清理锁与资源。

一次 `.await` 只有真的返回 Pending 才形成主动挂起。已完成的 Future 可以立即返回；持锁任务的时间片暂停也不会释放锁。锁等待者已经执行过 Wasm，Drain 重载必须等待它完成，不能将其交给新实例。

## 7. 跨 await 的状态协调

以下片段中的读取和设置都在一段同步推进内：

```rust
let value = self.counter.get();
self.counter.set(value + 1);
```

时间片暂停不会允许同实例其他任务插进这两步。跨过实际挂起点则不同：

```rust
let old = self.counter.get();
host::sleep(100).await?;
self.counter.set(old + 1);
```

其他允许的调用可能已经更新 counter，这时旧值写回会覆盖更新。可以按实际需求选择显式宿主锁、私有数据库中的原子 SQL，或者改变状态提交顺序；框架不会自动合流两份结果。

`RefCell` 的借用应在 await 前结束。挂起期间其他处理器再次借用同一 RefCell，可能造成运行时借用冲突。同步阻塞锁等待也可能阻止本实例推进解锁任务；需要跨挂起协调时，应使用 host::lock 等可挂起的协调方式。锁名不赋予配置访问权限，也不会自动约束不获取它的其他写入者。

详细调度不变量见[调度设计](../09-scheduling.md)与[状态机](../12-scheduler-state-machine.md)。
