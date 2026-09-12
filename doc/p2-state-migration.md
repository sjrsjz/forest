# P2：原生组件持久状态迁移

本页描述 `Kernel.install` 已实现的组件状态迁移。范围是 SDK `ctx.state` 的有界键值状态；Agent/Session、记忆事实库、活跃模型流和 Python 执行栈有各自的协议，不能塞入此快照迁移。

## 包契约

```toml
[[components]]
id = "example.counter"
scope = "bot"
state_schema = 2
state_value_schema = "schemas/state.json"
state_migrations = [{from_schema = 1, to_schema = 2}]
reload_mode = "drain"
config_schema = "schemas/config.json"
```

`state_schema` 是正整数。`state_value_schema` 可省略；提供时校验完整的 `{key: value}` 映射，也用于后续 `ctx.state.commit` 校验。映射最多 512 个键，键长 1–200 字符，有限 JSON 编码最多 120,000 字节，给 IPC 快照头和修订元数据保留空间。首次安装从空映射开始，因此状态 schema 应允许空映射；业务首次写入必须满足相应值约束。

`state_migrations` 列出候选能直接读取的来源版本，目标必须等于本组件的 `state_schema`。同版本重载严格保留值，不执行迁移。首次安装允许从空状态创建任意正整数版本。已有组件的升级和降级都必须显式声明对应转换，内核不根据版本数字猜测兼容性，也不自动串联历史迁移代码。

```python
from forest.sdk import Plugin

class Counter(Plugin):
    async def migrate(self, state, *, from_schema, to_schema):
        if (from_schema, to_schema) == (1, 2):
            return {"counter": {"amount": state.get("counter", 0)}}
        return await super().migrate(
            state, from_schema=from_schema, to_schema=to_schema
        )
```

输入和输出均为普通 JSON 值对象。版本与所有键的修订由宿主持有；插件不能在返回值中伪造修订。迁移应为确定性的纯转换，不调用网络、模型、工具或平台，不保存凭据。生命周期阶段没有业务 `CallContext`，状态和服务 RPC 会被宿主拒绝。原生 Python 进程隔离不等价于对恶意 Python 代码的系统调用沙箱，插件包仍须符合部署的信任策略。

## 发布与失败边界

1. 新代的包摘要、配置、状态 schema、值 schema、迁移声明和能力授予写入不可变 Generation 身份。未声明的现存版本转换在冻结旧代前拒绝。
2. 候选先准备、启动并通过健康检查。改变 schema 时，准备阶段不把旧格式当成新格式恢复。候选尚无生产调用权限。
3. 冻结路由，排空对该组件的直接调用与已预留的依赖调用，旧 Worker 进入静止状态。一个短数据库事务确认排空后，记录最终不可变来源快照：Bot、组件、旧 Generation、路由 epoch、状态 schema、整体修订、捕获时间、完整键修订/值/删除标志及 checksum。
4. 候选在 READY 阶段对同一来源独立接收两份 JSON 输入，执行两次 `migrate`。宿主校验有限 JSON、大小、目标 schema 与两次规范化摘要一致，然后持久化 shadow 结果。重复检查能发现差异，不构成任意程序数学确定性的证明。
5. 候选 `restore` 得到最终权威状态，随后再次检查健康。`restored_state["durable_state"]` 为 `{key: {revision, value}}`；旧 `Plugin.snapshot()` 无权替换数据库状态或使历史值重新出现。易失缓存由组件根据该值重建。
6. 发布事务按生命周期资源、组件路由、运行策略/宿主、状态头、业务行的顺序锁定，重验操作 lease、旧代与路由 epoch、来源整体修订和 checksum。迁移后的状态、状态 schema、路由新 epoch、生命周期 COMMITTED 以及已有配置发布触发器的 effective 修订一起提交。事务末再次检查宿主与操作的数据库时间，覆盖触发器等待。

迁移失败只产生失败候选和不可变诊断记录，正式状态及旧路由保持原值。冻结后恢复旧代时遵循**当前**维护策略；管理员已暂停业务时，不会因为重载失败而重新打开 admission。

操作 lease 过期同样不应把健康旧代永久留在冻结状态：仍然有效的宿主可在资源的 operation ID、`op_epoch` 和操作所有者精确匹配时终结自己的过期操作，再按当前维护策略恢复旧路由。这只是终结清理权限，不允许续租或发布已过期操作；资源已由后继操作接管或宿主 lease 失效时必须拒绝，不能解冻别人的操作。

每次 schema 变化都递增已有键的修订，包括值未改变的键。被移除键保留递增后的 tombstone，SDK 读取可得到其最新修订及空值；以后重新创建必须 CAS 该修订。新键从修订 1 开始。普通写入继续递增整体状态修订。迁移修订引用生命周期 shadow 收据；原始业务 state-step 收据和所有旧修订保持不可变。

已完成业务 operation 的重放仍可返回它的历史结果，但不会重新执行状态写入。旧 CallGrant 在新路由发布后不能读取、重放 state-step 或提交值。显式降级始终转换**当前**状态，例如 v3 已累计到 9 后降回 v1，不能恢复升级前的 4。

## 崩溃与恢复

来源快照与 shadow 结果分别保存在 `bot_state_migration_sources` 和 `bot_state_migration_results`，按生命周期 operation ID 幂等且不可变。正式状态直到 COMMITTED 事务才变化。

- shadow 已提交、宿主在发布前退出：正式状态仍为来源版本；新宿主取得有效 host lease 后可将旧宿主 operation 标为 ABANDONED，创建新候选并重新从当前正式状态迁移，不等待旧宿主较长的组件 lease。
- 发布事务失败：迁移数据和路由修改一起回滚，不能出现新 schema 配旧代码。
- 发布成功、应答丢失：协调器先读持久操作结果，不凭本地异常恢复旧代；不确定时关闭执行容器。后继宿主读取已提交 schema 和当前值再启动新 Worker，同版本恢复不会再次迁移或重置修订。
- 来源在捕获后被绕过正常守卫的写入改动：即使修订未变化，checksum 比较也会拒绝发布。此检查用于检测错误写入，正常业务写入本身已由排空和旧代 fence 阻止。

## 可执行验证

以专用测试 PostgreSQL 设置 `FOREST_TEST_DSN` 后运行：

```sh
python -m pytest tests/test_state_migration.py tests/test_kernel.py tests/test_bot_store.py tests/test_worker.py -q
```

`test_state_migration.py` 使用真实 PostgreSQL 与原生 Worker，覆盖 v1→v2→v3→显式反向迁移、键删除与修订保留、历史收据、旧 CallGrant 拒绝、非确定性/错误 schema/NaN/非法生产 RPC、事务失败且保持暂停、提交应答丢失、来源 checksum 冲突。独立子进程在 shadow 落库后 `os._exit`，验证宿主死亡后的接管。另有连续 100 次重载，检查旧进程退出、宿主拥有任务数、活动 Worker 数和调用/操作集合恢复基线，并在每次重载后继续累计状态。
