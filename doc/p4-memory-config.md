# P4 记忆配置与管理界面

每个 Bot 使用独立的 `MemoryConfig`。公开配置由 `MemorySettings` 规范化，密钥只接受写入；读取只暴露 `has_api_key` 与 `secret_revision`，不返回密钥、密文或其摘要。清除密钥是显式操作，空输入默认保留。配置默认禁用。

管理界面沿用 Bot 选择，将嵌入服务、索引费用、图片抓取存储、检索输出分为四组普通表单。流程是读取当前版本、编辑、预览、应用。预览不会调用嵌入服务；保存只表示配置通过校验并已提交。服务状态另行读取真实运行时观测，不能以保存成功代替远端模型可用性或语义质量验收。

## 配置提交

`bot_memory_config_heads` 保存当前版本和密钥版本。计划和已提交版本分别写入只增不改的 `bot_memory_config_plans`、`bot_memory_config_revisions`。计划绑定 Bot、issuer、subject、期望版本、规范化配置、密钥变化摘要和十分钟期限；应用使用 CAS，并在事务末尾重新检查短期 Web 权限期限。相同幂等请求返回原回执，旧请求重放不能推进当前版本。

服务地址是完整 `/embeddings` URL，拒绝内嵌凭据、query、fragment。更换地址必须使用新 `deployment_id`；历史中用过的部署身份也不能绑定到另一地址。模型固定为 Jina CLIP v2，维度选择 512 或 1024，文本与图片能力独立于聊天模型的 vision 配置。`model_revision` 是管理员对模型与预处理版本的声明，并非服务端权重哈希证明。

升级迁移 023 逐项继承已有权限：`config.read/plan/apply` 分别映射 `memory.config.read/plan/apply`，`message.read` 映射 `memory.read`。不会把只读 issuer 扩大为可写，也不会启用停用的 issuer。

## 宿主许可

`runtime(HostLease, revision=None)` 返回保存行，包含宿主内部使用的密文。它允许读取历史版本供诊断，但不构成执行许可。

真正的请求和发布在调用方持有 runtime/Host 锁后执行 `MemoryConfig.guard_in(conn, lease, revision)`。该方法对配置头取得 `FOR SHARE`，要求冻结版本仍是当前版本、enabled 为真、版本的 policy 与当前 Bot policy 一致，并返回该版本保存行。调用方保持相同事务，按自己的锁序继续验证来源、删除 epoch、请求/索引 owner 和最终 Host 时钟，再写入回执。历史读取不能绕过这个最终守卫。

所有配置变化都撤销旧版本后续许可和发布，包括只修改预算的情况。后台为新配置另起计划/epoch。新配置提交与持有许可的发布事务串行；已发出的 HTTP 请求无法被撤回，后续观测和成本不确定性仍由持久 owner 结算，不能因配置变更便认为调用未发生。

## HTTP 接口与界面并发

- `GET /management/v1/bots/{bot_id}/memory/configuration`
- `POST .../memory/change-plans`
- `POST .../memory/management-requests`
- `GET .../memory/status`

所有接口经过当前会话、Bot 可见性和签名权限；写入另需 CSRF 与稳定 `Idempotency-Key`。签名 ticket 不交给浏览器。运行时状态调用 `bot.platform.memory.inspect`，不拼造后端成功状态。

界面分别跟踪 Bot 选择、草稿编辑、读取序号与提交身份。读取、预览、应用的旧响应不能覆盖更新的草稿或另一个 Bot 的表单；密钥草稿也遵循这一规则。版本冲突后需要读取最新配置再预览，不自动把旧草稿应用到新版本。

`tests/test_memory_config.py` 覆盖真实 PostgreSQL CAS、幂等重放、凭据轮换、计划所属主体和 issuer、部署身份、policy 和最终许可串行。`tests/test_web_memory.py` 覆盖 HTTP 配置、跨 Bot 隔离、重启与鉴权。`scripts/smoke_memory_panel.py` 使用实际页面和 Edge，加受控 API 响应顺序验证草稿保护；该浏览器测试不代替真实 Provider 服务验收。
