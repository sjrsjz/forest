# P5 部署审计查询

> **历史快照｜禁止作为当前实现规格。** 本文只记录旧纵向切片及其当时的局部验证。它服从 [00 架构总契约](00-architecture-contract.md) 与 01–14 号详细设计；冲突内容已经失效。不得据此新增或恢复旧表、编号迁移、兼容读取、固定业务页面、专用宿主 API、巨型 Store、业务组合根或产品子 Agent。需要复用算法时，必须先把 owner、namespace、Repository contract、Generation、UnitOfWork participant、并发 fence 和 management contribution 映射到当前设计。

## 1. 范围与权威

部署审计页面读取部署数据库的不可变 `audit_records`，用于核对初始化、部署配置、Bot 目录、Binding 预留、插件导入和 Web 账号操作。它不合并各 Bot 数据库中的 `bot_audit_records`，也不把跨库记录伪装成一条全局顺序；Bot 的 Agent、消息、发送、记忆、组件事件和持久任务仍由对应 Bot 权威页面读取。

当前只有 `admin` 可以读取部署审计。固定 `viewer` 尚不等于审计角色：部署审计会暴露操作者和资源关联，因此在 Bot scope、内容级权限与审计角色落地前不向查看者开放。

## 2. 查询与分页

当前接口是 `deployment.audit` 在 deployment scope 发布的通用 query：
`GET /management/v1/scopes/deployment/{deployment_id}/modules/deployment.audit/queries/records`。
它支持 `action` 精确筛选、`resource_prefix` 前缀筛选、`actor_id` 精确筛选和 `limit`。结果按
`(created_at, audit_id)` 逆序排列；`next_before` 是服务端编码的稳定键集游标。后一页严格读取小于
该二元组的事实，因此同一时间写入多条记录时不会因只按时间分页而漏项或重复。旧
`GET /management/v1/audit` 已删除，不保留兼容别名。页面由 deployment 模块描述符声明模板和
ES module 控制器，通用壳层只发现和挂载贡献。

读取在事务开始和结果取得后两次复核当前管理员账号与会话。页面的晚到请求由单调读取序号丢弃，刷新筛选不会被旧分页结果覆盖。查询有 200 条硬上限，不提供无界导出。

## 3. 脱敏投影

数据库中的 `details` 不能直接作为 HTTP 响应。服务端按已知 action 选择允许的标量字段；未知 action 返回空详情并标记 `details_redacted=true`。即使未来写入方错误地把密码、token、DSN 或嵌套正文放进审计 JSON，通用页面也不会把它们透传到浏览器。

页面只用 `textContent` 建立动作、资源、操作者、请求引用和 JSON 元数据节点。日志内容不解释为 HTML、Markdown、命令或配置输入。审计读取自身不产生新审计事实，避免查看页面造成无限审计递归。

## 4. 容量边界

目标基线由 `deployment.audit` 自己的 schema bundle 和 Repository 拥有审计表与索引；不得从巨型部署迁移、壳层 ORM 或其他模块表读取。当前是否已经完成该切换只看
[实现状态](implementation-status.md)，本页不能把旧 `public` 表描述成实现前提。审计事实按部署生命周期保留；归档必须保留事实摘要、主体和请求关联，并通过独立备份/保留协议处理，不能由浏览器删除不可变记录。

## 5. 验证

PostgreSQL/ASGI 测试固定三条相同时间的记录，验证以 `audit_id` 消除并列歧义、两页无漏项、动作/资源/操作者筛选、非法游标拒绝、viewer 拒绝，以及已知 action 中额外密码和嵌套 token 不离开存储。Compose/Edge 流程在真实初始化和账号创建后打开部署审计页面，核对 `account.create` 与操作者，再继续完整双 Bot、Agent、记忆、热重载和重启恢复验收。
