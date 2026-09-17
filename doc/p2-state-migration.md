# 已撤销：P2 原生组件状态迁移切片

> **历史快照｜禁止作为当前实现规格。** 本文只记录旧纵向切片及其当时的局部验证。它服从 [00 架构总契约](00-architecture-contract.md) 与 01–14 号详细设计；冲突内容已经失效。不得据此新增或恢复旧表、编号迁移、兼容读取、固定业务页面、专用宿主 API、巨型 Store、业务组合根或产品子 Agent。需要复用算法时，必须先把 owner、namespace、Repository contract、Generation、UnitOfWork participant、并发 fence 和 management contribution 映射到当前设计。

> **本文件是防误用墓碑，不是实现规范。** 早期方案把未发布数据库升级、Worker 状态转换和模块热重载混在同一“迁移”概念中，已经撤销。不要从 Git 历史恢复其中的表、API、测试或兼容逻辑。

当前规则只有以下几条：

1. Forest 尚未发布。数据库 schema 变化直接修改对应模块的内容寻址 baseline，并重建开发/测试数据库；不实现编号迁移、旧表读取、双写、数据搬运或非空旧库升级。
2. 热重载通过新 Generation 的 `prepare → health → route cutover → old drain` 完成。进程栈、协程、客户端和连接不迁移。
3. 如果某个模块确实需要把少量已声明的持久运行状态交给新 Generation，必须按[生命周期设计](02-kernel-lifecycle.md)重新定义独立的 state handoff contract：固定来源 revision、目标 schema、输入/输出摘要、shadow 校验、资源上限和失败回退。它不能读取未知旧表，也不能成为数据库兼容层。
4. 没有明确业务需求和故障测试的 handoff 不实现。旧代可以通过持久 checkpoint 排空或由新代从权威事实恢复。
5. 首个正式版本发布后的模块数据升级不在当前范围；届时为具体模块单独设计升级边，不能建立宿主级全产品迁移链。

当前实现以[00 架构总契约](00-architecture-contract.md)、[02 生命周期](02-kernel-lifecycle.md)、[13 微内核控制面](13-microkernel-control-plane-redesign.md)和[14 干净数据库基线](14-clean-schema-and-payload-provenance.md)为准。
