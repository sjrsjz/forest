# Bot 停止与再次启动的驱动绑定释放

## 故障证据

管理端显示 READY / ONLINE / FAILED，实际启动错误为 `CONTROL_DRIVER_CONSUMER_CONFLICT`。只读记录显示旧运行图已 STOPPED、旧 Worker 均 DISPOSED，但 Supervisor 的进程内 ControlDriverBindingGate 仍保留旧 BotHost；后续启动虽然能够构建运行图，却在登记消费者时被拒绝，随后又退出。后来反复出现的绑定冲突不能证明最初退出由哪个模块触发，原始异常的完整因果未被当前观察行保留。

旧关闭路径只有无异常返回才执行 gate.detach，且在资源清理前就丢弃 Registry 引用。Registry 同时把“工作排空有错误”和“资源尚未释放”都当成未关闭，导致工作虽已退出，绑定仍可能残留。

## 最终所有权约束

Registry 仅在 Graph 已退休或被新 Graph 围栏、且全部本地组件已停止或合法移交所有权后，发布 resources_released。已完成资源清理时，排空错误仍返回调用方，但不阻止释放租约和驱动消费者绑定。

BotHost 在上述资源释放事实成立后才清空 Registry 引用；先停止续租任务，再释放 HostLease，只有成功释放或确认 STALE_HOST 才清空租约引用。异常路径也执行绑定收尾，条件为 Registry 与租约均已释放。Worker、Graph 退休或租约释放仍未完成时，引用和绑定继续保留，可由准确所有者重试。不存在覆盖消费者、忽略冲突或按 Bot ID 删除其他实例绑定的分支。

设计已先更新至[生命周期详细设计](02-kernel-lifecycle.md)。本次修改只涉及内核运行时和 BotHost，未改变插件协议或业务模块，也未重建数据库。

## 验证与部署

- 10 项针对性检查通过，覆盖排空报错后的资源释放、新实例接管、Worker 未退出时保留所有权、租约释放失败重试、Graph 退休失败重试，以及既有 Gate 和 Supervisor 停止约束。
- 隔离 PostgreSQL 的完整 BotHost 文本场景通过，包括真实 Worker、持久运行图、CQ 查询与关闭；模型和平台均为模拟实现。
- 修复镜像 `forest:runtime-shutdown-ownership-fix` 已构建并原地替换现役 Supervisor，沿用同一实例身份、配置、记忆、插件包目录和管理入口。未发送 QQ 测试消息。

故障恢复时先确认旧图已停止，再重启旧 Supervisor 清除已有进程内残留；随后部署代码修复。对新镜像的再次启动验证不能用该次 Supervisor 重启代替，必须在同一进程内完成正式 OFFLINE → ONLINE 操作。

实际已在修复镜像的同一 Supervisor 中完成 OFFLINE → ONLINE，随后回读 RuntimeGraph 修订 26、22/22 ACTIVE、22/22 schema bundle READY，部署观察为 ONLINE/ONLINE、error_code 为空，管理 HTTP 200。00:30 CST 新版只读观察首样本 violations 为空，没有活动 Run、Job 或待发送消息。记忆和费用数据仍在，RESERVED/UNKNOWN 均为零；后台 DOCUMENT 的日预算退避不影响该启动结论。仅保留一份低频观察，完整 24 小时尚未完成。
