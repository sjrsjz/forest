# P4 记忆来源管理与公开遗忘协议

状态：首版纵向切片已实现。底层来源抑制、删除 epoch、派生闭包和物理 scrub 已接入 Web 来源页、审核 plan/apply 与当前 `shell-gate-v7` 群命令；本文同时记录恢复和未覆盖的外部存储边界。记忆命令本身在 v3 引入，v7 只继承其冻结语义。

## 1. 管理对象和首版范围

管理员管理的是 Bot 全局记忆域中的稳定 `source_unit_id`，不是按群建立的独立记忆库。来源列表逐项返回来源群、作者、发生时间、内容类型、来源修订和正文可用状态；跨群来源不能在页面上丢失出处。

首版公开遗忘只接受完整来源单元，等价目标为 `SourceRef(source_unit_id, source_revision, "*")`。接口不宣称可以只删除一张图片或一句文本，因为当前 canonical source payload 依赖整个单元，部分删除会使原摘要和有序 ContentPart 失真。以后增加部分删除必须引入新的内容修订和独立事实身份。

公开协议分四步：

1. `memory.forget.read` 分页读取当前可见、已进入长期记忆资格且来源闭包完整的来源；
2. `memory.forget.plan` 固定请求目标、当前删除 epoch、解析后的别名/遗留保守目标、影响计数、主体、签发者和十分钟期限；
3. `memory.forget.apply` 在同一事务复验计划、权限、宿主、删除 epoch 和解析目标，然后提高 epoch、写永久 suppression、撤销受影响载荷并写管理回执；
4. MemoryRuntime 持续执行有界 scrub 批次，传播迟到的确切来源依赖并物理删除已撤销 body。

列表和计划可以展示内容；apply 回执只保留来源身份、计数和状态，不复制已删除正文。原模型、平台和用户已经收到的外部字节无法由本地 scrub 撤回。

## 2. 计划与并发合流

计划保存 `requested_targets`、当时解析的 `resolved_targets` 和受影响正文数。解析包括确切 alias 依赖、已有 legacy marker 和来源不完整的旧单元。apply 重新解析并要求目标闭包与影响数都和计划一致；若另一事务在其间新增了受影响派生正文或改变需要显式审阅的目标集合，返回 `FORGET_PLAN_CHANGED`，管理员重新预览。普通新消息不改变 deletion epoch，也不会仅因流量使无关计划失效。

apply 对 `bot_memory_clock` 加锁并检查 `expected_deletion_epoch`。与新载荷写入的排列只有两类：

- 新载荷先提交：apply 在同一 epoch 找到并撤销它；
- apply 先提交：新载荷看到更高 epoch 和 suppression，不能带已删除来源写入正文。

计划之后才出现的确切来源链接必须由 `forest_reconcile_source_links` 传播 suppression；scrub 的 `remaining` 包含这种待传播链接，所以恢复器只有在其归零后才报告物理清理排空。多个管理员基于同一 epoch 提交不同计划时只允许一个推进；另一请求返回删除版本冲突，不做最后写入覆盖。

同 operation ID 同输入返回原回执；同 ID 异输入拒绝。apply、来源 suppression、载荷撤销、Web operation、audit 和 outbox 同事务提交，不能出现页面显示成功但删除事实回滚的状态。

## 3. 授权和群聊 Shell

Web 票据增加 `memory.forget.read/plan/apply`。已有受限 issuer 只按原能力升级：read 需要 `memory.read`，plan 需要 `memory.read + memory.config.plan`，apply 需要 `memory.read + memory.config.apply`；disabled issuer 不因迁移恢复可用。

WebUI 可以管理该 Bot 全局范围。群聊 Shell 只能列出和操作当前群主干的来源，不能凭另一个群的 `source_unit_id` 删除跨群内容。Shell 的 plan 和 apply 必须由同一当前 Binding 管理员执行，apply 携带页面等价的 `plan_id + plan_digest + expected_deletion_epoch`。

当前 v4 继承的记忆命令为：

```text
#forest memory list
#forest memory forget-plan <source_unit_id> --expect-deletion-epoch <deletion_epoch>
#forest memory forget-apply <plan_id> --digest <plan_digest> --expect-deletion-epoch <deletion_epoch>
```

`shell-gate-v2` 的命令表和摘要保持不变。记忆命令首次发布于冻结的 `shell-gate-v3`，v4 完整继承 v3 契约并增加 Bot 维护命令，当前 v5 再继承 v4 并增加组件管理；Dispatcher 按持久 `gate_version` 选择冻结 registry，使升级前已排队的 v2/v3/v4 命令继续使用原 schema 和动作映射。

## 4. 持久结构和恢复

迁移新增不可变 `bot_memory_forget_plans`，保存计划身份、原 plan operation、主体、issuer、期望 epoch、请求/解析目标、影响计数、保守扩展标记、摘要与期限。实际完成事实继续使用既有 `bot_payload_forget_requests` 和 `bot_payload_suppression`，不再建立第二套删除账本。

MemoryRuntime 每次只 scrub 有界批次，不在 HTTP 或 Shell 请求中等待全闭包物理删除。状态接口报告当前 deletion epoch、REVOKED body 数、待传播 source link 数和最近遗忘操作。宿主重启后扫描同一账本继续；失效 Host 不能 scrub，新的 Host 从剩余计数恢复。

来源正文已经不可用时，列表不返回伪造默认值；历史遗忘请求仍显示固定 ID、epoch、目标数和是否发生 legacy conservative 扩展。数据库旧页、WAL、备份和部署库正文仍按独立运维阶段处理。

## 5. 验收

- Web 双 Bot 来源列表、计划和 apply 隔离；CSRF、幂等、过期计划和同 epoch 竞争；
- plan 后新增无关消息仍可 apply，解析目标变化必须重新计划；
- apply 与并发派生载荷写入的两种排列都不复活正文；
- apply 事务内故障回滚 suppression、epoch、Web 回执、audit 和 outbox；
- scrub 跨宿主恢复、批次上限和迟到 source link 传播；
- text/image 源均可选择，列表和计划逐项保留群与作者；
- Shell v2 已排队命令继续回放，v3 当前群计划/提交可用，跨群 source ID 拒绝；
- 真实 PostgreSQL 升级保持旧 issuer 权限子集和既有迁移 checksum。
