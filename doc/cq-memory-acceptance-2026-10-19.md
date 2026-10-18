# 整条 CQ 消息归档与检索验收

## 问题与最终行为

旧 Adapter 把 OneBot at、reply 转为自造的 `[mention:…]` / `[reply:…]` TEXT part，正文另存一个 part。Memory 按 part 编码、检索并融合，导致一个 @ 获得独立的查询排名贡献。实际样本中，不同账号的 @ 标记相似度约 0.85，正文候选约 0.58–0.64；这些候选通过当时的 0.35 门槛，再沿顺序边扩展为无关闲聊。`rank_score` 是 RRF 排名分数，不能当作 cosine；扩散节点上限和相似度筛选当时均存在，跨群召回本身符合全局记忆设计。

Codec 2.1.1 提供纯读取服务 `onebot.codec.memory-message/1`，负责规范的完整 CQ 编码。Adapter 3.1.1 保存一个完整 CQ TEXT part，并将相同正文用于当前 Context；Memory 的入库、查询、召回因此读取同一种表示。控制段不再单独嵌入，文本中的 CQ 示例经过转义，at、reply、face 保持顺序。未知段只保留无参数类型标记，其完整原事件独立保存。空正文不制造占位消息。

图片 CQ 只含受管引用四元组，真实图片仍为独立 IMAGE part，经 Media 捕获、VL 编码和能力投影处理。图片序号从完整 CQ TEXT part 之后开始，Context 位置、part_id 和 media_ref 的派生使用相同序号，满足既有媒体成本记录契约。平台 URL、file 和 base64 不复制进 CQ 图片参数或未知段正文。Milvus 继续只保存向量与 ref_id，规范 CQ 正文、来源和作者由 Memory 权威存储持有。

设计先于代码更新，见 [CQ 协议](22-onebot-cq-presentation.md)和[多模态记忆](11-multimodal-memory-and-model-projection.md)。没有为旧编码增加在线兼容分支，也没有将 CQ 解析引入内核、Provider 或 Vector 模块。

## 旧数据处理

这是未上线实例的离线重建。正式停止 Bot 后，先在回滚事务内演练，再从原事件重新生成已有 599 条消息的 CQ 正文；104 个图片 part、原事件、来源主干、作者、配置和人格保留。旧查询、向量及旧种子学习出的联想被清除，Memory 图经正式重建接口发布新投影。Milvus 保留同一物理 collection 身份，只清除该 Bot 的派生行。

嵌入成本账本未清空：重建前 143 笔 CHARGED、67,381 tokens 仍保留，新索引生成继续经过原有预算与 Effect 账本。仅有已满足可靠归档资格的来源重新索引；仍在短期 Context 中的消息继续等待正常归档，不为补数量而提前发布。

重建工具仅为本次预上线维护使用，不进入任何运行包。没有数据库版本迁移、双格式读取或恢复旧分段语义的运行分支。

## 验证

- CQ 单次转义、字面 CQ 往返、at/reply/face 顺序、受管图片参数和未知多媒体数据隔离检查通过；与 OneBot 原有检查合计 47 passed。
- 包检查 114 passed、1 skipped；修改文件 Ruff 与 diff whitespace 检查通过。
- 隔离 PostgreSQL 的完整 BotHost 文本、图文两条链均通过，检查实际查询表每个消息只有一个 TEXT 输入。图文路径同时验证当前上下文的媒体位置和 VL 输入；不连接真实 QQ 或对话模型。
- 经正式包选择、候选预检和热替换发布 Codec 2.1.1、Adapter 3.1.1，热替换图修订 9；离线维护后恢复的 RuntimeGraph 修订 11，22/22 ACTIVE、22/22 schema bundle READY。
- Memory 图重建为 SUCCEEDED，发布修订 51。运行期只读检查显示旧自造标记、CQ 图片中的 URL/file/base64 定位符均为零，每条消息一个 TEXT part。
- 00:04 CST 首个新版本观察样本：569 个 DOCUMENT facet 全部 READY，待处理嵌入为零，228 笔费用已结算、147,769 tokens，RESERVED/UNKNOWN 均为零；无活动 Run、Job 或待发送消息，violations 为空。旧观察进程已停止，只保留一份每 300 秒采样的只读观察，完整 24 小时观察尚未结束。

后台索引使用真实已配置的 VL 嵌入服务。未由维护进程发送 QQ 测试消息，也没有用对话模型的自述作为检索准确性证据。上述检查确认输入表示与链路正确，不等于已证明所有语义查询都能得到相关结果。
