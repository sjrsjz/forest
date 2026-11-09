# 多模态召回重排

Memory 在粗召回与图扩散之间调用独立的 `/module/qwen-vl-reranker` Wasm 包。模型为 `Qwen/Qwen3-VL-Reranker-8B`，通过远端 HTTP 推理。重排策略由 Memory 维护，不修改原始记忆与向量空间。

## 1. 模块契约

接口为 `/rank`，输入 `Request { query: Input, documents: Vec<Input> }`，其中 `Input = Text(string) | Image(string)`。图片使用调用期的数据 URI 或服务可访问的 URL。输出 `Result<Scores, Error>`，`Scores.json` 为按输入文档顺序排列的浮点数组，不按 API 的返回排名排列；浮点不进入 Forest 的可哈希 ADT。

提供方只负责协议映射、HTTP、文档下标验证和顺序恢复。模型分数原样返回，不再次 sigmoid，也不解释成概率。`/connection` 包含 endpoint、api_key、batch_size（默认 64），管理员可通过配置表修改并热重载。超过批量大小时顺序分批，每批使用同一查询；失败直接返回，不自动重试或回退旧排序。模块不读取 Memory 数据库、不获取媒体、不选择种子。

适配器按单文本或单图片对象调用服务，Memory 分别评分再合并。该边界适用于当前 endpoint 的输入格式；模型原生支持的混合输入不等同于 HTTP 服务接受联合图文对象。

## 2. Memory 流程

```text
触发 @ 的原始 MemoryUnit
  → Markdown / 图片查询表示
  → Qdrant 粗召回，按 MemoryId 合并最高余弦分数
  → 回读归档原文，排除无可评分内容的单元，按粗分选最多 candidate_limit 个候选
  → 候选的文本跨度和真实图片分别成为重排文档
  → 每种查询表示对候选文档评分
  → 按 MemoryId 取所有表示配对的最高重排分数
  → rerank_threshold 过滤，最多 seed_limit 个种子
  → 既有主干/联想扩散，返回带来源的有界区域
```

文本与图片允许交叉匹配。查询只来自当前持续点，不拼接普通聊天。纯 @、回复标记、媒体引用和空白不构成独立的文本查询或文本重排文档；有正文时保留完整 Markdown 投影，纯图片通过实际像素参与。该筛选不删除原文，不重算既有归档向量。

重排图片由 Memory 经 Image Projection `/thumbnail` 取得，最长边随调用传入，由 Memory 的 `rerank_image_max_side` 配置，默认 1024。它总是把解码后的静态首帧重新编码，并等比例缩小且不放大小图；避免尺寸不大的动图或附加大容器内容仍携带数十 MiB 原文件。缩略图持久缓存与聊天模型的 `/prepare` 派生图分开，聊天图片设置不变。

Memory 只保留候选的文本跨度和图片引用，按 rerank_batch_size 分批解析像素、调用重排、合并分数，再释放该批图片；不会预先把 64 个候选的所有图片载入 Wasm。不同查询表示使用同一分批规则，结果始终按原 MemoryId 合并。图片取回失败记录日志并略过该表示，有效文本仍可参与；没有可评分表示时不调用重排服务。

`Memory /settings.retrieval_json` 的初始值如下，均可配置：

| 字段 | 默认值 | 语义 |
|---|---:|---|
| seed_threshold | 0.35 | 向量粗召回门槛 |
| candidate_limit | 64 | 按 MemoryId 去重后的候选单元上限 |
| rerank_threshold | 0.1 | 重排分数门槛，与余弦门槛分开 |
| rerank_batch_size | 8 | Memory 每批加载的文档表示数；控制像素驻留量 |
| rerank_image_max_side | 1024 | 重排缩略图最长边；与聊天图片尺寸分开 |
| seed_limit | 8 | 直接种子上限 |
| result_limit | 24 | 扩散后区域上限 |

向量查询每种表示最多取 `candidate_limit × 4` 个点，为同一单元的多个向量留去重空间；回表后只对最多 candidate_limit 个单元评分。候选单元数、文档表示数与 HTTP 批次数是不同的量。阈值是初始调参值，不是模型准确率保证。

图算法与边权不变。种子仍以激活 1 起步，重排分数不与图激活相加；`graph_json.seed_score_kind = "rerank"`，节点的 seed_score 表示重排分数。只有最终选中且实际进入模型请求的直接种子才按原有规则登记联想。重排失败沿既有召回错误路径报告，禁止悄悄退回粗召回种子。

## 3. 验证与边界

真实 Wasm 回归覆盖：外部 API 乱序下标恢复、跨批次合并、ACL、在途排空热重载、错误正文与无自动重试；Memory 扩大候选后重排选种子、纯控制文本排除、跨模态像素输入、重排失败不回退，以及 Agent/Context/Provider/工具完整调用链。缩略图检查使用含 20 MiB 容器数据的小尺寸 PNG，输出小于 4 KiB，原始 `/prepare` 仍保留原字节，热重载可复用缩略图缓存。

检索质量应使用带相关性标注的数据集评估，并保留同内容的正对照。费用和延迟随候选表示数量、批次数和图片 token 增长；嵌入失败、缺失向量或不可读取的图片不会被重排修复。

连接配置在重排模块中保存，检索参数在 Memory 中保存，各自修改后重载生效。引入重排不要求重建现有向量库；Agent 继续通过 `/recall` 取得完整来源与图信息。

参考：[Qwen 官方模型卡](https://huggingface.co/Qwen/Qwen3-VL-Reranker-8B)、[硅基流动 Rerank API](https://docs.siliconflow.cn/docs/api/rerank-post)。
