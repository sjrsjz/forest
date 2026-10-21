# Qwen VL 嵌入与记忆设施选择

调研日期：2026-10-21。本文依据[记忆模型](15-memory-model.md)选择第一套实现设施。现已完成[向量化 Wasm 模块](17-embedding-module.md)及一批在线图文验证；记忆数据库和向量索引仍待实现，尚未进行规模性能评估。

## 1. 由记忆模型得到的要求

一个 Bot 的各群共享检索空间，一群一主干不等于一群一张向量 collection。消息、来源、归档资格和经验联想需要本地原子提交；图片需要长期可读的受管文件；向量是可重建的派生索引。

向量设施需要支持选定模型的完整维度、持续增量写入、稳定 point_id、原文 ref、按群/作者/时间等元数据过滤，以及同一消息多个向量表示的归并。每条命中必须回到事实库取得正文，不依赖向量 payload 保存第二份消息。

Wasm 中实现 CQ 编排、调用嵌入 HTTP API、控制召回和图遍历。通用数据库驱动在宿主侧提供资源能力，消息表结构、关系语义、索引发布和恢复均属于 Memory。选择某个原生驱动不使 Memory 成为内核服务。

## 2. Qwen 的实际模型与服务契约

### 2.1 开源模型

Qwen 官方模型卡列出：

| 模型 | 原生维度 | 模型上下文 | 内容能力 |
|---|---:|---:|---|
| Qwen3-VL-Embedding-2B | 2048 | 32K | 文本、图像、视频及混合输入 |
| Qwen3-VL-Embedding-8B | 4096 | 32K | 同上 |

两者支持 MRL 输出降维和任务指令。原生输出是每个输入对象一个稠密向量，不是 ColBERT 式每 token 一向量。官方实现对混合对象作联合编码，默认将结果作 L2 归一化。模型卡的最大上下文不等于部署包装器或在线服务的实际限制，官方示例包装器自身也有更小的默认 max_length。[8B 模型卡](https://huggingface.co/Qwen/Qwen3-VL-Embedding-8B)、[官方输入说明](https://github.com/QwenLM/Qwen3-VL-Embedding)、[编码实现](https://raw.githubusercontent.com/QwenLM/Qwen3-VL-Embedding/main/src/models/qwen3_vl_embedding.py)。

Qwen 的 `qwen3-vl-embedding` 百炼服务名不能与上述开源模型 ID 等同。百炼当前公布的默认维度是 2560，并有独立的融合参数、媒体和请求限制。[百炼多模态嵌入 API](https://www.alibabacloud.com/help/zh/model-studio/multimodal-embedding-api-reference)。

### 2.2 QiYu 使用路径与第一版选择

本地 Agentlib 的 `integrations/siliconflow.py` 默认模型是 `Qwen/Qwen3-VL-Embedding-8B`。它以单独的 text/image 对象组成批量请求；`memory/encoder.py` 再把文本和各图片的输出平均为单一向量。这个本地默认值不证明当前 QiYu 部署仍采用相同配置；当前 compose 另含开发用文本嵌入配置，不能把它当作在线 VL 实例配置。

第一版选择该 8B 模型的在线 API 路径，默认使用 MRL 的 2048 维 float32 输出，4096 维作为质量比较的对照。实际 endpoint 已通过 Wasm 返回 2048 维文本及图片向量，记录见[模块验证](17-embedding-module.md)。Bot 主机先不加载 8B 推理模型；本地推理是另一项需要硬件与延迟实测的部署选择。

硅基流动现行 `/v1/embeddings` 文档明确提供 VL 请求，支持文本对象、图片 URL/base64 对象和混合列表；该服务当前不支持视频嵌入。文档的 dimensions 枚举仍混有文本模型条目，虽有 VL 降维示例，不能由此推断任意 MRL 维度或完整开源模型能力均已开放。[硅基流动嵌入 API](https://api-docs.siliconflow.cn/docs/api/embeddings-post)。

因此第一版采用单独文本/图片表示，不把混合批量列表解释成一个融合对象，也不在适配器中静默平均输出。模型原生支持联合编码，但该 endpoint 是否支持相同联合对象、任务指令和降维范围，必须分别确认；未确认的能力不属于第一版契约。

## 3. 索引表示与查询

第一版固定编码配方为：

| 单元内容 | 索引表示 |
|---|---|
| 纯文本/CQ 消息 | 规范 CQ 文本；超过输入长度时拆为稳定跨度 |
| 只有图片 | 每张图一个表示；纯媒体占位不额外制造文本点 |
| 文字与图片 | 规范 CQ 文本，加每张图的独立表示 |

图片只在发起嵌入时解析为服务可读 URL 或临时 base64 请求内容。生成的 base64 不写回消息、日志、向量 payload 或模型文本。失败的图片表示不能用一段占位文字伪装成成功的图片向量。

同模型的待办合并为有界批次；同一空间、相同媒体输入可复用嵌入计算结果，不因跨群重复投稿重新计算图片，但各条发言仍有独立 point_id 与来源。普通上下文读取和召回不会重新嵌入已归档文档。

所有表示共享同一模型空间和输出维度；文本可命中图片，图片也可命中文本。没有证据表明不同模型或仅名字相近的服务空间等价时，不能互相查询。

每个查询表示分别搜索并应用该表示配置的相似度门槛；同一 MemoryId 的多个文档点先取最大相似度，同一查询表示只保留一项。多个查询表示的候选以最佳合格相似度形成第一版 OR 型种子集合，再按全局种子上限选择。纯图片查询不额外制造空文本查询。多个图片点不能挤满结果而只返回一条消息；查询需要数据库分组或有界补取。

这是独立模态检索融合，不能等同于理解“这段文字限定这张图片”的原生联合向量。以后若选择已验证的联合编码 endpoint，应在新记忆空间创建前固定对应配方，并使用联合对象构造查询与文档，不能在已有空间里临时改变配方。

使用 Cosine 距离；本地需要降维时须按模型支持的 MRL 方式处理，并在截取后重新归一化。服务直接输出目标维度时按其实际契约处理。第一版已验证服务直接输出 2048 维及模块归一化，4096/1024 维的质量比较仍在独立试验空间进行；不能仅凭压缩倍率宣称召回质量相同。

索引空间固定模型 ID、维度、归一化、文档/查询指令及编码配方。endpoint 和凭据属于可变连接设置；如果新 endpoint 不提供同一契约，则不能作为同一空间的替代。

## 4. 设施比较

| 设施 | 与当前模型的适配 | 对 Forest 的影响 | 本轮选择 |
|---|---|---|---|
| Qdrant Server | 固定维度稠密向量、过滤索引、稳定点 ID、分组查询 | Wasm 直接使用现有 HTTP 能力；独立向量服务 | 第一版采用 |
| LanceDB OSS | Rust 嵌入式，支持向量与元数据查询 | 需要原生向量资源接口和持续写入后的索引维护 | 嵌入式备选 |
| Qdrant Edge | 已提供 Rust 嵌入式 API | 也需要原生资源接口；官方目前标为 beta | 后续评估 |
| Milvus Standalone | 高维向量、多索引和过滤 | 可通过 HTTP 接入，但增加一套集合与查询维护选择 | 已有设施时可选 |
| PostgreSQL + pgvector | 普通 vector ANN 上限 2000 维，halfvec 上限 4000 维；2048 维可用半精度索引 | 可用于已有 PostgreSQL 的环境；当前仍需新增服务与驱动 | 本轮保持 SQLite + Qdrant |

Qdrant 的向量参数、payload 索引、按消息 ref 分组查询直接覆盖当前需求；选择它是接入边界和功能匹配的判断，不是已经测得它最快。[向量](https://qdrant.tech/documentation/manage-data/vectors/)、[索引](https://qdrant.tech/documentation/manage-data/indexing/)、[分组查询](https://qdrant.tech/documentation/search/search/)。

Qwen 输出的一个稠密向量不要求数据库支持 token 级 multivector。多个消息表示先用多个普通 point 表达即可。Qdrant 的 MaxSim 会按查询子向量累计最佳匹配分数，和本方案的普通余弦门槛不同，不能只因名字叫“多向量”就直接套用。[Qdrant multivectors](https://qdrant.tech/documentation/manage-data/vectors/)。

LanceDB 可在 Rust 进程内运行；OSS 的持续写入需要自行安排 optimize/reindex，未索引行通常仍参与平扫，索引维护节奏会影响延迟。[Rust 库](https://docs.rs/lancedb/latest/lancedb/)、[索引维护](https://docs.lancedb.com/indexing/reindexing)。Qdrant Edge 现在同样是嵌入式选择，不能再笼统声称 Qdrant 只能服务端运行，但其 beta 状态与 Server 应分开评估。[Qdrant Edge](https://qdrant.tech/documentation/edge/)。

Milvus 当前 3.0 文档将 Standalone 描述为单镜像部署，不能沿用旧版本“必须多个容器”的结论；本轮不选择它，不代表其无法满足需求。[Milvus 部署形态](https://milvus.io/docs/overview.md)。pgvector 的限制是相应 ANN 索引的维度限制，不是 PostgreSQL 无法存储 4096 个浮点数。[pgvector 官方说明](https://github.com/pgvector/pgvector)。

## 5. 第一套设施组合

```text
Memory Wasm 模块
  ├─ 模块私有 SQLite：单元、归档集合、联想、索引待办
  ├─ 受管文件：图片等实际字节
  ├─ HTTP → Qwen3-VL-Embedding-8B 服务
  └─ HTTP → Qdrant Server：向量 + 原文 ref + 过滤元数据
```

### 5.1 SQLite 事实库与关系

原文、归档资格和联想放在同一个 Memory 私有 SQLite 文件；与宿主配置 SQLite 分开。结构化单元通过 forest-serde 保存，关系和查询常用字段使用有类型的索引列。群号、作者、时间、单元类型和规范 CQ 文本可直接查看；消息段与媒体仍以完整类型为准，索引列由同一写入生成。

逻辑集合为 trunks/units/archived/recall_links/vector_entries，另有媒体资源记录；trunks 保存各群序号分配状态。媒体记录在接纳事务中固定引用及获取来源，随后异步保存字节，消息位置不随下载完成顺序改变。表布局和查询属于模块，不进入内核。vector_entries 同时就是持久索引待办，不再复制一套通用任务表和 outbox。

主干邻居通过 `(group, sequence)` 范围索引读取；联想按 trigger/recalled 两端建立索引，遍历时去重端点对。第一版不需要专用图数据库，也不把整图搬入 Wasm。高连接节点可采用随机起点的环形抽样：为规范端点对生成固定散列排序键，按端点与该键建立邻接索引，从本次查询固定的随机起点作有界范围读取，末尾回绕。正反形成记录按同一端点对去重后再取上限，不用全表 `ORDER BY random()` 假装有界工作。

SQLite 使用短事务、单写入入口和独立有界读取。WAL 支持读写并行，但同一时刻只有一个写者；外部模型和 Qdrant 请求不占用本地事务。[SQLite WAL](https://www.sqlite.org/wal.html)。是否达到长期写入与读取目标需要负载验证，不能由“使用 WAL”直接推导吞吐保证。

现有宿主配置 get/set/reset 和整文件读写不足以直接提供这个数据库。实现前需要独立确定模块私有 SQLite 的连接资源、参数化查询及原子短事务入口；不能把 SQL 藏进配置值，也不能每条消息整文件覆写数据库。该入口只提供通用原语，不硬编码上述表。

### 5.2 Qdrant 点与发布

每个记忆空间使用一个 collection，第一版维度为 2048，距离为 Cosine，启用正常增量索引。每个 VectorEntry 使用独立 UUID point_id，本地保留它与 MemoryId/消息段路径或文本跨度的映射。Qdrant 支持 UUID 与无符号整数 ID，因此不能直接把任意复合 MemoryId 字符串当作 point_id。[Point IDs](https://qdrant.tech/documentation/manage-data/points/)。

payload 仅包含消息 ref、group_id、作者 ID（适用时）、时间、单元类型及表示类型等小型字段。用于过滤或分组的字段建立对应 payload index，按实际查询需要选择，不把所有字段自动建立索引。正文、完整 OneBot 对象和媒体字节不放入 payload。

归档事务创建未发布 VectorEntry；Memory 在事务外按固定源内容生成向量、以原 point_id upsert，并等待服务确认应用完成，再标记已发布。未完成项可显式重试；重试不会重复创建点。向量服务不可用时归档与上下文裁剪照常完成，语义召回报告索引不可用或不完整，不能临时全库扫描替代。

Qdrant 的 HNSW 邻接图只用于 ANN 查找，不承担 Memory 的时序和 RecallLink 关系。实际图扩散回到 SQLite，按同一次只读事务取得有界邻接与来源记录；不会为图一致性跨 HTTP 持有 SQLite 事务。

### 5.3 部署与容量

Server 只增加一个向量服务进程，模型先使用现有兼容在线服务。无需为了这套记忆引入 PostgreSQL、Neo4j、Redis、独立任务队列或本地 GPU 模型进程。

Windows 开发若使用 Docker，Qdrant 数据使用 Docker 命名卷，不直接挂载 NTFS 工作目录。官方要求 POSIX 兼容块存储，并明确记录了 Windows Docker/WSL 挂载问题。[安装要求](https://qdrant.tech/documentation/installation/)。本轮只作部署选择，不启动 Docker 或修改现有 Bot 进程。

按 float32 计算的原始向量容量为 `点数 × 维度 × 4`：

| 向量点数 | 4096 维 | 2048 维 | 1024 维 |
|---:|---:|---:|---:|
| 10 万 | 约 1.53 GiB | 约 0.76 GiB | 约 0.38 GiB |
| 100 万 | 约 15.26 GiB | 约 7.63 GiB | 约 3.81 GiB |

这些是计算值，不是内存实测，不含 HNSW、payload、WAL、图片、原文或维护空间。点数可能大于消息数，文本与每张图均有表示。因而第一版保留磁盘向量及量化的评估空间，而不是假定向量全部常驻 RAM。Qdrant 当前文档提供向量和 HNSW 的 memory tier 配置；具体参数按实际安装版本制定，不抄旧版开关。[存储](https://qdrant.tech/documentation/manage-data/storage/)。

## 6. 实现前验证

以下是决定正式配置所需的针对性验证；第 1 项已完成基础连通性与维度确认，其余仍待实施：

1. 实际 endpoint 的文本与图片输出维度、响应顺序、同空间跨模态检索及真实延迟；分别记录 API、ANN、回表和扩散耗时。
2. 用带人工相关项的群聊文本、@、回复、表情、图片及图文组合验证编码配方；比较完整维度与 MRL 降维，校准各查询表示的种子门槛。
3. 在十万及百万点、持续新增和常用过滤条件下，测量 ANN 相对精确检索的召回率、延迟、磁盘和内存；消息数与点数分别统计。
4. 对固定接纳身份和位置打乱归档/联想完成顺序，确认主干、关系和重复发布的最终结果不变。
5. 验证静默裁剪、跨群来源、图片回读、文本模型去图、进程中断后的未发布项，以及有界高连接扩散。

数据模型独立于设施成立；完成这些验证后才能声称该组参数可用于长期 Bot 实例。设施选择不会把正式 Agent 行为、数据库宿主 ABI 或未测量的性能提前标为已实现。
