# P4 原生图文嵌入驱动

`forest.embeddings` 是独立的原生异步 HTTP Provider 边界，复用现有 httpx 和 `MediaSnapshot`。它不连接数据库、不读取配置文件、不保存密钥、不获取图片 URL、不调用聊天模型，也不重试请求。图文混批返回各片段的独立向量，供后续记忆检索合并排名。

## 已核实的 Provider 契约

首个协议为 Jina CLIP v2：`POST https://api.jina.ai/v1/embeddings`，Bearer 鉴权，JSON 字段为 `model="jina-clip-v2"`、整数 `dimensions`、`embedding_type="float"`、`normalized=true`、`truncate=false`，以及 `input:[{"text":...},{"image":...}]`。`image` 使用原始图片字节的标准 base64，不能使用旧示例的 `bytes/url` 字段。文档请求省略 task；查询请求加 `retrieval.query`，不发送 CLIP 不支持的 `retrieval.passage`。核实版本为官方 OpenAPI `2026.09.10.1829`。[官方协议](https://api.jina.ai/openapi.json)

CLIP v2 对齐文本与图像空间；API 支持 1–1024 维，生产配置宜优先选择官方模型说明列出的 512 或 1024 维。混合列表是每项一个向量，不是把图文融合成一个向量。[官方模型卡](https://huggingface.co/jinaai/jina-clip-v2)

官方 FAQ 没有设置嵌入项目数量硬上限。图片限制在 FAQ 为 5 MB，在现行 OpenAPI 为 8 MB，因此本驱动采用更小的本地限额，不把两份文档中的差异视为已经通过真实服务验证。[官方 FAQ](https://jina.ai/embeddings/)

## 公共 API

```python
profile = EmbeddingProfile(
    deployment_id="embedding-production-a",
    model_revision="operator-pinned-revision",
    dimensions=512,
)
inputs = (
    EmbeddingInput("text-facet", text="红色拱桥",
                   source_json='{"bot_id":"bot-a","unit_id":"unit-a"}'),
    EmbeddingInput("image-facet", image=authorized_media_snapshot),
)
prepared = prepare_embedding_request(
    profile, inputs, request_id="logical-request", transport_attempt_id="attempt-one",
    purpose="document",  # 或 query
)
client = EmbeddingClient(
    "https://api.jina.ai/v1/embeddings", runtime_decrypted_key, profile,
)
try:
    result = await client.dispatch(prepared, permitcheck=host_final_permit)
finally:
    await client.aclose()
```

`EmbeddingInput` 恰好包含 text 或已有 `MediaSnapshot`。后者携带内容 bytes、摘要、Bot、单元、part、媒体 revision 与尺寸，原始对象不变。HTTP 请求中只出现文本/图片数据，来源元数据和 capture lineage 放在本地 manifest；这些事实不授权新的读取。`source_json` 限定为 4 KiB 的已知标识/修订字段，拒绝自由正文、URL 字段、嵌套自由对象及重复 JSON key。

`PreparedEmbeddingRequest` 固定 body bytes、request ID、wire SHA-256、space ID 和 manifest。manifest 的 facets 逐一绑定 facet ID、来源、模态、内容摘要、字节长度和可选 capture。`verify()` 重新核对完整请求与 manifest，不能把另一个来源或图片替入既有请求。读取 `.manifest` 返回独立 JSON 对象。

`.replay(new_attempt_id)` 只复制原字节与逻辑身份；它不是重试授权，也不会发起网络。持久 owner 必须先证明该次尝试允许重试，再分配新 attempt ID。Provider 未承诺本协议的请求幂等键，本驱动不发送或假定这种键存在。

`EmbeddingProfile.space_id` 包含 deployment ID、model、model revision、维度、协议版本、文档/查询 task 配对、capture 输入策略、归一化策略和 independent-facets 策略。Endpoint 和密钥与 profile 分离；部署注册方必须保证一个 deployment ID 只绑定一个实际 endpoint/模型安装，切换 endpoint 时创建新 deployment ID。云端只返回模型名称，`model_revision` 是管理员声明，不能当作远端权重 hash 的证明。自托管端点可以实现相同协议，但需要部署方固定实际权重与预处理版本。

## 限额与响应

本地硬限额为每批 1–32 个唯一 facet、文本每项 8192 UTF-8 bytes、单图 4 MiB、整批图片原始字节 8 MiB、请求体 12 MiB、响应体 1 MiB、manifest 256 KiB。超限不静默删图、不缩图、不换成文本，不调用额外摘要模型。文本字节上限不充当精确 tokenizer；服务端仍可按实际 token 上限拒绝，`truncate=false` 保留源输入语义。

响应只接纳当前模型、完整数量、唯一连续 index、正确维度、有限且非布尔数值的 dense float 向量。按 index 恢复输入顺序和 facet ID；拒绝多向量、稀疏字典、base64 输出、缺项、重复项、零向量、明显未归一化向量。允许浮点 norm 的小误差，再以 `l2_float32_v1` 进行固定输出处理。usage 必须有有效 total_tokens；可选模态计数为 null 时省略，单项计数不能超过总数。官方精确分项求和约束明确限定 v5-omni；CLIP 驱动不把可能重叠的计数相加，也不自行推导计费总量。[官方 Usage 定义](https://api.jina.ai/openapi.json)

## 发送、取消与计费事实

| status | 含义 | 成本处理依据 |
| --- | --- | --- |
| RESPONDED | 完整响应通过校验，附有按来源排序的向量 | 已验证 usage |
| REJECTED | 收到明确的 3xx/4xx HTTP 拒绝，包括 429；不跟随跳转 | usage 未知，不能自动记零 |
| NOT_SENT | 可证明没有提交网络，例如关闭、等待容量超时、明确连接建立失败 | 记录该事实供持久 owner 结算 |
| UNKNOWN | 网络提交后断流/超时、5xx、成功响应中存在协议或向量错误 | 保留不确定成本，不能自动重试 |

错误码来自本地固定分类，返回值不复制远端响应体、输入或密钥。错误响应不会被用于更新向量库。source/lease/policy 的最终许可由 `permitcheck` 在真实网络调用之前执行；回调异常传播给宿主。

`EmbeddingCancelled` 继承 `asyncio.CancelledError` 并携带安全 `.result`，标记本驱动能够确认的 NOT_SENT 或 UNKNOWN。取消正常传播，数据库记录由持久 owner 负责；驱动没有隐式提交。池等待计入总 timeout。每次 dispatch 创建驱动自己拥有的 transport task；关闭只取消这些内部任务，不等待调用方捕获取消后的其他工作。transport 回收与连接池关闭各有 1 秒等待限额。若 host permit 回调吞掉取消，仍关闭底层连接池，保留未退出任务并返回 `EMBEDDING_CLEANUP_FAILED`，由宿主监督和重试清理；回调返回后也不能重新发送。外层任务在 finally 中再次关闭同一客户端不会发生相互等待。

API key 只在构造客户端时成为 Authorization header，不进入 profile、prepared manifest、结果、repr 或配置读取响应。当前 [Web 记忆配置](p4-memory-config.md)使用只写输入、加密存储和 has_api_key/secret_revision 读取形式。EmbeddingProfile 不读取 Agent 的 vision 开关；文本 LLM 仍能配合该驱动进行图片召回。持久预算、索引及 Agent 接线见[记忆运行链路](p4-memory-runtime.md)。

## 验证范围

`tests/test_embeddings.py` 使用真实本地 TCP 服务核对精确请求字节、图片 base64 解码、混合输入独立向量、乱序响应还原、来源隔离、坏 index/维度/NaN/Infinity/超大整数/usage、响应编码和大小、无跳转/无重试、明确拒绝的未知计费，以及容量超时、外部取消和关闭资源回收。测试服务返回人为构造向量，只验证协议与生命周期；没有验证 Jina 付费服务可用性、真实语义检索质量、计费金额或远端权重版本。
