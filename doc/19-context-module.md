# 上下文格式化模块设计

本模块已实现并通过实际 Wasm 验证，衔接[记忆内容](15-memory-model.md)与[OpenAI Provider](18-openai-provider.md)。

## 1. 职责

Context 是 `/module/context` 普通 Wasm 包，完成一次转换：

```text
调用方已选好、排好序的上下文内容 + 格式化所需信息
  → 配置的 Agent 提示词 + OpenAI 标准 messages
```

它负责提示词和内容表示。要发送哪些记录、何时发送，由调用方决定。短期窗口、裁剪、归档、RAG 筛选、群聊排队及工具执行均在本模块之外处理。

## 2. 一个入口

仅提供 `/format`，成功值为 OpenAI 标准 messages。外部接口由 Manifest 的 Schema 定义，完整结果是 `Result<messages, error>` 的 Forest Value，外层沿用调度器的 DispatchResult。以下 Rust 代码只是本插件对 Schema 的本地映射；调用方可以复制源码，也可以使用其他语言编码相同的 Value，不依赖本插件或公共 Context crate。

```rust
struct FormatInput {
    identity: PromptIdentity,
    items: Vec<ContextItem>,
    images: Vec<ImageInput>,
    multimodal: bool,
}

enum ContextItem {
    GroupMessage {
        id: MemoryId,
        message: Box<GroupMessage>,
        trigger: bool,
    },
    ModelTurn(ModelTurn),
    ToolResult(ToolResult),
    Recall(RecallBatch),
}

struct RecallBatch {
    trigger: MemoryId,
    units: Vec<MemoryUnit>,
    seeds: Vec<MemoryId>,
    graph_json: String,
}
```

- identity 提供本次提示词需要的 Bot 账号、当前群及可用名称。
- items 是本次要发送的完整有序内容，包含群消息、模型回合、工具结果及召回资料；Context 保留传入次序。
- images 将内容中的受管媒体引用映射到本次可用的图片 URL/data URL；由调用方取得资源。
- multimodal 来自 Provider 的能力声明，只控制输出是否包含图片块。

群消息带有稳定的 MemoryId 和显式 trigger 标记，由 Agent 决定它属于触发消息还是普通聊天。Context 不扫描 CQ 来重新推断触发条件。模型回合与工具结果沿用原始类型，不额外要求为未提交的模型回合分配 MemoryId。

RecallBatch 表示一次召回操作，保留触发消息引用、完整来源单元、种子和图信息。每个来源单元仍复用 `MemoryUnit { id: MemoryId, content: MemoryContent }`；作者、时间等从原内容取得。graph_json 保留检索节点、浮点分数和关联边，作为 XML 转义后的 JSON 证据元数据输出；Context 不解析分数、不扩散图，也不重新选择种子。召回包装不加入持久 MemoryContent 枚举，不产生新的记忆单元。

Agent 对每条 @ 分别召回，把整个 RecallBatch 放在对应触发 GroupMessage 前，例如 `GroupMessage(trigger=false) → RecallBatch(for=12) → GroupMessage(sequence=12, trigger=true)`。同一输入批次可以包含多组这样的项。Context 仅按传入顺序格式化，不把它们集中移动到上下文开头；触发与位置规则见[Agent 设计](20-agent-state-and-window.md#21-每条触发消息的召回位置)。

返回值就是 messages，不附带裁剪方案、召回种子集合或 ContextView。调用方使用现有 Provider 请求类型：

```rust
let request = Request {
    messages,
    tools,
    tool_choice,
};
```

工具声明和选择属于 Provider 请求，不需要经过上下文格式化。调用模块在 Manifest 的 uses 中声明 `( /module/context, /format )` 及对应输入、返回 Schema。

## 3. Agent 提示词

模块声明 `/prompt/agent: String`，存放可配置人格，提供英文默认值：

> Be helpful, natural, and attentive to the participants in the conversation. Clearly distinguish established information, historical evidence, and uncertainty.

开头的 System 消息严格按以下顺序拼接：

1. 包内英文 Markdown 系统提示词 [system.md](../plugins/context/src/system.md)，详细定义请求顺序、群消息、触发点、召回批次、来源与图关系、原生工具序列、CQ 和多模态内容。
2. `## Runtime identity values`：在 `runtime_identity` 中注入 Bot 账号和当前群。
3. `## Persona`：原样附加 `/prompt/agent`，不翻译或改写用户的人格配置。

人格负责角色和语气，输入契约负责来源解释。提示词明确说明召回只为对应触发点提供候选证据、跨群来源不能改写为本群发言、关联边不等于事实或同一话题、检索排列不一定是时间顺序。当前会话与实际问题决定回答目标。输入标签不约束模型的输出；Assistant、thinking 和 tool_calls 均沿用原生协议。

提示词在 create 读取为实例内不可变值，修改配置并热重载后用于新的 format 调用。配置读权限为 SelfModule，写权限为 `/user/admin`；format 通过静态 callers 授权给调用模块。Context 的长期配置只有提示词，没有预算、保留数量或会话状态。

## 4. 内容转换

| 输入 | 输出 |
|---|---|
| 群消息 | User：group_message，含稳定引用、trigger、原始来源与规范 CQ 正文 |
| 模型回合 | 原生 Assistant，保留正文、tool_calls、refusal、reasoning_content |
| 工具结果 | 原生 Tool，保留 tool_call_id 与结果正文 |
| RecallBatch | 一个 User 中的 memory_retrieval，包含 records 和 retrieval_graph |

群消息使用 `<group_message source="conversation" ...><body encoding="cq">...</body></group_message>`。保留原群、消息 ID、时间、Bot 账号、消息子类型、作者账号及可用昵称、群名片、群角色、头衔等发送者信息。匿名来源单独标识，不输出匿名 flag 或将占位账号作为真实用户。每条群消息输出一个 User，保持输入次序。

召回使用 `<memory_retrieval source="archive" for_group_id="..." for_sequence="...">` 标明服务的触发点。每个 `<memory_record>` 保留原群、序号、SAME_GROUP/CROSS_GROUP、来源类型和 seed 标记；来源群消息使用 `source="archive"`，模型与工具来源分别使用 `archived_assistant` 和 `archived_tool_result`。`retrieval_graph` 保留 JSON 图及其原引用。Context 不把激活度排序伪装为对话时间顺序。

CQ 来自类型化消息段；字符串、数组和单段输入都通过 `forest_onebot::cq` 转换，不读取可能过时的 raw_message。CQ 转义后再作 XML 转义，字面 CQ 文本与真实消息段保持区别。嵌套转发递归保留 CQ 参数层次及图片位置。规则依据 [OneBot CQ 字符串格式](https://github.com/botuniverse/onebot-11/blob/master/message/string.md)与[消息段定义](https://github.com/botuniverse/onebot-11/blob/master/message/segment.md)。

召回中的 ModelTurn/ToolResult 作为历史资料展示，不作为本轮顶层工具调用或工具结果回放。历史 reasoning 不进入召回正文，当前工作中的 reasoning_content 保持原值。原群身份始终保留，跨群资料不改写成当前群消息。检索命中的文本跨度由调用方用于定位并回读对应完整单元。CQ reply 保留目标 ID；格式化本身不联网获取目标正文，提示词明确 ID 不代表已经拿到被引用内容。

工具调用及其结果的完整性、次序由调用方构造。Context 按类型转换，不修补缺失结果、不生成调用 ID，也不重排工具链。

## 5. 多模态表示

开启 multimodal 时，按消息段顺序将文字和已提供的图片映射为 Text/ImageUrl 内容块。图片保留其所属消息和受管引用，避免同批多图失去对应关系。

关闭时只输出文字，保留 CQ 中的受管图片引用与图片存在标记。没有取得图片输入时也保留存在信息，不编造图像描述。原始 MemoryContent、媒体文件和向量索引不受投影影响。

base64 仅进入本次 ImageUrl 的 data URL，不嵌入 CQ 正文或来源标签。图片、语音、视频的 CQ 投影只保留资源引用及内容属性，不输出 url/cache/proxy/timeout 等传输选项。输入中的媒体 file 应已规范化为引用；若直接包含 base64/data URL，返回 InlineMedia，由接纳方先管理资源。普通文本不会因包含类似字符串而被改写。Context 不下载图片或读取媒体文件。

## 6. 模块边界

处理入口只读取输入与提示词，可重入。模块不调用 Memory 或 Provider，无需声明对它们的运行时调用依赖。与其他模块交换内容时依据 Schema/Value，Rust 结构体名称或 TypeId 不参与模块间契约。热重载使用现有生命周期，没有待迁移的群聊状态。

全部实现归属 `plugins/context`：`src/format.rs` 是私有的格式化逻辑；提示词在本包中；`types.rs`、`memory.rs`、`openai.rs` 是本包所需的 Rust 映射。宿主不依赖这些源码，不直接执行格式化函数，也不解释 MemoryContent。跨模块调用只按对象、接口地址通过调度器传递 Value。格式化阶段的模块错误仅有 CQ 输入错误与 InlineMedia。

## 7. 构建与验证

```powershell
cargo run --offline --locked -p forest-context-module --bin package-context -j 2
cargo test --offline --locked -p forest-context-module --test wasm -j 2
```

生成包位于 `packages/context`，包含 Wasm、Manifest 和 `format.input.schema`、`format.output.schema`。两个 Schema 语言文件可由其他语言的模块作者独立读取；Manifest 中保存等价的类型图，宿主据此检查输入、输出及热重载兼容性。配置和热重载继续使用已有管理能力。

实际 Wasm 验证了英文契约与人格顺序、触发点与整批召回的对应关系、图信息保留、来源转义、原生工具与 reasoning 保留、跨群召回、匿名来源、同批多图及嵌套转发、关闭多模态后的文字一致性、配置跨重载生效和不同群调用之间无状态串扰。还使用不构造本插件 Rust 类型的 Value DSL 输入完成调用，并依据 Schema 语言验证输入输出。测试使用隔离调度器与临时配置库，没有接入 OneBot 或调用在线模型；这些检查验证协议结构，不代替真实模型对提示词的行为验证。
