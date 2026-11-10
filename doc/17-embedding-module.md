# Qwen VL 向量化 Wasm 模块

本模块提供文本和图片向量化。Memory、归档、图扩散与向量索引由其他模块负责。

## 1. 包与接口

源代码位于 `plugins/qwen-vl-embedding`，包身份为 `/module/qwen-vl-embedding`，接口为 `/embed`。使用现有 SDK、配置、HTTP 和生命周期能力，未新增宿主导入，也未修改 forest-serde 的 Value 类型。

```rust
// 本插件内部对 Schema 的 Rust 映射：
use crate::types::{Input, Embeddings, Error};

// /embed 的输入和模块返回值：
type Request = Vec<Input>;
type Response = Result<Embeddings, Error>;
```

`Input::Text(String)` 编码文字，`Input::Image(String)` 接受服务可读图片 URL 或临时 base64/data URL。混合列表代表多个独立输入，按输入顺序返回一个向量，不融合或平均。空列表直接返回空结果，不调用模型。

接口以包内 Manifest 的 Schema 和 Forest Value 为契约。Rust 类型定义位于本插件的 `src/types.rs`，调用方可以复制所需类型，也可以直接按 Schema 编码 Value；不依赖本插件或模块专属共享 crate。实现、默认连接及模型设置全部在本插件中。

模型固定为 `Qwen/Qwen3-VL-Embedding-8B`，维度固定为 2048，返回向量进行 L2 归一化。外部响应只在边界解析一次，确认输入索引、数量、维度和可归一化性；错误通过模块 Result 返回，不修补成替代向量、不自动重试。

浮点数据采用明确的 JSON 二维数组存放在 Embeddings 的 `json` 字段，外层走现有类型化 ABI。Rust 调用者使用 `result.vectors()?` 解码为 `Vec<Vec<f32>>`。因此 Forest Value 仍保持可哈希，未引入浮点键或对 NaN 的自定义相等规则。

接口允许 `/module/**` 与 `/user/admin` 调用；其他主体由静态 ACL 拒绝。HTTP 错误返回状态码及可用的供应商 request ID，不转发原始错误响应正文或打印凭据。Host 错误的文字仅用于诊断，不参与路由或错误类型判定。

## 2. 配置与热重载

模块在 create 中读取自己的 `/connection`：

```json
{
  "endpoint": "https://api.siliconflow.cn/v1/embeddings",
  "api_key": "<本地设置>"
}
```

默认凭据为空；模块仍可加载，非空向量请求返回 NotConfigured。配置仅允许模块自身读取、管理员写入；受信宿主调试台仍遵循其已经明确授权的全配置查看能力。凭据不写进 Manifest 或源代码。

连接设置在 create 时读取并保持为实例内不可变值。更换 endpoint/凭据后执行热重载生效，原请求使用原实例连接，新请求使用新实例。模型和维度不是请求参数或可变连接配置，不在调用途中改变向量空间。

模块没有后台循环、未托管任务、订阅或外部连接句柄。每次 HTTP 请求由其接口任务持有，重载直接使用调度器现有 Drain/AtYield/Force 语义。

## 3. 构建与隔离验证

```powershell
cargo run --offline --locked -p forest-qwen-vl-embedding --bin package-qwen-vl-embedding
cargo test --offline --locked -p forest-qwen-vl-embedding --test wasm
```

打包生成 `packages/qwen-vl-embedding/manifest.toml`、`module.wasm` 及 `generated/` 下的 Schema/Value 文本；接口输入和输出的具体文件由 Manifest 引用。文件采用 Forest Schema/Value 语言表示，不依赖 Rust，并纳入包的 SHA-256 索引。默认值中无真实凭据；Wasm 构建产物忽略提交。宿主调试台可以发现本地包。

在线验证程序读取本地 Connection JSON，为隔离调度器建立临时配置库并加载实际包，发出一批请求后卸载，不连接 OneBot：

```powershell
cargo run --offline --locked -p forest-qwen-vl-embedding --example verify-qwen-vl-embedding -- <connection.json>
```

验证使用 Qwen 官方示例图，包含两段文字、图片 URL 和相同图片的 data URL。连接文件应放在忽略提交的位置；命令行只传文件路径。程序不输出凭据、原图 base64 或完整向量。

## 4. 验证范围

Wasm 集成测试覆盖批量映射、响应乱序还原、归一化、空请求、ACL、Drain 热重载期间的请求交接、配置保留，以及 HTTP 错误和错误维度的类型化返回。测试使用本地 HTTP 服务，不需要真实凭据。

在线验证用于检查 endpoint 的维度、输入顺序和跨模态连通性。检索门槛与召回质量需另用实际相关样本评估，单次调用的余弦分数不作为通用阈值。
