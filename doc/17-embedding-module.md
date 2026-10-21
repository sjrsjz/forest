# Qwen VL 向量化 Wasm 模块

本模块已实现并通过真实 Wasm 与在线模型验证。它只提供向量化，不实现 Memory、归档、图扩散或向量数据库。

## 1. 包与接口

源代码位于 `plugins/qwen-vl-embedding`，包身份为 `/module/qwen-vl-embedding`，接口为 `/embed`。使用现有 SDK、配置、HTTP 和生命周期能力，未新增宿主导入，也未修改 forest-serde 的 Value 类型。

```rust
use forest_qwen_vl_embedding::api::{Input, Embeddings, Error};

// /embed 的输入和模块返回值：
type Request = Vec<Input>;
type Response = Result<Embeddings, Error>;
```

`Input::Text(String)` 编码文字，`Input::Image(String)` 接受服务可读图片 URL 或临时 base64/data URL。混合列表代表多个独立输入，按输入顺序返回一个向量，不融合或平均。空列表直接返回空结果，不调用模型。

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
cargo run --offline --locked -p forest-qwen-vl-embedding --bin package
cargo test --offline --locked -p forest-qwen-vl-embedding --test wasm
```

打包生成 `packages/qwen-vl-embedding/manifest.json` 与 `module.wasm`。Manifest 从处理器类型声明生成，默认值中无真实凭据；Wasm 构建产物忽略提交。宿主调试台可以发现本地包。

在线验证程序读取本地 Connection JSON，为隔离调度器建立临时配置库并加载实际包，发出一批请求后卸载，不连接 OneBot：

```powershell
cargo run --offline --locked -p forest-qwen-vl-embedding --example verify -- <connection.json>
```

验证使用 Qwen 官方示例图，包含两段文字、图片 URL 和相同图片的 data URL。连接文件应放在忽略提交的位置；命令行只传文件路径。程序不输出凭据、原图 base64 或完整向量。

## 4. 实测记录

2026-10-21，使用现有 QiYu VL 服务配置进行一批在线验证：

| 项目 | 结果 |
|---|---|
| 执行链 | 隔离 Rust 调度器 → 实际 Wasm → 宿主 HTTP → 在线模型 |
| 输入/输出 | 4 个独立输入，4 个 2048 维 float32 向量 |
| 向量调用耗时 | 1.794 秒；不含包编译、加载及验证程序预先下载示例图 |
| 图片与相关文字余弦 | 0.159763 |
| 图片与无关文字余弦 | 0.092567 |
| 同图 URL 与 data URL 余弦 | 0.999367 |
| 输出归一化 | 各向量平方范数在校验精度内为 1 |

这是一次真实连通性及基本跨模态排序验证，不是群聊召回质量或长期性能基准。尤其不能把旧文本模型的 0.35 阈值直接应用于图文检索；具体门槛仍须按查询类型和相关样本确定。

假 HTTP 服务经实际 Wasm 验证了批量映射、响应乱序还原、归一化、空请求、ACL、Drain 热重载期间的在途请求及排队请求、配置跨重载保留，以及 HTTP 400 和错误维度的类型化返回。该测试不使用真实凭据或发送群消息。
