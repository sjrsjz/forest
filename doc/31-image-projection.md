# 模型输入图片投影

`/module/image-projection` 是独立、可热重载的 Wasm 包。`/prepare` 接收受管媒体引用，返回 `Result<Blob, Error>`，Blob 包含本次模型输入使用的 MIME 与字节。Memory 的 `/project` 调用它，再形成上下文需要的数据 URI。Context 仍只格式化消息，Provider 仍发送调用方提供的请求。

```text
Memory /project → image-projection /prepare → Media /get
                         ↓
                   模块私有派生缓存
```

原始消息、媒体引用与 Media 中的原始字节不变。嵌入及向量查询继续使用原始媒体来源；本模块分别生成聊天模型与重排模型的输入图片视图。当前视图仍关联原来的受管引用，活动上下文和召回历史的聊天图片使用同一条 `/prepare` 路径。

## 处理与持久化

依据图片实际内容识别 JPEG、PNG、GIF、WebP，无法识别格式或处理失败时返回图片错误。尺寸已符合要求时保留原字节，MIME 使用识别出的格式。超出最长边上限时等比例缩小，不放大；缩放前应用图片的方向信息。

缩放后的 JPEG 使用质量 90 编码为 JPEG，其他格式输出 PNG。GIF/WebP 缩放采用静态首帧；原始动画仍完整保存在 Media。处理在 Wasm 中完成，使用优化的 guest 构建；`/prepare` 与缩略图入口共同获取 `/project` 宿主锁，以串行执行控制同时存在的解码工作区。

派生图写入本模块私有 SQLite，以 `(reference, max_side)` 为键。Media 中已经取得的原图字节不可变，因此已完成的同键派生图可复用。缓存只保存缩放结果，不复制尺寸已经合适的原图；热重载后仍可读取缓存。配置更换尺寸后使用新的键，不覆盖旧尺寸结果。

Memory 将单张图片准备失败记为诊断并跳过其内联图片，保留原始消息与文本引用。该失败不触发模型重试，不修改原始记忆。

## 重排缩略图

`/thumbnail` 接收 `{ reference: string, max_side: i32 }`、返回 `Result<Blob, Error>`。它总是解码静态首帧并重新编码，等比例缩小且不放大小图；因此尺寸已经合适的动图、附加大容器数据也不会把原始大文件传给重排服务。JPEG 输出 JPEG，其他格式输出 PNG，原始动画仍由 Media 保存。

最长边由调用者传入，须为正整数；Memory 使用自己的 `rerank_image_max_side` 配置，默认 1024。解码使用本模块既有 `/settings.decode_memory_mib`。缩略图缓存的引用键为 `thumbnail:<reference>`，与聊天图片派生缓存分开，热重载后可复用。该接口只允许 Memory 与受信管理员调用，见[多模态召回重排](32-multimodal-reranking.md)。

## 配置

WebUI 中选择 `image-projection` → 配置表 → `/settings`：

| 字段 | 默认值 | 含义 |
|---|---|---|
| `max_side` | 4096 | 派生图最长边，单位像素 |
| `decode_memory_mib` | 64 | 图片解码器的内存预算，单位 MiB；调整时需为模块内其他数据和编码留出空间 |

两个字段都须为正整数，保存后重载本模块生效。默认 4096 对应 DeepSeek 当前多图片请求的单边限制；其单图上限为 8192，包含至少 15 张图片时降为 4096，依据为 [DeepSeek 视觉输入限制](https://api-docs.deepseek.com/guides/vision/#limits)。这是图片尺寸投影，不是上下文裁剪或 token 预算。

模块配置仅自身读取、受信管理员写入；`/prepare` 的 callers 为 Memory 与 `/user/admin`。Media 原始读取与派生缓存写入分别归各自模块。

## 构建与验证

```powershell
cargo run --locked --offline -j 2 -p forest-image-projection --bin package-image-projection
cargo test --locked --offline -j 2 -p forest-image-projection --test wasm
```

外部包位于 `packages/image-projection`，包含 Manifest、Schema 与 Wasm。真实 Wasm 验证覆盖长图等比缩放、小图字节保留、热重载后的缓存复用、尺寸配置更新及解码失败后的继续服务；另覆盖 20 MiB 小尺寸图片容器的静态缩略图和独立缓存复用。

本地单图验证不连接 Bot：

```powershell
cargo run --locked --offline -j 2 -p forest-image-projection --example verify-image -- <source> <destination>
```
