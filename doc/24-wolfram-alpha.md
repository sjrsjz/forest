# Wolfram Alpha 计算工具

`plugins/wolfram-alpha` 是独立 Wasm 包，提供 `/module/wolfram-alpha` 的 `/query` 接口。它沿用 QiYu 当前后端的官方 [LLM API](https://products.wolframalpha.com/llm-api/documentation)，不采用旧调试工具中的网页 WebSocket 协议。

工具网关默认发布 `wolfram_alpha`，通过 Manifest 的 uses 和目标接口 ACL 调用。参数为 `query: string`、可选 `max_chars: integer`；查询使用简明的单行英文，长度为 1–2000 个字符，结果字符额度默认 4000，可选 500–6000，与 QiYu 的工具参数一致。

插件通过宿主通用 HTTP 发出 GET 请求，查询参数只有 `input` 和 `maxchars`。AppID 使用官方支持的 `Authorization: Bearer <AppID>`，不进入 URL、工具参数或结果。插件不重试，不调用模型，也不负责上下文裁剪或消息发送。

成功返回 JSON 文本 `{"status":"complete","query":"...","result":"..."}`。HTTP 501 表示无法解释查询，返回 `status: "not_interpreted"` 和 `suggestion`，由模型决定是否改写后另行调用。其他 HTTP 错误只返回状态码，不复制可能包含凭据的供应商错误正文；空响应和无效 UTF-8 返回工具执行错误。响应中的图片 URL 只是来源信息，不自动转成 CQ 图片或图片字节。

配置项 `/connection` 包含 `endpoint` 和 `app_id`。默认 endpoint 为 `https://www.wolframalpha.com/api/v1/llm-api`，AppID 默认空值；空值不阻止模块加载，但调用会返回未配置。配置只允许模块自己读取、管理员修改，保存后热重载生效。端点、凭据和接口类型映射全部留在插件内，不向内核添加 Wolfram 专用逻辑。

```powershell
cargo run --locked --offline -j 2 -p forest-wolfram-alpha --bin package-wolfram-alpha
cargo run --locked --offline -j 2 -p forest-agent-tools --bin package-agent-tools
```

初次配置辅助程序支持 `FOREST_WOLFRAM_APP_ID`、可选 `FOREST_WOLFRAM_ENDPOINT`。已有实例需先加载新包，在管理端填写 `/connection`，将新工具路由加入网关 `/routes`，再重载网关。更新 Manifest 默认路由会保留已有的当前配置，不自动覆盖管理员设置。

`plugins/agent/tests/system.rs` 使用真实 Wasm 和本地 HTTP，验证网关发现与调用、查询编码、Header 鉴权、501 建议、403 错误不泄漏、输入边界、无自动重试及连接配置热重载。真实服务检查使用临时调度器，从配置库复制连接信息，仅执行一次积分，不连接 OneBot 或调用聊天模型：

```powershell
cargo run --locked --offline -j 2 -p forest-wolfram-alpha --example verify -- data/forest.sqlite3
```

2026-10-22 已使用 QiYu 配置的 AppID 完成真实 Wasm 调用：`integrate x^2 sin(x) from 0 to pi` 返回 `π² − 4 ≈ 5.8696`，调用约 1.8 秒。当前实例经管理员配置入口加入该工具并热重载网关，八个模块均为 Serving；未发送测试群消息。AppID 只保存在本地配置库，未写入源码、Manifest 或本文。
