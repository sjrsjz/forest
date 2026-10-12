# v29 GIF 契约修复候选与在线切换（2026-10-12）

源码提交 `97bf3ff` 修正内核管理资产表和 `memory.graph` 试验动作对 GIF 的两处 SQL 白名单缺项；Memory 包为 1.93.4 / SQL 3.92.0。隔离 PostgreSQL 中，真实 GIF 资产上传/发布、认证资产交接到 Memory action、混合输入登记共 3 passed，Ruff 通过。内核基线内容摘要改变，因此新建独立 `forest_deployment_v29`（owner `forest`）、`forest_host_v29`（owner `forest_host`）、Bot 数据库及 `forest-candidate-v29_packages` 包卷，没有将新镜像套到 v28 旧库。

运行镜像 `forest:runtime-candidate-97bf3ff` 的 ID 为 `sha256:711d8478642efc8296173d20d8528e9b6befd069624649615e764151d784784c`。部署身份 `forest/candidate-v29-20261012`，Bot 身份 `forest-main-v29`，管理入口 `http://127.0.0.1:18100`。沿用只读密钥卷，v29 的配对码、角色密码和脚本仅存在忽略的 `.local/candidate-v29`，本文不记录其值。v29 Supervisor 启动 healthy、重启 0 次；正式管理动作使存储请求 SETTLED、供应 Job SUCCEEDED、Bot READY/OFFLINE。配置从 v28 冻结值复制，Secret 重新签发为 v29 专属引用；显式 @ 策略仍仅许可群 `933179609`。

切换时先把 v28 Bot 的期望/观测状态均收敛到 OFFLINE，核验 v28/v29 到 OneBot 8080 的已建立连接均为 0，再将 v29 设为 ONLINE。v29 图修订 1 ACTIVE，22 个 Bot 模块 ACTIVE，Memory 为 1.93.4 / SQL 3.92.0，Agent 为 4.51.0。切换后 v28/v29 的 OneBot 已建立连接数为 0/1，两套 Supervisor 均 healthy、重启 0 次。v28 的独立库与包卷保留。

正式 v29 管理入口对从 v28 已归档 Payload 只读提取、SHA-256 核对的 1,352,134 字节 GIF 完成 6 分块上传；`memory.trial.start` 成功，试验从 CLAIMED/PENDING 进入 READY，`memory.trial.result` 返回 0 项。v29 从空记忆开始，0 项只能证明 GIF 输入的资产、Memory 登记、VL 嵌入及查询链没有旧版 500；尚不能证明 GIF 来源的结果展示。此轮没有主动向 QQ 发消息，许可群真实显式 @→Agent→Provider→发送及长期负载仍待验收。

回退顺序：先通过 v29 管理面将 `forest-main-v29` 设为 OFFLINE 并确认观测状态及 OneBot 连接释放，再通过 v28 管理面将 `forest-main-v28` 设为 ONLINE。两套库独立，回退不会合并 v29 新产生的消息和记忆。
