# v34 隔离候选：图片查询召回跨群文本记忆（2026-10-17）

沿用独立的 `forest-accept-v34` Supervisor、Bot 数据库、假 OneBot 和假嵌入端点。第一 Bot 通过正式运行意图再次上线至 RuntimeGraph r14，22/22 组件 ACTIVE；第二 Bot 和现役 v33 均未接入本次试验。候选端点拒绝所有 OneBot 发送动作，没有向真实 QQ 发消息。

在第一 Bot 当前 `memory.graph` Generation 内，通过正式管理资产上传端口提交一张本地生成的 64×64 PNG（158 字节），并以 `trial:accept-v34-image-query-20261017` 调用 `memory.trial.start`。请求从 CLAIMED、PENDING 结算为 READY，查询模式为 IMAGE。`memory.trial.result` 返回遗忘后仍可见的 3 条归档文本：假群 `933179610` 两条、群 `933179609` 一条；每条均带原群主干及对应作者 `qq:20002` 或 `qq:20001`，已被遗忘的第一群来源没有重新出现。

Bot 库只读账本核对：该试验输入为 `ADMIN_TRIAL`，有一条 `IMAGE/image/png` part，宽高均为 64；`embedding_facets` 为 `QUERY/IMAGE/READY` 1 条且有向量引用，另有 `DOCUMENT/TEXT/READY` 3 条、`DOCUMENT/TEXT/SUPPRESSED` 1 条。每日嵌入账本累计已结算 6 次，UNKNOWN 与预留均为 0；假端点报告 0 tokens。试验后经正式运行意图使第一 Bot 回到 OFFLINE，并停止假 OneBot sidecar；现役 v33 未切换。

此处验证的是生产候选管理端口的 **IMAGE 查询 → 已归档 TEXT 来源**、跨群归因与遗忘可见性。假嵌入端点对任何内容返回同一固定向量，因此结果不证明语义质量或真实成本。候选没有由群聊图片形成 `DOCUMENT/IMAGE` facet，也未验证 IMAGE→IMAGE 种子或真实 QQ 图片入站；这些能力另有隔离 PostgreSQL 与真实 QiYu VL/Milvus 的整链用例，以及 v33 真实群图片媒体接入证据，但仍需针对最终生产候选分别验收。`media.capture` 的公网 URL 校验会拒绝本机和私网地址，不能仅为测试放宽该边界来让假 OneBot 提供本地图片。
