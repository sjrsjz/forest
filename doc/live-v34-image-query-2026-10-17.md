# v34 隔离候选：图片查询召回跨群文本记忆（2026-10-17）

沿用独立的 `forest-accept-v34` Supervisor、Bot 数据库、假 OneBot 和假嵌入端点。第一 Bot 通过正式运行意图再次上线至 RuntimeGraph r14，22/22 组件 ACTIVE；第二 Bot 和现役 v33 均未接入本次试验。候选端点拒绝所有 OneBot 发送动作，没有向真实 QQ 发消息。

在第一 Bot 当前 `memory.graph` Generation 内，通过正式管理资产上传端口提交一张本地生成的 64×64 PNG（158 字节），并以 `trial:accept-v34-image-query-20261017` 调用 `memory.trial.start`。请求从 CLAIMED、PENDING 结算为 READY，查询模式为 IMAGE。`memory.trial.result` 返回遗忘后仍可见的 3 条归档文本：假群 `<GROUP_ID_2>` 两条、群 `<GROUP_ID>` 一条；每条均带原群主干及对应作者 `qq:20002` 或 `qq:20001`，已被遗忘的第一群来源没有重新出现。

Bot 库只读账本核对：该试验输入为 `ADMIN_TRIAL`，有一条 `IMAGE/image/png` part，宽高均为 64；`embedding_facets` 为 `QUERY/IMAGE/READY` 1 条且有向量引用，另有 `DOCUMENT/TEXT/READY` 3 条、`DOCUMENT/TEXT/SUPPRESSED` 1 条。每日嵌入账本累计已结算 6 次，UNKNOWN 与预留均为 0；假端点报告 0 tokens。试验后经正式运行意图使第一 Bot 回到 OFFLINE，并停止假 OneBot sidecar；现役 v33 未切换。

第一阶段验证的是生产候选管理端口的 **IMAGE 查询 → 已归档 TEXT 来源**、跨群归因与遗忘可见性。假嵌入端点对任何内容返回同一固定向量，因此结果不证明语义质量或真实成本。`media.capture` 的公网 URL 校验会拒绝本机和私网地址，不能仅为测试放宽该边界来让假 OneBot 提供本地图片。

随后复用许可群在 v33 已接入的一条 GIF 的公网来源地址作只读尝试，但 QQ CDN 返回 `MEDIA_HTTP_REJECTED`，无法复用。地址只暂存于 Git 忽略的本地文件和候选容器 `/tmp`，尝试后两处均已删除；没有修改 v33 数据或 QQ 登录态。

第二阶段改用公开测试图片 `https://httpbin.org/image/png`，先由候选真实 `PublicImageCaptureClient` 校验为 100×100、8090 字节的 PNG。第一 Bot 通过正式意图上线至图 r16、22/22 ACTIVE 后，向**隔离假 OneBot** 注入一条无触发词的图片群消息和两条被动文本消息。媒体工作及 IMAGE part 均为 READY；静默裁剪使作者 `qq:20003` 的图片来源成为 `ELIGIBLE / IMAGE`，同一群主干上的 `DOCUMENT/IMAGE/READY` facet 有独立向量引用。未触发 Agent 或 QQ 发送。

在该 Generation 新建 `trial:accept-v34-image-source-20261017` 的正式 IMAGE 管理试验，状态 CLAIMED → PENDING → READY。`memory.trial.result` 返回 5 条可见证据：其中图片来源为直接种子，带群 `<GROUP_ID>`、作者 `qq:20003` 和 1 张图片；其余 4 条为两群的文本来源，群号与各自作者保持正确，已遗忘的来源仍不可见。三条入站的分类均为 `FINAL / NO_TRIGGER / mention_required`，Agent Run 为 0。此时账本为 `DOCUMENT/IMAGE/READY` 1、`DOCUMENT/TEXT/READY` 4、`DOCUMENT/TEXT/SUPPRESSED` 1、`QUERY/IMAGE/READY` 2；READY facet 均有向量引用。累计嵌入 9 次已结算、UNKNOWN/预留 0、假端点报告 0 tokens。第二 Bot 独立库仍为 0 来源/0 facet；第一 Bot 已正式退役到 OFFLINE，假端点已停止且发送动作请求数为 0，v33 Supervisor 保持 healthy。

第二阶段证明了最终候选进程中的公网媒体抓取、群图片静默归档、IMAGE 文档 facet、IMAGE→IMAGE 直接种子，以及跨群召回归因。固定测试向量让所有内容相同，不能证明真实 VL 语义质量、跨主干联想边的选择效果或实际费用；公开测试图片也不等同于真实 QQ 图片入站。隔离 PostgreSQL 与真实 QiYu VL/Milvus 的整链用例、v33 真实群图片媒体接入分别覆盖这些路径的其他部分，最终候选的低频真实 QQ 图片和长期运行仍待验收。
