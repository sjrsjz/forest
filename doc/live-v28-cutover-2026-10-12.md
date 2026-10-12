# v28 独立候选与在线切换（2026-10-12）

从提交 `89138c8` 构建的 `forest:runtime-candidate-89138c8` 镜像 ID 为 `sha256:a8d1810982cedbf76b7549b7a4f0e61ecca4f1655b4a453e169a6769c0506459`。此版本装载普通控制包的包私有持久状态和 `forest.memory-graph` 1.93.3 的 GIF 管理结果契约。新建 `forest_deployment_v28`、`forest_host_v28`、独立 Bot 数据库及 `forest-candidate-v28_packages` 包卷，部署身份为 `forest/candidate-v28-20261012`，Bot 身份为 `forest-main-v28`，管理入口为 `http://127.0.0.1:18099`。沿用的密钥卷只读挂载；新引导凭据、角色口令和脚本只保存在忽略的 `.local/candidate-v28`，此文档不记录其值。

首次启动失败于 Host 基线安装。隔离诊断取得 PostgreSQL SQLSTATE `42501`：`forest_host` 对新建的 `forest_host_v28` 无 CREATE 权限。对照健康的 v27，发现 v27 Host 库 owner 为 `forest_host`，而新建 v28 Host 库误设为 `forest`；将**仅 v28 Host 库** owner 修正为 `forest_host` 后，原 Supervisor 容器成功启动并达到 healthy。失败诊断事务已回滚，未修改 v27 或源代码。这是候选数据库创建错误，不是 Host SQL 缺陷。

经正式管理动作完成 v28 Host 存储请求、Bot bootstrap profile 和供应 Job。Job 为 `SUCCEEDED`，Bot 达到 `READY/OFFLINE`。从 v27 冻结配置复制配置值并重新签发 v28 专属 Secret 引用和存储角色；未复制消息、记忆或 Agent Run。触发配置仍只许可群 `933179609`，需要显式 @。切换时先让 v27 Bot 的期望及观测状态均收敛到 OFFLINE，再将 v28 置为 ONLINE；没有两个 Bot 同时接入 OneBot。

北京时间约 13:45 的只读采样：v28 Supervisor healthy，部署图 8/8 ACTIVE，Bot 为 READY/ONLINE、运行状态 ACTIVE，Bot 图 22 个模块和 22 个 schema bundle 全部就绪。实际包版本为 `forest.agent-runtime` 4.51.0、`forest.memory-graph` 1.93.3、`forest.vector-index.milvus` 2.23.0。树莓派旧 `alicebot.service` 为 inactive，OneBot `8080` 端口仅有 1 条已建立连接。v27 Supervisor 仍 healthy，Bot 保持 OFFLINE，作为隔离回退对象。

新 Bot 的入站和 Memory 来源账本已随自然群消息增长；首个采样分别为 12 条，随后入站 13 条，均被分类为 `group_not_allowed`。随后许可群 `933179609` 有 5 条自然入站，分类均为 `NO_TRIGGER/mention_required`，Agent Run 为 0：这些记录不能作为显式 @→Agent→Provider→发送链的验收。此轮未主动向 QQ 发消息。

北京时间约 15:45 的在线记忆试验：来源账本中 62 个 source unit 为 ELIGIBLE，75 个嵌入 facet；经 Bot 管理 scope 的标准资产上传与 `memory.trial.start/get/result/image` 端口，对文本「你有什么功能」发起一次试验，约 10 秒 READY，返回 8 项带准确来源群号和主干 ID 的结果。结果第 6 项包含 790×296 PNG，经模块 `trial.image` 分块读取并核对 SHA-256 后，将原图作为独立图片输入再次试验，约 10 秒 READY，返回 8 项；首页 4 项均有图片且为直接种子，群号和主干 ID 齐全。两个试验各调用一次嵌入，均未发送 QQ 消息。这证明本实例的管理侧文本/图片召回及结果归因能工作；来源管理端另查到 3 个符合资格的 GIF 来源单元，但本次返回的 8 项中没有 GIF，因此尚未证明 GIF 管理结果实际展示，也未证明许可群真实 @ 回复或长期负载。

随后按来源页定位到 3 个 ELIGIBLE GIF，仅读取其中一张已归档图片（1,352,134 字节）作为试验样本，摘要与来源账本一致。v28 旧镜像的 `trial.input` 上传声明返回 HTTP 500；容器日志确认内核 `management_asset_uploads_media_type_check` 拒绝 `image/gif`。源码审计还发现 Memory 试验 SQL 输入白名单漏了 GIF。当前源码已修正两处 SQL，Memory 包为 1.93.4 / SQL 3.92.0；隔离 PostgreSQL 的 GIF 资产上传、认证资产交接及试验登记 3 passed，Ruff 通过。v28 在线旧镜像及旧库仍有此缺陷，需独立新 Bot 库验收 GIF 管理结果。

回退顺序：先通过 v28 管理面将 `forest-main-v28` 设为 OFFLINE 并确认观测状态，再通过 v27 管理面将 `forest-main-v27` 设为 ONLINE；两套数据库及包卷独立保留，回退不会合并 v28 新产生的消息和记忆。
