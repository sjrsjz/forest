# v28 独立候选与在线切换（2026-10-12）

从提交 `89138c8` 构建的 `forest:runtime-candidate-89138c8` 镜像 ID 为 `sha256:a8d1810982cedbf76b7549b7a4f0e61ecca4f1655b4a453e169a6769c0506459`。此版本装载普通控制包的包私有持久状态和 `forest.memory-graph` 1.93.3 的 GIF 管理结果契约。新建 `forest_deployment_v28`、`forest_host_v28`、独立 Bot 数据库及 `forest-candidate-v28_packages` 包卷，部署身份为 `forest/candidate-v28-20261012`，Bot 身份为 `forest-main-v28`，管理入口为 `http://127.0.0.1:18099`。沿用的密钥卷只读挂载；新引导凭据、角色口令和脚本只保存在忽略的 `.local/candidate-v28`，此文档不记录其值。

首次启动失败于 Host 基线安装。隔离诊断取得 PostgreSQL SQLSTATE `42501`：`forest_host` 对新建的 `forest_host_v28` 无 CREATE 权限。对照健康的 v27，发现 v27 Host 库 owner 为 `forest_host`，而新建 v28 Host 库误设为 `forest`；将**仅 v28 Host 库** owner 修正为 `forest_host` 后，原 Supervisor 容器成功启动并达到 healthy。失败诊断事务已回滚，未修改 v27 或源代码。这是候选数据库创建错误，不是 Host SQL 缺陷。

经正式管理动作完成 v28 Host 存储请求、Bot bootstrap profile 和供应 Job。Job 为 `SUCCEEDED`，Bot 达到 `READY/OFFLINE`。从 v27 冻结配置复制配置值并重新签发 v28 专属 Secret 引用和存储角色；未复制消息、记忆或 Agent Run。触发配置仍只许可群 `933179609`，需要显式 @。切换时先让 v27 Bot 的期望及观测状态均收敛到 OFFLINE，再将 v28 置为 ONLINE；没有两个 Bot 同时接入 OneBot。

北京时间约 13:45 的只读采样：v28 Supervisor healthy，部署图 8/8 ACTIVE，Bot 为 READY/ONLINE、运行状态 ACTIVE，Bot 图 22 个模块和 22 个 schema bundle 全部就绪。实际包版本为 `forest.agent-runtime` 4.51.0、`forest.memory-graph` 1.93.3、`forest.vector-index.milvus` 2.23.0。树莓派旧 `alicebot.service` 为 inactive，OneBot `8080` 端口仅有 1 条已建立连接。v27 Supervisor 仍 healthy，Bot 保持 OFFLINE，作为隔离回退对象。

新 Bot 的入站和 Memory 来源账本已随自然群消息增长；首个采样分别为 12 条，随后入站 13 条，均被分类为 `group_not_allowed`。许可群 `933179609` 的入站仍为 0，Agent Run 为 0；因此这里没有宣称真实 @→Agent→Provider→发送链、在线 GIF 图片召回结果或长期成本已经通过。此轮未主动向 QQ 发消息。自然入站账本有增长并不能代替许可群触发验收。

回退顺序：先通过 v28 管理面将 `forest-main-v28` 设为 OFFLINE 并确认观测状态，再通过 v27 管理面将 `forest-main-v27` 设为 ONLINE；两套数据库及包卷独立保留，回退不会合并 v28 新产生的消息和记忆。
