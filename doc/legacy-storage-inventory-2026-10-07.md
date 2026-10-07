# 旧预发布 Bot 存储只读盘点（2026-10-07）

本记录只对应当时运行中的 `forest-clean-postgres-1`，不是通用资源迁移方案。盘点仅查询 PostgreSQL 系统目录和三个 deployment 库的 Bot/profile 身份，没有读取 Secret、消息或数据库正文，也没有停止容器或改写资源。

| deployment 库 | Bot ID | READY profile | 旧分配器派生的数据库与 owner role |
| --- | --- | --- | --- |
| `forest_deployment` | `forest-main` | `storage-profile/forest-main/1` | `forest_bot_40d178eeaa530f486c034901` / `forest_role_40d178eeaa530f486c034901` |
| `forest_deployment_candidate_20261004` | `bot/candidate-offline-20261004` | `storage-profile/candidate-offline-20261004` | `forest_bot_990c1bce05397e6d0ae12b74` / `forest_role_990c1bce05397e6d0ae12b74` |
| `forest_deployment_candidate_v3_20261004` | `bot/candidate-v3-mock-online-20261004` | `storage-profile/candidate-v3-mock-online-20261004` | `forest_bot_046cdc5150e4dd3d7750429c` / `forest_role_046cdc5150e4dd3d7750429c` |
| `forest_deployment_candidate_v3_20261004` | `bot/candidate-v3-offline-20261004` | `storage-profile/candidate-v3-offline-20261004` | `forest_bot_fdc22131c1fa8d6b04f8d65f` / `forest_role_fdc22131c1fa8d6b04f8d65f` |

这四组名称符合已退役本地分配器的 `sha256(bot_id)[:24]` 命名规则；物理数据库的 owner 分别是表中的 role。系统目录显示四个数据库和四个 role 都没有归属注释。`forest_deployment` 与候选 v3 库仍有旧 `storage_profiles`，但没有 `storage_request_pins`；该 PostgreSQL 集群的数据库目录也没有 `forest_host`。因此这只是旧路径的**候选对应关系**，不是可用于物理删除的 Host owner marker/pin 证明。盘点时相关 Forest Supervisor 容器仍在运行。

不得按此表逐个删除或宣称这些资源已被新 Host 账本接管。预发布整体切换应先停止对应 Supervisor、备份需要保留的 Bot 数据和向量集合，再把旧 deployment/Host/Bot 物理资源作为同一环境重建；若需保留单 Bot 原位迁移，须先实现并核验准确归属登记、引用 fence 与回收协议。
