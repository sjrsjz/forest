# 在线 v21 OneBot 传输监督热修（2026-10-11）

在线 `forest-main` 的部署观察曾保持 `ONLINE`，但只读探针发现最新入站停在 2026-10-10 15:32 UTC，Supervisor 进程中也没有到树莓派 `<ONEBOT_HOST>:<ONEBOT_PORT>` 的连接。目标端点从同一 Docker 网络可达。代码检查确认 OneBot 客户端的读取任务在 WebSocket 关闭后会结束，而 `PackageGenerationAdapter.owned_tasks()` 原先只上报包持有的工作任务，没有上报 Effect Driver 的任务。因此 HTTP 健康和 RuntimeGraph 状态都不能发现该失联。

修复给持有长期任务的 Effect Driver 定义 `owned_tasks` 端口；OneBot Driver 上报每个 Binding 的客户端任务，Package Generation 再汇入 RuntimeGraph 的宿主监督集合。当前图中的读取任务结束会使 BotHost 失败，并由多 Bot Supervisor 按既有退避和运行意图重试。定向 OneBot/Driver 测试 `6 passed`，镜像导入检查通过。

线上 Bot 数据库使用 v21 包基线；当前源码中的 `memory.graph` 和 `vector.index` 已升级私有 SQL，因此没有直接将当前完整镜像套到旧库。在线热修镜像 `forest:runtime-candidate-v21-transport-hotfix` 从原镜像 `forest:runtime-candidate-v21` 派生，仅覆盖三个宿主 Python 文件：`package_effects.py`、`package_runtime.py`、`onebot_transport_effect_driver.py`；包工件、卷和 SQL 未改变。旧容器保留为停止的 `forest-live-candidate-v19-supervisor-pre-hotfix-20261011`。首次预创建同端口容器导致 Docker 启动停滞，已删除未启动容器并恢复旧实例；随后先停止旧实例，再创建新容器完成切换。

切换后曾短暂收到 `HOST_LEASE_HELD`，租约重试后部署观察为 `forest-main: ONLINE/ONLINE`、错误码为空；新容器 `running/healthy`、重启计数 0，且存在到树莓派 `8080` 的已建立 TCP 连接。没有向 QQ 发送测试消息。断线期间未被 OneBot 投递的消息无法由此次热修追溯；后续仍需真实低频入站验证，以及独立设计当前包基线到在线数据库的升级路径。
