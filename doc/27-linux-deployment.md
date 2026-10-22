# Linux ARM64 部署

宿主使用 Wasmtime 48.0.7，包含 ARM64 `br_table` 编译问题的官方修复。Wasm 插件与 Vue 构建产物可以复用；宿主和预热程序需要构建为目标平台的本机程序。

## 构建

```sh
rustup target add aarch64-unknown-linux-gnu
pip install cargo-zigbuild ziglang==0.14.1
cargo zigbuild --release --locked -j 2 --target aarch64-unknown-linux-gnu.2.36 -p forest-cli
cargo zigbuild --release --locked -j 2 --target aarch64-unknown-linux-gnu.2.36 -p forest-runtime --example prepare_packages
```

宿主产物为 `target/aarch64-unknown-linux-gnu/release/forest`，预热程序位于同目录的 `examples/prepare_packages`。目标系统需提供 glibc 2.36 或更高版本。

## 运行目录与服务

运行目录包含 `bin/forest`、`bin/qdrant`、`packages/`、`webui/dist/`、`data/`、`.env` 和 `qdrant.yaml`，可选安装预热工具 `bin/prepare_packages`。配置数据库为 `data/config.sqlite3`，模块私有数据库、媒体与编译缓存位于 `data/modules/`。凭据文件只允许服务用户读取，使用 `FOREST_WEB_PASSWORD` 和 `FOREST_ONEBOT_TOKEN`；Qdrant 启用密钥时另设 `QDRANT__SERVICE__API_KEY`。

[服务模板](../deploy/linux/forest.service.in)与 [Qdrant 服务模板](../deploy/linux/forest-qdrant.service.in)中的 `@USER@` 和 `@ROOT@` 分别替换为实际用户与绝对运行目录，再安装至 `/etc/systemd/system/`。Qdrant 使用[独立配置](../deploy/linux/qdrant.yaml)，只在本机监听；WebUI 在 `0.0.0.0:8081` 监听并保留密码登录。

CLI 允许重复 `--package` 声明启动根包，依赖闭包一次规划、共享依赖一次加载。服务模板同时启动 Agent、Shell 与学校帮助包，重启后无需手动恢复后两者。OneBot 使用本机 `ws://127.0.0.1:8080`；不同网关部署可直接修改服务参数。

首次启动服务前，可以在目标机器运行 `bin/prepare_packages data/modules packages/typst ...`，列出全部待启动包以预热本机编译缓存。预热不创建来宾实例，不连接 OneBot，不调用模型 API。跨架构迁移不复制旧机器的编译缓存。

```sh
sudo systemctl daemon-reload
sudo systemctl enable --now forest-qdrant.service
sudo systemctl enable --now forest.service
systemctl status forest.service forest-qdrant.service
journalctl -u forest.service -f
```

Forest 服务通过 SIGINT 进入既有排空关闭过程，等待模块析构保存状态；服务文件不设置自动强制停止期限。模块仍按原有生命周期与 ACL 工作，不因 systemd 托管而改变语义。

## 数据搬迁

先排空旧宿主模块，使 Agent 保存短期窗口并结束私有数据库写入，再备份配置与各模块 SQLite，复制受管文件。向量通过 Qdrant 的 scroll/upsert API 搬迁，保留 point id、payload 与向量，不重新调用嵌入模型。目标集合使用 2048 维 Cosine，向量与 HNSW 使用磁盘存储。

核对数据库完整性、向量数量及目标模块状态后，停用旧实例并启用新服务。保留旧配置、数据库与原服务文件，停止旧服务的自启动不会删除这些内容。
