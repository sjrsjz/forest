# P2 插件包、锁定依赖与 Worker 环境

> **历史快照｜禁止作为当前实现规格。** 本文只记录旧纵向切片及其当时的局部验证。它服从 [00 架构总契约](00-architecture-contract.md) 与 01–14 号详细设计；冲突内容已经失效。不得据此新增或恢复旧表、编号迁移、兼容读取、固定业务页面、专用宿主 API、巨型 Store、业务组合根或产品子 Agent。需要复用算法时，必须先把 owner、namespace、Repository contract、Generation、UnitOfWork participant、并发 fence 和 management contribution 映射到当前设计。

本文记录当前可执行接口。目标契约继续见 [07](07-python-plugin-development.md)，管理权限和配置计划见 [12](12-webui-administration.md)。普通 Python 插件运行在受监督的独立进程；这里的路径隔离、摘要检查和 SDK 权限边界不构成恶意本机代码的操作系统沙箱。

## 完整、不可变的插件包

`module.toml`、`python/` 中的入口及 helpers、配置/服务/state schemas、模板和锁定 wheelhouse 一起进入 SHA-256 内容寻址快照。静态检查只解析 TOML/JSON/AST 与 wheel 元数据，不导入插件、不执行 setup.py、不运行构建后端。

```text
my-plugin/
  module.toml
  python/my_plugin.py
  python/helpers.py
  schemas/config.json
  schemas/input.json
  schemas/output.json
  dependencies.lock.json       # 使用第三方库时提供
  wheelhouse/*.whl              # 完整依赖闭包
```

只依赖标准库与 `forest.sdk` 的插件无需 lock。所有 vendored wheel 都必须出现在 lock 中。打包过程忽略 `.git`、`.venv`、`__pycache__`、`.pytest_cache` 和 `.ruff_cache`；ZIP 导入明确拒绝这些目录，避免导入与目录捕获产生不同工件。

正式 `(package_id, version)` 只允许对应一个摘要；同版本不同内容失败。`PackageCatalog.build(source, development=True)` 在快照中生成 `原版本去掉local部分+dev.<原始源内容完整SHA256>`，不修改工作区 `module.toml`。helpers、schema、模板、wheel 或 lock 变化都会改变源指纹。进程重启后 `get(digest)`、`find(package_id, version)` 和 `list()` 从持久 catalog 恢复并重新验证文件。

CLI 可构建并导出完整 ZIP，在 WebUI 的插件包页面导入；上传成功后仍须给目标 Bot 预览并应用组件计划。ZIP 入库不会自动给所有 Bot 装载代码，也不授予 manifest 请求的权限。

```sh
forest plugin build ./my-plugin --cache-dir ./.artifacts
forest plugin export sha256:<构建返回的摘要> --cache-dir ./.artifacts --output ./my-plugin.zip
forest plugin import ./my-plugin.zip --cache-dir ./.another-artifacts
```

对应 Python API：`PackageCatalog.import_archive(data: bytes)`、`export_archive(digest) -> bytes`。导出采用固定时间戳和排序条目；相同摘要的内容不会依赖源文件此后的修改。

Web 导入使用 `check_manifest` 在任何发布前拒绝保留的内建命名空间，并以 `bind_version=False` 只暂存内容摘要；最终 `(package_id, version)` 唯一性和当前管理员会话校验在部署数据库的同一登记事务内决定。失效会话可能留下可回收的孤儿工件，不能占用正式版本名。CLI/build 默认仍在本地 catalog 绑定正式版本。`get(digest)` 可读取暂存工件，`list()/find()` 只列本地已绑定版本；Web 可用包列表始终来自经过授权登记的部署数据库。

ZIP 检查在发布文件前完成，拒绝绝对路径、`..`、反斜线、Windows 设备名、大小写冲突、重复条目、文件遮蔽父目录、符号链接/特殊文件、加密、非 stored/deflate 压缩和损坏 CRC。逐项限制声明与实际解压尺寸，不调用 `extractall()`。

| 资源 | 当前上限 |
| --- | --- |
| 上传 ZIP | 32 MiB |
| 包展开内容 | 64 MiB |
| 普通文件 / 单个 wheel | 4 MiB / 32 MiB |
| 包文件 / ZIP 条目 / 目录深度 | 512 / 4096 / 32 |
| 锁定发行包 | 128 |
| 展开依赖环境 | 256 MiB、8192 个文件；单个 wheel 仍受 ZIP 条目上限约束 |

## 在目标平台生成依赖 lock

作者先在与部署相同的 Python/平台构建环境取得 wheel。此步骤允许作者显式下载和解析依赖；Forest 装载阶段没有联网解析或安装逻辑。使用已审核的精确顶层版本，例如把实际版本填入下列 `X.Y.Z`：

```sh
python -m pip download --only-binary=:all: --dest ./my-plugin/wheelhouse "your-library==X.Y.Z"
forest plugin lock ./my-plugin --require "your-library==X.Y.Z"
```

`pip download` 只取得构件，`--only-binary=:all:` 拒绝源码构建。已有带完整传递版本与 hash 的 requirements 文件时，可用 `--require-hashes -r requirements.txt` 重建相同 wheelhouse；跨平台下载需明确目标参数，不能假设本机默认值等于部署环境。参见 [pip download 官方文档](https://pip.pypa.io/en/stable/cli/pip_download/)。

在 `module.toml` 顶层加入：

```toml
dependency_lock = "dependencies.lock.json"
```

`forest plugin lock` 从已有 wheel 生成并校验 lock，原子替换 `dependencies.lock.json`；它不会替作者选择版本、下载缺失依赖、修改 manifest 或执行包代码。也可调用 `forest.environments.create_dependency_lock(source, requirements)`，该函数只返回 JSON 对象，不写文件。

lock 的格式如下，具体值由工具读取当前目标生成，不应照抄示例平台/hash：

```json
{
  "lock_version": 1,
  "target": {
    "implementation": "cpython",
    "python_version": "3.11.13",
    "platform": "linux-x86_64",
    "abi": "cpython-311-x86_64-linux-gnu",
    "sdk_api": "1.0"
  },
  "requirements": ["your-library==X.Y.Z"],
  "wheels": [
    {
      "file": "wheelhouse/your_library-X.Y.Z-py3-none-any.whl",
      "name": "your-library",
      "version": "X.Y.Z",
      "sha256": "<该wheel完整字节的64位十六进制SHA256>"
    }
  ]
}
```

`target` 必须与部署解释器的实现、完整版本、平台、ABI、SDK API 完全一致。即使 wheel 是 `py3-none-any`，Windows 生成的 lock 也不会被 Linux 部署默默接受；在目标构建环境重新生成相应发布版本的 lock/工件。`current_target()` 可供构建工具输出真实目标。

顶层 requirements 必须使用不含通配符的精确 `==`；允许 extras 和环境 marker。每个 wheel 的锁定名字/版本必须匹配文件名、`.dist-info` 和 METADATA，兼容当前 wheel tags/Requires-Python，并满足完整的传递依赖及 extras/marker 选择。缺少依赖、版本冲突、重复发行包、闭包之外的多余 wheel 和未锁定 wheel 都会失败。URL/VCS/源码依赖必须先在作者构建环境产出受支持的 wheel。

每个 wheel 校验完整工件 SHA-256 与 RECORD 的逐文件 SHA-256/384/512、大小和覆盖范围。支持标准 site-packages 布局以及 `.data/purelib`、`.data/platlib`，可接受目标标签匹配的预编译扩展；不安装 scripts/headers/外部 data scheme，不运行 `.pth`，拒绝 SDK/标准库遮蔽与重叠文件。这些限制会产生明确构建错误，不会退回调用 pip 或修改宿主环境。wheel 格式依据 [Python Packaging 官方规范](https://packaging.python.org/en/latest/specifications/binary-distribution-format/)。

## 不覆盖活跃环境

候选启动前，`EnvironmentCatalog.materialize(snapshot)` 将已经验证的 SDK 与依赖字节写入新的内容寻址环境。环境描述包含完整解释器 SHA-256、目标信息、lock 摘要和每个 SDK/依赖文件摘要。SDK 或依赖发生变化会产生不同环境，现存目录只校验和复用，不原地安装/修补。

Worker 使用已验证的 base CPython，以 `-I -S -B` 启动；搜索路径只增加冻结 SDK、该环境的依赖目录及不可变包的 `python/`。不加载宿主 venv 的 site-packages、用户 site、工作区 PYTHONPATH 或 site 启动钩子。标准库仍由目标解释器提供。原生扩展仍然属于受信任的本机代码。

文件在支持的系统上按只读模式落盘，每次复用重新校验；校验失败返回 `ENVIRONMENT_TAMPERED`，不会自动修复活跃环境。环境构建由 Worker 生命周期持有；取消构建或同时停止 Worker 时，宿主等待真实构建线程退出后才完成清理，不会因为取消了 `to_thread` 的等待器就遗留后台构建。

内建 OneBot 从 `0.2.0` 起把纯 codec 作为包内 helper 一起发布。格式化/规范化 Worker 不再依赖宿主 `httpx`；有凭据的 HTTP transport 留在宿主，复用同一纯 codec。旧 `0.1.0` 工件仍保持不可变。

## 可执行验证

`tests/test_packages.py` 覆盖两个真实进程竞争同一正式版本、开发版本稳定性、catalog 重启、完整导入导出、被拒登记不占版本、路径/链接/重复/展开上限和历史 state-handoff manifest 校验。该历史校验不是数据库迁移要求，后续只能按 00/02 的 Generation handoff 契约保留或直接删除。`tests/test_environments.py` 覆盖闭包、marker/extras、hash/目标不匹配、只读环境篡改、构建取消/停止收尾，并同时启动两个真实 Worker，验证不同依赖版本共存、旧进程不受影响、宿主已安装的 httpx 不会进入插件搜索路径。内建 OneBot 也在实际隔离进程内执行规范化与发送字节准备。

这些测试使用本地构造的标准 wheel 和真实 Python 进程，不需要外部 API 凭据。插件自身逻辑正确性、第三方库在实际目标系统上的运行行为，以及真实 QQ/模型服务联调仍需对应插件和部署验收；静态 wheel 检查不能替代候选启动、健康检查与状态 handoff 门禁。
