# 快速开始

[返回目录](README.md)

目标：构建 `/module/example-hello`，在群内收到 `#forest-hello` 后回复 `hello world`，再通过 WebUI 修改回复内容并热重载。

## 1. 准备工具链

从仓库根目录执行，工具链要求与[项目构建说明](../../README.md#构建)相同：

```sh
rustup target add wasm32-unknown-unknown
cargo fetch --locked
cargo fetch --locked --manifest-path doc/module-development/examples/hello/Cargo.toml
```

示例使用 `wasm32-unknown-unknown`，宿主提供显式 I/O。SDK 没有要求模块启动 Tokio、依赖 WASI 或直接打开操作系统文件。

示例的 Cargo 工程是独立 workspace，不需要修改框架根目录的 members。其路径依赖指向当前仓库中的 SDK；复制到其他位置时需调整这些路径。

## 2. 构建插件包

```sh
cargo run --locked -j 2 --manifest-path doc/module-development/examples/hello/Cargo.toml --target-dir target --bin package-hello
```

产物位于 `packages/example-hello/`：

```text
manifest.toml
module.wasm
generated/
  lifecycle-error.schema
  configs/...
  provides/...
  uses/...
```

打包程序也接受一个输出目录参数，例如：

```sh
cargo run --locked -j 2 --manifest-path doc/module-development/examples/hello/Cargo.toml --target-dir target --bin package-hello -- data/example-hello-package
```

Wasm 构建本身不等于形成完整包。实际装载还需要 Manifest、引用文件和匹配的 SHA-256；`forest-package::write` 统一生成这些内容。

## 3. 认识示例代码

[src/lib.rs](examples/hello/src/lib.rs) 的各部分对应以下职责：

| 部分 | 作用 |
| --- | --- |
| `Greeting` | `/greet` 的类型化输入，包含 `name: String` |
| `Error` | 保留调度异常和 OneBot 操作错误，避免压成一个 String |
| `Hello::create` | 读取 `/settings/reply`，建立当前实例 |
| `handlers` | 注册路径、调用许可及实际处理函数 |
| `on_message` | 识别命令，通过宿主 OneBot 接口发送文本 |
| `greet` | 与群事件共用同一模块对象，返回格式化问候 |
| `manifest` | 声明身份、配置、依赖和事件接收关系 |
| `export_module!` | 提供宿主要求的固定 Wasm 导出 |

`LifecycleError` 使用 SDK 的 `HostError`，配置读取中的两层错误因此可以用 `.await??` 保留类型。普通接口也返回完整 `Result`；`/greet` 的模块逻辑没有错误分支，使用 `Infallible`，外层调度仍然可能失败。

`/group_message` 在回复与计数持久化期间显式持有 `/replies` 宿主锁；后续事件可以进入处理器，但须等锁释放才进入同一临界区。`/greet` 不操作这份状态，无需获取该锁。同一实例仍不会同时执行两段来宾指令。

群事件来自 Manifest 的 subscriptions。向 OneBot 发送消息则是另一条显式 call，必须另行声明 uses。接收消息不会自动赋予发送权限。

## 4. 启动测试宿主

准备支持 OneBot v11 正向 WebSocket 的网关。以下群号 `123456789` 需换成测试群；环境变量需按网关设置填写。

PowerShell：

```powershell
$env:FOREST_WEB_PASSWORD = 'replace-with-a-console-password'
$env:FOREST_ONEBOT_TOKEN = 'replace-with-the-gateway-token'
```

POSIX shell：

```sh
export FOREST_WEB_PASSWORD='replace-with-a-console-password'
export FOREST_ONEBOT_TOKEN='replace-with-the-gateway-token'
```

网关未启用令牌认证时，不设置 `FOREST_ONEBOT_TOKEN`。CLI 不自动读取 `.env` 文件。

```sh
cargo build --locked -j 2 -p forest-cli
pnpm --dir webui install --frozen-lockfile
pnpm --dir webui build
cargo run --locked -p forest-cli -- --package packages/example-hello --packages-directory packages --database data/example-hello.sqlite3 --data-directory data/example-hello-modules --events-ws ws://127.0.0.1:8080 --allow-group 123456789 --webui 127.0.0.1:8081
```

这是启动独立测试宿主的命令。已有运行中的 Forest 时，可以直接在其 WebUI 的“本地包”中加载示例，复用已有连接和配置库；无需再启动一个宿主。

模块进入 `Serving` 后，在测试群发送 `#forest-hello`。示例只识别这一条命令，发送内容使用原生 OneBot 文本段。

## 5. 修改配置并重载

打开 `http://127.0.0.1:8081`，使用管理口令登录：

1. 选择 `/module/example-hello`，进入配置表。
2. 编辑 `/settings/reply`，例如改为 `Hello from a reloaded module`。
3. 保存，然后选择模块的重载操作，使用 Drain。
4. 再次发送命令，回复应使用新内容。

该示例在 create 中读取配置，保存配置不会自动修改已有 `Hello.reply`。重载后新实例读取当前值；其他模块也可以选择在每次处理时读取配置，具体策略属于模块实现。

## 6. 扩展为自己的模块

替换包 ID、模块名、处理器和命令，保留以下对应关系：

- Manifest 的 provides 与实际 handlers 对应。
- 每个外部 call/send 都有匹配的 uses，目标接口也授予 callers。
- subscriptions 指向本模块已声明的接收接口。
- 配置项声明包含 Schema、默认值和读写规则。
- 公共参数与错误类型可以从生成的 Schema 独立理解。

继续阅读[类型契约](02-contracts.md)与[生命周期和并发](03-lifecycle-and-scheduling.md)，再添加持久状态或长任务。
