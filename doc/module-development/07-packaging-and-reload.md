# 打包与热重载

[返回目录](README.md)

## 1. 包格式

当前包是一个目录，以 `manifest.toml` 为入口。Schema 与默认值使用 Forest 文本文件；全部引用都基于包根，而非宿主当前工作目录。

```toml
[package]
id = "/module/example-hello"
version = "0.1.0"
name = "Hello module example"
authors = ["Example Author"]
repository = "https://example.org/source"
license = "MIT"

[module]
file = "module.wasm"
lifecycle-error = "generated/lifecycle-error.schema"

[[configs]]
key = "/settings/reply"
schema = "generated/configs/0.schema"
default = "generated/configs/0.value"

[configs.access]
readers = ["self"]
writers = ["/user/admin"]

[sha256]
# 打包器在此写入全部有效负载文件的实际摘要。
```

以上为配置声明片段，完整可运行示例还包含 provides、uses 和 subscriptions。允许的字段及完整语法见[Manifest 格式](../07-module-manifest.md)。

package.id 是单例对象身份。version 是发布标记，不作为接口兼容性判断或依赖版本选择依据。作者、仓库、许可证、README、主页、文档和关键词是可选元信息；元信息不会自动授予可信身份。

文件引用必须是包内相对路径，不能使用绝对路径、反斜杠、盘符、空段或 `.`、`..`。文件路径与对象/接口 Token 分别解析；接口路径中合法的符号不必成为磁盘文件名。

## 2. 统一打包工具

Rust 包工具先编译并放置 Wasm，再调用：

```rust
forest_package::write(&output_directory, &manifest)?;
```

它从类型化声明生成 `generated/` 下的 Schema/Value，校验默认值和契约，收集其他静态资源的 SHA-256，最后替换 TOML。`generated/` 由打包器管理并会重建；手写资源应放在其他目录。生成文件的序号是文件名，不是接口身份。

`forest-package` 属于宿主侧工具依赖。示例将它放在 `cfg(not(target_family = "wasm"))` 的依赖中；模块代码不需要把 TOML 解析器或宿主装载器链接进 Wasm。

其他语言可以自行生成相同文件，不需要调用 Rust 工具。摘要按原始文件字节计算；引用文件须出现在 sha256 表中，Manifest 本身不自包含摘要。

## 3. 装载检查

宿主读取 TOML 后，先校验声明文件摘要，再解析 Schema/Value，形成完整 Manifest。实际加载时校验所有索引文件，并把已验证的同一份 Wasm 字节交给编译器。

随后检查固定导出、私有 wasm32 内存、依赖、接口、ACL 与配置。管理端浏览包目录只读取声明，不预先编译所有 Wasm；因此“目录中可见”不是“已经创建成功”。

依赖按 uses 和宿主事件声明分析；加载根包时依赖先初始化。当前不支持跨模块的静态依赖环，自身接口引用按[通信章节](04-communication-and-permissions.md#3-uses-与-callers-都要满足)处理。某个 create 失败不表示已经加载的其他依赖被一并回滚。

## 4. 兼容性方向

候选保持同一对象 ID，保留已经发布的接口，并满足：

```text
旧输入 <: 新输入
新输出 <: 旧输出
```

实现可以接受更多输入，返回的可能结果必须仍能被旧调用方接收。完整 Result 的错误分支同样参与检查。扩大一个错误 enum 的分支，可能使新输出不再是旧输出的子类型。

配置键集合必须保持一致，各项 Schema 必须结构等价。默认值可以更新，全部当前值保留。增删配置键或改换其类型目前会拒绝兼容重载，应在设计阶段确定如何重新装载或调整数据，而不是伪装成同一份兼容契约。

生命周期错误 Schema 属于具体实例，可以随候选改变。错误记录携带所属实例的 Schema，不能用新实例的类型重新解释旧错误。

不兼容候选在停止旧实例之前被拒绝；选择 Force 也不会绕过兼容性检查。

## 5. 三种停止方式

| 模式 | 已开始和排队的旧工作 | 来宾 destroy |
| --- | --- | --- |
| Drain | 完成停止边界前已接纳的旧集合 | 执行 |
| AtYield | 等当前推进交回，再中断其他旧工作 | 执行 |
| Force | 中断推进和旧工作 | 跳过 |

替换期间的新接纳请求进入交接集合，仍受额度约束。候选正常 Serving 后再执行；初始化失败会得到调度失败，不把候选的生命周期错误伪装成普通接口返回值。

AtYield 等待的是主动推进边界，不是一次时间片暂停。永不主动挂起的同步循环，不能依靠 AtYield 安全退出。Force 明确放弃来宾清理；三种方式均需等已提交的本地文件/数据库操作结算后才能创建新实例。

已接纳的下游调用不递归回滚，远端副作用也不回滚。由此需要恢复、幂等或去重的接口，应在其自身协议中明确处理。

## 6. 操作方式

重新打包后在 WebUI 选择候选目录和停止方式。配置修改是否需要重载由模块读取策略决定。当前没有自动文件监视，不会因为覆盖磁盘上的 Wasm 就自动替换运行实例。

运行中实例的 Wasm SHA-256 可以用于确认实际代码；候选目录的文件可能已经更新，两者不是同一份状态。失败后先核对错误发生于包读取、兼容检查、create 还是普通调用，再选择管理操作。

仍有静态使用者的模块不能直接卸载；需要先卸载使用者。Failed 对象可以通过外部 load 恢复或 unload 移除。管理员也可显式 Force 一个正在进行的生命周期操作，管理入口及认证见[WebUI 文档](../14-inspection-webui.md)。

## 7. 部署产物

Wasm 和前端静态文件可以跨支持的平台复用，宿主本机程序按目标平台构建。Wasm 编译缓存需要在目标平台生成，可用 `prepare_packages` 预热，不需要创建模块或调用外部模型。

发布包与配置库、模块私有数据库和受管文件分别存放。更新包不应覆盖运行数据；备份和服务配置示例见[Linux ARM64 部署](../27-linux-deployment.md)。
