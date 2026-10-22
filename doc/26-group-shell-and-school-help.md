# 群聊 Shell 与学校服务器帮助

两个功能分别由 `/module/shell` 和 `/module/npucraft` 独立 Wasm 包实现。它们订阅宿主 OneBot 群消息事件，不调用 Agent、Provider 或 Memory。命令解析、触发匹配、授权用户和回复内容均属于插件。

## 构建与启用

```powershell
cargo run --offline --locked -j 2 -p forest-shell --bin package-shell
cargo run --offline --locked -j 2 -p forest-npucraft --bin package-npucraft
cargo build --offline --locked -j 2 -p forest-cli
```

两个包出现在 `packages/shell`、`packages/npucraft`，也纳入 `scripts/build-agent.ps1`。使用更新后的 `forest` 宿主，在 WebUI 的本地包页面分别加载它们。Shell 所需 `/core/control` 端口由 CLI 安装，独立于 WebUI 是否启用；自定义宿主需显式安装同一适配器。

## 学校 MC 帮助

`npucraft` 精确匹配去除首尾空白后的“地图”“卫星地图”和小写 `ciallo`，返回初号机现有的主服、工业服、资源服地图及数据面板链接。附带其他文字、@ 或媒体的消息不匹配。自身账号消息不触发。

`/module/npucraft` 的 `/settings` 包含：

| 字段 | 含义 |
|---|---|
| `allowed_groups` | 允许群；空集合沿用初号机的所有群行为 |
| `triggers` | 精确匹配的完整消息文本 |
| `reply` | 回复原文 |

默认回复保存在插件自己的 `src/reply.txt`。配置由 create 读取，保存修改后重载此包生效。回复作为 OneBot 文本段发送，文本里的 CQ 样式字符串不会转为 @、图片等操作。

## Shell 授权

在 WebUI 中打开 `/module/shell` 的 `/settings`，设置 `administrators` 和 `allowed_groups`。两者初始均为空；必须同时匹配发送者和所在群才能管理。它们分别是账号整数数组和群整数数组。以下仅为虚构配置示例：

```text
{
  administrators: [10001i64],
  allowed_groups: [20001i64]
}
```

每条命令读取一次当前授权，因此更改授权不需要重载 Shell。配置只能由 `/user/admin` 写入，不能由群成员给自己授权。身份来自 OneBot `user_id`，还要与宿主注入的 `/user/onebot/<账号>` origin 一致；昵称、群名片和 QQ 群角色不授予权限。匿名及自身账号消息不执行命令。

未允许群不响应。允许群中的未授权用户收到拒绝回复。接收入口的静态 callers 仅允许 `/core/onebot`，其他模块不能伪造一个发送者账号直接调用它。

## 命令

直接发送纯文本命令，无需 @。第一版仅解析纯文本消息，命令前不附加 @、引用或其他 CQ 段。

```text
#forest help
#forest status
#forest packages
#forest module list
#forest module show /module/agent
#forest module load /module/npucraft
#forest module reload /module/npucraft
#forest module unload /module/npucraft
#forest module force /module/npucraft
#forest config keys /module/context
#forest config get /module/context /prompt/agent
#forest config set /module/context /prompt/agent "You are Alice."
```

`load` 按包的对象 Token 从宿主配置的包目录定位，并通过现有静态依赖计划加载依赖。目录中不合法的包在 `packages` 结果里显示错误，重复对象声明不能任取其中一个。

`reload`、`unload` 的最后一个参数可为 `drain`、`yield`、`force`，默认 `drain`。分别沿用排空、主动让出处中断和强制中断三种生命周期语义，不自动超时升级。独立 `module force` 命令用于显式强制中断正在创建或已经处于停止过程的模块；它报告中断请求已提交，原生命周期命令仍负责报告最终结果。

Shell 自身的重载、卸载和强制中断通过 WebUI 执行。同步群命令不能等待正在运行该命令的 Shell 自身退出。其余模块按现有依赖、接口兼容性与配置 Schema 规则管理。

配置值使用 Forest 文本语法，字符串要有引号，整数和复杂记录保持原类型。保存后是否需重载由目标模块的配置语义决定；例如人格和 MC 回复需要重载，而 Shell 授权即时生效。

命令通常只有一条结果回复，单条结果展示最多 3500 个字符，其余在 WebUI 查看，不自动拆成大量群消息。

## 宿主边界

`crates/forest-control` 是宿主管理适配器，复用 `Inspector`、`ModuleManager`、绑定管理员来源的 `ConfigWriter`，并向指定 callers 授予 `/core/control` 的管理能力。CLI 仅授予 `/module/shell`。接口为：

| 接口 | 输入 | 成功值 |
|---|---|---|
| `/snapshot` | `()` | 类型化运行状态与模块摘要 |
| `/packages` | `()` | Ready/Invalid 包清单 |
| `/load` | 对象路径 | `()` |
| `/reload`、`/unload` | 对象路径、StopMode | `()` |
| `/force` | 对象路径 | `()` |
| `/config/keys` | 对象路径 | 配置键清单 |
| `/config/get` | 对象路径、键路径 | Schema、默认值、当前值的 Forest 文本 |
| `/config/set` | 对象路径、键路径、Forest 值文本 | `()` |

地址在线上 Schema 中为字符串，在宿主边界解析为 Token。所有接口返回 `Result<T, Error>`，调度失败仍走外层 DispatchResult，不使用动态字典反射或字符串操作码分派。Shell 只持有这些 Schema 的本地映射，不链接宿主管理 crate；完整使用声明在 Shell Manifest 的 uses 中，等价的 Schema 语言文件输出到 `packages/shell/control/`，其他语言无需链接 Rust 类型即可使用。

配置读取使用受信管理员调试台的显式 Inspector 权限；配置写入与 WebUI 共用绑定 `/user/admin` 的 set 路径，检查目标静态 ACL 和 Schema。没有跳过检查直接写库，模块仅授权 SelfModule 的内部状态仍不能由管理员 set。

WebUI 与 Shell 共用同一份本地包目录逻辑。`ModuleManager` 与 Inspector、ConfigWriter 一样持有弱宿主引用，原生管理端口不会通过闭包形成自持有的 Scheduler 环。

Shell 接收接口可重入，等待另一个模块停止不会阻塞后续状态查询。已开始的生命周期操作由宿主持有，等待者断开或 Shell 被外部重载不会取消已开始的操作。管理日志记录 origin、caller、操作和结果，不记录配置写入正文。

Shell、学校回复与 Agent 是独立调度目标，不经过模型调用或 Agent 的群队列。正常纯文本命令不包含 @，在现有 Agent 中作为普通群聊接纳，不成为持续点。框架未引入事件抢占或由模块消费并截断其他订阅者的机制。

## 验证

```powershell
cargo test --offline --locked -j 2 -p forest-shell --test wasm
cargo test --offline --locked -j 2 -p forest-webui --test management
```

隔离 Wasm 集成使用本地 OneBot 替身验证闭合授权、来源与 callers 检查、配置 ACL、实际装卸和重载、授权即时更改、学校帮助触发、CQ 文本转义与配置持久化。不会向真实群发送消息，也不调用模型。
