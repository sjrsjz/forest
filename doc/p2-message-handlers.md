# P2 普通群消息插件链详细设计

状态：协议冻结，SDK/包声明/Worker/Kernel 类型化单项调用切片已完成；持久路由、Conversation Actor 执行与 Web 策略尚未接通。

## 1. 参考实现与目标

AliceBot Plus 的 `Bot.receive_group_message` 会按插件 `priority` 排序，依次调用 `on_group_message`；插件抛出 `Skip` 时继续，抛出 `SkipFollow` 时停止后续插件和默认回复。这个接口简单，适合普通 Python 插件开发，但排序由当前内存插件集合即时产生，异常仅记录日志，插件直接持有 WebSocket 并可立即发送，因此无法提供崩溃恢复、热重载代际一致性、同群唯一回复链和原子 outbox。

Forest 保留三个易用语义：异步 handler、显式优先级、继续或终止链。执行方式改为宿主管理的持久消息链：插件只返回不可变结果，不持有 Adapter；链的路由快照、执行进度和结果均可恢复；所有 handler 与 Agent 共用本群 Conversation Actor。

普通消息 handler 不是 Agent，也不会创建 Agent。它没有独立 Prompt、Context 或模型循环。组件若需要模型能力，只能调用显式获准的服务，并仍受本次 handler deadline、预算与来源约束。

## 2. 声明和结果

Python 插件使用：

```python
from forest.sdk import Consumed, Pass, Reply, message_handler, plugin

@plugin("greeter", "1.0.0")
class Greeter:
    @message_handler("greeting", priority=20)
    async def greet(self, event, ctx):
        if event["text"] != "你好":
            return Pass()
        return Reply([[{"type": "text", "text": "你好"}]])
```

生成的 `module.toml` 含有：

```toml
[[components.message_handlers]]
handler_id = "greeting"
handler = "greet"
priority = 20
```

`handler_id` 是组件内稳定身份，必须是 1–128 字符的受限标识；`handler` 是入口方法名；`priority` 范围为 -100 到 100，数值越大越早。不同组件同优先级时按 `(component_id, handler_id)` 升序确定顺序，不能依赖安装、扫描或启动时间。

handler 必须是 `async method(event, ctx)`，只返回：

- `Pass()`：当前 handler 不处理，执行冻结链中的下一项；
- `Consumed()`：停止链且不产生回复，也不触发 Agent；
- `Reply(parts)`：停止链，宿主校验并原子提交 CHAT 回复；不再触发 Agent。

异常不是 `Pass`。可重试故障保留当前链位置并有界退避；达到 deadline 或永久失败时终结该 handler 链，产生固定文案的安全通知，不回显异常正文。管理员可以在 WebUI 观察和重试。首版不允许一个消息产生多个普通 handler 回复。

## 3. 接入顺序与 Agent 回落

固定分类顺序为：

```text
可信连接与有界平台解析
  -> ShellGate 保留前缀
  -> Inbox、作者和原始载荷接纳
  -> 确定性业务命令匹配和策略
  -> 冻结普通消息 handler 链
  -> 冻结 Agent trigger fallback
```

Shell 命中后不进入普通链。已启用且命中的业务命令拥有该消息，即使命令被拒绝、失败或返回 `Pass` 也不进入普通链。其余合法群消息都可以冻结一条普通链，包括最终不会触发 Agent 的消息。

整条链都返回 `Pass` 后，宿主采用接纳时冻结的 Agent trigger decision。若结果是触发，则进入既有 trigger 队列；若结果为 `NO_TRIGGER`，消息只进入 Context 并静默排空。这样插件热重载或策略变化不会重新解释旧消息，也不会通过延迟分类改变当时的触发规则。

无论结果为何，原消息及可信作者都按 ingress 顺序进入 Context。`Consumed` 只控制响应所有权，不能删除历史或把作者改成 Bot。

## 4. 持久链与并发

接纳事务保存不可变 `MessageHandlerChain`：消息/载荷引用、Binding、作者 principal、registry revision、fallback trigger、deadline，以及按确定顺序排列的每个 `HandlerRoute`。每项固定 component、generation、route epoch、handler identity 和 spec digest。链 head 保存当前索引、状态、attempt 和 next-at；每次 attempt 另写不可变记录。

Conversation Actor 只在没有活跃 Agent quantum、没有更早业务命令或安全通知时领取最早 ingress 的链。一个 Actor quantum 最多执行一个 handler 调用；返回 `Pass` 后提交进度并重新参加公平调度，不在一个 Python 调用栈中扫完整链。不同群可并行，同群的普通 handler、业务命令和 Agent 不并发生成回复。

活跃 Agent 期间新消息仍接纳并冻结链，但普通 handler 不插入当前 ModelTurn 或工具因果组。当前 quantum 闭合后 Actor 先按 ingress 完成尚未分类的普通链；只有链最终回落为 trigger 后，该消息才可加入下一次模型调用的“截至最后一个触发记录”切口。链未终结的 ingress 是连续分类水位的屏障，后续消息不能越过它进入 Agent。

## 5. 热重载和效果

发布新代时先停止向旧路由冻结新链项，再等待已调用项闭合或持久终结。尚未开始的旧项不偷偷改投新代码；只有 manifest 明确保持相同 `handler_id` 与兼容 spec、且管理计划允许接管时，才写可审计的 reroute 事实。否则旧项安全失败。旧 Worker 返回后，最终提交必须重验 Actor lease、chain revision、generation exposure、来源可读性和当前输出授权。

Worker 不能直接发送 OneBot action。`Reply` 经宿主在一个事务内写 handler result、链终态、Context 单元、reply sequence、payload lineage 和 SendIntent。事务前崩溃可按稳定 operation ID 重试；事务后丢 ACK 读取既有结果；平台发送不确定继续使用 Delivery 的 `UNKNOWN` 对账。

`ctx` 的 state/service/event/job 能力仍取组件声明和部署 grant 的交集。MessageEvent 中的 Bot、Binding、conversation、作者和 payload 来源全部由宿主构造。插件正文不能授予权限，handler 也不能调用 Shell 内部管理接口。

## 6. Web 管理和容量

WebUI 按 Binding 显示并配置普通 handler 的 `DISABLED / EVERYONE / ADMINS / USER_SET` 策略；默认 `DISABLED`。策略和注册表均使用 preview/apply、expected revision、重新鉴权与来源 payload。接纳时冻结允许执行的路由及策略 revision，撤权可取消尚未开始的项，最终回复前仍重验输出授权。

容量至少包含每 Bot/Binding/主体待处理链上限、每链最大 handler 数、单 event 字节上限、主体令牌桶、handler deadline 和全局 P2 槽。拒绝不得回落 Agent 以绕过治理；原消息仍进入 Context，并由本群 Actor 生成固定的安全通知。

## 7. 实施切片

1. **已完成：** SDK 装饰器、静态 manifest、包校验和 Worker 全字段对照；
2. **已完成：** Kernel 载入定义及类型化单 handler 调用；
3. PostgreSQL registry/policy、冻结链、attempt/result 与代际接管；
4. OneBot 接纳分类和 Conversation Actor 单步执行；
5. Context/Agent fallback、Reply/Delivery 原子提交和故障恢复；
6. WebUI 策略、诊断、容量治理，以及真实 PostgreSQL/浏览器/低频 OneBot 验收。

验收必须覆盖 priority 稳定顺序、Pass/Consumed/Reply、异常重试、重启续跑、热重载竞争、同群串行、跨群并行、忙时 Agent 安全边界、多用户作者保持、策略撤权、遗忘与 Delivery UNKNOWN。模拟测试不能替代允许群的低频真实外部验收。
