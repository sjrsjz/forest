# OneBot CQ 呈现与受管图片发送

Agent 的 assistant 正文允许使用当前 Adapter 声明的平台呈现语法。人格配置不承担实际账号、当前群、媒体引用或平台协议的权威；接收账号继续由 [21](21-native-assistant-and-thinking.md) 的 Context 消息头提供。首版支持文本、at、reply、face 和受管 image。

## 所有权与数据流

Agent 最终回复发布为 `parts=[{type:text,format:platform-markup,text:...}]`。Shell、控制回执及其他未声明格式的 text 保持字面语义。通用 Delivery 只传递 parts 和准备结果引用，不解析 CQ 或图片，不携带图片字节。

OneBot Codec 是纯函数模块：把显式 platform-markup 解析一次，转换为有序消息段模板。普通 text 不解析 CQ。文字解码 `&amp;`、`&#91;`、`&#93;`；参数另解码 `&#44;`。先识别原始 CQ 边界与参数分隔，再做单次解码，转义出的 `[CQ:...]` 永不再次执行；最终通过 OneBot 数组消息发送，字段不再做 CQ 转义。未知、重复参数、缺字段的 CQ 候选不是可执行片段，保留为字面文字；已识别的受管图片引用无法解析时整次发送准备失败，不发送部分消息。

图片语法为 `[CQ:image,file=<media_ref>,revision=<media_revision>,source_unit_id=<source_unit_id>,part_id=<part_id>]`。模型输入中的图片标签明确提供这四项，未向文本模型投影像素时也可提供引用，但必须标记没有看过图片。OneBot Adapter 通过 Media 的 `media.context.resolve` 读取相应受管资产的准确 Payload 引用；任意 URL、本地路径和模型生成的 base64 均不作为图片来源。

Codec 生成的模板中图片仍是引用。Adapter 的代际受限宿主 effect `adapter.onebot.wire.materialize` 对准确的 Media Payload 逐项验证与读取，替换为 OneBot base64 图片参数，校验最终平台消息并把规范字节写入 `kernel.effect/effect.wire`，以图片 Payload 为父依赖。只返回 action、wire Payload 引用和 digest。它不发送消息，也不更改 Delivery 状态。

Delivery 以该 wire 引用走原有授权、准备与 DISPATCHING 账本。OneBot 发送 effect 在最终交接时重新读取并校验准确 wire 引用、摘要、目标账号、群与 RuntimeGraph fence，然后交给平台。Worker IPC 只有元数据；单张图片上限沿用受管媒体的 8 MiB，整条规范发送字节上限 12 MiB。不拆成高频消息，不提高 Worker 帧上限，不设置不确定发送的自动重试。

准备端明确返回 READY 或 REJECTED。图片不存在、被撤回、超过大小上限或平台消息无法呈现时，Delivery 在持有当前 claim/epoch 的事务中记为 FAILED；不创建传输 attempt，也不重新排队。基础设施故障继续沿原有重试路径。发送交接时若准确 wire 已因来源删除而失效，驱动明确返回尚未发送的 REJECTED，不能当成一次未知网络发送。

Adapter prepare/dispatch 和底层 send 升级至 3.0 契约，Codec prepare-send 升级至 2.0，删除旧完整 wire_body_b64 的发送 DTO。开发期以完整匹配的包图启动新实例，不保留双协议分支或数据库迁移。宿主仍只承载验证、Payload 字节物化和 I/O；CQ 解释、消息语义、媒体资产管理、发送调度分别属于 Codec、Adapter、Media、Delivery。

## 依据

AliceBot-Plus 的 `alicebot/entity/message/codec.py` 提供了拆分 CQ 的现有做法，但没有单次实体还原；`plugin/chat.py` 已显式提示自身 QQ 号。实现采用 OneBot 官方[字符串转义](https://github.com/botuniverse/onebot-11/blob/master/message/string.md)、[数组消息](https://github.com/botuniverse/onebot-11/blob/master/message/array.md)和[消息段](https://github.com/botuniverse/onebot-11/blob/master/message/segment.md)契约。
