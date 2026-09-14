"""Independent typed group control; never invokes the chat Agent for commands."""

from __future__ import annotations

import asyncio
from collections.abc import Callable
from uuid import uuid4

from forest.errors import ForestError
from forest.shell_authority import is_shell_admin
from forest.shell_registry import CONTROL_DEADLINE
from forest.shell_registry_v12 import get_command_spec, render_help
from forest.storage.bindings import BindingStore
from forest.storage.bot import BotStore, HostLease
from forest.storage.delivery import DeliveryStore
from forest.storage.messages import MessageStore


class ShellDispatcher:
    def __init__(
        self,
        messages: MessageStore,
        bindings: BindingStore,
        delivery: DeliveryStore,
        lease: HostLease | Callable[[], HostLease],
        *,
        runtime: BotStore | None = None,
        max_concurrency: int = 4,
        poll_interval: float = 0.2,
        command_timeout: float = CONTROL_DEADLINE.default_seconds,
        scan_page_size: int = 64,
        on_change: Callable[[], None] | None = None,
        module_packages=None,
    ):
        if not (type(max_concurrency) is int and 1 <= max_concurrency <= 16
                and 0.01 <= poll_interval <= 60
                and CONTROL_DEADLINE.minimum_seconds <= command_timeout <= CONTROL_DEADLINE.maximum_seconds
                and type(scan_page_size) is int and 1 <= scan_page_size <= 256):
            raise ValueError("Invalid Shell resource bounds")
        self.messages, self.bindings, self.delivery = messages, bindings, delivery
        self.runtime = runtime or messages.store
        self._lease_source = lease
        self.max_concurrency, self.poll_interval = max_concurrency, poll_interval
        self.command_timeout, self.scan_page_size = command_timeout, scan_page_size
        self.on_change = on_change
        self.module_packages = {} if module_packages is None else module_packages
        self.owner_id = "shell:" + str(uuid4())
        self.task: asyncio.Task | None = None
        self._jobs: dict[str, asyncio.Task] = {}
        self._priorities: dict[str, int] = {}
        self._cursor: str | None = None
        self._closed = False
        self._close_task: asyncio.Task | None = None
        self._wake = asyncio.Event()
        self.errors: dict[str, str] = {}

    def _lease(self) -> HostLease:
        return self._lease_source() if callable(self._lease_source) else self._lease_source

    @property
    def failure(self) -> ForestError | None:
        if not self._closed and self.task is not None and self.task.done():
            return ForestError("SHELL_STOPPED", "Control dispatcher stopped", 503)
        return None

    def _error(self, key: str, error: BaseException) -> None:
        if key not in self.errors and len(self.errors) >= 256:
            self.errors.pop(next(iter(self.errors)))
        self.errors[key] = error.code if isinstance(error, ForestError) else "CONTROL_UNAVAILABLE"

    async def start(self) -> None:
        if self._closed:
            raise ForestError("SHELL_CLOSED", "Control dispatcher is closed", 503)
        if self.task is None or self.task.done():
            self.task = asyncio.create_task(self.run_forever(), name=self.owner_id)

    def notify(self) -> None:
        self._wake.set()

    async def run_forever(self) -> None:
        try:
            while not self._closed:
                self._wake.clear()
                try:
                    pending = await self.messages.pending_controls(
                        self._lease(), limit=self.scan_page_size, after_command_id=self._cursor,
                    )
                    self._cursor = max((item["command_id"] for item in pending), default=None)
                    for item in sorted(pending, key=lambda row: (row.get("priority", 1), row["command_id"])):
                        key = item["command_id"]
                        if key in self._jobs or len(self._jobs) >= self.max_concurrency:
                            continue
                        priority = item.get("priority", 1)
                        ordinary = sum(value > 0 for value in self._priorities.values())
                        if (priority > 0 and self.max_concurrency > 1
                                and ordinary >= self.max_concurrency - 1):
                            continue
                        job = asyncio.create_task(self._process(key), name="control:" + key)
                        self._jobs[key] = job
                        self._priorities[key] = priority
                        job.add_done_callback(lambda task, key=key: self._completed(key, task))
                except asyncio.CancelledError:
                    raise
                except Exception as exc:
                    self._error("dispatcher", exc)
                try:
                    await asyncio.wait_for(self._wake.wait(), timeout=self.poll_interval)
                except TimeoutError:
                    pass
        except asyncio.CancelledError:
            pass

    def _completed(self, command_id: str, task: asyncio.Task) -> None:
        if self._jobs.get(command_id) is task:
            self._jobs.pop(command_id)
            self._priorities.pop(command_id, None)
        if not task.cancelled():
            error = task.exception()
            if error:
                self._error(command_id, error)
        self._wake.set()

    async def _process(self, command_id: str) -> None:
        try:
            async with asyncio.timeout(self.command_timeout):
                lease = self._lease()
                claim = await self.messages.claim_control(
                    lease, command_id, self.owner_id, ttl_seconds=self.command_timeout + 1,
                )
                if claim["status"] in {"COMPLETED", "CANCELLED"}:
                    return
                spec = get_command_spec(claim["command_name"], claim["gate_version"])
                builtin_method = spec.execution_method if spec and spec.execution_target == "builtin" else None
                # Current effective Binding configuration supplies the only P1
                # administrator allowlist. Platform role/nickname never grants it.
                async with self.runtime.pool.connection() as conn, conn.transaction():
                    binding = await self.bindings._guard(
                        conn, lease, claim["binding_id"], claim["binding_epoch"], data=False,
                    )
                    author = claim["source"]["author"]
                    settings = await self.bindings.current_config_in(
                        conn, self.runtime.bot_id, claim["binding_id"],
                        expected_binding_epoch=claim["binding_epoch"],
                    )
                    authorized = is_shell_admin(author, settings["config"])
                    runtime_state = binding["_runtime"]
                    paused = not runtime_state["data_admission"]
                    if authorized and builtin_method == "status":
                        count = await (await conn.execute(
                            "SELECT COALESCE(sum(next_ingress_seq-1-context_applied_seq),0) AS pending "
                            "FROM public.bot_conversations WHERE bot_id=%s", (self.runtime.bot_id,),
                        )).fetchone()
                    else:
                        count = None
                    await self.bindings._final_guard(conn, lease, binding, data=False, require_config=True)
                if not authorized:
                    text, outcome = "当前账号没有 Forest 管理权限。", {"code": "DENIED"}
                elif builtin_method == "help":
                    text = render_help(claim["gate_version"])
                    outcome = {"code": "HELP"}
                elif builtin_method == "status":
                    text = (
                        f"Forest 状态：{'业务已暂停' if paused else '运行中'}；"
                        f"维护版本：{runtime_state['maintenance_epoch']}；"
                        f"待处理会话消息：{count['pending']}。"
                    )
                    outcome = {"code": "STATUS", "paused": paused,
                               "maintenance_epoch": runtime_state["maintenance_epoch"],
                               "pending_messages": int(count["pending"])}
                elif spec is not None and spec.action is not None:
                    try:
                        text, outcome = await self._typed(lease, claim)
                    except ForestError as exc:
                        text = f"管理操作未接纳：{exc.code}。请刷新本群状态后核对目标与版本。"
                        outcome = {"code": exc.code}
                else:
                    text = "这条管理命令尚不受支持或参数无效。可使用 #forest help 查看。"
                    outcome = {"code": "UNSUPPORTED"}
                await self.messages.complete_control(
                    lease, claim, "shell-reply:" + command_id,
                    reply_parts=[[{"type": "text", "text": text}]], outcome=outcome, delivery=self.delivery,
                )
                self.errors.pop(command_id, None)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            self._error(command_id, exc)

    async def _typed(self, lease, claim):
        from forest.storage.agent import AgentStore
        from forest.storage.shell_control import ShellControl

        control = ShellControl(self.messages, lease, claim)
        admission, operation = control.admission, "shell-action:" + claim["command_id"]
        payload, name = claim["arguments"], claim["command_name"]
        # The immutable key and typed arguments came from durable admission.
        # Do not parse the source text or validate it under a newer schema here.
        spec = get_command_spec(name, claim["gate_version"])
        if spec is None or spec.action is None:
            raise ForestError("SHELL_UNSUPPORTED", "Unknown typed command", 422)
        if spec.execution_target == "agent":
            repository = AgentStore(self.messages)
            repository.control = control
        elif spec.execution_target == "job":
            from forest.storage.jobs import JobStore

            repository = JobStore(self.messages, control=control)
        elif spec.execution_target == "memory":
            from forest.storage.memory_administration import MemoryAdministration

            repository = MemoryAdministration(self.messages, control=control)
        elif spec.execution_target == "bot":
            from forest.storage.bot_runtime import BotRuntimeAdministration

            repository = BotRuntimeAdministration(self.messages.store, control=control)
        elif spec.execution_target == "event":
            from forest.storage.events import EventStore

            repository = EventStore(self.messages.store, control=control)
        elif spec.execution_target == "component_job":
            from forest.storage.component_jobs import ComponentJobStore

            repository = ComponentJobStore(self.messages.store, control=control)
        elif spec.execution_target == "audit":
            from forest.storage.bot_audit import BotAuditStore

            repository = BotAuditStore(self.messages.store, control=control)
        elif spec.execution_target == "context":
            from forest.storage.context_administration import ContextAdministration

            repository = ContextAdministration(self.messages, control=control)
        elif spec.execution_target == "module":
            from forest.storage.module_administration import ModuleAdministration

            repository = ModuleAdministration(
                self.messages.store, control=control, packages=self.module_packages
            )
        else:
            raise ForestError("SHELL_UNSUPPORTED", "Unknown typed command", 422)
        kwargs = {"lease": lease} if (
            (spec.execution_target, spec.execution_method) == ("memory", "apply")
            or spec.execution_target == "bot"
        ) else {}
        result = await getattr(repository, spec.execution_method)(
            admission, operation, payload, **kwargs
        )
        if control.is_unavailable(result):
            return ("这条管理请求已有记录，回执内容已不可用；没有再次执行。",
                    {"code": "RECORDED_UNAVAILABLE", "operation_id": operation,
                     "operation_status": "RECORDED", "result_payload_state": "UNAVAILABLE"})
        if result is None:
            return ("这条管理请求已有空回执记录；没有再次执行，实际状态请另行查询。",
                    {"code": "RECORDED_NULL", "operation_id": operation, "operation_status": "RECORDED"})
        if name == "run.read":
            lines = [
                f"{row['run_id']}：{row['phase']}；检查点 {row['checkpoint_seq']}"
                for row in result["runs"] if row["conversation_id"] == claim["conversation_id"]
            ][:12]
            approvals = [
                f"待审批 {row['approval_id']}；参数摘要 {row['arguments_digest']}"
                for row in result["approvals"] if row["conversation_id"] == claim["conversation_id"]
                and row["decision"] is None and row["tool_status"] == "WAIT_APPROVAL"
            ][:8]
            text = "本群 Agent：\n" + "\n".join(lines + approvals or ["暂无运行记录。"])
            outcome = {"code": "RUN_STATUS", "run_count": len(lines), "approval_count": len(approvals)}
        elif name == "job.read":
            text, outcome = self._job_status(result, claim["conversation_id"])
        elif name == "event.read":
            text, outcome = self._event_status(result)
        elif name == "event.replay":
            text = (
                f"组件事件投递 {result['delivery_id']} 已进入回放轮次 "
                f"{result['replay_epoch']}；管理修订 {result['management_revision']}。"
            )
            outcome = {
                "code": "EVENT_REPLAYED",
                "delivery_id": result["delivery_id"],
                "replay_epoch": result["replay_epoch"],
                "management_revision": result["management_revision"],
            }
            if self.on_change is not None:
                self.on_change()
        elif name == "event.retire.plan":
            text = (
                f"事件订阅退役预览：待终结 {result['impact']['retire_pending']}，"
                f"执行中 {result['impact']['inflight']}。提交命令：#forest event retire-apply "
                f"{result['plan_id']} --digest {result['plan_digest']} --expect-revision "
                f"{result['expected_revision']}"
            )
            outcome = {
                "code": "EVENT_RETIREMENT_PLAN",
                "plan_id": result["plan_id"],
                "plan_digest": result["plan_digest"],
                "management_revision": result["expected_revision"],
                "retire_pending": result["impact"]["retire_pending"],
                "inflight": result["impact"]["inflight"],
            }
        elif name == "event.retire.apply":
            text = (
                f"事件订阅 {result['subscriber_id']} v{result['contract_major']} 已退役；"
                f"终结投递 {result['retired_delivery_count']}，管理修订 "
                f"{result['management_revision']}。"
            )
            outcome = {
                "code": "EVENT_SUBSCRIPTION_RETIRED",
                "subscriber_id": result["subscriber_id"],
                "contract_major": result["contract_major"],
                "management_revision": result["management_revision"],
                "retired_delivery_count": result["retired_delivery_count"],
            }
            if self.on_change is not None:
                self.on_change()
        elif name == "component_job.read":
            text, outcome = self._component_job_status(result)
        elif name == "component_job.replay":
            text = (
                f"组件持久任务 {result['job_id']} 已进入回放轮次 "
                f"{result['replay_epoch']}；管理修订 {result['management_revision']}。"
            )
            outcome = {
                "code": "COMPONENT_JOB_REPLAYED",
                "job_id": result["job_id"],
                "replay_epoch": result["replay_epoch"],
                "management_revision": result["management_revision"],
            }
            if self.on_change is not None:
                self.on_change()
        elif name == "component_job.retire.plan":
            text = (
                f"组件任务处理器退役预览：待终结 {result['impact']['retire_pending']}，"
                f"执行中 {result['impact']['inflight']}。提交命令："
                f"#forest component-job retire-apply {result['plan_id']} --digest "
                f"{result['plan_digest']} --expect-revision {result['expected_revision']}"
            )
            outcome = {
                "code": "COMPONENT_JOB_RETIREMENT_PLAN",
                "plan_id": result["plan_id"],
                "plan_digest": result["plan_digest"],
                "management_revision": result["expected_revision"],
                "retire_pending": result["impact"]["retire_pending"],
                "inflight": result["impact"]["inflight"],
            }
        elif name == "component_job.retire.apply":
            text = (
                f"组件任务处理器 {result['component_id']} / {result['handler_id']} 已退役；"
                f"终结任务 {result['retired_job_count']}，管理修订 "
                f"{result['management_revision']}。"
            )
            outcome = {
                "code": "COMPONENT_JOB_HANDLER_RETIRED",
                "component_id": result["component_id"],
                "handler_id": result["handler_id"],
                "management_revision": result["management_revision"],
                "retired_job_count": result["retired_job_count"],
            }
            if self.on_change is not None:
                self.on_change()
        elif name == "audit.read":
            text, outcome = self._audit_status(result)
        elif name == "context.read":
            active = (
                f"Run {result['run_id']} / {result['run_phase']} / 检查点 "
                f"{result['checkpoint_seq']}"
                if result["run_id"] else "无活跃 Run"
            )
            recent = [
                f"{row['request_id']}：{row['status']}"
                + (f" → 视图 {row['applied_view_revision']}" if row['applied_view_revision'] is not None else "")
                + (f"（{row['terminal_reason']}）" if row['terminal_reason'] else "")
                for row in result["requests"][:4]
            ]
            text = (
                f"本群上下文：内容修订 {result['context_revision']}；视图修订 "
                f"{result['context_view_revision']}；已接入序号 {result['context_applied_seq']} / "
                f"已分类 {result['classified_through_seq']}。\n"
                f"可见 {result['visible_units']} 单元 / {result['visible_tokens']} 估算 token；"
                f"归档 {result['archived_units']} 单元；预算 {result['token_budget']}。\n"
                f"待触发 {result['pending_triggers']}；已领取触发 {result['claimed_triggers']}；{active}。"
                + ("\n最近裁剪请求：\n" + "\n".join(recent) if recent else "")
            )
            outcome = {
                "code": "CONTEXT_STATUS",
                "context_revision": result["context_revision"],
                "context_view_revision": result["context_view_revision"],
                "visible_units": result["visible_units"],
                "archived_units": result["archived_units"],
                "pending_triggers": result["pending_triggers"],
                "run_phase": result["run_phase"],
            }
        elif name == "context.compact":
            text = (
                f"上下文裁剪请求 {result['request_id']} 已排队；会话 Actor 将在模型调用安全点"
                f"核对视图 {result['expected_view_revision']} 后执行。该操作不会调用摘要模型。"
            )
            outcome = {
                "code": "CONTEXT_COMPACTION_QUEUED",
                "request_id": result["request_id"],
                "status": result["status"],
                "expected_view_revision": result["expected_view_revision"],
            }
            if self.on_change is not None:
                self.on_change()
        elif name == "context.reset":
            text = (
                f"上下文重置请求 {result['request_id']} 已排队；会话 Actor 将在模型调用安全点"
                f"核对视图 {result['expected_view_revision']} 后发布新视图。"
                "当前未闭合因果组会保留；消息不会被删除，也不会调用摘要模型。"
            )
            outcome = {
                "code": "CONTEXT_RESET_QUEUED",
                "request_id": result["request_id"],
                "status": result["status"],
                "expected_view_revision": result["expected_view_revision"],
                "mode": result["mode"],
            }
            if self.on_change is not None:
                self.on_change()
        elif name == "module.read":
            lines = []
            for row in result["modules"][:20]:
                lifecycle = row["lifecycle_phase"] or "NONE"
                if row["lifecycle_error_code"]:
                    lifecycle += "/" + row["lifecycle_error_code"]
                lines.append(
                    f"{row['component_id']}：{row['state']}；desired "
                    f"{row['desired_revision']} / effective {row['effective_revision']}；"
                    f"generation {row['generation_status'] or 'NONE'}；lifecycle {lifecycle}"
                )
            text = "Bot 组件：\n" + "\n".join(lines or ["暂无已配置组件。"])
            outcome = {
                "code": "MODULE_STATUS",
                "count": len(lines),
                "pending": sum(
                    row["state"].startswith("PENDING") for row in result["modules"][:20]
                ),
            }
        elif name == "module.reload":
            text = (
                f"组件 {result['component_id']} 的重载已接纳：复制修订 "
                f"{result['reload_of_revision']} 为 desired {result['desired_revision']}；"
                f"当前 effective 仍为 {result['effective_revision']}。"
                "生命周期协调器发布后才算生效，可用 module status 查询。"
            )
            outcome = {
                "code": "MODULE_RELOAD_ACCEPTED",
                "operation_id": result["operation_id"],
                "component_id": result["component_id"],
                "reload_of_revision": result["reload_of_revision"],
                "desired_revision": result["desired_revision"],
                "effective_revision": result["effective_revision"],
                "status": result["status"],
            }
            if self.on_change is not None:
                self.on_change()
        elif name == "module.validate":
            text = (
                f"组件 {result['component_id']} 验证通过：config "
                f"{result['config_revision']}，state {result['state_revision']}；"
                f"必需依赖 {', '.join(result['required_components']) or '无'}。"
            )
            outcome = {
                "code": "MODULE_VALID",
                "component_id": result["component_id"],
                "package_digest": result["package_digest"],
                "config_revision": result["config_revision"],
                "state_revision": result["state_revision"],
                "required_components": result["required_components"],
            }
        elif name in {"module.enable", "module.disable"}:
            state = "启用" if result["enabled"] else "停用"
            text = (
                f"组件 {result['component_id']} 的{state}请求已接纳：desired state "
                f"{result['desired_state_revision']} / effective state "
                f"{result['effective_state_revision']}。生命周期发布后才算生效。"
            )
            outcome = {
                "code": "MODULE_STATE_ACCEPTED",
                **{key: result[key] for key in (
                    "operation_id", "component_id", "desired_config_revision",
                    "effective_config_revision", "desired_state_revision",
                    "effective_state_revision", "enabled", "status",
                )},
            }
            if self.on_change is not None:
                self.on_change()
        elif name == "memory.read":
            lines = []
            for row in result["sources"][:20]:
                author = row.get("author", {})
                parts = row.get("parts", [])
                preview = " ".join(
                    part.get("text", "") for part in parts if part.get("type") == "text"
                ).strip().replace("\n", " ")[:80]
                images = sum(part.get("type") == "image" for part in parts)
                detail = preview or (f"{images} 张图片" if images else "无文本预览")
                who = author.get("display_name") or author.get("external_user_id") or "未知用户"
                lines.append(f"{row['source_unit_id']}；{who}；{detail}")
            text = (
                f"本群记忆来源；删除版本 {result['deletion_epoch']}：\n"
                + "\n".join(lines or ["暂无可见来源。"])
            )
            outcome = {"code": "MEMORY_SOURCES", "count": len(lines),
                       "deletion_epoch": result["deletion_epoch"]}
        elif name == "memory.forget.plan":
            text = (
                f"遗忘预览：来源 {result['source_count']}，展开目标 {result['resolved_target_count']}，"
                f"受影响正文 {result['affected_payloads']}。提交命令：#forest memory forget-apply "
                f"{result['plan_id']} --digest {result['plan_digest']} --expect-deletion-epoch "
                f"{result['expected_deletion_epoch']}"
            )
            outcome = {"code": "MEMORY_FORGET_PLAN", "plan_id": result["plan_id"],
                       "plan_digest": result["plan_digest"],
                       "deletion_epoch": result["expected_deletion_epoch"],
                       "source_count": result["source_count"],
                       "affected_payloads": result["affected_payloads"]}
        elif name == "memory.forget.apply":
            text = (
                f"遗忘已提交：删除版本 {result['deletion_epoch']}，来源 {result['source_count']}，"
                f"展开目标 {result['target_count']}；后台正在物理清理正文。"
            )
            outcome = {"code": "ACCEPTED", "operation_id": operation,
                       "deletion_epoch": result["deletion_epoch"],
                       "source_count": result["source_count"],
                       "target_count": result["target_count"]}
            if self.on_change is not None:
                self.on_change()
        elif name in {"bot.pause", "bot.resume"}:
            text = (
                f"Bot {'已恢复业务处理' if result['data_admission'] else '已暂停业务处理'}；"
                f"维护版本 {result['maintenance_epoch']}。"
            )
            outcome = {
                "code": "BOT_MAINTENANCE",
                "maintenance_epoch": result["maintenance_epoch"],
                "data_admission": result["data_admission"],
            }
            if self.on_change is not None:
                self.on_change()
        else:
            # Only metadata is safe to publish to every member of the source group.
            visible = {key: result[key] for key in (
                "job_id", "revision", "occurrence_id", "run_id", "phase", "checkpoint_seq", "decision", "status"
            ) if key in result and isinstance(result[key], (str, int, bool))}
            text = "管理操作已接纳：" + "；".join(f"{key}={value}" for key, value in visible.items())
            outcome = {"code": "ACCEPTED", "operation_id": operation, **visible}
            if self.on_change is not None:
                self.on_change()
        return text[:6000], outcome

    @staticmethod
    def _job_status(result, conversation_id):
        # Repositories filter before pagination; this final projection also prevents
        # accidentally publishing task prompts, external account IDs or credentials.
        rows = result.get("jobs", [])
        allowed = {row["job_id"] for row in rows if row.get("conversation_id") == conversation_id}
        lines = [
            f"{row['job_id']}：修订 {row['revision']}；{'启用' if row['enabled'] else '停用'}"
            for row in rows if row.get("conversation_id") == conversation_id
        ][:16]
        occurrences = []
        for row in result.get("occurrences", []):
            if row["job_id"] not in allowed or len(occurrences) >= 8:
                continue
            detail = f"{row['occurrence_id']}：{row['status']}；执行修订 {row['revision']}"
            if row.get("run_id"):
                detail += f"；Run {row['run_id']}"
            if row.get("delivery_state"):
                detail += f"；平台发送 {row['delivery_state']}"
            if row.get("error_code"):
                detail += f"；{row['error_code']}"
            occurrences.append(detail)
        return "本群计划：\n" + "\n".join(lines + occurrences or ["暂无计划。"]), {
            "code": "JOB_STATUS", "count": len(lines), "occurrence_count": len(occurrences),
        }

    @staticmethod
    def _event_status(result):
        subscriptions = [
            f"订阅 {row['subscriber_id']} v{row['contract_major']}："
            f"{'启用' if row['active'] else '已退役'}；修订 {row['management_revision']}"
            for row in result.get("subscriptions", [])[:12]
        ]
        deliveries = [
            f"投递 {row['delivery_id']}：{row['status']}；回放 {row['replay_epoch']}；"
            f"修订 {row['management_revision']}"
            for row in result.get("deliveries", [])[:12]
        ]
        return "组件事件：\n" + "\n".join(
            subscriptions + deliveries or ["暂无组件事件订阅或投递。"]
        ), {
            "code": "EVENT_STATUS",
            "subscription_count": len(subscriptions),
            "delivery_count": len(deliveries),
        }

    @staticmethod
    def _component_job_status(result):
        handlers = [
            f"处理器 {row['component_id']} / {row['handler_id']}："
            f"{'启用' if row['active'] else '已退役'}；修订 {row['management_revision']}"
            for row in result.get("handlers", [])[:12]
        ]
        jobs = [
            f"任务 {row['job_id']}：{row['status']}；回放 {row['replay_epoch']}；"
            f"修订 {row['management_revision']}"
            for row in result.get("jobs", [])[:12]
        ]
        return "组件持久任务（不属于会话 Agent 计划）：\n" + "\n".join(
            handlers + jobs or ["暂无组件持久任务处理器或任务。"]
        ), {
            "code": "COMPONENT_JOB_STATUS",
            "handler_count": len(handlers),
            "job_count": len(jobs),
        }

    @staticmethod
    def _audit_status(result):
        lines = [
            f"{row['audit_id']}：{row['action']}；{row['origin_kind']}；{row['created_at']}"
            for row in result.get("records", [])[:20]
        ]
        cursor = result.get("next_before")
        if cursor:
            lines.append(f"继续读取：#forest audit list --before {cursor}")
        return "Bot 审计元数据：\n" + "\n".join(lines or ["暂无审计记录。"]), {
            "code": "AUDIT_STATUS",
            "count": min(len(result.get("records", [])), 20),
            "next_before": cursor,
        }

    async def close(self) -> None:
        if self._close_task is None:
            self._close_task = asyncio.create_task(self._close(), name="close:" + self.owner_id)
        await asyncio.shield(self._close_task)

    async def _close(self):
        self._closed = True
        self._wake.set()
        if self.task is not None and self.task is not asyncio.current_task():
            self.task.cancel()
            await asyncio.gather(self.task, return_exceptions=True)
        jobs = tuple(self._jobs.values())
        for job in jobs:
            if not job.cancelling():
                job.cancel()
        if jobs:
            await asyncio.gather(*jobs, return_exceptions=True)
