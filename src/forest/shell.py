"""Independent typed group control; never invokes the chat Agent for commands."""

from __future__ import annotations

import asyncio
from collections.abc import Callable
from uuid import uuid4

from forest.errors import ForestError
from forest.shell_registry import CONTROL_DEADLINE, get_command_spec, render_help
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
                if claim["status"] == "COMPLETED":
                    return
                spec = get_command_spec(claim["command_name"])
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
                    admins = settings["config"].get("admin_user_ids", [])
                    authorized = (
                        not author.get("is_self") and author.get("anonymous") is None
                        and author.get("identity_origin") == "adapter_verified"
                        and isinstance(admins, list)
                        and all(isinstance(admin, str) for admin in admins)
                        and author.get("external_user_id") in admins
                    )
                    paused = not binding["_runtime"]["data_admission"]
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
                    text = render_help()
                    outcome = {"code": "HELP"}
                elif builtin_method == "status":
                    text = f"Forest 状态：{'业务已暂停' if paused else '运行中'}；待处理会话消息：{count['pending']}。"
                    outcome = {"code": "STATUS", "paused": paused, "pending_messages": int(count["pending"])}
                elif claim["command_name"].startswith(("job.", "run.")):
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
        spec = get_command_spec(name)
        if spec is None or spec.action is None:
            raise ForestError("SHELL_UNSUPPORTED", "Unknown typed command", 422)
        if spec.execution_target == "agent":
            repository = AgentStore(self.messages)
            repository.control = control
        elif spec.execution_target == "job":
            from forest.storage.jobs import JobStore

            repository = JobStore(self.messages, control=control)
        else:
            raise ForestError("SHELL_UNSUPPORTED", "Unknown typed command", 422)
        result = await getattr(repository, spec.execution_method)(admission, operation, payload)
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
