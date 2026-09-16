"""Per-Bot microkernel composition and cross-module conversation execution."""

from __future__ import annotations

import asyncio
from uuid import uuid4

from forest.errors import ForestError
from forest.runtime_modules import BotModuleRegistry


def _observe(task):
    if not task.cancelled():
        task.exception()


class BindingRuntime:
    def __init__(self, bot, vault, packages: dict, *, global_slots=None, job_authority=None,
                 global_model_slots=None, module_packages=None):
        self.bot, self.kernel, self.vault = bot, bot.kernel, vault
        self.global_slots = global_slots
        self.global_model_slots = global_model_slots
        self.packages = dict(packages)
        self.module_packages = {} if module_packages is None else module_packages
        self.errors: dict[str, str] = {}
        self._component_locks: dict[str, asyncio.Lock] = {}
        self._closed = asyncio.Event()
        self._tasks: list[asyncio.Task] = []
        self._start_lock = asyncio.Lock()
        self._close_task: asyncio.Task | None = None
        self._started = False
        self._failure: ForestError | None = None
        self.job_authority = job_authority
        self.module_registry = BotModuleRegistry(self)

    def module(self, component_id: str):
        """Resolve a runtime capability owner by its stable component identity."""
        return self.module_registry.get(component_id)

    def _notify_work(self):
        self.module("conversation.scheduler").notify()
        self.module("scheduler.jobs").notify()
        self.module("memory.graph").notify()
        self.kernel.notify_events()
        self.kernel.notify_component_jobs()

    def notify(self):
        """Wake durable business workers after an out-of-band control change."""
        self._notify_work()

    async def _authorize_job(self, claim):
        grant = claim.delegation
        if grant["issuer_id"] != self.job_authority.issuer_id:
            raise ForestError("JOB_DELEGATION_REVOKED", "Job authority issuer changed", 403)
        request = self.module("scheduler.jobs").store.authorization_request(claim)
        return await self.job_authority.issue_delegation(
            subject_id=grant["subject_id"], bot_id=self.bot.store.bot_id,
            operation_id=request["operation_id"], payload=request["payload"],
            policy_revision=grant["policy_revision"], delegation_id=grant["delegation_id"],
            expected_account_revision=grant["account_revision"], expected_key_revision=grant["key_revision"],
        )

    @property
    def failure(self):
        return self._failure

    def _fatal(self, code):
        if self._failure is None:
            self._failure = ForestError(code, "Platform supervisor requires recovery", 503)
        self._closed.set()
        adapter = self.module_registry.instances.get("adapter.onebot")
        if adapter is not None:
            adapter.stop_admission()

    def _critical_done(self, task):
        _observe(task)
        if not self._closed.is_set():
            self._fatal("RUNTIME_SUPERVISOR_STOPPED")

    async def start(self):
        async with self._start_lock:
            if self._closed.is_set():
                raise ForestError("BINDING_RUNTIME_CLOSED", "Platform runtime is closed", 503)
            if self._started:
                return
            await self.module_registry.start()
            self._tasks = [
                asyncio.create_task(self._health_loop(), name="runtime-health"),
            ]
            self._tasks.extend(self.module_registry.tasks())
            for task in self._tasks:
                task.add_done_callback(self._critical_done)
            self._started = True

    async def _health_loop(self):
        while not self._closed.is_set():
            if (
                self.module_registry.failures()
            ):
                self._fatal("PLATFORM_CHILD_FAILED")
                return
            await asyncio.sleep(0.1)

    async def ensure_components(self, component_ids=None):
        ids = tuple(self.packages) if component_ids is None else tuple(component_ids)
        results = await asyncio.gather(
            *(self._ensure_component(key) for key in ids), return_exceptions=True
        )
        for result in results:
            if isinstance(result, BaseException):
                raise result

    async def _ensure_component(self, component_id):
        async with self._component_locks.setdefault(component_id, asyncio.Lock()):
            if self._closed.is_set():
                raise ForestError("BINDING_RUNTIME_CLOSED", "Platform runtime is closed", 503)
            self.kernel._authority()
            component = self.kernel.components.get(component_id)
            if (
                component is not None
                and component.worker.alive
                and component.worker.state == "ACTIVE"
            ):
                return
            try:
                await self.kernel.install(
                    self.packages[component_id],
                    config={},
                    grants=frozenset(),
                    operation_id="infrastructure:" + str(uuid4()),
                )
                self.errors.pop(component_id, None)
            except Exception as exc:
                self.errors[component_id] = (
                    exc.code if isinstance(exc, ForestError) else "COMPONENT_START_FAILED"
                )
                raise

    def _cleanup_lease(self):
        # SQL validates this last lease even after local admission has closed.
        if self.kernel.lease is None:
            raise ForestError("STALE_HOST", "No host lease remains", 409)
        return self.kernel.lease

    async def execute(self, actor, selected, context):
        connection = self.module("adapter.onebot").connection(actor.binding_id)
        if connection.row["binding_epoch"] != actor.binding_epoch:
            raise ForestError("STALE_BINDING", "Conversation belongs to an old connection", 409)
        if selected["status"] == "MESSAGE_HANDLER":
            request = selected["request"]
            await self.ensure_components((request["component_id"],))
            call_operation_id = selected["attempt"]["call_operation_id"]
            receipt = await self.kernel.invoke_message_handler_with_receipt(
                request,
                selected["event"],
                operation_id=call_operation_id,
                input_parent_ids=tuple(selected["input_parent_ids"]),
            )
            from forest.group_scheduler import MessageHandlerExecution

            return MessageHandlerExecution(
                call_operation_id,
                receipt.value,
                receipt.result_payload_id,
                receipt.result_digest,
            )
        if selected["status"] == "BUSINESS_COMMAND":
            request = selected["request"]
            await self.ensure_components((request["component_id"],))
            call_operation_id = "business-command-call:" + request["request_id"]
            receipt = await self.kernel.invoke_business_command_with_receipt(
                request,
                selected["event"],
                operation_id=call_operation_id,
                input_parent_ids=tuple(selected["input_parent_ids"]),
            )
            from forest.group_scheduler import BusinessCommandExecution

            return BusinessCommandExecution(
                call_operation_id,
                receipt.value,
                receipt.result_payload_id,
                receipt.result_digest,
            )
        current = await self.module("adapter.onebot").bindings.execution_config(
            self.kernel._authority(), actor.binding_id, expected_binding_epoch=actor.binding_epoch, data=True,
        )
        config = current["config"]
        if selected.get("agent_phase") is not None or config["response_mode"] == "agent":
            return await self.module("agent.runtime").runner.advance(actor, selected, context)
        if config["response_mode"] != "component":
            raise ForestError("RESPONDER_DISABLED", "This Binding has no active responder", 409)
        await self.ensure_components(("forest.reply",))
        trigger_ids = {t["event_id"] for t in context["triggers"]}
        units = [u for u in context["units"] if u.get("event_id") in trigger_ids]
        if not units:
            raise ForestError("TRIGGER_CONTEXT_MISSING", "Trigger source is not visible", 409)
        unit = units[-1]
        text = "".join(s.get("text", "") for s in unit["segments"] if s["type"] == "text")
        # Explicit P1 template service; P3 owns durable model/provider attempts.
        result = await self.kernel.invoke_with_receipt(
            "forest.reply",
            "reply.respond",
            {
                "template": config["response_template"],
                "text": text[:8000],
                "sender": unit["author"].get("display_name", unit["author"]["external_user_id"])[
                    :160
                ],
                "group": context["conversation"]["channel_id"][:160],
            },
            operation_id="response:" + str(uuid4()),
            expected_policy_revision=actor.policy_revision,
            input_parent_ids=(*context["payload_ids"], current["config_payload_id"]),
        )
        from forest.group_scheduler import ComponentReply

        return ComponentReply(result.value["parts"], (result.result_payload_id,))

    async def close(self):
        self._closed.set()
        adapter = self.module_registry.instances.get("adapter.onebot")
        if adapter is not None:
            adapter.stop_admission()
        if self._close_task is None or (
            self._close_task.done()
            and (self._close_task.cancelled() or self._close_task.exception())
        ):
            self._close_task = asyncio.create_task(self._close(), name="close:binding-runtime")
            self._close_task.add_done_callback(_observe)
        await asyncio.shield(self._close_task)

    async def _close(self):
        for task in self._tasks:
            task.cancel()
        await asyncio.gather(*self._tasks, return_exceptions=True)
        errors = await self.module_registry.close()
        if errors:
            raise ForestError("PLATFORM_CLEANUP_FAILED", "Runtime modules remain owned", 503)
        self._tasks.clear()
