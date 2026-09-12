"""Host-owned OneBot clients and supervised, credential-free protocol Workers."""

from __future__ import annotations

import asyncio
import base64
import json
from dataclasses import dataclass, field
from uuid import uuid4

import psycopg
from psycopg_pool import PoolTimeout

from forest.adapters.onebot import OneBotHTTPClient, PreparedSend, SendResult, verify_signature
from forest.delivery import DeliveryWorker, PreparedDelivery
from forest.errors import ForestError
from forest.storage.bindings import BindingStore
from forest.storage.delivery import DeliveryStore
from forest.storage.messages import MessageStore

_LOST_AUTHORITY = {"STALE_HOST", "STORAGE_SEALED", "KERNEL_UNAVAILABLE", "BOT_SCOPE_MISMATCH"}


def _observe(task):
    if not task.cancelled():
        task.exception()


@dataclass
class Connection:
    row: dict
    client: OneBotHTTPClient
    webhook_secret: str
    accepting: bool = True
    tasks: set[asyncio.Task] = field(default_factory=set)
    _close_task: asyncio.Task | None = field(default=None, init=False)

    async def send(self, prepared, *, timeout=None):
        if not self.accepting:
            return SendResult("NOT_SENT", error_code="CONNECTION_RETIRED_BEFORE_SEND")
        task = asyncio.current_task()
        self.tasks.add(task)
        try:
            return await self.client.send(prepared, timeout=timeout)
        finally:
            self.tasks.discard(task)

    async def close(self):
        self.accepting = False
        if self._close_task is None or (
            self._close_task.done()
            and (self._close_task.cancelled() or self._close_task.exception())
        ):
            self._close_task = asyncio.create_task(self._close(), name="close:onebot-client")
            self._close_task.add_done_callback(_observe)
        await asyncio.shield(self._close_task)

    async def _close(self):
        active = {task for task in self.tasks if not task.done()}
        if active:
            _, pending = await asyncio.wait(active, timeout=3)
            for task in pending:
                task.cancel()
            if pending:
                _, still_owned = await asyncio.wait(pending, timeout=3)
                if still_owned:
                    await self.client.aclose()
                    raise ForestError(
                        "CONNECTION_CLEANUP_FAILED", "Connection work remains owned", 503
                    )
        await self.client.aclose()


@dataclass
class PendingActivation:
    binding_id: str
    revision: int
    client_incarnation: str
    expected_binding_epoch: int
    client: OneBotHTTPClient | None
    webhook_secret: str


class BindingRuntime:
    def __init__(self, bot, vault, packages: dict, *, global_slots=None, job_authority=None,
                 global_model_slots=None, global_child_slots=None):
        self.bot, self.kernel, self.vault = bot, bot.kernel, vault
        self.global_model_slots, self.global_child_slots = global_model_slots, global_child_slots
        self.packages = dict(packages)
        self.bindings = BindingStore(bot.store)
        self.messages = MessageStore(bot.store, self.bindings)
        self.memory_sources = self.messages._memory_sources()
        self.delivery = DeliveryStore(bot.store, self.bindings)
        self.connections: dict[str, Connection] = {}
        self.errors: dict[str, str] = {}
        self._applied: dict[str, tuple[int, str]] = {}
        self._retries: dict[str, tuple[int, int, float]] = {}
        self._jobs: dict[str, asyncio.Task] = {}
        self._component_jobs: dict[str, asyncio.Task] = {}
        self._component_locks: dict[str, asyncio.Lock] = {}
        self._clients: set[OneBotHTTPClient] = set()
        self._pending: dict[str, PendingActivation] = {}
        self._connect_slots = asyncio.Semaphore(4)
        self._closed = asyncio.Event()
        self._tasks: list[asyncio.Task] = []
        self._start_lock = asyncio.Lock()
        self._close_task: asyncio.Task | None = None
        self._started = False
        self._failure: ForestError | None = None
        self.delivery_worker = DeliveryWorker(
            self.delivery, self.kernel._authority, self.prepare, on_error=self._delivery_error
        )
        from forest.agent_runtime import AgentRuntime
        from forest.group_scheduler import GroupScheduler
        from forest.job_scheduler import JobDispatcher
        from forest.shell import ShellDispatcher
        from forest.storage.jobs import JobStore

        self.agent = AgentRuntime(self)

        self.scheduler = GroupScheduler(
            self.messages,
            lease=self.kernel._authority,
            execute=self.execute,
            delivery=self.delivery,
            global_slots=global_slots,
            finalize_agent=self.agent.runner.finalize,
            execution_timeout=3600,
        )
        self.shell = ShellDispatcher(
            self.messages, self.bindings, self.delivery, self.kernel._authority,
            on_change=self._notify_work,
        )
        self.job_store = JobStore(self.messages)
        self.job_authority = job_authority
        self.jobs = JobDispatcher(
            self.job_store, self.kernel._authority,
            authorize=self._authorize_job if job_authority is not None else None,
            on_change=self.scheduler.notify,
        )

    def _notify_work(self):
        self.scheduler.notify()
        self.jobs.notify()

    async def _authorize_job(self, claim):
        grant = claim.delegation
        if grant["issuer_id"] != self.job_authority.issuer_id:
            raise ForestError("JOB_DELEGATION_REVOKED", "Job authority issuer changed", 403)
        request = self.job_store.authorization_request(claim)
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
        for connection in self.connections.values():
            connection.accepting = False

    def _critical_done(self, task):
        _observe(task)
        if not self._closed.is_set():
            self._fatal("BINDING_SUPERVISOR_STOPPED")

    def _delivery_error(self, code):
        self.errors["delivery"] = code

    async def start(self):
        async with self._start_lock:
            if self._closed.is_set():
                raise ForestError("BINDING_RUNTIME_CLOSED", "Platform runtime is closed", 503)
            if self._started:
                return
            await self.scheduler.start()
            await self.shell.start()
            await self.agent.start()
            await self.jobs.start()
            self._tasks = [
                asyncio.create_task(self.run_forever(), name="binding-supervisor"),
                asyncio.create_task(self.delivery_worker.run_forever(), name="binding-delivery"),
                asyncio.create_task(self._health_loop(), name="binding-health"),
                asyncio.create_task(self._memory_backfill_loop(), name="memory-source-backfill"),
            ]
            if self.scheduler.task is not None:
                self._tasks.append(self.scheduler.task)
            if self.shell.task is not None:
                self._tasks.append(self.shell.task)
            if self.jobs.task is not None:
                self._tasks.append(self.jobs.task)
            if self.agent.children.task is not None:
                self._tasks.append(self.agent.children.task)
            for task in self._tasks:
                task.add_done_callback(self._critical_done)
            self._started = True

    async def _health_loop(self):
        while not self._closed.is_set():
            if (
                self.scheduler.failure is not None
                or self.delivery_worker.failure is not None
                or self.shell.failure is not None
                or self.jobs.failure is not None
                or self.agent.children.failure is not None
            ):
                self._fatal("PLATFORM_CHILD_FAILED")
                return
            await asyncio.sleep(0.1)

    async def _memory_backfill_loop(self):
        """Incremental legacy source recovery; no model or network dependency.

        Each bounded repository batch validates current ownership. Memory
        recovery errors remain visible and retry without stopping live input.
        The task belongs to the platform supervisor and is cancelled on close.
        """
        while not self._closed.is_set():
            delay = 10
            try:
                async with asyncio.timeout(5):
                    result = await self.memory_sources.backfill(self.kernel._authority(), limit=32)
                self.errors.pop("memory_sources", None)
                delay = 0.25 if result["more_possible"] else 5
            except asyncio.CancelledError:
                raise
            except ForestError as exc:
                if exc.code in _LOST_AUTHORITY:
                    raise
                self.errors["memory_sources"] = exc.code[:128]
            except (TimeoutError, psycopg.Error, PoolTimeout):
                self.errors["memory_sources"] = "MEMORY_SOURCE_BACKFILL_UNAVAILABLE"
            except Exception:
                self.errors["memory_sources"] = "MEMORY_SOURCE_BACKFILL_FAILED"
            try:
                await asyncio.wait_for(self._closed.wait(), delay)
            except TimeoutError:
                pass

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

    async def _close_client(self, client):
        await client.aclose()
        self._clients.discard(client)

    async def _disconnect(self, connection):
        try:
            await self.bindings.disconnect(
                self._cleanup_lease(),
                connection.row["binding_id"],
                client_incarnation=connection.row["client_incarnation"],
                expected_binding_epoch=connection.row["binding_epoch"],
            )
        except ForestError as exc:
            if exc.code not in _LOST_AUTHORITY | {"STALE_BINDING", "BINDING_NOT_FOUND"}:
                raise

    async def _resolve_activation(self, pending, *, adopt):
        async with asyncio.timeout(5):
            try:
                row = await self.bindings.current(self._cleanup_lease(), pending.binding_id)
            except ForestError as exc:
                if exc.code not in _LOST_AUTHORITY | {"BINDING_NOT_FOUND"}:
                    raise
                self._pending.pop(pending.client_incarnation, None)
                return False
            ours = (
                row["client_incarnation"] == pending.client_incarnation
                and row["binding_epoch"] == pending.expected_binding_epoch + 1
                and row["effective_revision"] == pending.revision
            )
            if ours and adopt:
                self._adopt(pending, row)
                return True
            if ours:
                await self.bindings.disconnect(
                    self._cleanup_lease(),
                    pending.binding_id,
                    client_incarnation=pending.client_incarnation,
                    expected_binding_epoch=row["binding_epoch"],
                )
            self._pending.pop(pending.client_incarnation, None)
            return False

    def _adopt(self, pending, row):
        # No await between publication/reconciliation and ownership registration.
        if pending.client is not None:
            self.connections[pending.binding_id] = Connection(
                row,
                pending.client,
                pending.webhook_secret,
                accepting=(
                    not self._closed.is_set()
                    and row["state"] == "ONLINE"
                    and row["enabled"]
                    and row["receive_enabled"]
                ),
            )
        self._applied[pending.binding_id] = (pending.revision, pending.client_incarnation)
        self._pending.pop(pending.client_incarnation, None)
        self._retries.pop(pending.binding_id, None)
        self.errors.pop(pending.binding_id, None)

    async def _replace(self, desired):
        binding_id, revision = desired["binding_id"], desired["revision"]
        candidate, pending = None, None
        try:
            async with self._connect_slots:
                if self._closed.is_set():
                    return
                values = desired["desired_values"]
                secrets = json.loads(self.vault.decrypt(desired["secret_ciphertext"]))
                webhook = secrets["webhook_secret"]
                if values["enabled"]:
                    if not isinstance(webhook, str) or not 1 <= len(webhook) <= 4096:
                        raise ForestError("BINDING_SECRET_INVALID", "Invalid webhook secret", 422)
                    webhook.encode("utf-8")
                    await self.ensure_components(("adapter.onebot",))
                    candidate = OneBotHTTPClient(values["api_base_url"], secrets["access_token"])
                    self._clients.add(candidate)
                    identity = await candidate.get_login_info(timeout=5)
                    if identity["self_id"] != desired["self_id"]:
                        raise ForestError(
                            "ONEBOT_IDENTITY_MISMATCH", "API account differs from binding", 409
                        )
                drained = await self.bindings.drain(self.kernel._authority(), binding_id, revision)
                old = self.connections.get(binding_id)
                if old is not None:
                    old.accepting = False
                    await old.close()
                    await self._disconnect(old)
                    self._clients.discard(old.client)
                    self.connections.pop(binding_id, None)
                if self._closed.is_set():
                    return
                pending = PendingActivation(
                    binding_id, revision, str(uuid4()), drained["binding_epoch"], candidate, webhook
                )
                self._pending[pending.client_incarnation] = pending
                row = await self.bindings.activate(
                    self.kernel._authority(),
                    binding_id,
                    revision,
                    enabled=values["enabled"],
                    client_incarnation=pending.client_incarnation,
                    expected_binding_epoch=pending.expected_binding_epoch,
                )
                self._adopt(pending, row)
                candidate = None
        except BaseException as exc:
            if pending is not None and pending.client_incarnation in self._pending:
                try:
                    if await self._resolve_activation(pending, adopt=True):
                        candidate = None
                        if not isinstance(exc, asyncio.CancelledError):
                            return
                except BaseException:
                    # Keep both the pending publication and its client until a
                    # cleanup attempt can prove the result or observe host loss.
                    self._fatal("BINDING_PUBLICATION_UNCERTAIN")
                    raise
            if isinstance(exc, asyncio.CancelledError):
                raise
            if not isinstance(exc, Exception):
                raise
            code = exc.code if isinstance(exc, ForestError) else "BINDING_CONNECT_FAILED"
            self.errors[binding_id] = code
            previous = self._retries.get(binding_id)
            failures = previous[1] + 1 if previous and previous[0] == revision else 1
            self._retries[binding_id] = (
                revision,
                failures,
                asyncio.get_running_loop().time() + min(60, 2 ** min(failures, 6)),
            )
            if code in _LOST_AUTHORITY:
                self._fatal(code)
            try:
                await self.bindings.report_error(
                    self.kernel._authority(), binding_id, revision, code
                )
            except Exception:
                self.errors["store"] = "BINDING_REPORT_UNAVAILABLE"
        finally:
            retained = pending is not None and pending.client_incarnation in self._pending
            if candidate is not None and not retained:
                try:
                    await self._close_client(candidate)
                except BaseException:
                    self._fatal("BINDING_CLIENT_CLEANUP_FAILED")
                    raise

    async def run_forever(self):
        while not self._closed.is_set():
            try:
                desired = await self.bindings.desired(self.kernel._authority())
                for row in desired:
                    binding_id = row["binding_id"]
                    job = self._jobs.get(binding_id)
                    if job is not None:
                        if not job.done():
                            continue
                        _observe(job)
                        self._jobs.pop(binding_id)
                    if self._matches_owner(row):
                        continue
                    retry = self._retries.get(binding_id)
                    if (
                        retry
                        and retry[0] == row["revision"]
                        and asyncio.get_running_loop().time() < retry[2]
                    ):
                        continue
                    job = asyncio.create_task(self._replace(row), name="binding:" + binding_id)
                    job.add_done_callback(_observe)
                    self._jobs[binding_id] = job
                if self.connections:
                    for key in self.packages:
                        job = self._component_jobs.get(key)
                        if job is None or job.done():
                            if job is not None:
                                _observe(job)
                            job = asyncio.create_task(
                                self._ensure_component(key), name="component:" + key
                            )
                            job.add_done_callback(_observe)
                            self._component_jobs[key] = job
            except asyncio.CancelledError:
                raise
            except ForestError as exc:
                self.errors["store"] = exc.code
                if exc.code in _LOST_AUTHORITY:
                    self._fatal(exc.code)
                    return
            except (psycopg.Error, PoolTimeout):
                self.errors["store"] = "BINDING_STORE_UNAVAILABLE"
            except Exception:
                self._fatal("BINDING_SUPERVISOR_FAILED")
                return
            try:
                await asyncio.wait_for(self._closed.wait(), timeout=1)
            except TimeoutError:
                pass

    def _matches_owner(self, row):
        if self._applied.get(row["binding_id"]) != (row["revision"], row["client_incarnation"]):
            return False
        if not row["desired_values"]["enabled"]:
            return row["state"] == "DISABLED" and not row["enabled"]
        connection = self.connections.get(row["binding_id"])
        return (
            connection is not None
            and connection.accepting
            and not connection.client._client.is_closed
            and row["state"] == "ONLINE"
            and all(
                connection.row[key] == row[key]
                for key in (
                    "binding_epoch",
                    "connection_epoch",
                    "client_incarnation",
                    "effective_revision",
                )
            )
        )

    def connection(self, binding_id: str) -> Connection:
        if self._closed.is_set() or self._failure is not None:
            raise ForestError("BINDING_RUNTIME_CLOSED", "Platform runtime requires recovery", 503)
        connection = self.connections.get(binding_id)
        if connection is None or not connection.accepting:
            raise ForestError("BINDING_OFFLINE", "Binding has no active connection", 503)
        self.kernel._authority()
        return connection

    async def receive(self, binding_id: str, body: bytes, signature: str | None) -> dict:
        connection = self.connection(binding_id)
        verify_signature(body, connection.webhook_secret, signature)
        task = asyncio.current_task()
        connection.tasks.add(task)
        try:
            await self.ensure_components(("adapter.onebot",))
            # Retrying a failed pure codec call is safe. The durable Inbox, not
            # this disposable Worker call, owns platform-message deduplication.
            normalized = await self.kernel.invoke_control(
                "adapter.onebot",
                "onebot.normalize",
                {
                    "body_b64": base64.b64encode(body).decode("ascii"),
                    "self_id": connection.row["self_id"],
                },
                operation_id="ingress:" + str(uuid4()),
                timeout=10,
            )
            event = normalized["message"]
            if event is None:
                return {"accepted": True, "kind": "NON_MESSAGE"}
            if event["author"].get("is_self"):
                echo = await self.delivery.find_echo(
                    self.kernel._authority(),
                    binding_id,
                    event["platform_message_id"],
                    event["channel_kind"],
                    event["channel_id"],
                )
                if echo:
                    return {"accepted": True, "kind": "KNOWN_ECHO"}
            config = connection.row["config"]
            text = "".join(s.get("text", "") for s in event["segments"] if s["type"] == "text")
            mention = any(
                s["type"] == "mention" and s.get("external_user_id") == connection.row["self_id"]
                for s in event["segments"]
            )
            trigger = (
                config["response_mode"] in {"component", "agent"}
                and not event["author"].get("is_self")
                and (
                    (config["trigger_mentions"] and mention)
                    or (
                        config["trigger_prefix"]
                        and text.lstrip().startswith(config["trigger_prefix"])
                    )
                    or (event["channel_kind"] == "private" and config["private_trigger"])
                )
            )
            receipt = await self.messages.ingest(
                self.kernel._authority(),
                binding_id,
                event,
                expected_binding_epoch=connection.row["binding_epoch"],
                decision="TRIGGER" if trigger else "NO_TRIGGER",
                rule_revision="binding:" + str(connection.row["effective_revision"]),
            )
            self.scheduler.notify()
            self.shell.notify()
            return {"accepted": True, "event_id": receipt["event_id"]}
        finally:
            connection.tasks.discard(task)

    async def execute(self, actor, selected, context):
        connection = self.connection(actor.binding_id)
        if connection.row["binding_epoch"] != actor.binding_epoch:
            raise ForestError("STALE_BINDING", "Conversation belongs to an old connection", 409)
        config = connection.row["config"]
        if selected.get("agent_phase") is not None or config["response_mode"] == "agent":
            return await self.agent.runner.advance(actor, selected, context)
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
        result = await self.kernel.invoke(
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
        )
        return result["parts"]

    async def prepare(self, claim) -> PreparedDelivery:
        connection = self.connection(claim.route["binding_id"])
        if (
            connection.row["binding_epoch"] != claim.binding_epoch
            or connection.row["connection_epoch"] != claim.connection_epoch
        ):
            raise ForestError("STALE_BINDING", "Delivery belongs to an old connection", 409)
        await self.ensure_components(("adapter.onebot",))
        operation = "format:" + str(uuid4())
        value = await self.kernel.invoke_control(
            "adapter.onebot",
            "onebot.prepare",
            {
                "channel_kind": claim.route["channel_kind"],
                "channel_id": claim.route["channel_id"],
                "segments": claim.payload,
            },
            operation_id=operation,
            timeout=5,
        )
        call = await self.bot.store.read_call(operation)
        if call is None or call["status"] != "COMPLETED":
            raise ForestError(
                "FORMAT_RECEIPT_MISSING", "Adapter output has no durable receipt", 503
            )
        prepared = PreparedSend(
            value["action"],
            base64.b64decode(value["wire_body_b64"], validate=True),
            value["wire_digest"],
        )
        return PreparedDelivery(
            prepared,
            connection.send,
            {
                key: call[key]
                for key in (
                    "component_id",
                    "generation_id",
                    "worker_incarnation",
                    "route_epoch",
                    "policy_revision",
                    "admission_lane",
                )
            },
        )

    async def close(self):
        self._closed.set()
        for connection in self.connections.values():
            connection.accepting = False
        if self._close_task is None or (
            self._close_task.done()
            and (self._close_task.cancelled() or self._close_task.exception())
        ):
            self._close_task = asyncio.create_task(self._close(), name="close:binding-runtime")
            self._close_task.add_done_callback(_observe)
        await asyncio.shield(self._close_task)

    async def _close(self):
        for task in (*self._jobs.values(), *self._component_jobs.values(), *self._tasks):
            task.cancel()
        errors = []
        for closer in (self.jobs.close, self.scheduler.close, self.shell.close,
                       self.delivery_worker.close, self.agent.close):
            try:
                await closer()
            except Exception as exc:
                errors.append(exc)
        await asyncio.gather(
            *self._tasks,
            *self._jobs.values(),
            *self._component_jobs.values(),
            return_exceptions=True,
        )
        for pending in list(self._pending.values()):
            try:
                await self._resolve_activation(pending, adopt=False)
            except Exception as exc:
                errors.append(exc)
        for binding_id, connection in list(self.connections.items()):
            try:
                await self._disconnect(connection)
                await connection.close()
                self._clients.discard(connection.client)
                self.connections.pop(binding_id)
            except Exception as exc:
                errors.append(exc)
        retained = {p.client for p in self._pending.values()} | {
            c.client for c in self.connections.values()
        }
        for client in list(self._clients - retained):
            try:
                await self._close_client(client)
            except Exception as exc:
                errors.append(exc)
        if errors or self._clients or self._pending or self.connections:
            raise ForestError("PLATFORM_CLEANUP_FAILED", "Platform resources remain owned", 503)
        self._tasks.clear()
        self._jobs.clear()
        self._component_jobs.clear()
