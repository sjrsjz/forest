"""OneBot adapter runtime: network admission, connection generations and delivery."""

from __future__ import annotations

import asyncio
import base64
import json
from dataclasses import dataclass, field
from uuid import uuid4

import psycopg
from psycopg_pool import PoolTimeout

from forest.adapters.onebot import (
    OneBotHTTPClient,
    OneBotWebSocketClient,
    PreparedSend,
    SendResult,
    parse_event,
    verify_signature,
)
from forest.delivery import DeliveryWorker, PreparedDelivery
from forest.errors import ForestError
from forest.shell_ingress import ShellIngressGuard
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
    client: OneBotHTTPClient | OneBotWebSocketClient
    webhook_secret: str
    transport: str = "http-post"
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
    client: OneBotHTTPClient | OneBotWebSocketClient | None
    webhook_secret: str
    transport: str

class OneBotRuntime:
    def __init__(self, platform, *, component_host):
        self.platform = platform
        self.component_host = component_host
        self.bindings = BindingStore(platform.bot.store)
        self.messages = MessageStore(platform.bot.store, self.bindings)
        self.shell_ingress = ShellIngressGuard(platform.bot.store.bot_id)
        self.delivery = DeliveryStore(platform.bot.store, self.bindings)
        self.errors: dict[str, str] = {}
        self.connections: dict[str, Connection] = {}
        self._applied: dict[str, tuple[int, str]] = {}
        self._retries: dict[str, tuple[int, int, float]] = {}
        self._jobs: dict[str, asyncio.Task] = {}
        self._component_jobs: dict[str, asyncio.Task] = {}
        self._clients: set[OneBotHTTPClient | OneBotWebSocketClient] = set()
        self._pending: dict[str, PendingActivation] = {}
        self._connect_slots = asyncio.Semaphore(4)
        self._closed = asyncio.Event()
        self._failure: ForestError | None = None
        self.supervisor_task: asyncio.Task | None = None
        self.delivery_task: asyncio.Task | None = None
        self.worker = DeliveryWorker(
            self.delivery,
            platform.kernel._authority,
            self.prepare,
            on_error=self._delivery_error,
        )

    @property
    def failure(self):
        return self._failure or self.worker.failure

    def _delivery_error(self, code):
        self.errors["delivery"] = code

    def stop_admission(self):
        self._closed.set()
        for connection in self.connections.values():
            connection.accepting = False

    def _fatal(self, code):
        if self._failure is None:
            self._failure = ForestError(code, "OneBot adapter requires recovery", 503)
        self.stop_admission()
        self.platform._fatal(code)

    async def start(self):
        if self._closed.is_set():
            raise ForestError("ONEBOT_RUNTIME_CLOSED", "OneBot adapter is closed", 503)
        if self.supervisor_task is not None:
            return
        self.supervisor_task = asyncio.create_task(
            self.run_forever(), name="onebot-binding-supervisor"
        )
        self.delivery_task = asyncio.create_task(
            self.worker.run_forever(), name="onebot-delivery"
        )

    def owned_tasks(self):
        return tuple(
            task for task in (self.supervisor_task, self.delivery_task) if task is not None
        )

    async def quiesce(self):
        self.stop_admission()

    async def close(self):
        self.stop_admission()
        for task in (
            *self._jobs.values(),
            *self._component_jobs.values(),
            *self.owned_tasks(),
        ):
            task.cancel()
        await asyncio.gather(
            *self._jobs.values(),
            *self._component_jobs.values(),
            *self.owned_tasks(),
            return_exceptions=True,
        )
        await self.worker.close()
        errors = []
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
            raise ForestError("PLATFORM_CLEANUP_FAILED", "OneBot resources remain owned", 503)
        self._jobs.clear()
        self._component_jobs.clear()
    async def _close_client(self, client):
        await client.aclose()
        self._clients.discard(client)

    async def _disconnect(self, connection):
        try:
            await self.bindings.disconnect(
                self.platform._cleanup_lease(),
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
                row = await self.bindings.current(
                    self.platform._cleanup_lease(), pending.binding_id
                )
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
            if ours and adopt and row["config_payload_state"] == "ACTIVE":
                self._adopt(pending, row)
                return True
            if ours:
                await self.bindings.disconnect(
                    self.platform._cleanup_lease(),
                    pending.binding_id,
                    client_incarnation=pending.client_incarnation,
                    expected_binding_epoch=row["binding_epoch"],
                )
            self._pending.pop(pending.client_incarnation, None)
            return False

    def _adopt(self, pending, row):
        # No await between publication/reconciliation and ownership registration.
        if pending.client is not None:
            connection = Connection(
                row,
                pending.client,
                pending.webhook_secret,
                pending.transport,
                accepting=(
                    not self._closed.is_set()
                    and row["state"] == "ONLINE"
                    and row["enabled"]
                    and row["receive_enabled"]
                ),
            )
            self.connections[pending.binding_id] = connection
            if isinstance(pending.client, OneBotWebSocketClient):
                pending.client.activate_events(
                    lambda body, owned=connection: self._receive_websocket(owned, body)
                )
        self._applied[pending.binding_id] = (pending.revision, pending.client_incarnation)
        self._pending.pop(pending.client_incarnation, None)
        self._retries.pop(pending.binding_id, None)
        self.errors.pop(pending.binding_id, None)

    async def _replace(self, desired):
        binding_id = desired["binding_id"]
        recover = desired["desired_values_payload_state"] != "ACTIVE"
        revision = desired["effective_revision"] if recover else desired["revision"]
        candidate, pending = None, None
        try:
            async with self._connect_slots:
                if self._closed.is_set():
                    return
                saved = await self.bindings.candidate_config(
                    self.platform.kernel._authority(), binding_id, revision,
                    expected_desired_revision=desired["revision"], recover_effective=recover,
                )
                values = saved["values"]
                secrets = json.loads(self.platform.vault.decrypt(saved["secret_ciphertext"]))
                transport = values.get("transport", "http-post")
                websocket_url = values.get("websocket_url", "ws://127.0.0.1:8080")
                webhook = secrets.get("webhook_secret", "")
                if values["enabled"]:
                    if transport not in {"http-post", "forward-websocket"}:
                        raise ForestError(
                            "BINDING_CONFIG_INVALID", "Unsupported OneBot transport", 422
                        )
                    if (
                        transport == "http-post"
                        and (not isinstance(webhook, str) or not 1 <= len(webhook) <= 4096)
                    ):
                        raise ForestError("BINDING_SECRET_INVALID", "Invalid webhook secret", 422)
                    if webhook:
                        webhook.encode("utf-8")
                    await self.component_host.ensure(("adapter.onebot",))
                    if transport == "forward-websocket":
                        candidate = await OneBotWebSocketClient.create(
                            websocket_url, secrets["access_token"]
                        )
                    else:
                        candidate = OneBotHTTPClient(
                            values["api_base_url"], secrets["access_token"]
                        )
                    self._clients.add(candidate)
                    identity = await candidate.get_login_info(timeout=5)
                    if identity["self_id"] != desired["self_id"]:
                        raise ForestError(
                            "ONEBOT_IDENTITY_MISMATCH", "API account differs from binding", 409
                        )
                drained = await self.bindings.drain(
                    self.platform.kernel._authority(), binding_id, desired["revision"]
                )
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
                    binding_id,
                    revision,
                    str(uuid4()),
                    drained["binding_epoch"],
                    candidate,
                    webhook,
                    transport,
                )
                self._pending[pending.client_incarnation] = pending
                row = await self.bindings.activate(
                    self.platform.kernel._authority(),
                    binding_id,
                    revision,
                    enabled=values["enabled"],
                    client_incarnation=pending.client_incarnation,
                    expected_binding_epoch=pending.expected_binding_epoch,
                    recover_effective=recover,
                    expected_desired_revision=desired["revision"],
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
                    self.platform.kernel._authority(), binding_id, desired["revision"], code
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
                desired = await self.bindings.desired(
                    self.platform.kernel._authority()
                )
                for row in desired:
                    binding_id = row["binding_id"]
                    old = self.connections.get(binding_id)
                    if old is not None and row["config_payload_state"] != "ACTIVE":
                        old.accepting = False
                        old.row["config"] = None
                        old.row["config_payload_state"] = "UNAVAILABLE"
                        await self._disconnect(old)
                        await old.close()
                        self._clients.discard(old.client)
                        self.connections.pop(binding_id, None)
                        self._applied.pop(binding_id, None)
                    if (row["desired_values_payload_state"] != "ACTIVE"
                            and row["effective_values_payload_state"] != "ACTIVE"):
                        self.errors[binding_id] = "CONTROL_CONTENT_REVOKED"
                        continue
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
                    for key in self.component_host.packages:
                        job = self._component_jobs.get(key)
                        if job is None or job.done():
                            if job is not None:
                                _observe(job)
                            job = asyncio.create_task(
                                self.component_host.ensure_one(key), name="component:" + key
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
        desired_available = row["desired_values_payload_state"] == "ACTIVE"
        revision = row["revision"] if desired_available else row["effective_revision"]
        values = row["desired_values"] if desired_available else row["effective_values"]
        if (row["config_payload_state"] != "ACTIVE" or values is None
                or self._applied.get(row["binding_id"]) != (revision, row["client_incarnation"])):
            return False
        if not values["enabled"]:
            return row["state"] == "DISABLED" and not row["enabled"]
        connection = self.connections.get(row["binding_id"])
        return (
            connection is not None
            and connection.accepting
            and not connection.client.is_closed
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
        self.platform.kernel._authority()
        return connection

    async def receive(self, binding_id: str, body: bytes, signature: str | None) -> dict:
        connection = self.connection(binding_id)
        if connection.transport != "http-post":
            raise ForestError(
                "ONEBOT_TRANSPORT_MISMATCH",
                "This Binding receives events through its WebSocket",
                409,
            )
        verify_signature(body, connection.webhook_secret, signature)
        return await self._receive_connection(connection, body)

    async def _receive_websocket(self, connection: Connection, body: bytes) -> dict:
        current = self.connections.get(connection.row["binding_id"])
        if current is not connection or not connection.accepting:
            raise ForestError("STALE_BINDING", "WebSocket connection was retired", 409)
        if connection.transport != "forward-websocket":
            raise ForestError("ONEBOT_TRANSPORT_MISMATCH", "Unexpected WebSocket event", 409)
        return await self._receive_connection(connection, body)

    async def _receive_connection(self, connection: Connection, body: bytes) -> dict:
        binding_id = connection.row["binding_id"]
        preview = parse_event(body, self_id=connection.row["self_id"])
        preview_event = (
            self.messages._normalized(preview.as_dict())
            if preview is not None else None
        )
        adapter = self
        ingress_plan = adapter.shell_ingress.classify(connection.row, preview_event)
        task = asyncio.current_task()
        connection.tasks.add(task)
        try:
            async with adapter.shell_ingress.enter(ingress_plan) as priority_proof:
                staged = await adapter.messages.stage_platform_input(
                    self.platform.kernel._authority(), binding_id, body,
                    expected_binding_epoch=connection.row["binding_epoch"],
                )
                if staged.kind == "NON_MESSAGE":
                    return {"accepted": True, "kind": "NON_MESSAGE"}
                if staged.kind in {"DUPLICATE", "SUPPRESSED_OCCURRENCE"}:
                    return {"accepted": True, "kind": staged.kind, **staged.receipt}
                event = staged.event
                if event != preview_event:
                    raise ForestError(
                        "ONEBOT_PROJECTION_CONFLICT",
                        "Staged input differs from the signed host projection",
                        409,
                    )
                if priority_proof is None:
                    await self.component_host.ensure(("adapter.onebot",))
                    # Retrying a failed pure codec call is safe. The durable
                    # Inbox owns deduplication; reserved Shell syntax stays on
                    # the fixed host parser so a broken Adapter can be rescued.
                    normalized = await self.platform.kernel.invoke_control_with_receipt(
                        "adapter.onebot",
                        "onebot.normalize",
                        {
                            "body_b64": base64.b64encode(body).decode("ascii"),
                            "self_id": connection.row["self_id"],
                        },
                        operation_id="ingress:" + str(uuid4()),
                        timeout=10,
                        input_parent_ids=(staged.origin_payload_id,),
                    )
                    if adapter.messages._normalized(normalized.value["message"]) != event:
                        raise ForestError(
                            "ONEBOT_PROJECTION_CONFLICT",
                            "Codec changed the authenticated platform projection",
                            409,
                        )
                if event["author"].get("is_self"):
                    echo = await adapter.delivery.find_echo(
                        self.platform.kernel._authority(),
                        binding_id,
                        event["platform_message_id"],
                        event["channel_kind"],
                        event["channel_id"],
                    )
                    if echo:
                        return {"accepted": True, "kind": "KNOWN_ECHO"}
                trigger = False
                if priority_proof is None:
                    current = await adapter.bindings.execution_config(
                        self.platform.kernel._authority(), binding_id,
                        expected_binding_epoch=connection.row["binding_epoch"],
                    )
                    config = current["config"]
                    text = "".join(
                        s.get("text", "") for s in event["segments"] if s["type"] == "text"
                    )
                    mention = any(
                        s["type"] == "mention"
                        and s.get("external_user_id") == connection.row["self_id"]
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
                            or (
                                event["channel_kind"] == "private"
                                and config["private_trigger"]
                            )
                        )
                    )
                receipt = await adapter.messages.ingest(
                    self.platform.kernel._authority(),
                    binding_id,
                    event,
                    expected_binding_epoch=connection.row["binding_epoch"],
                    decision="TRIGGER" if trigger else "NO_TRIGGER",
                    rule_revision="binding:" + str(connection.row["effective_revision"]),
                    priority_proof=priority_proof,
                )
                if receipt.get("kind") == "SUPPRESSED_OCCURRENCE":
                    return {"accepted": True, **receipt}
                self.platform.notify()
                return {"accepted": True, "event_id": receipt["event_id"]}
        finally:
            connection.tasks.discard(task)


    async def prepare(self, claim) -> PreparedDelivery:
        connection = self.connection(claim.route["binding_id"])
        if (
            connection.row["binding_epoch"] != claim.binding_epoch
            or connection.row["connection_epoch"] != claim.connection_epoch
        ):
            raise ForestError("STALE_BINDING", "Delivery belongs to an old connection", 409)
        current = await self.bindings.execution_config(
            self.platform.kernel._authority(), claim.route["binding_id"], expected_binding_epoch=claim.binding_epoch,
        )
        await self.component_host.ensure(("adapter.onebot",))
        operation = "format:" + str(uuid4())
        receipt = await self.platform.kernel.invoke_control_with_receipt(
            "adapter.onebot",
            "onebot.prepare",
            {
                "channel_kind": claim.route["channel_kind"],
                "channel_id": claim.route["channel_id"],
                "segments": claim.payload,
            },
            operation_id=operation,
            timeout=5,
            input_parent_ids=(claim.payload_id, current["config_payload_id"]),
        )
        value = receipt.value
        call = await self.platform.bot.store.read_call(operation)
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
                "call_id": call["call_id"],
                "result_payload_id": receipt.result_payload_id,
                "result_digest": receipt.result_digest,
                **{
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
            },
        )
