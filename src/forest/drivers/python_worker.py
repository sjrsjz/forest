"""Host-side native driver, compatible with Windows Selector event loops.

The loopback socket is an authenticated protocol boundary, not a malicious-code
sandbox. No database locator, cloud credential, or inherited environment is
provided to the child. A timed-out uncooperative process is killed and reaped.
"""

from __future__ import annotations

import asyncio
import hmac
import json
import os
import secrets
import signal
import subprocess
import sys
import tempfile
import time
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Awaitable, Callable
from uuid import uuid4

from forest.command_contract import schema_digest
from forest.drivers.protocol import MAX_FRAME_BYTES, FrameWriter, encode_frame, read_frame
from forest.errors import ForestError


def _error_details(error: dict) -> dict | None:
    """Only typed state-repair metadata may cross the private IPC boundary."""
    details = error.get("details")
    if error.get("code") != "KERNEL_CONTENT_REVOKED" or not isinstance(details, dict):
        return None
    revision, digest, state = (details.get(key) for key in (
        "current_revision", "value_digest", "payload_state"
    ))
    if (
        set(details) != {"current_revision", "value_digest", "payload_state"}
        or type(revision) is not int or not 1 <= revision <= 2**63 - 1
        or not isinstance(digest, str) or not digest.startswith("sha256:")
        or len(digest) != 71 or any(char not in "0123456789abcdef" for char in digest[7:])
        or state != "UNAVAILABLE"
    ):
        return None
    return {"current_revision": revision, "value_digest": digest, "payload_state": state}


def _error_envelope(error: ForestError) -> dict:
    value = {"code": error.code, "message": str(error), "status": error.status}
    details = _error_details({**value, "details": error.details})
    if details is not None:
        value["details"] = details
    return value


@dataclass(frozen=True)
class WorkerIdentity:
    instance_id: str
    generation_id: str
    incarnation: str
    bot_id: str
    graph_id: str | None = None
    graph_revision: int | None = None

    def __post_init__(self):
        if any(
            not isinstance(value, str) or not value or len(value) > 128
            for value in (
                self.instance_id,
                self.generation_id,
                self.incarnation,
                self.bot_id,
            )
        ) or (
            self.graph_id is not None
            and (not isinstance(self.graph_id, str) or not self.graph_id or len(self.graph_id) > 128)
        ) or (
            self.graph_revision is not None
            and (type(self.graph_revision) is not int or self.graph_revision < 1)
        ):
            raise ValueError("Worker identity fields must contain 1 to 128 characters")


@dataclass(frozen=True)
class WorkerLimits:
    max_frame_bytes: int = MAX_FRAME_BYTES
    max_inflight: int = 16
    max_queue_items: int = 64
    max_state_inflight: int = 16
    max_owned_tasks: int = 64
    max_owned_resources: int = 64

    def __post_init__(self):
        if type(self.max_frame_bytes) is not int or not 1024 <= self.max_frame_bytes <= MAX_FRAME_BYTES:
            raise ValueError("Frame limit must be 1024 to 262144 bytes")
        for value in (
            self.max_inflight,
            self.max_state_inflight,
            self.max_owned_tasks,
            self.max_owned_resources,
        ):
            if type(value) is not int or not 1 <= value <= 256:
                raise ValueError("Execution limits must be 1 to 256")
        if type(self.max_queue_items) is not int or not 0 <= self.max_queue_items <= 256:
            raise ValueError("Queue size must be 0 to 256")


StateHandler = Callable[[WorkerIdentity, str, str, dict], Awaitable[Any]]
ServiceHandler = Callable[[WorkerIdentity, str, dict], Awaitable[Any]]
EventHandler = Callable[[WorkerIdentity, str, dict], Awaitable[Any]]
JobHandler = Callable[[WorkerIdentity, str, dict], Awaitable[Any]]
ContributionDirectoryHandler = Callable[[WorkerIdentity, str, dict], Awaitable[Any]]
DomainHandler = Callable[[WorkerIdentity, str, dict], Awaitable[Any]]
UnitOfWorkHandler = Callable[[WorkerIdentity, str, dict], Awaitable[Any]]


class PythonWorker:
    def __init__(
        self,
        snapshot,
        identity: WorkerIdentity,
        state_handler: StateHandler | None = None,
        *,
        limits: WorkerLimits | None = None,
        python_executable: str = sys.executable,
        service_handler: ServiceHandler | None = None,
        domain_query_handler: DomainHandler | None = None,
        domain_command_handler: DomainHandler | None = None,
        unit_of_work_handler: UnitOfWorkHandler | None = None,
        event_handler: EventHandler | None = None,
        job_handler: JobHandler | None = None,
        contribution_directory_handler: ContributionDirectoryHandler | None = None,
    ):
        self.package = snapshot
        self.identity = identity
        self.state_handler = state_handler
        self.service_handler = service_handler
        self.domain_query_handler = domain_query_handler
        self.domain_command_handler = domain_command_handler
        self.unit_of_work_handler = unit_of_work_handler
        self.event_handler = event_handler
        self.job_handler = job_handler
        self.contribution_directory_handler = contribution_directory_handler
        self.limits = limits or WorkerLimits()
        self.python_executable = python_executable
        self.environment = None
        self._environment_task: asyncio.Task | None = None
        self.state = "NEW"
        self.services: list[str] = []
        self.subscriptions: list[str] = []
        self.jobs: list[str] = []
        self.business_commands: list[str] = []
        self.message_handlers: list[str] = []
        self.process: subprocess.Popen | None = None
        self._temporary: tempfile.TemporaryDirectory | None = None
        self._writer: FrameWriter | None = None
        self._reader_task: asyncio.Task | None = None
        self._server: asyncio.Server | None = None
        self._handshakes: set[asyncio.Task] = set()
        self._pending: dict[str, asyncio.Future] = {}
        self._calls: dict[str, str] = {}
        self._call_operations: dict[str, str] = {}
        self._call_roots: dict[str, tuple[str, str]] = {}
        self._call_deadlines: dict[str, float] = {}
        self._reserved_calls: set[str] = set()
        self._invocation_tasks: dict[str, asyncio.Task] = {}
        self._cancelled_calls: set[str] = set()
        self._closed_calls: set[str] = set()
        self._state_tasks: dict[str, asyncio.Task] = {}
        self._state_call_ids: dict[str, str] = {}
        self._worker_pid: int | None = None
        self._slots = asyncio.Semaphore(self.limits.max_inflight)
        self._admitted = 0
        self._terminating = asyncio.Lock()

    @property
    def pid(self) -> int | None:
        return self._worker_pid or (self.process.pid if self.process else None)

    @property
    def alive(self) -> bool:
        return self.process is not None and self.process.poll() is None

    def _expected_services(self) -> list[str]:
        components = self.package.manifest.get("components", [])
        if len(components) != 1:
            raise ForestError(
                "COMPONENT_COUNT_UNSUPPORTED", "P0 packages must contain exactly one component", 422
            )
        component = components[0]
        provided = component.get("provides", [])
        effect_ingress = [
            item["ingress"]["name"]
            for item in component.get("effect_services", [])
            if item.get("ingress") is not None
        ]
        names = [item["name"] for item in provided] + effect_ingress
        if len(names) != len(set(names)):
            raise ForestError(
                "SERVICE_DECLARATION_INVALID", "Manifest contains duplicate services", 422
            )
        return sorted(names)

    def _expected_subscriptions(self) -> list[dict]:
        subscriptions = self.package.manifest["components"][0].get("subscribes", [])
        identities = [item["subscriber_id"] for item in subscriptions]
        if len(identities) != len(set(identities)):
            raise ForestError(
                "SUBSCRIPTION_DECLARATION_INVALID",
                "Manifest contains duplicate subscriber identities",
                422,
            )
        return sorted(
            (
                {
                    "subscriber_id": item["subscriber_id"],
                    "topic": item["topic"],
                    "contract": item["contract"],
                }
                for item in subscriptions
            ),
            key=lambda item: item["subscriber_id"],
        )

    def _expected_jobs(self) -> list[str]:
        jobs = self.package.manifest["components"][0].get("jobs", [])
        job_types = self.package.manifest["components"][0].get("job_types", [])
        owned_work = self.package.manifest["components"][0].get(
            "owned_work_handlers", []
        )
        identities = [item["handler_id"] for item in jobs] + [
            item["handler"] for item in job_types
        ] + [
            item["handler"] for item in owned_work
        ]
        if len(identities) != len(set(identities)):
            raise ForestError(
                "JOB_DECLARATION_INVALID", "Manifest contains duplicate job handlers", 422
            )
        return sorted(identities)

    def _expected_commands(self) -> list[dict]:
        commands = self.package.manifest["components"][0].get("commands", [])
        identities = [item["command_id"] for item in commands]
        if len(identities) != len(set(identities)):
            raise ForestError(
                "COMMAND_DECLARATION_INVALID", "Manifest contains duplicate commands", 422
            )
        return sorted(
            (
                {
                    "command_id": item["command_id"],
                    "name": item["name"],
                    "handler": item["handler"],
                    "permission": item["permission"],
                    "argument_mode": item["argument_mode"],
                    "input_schema": item["input_schema"],
                    "schema_digest": schema_digest(
                        self.package.schema(item["input_schema"])
                    ),
                    "priority": item["priority"],
                }
                for item in commands
            ),
            key=lambda item: item["command_id"],
        )

    def _expected_message_handlers(self) -> list[dict]:
        handlers = self.package.manifest["components"][0].get("message_handlers", [])
        identities = [item["handler_id"] for item in handlers]
        if len(identities) != len(set(identities)):
            raise ForestError(
                "MESSAGE_HANDLER_DECLARATION_INVALID",
                "Manifest contains duplicate message handlers",
                422,
            )
        return sorted(
            (
                {
                    "handler_id": item["handler_id"],
                    "handler": item["handler"],
                    "priority": item["priority"],
                }
                for item in handlers
            ),
            key=lambda item: item["handler_id"],
        )

    async def launch(self, timeout: float = 10) -> None:
        if self.state != "NEW":
            raise ForestError("LIFECYCLE_INVALID", "Worker has already been launched")
        from forest.environments import EnvironmentCatalog

        if self.python_executable not in {sys.executable, getattr(sys, "_base_executable", "")}:
            raise ForestError(
                "DEPENDENCY_TARGET_MISMATCH", "Worker must use the verified deployment interpreter", 422
            )
        # Snapshot dependencies and the SDK never come from a mutable workspace
        # or the host venv. Materialization only copies already validated bytes.
        self.state = "MATERIALIZING"
        materialization = asyncio.create_task(
            asyncio.to_thread(
                EnvironmentCatalog(self.package.root.parent / "environments").materialize,
                self.package,
            ), name=f"forest-environment-{self.identity.instance_id}",
        )
        self._environment_task = materialization
        try:
            self.environment = await asyncio.shield(materialization)
        except BaseException:
            self.state = "STOPPED"
            # Cancelling to_thread only detaches the awaiting coroutine. Join
            # the owned builder before declaring launch/cleanup complete.
            await self._join_environment(materialization)
            raise
        finally:
            self._environment_task = None
        if self.state != "MATERIALIZING":
            raise ForestError("LIFECYCLE_INVALID", "Worker stopped while preparing its environment")
        expected_services = self._expected_services()
        token = secrets.token_urlsafe(32)
        connected = asyncio.get_running_loop().create_future()

        async def authenticate(reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
            accepted = False
            try:
                hello = await asyncio.wait_for(
                    read_frame(reader, self.limits.max_frame_bytes), timeout=3
                )
                supplied = hello.get("token")
                if (
                    hello.get("kind") != "hello"
                    or not isinstance(supplied, str)
                    or not hmac.compare_digest(supplied, token)
                    or hello.get("identity") != asdict(self.identity)
                    or connected.done()
                ):
                    return
                connected.set_result((reader, writer))
                self._worker_pid = hello.get("pid")
                accepted = True
            except (Exception, asyncio.CancelledError):
                pass
            finally:
                if not accepted:
                    writer.close()
                    await writer.wait_closed()

        def accept(reader, writer):
            if len(self._handshakes) >= 4 or connected.done():
                writer.close()
                return
            task = asyncio.create_task(authenticate(reader, writer))
            self._handshakes.add(task)
            task.add_done_callback(self._handshakes.discard)

        try:
            self._server = await asyncio.start_server(
                accept, host="127.0.0.1", port=0, backlog=4, limit=self.limits.max_frame_bytes + 4
            )
            port = self._server.sockets[0].getsockname()[1]
            self._temporary = tempfile.TemporaryDirectory(prefix="forest-worker-")
            Path(self._temporary.name).chmod(0o700)
            environment = {
                name: os.environ[name]
                for name in ("SystemRoot", "WINDIR", "COMSPEC", "LANG", "LC_ALL")
                if name in os.environ
            }
            environment.update(
                PATH=os.defpath,
                HOME=self._temporary.name,
                USERPROFILE=self._temporary.name,
                TEMP=self._temporary.name,
                TMP=self._temporary.name,
                TMPDIR=self._temporary.name,
                PYTHONUNBUFFERED="1",
                PYTHONDONTWRITEBYTECODE="1",
                PYTHONIOENCODING="utf-8",
            )
            executable = self.environment.python_executable
            bootstrap_code = (
                "import sys,runpy;sys.path[:0]=sys.argv[1:];"
                "runpy.run_module('forest.drivers.worker',run_name='__main__')"
            )
            self.process = subprocess.Popen(
                [executable, "-I", "-S", "-B", "-c", bootstrap_code, *self.environment.search_paths],
                stdin=subprocess.PIPE,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                cwd=self._temporary.name,
                env=environment,
                close_fds=True,
                creationflags=subprocess.CREATE_NO_WINDOW if os.name == "nt" else 0,
                start_new_session=os.name != "nt",
            )
            bootstrap = {
                "port": port,
                "token": token,
                "identity": asdict(self.identity),
                "entrypoint": self.package.entrypoint,
                "module_root": str(self.package.module_root),
                "package_digest": self.package.digest,
                "services": expected_services,
                "subscriptions": self._expected_subscriptions(),
                "jobs": self._expected_jobs(),
                "commands": self._expected_commands(),
                "message_handlers": self._expected_message_handlers(),
                "limits": asdict(self.limits),
            }
            encoded = json.dumps(bootstrap, ensure_ascii=False).encode("utf-8") + b"\n"
            if len(encoded) > MAX_FRAME_BYTES:
                raise ForestError(
                    "IPC_FRAME_TOO_LARGE", "Worker bootstrap exceeds protocol limit", 413
                )

            def deliver_bootstrap():
                self.process.stdin.write(encoded)
                self.process.stdin.flush()
                self.process.stdin.close()

            async with asyncio.timeout(timeout):
                await asyncio.to_thread(deliver_bootstrap)
                reader, writer = await connected
                self._writer = FrameWriter(writer, self.limits.max_frame_bytes)
                await self._writer.send({"kind": "hello_ack"})
            self._server.close()
            await self._server.wait_closed()
            self._server = None
            self.state = "CONNECTED"
            self._reader_task = asyncio.create_task(
                self._read_loop(reader), name=f"forest-ipc-{self.identity.instance_id}"
            )
        except BaseException:
            await self.terminate()
            raise

    @staticmethod
    async def _join_environment(task: asyncio.Task) -> None:
        while not task.done():
            try:
                await asyncio.shield(task)
            except asyncio.CancelledError:
                continue
            except Exception:
                break
        if not task.cancelled():
            task.exception()  # Retrieve a failed builder even when the caller was cancelled.

    async def _read_loop(self, reader: asyncio.StreamReader) -> None:
        try:
            while True:
                message = await read_frame(reader, self.limits.max_frame_bytes)
                if message.get("kind") == "result":
                    for call_id, request_id in self._calls.items():
                        if request_id == message.get("id"):
                            self._closed_calls.add(call_id)
                            break
                    future = self._pending.get(message.get("id"))
                    if future is None or future.done():
                        continue
                    if message.get("ok") is True:
                        future.set_result(message.get("value"))
                    else:
                        error = message.get("error", {})
                        future.set_exception(
                            ForestError(
                                str(error.get("code", "WORKER_FAILED")),
                                str(error.get("message", "Worker operation failed")),
                                int(error.get("status", 500)),
                                details=_error_details(error),
                            )
                        )
                elif message.get("kind") in {
                    "state",
                    "service",
                    "domain_query",
                    "domain_command",
                    "unit_of_work",
                    "event",
                    "job",
                    "contribution_directory",
                }:
                    request_id = message.get("id")
                    if (
                        not isinstance(request_id, str)
                        or len(request_id) > 128
                        or request_id in self._state_tasks
                    ):
                        raise ForestError(
                            "IPC_FRAME_INVALID",
                            "Invalid or duplicate state request identifier",
                            400,
                        )
                    if len(self._state_tasks) >= self.limits.max_state_inflight:
                        await self._writer.send(
                            {
                                "kind": message["kind"] + "_result",
                                "id": request_id,
                                "ok": False,
                                "error": {
                                    "code": "OVERLOADED",
                                    "message": "Host RPC gateway capacity exhausted",
                                    "status": 429,
                                },
                            }
                        )
                        continue
                    task = asyncio.create_task(self._handle_state(message))
                    self._state_tasks[request_id] = task
                    self._state_call_ids[request_id] = message.get("call_id")
                    task.add_done_callback(lambda done, key=request_id: self._state_done(key))
                else:
                    raise ForestError("IPC_FRAME_INVALID", "Unexpected worker frame kind", 400)
        except asyncio.CancelledError:
            raise
        except Exception:
            self.state = "FAILED"
            self._fail_pending()
            # Reap workers that close IPC but leave their event loop alive.
            asyncio.create_task(self.terminate())

    def _state_done(self, request_id: str) -> None:
        task = self._state_tasks.pop(request_id, None)
        self._state_call_ids.pop(request_id, None)
        if task is not None and not task.cancelled() and task.exception() is not None:
            self._fail_pending()

    async def _close_state_calls(self, call_id: str) -> None:
        tasks = [
            task
            for request_id, task in self._state_tasks.items()
            if self._state_call_ids.get(request_id) == call_id
        ]
        for task in tasks:
            task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)

    async def _handle_state(self, message: dict) -> None:
        kind = message["kind"]
        result = {"kind": kind + "_result", "id": message["id"]}
        try:
            call_id, action, payload = (
                message.get("call_id"),
                message.get("action"),
                message.get("payload"),
            )
            if (
                call_id not in self._calls
                or call_id in self._cancelled_calls
                or call_id in self._closed_calls
                or self.state not in {"ACTIVE", "QUIESCING"}
            ):
                raise ForestError(
                    "CALL_CONTEXT_REQUIRED",
                    "State request has no active host-owned invocation",
                    403,
                )
            if kind == "service":
                expected = {"service", "payload", "contract_major", "operation_id", "timeout"}
                if not isinstance(payload, dict) or payload.keys() != expected:
                    raise ForestError(
                        "SERVICE_REQUEST_INVALID",
                        "Service request contains unsupported fields",
                        422,
                    )
                if self.service_handler is None:
                    raise ForestError("CAPABILITY_DENIED", "No service gateway is available", 403)
                value = await self.service_handler(self.identity, call_id, payload)
            elif kind == "domain_query":
                expected = {"name", "payload", "contract_major", "timeout"}
                if not isinstance(payload, dict) or payload.keys() != expected:
                    raise ForestError(
                        "DOMAIN_QUERY_REQUEST_INVALID",
                        "Domain query request contains unsupported fields",
                        422,
                    )
                if self.domain_query_handler is None:
                    raise ForestError(
                        "CAPABILITY_DENIED", "No domain query gateway is available", 403
                    )
                value = await self.domain_query_handler(self.identity, call_id, payload)
            elif kind == "domain_command":
                expected = {
                    "name",
                    "payload",
                    "contract_major",
                    "operation_id",
                    "timeout",
                }
                if not isinstance(payload, dict) or payload.keys() != expected:
                    raise ForestError(
                        "DOMAIN_COMMAND_REQUEST_INVALID",
                        "Domain command request contains unsupported fields",
                        422,
                    )
                if self.domain_command_handler is None:
                    raise ForestError(
                        "CAPABILITY_DENIED", "No domain command gateway is available", 403
                    )
                value = await self.domain_command_handler(self.identity, call_id, payload)
            elif kind == "unit_of_work":
                expected = {
                    "name",
                    "payload",
                    "contract_major",
                    "operation_id",
                    "timeout",
                }
                if not isinstance(payload, dict) or payload.keys() != expected:
                    raise ForestError(
                        "UNIT_OF_WORK_REQUEST_INVALID",
                        "UnitOfWork request contains unsupported fields",
                        422,
                    )
                if self.unit_of_work_handler is None:
                    raise ForestError(
                        "CAPABILITY_DENIED", "No UnitOfWork gateway is available", 403
                    )
                value = await self.unit_of_work_handler(self.identity, call_id, payload)
            elif kind == "event":
                expected = {"topic", "payload", "contract", "operation_id"}
                if not isinstance(payload, dict) or payload.keys() != expected:
                    raise ForestError(
                        "EVENT_REQUEST_INVALID",
                        "Event request contains unsupported fields",
                        422,
                    )
                if self.event_handler is None:
                    raise ForestError("CAPABILITY_DENIED", "No event gateway is available", 403)
                value = await self.event_handler(self.identity, call_id, payload)
            elif kind == "job":
                expected = {"handler_id", "payload", "operation_id", "delay_seconds"}
                if not isinstance(payload, dict) or payload.keys() != expected:
                    raise ForestError(
                        "JOB_REQUEST_INVALID", "Job request contains unsupported fields", 422
                    )
                if self.job_handler is None:
                    raise ForestError("CAPABILITY_DENIED", "No job gateway is available", 403)
                value = await self.job_handler(self.identity, call_id, payload)
            elif kind == "contribution_directory":
                if not isinstance(payload, dict):
                    raise ForestError(
                        "CONTRIBUTION_DIRECTORY_REQUEST_INVALID",
                        "Contribution directory request must be an object",
                        422,
                    )
                if self.contribution_directory_handler is None:
                    raise ForestError(
                        "CAPABILITY_DENIED",
                        "No contribution directory gateway is available",
                        403,
                    )
                value = await self.contribution_directory_handler(
                    self.identity, call_id, payload
                )
            else:
                expected = (
                    {"key"}
                    if action == "get"
                    else {"key", "value", "expected_revision", "operation_id"}
                    if action == "commit"
                    else None
                )
                if expected is None or not isinstance(payload, dict) or payload.keys() != expected:
                    raise ForestError(
                        "STATE_REQUEST_INVALID", "State request contains unsupported fields", 422
                    )
                if not isinstance(payload["key"], str) or not 1 <= len(payload["key"]) <= 128:
                    raise ForestError(
                        "STATE_REQUEST_INVALID", "State key must contain 1 to 128 characters", 422
                    )
                if self.state_handler is None:
                    raise ForestError(
                        "CAPABILITY_DENIED", "This worker has no state capability", 403
                    )
                value = await self.state_handler(self.identity, call_id, action, payload)
            result.update(ok=True, value=value)
        except ForestError as error:
            result.update(ok=False, error=_error_envelope(error))
        except Exception:
            result.update(
                ok=False,
                error={
                    "code": "HOST_STATE_FAILED",
                    "message": "Host state request failed",
                    "status": 500,
                },
            )
        try:
            await self._writer.send(result)
        except ForestError:
            await self._writer.send(
                {
                    "kind": kind + "_result",
                    "id": message["id"],
                    "ok": False,
                    "error": {
                        "code": (
                            "STATE_RESULT_INVALID"
                            if kind == "state"
                            else "SERVICE_RESULT_INVALID"
                            if kind == "service"
                            else "DOMAIN_QUERY_RESULT_INVALID"
                            if kind == "domain_query"
                            else "DOMAIN_COMMAND_RESULT_INVALID"
                            if kind == "domain_command"
                            else "UNIT_OF_WORK_RESULT_INVALID"
                            if kind == "unit_of_work"
                            else "EVENT_RESULT_INVALID"
                            if kind == "event"
                            else "JOB_RESULT_INVALID"
                        ),
                        "message": "Host RPC result exceeds JSON protocol limits",
                        "status": 413,
                    },
                }
            )
        except Exception:
            self._fail_pending()

    def _fail_pending(self) -> None:
        for future in self._pending.values():
            if not future.done():
                future.set_exception(
                    ForestError(
                        "WORKER_DISCONNECTED", "Worker process or IPC connection was lost", 503
                    )
                )

    async def _request(
        self, command: str, payload: dict, *, timeout: float, request_id: str | None = None
    ) -> Any:
        if self._writer is None or not self.alive:
            raise ForestError("WORKER_DISCONNECTED", "Worker is not connected", 503)
        if len(self._pending) >= self.limits.max_inflight + 8:
            raise ForestError("OVERLOADED", "Worker control request capacity exhausted", 429)
        request_id = request_id or str(uuid4())
        future = asyncio.get_running_loop().create_future()
        self._pending[request_id] = future
        try:
            await self._writer.send(
                {
                    "kind": "command",
                    "id": request_id,
                    "command": command,
                    "deadline_at": time.time() + timeout,
                    "payload": payload,
                }
            )
            return await asyncio.wait_for(asyncio.shield(future), timeout=timeout)
        finally:
            self._pending.pop(request_id, None)
            if not future.done():
                future.cancel()
            elif not future.cancelled():
                future.exception()

    async def _lifecycle(
        self, command: str, payload: dict | None = None, *, timeout: float = 10
    ) -> Any:
        try:
            return await self._request(command, payload or {}, timeout=timeout)
        except (asyncio.TimeoutError, asyncio.CancelledError):
            await self.terminate()
            raise

    async def prepare(self, config: dict, restored_state: Any = None, timeout: float = 10) -> dict:
        if self.state != "CONNECTED":
            raise ForestError("LIFECYCLE_INVALID", "Prepare requires a connected candidate")
        try:
            result = await self._lifecycle(
                "prepare", {"config": config, "restored_state": restored_state}, timeout=timeout
            )
            self.services = result["services"]
            self.subscriptions = result["subscriptions"]
            self.jobs = result["jobs"]
            self.business_commands = result["commands"]
            self.message_handlers = result["message_handlers"]
            self.state = "PREPARED"
            return result
        except BaseException:
            await self.terminate()
            raise

    async def start(self, timeout: float = 10) -> Any:
        result = await self._lifecycle("start", timeout=timeout)
        self.state = "READY"
        return result

    async def health(self, timeout: float = 10) -> dict:
        return await self._lifecycle("health", timeout=timeout)

    async def activate(self, timeout: float = 10) -> None:
        await self._lifecycle("activate", timeout=timeout)
        self.state = "ACTIVE"

    async def invoke(
        self,
        service: str,
        payload: Any,
        *,
        call_id: str | None = None,
        operation_id: str | None = None,
        causal_root_kind: str | None = None,
        causal_root_id: str | None = None,
        timeout: float = 30,
        before_send: Callable[[], Awaitable[None]] | None = None,
    ) -> Any:
        return await self._invoke(
            "invoke", "service", service, payload,
            call_id=call_id, operation_id=operation_id,
            causal_root_kind=causal_root_kind, causal_root_id=causal_root_id, timeout=timeout,
            before_send=before_send,
        )

    async def deliver_event(
        self,
        subscriber_id: str,
        event: dict,
        *,
        call_id: str,
        operation_id: str,
        causal_root_kind: str | None = None,
        causal_root_id: str | None = None,
        timeout: float = 30,
        before_send: Callable[[], Awaitable[None]] | None = None,
    ) -> Any:
        return await self._invoke(
            "invoke_event", "subscriber_id", subscriber_id, event,
            call_id=call_id, operation_id=operation_id,
            causal_root_kind=causal_root_kind, causal_root_id=causal_root_id, timeout=timeout,
            before_send=before_send,
        )

    async def deliver_job(
        self,
        handler_id: str,
        task: dict,
        *,
        call_id: str,
        operation_id: str,
        causal_root_kind: str | None = None,
        causal_root_id: str | None = None,
        timeout: float = 30,
        before_send: Callable[[], Awaitable[None]] | None = None,
    ) -> Any:
        return await self._invoke(
            "invoke_job", "handler_id", handler_id, task,
            call_id=call_id, operation_id=operation_id,
            causal_root_kind=causal_root_kind, causal_root_id=causal_root_id, timeout=timeout,
            before_send=before_send,
        )

    async def invoke_command(
        self,
        command_id: str,
        event: dict,
        *,
        call_id: str,
        operation_id: str,
        causal_root_kind: str | None = None,
        causal_root_id: str | None = None,
        timeout: float = 30,
        before_send: Callable[[], Awaitable[None]] | None = None,
    ) -> Any:
        return await self._invoke(
            "invoke_command",
            "command_id",
            command_id,
            event,
            call_id=call_id,
            operation_id=operation_id,
            causal_root_kind=causal_root_kind,
            causal_root_id=causal_root_id,
            timeout=timeout,
            before_send=before_send,
        )

    async def invoke_message_handler(
        self,
        handler_id: str,
        event: dict,
        *,
        call_id: str,
        operation_id: str,
        causal_root_kind: str | None = None,
        causal_root_id: str | None = None,
        timeout: float = 30,
        before_send: Callable[[], Awaitable[None]] | None = None,
    ) -> Any:
        return await self._invoke(
            "invoke_message_handler",
            "handler_id",
            handler_id,
            event,
            call_id=call_id,
            operation_id=operation_id,
            causal_root_kind=causal_root_kind,
            causal_root_id=causal_root_id,
            timeout=timeout,
            before_send=before_send,
        )

    async def _invoke(
        self,
        command: str,
        target_key: str,
        target: str,
        payload: Any,
        *,
        call_id: str | None,
        operation_id: str | None,
        causal_root_kind: str | None,
        causal_root_id: str | None,
        timeout: float,
        before_send: Callable[[], Awaitable[None]] | None,
    ) -> Any:
        if self.state != "ACTIVE":
            raise ForestError(
                "WORKER_NOT_ACTIVE", "Candidate or quiescing worker has no business admission", 409
            )
        if self._admitted >= self.limits.max_inflight + self.limits.max_queue_items:
            raise ForestError("OVERLOADED", "Worker bounded invocation queue is full", 429)
        available = {
            "invoke": self.services,
            "invoke_event": self.subscriptions,
            "invoke_job": self.jobs,
            "invoke_command": self.business_commands,
            "invoke_message_handler": self.message_handlers,
        }[command]
        if target not in available:
            raise ForestError(
                "SERVICE_NOT_FOUND"
                if command == "invoke"
                else "SUBSCRIPTION_NOT_FOUND"
                if command == "invoke_event"
                else "JOB_HANDLER_NOT_FOUND"
                if command == "invoke_job"
                else "BUSINESS_COMMAND_NOT_FOUND"
                if command == "invoke_command"
                else "MESSAGE_HANDLER_NOT_FOUND",
                "Invocation target is not registered by this worker",
                404,
            )
        call_id = call_id or str(uuid4())
        operation_id = operation_id or call_id
        causal_root_kind = causal_root_kind or "worker.operation"
        causal_root_id = causal_root_id or operation_id
        if (
            not isinstance(call_id, str)
            or not 1 <= len(call_id) <= 128
            or call_id in self._reserved_calls
        ):
            raise ForestError(
                "CALL_ID_CONFLICT", "Invocation identifier is invalid or already active", 409
            )
        if not isinstance(operation_id, str) or not 1 <= len(operation_id) <= 256:
            raise ForestError(
                "OPERATION_ID_INVALID",
                "Business operation identifier must contain 1 to 256 characters",
                422,
            )
        if (
            not isinstance(causal_root_kind, str)
            or not 1 <= len(causal_root_kind) <= 128
            or not isinstance(causal_root_id, str)
            or not 1 <= len(causal_root_id) <= 256
        ):
            raise ForestError(
                "CAUSAL_ROOT_INVALID",
                "Invocation causal root identity is invalid",
                422,
            )
        request_id = str(uuid4())
        # Validate before waiting for capacity, without retaining unbounded payloads.
        encoded = encode_frame({"wire_version": 1, "payload": payload}, self.limits.max_frame_bytes)
        payload = json.loads(encoded[4:])["payload"]
        self._admitted += 1
        self._reserved_calls.add(call_id)
        self._invocation_tasks[call_id] = asyncio.current_task()
        deadline = time.monotonic() + timeout
        self._call_operations[call_id] = operation_id
        self._call_roots[call_id] = (causal_root_kind, causal_root_id)
        self._call_deadlines[call_id] = deadline
        sent = False
        try:
            async with asyncio.timeout(timeout):
                async with self._slots:
                    if self.state != "ACTIVE":
                        raise ForestError(
                            "WORKER_NOT_ACTIVE",
                            "Worker stopped business admission while queued",
                            409,
                        )
                    if before_send is not None:
                        await before_send()
                    # The callback may wait on a database fence while this
                    # Worker is concurrently stopped. It never grants transport
                    # admission after local retirement or deadline expiry.
                    if self.state != "ACTIVE":
                        raise ForestError(
                            "WORKER_NOT_ACTIVE", "Worker stopped before invocation dispatch", 409
                        )
                    self._calls[call_id] = request_id
                    sent = True
                    return await self._request(
                        command,
                        {
                            target_key: target,
                            "payload": payload,
                            "call_id": call_id,
                            "operation_id": operation_id,
                        },
                        timeout=max(0.001, deadline - time.monotonic()),
                        request_id=request_id,
                    )
        except asyncio.TimeoutError:
            if sent:
                await self.cancel(call_id)
            raise ForestError(
                "DEADLINE_EXCEEDED", "Worker invocation exceeded its deadline", 504
            ) from None
        except asyncio.CancelledError:
            requested_by_host = call_id in self._cancelled_calls
            if sent:
                await self.cancel(call_id)
            if requested_by_host:
                raise ForestError(
                    "CALL_CANCELLED", "Queued worker call was cancelled", 409
                ) from None
            raise
        finally:
            await self._close_state_calls(call_id)
            self._calls.pop(call_id, None)
            self._call_operations.pop(call_id, None)
            self._call_roots.pop(call_id, None)
            self._call_deadlines.pop(call_id, None)
            self._reserved_calls.discard(call_id)
            self._invocation_tasks.pop(call_id, None)
            self._cancelled_calls.discard(call_id)
            self._closed_calls.discard(call_id)
            self._admitted -= 1

    def call_operation_id(self, call_id: str) -> str:
        operation_id = self._call_operations.get(call_id)
        if call_id not in self._calls or operation_id is None:
            raise ForestError(
                "CALL_CONTEXT_REQUIRED",
                "Capability request has no live parent invocation",
                403,
            )
        return operation_id

    def call_causal_root(self, call_id: str) -> tuple[str, str]:
        root = self._call_roots.get(call_id)
        if call_id not in self._calls or root is None:
            raise ForestError(
                "CALL_CONTEXT_REQUIRED",
                "Capability request has no live causal root",
                403,
            )
        return root

    def remaining_timeout(
        self, call_id: str, requested: float | None = None
    ) -> float:
        deadline = self._call_deadlines.get(call_id)
        if deadline is None:
            raise ForestError(
                "CALL_CONTEXT_REQUIRED",
                "Nested service request has no live parent deadline",
                403,
            )
        if requested is not None and (
            isinstance(requested, bool)
            or not isinstance(requested, (int, float))
            or not 0 < requested <= 86_400
        ):
            raise ForestError(
                "INVALID_SERVICE_REQUEST", "Nested service timeout is invalid", 422
            )
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise ForestError(
                "DEADLINE_EXCEEDED", "Parent service invocation deadline expired", 504
            )
        return min(remaining, float(requested)) if requested is not None else remaining

    async def cancel(self, call_id: str, timeout: float = 0.5) -> None:
        task = self._invocation_tasks.get(call_id)
        if task is None:
            return
        self._cancelled_calls.add(call_id)
        if call_id not in self._calls:
            task.cancel()
            if task is not asyncio.current_task():
                await asyncio.gather(task, return_exceptions=True)
            return
        try:
            # The worker acknowledges only after the invocation and its children
            # have exited. An uncooperative coroutine therefore causes a kill.
            await self._request("cancel", {"call_id": call_id}, timeout=timeout)
        except (Exception, asyncio.CancelledError):
            await self.terminate()
        if task is not asyncio.current_task():
            await asyncio.gather(task, return_exceptions=True)

    async def quiesce(self, reason: str = "reload", timeout: float = 10) -> Any:
        if self.state != "ACTIVE":
            raise ForestError("LIFECYCLE_INVALID", "Only an active worker can quiesce")
        self.state = "QUIESCING"
        result = await self._lifecycle("quiesce", {"reason": reason}, timeout=timeout)
        self.state = "QUIESCENT"
        return result

    async def snapshot(self, timeout: float = 10) -> Any:
        return await self._lifecycle("snapshot", timeout=timeout)

    async def restore(self, state: Any, timeout: float = 10) -> Any:
        return await self._lifecycle("restore", {"state": state}, timeout=timeout)

    async def migrate(
        self, state: dict, *, from_schema: int, to_schema: int, timeout: float = 10
    ) -> dict:
        return await self._lifecycle(
            "migrate", {"state": state, "from_schema": from_schema, "to_schema": to_schema},
            timeout=timeout,
        )

    async def resume(self, timeout: float = 10) -> Any:
        result = await self._lifecycle("resume", timeout=timeout)
        self.state = "ACTIVE"
        return result

    async def stop(self, reason: str = "shutdown", timeout: float = 5) -> None:
        if self.state == "STOPPED":
            return
        self.state = "STOPPING"
        try:
            if self.alive and self._writer:
                await self._request("stop", {"reason": reason}, timeout=timeout)
        except (Exception, asyncio.CancelledError):
            pass
        finally:
            await self.terminate()

    async def terminate(self) -> None:
        async with self._terminating:
            self.state = "STOPPED"
            if self._environment_task:
                await self._join_environment(self._environment_task)
            if self._server:
                self._server.close()
                await self._server.wait_closed()
                self._server = None
            for task in list(self._handshakes):
                task.cancel()
            if self._handshakes:
                await asyncio.gather(*self._handshakes, return_exceptions=True)
            if self.process and self.process.poll() is None:
                if os.name == "nt":
                    # Windows virtualenv launchers may own a second interpreter.
                    # The live Popen handle identifies this exact process tree.
                    taskkill = str(Path(os.environ["SystemRoot"]) / "System32/taskkill.exe")
                    await asyncio.to_thread(
                        subprocess.run,
                        [taskkill, "/PID", str(self.process.pid), "/T", "/F"],
                        stdout=subprocess.DEVNULL,
                        stderr=subprocess.DEVNULL,
                        timeout=5,
                        creationflags=subprocess.CREATE_NO_WINDOW,
                    )
                    if self.process.poll() is None:
                        self.process.kill()
                else:
                    try:
                        os.killpg(self.process.pid, signal.SIGKILL)
                    except ProcessLookupError:
                        pass
            if self.process:
                await asyncio.to_thread(self.process.wait, timeout=5)
                if self.process.stdin and not self.process.stdin.closed:
                    self.process.stdin.close()
            if self._writer:
                self._writer.writer.close()
                try:
                    await self._writer.writer.wait_closed()
                except (OSError, ConnectionError):
                    pass
                self._writer = None
            if self._reader_task and self._reader_task is not asyncio.current_task():
                self._reader_task.cancel()
                await asyncio.gather(self._reader_task, return_exceptions=True)
                self._reader_task = None
            for task in list(self._state_tasks.values()):
                task.cancel()
            if self._state_tasks:
                await asyncio.gather(*self._state_tasks.values(), return_exceptions=True)
            self._fail_pending()
            if self._temporary:
                await asyncio.to_thread(self._temporary.cleanup)
                self._temporary = None
