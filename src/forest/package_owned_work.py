"""Generation-bound supervisor for package-owned durable work."""

from __future__ import annotations

import asyncio
import hashlib
from collections.abc import Callable
from typing import Any
from uuid import uuid4

from forest.errors import ForestError
from forest.package_capabilities import (
    PackageCallScope,
    PackageOwnedWorkAuthority,
    PackageServiceError,
    PackageServiceOperationLedger,
    PackageServiceRoute,
)
from forest.runtime_contracts import RuntimeServiceInvocation


class PackageOwnedWorkSupervisor:
    """Run package-declared claim/Worker/settle loops without knowing queue state."""

    def __init__(
        self,
        *,
        bot_id: str,
        component_id: str,
        declarations: tuple[dict, ...],
        schemas: dict,
        authority: PackageOwnedWorkAuthority,
        operation_ledger: PackageServiceOperationLedger,
        binding: Callable[[], tuple[Any, Any, Any]],
        generation_member_verifier: Callable[..., Any],
        active: Callable[[], bool],
        failed: Callable[[str], None],
    ) -> None:
        self.bot_id = bot_id
        self.component_id = component_id
        self.declarations = declarations
        self.schemas = schemas
        self.authority = authority
        self.operation_ledger = operation_ledger
        self.binding = binding
        self.generation_member_verifier = generation_member_verifier
        self.active = active
        self.failed = failed
        self.inflight = 0
        self.idle = asyncio.Event()
        self.idle.set()
        self.admission = asyncio.Event()
        self.wake = asyncio.Event()
        self.tasks: list[asyncio.Task] = []
        self.closing = False
        self.failure: str | None = None

    def notify(self) -> None:
        self.wake.set()

    def owned_tasks(self) -> tuple[asyncio.Task, ...]:
        return tuple(self.tasks)

    def freeze_admission(self) -> None:
        self.admission.clear()
        self.wake.set()

    async def drain(self) -> None:
        await self.idle.wait()

    def open_admission(self) -> None:
        if not self.active() or self.failure is not None:
            raise ForestError(
                "RUNTIME_OWNED_ADMISSION_INVALID",
                "Package owned work cannot open outside a healthy active Generation",
                409,
            )
        self.admission.set()
        self.wake.set()

    def _scope(
        self,
        operation_id: str,
        *,
        causal_root_kind: str = "package.owned-work",
        causal_root_id: str | None = None,
    ) -> PackageCallScope:
        identity, prepared, _worker = self.binding()
        return PackageCallScope(
            bot_id=self.bot_id,
            component_id=self.component_id,
            generation_id=identity.generation_id,
            instance_id=prepared.instance_id,
            worker_incarnation=prepared.worker_incarnation,
            call_id="call:" + uuid4().hex,
            call_operation_id=operation_id,
            causal_root_kind=causal_root_kind,
            causal_root_id=causal_root_id or operation_id,
        )

    @staticmethod
    def _claim_envelope(value: object, input_validator) -> dict:
        required = {
            "protocol",
            "work_id",
            "claim_token",
            "attempt",
            "operation_id",
            "causal_root_kind",
            "causal_root_id",
            "payload",
        }
        if (
            not isinstance(value, dict)
            or set(value) != required
            or value.get("protocol") != "forest.owned-work-claim/1"
            or not isinstance(value.get("work_id"), str)
            or not 1 <= len(value["work_id"]) <= 256
            or not isinstance(value.get("claim_token"), str)
            or not 1 <= len(value["claim_token"]) <= 256
            or type(value.get("attempt")) is not int
            or not 1 <= value["attempt"] <= 2_147_483_647
            or not isinstance(value.get("payload"), dict)
            or not input_validator.is_valid(value["payload"])
        ):
            raise ForestError(
                "OWNED_WORK_CLAIM_INVALID",
                "Package claim function returned an invalid work envelope",
                502,
            )
        try:
            RuntimeServiceInvocation(
                operation_id=value["operation_id"],
                timeout_seconds=1,
                causal_root_kind=value["causal_root_kind"],
                causal_root_id=value["causal_root_id"],
            )
        except ValueError as exc:
            raise ForestError(
                "OWNED_WORK_CLAIM_INVALID",
                "Package claim function returned invalid operation identity",
                502,
            ) from exc
        return value

    async def _claim(self, declaration: dict) -> dict | None:
        claim_token = "claim:" + uuid4().hex
        value = await self.authority.invoke_owned_work(
            "claim",
            self._scope(claim_token),
            declaration,
            {
                "protocol": "forest.owned-work-claim-request/1",
                "claim_token": claim_token,
                "lease_seconds": declaration["lease_seconds"],
            },
            timeout_seconds=declaration["claim_timeout_s"],
        )
        if value is None:
            return None
        input_validator, _output_validator = self.schemas[declaration["name"]]
        return self._claim_envelope(value, input_validator)

    async def _settle(self, declaration: dict, claim: dict, outcome: dict) -> None:
        operation_id = "owned-settle:" + hashlib.sha256(
            claim["claim_token"].encode("utf-8")
        ).hexdigest()
        result = await self.authority.invoke_owned_work(
            "settle",
            self._scope(
                operation_id,
                causal_root_kind=claim["causal_root_kind"],
                causal_root_id=claim["causal_root_id"],
            ),
            declaration,
            {
                "protocol": "forest.owned-work-settle-request/1",
                "work_id": claim["work_id"],
                "claim_token": claim["claim_token"],
                "attempt": claim["attempt"],
                "outcome": outcome,
            },
            timeout_seconds=declaration["settle_timeout_s"],
        )
        if result != {
            "protocol": "forest.owned-work-settled/1",
            "work_id": claim["work_id"],
        }:
            raise ForestError(
                "OWNED_WORK_SETTLEMENT_INVALID",
                "Package settle function returned an invalid acknowledgement",
                502,
            )

    async def _execute(self, declaration: dict, claim: dict) -> None:
        identity, _prepared, worker = self.binding()
        scope = self._scope(
            claim["operation_id"],
            causal_root_kind=claim["causal_root_kind"],
            causal_root_id=claim["causal_root_id"],
        )
        route = PackageServiceRoute(
            service_id="owned-work." + declaration["name"],
            graph_id=identity.graph_id,
            graph_revision=identity.graph_revision,
            admission_lane="DATA",
            owned_work=True,
            parent_call_id=None,
            timeout_seconds=declaration["handler_timeout_s"],
        )
        operation_input = {
            "work_id": claim["work_id"],
            "payload": claim["payload"],
        }
        stage = "before"
        try:
            admission = await self.operation_ledger.begin(scope, route, operation_input)
            if admission.execute:
                stage = "dispatch"
                result = await worker.deliver_job(
                    declaration["handler"],
                    {
                        "protocol": "forest.owned-work-task/1",
                        "work_id": claim["work_id"],
                        "attempt": claim["attempt"],
                        "payload": claim["payload"],
                    },
                    call_id=scope.call_id,
                    operation_id=scope.call_operation_id,
                    causal_root_kind=claim["causal_root_kind"],
                    causal_root_id=claim["causal_root_id"],
                    timeout=declaration["handler_timeout_s"],
                    before_send=lambda: self.generation_member_verifier(
                        component_id=self.component_id,
                        generation_id=identity.generation_id,
                    ),
                )
                _input_validator, output_validator = self.schemas[declaration["name"]]
                if not output_validator.is_valid(result):
                    raise ForestError(
                        "OWNED_WORK_OUTPUT_INVALID",
                        "Package owned work result violates its immutable schema",
                        502,
                    )
                stage = "ledger-commit"
                await self.operation_ledger.succeed(scope, route, result)
                stage = "done"
            elif admission.error is not None:
                stage = "replay-failure"
                raise ForestError(
                    admission.error.code,
                    admission.error.message,
                    admission.error.status,
                    details=admission.error.details,
                )
            else:
                result = admission.result
                _input_validator, output_validator = self.schemas[declaration["name"]]
                if not output_validator.is_valid(result):
                    raise ForestError(
                        "SERVICE_OPERATION_CORRUPT",
                        "Stored owned work result violates its package contract",
                        500,
                    )
            outcome = {"status": "SUCCEEDED", "result": result}
        except asyncio.CancelledError:
            raise
        except ForestError as error:
            if stage in {"before", "ledger-commit"}:
                raise
            if stage == "dispatch":
                await self.operation_ledger.fail(
                    scope,
                    route,
                    PackageServiceError(
                        error.code,
                        error.message,
                        error.status,
                        error.details if isinstance(error.details, dict) else None,
                    ),
                )
            outcome = {
                "status": "FAILED",
                "error": {
                    "code": error.code,
                    "status": error.status,
                    "retryable": error.status >= 500,
                    "unknown": error.code
                    in {"CALL_TIMEOUT", "SERVICE_OPERATION_UNKNOWN"},
                },
            }
        except Exception:
            if stage in {"before", "ledger-commit"}:
                raise
            error = PackageServiceError(
                "OWNED_WORK_HANDLER_UNKNOWN",
                "Package owned work handler failed without a typed result",
                500,
            )
            if stage == "dispatch":
                await self.operation_ledger.fail(scope, route, error)
            outcome = {
                "status": "FAILED",
                "error": {
                    "code": error.code,
                    "status": error.status,
                    "retryable": False,
                    "unknown": True,
                },
            }
        await self._settle(declaration, claim, outcome)

    async def _wait(self, milliseconds: int) -> None:
        try:
            await asyncio.wait_for(self.wake.wait(), timeout=milliseconds / 1000)
        except TimeoutError:
            pass
        self.wake.clear()

    async def _pump(self, declaration: dict) -> None:
        try:
            while not self.closing:
                await self.admission.wait()
                if self.closing:
                    return
                if not self.admission.is_set():
                    continue
                self.inflight += 1
                self.idle.clear()
                claim = None
                try:
                    claim = await self._claim(declaration)
                    if claim is not None:
                        await self._execute(declaration, claim)
                except ForestError as error:
                    if error.code == "RUNTIME_GENERATION_NOT_ACTIVE" and not self.admission.is_set():
                        pass
                    else:
                        self.failure = error.code
                        self.failed(error.code)
                        self.admission.clear()
                        self.wake.set()
                        raise
                finally:
                    self.inflight -= 1
                    if self.inflight == 0:
                        self.idle.set()
                if claim is None or not self.admission.is_set():
                    await self._wait(declaration["poll_interval_ms"])
        except asyncio.CancelledError:
            raise
        except Exception:
            if self.failure is None:
                self.failure = "PACKAGE_OWNED_WORK_FAILED"
                self.failed(self.failure)
            self.admission.clear()
            self.wake.set()
            raise

    def start(self) -> None:
        for declaration in self.declarations:
            for slot in range(declaration["max_concurrency"]):
                self.tasks.append(
                    asyncio.create_task(
                        self._pump(declaration),
                        name=(
                            f"package-owned-work/{self.component_id}/"
                            f"{declaration['name']}/{slot}"
                        ),
                    )
                )

    async def close(self) -> None:
        self.closing = True
        self.admission.set()
        self.wake.set()
        for task in self.tasks:
            task.cancel()
        if self.tasks:
            await asyncio.gather(*self.tasks, return_exceptions=True)
        self.tasks.clear()
