"""Lifecycle-owned media capture worker and current-context image resolver."""

from __future__ import annotations

import asyncio
import hashlib
from dataclasses import dataclass

from forest.errors import ForestError
from forest.media_capture_io import PublicImageCaptureClient
from forest.models import MediaSnapshot
from forest.runtime_generation import BoundGenerationIdentity, PreparedGenerationBinding
from forest.storage.memory_graph_authority import MemoryOccurrence, MemoryPayloadRef
from forest.storage.payload_authority import PayloadSource

_STATE_CONTRACT = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "type": "object",
    "properties": {},
    "additionalProperties": False,
}


@dataclass(frozen=True)
class FrozenMediaProfile:
    timeout_seconds: int
    max_connections: int
    max_attempts: int
    context_wait_seconds: float


class CleanMediaCaptureRuntime:
    runtime_contract = {
        "protocol": "forest.runtime-services/1",
        "services": [
            {"service_id": "media.context.resolve", "version": 1},
        ],
        "requires": [
            {"service_id": "memory.source.capture", "version": 1},
            {"service_id": "memory.source.unavailable", "version": 1},
        ],
    }
    state_schema = 1
    state_contract = _STATE_CONTRACT

    def __init__(
        self,
        *,
        repository,
        payloads,
        module_configuration,
        grants,
        generation_verifier,
        generation_admission_verifier,
        generation_member_verifier,
        memory,
        client_factory=PublicImageCaptureClient,
    ) -> None:
        self.repository = repository
        self.payloads = payloads
        self.module_configuration = module_configuration
        self.grants = grants
        self.generation_verifier = generation_verifier
        self.generation_admission_verifier = generation_admission_verifier
        self.generation_member_verifier = generation_member_verifier
        self.memory = memory
        self.client_factory = client_factory
        self.generation_identity: BoundGenerationIdentity | None = None
        self.profile: FrozenMediaProfile | None = None
        self.client = None
        self.active = False
        self._closing = False
        self._admission = asyncio.Event()
        self._wake = asyncio.Event()
        self._idle = asyncio.Event()
        self._idle.set()
        self._task: asyncio.Task | None = None
        self._owner_id: str | None = None
        self.failure: str | None = None

    def runtime_service_handlers(self) -> dict:
        return {("media.context.resolve", 1): self.resolve_context}

    @staticmethod
    def _parse_profile(value: object) -> FrozenMediaProfile:
        if (
            not isinstance(value, dict)
            or set(value)
            != {
                "protocol",
                "timeout_seconds",
                "max_connections",
                "max_attempts",
                "context_wait_seconds",
            }
            or value.get("protocol") != "forest.media.capture/1"
            or type(value.get("timeout_seconds")) is not int
            or not 1 <= value["timeout_seconds"] <= 30
            or type(value.get("max_connections")) is not int
            or not 1 <= value["max_connections"] <= 16
            or type(value.get("max_attempts")) is not int
            or not 1 <= value["max_attempts"] <= 10
            or type(value.get("context_wait_seconds")) not in {int, float}
            or not 0 <= value["context_wait_seconds"] <= 15
        ):
            raise ForestError("MEDIA_CONFIGURATION_INVALID", "Media configuration is invalid", 422)
        return FrozenMediaProfile(
            value["timeout_seconds"],
            value["max_connections"],
            value["max_attempts"],
            float(value["context_wait_seconds"]),
        )

    async def prepare_generation(self, context) -> PreparedGenerationBinding:
        desired = await self.module_configuration.desired(include_value=True)
        self.profile = self._parse_profile(desired.value)
        return PreparedGenerationBinding(
            component_id=context.component_id,
            requires=context.requires,
            package_id=context.package_id,
            package_version=context.package_version,
            package_digest=context.package_digest,
            driver=context.driver,
            trust_level=context.trust_level,
            descriptor_digest=context.descriptor_digest,
            route_contract_digest=context.route_contract_digest,
            management_contract_digest=context.management_contract_digest,
            management_ui_digest=context.management_ui_digest,
            config_revision=desired.revision,
            config_payload_id=desired.payload_id,
            config_payload_digest=desired.payload_digest,
            instance_id=f"instance:media.capture:{context.activation_id}",
            worker_incarnation=f"worker:media.capture:{context.activation_id}",
            state_schema=context.state_schema,
            state_contract_digest=context.state_contract_digest,
            capability_grants_digest=self.grants.grant_set_digest,
            data_admission=True,
            control_admission=True,
            storage=context.storage,
        )

    async def bind_generation(self, identity: BoundGenerationIdentity) -> None:
        if identity.component_id != "media.capture":
            raise ForestError("RUNTIME_GENERATION_BINDING_MISMATCH", "Media binding differs", 409)
        self.generation_identity = identity
        self._owner_id = ("media-capture/" + identity.generation_id)[-256:]

    async def start(self) -> None:
        if self.generation_identity is None or self.profile is None:
            raise ForestError("RUNTIME_GENERATION_IDENTITY_UNAVAILABLE", "Media is not prepared", 503)
        self.client = self.client_factory(
            timeout_seconds=self.profile.timeout_seconds,
            max_connections=self.profile.max_connections,
        )
        self._task = asyncio.create_task(
            self._run(), name="media-capture/" + self.generation_identity.generation_id
        )

    def activate_generation(self) -> None:
        self.active = True
        self._admission.set()
        self._wake.set()

    def notify(self) -> None:
        self._wake.set()

    async def quiesce(self) -> None:
        self.active = False
        self._admission.clear()
        self._wake.set()
        await self._idle.wait()

    async def close(self) -> None:
        self.active = False
        self._closing = True
        self._admission.set()
        self._wake.set()
        if self._task is not None:
            self._task.cancel()
            await asyncio.gather(self._task, return_exceptions=True)
            self._task = None
        if self.client is not None:
            await self.client.aclose()
            self.client = None

    def owned_tasks(self):
        return (self._task,) if self._task is not None else ()

    async def _admit_in(
        self, conn, *, owned_work: bool, admission_lane: str = "DATA"
    ) -> None:
        if not self.active or self.generation_identity is None:
            raise ForestError("RUNTIME_GENERATION_NOT_ACTIVE", "Media is not active", 409)
        verifier = (
            self.generation_member_verifier
            if owned_work
            else self.generation_admission_verifier
        )
        values = {
            "component_id": "media.capture",
            "generation_id": self.generation_identity.generation_id,
        }
        if not owned_work:
            values["admission_lane"] = admission_lane
        await verifier(conn, **values)

    async def register_capture_in(self, conn, **values):
        admission_lane = values.pop("admission_lane", "DATA")
        await self._admit_in(conn, owned_work=False, admission_lane=admission_lane)
        return await self.repository.register_in(conn, **values)

    async def _wait(self, timeout: float = 1) -> None:
        try:
            await asyncio.wait_for(self._wake.wait(), timeout=timeout)
        except TimeoutError:
            pass
        self._wake.clear()

    async def _run(self) -> None:
        try:
            while not self._closing:
                await self._admission.wait()
                if self._closing:
                    return
                self._idle.clear()
                try:
                    claim = await self.repository.claim(
                        owner_id=self._owner_id,
                    )
                    if claim is not None:
                        await self._execute(claim)
                finally:
                    self._idle.set()
                if claim is None or not self._admission.is_set():
                    await self._wait()
        except asyncio.CancelledError:
            raise
        except ForestError as exc:
            self.failure = exc.code
            raise
        except Exception:
            self.failure = "MEDIA_CAPTURE_WORKER_FAILED"
            raise

    async def _execute(self, claim: dict) -> None:
        owner_id = self._owner_id
        failed = False
        for part in claim["parts"]:
            if part["part_kind"] != "IMAGE" or part["state"] != "PENDING":
                continue
            try:
                image = await self.client.fetch(part["locator"])
                epoch = await self.payloads.epoch()
                saved = await self.payloads.put_bytes(
                    owner_component_id="media.capture",
                    owner_kind="media.asset",
                    owner_id=part["media_ref"],
                    media_type=image.media_type,
                    content=image.content,
                    sources=(
                        PayloadSource(
                            "adapter.onebot",
                            "onebot.media",
                            claim["event_ref"],
                            1,
                            part_id=part["part_id"],
                            source_trunk_id=claim["trunk_id"],
                            observation_conversation_id=claim["conversation_ref"],
                            observation_binding_id=claim["binding_ref"],
                            author_subject_id=claim["author_subject_ref"],
                            occurred_at=claim["occurred_at"],
                        ),
                    ),
                    expected_deletion_epoch=epoch,
                )
                await self.repository.publish_image(
                    capture_id=claim["capture_id"],
                    part_id=part["part_id"],
                    owner_id=owner_id,
                    payload=MemoryPayloadRef(
                        saved.payload_id,
                        saved.content_digest,
                        "media.capture",
                        "media.asset",
                        part["media_ref"],
                    ),
                    media_type=image.media_type,
                    width=image.width,
                    height=image.height,
                    byte_length=len(image.content),
                )
            except ForestError as exc:
                failed = True
                final = claim["attempt_count"] >= self.profile.max_attempts
                await self.repository.fail_part(
                    capture_id=claim["capture_id"],
                    part_id=part["part_id"],
                    owner_id=owner_id,
                    error_code=exc.code,
                    final=final,
                )
            except Exception:
                failed = True
                final = claim["attempt_count"] >= self.profile.max_attempts
                await self.repository.fail_part(
                    capture_id=claim["capture_id"],
                    part_id=part["part_id"],
                    owner_id=owner_id,
                    error_code="MEDIA_TRANSPORT_FAILED",
                    final=final,
                )
        current = await self.repository.load(claim["capture_id"])
        if any(part["state"] == "PENDING" for part in current["parts"]):
            delay = min(2 ** current["attempt_count"], 300)
            await self.repository.retry(
                capture_id=claim["capture_id"],
                owner_id=owner_id,
                error_code="MEDIA_CAPTURE_RETRY",
                delay=delay,
            )
            return
        identity, author, parts = self.repository.memory_values(current)
        if not parts:
            await self.memory.mark_unavailable(
                conversation_ref=current["conversation_ref"],
                event_ref=current["event_ref"],
                error_code="MEDIA_CAPTURE_UNAVAILABLE",
            )
            await self.repository.settle(
                capture_id=claim["capture_id"],
                owner_id=owner_id,
                state="FAILED",
                error_code="MEDIA_CAPTURE_UNAVAILABLE",
            )
            return
        await self.memory.capture_source(
            operation_id="media-memory:" + hashlib.sha256(claim["capture_id"].encode()).hexdigest(),
            identity=identity,
            source_unit_id=current["source_unit_id"],
            source_revision=current["source_revision"],
            unit_kind=current["unit_kind"],
            author_subject_ref=current["author_subject_ref"],
            author=author,
            parts=parts,
            occurrence=MemoryOccurrence(
                "media-occurrence:" + hashlib.sha256(claim["capture_id"].encode()).hexdigest(),
                current["binding_ref"],
                current["conversation_ref"],
                current["event_ref"],
                current["ingress_seq"],
                current["observed_at"],
            ),
            occurred_at=current["occurred_at"],
        )
        terminal = "PARTIAL" if failed or any(
            part["state"] == "FAILED" for part in current["parts"]
        ) else "READY"
        await self.repository.settle(
            capture_id=claim["capture_id"],
            owner_id=owner_id,
            state=terminal,
            error_code="MEDIA_CAPTURE_PARTIAL" if terminal == "PARTIAL" else None,
        )

    @staticmethod
    def _image_parts(snapshot: dict) -> tuple[tuple[str, str, str], ...]:
        values = []
        for unit in snapshot.get("units", ()):
            if not isinstance(unit, dict):
                continue
            unit_id = unit.get("unit_id")
            for part in unit.get("parts", ()):
                if (
                    isinstance(part, dict)
                    and part.get("type") == "image"
                    and isinstance(part.get("part_id"), str)
                    and isinstance(part.get("media_ref"), str)
                    and isinstance(unit_id, str)
                ):
                    values.append((unit_id, part["part_id"], part["media_ref"]))
        return tuple(values)

    async def resolve_context(self, snapshot: dict) -> dict:
        if not self.active or self.profile is None:
            raise ForestError("RUNTIME_GENERATION_NOT_ACTIVE", "Media is not active", 409)
        parts = self._image_parts(snapshot)
        if not parts:
            return {}
        refs = tuple(item[2] for item in parts)
        deadline = asyncio.get_running_loop().time() + self.profile.context_wait_seconds
        while True:
            rows = await self.repository.asset_rows(refs)
            by_ref = {row["media_ref"]: row for row in rows}
            if len(by_ref) == len(set(refs)) or asyncio.get_running_loop().time() >= deadline:
                break
            self.notify()
            await asyncio.sleep(0.05)
        result = {}
        for unit_id, part_id, media_ref in parts:
            row = by_ref.get(media_ref)
            if row is None:
                continue
            content = await self.payloads.read_bytes(row["payload_id"])
            image = MediaSnapshot(
                self.repository.store.bot_id,
                media_ref,
                row["media_revision"],
                unit_id,
                part_id,
                row["media_type"],
                content,
                row["payload_digest"],
                row["width"],
                row["height"],
            )
            result[(unit_id, part_id)] = image
            result[media_ref] = image
        return result
