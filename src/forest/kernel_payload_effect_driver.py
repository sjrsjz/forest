"""Generation-fenced JSON projection over the kernel Payload authority."""

from __future__ import annotations

import json

from forest.errors import ForestError


def _canonical(value: object) -> bytes:
    try:
        return json.dumps(
            value,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=False,
            allow_nan=False,
        ).encode("utf-8")
    except (TypeError, ValueError, UnicodeError, OverflowError, RecursionError) as exc:
        raise ForestError("PAYLOAD_JSON_INVALID", "Payload value is not finite JSON", 422) from exc


class KernelPayloadJsonEffectDriver:
    """Read exact JSON references or materialize Effect-owned JSON bytes."""

    def __init__(
        self,
        _configuration,
        _configuration_revision,
        identity,
        declaration,
        *,
        runtime,
        payloads,
    ) -> None:
        if declaration.get("kind") != "kernel.payload-json" or declaration.get("name") not in {
            "kernel.payload.json-read",
            "kernel.effect.json-materialize",
        }:
            raise ForestError(
                "PACKAGE_EFFECT_DRIVER_BINDING_INVALID",
                "Payload JSON driver received another effect service",
                500,
            )
        self.identity = identity
        self.declaration = declaration
        self.runtime = runtime
        self.payloads = payloads
        self.accepting = False

    async def start(self) -> None:
        self.accepting = True

    async def invoke(self, payload, *, scope, invocation):
        if (
            not self.accepting
            or scope.component_id != self.identity.component_id
            or scope.generation_id != self.identity.generation_id
        ):
            raise ForestError(
                "RUNTIME_GENERATION_NOT_ACTIVE",
                "Payload JSON driver is not accepting effects",
                409,
            )
        await self.runtime.verify_generation_member(
            component_id=self.identity.component_id,
            generation_id=self.identity.generation_id,
        )
        if self.declaration["name"] == "kernel.payload.json-read":
            return await self._read(payload, invocation.operation_id)
        return await self._materialize(payload, invocation.operation_id)

    async def _read(self, payload: dict, operation_id: str) -> dict:
        reference = payload.get("payload")
        fields = {
            "payload_id",
            "digest",
            "owner_component_id",
            "owner_kind",
            "owner_id",
        }
        if not isinstance(reference, dict) or set(reference) != fields:
            raise ForestError("PAYLOAD_JSON_INPUT_INVALID", "Payload reference is invalid", 422)
        async with self.payloads.store.pool.connection() as conn, conn.transaction():
            verified = await self.payloads.verify_in(
                conn,
                payload_id=reference["payload_id"],
                digest=reference["digest"],
                owner_component_id=reference["owner_component_id"],
                owner_kind=reference["owner_kind"],
                owner_id=reference["owner_id"],
            )
            content = await self.payloads.read_bytes_in(
                conn,
                reference["payload_id"],
                expected_deletion_epoch=verified.accepted_deletion_epoch,
            )
        try:
            value = json.loads(content)
        except (UnicodeError, json.JSONDecodeError) as exc:
            raise ForestError(
                "PAYLOAD_JSON_INVALID", "Payload does not contain valid JSON", 422
            ) from exc
        _canonical(value)
        return {
            "protocol": "forest.payload-json/1",
            "operation_id": operation_id,
            "value": value,
        }

    async def _materialize(self, payload: dict, operation_id: str) -> dict:
        if (
            set(payload) != {"protocol", "owner_kind", "owner_id", "value"}
            or payload.get("protocol") != "forest.effect-json-materialize/1"
        ):
            raise ForestError("PAYLOAD_JSON_INPUT_INVALID", "Effect payload input is invalid", 422)
        if payload["owner_kind"] not in {"effect.wire", "effect.evidence"}:
            raise ForestError("PAYLOAD_JSON_INPUT_INVALID", "Effect payload kind is invalid", 422)
        epoch = await self.payloads.epoch()
        saved = await self.payloads.put_bytes(
            owner_component_id="kernel.effect",
            owner_kind=payload["owner_kind"],
            owner_id=payload["owner_id"],
            media_type="application/json",
            content=_canonical(payload["value"]),
            expected_deletion_epoch=epoch,
        )
        return {
            "protocol": "forest.effect-json-materialized/1",
            "operation_id": operation_id,
            "payload": {
                "payload_id": saved.payload_id,
                "digest": saved.content_digest,
                "owner_component_id": "kernel.effect",
                "owner_kind": payload["owner_kind"],
                "owner_id": payload["owner_id"],
            },
        }

    async def quiesce(self) -> None:
        self.accepting = False

    async def resume(self) -> None:
        self.accepting = True

    async def close(self) -> None:
        self.accepting = False


def kernel_payload_json_effect_driver_factory(*, runtime, payloads):
    def create(configuration, configuration_revision, identity, declaration):
        return KernelPayloadJsonEffectDriver(
            configuration,
            configuration_revision,
            identity,
            declaration,
            runtime=runtime,
            payloads=payloads,
        )

    return create
