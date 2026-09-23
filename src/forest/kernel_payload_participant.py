"""Generation-derived Payload writes for package UnitOfWork plans."""

from __future__ import annotations

from datetime import datetime

from forest.errors import ForestError
from forest.storage.payload_authority import PayloadSource


def _object(value: object, fields: set[str], code: str) -> dict:
    if not isinstance(value, dict) or set(value) != fields:
        raise ForestError(code, "Payload UnitOfWork input is invalid", 422)
    return value


class KernelPayloadUnitOfWorkParticipant:
    """Persist one caller-owned JSON payload prepared by an earlier UoW step."""

    component_id = "kernel.payload"

    def __init__(self, runtime, service) -> None:
        self.runtime = runtime
        self.service = service

    @property
    def generation_id(self) -> str:
        identity = self.runtime.generation_identity
        if identity is None:
            raise ForestError(
                "RUNTIME_UOW_PARTICIPANT_UNAVAILABLE",
                "Payload Generation is not bound",
                503,
            )
        return identity.generation_id

    async def invoke_in(self, conn, name: str, payload: dict, **_invocation) -> dict:
        if name != "payload.put-json-from-prior":
            raise ForestError(
                "RUNTIME_UOW_PARTICIPANT_INPUT_INVALID",
                "Payload UnitOfWork operation is undeclared",
                422,
            )
        envelope = _object(
            payload,
            {"protocol", "caller", "input", "prior"},
            "PAYLOAD_UOW_INPUT_INVALID",
        )
        if envelope["protocol"] != "forest.uow-step/1":
            raise ForestError(
                "PAYLOAD_UOW_INPUT_INVALID", "UnitOfWork step protocol is invalid", 422
            )
        caller = _object(
            envelope["caller"],
            {"component_id", "generation_id"},
            "PAYLOAD_UOW_INPUT_INVALID",
        )
        request = _object(
            envelope["input"],
            {"protocol", "prior_step_id"},
            "PAYLOAD_UOW_INPUT_INVALID",
        )
        if request["protocol"] != "forest.payload-json-from-prior/1":
            raise ForestError(
                "PAYLOAD_UOW_INPUT_INVALID", "Payload write protocol is invalid", 422
            )
        prior_step_id = request["prior_step_id"]
        if not isinstance(prior_step_id, str) or not 1 <= len(prior_step_id) <= 128:
            raise ForestError(
                "PAYLOAD_UOW_INPUT_INVALID", "Prior step identity is invalid", 422
            )
        prior = envelope["prior"]
        if not isinstance(prior, dict) or prior_step_id not in prior:
            raise ForestError(
                "PAYLOAD_UOW_INPUT_INVALID", "Prepared payload step is absent", 422
            )
        plan = prior[prior_step_id]
        if (
            not isinstance(plan, dict)
            or plan.get("protocol") != "forest.payload-json-plan/1"
            or plan.get("status") not in {"SKIP", "WRITE"}
        ):
            raise ForestError(
                "PAYLOAD_UOW_INPUT_INVALID", "Prepared payload plan is invalid", 422
            )
        if plan["status"] == "SKIP":
            if set(plan) != {"protocol", "status"}:
                raise ForestError(
                    "PAYLOAD_UOW_INPUT_INVALID", "Skipped payload plan is invalid", 422
                )
            return {"protocol": "forest.payload-json-put/1", "status": "SKIPPED"}

        if set(plan) != {"protocol", "status", "payload", "commit"}:
            raise ForestError(
                "PAYLOAD_UOW_INPUT_INVALID", "Prepared payload plan is invalid", 422
            )
        write = _object(
            plan["payload"],
            {"owner_kind", "owner_id", "value", "parents", "sources"},
            "PAYLOAD_UOW_INPUT_INVALID",
        )
        if not isinstance(write["parents"], list) or not isinstance(write["sources"], list):
            raise ForestError(
                "PAYLOAD_UOW_INPUT_INVALID", "Payload lineage is invalid", 422
            )
        sources = []
        source_fields = {
            "source_component_id",
            "source_kind",
            "source_id",
            "source_revision",
            "part_id",
            "source_trunk_id",
            "observation_conversation_id",
            "observation_binding_id",
            "author_subject_id",
            "occurred_at",
            "propagation_class",
        }
        for raw in write["sources"]:
            source = _object(raw, source_fields, "PAYLOAD_UOW_INPUT_INVALID")
            occurred_at = source["occurred_at"]
            if occurred_at is not None:
                if not isinstance(occurred_at, str):
                    raise ForestError(
                        "PAYLOAD_UOW_INPUT_INVALID", "Payload source time is invalid", 422
                    )
                try:
                    occurred_at = datetime.fromisoformat(occurred_at.replace("Z", "+00:00"))
                except ValueError:
                    raise ForestError(
                        "PAYLOAD_UOW_INPUT_INVALID", "Payload source time is invalid", 422
                    ) from None
            sources.append(PayloadSource(**{**source, "occurred_at": occurred_at}))
        deletion_epoch = await self.service.epoch_in(conn)
        saved = await self.service.put_json_in(
            conn,
            owner_component_id=caller["component_id"],
            owner_kind=write["owner_kind"],
            owner_id=write["owner_id"],
            value=write["value"],
            parents=tuple(write["parents"]),
            sources=tuple(sources),
            expected_deletion_epoch=deletion_epoch,
        )
        return {
            "protocol": "forest.payload-json-put/1",
            "status": "WRITTEN",
            "payload": {
                "payload_id": saved.payload_id,
                "digest": saved.content_digest,
                "owner_component_id": caller["component_id"],
                "owner_kind": write["owner_kind"],
                "owner_id": write["owner_id"],
            },
        }
