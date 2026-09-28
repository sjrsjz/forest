"""Generic Payload writes and revocation for package UnitOfWork plans."""

from __future__ import annotations

from datetime import datetime

from forest.errors import ForestError
from forest.storage.payload_authority import PayloadSource, SourceTarget


def _object(value: object, fields: set[str], code: str) -> dict:
    if not isinstance(value, dict) or set(value) != fields:
        raise ForestError(code, "Payload UnitOfWork input is invalid", 422)
    return value


class KernelPayloadUnitOfWorkParticipant:
    """Apply Payload-only protocols prepared by earlier UnitOfWork steps."""

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
        if name == "payload.capture-deletion-epoch":
            envelope = _object(
                payload,
                {"protocol", "caller", "input", "prior"},
                "PAYLOAD_UOW_INPUT_INVALID",
            )
            caller = _object(
                envelope["caller"],
                {"component_id", "generation_id"},
                "PAYLOAD_UOW_INPUT_INVALID",
            )
            if (
                envelope["protocol"] != "forest.uow-step/1"
                or envelope["input"] != {"protocol": "forest.payload-epoch-capture/1"}
                or any(not isinstance(value, str) or not value for value in caller.values())
            ):
                raise ForestError(
                    "PAYLOAD_UOW_INPUT_INVALID",
                    "Payload deletion epoch request is invalid",
                    422,
                )
            epoch = await self.service.epoch_in(conn)
            return {
                "protocol": "forest.payload-epoch/1",
                "deletion_epoch": epoch,
            }
        if name in {
            "payload.preview-forget-anchor-from-prior",
            "payload.forget-anchor-from-prior",
            "payload.preview-forget-anchors-from-prior",
            "payload.forget-anchors-from-prior",
        }:
            preview = name.startswith("payload.preview-forget-")
            batch = name.endswith("anchors-from-prior")
            expected_protocol = {
                (False, False): "forest.payload-forget-anchor/1",
                (False, True): "forest.payload-forget-anchor-preview/1",
                (True, False): "forest.payload-forget-anchors/1",
                (True, True): "forest.payload-forget-anchors-preview/1",
            }[(batch, preview)]
            envelope = _object(
                payload, {"protocol", "caller", "input", "prior"},
                "PAYLOAD_UOW_INPUT_INVALID",
            )
            caller = _object(
                envelope["caller"], {"component_id", "generation_id"},
                "PAYLOAD_UOW_INPUT_INVALID",
            )
            request = envelope["input"]
            required = {"protocol", "prior_step_id"}
            optional = set() if preview else {
                "operation_id", "expected_deletion_epoch", "expected_selection_digest"
            }
            if (
                not isinstance(request, dict)
                or not required <= set(request)
                or not set(request) <= required | optional
                or (not preview and "operation_id" not in request)
            ):
                raise ForestError(
                    "PAYLOAD_UOW_INPUT_INVALID", "Payload forget request is invalid", 422
                )
            prior = envelope["prior"]
            if (
                envelope["protocol"] != "forest.uow-step/1"
                or any(not isinstance(value, str) or not value for value in caller.values())
                or request["protocol"] != expected_protocol
                or not isinstance(prior, dict)
                or not isinstance(request["prior_step_id"], str)
                or request["prior_step_id"] not in prior
                or (
                    not preview and (
                        not isinstance(request["operation_id"], str)
                        or not 1 <= len(request["operation_id"].encode("utf-8")) <= 256
                    )
                )
            ):
                raise ForestError(
                    "PAYLOAD_UOW_INPUT_INVALID", "Payload forget plan is invalid", 422
                )
            plan_fields = (
                {"protocol", "anchor_payload_ids"} if batch else
                {"protocol", "anchor_payload_id"}
            )
            plan = _object(
                prior[request["prior_step_id"]], plan_fields,
                "PAYLOAD_UOW_INPUT_INVALID",
            )
            anchor_ids = (
                plan["anchor_payload_ids"] if batch else [plan["anchor_payload_id"]]
            )
            expected_plan_protocol = (
                "forest.payload-forget-batch-plan/1" if batch else
                "forest.payload-forget-plan/1"
            )
            if (plan["protocol"] != expected_plan_protocol
                or not isinstance(anchor_ids, list)
                or not 1 <= len(anchor_ids) <= (64 if batch else 1)
                or any(
                    not isinstance(item, str) or
                    not 1 <= len(item.encode("utf-8")) <= 256
                    for item in anchor_ids
                )
                or len(set(anchor_ids)) != len(anchor_ids)):
                raise ForestError(
                    "PAYLOAD_UOW_INPUT_INVALID", "Payload forget anchor is invalid", 422
                )
            source_map = await self.service._sources_for_payloads_in(
                conn, tuple(anchor_ids)
            )
            targets = []
            for anchor_id in anchor_ids:
                sources = source_map[anchor_id]
                if len(sources) != 1 or sources[0].part_id != "*":
                    raise ForestError(
                        "PAYLOAD_FORGET_SOURCE_AMBIGUOUS",
                        "Anchor Payload must identify exactly one root source",
                        409,
                    )
                source = sources[0]
                targets.append(SourceTarget(
                    source_component_id=source.source_component_id,
                    source_kind=source.source_kind,
                    source_id=source.source_id,
                    source_revision=source.source_revision,
                ))
            if len(set(targets)) != len(targets):
                raise ForestError(
                    "PAYLOAD_FORGET_SOURCE_AMBIGUOUS",
                    "Anchor Payloads must identify distinct root sources",
                    409,
                )
            if preview:
                result = await self.service.preview_forget_in(
                    conn, source_targets=tuple(targets)
                )
                return {"protocol": "forest.payload-forget-preview/1", **result}
            receipt = await self.service.forget_in(
                conn,
                operation_id=request["operation_id"],
                source_targets=tuple(targets),
                expected_deletion_epoch=request.get("expected_deletion_epoch"),
                expected_selection_digest=request.get("expected_selection_digest"),
            )
            selected_ids = await self.service.forget_selected_payload_ids_in(
                conn, request["operation_id"]
            )
            if len(selected_ids) != receipt["selected_count"]:
                raise ForestError(
                    "PAYLOAD_FORGET_SELECTION_INVALID",
                    "Payload forget selection differs from its committed receipt",
                    503,
                )
            return {
                "protocol": "forest.payload-forgotten/1",
                "operation_id": receipt["operation_id"],
                "deletion_epoch": receipt["deletion_epoch"],
                "selection_digest": receipt["selection_digest"],
                "selected_payload_ids": selected_ids,
            }
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
