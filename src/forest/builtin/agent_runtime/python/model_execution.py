"""One Agent-owned model attempt over typed package and kernel ports."""

from __future__ import annotations

import hashlib
import json
from datetime import UTC, datetime, timedelta

from agent_protocol import ProtocolRepairView
from model_projection import prepare_model_request
from turn_recording import stable

from forest.errors import ForestError
from forest.model_contract import MediaSnapshot, ModelCapabilities

_FROZEN_PAYLOAD_FAILURES = frozenset({
    "PAYLOAD_REFERENCE_INVALID",
    "PAYLOAD_IDENTITY_CONFLICT",
    "PAYLOAD_NOT_FOUND",
    "PAYLOAD_REVOKED",
    "PAYLOAD_DIGEST_MISMATCH",
    "PAYLOAD_ID_CONFLICT",
    "PAYLOAD_PARENT_UNAVAILABLE",
})


async def _frozen_payload_result(awaitable):
    try:
        return await awaitable
    except ForestError as exc:
        if exc.code not in _FROZEN_PAYLOAD_FAILURES:
            raise
        raise ForestError(
            "AGENT_FROZEN_MODEL_INVALID",
            "Frozen model Payload is unavailable or differs from its exact reference",
            409,
        ) from exc


def _digest(value) -> str:
    encoded = json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        allow_nan=False,
    ).encode("utf-8")
    return "sha256:" + hashlib.sha256(encoded).hexdigest()


def _learning_candidates(manifest: dict) -> list[dict]:
    """Freeze direct seeds whose content was actually sent to this model."""
    evidence = manifest.get("memory_evidence")
    if not isinstance(evidence, dict) or evidence.get("status") != "READY":
        return []
    included: dict[str, list[str]] = {}
    for part in manifest["included_part_refs"]:
        if part.get("context_data") == "memory_evidence":
            included.setdefault(part["source_unit_id"], []).append(part["part_id"])
    candidates = []
    for source in manifest["source_manifest"]:
        if (source.get("context_data") != "memory_evidence"
                or source.get("bundle_id") != evidence["bundle_id"]
                or not source.get("retrieval", {}).get("is_seed")):
            continue
        part_ids = list(dict.fromkeys(included.get(source["source_unit_id"], ())))[:8]
        if part_ids:
            candidates.append({
                "bundle_id": evidence["bundle_id"],
                "query_id": evidence["query_id"],
                "source_unit_id": source["source_unit_id"],
                "source_revision": source["source_revision"],
                "rank_score": source["retrieval"]["rank_score"],
                "part_ids": part_ids,
            })
    return candidates[:64]


def _receipt(value, *, request_id: str, attempt_id: str, payload_digest: str) -> dict:
    fields = {
        "status",
        "request_id",
        "transport_attempt_id",
        "payload_digest",
        "result",
        "usage",
        "error_code",
    }
    if (
        not isinstance(value, dict)
        or set(value) != fields
        or value.get("status") not in {"RESPONDED", "NOT_SENT", "UNKNOWN"}
        or value.get("request_id") != request_id
        or value.get("transport_attempt_id") != attempt_id
        or value.get("payload_digest") != payload_digest
    ):
        raise ForestError(
            "PROVIDER_RESULT_FENCE_MISMATCH",
            "Provider receipt differs from the frozen model attempt",
            502,
        )
    if value["status"] == "RESPONDED":
        if value["result"] is None or value["error_code"] is not None:
            raise ForestError(
                "PROVIDER_RESULT_INVALID", "Successful Provider receipt is incomplete", 502
            )
    elif value["result"] is not None or value["usage"] is not None:
        raise ForestError(
            "PROVIDER_RESULT_INVALID", "Unsettled Provider receipt carries payloads", 502
        )
    return value


def _provider_result(value, *, receipt: dict) -> dict:
    fields = {
        "protocol",
        "status",
        "request_id",
        "transport_attempt_id",
        "payload_digest",
        "text",
        "tool_calls",
        "finish_reason",
        "provider_response_id",
        "provider_model",
        "error_code",
    }
    if (
        not isinstance(value, dict)
        or set(value) != fields
        or value.get("protocol") != "forest.provider-model-result/1"
        or value.get("status") != "RESPONDED"
        or value.get("request_id") != receipt["request_id"]
        or value.get("transport_attempt_id") != receipt["transport_attempt_id"]
        or value.get("payload_digest") != receipt["payload_digest"]
        or not isinstance(value.get("text"), str)
        or not isinstance(value.get("tool_calls"), list)
        or value.get("error_code") is not None
    ):
        raise ForestError(
            "PROVIDER_RESULT_INVALID", "Provider result Payload is invalid", 502
        )
    return value


class ModelExecution:
    """Freeze, authorize, hand off and observe exactly one Provider attempt."""

    def __init__(self, ctx) -> None:
        self.ctx = ctx

    def _identity(self) -> tuple[str, int, str]:
        graph_id = self.ctx.identity.get("graph_id")
        graph_revision = self.ctx.identity.get("graph_revision")
        bot_id = self.ctx.identity.get("bot_id")
        if (
            not isinstance(graph_id, str)
            or not graph_id
            or type(graph_revision) is not int
            or graph_revision < 1
            or not isinstance(bot_id, str)
            or not bot_id
        ):
            raise ForestError(
                "AGENT_GENERATION_FENCE_MISSING",
                "Agent Worker has no frozen RuntimeGraph identity",
                409,
            )
        return graph_id, graph_revision, bot_id

    async def _profile(self, request_id: str) -> tuple[dict, ModelCapabilities]:
        profile = await self.ctx.services.call(
            "provider.profile.bind",
            {"protocol": "forest.provider-bind-request/1", "request_id": request_id},
            operation_id=self.ctx.operations.stable(f"provider-bind:{request_id}"),
            timeout=15,
        )
        if (
            profile.get("protocol") != "forest.provider-profile/1"
            or profile.get("request_id") != request_id
            or profile.get("component_id") != "provider.chat-completions"
        ):
            raise ForestError(
                "PROVIDER_PROFILE_FENCE_MISMATCH",
                "Provider profile differs from the requested model step",
                409,
            )
        target = ModelCapabilities(**profile["capabilities"])
        if _digest(target.as_dict()) != profile["capability_digest"]:
            raise ForestError(
                "PROVIDER_PROFILE_FENCE_MISMATCH",
                "Provider capabilities differ from their bound digest",
                409,
            )
        return profile, target

    async def invoke(
        self,
        *,
        owner: dict,
        request_id: str,
        attempt_id: str,
        effect_operation_id: str,
        snapshot: dict,
        preset_context: list[str],
        source_refs: list[dict],
        tools: list[dict],
        evidence_ref: dict | None,
        memory_query_id: str | None,
        repair: dict | None,
        output_tokens: int,
        stream: bool,
        request_kind: str,
    ) -> dict:
        graph_id, graph_revision, bot_id = self._identity()
        profile, target = await self._profile(request_id)
        capabilities = profile["capabilities"]
        images = {}
        media_parents = []
        seen_media = set()
        context_media = []
        if "image" in target.input_modalities:
            for unit in snapshot.get("units", ()):
                for part in unit.get("parts", ()):
                    if (part.get("type") == "image"
                            and isinstance(part.get("media_ref"), str)
                            and type(part.get("media_revision")) is int):
                        context_media.append({
                            "source_unit_id": unit["unit_id"],
                            "part_id": part["part_id"],
                            "media_ref": part["media_ref"],
                            "media_revision": part["media_revision"],
                        })
        # Prefer the newest visible images; older context still carries an
        # explicit unavailable marker in the model projection.
        requested_media = context_media[-32:]
        requested_identities = {
            (item["source_unit_id"], item["part_id"], item["media_ref"], item["media_revision"])
            for item in requested_media
        }
        assets = []
        if requested_media:
            resolved = await self.ctx.services.call(
                "media.context.resolve", {"items": requested_media},
                operation_id=self.ctx.operations.stable(f"media-resolve:{request_id}"),
                timeout=30,
            )
            assets = resolved["assets"]
        context_image_budget = 12 * 1024 * 1024
        for asset in assets:
            identity = (asset["source_unit_id"], asset["part_id"])
            binding = (*identity, asset["media_ref"], asset["media_revision"])
            if identity in seen_media or binding not in requested_identities:
                raise ForestError(
                    "AGENT_MEDIA_INPUT_INVALID", "Resolved image differs from its context source", 422
                )
            seen_media.add(identity)
            if asset["byte_length"] > context_image_budget:
                continue
            reference = asset["payload"]
            content = await self.ctx.payloads.read_bytes(reference)
            if len(content) != asset["byte_length"]:
                raise ForestError(
                    "AGENT_MEDIA_INPUT_INVALID", "Managed image length changed", 409
                )
            context_image_budget -= len(content)
            image = MediaSnapshot(
                bot_id, asset["media_ref"], asset["media_revision"],
                asset["source_unit_id"], asset["part_id"], asset["media_type"],
                content, reference["digest"], asset["width"], asset["height"],
            )
            images[identity] = image
            images[asset["media_ref"]] = image
            media_parents.append(reference)
        if (evidence_ref is None) != (memory_query_id is None):
            raise ForestError(
                "AGENT_MEMORY_FENCE_MISMATCH", "Memory evidence and query identity differ", 409
            )
        evidence = None
        if evidence_ref is not None:
            if (evidence_ref.get("owner_component_id") != "memory.graph"
                    or evidence_ref.get("owner_kind") != "retrieval.evidence"):
                raise ForestError(
                    "AGENT_MEMORY_FENCE_MISMATCH", "Memory evidence owner is invalid", 409
                )
            evidence = await self.ctx.payloads.read_json(evidence_ref)
            if (not isinstance(evidence, dict)
                    or evidence.get("query_id") != memory_query_id
                    or evidence.get("bot_id") != bot_id):
                raise ForestError(
                    "AGENT_MEMORY_FENCE_MISMATCH", "Memory evidence belongs to another query", 409
                )
            if "image" in target.input_modalities:
                memory_image_budget = 12 * 1024 * 1024
                for unit in evidence.get("evidence", ()):
                    for part in unit.get("parts", ()):
                        if part.get("kind") != "image":
                            continue
                        reference = part["media_ref"]
                        if memory_image_budget <= 0:
                            continue
                        content = await self.ctx.payloads.read_bytes(reference)
                        if len(content) > memory_image_budget:
                            continue
                        memory_image_budget -= len(content)
                        image = MediaSnapshot(
                            bot_id, reference["payload_id"], unit["source_revision"],
                            unit["source_unit_id"], part["part_id"],
                            part["media_type"], content, reference["digest"],
                            part["width"], part["height"],
                        )
                        images[(unit["source_unit_id"], part["part_id"])] = image
                        images[reference["payload_id"]] = image
        model_input = {
            "protocol": "forest.agent-model-input/1",
            "request_id": request_id,
            "snapshot": snapshot,
            "preset_context": preset_context,
            "tools": tools,
            "evidence": evidence,
            "repair": repair,
            "output_tokens": output_tokens,
            "stream": stream,
        }
        context_parent = snapshot.get("context_snapshot_ref")
        if not source_refs:
            raise ForestError(
                "AGENT_MODEL_LINEAGE_MISSING",
                "Model input must inherit its Preset or repair feedback",
                409,
            )
        model_input_ref = await self.ctx.payloads.put_json(
            model_input,
            owner_kind="model.input",
            owner_id=request_id,
            parents=(
                *((context_parent,) if context_parent is not None else ()),
                *source_refs,
                *((evidence_ref,) if evidence_ref is not None else ()),
                *media_parents,
            ),
        )
        repair_view = ProtocolRepairView.from_record(repair) if repair is not None else None
        prepared = prepare_model_request(
            snapshot, target, request_id=request_id, transport_attempt_id=attempt_id,
            preset_context=preset_context, tools=tools, media=images,
            output_tokens=output_tokens, stream=stream, evidence=evidence,
            repair_view=repair_view,
        )
        projection = await self.ctx.payloads.put_bytes(
            prepared.manifest_json.encode("utf-8"), owner_kind="model.projection",
            owner_id=request_id, media_type="application/json",
            parents=(model_input_ref,),
        )
        wire = await self.ctx.payloads.put_bytes(
            prepared.body, owner_kind="model.wire", owner_id=attempt_id,
            media_type="application/json", parents=(projection,),
        )
        manifest = prepared.manifest
        materialized = {
            **{name: profile[name] for name in (
                "component_id", "generation_id", "profile_ref", "capability_digest"
            )},
            "projection": projection,
            "wire": wire,
            "projected_content_digest": manifest["projected_content_digest"],
            "source_manifest_digest": _digest(manifest["source_manifest"]),
            "max_output_tokens": prepared.max_output_tokens,
            "stream": prepared.stream,
            "serializer_revision": prepared.serializer_revision,
        }
        await self.ctx.uow.call(
            "agent.prepare-model-request",
            {
                "owner": owner,
                "request": {
                    "request_id": request_id,
                    "request_kind": request_kind,
                    "provider_component_id": profile["component_id"],
                    "provider_generation_id": profile["generation_id"],
                    "provider_profile_ref": profile["profile_ref"],
                    "model_ref": capabilities["model"],
                    "instruction_channel": {
                        "system": "SYSTEM",
                        "developer": "DEVELOPER",
                    }[capabilities["system_role"]],
                    "capability_digest": profile["capability_digest"],
                    "projection": materialized["projection"],
                    "first_attempt_id": attempt_id,
                    "initial_wire": materialized["wire"],
                    "projected_content_digest": materialized["projected_content_digest"],
                    "source_manifest_digest": materialized["source_manifest_digest"],
                    "learning_candidates": _learning_candidates(manifest),
                    "output_token_budget": materialized["max_output_tokens"],
                },
            },
            operation_id=self.ctx.operations.stable(f"model-request:{request_id}"),
            timeout=15,
        )
        return await self._dispatch_attempt(
            graph_id=graph_id,
            graph_revision=graph_revision,
            bot_id=bot_id,
            profile=profile,
            materialized=materialized,
            owner=owner,
            request_id=request_id,
            attempt_id=attempt_id,
            effect_operation_id=effect_operation_id,
        )

    async def resume_frozen(
        self, *, owner: dict, request_id: str, attempt_id: str,
        effect_operation_id: str,
    ) -> dict:
        """Dispatch an unstarted or proven-unsent request from frozen bytes."""

        graph_id, graph_revision, bot_id = self._identity()
        profile, target = await self._profile(request_id)
        frozen = await self.ctx.queries.call(
            "agent.frozen-model-resume",
            {"run_id": owner["run_id"], "request_id": request_id},
        )
        if (
            frozen["request_id"] != request_id
            or frozen["provider_profile_ref"] != profile["profile_ref"]
            or frozen["capability_digest"] != profile["capability_digest"]
            or frozen["model_ref"] != target.model
            or frozen["projection"]["owner_id"] != request_id
        ):
            raise ForestError(
                "MODEL_REQUEST_PROVIDER_REBIND_REQUIRED",
                "Frozen request is bound to an incompatible Provider profile",
                409,
            )
        projection_bytes = await _frozen_payload_result(
            self.ctx.payloads.read_bytes(frozen["projection"])
        )
        wire_bytes = await _frozen_payload_result(
            self.ctx.payloads.read_bytes(frozen["wire"])
        )
        try:
            manifest = json.loads(projection_bytes)
            body = json.loads(wire_bytes)
        except (TypeError, ValueError) as exc:
            raise ForestError(
                "AGENT_FROZEN_MODEL_INVALID",
                "Frozen model Payload is not valid JSON",
                409,
            ) from exc
        if (
            not isinstance(manifest, dict)
            or not isinstance(body, dict)
            or manifest.get("request_id") != request_id
            or manifest.get("projected_content_digest")
                != frozen["projected_content_digest"]
            or _digest(manifest.get("source_manifest"))
                != frozen["source_manifest_digest"]
            or body.get("model") != target.model
            or type(body.get("stream")) is not bool
            or body.get(target.output_token_parameter)
                != frozen["output_token_budget"]
            or not isinstance(manifest.get("serializer_revision"), str)
        ):
            raise ForestError(
                "AGENT_FROZEN_MODEL_INVALID",
                "Frozen projection or wire differs from its request identity",
                409,
            )
        if frozen["provider_generation_id"] != profile["generation_id"]:
            successor_id = await self._inherit_frozen(
                owner=owner,
                frozen=frozen,
                profile=profile,
                target=target,
                manifest=manifest,
                wire_bytes=wire_bytes,
            )
            successor_attempt_id = stable("model-attempt", successor_id, 1)
            return await self.resume_frozen(
                owner=owner,
                request_id=successor_id,
                attempt_id=successor_attempt_id,
                effect_operation_id=stable("model-effect", successor_attempt_id),
            )
        if frozen["state"] == "READY":
            if frozen["wire"]["owner_id"] != attempt_id:
                raise ForestError(
                    "AGENT_FROZEN_MODEL_INVALID",
                    "Initial wire belongs to another transport attempt",
                    409,
                )
            wire = frozen["wire"]
        else:
            wire = await self.ctx.payloads.put_bytes(
                wire_bytes,
                owner_kind="model.wire",
                owner_id=attempt_id,
                media_type="application/json",
                parents=(frozen["projection"], frozen["wire"]),
            )
        if wire["digest"] != frozen["wire"]["digest"]:
            raise ForestError(
                "AGENT_FROZEN_MODEL_INVALID",
                "Retried wire differs from the proven unsent attempt",
                409,
            )
        materialized = {
            "projection": frozen["projection"],
            "wire": wire,
            "max_output_tokens": frozen["output_token_budget"],
            "stream": body["stream"],
            "serializer_revision": manifest["serializer_revision"],
        }
        return await self._dispatch_attempt(
            graph_id=graph_id,
            graph_revision=graph_revision,
            bot_id=bot_id,
            profile=profile,
            materialized=materialized,
            owner=owner,
            request_id=request_id,
            attempt_id=attempt_id,
            effect_operation_id=effect_operation_id,
        )

    async def _inherit_frozen(
        self, *, owner: dict, frozen: dict, profile: dict,
        target: ModelCapabilities, manifest: dict, wire_bytes: bytes,
    ) -> str:
        predecessor_id = frozen["request_id"]
        successor_id = stable(
            "model-request-inherited", predecessor_id, profile["generation_id"]
        )
        successor_attempt_id = stable("model-attempt", successor_id, 1)
        successor_manifest = {**manifest, "request_id": successor_id}
        projection_write = self.ctx.payloads.put_bytes(
            json.dumps(
                successor_manifest, sort_keys=True, separators=(",", ":"),
                ensure_ascii=False, allow_nan=False,
            ).encode("utf-8"),
            owner_kind="model.projection", owner_id=successor_id,
            media_type="application/json", parents=(frozen["projection"],),
        )
        projection = await _frozen_payload_result(projection_write)
        wire_write = self.ctx.payloads.put_bytes(
            wire_bytes,
            owner_kind="model.wire", owner_id=successor_attempt_id,
            media_type="application/json", parents=(frozen["wire"], projection),
        )
        wire = await _frozen_payload_result(wire_write)
        if wire["digest"] != frozen["wire"]["digest"]:
            raise ForestError(
                "AGENT_FROZEN_MODEL_INVALID",
                "Inherited wire differs from the frozen predecessor", 409,
            )
        inheritance = self.ctx.uow.call(
            "agent.inherit-model-request",
            {
                "effect": {
                    "protocol": "forest.effect-unsent-proof-request/1",
                    "attempts": [{
                        "effect_operation_id": item["effect_operation_id"],
                        "attempt_id": item["attempt_id"],
                    } for item in frozen["attempts"]],
                },
                "owner": owner,
                "predecessor_request_id": predecessor_id,
                "successor": {
                    "request_id": successor_id,
                    "first_attempt_id": successor_attempt_id,
                    "provider_component_id": profile["component_id"],
                    "provider_generation_id": profile["generation_id"],
                    "provider_profile_ref": profile["profile_ref"],
                    "model_ref": target.model,
                    "instruction_channel": {
                        "system": "SYSTEM", "developer": "DEVELOPER",
                    }[target.system_role],
                    "capability_digest": profile["capability_digest"],
                    "projection": projection,
                    "initial_wire": wire,
                    "projected_content_digest": frozen["projected_content_digest"],
                    "source_manifest_digest": frozen["source_manifest_digest"],
                    "output_token_budget": frozen["output_token_budget"],
                },
            },
            operation_id=stable(
                "inherit-model-request", predecessor_id, profile["generation_id"]
            ),
            timeout=15,
        )
        await _frozen_payload_result(inheritance)
        return successor_id

    async def _dispatch_attempt(
        self, *, graph_id: str, graph_revision: int, bot_id: str,
        profile: dict, materialized: dict, owner: dict, request_id: str,
        attempt_id: str, effect_operation_id: str,
    ) -> dict:
        input_digest = _digest(
            {
                "profile_ref": profile["profile_ref"],
                "capability_digest": profile["capability_digest"],
                "payload_digest": materialized["wire"]["digest"],
            }
        )
        permit_operation_id = self.ctx.operations.stable(f"effect-permit:{attempt_id}")
        ticket_id = self.ctx.operations.stable(f"authorization-ticket:{attempt_id}")
        expires_at = (datetime.now(UTC) + timedelta(seconds=30)).isoformat()
        dispatch = {
            "protocol": "forest.provider-dispatch/1",
            "effect_operation_id": effect_operation_id,
            "authorization_ticket_id": ticket_id,
            "component_id": profile["component_id"],
            "generation_id": profile["generation_id"],
            "profile_ref": profile["profile_ref"],
            "capability_digest": profile["capability_digest"],
            "prepared": {
                "request_id": request_id,
                "transport_attempt_id": attempt_id,
                "projection": materialized["projection"],
                "wire": materialized["wire"],
                "payload_digest": materialized["wire"]["digest"],
                "max_output_tokens": materialized["max_output_tokens"],
                "stream": materialized["stream"],
                "serializer_revision": materialized["serializer_revision"],
            },
        }
        prepared_effect = await self.ctx.uow.call(
            "agent.prepare-model-effect",
            {
                "authorization": {
                    "protocol": "forest.authorization-issue/1",
                    "ticket_id": ticket_id,
                    "issuance_operation_id": self.ctx.operations.stable(
                        f"authorization-issue:{attempt_id}"
                    ),
                    "operation_id": permit_operation_id,
                    "action": "provider.invoke",
                    "resource_kind": "bot",
                    "resource_id": bot_id,
                    "input_digest": input_digest,
                    "plan_digest": profile["capability_digest"],
                    "expires_at": expires_at,
                    "audience_component_id": profile["component_id"],
                    "audience_generation_id": profile["generation_id"],
                },
                "effect": {
                    "protocol": "forest.effect-command-prepare/1",
                    "effect_operation_id": effect_operation_id,
                    "input_digest": input_digest,
                    "effect_kind": "provider.model.dispatch",
                    "target_kind": "component.service",
                    "target_id": "provider.chat-completions/provider.model.dispatch",
                    "attempt_id": attempt_id,
                    "permit_operation_id": permit_operation_id,
                    "authorization_step_id": "authorization",
                    "authorization": {
                        "action": "provider.invoke",
                        "resource_kind": "bot",
                        "resource_id": bot_id,
                        "plan_digest": profile["capability_digest"],
                    },
                    "executor": {
                        "component_id": profile["component_id"],
                        "generation_id": profile["generation_id"],
                        "fence_kind": "runtime.graph",
                        "fence_id": graph_id,
                        "fence_epoch": graph_revision,
                    },
                    "deadline_at": expires_at,
                    "command": {
                        "value": dispatch,
                        "parents": [materialized["projection"], materialized["wire"]],
                    },
                },
                "agent": {
                    "owner": owner,
                    "request_id": request_id,
                    "attempt_id": attempt_id,
                    "effect_operation_ref": effect_operation_id,
                    "provider_service_id": "provider.model.dispatch",
                    "effect_step_id": "effect",
                },
            },
            operation_id=self.ctx.operations.stable(f"prepare-model-effect:{attempt_id}"),
            timeout=15,
        )
        if prepared_effect.get("authorization_ticket_id") != ticket_id:
            raise ForestError(
                "AGENT_EFFECT_AUTHORIZATION_CONFLICT",
                "Prepared Effect returned another authorization ticket",
                409,
            )
        await self.ctx.uow.call(
            "agent.handoff-model-effect",
            {
                "effect": {
                    "protocol": "forest.effect-handoff/1",
                    "permit": prepared_effect["permit"],
                    "handoff_operation_id": self.ctx.operations.stable(
                        f"effect-handoff:{attempt_id}"
                    ),
                },
                "agent": {
                    "owner": owner,
                    "request_id": request_id,
                    "attempt_id": attempt_id,
                },
            },
            operation_id=self.ctx.operations.stable(f"handoff-model-effect:{attempt_id}"),
            timeout=15,
        )
        try:
            raw_receipt = await self.ctx.services.call(
                "provider.model.dispatch",
                dispatch,
                operation_id=effect_operation_id,
                contract_major=2,
                timeout=120,
            )
            receipt = _receipt(
                raw_receipt,
                request_id=request_id,
                attempt_id=attempt_id,
                payload_digest=materialized["wire"]["digest"],
            )
        except Exception:
            receipt = {
                "status": "UNKNOWN",
                "request_id": request_id,
                "transport_attempt_id": attempt_id,
                "payload_digest": materialized["wire"]["digest"],
                "result": None,
                "usage": None,
                "error_code": "PROVIDER_DISPATCH_RESULT_UNKNOWN",
            }
        observed_state = {
            "RESPONDED": "SUCCEEDED",
            "NOT_SENT": "NOT_APPLIED",
            "UNKNOWN": "UNKNOWN",
        }[receipt["status"]]
        parents = [item for item in (receipt["result"], receipt["usage"]) if item is not None]
        payloads = [
            {"role": role, "payload": item}
            for role, item in (("RESULT", receipt["result"]), ("USAGE", receipt["usage"]))
            if item is not None
        ]
        observation_id = self.ctx.operations.stable(f"model-observation:{attempt_id}")
        await self.ctx.uow.call(
            "agent.observe-model-effect",
            {
                "effect": {
                    "protocol": "forest.effect-command-result/1",
                    "effect_operation_id": effect_operation_id,
                    "attempt_id": attempt_id,
                    "observation_operation_id": observation_id,
                    "status": observed_state,
                    "evidence_kind": "executor",
                    "evidence": {"value": receipt, "parents": parents},
                },
                "agent": {
                    "owner": owner,
                    "observation_operation_id": observation_id,
                    "request_id": request_id,
                    "attempt_id": attempt_id,
                    "observed_state": observed_state,
                    "evidence_kind": "EXECUTOR",
                    "effect_observation_ref": self.ctx.operations.stable(
                        f"effect-observation:{attempt_id}"
                    ),
                    "effect_step_id": "effect",
                    "evidence_digest": _digest(receipt),
                    "payloads": payloads,
                },
            },
            operation_id=self.ctx.operations.stable(f"observe-model-effect:{attempt_id}"),
            timeout=15,
        )
        if receipt["status"] != "RESPONDED":
            return {
                "status": receipt["status"],
                "request_id": request_id,
                "attempt_id": attempt_id,
                "error_code": receipt["error_code"],
                "result": None,
            }
        result = _provider_result(
            await self.ctx.payloads.read_json(receipt["result"]), receipt=receipt
        )
        return {
            "status": "RESPONDED",
            "request_id": request_id,
            "attempt_id": attempt_id,
            "error_code": None,
            "result_ref": receipt["result"],
            "result": result,
        }
