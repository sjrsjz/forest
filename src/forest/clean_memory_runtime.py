"""Generation-bound runtime shell for the Bot-global multimodal Memory graph."""

from __future__ import annotations

import asyncio
import hashlib
import json
from dataclasses import dataclass
from datetime import datetime

from forest.builtin_memory_shell import execute_memory_shell
from forest.embeddings import EmbeddingProfile
from forest.errors import ForestError
from forest.memory_embedding_service import CleanMemoryEmbeddingService
from forest.models import MediaSnapshot
from forest.packages import digest_json
from forest.runtime_generation import BoundGenerationIdentity, PreparedGenerationBinding
from forest.storage.memory_embedding_budget import MemoryEmbeddingBudgetRepository
from forest.storage.payload_authority import PayloadSource

_STATE_CONTRACT = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "type": "object",
    "properties": {},
    "additionalProperties": False,
}


@dataclass(frozen=True)
class FrozenMemoryProfile:
    embedding: EmbeddingProfile
    fusion_policy_revision: str
    budget_policy_revision: str
    daily_request_limit: int
    daily_token_limit: int
    reservation_tokens: int


@dataclass(frozen=True)
class MemoryRecallResult:
    evidence: dict
    media: dict


class CleanMemoryRuntime:
    """Expose attributed graph reads while Memory owns its complete private schema."""

    runtime_contract = {
        "protocol": "forest.runtime-services/1",
        "services": [
            {"service_id": "memory.graph.trunks.list", "version": 1},
            {"service_id": "memory.graph.sources.query", "version": 1},
            {"service_id": "memory.graph.relations.query", "version": 1},
            {"service_id": "memory.recall", "version": 1},
            {"service_id": "memory.source.capture", "version": 1},
            {"service_id": "memory.source.unavailable", "version": 1},
            {"service_id": "shell.memory.inspect", "version": 1},
        ],
        "requires": [{"service_id": "embedding.encode", "version": 1}],
    }
    state_schema = 1
    state_contract = _STATE_CONTRACT

    def __init__(
        self,
        *,
        repository,
        retrieval,
        payloads,
        module_configuration,
        grants,
        generation_verifier,
        generation_admission_verifier,
        generation_member_verifier,
        embedding_provider,
    ) -> None:
        self.repository = repository
        self.retrieval = retrieval
        self.payloads = payloads
        self.module_configuration = module_configuration
        self.grants = grants
        self.generation_verifier = generation_verifier
        self.generation_admission_verifier = generation_admission_verifier
        self.generation_member_verifier = generation_member_verifier
        self.embedding_provider = embedding_provider
        self.generation_identity: BoundGenerationIdentity | None = None
        self.profile: FrozenMemoryProfile | None = None
        self.embedding_service: CleanMemoryEmbeddingService | None = None
        self.configuration_revision: int | None = None
        self.active = False

    def runtime_service_handlers(self) -> dict:
        return {
            ("memory.graph.trunks.list", 1): self.list_trunks,
            ("memory.graph.sources.query", 1): self.query_sources,
            ("memory.graph.relations.query", 1): self.query_relations,
            ("memory.recall", 1): self.recall,
            ("memory.source.capture", 1): self.capture_ingress_source,
            ("memory.source.unavailable", 1): self.mark_source_unavailable,
            ("shell.memory.inspect", 1): self.execute_shell_command,
        }

    async def execute_shell_command(self, request):
        return await execute_memory_shell(self, request)

    @staticmethod
    def _profile(value: object) -> FrozenMemoryProfile:
        if (
            not isinstance(value, dict)
            or set(value) != {
                "protocol",
                "embedding",
                "fusion_policy_revision",
                "embedding_budget",
            }
            or value.get("protocol") != "forest.memory.graph/1"
        ):
            raise ForestError("MEMORY_CONFIGURATION_INVALID", "Memory configuration is invalid", 500)
        embedding = value.get("embedding")
        if not isinstance(embedding, dict) or set(embedding) != {
            "deployment_id",
            "model_revision",
            "model",
            "dimensions",
        }:
            raise ForestError(
                "MEMORY_CONFIGURATION_INVALID", "Embedding space configuration is invalid", 500
            )
        try:
            profile = EmbeddingProfile(**embedding)
        except (TypeError, ValueError, ForestError) as exc:
            raise ForestError(
                "MEMORY_CONFIGURATION_INVALID", "Embedding profile is invalid", 500
            ) from exc
        fusion = value.get("fusion_policy_revision")
        budget = value.get("embedding_budget")
        if (
            not isinstance(fusion, str)
            or not fusion
            or not isinstance(budget, dict)
            or set(budget) != {
                "policy_revision",
                "daily_request_limit",
                "daily_token_limit",
                "reservation_tokens",
            }
            or not isinstance(budget["policy_revision"], str)
            or not budget["policy_revision"]
            or type(budget["daily_request_limit"]) is not int
            or not 1 <= budget["daily_request_limit"] <= 1_000_000
            or type(budget["daily_token_limit"]) is not int
            or not 1 <= budget["daily_token_limit"] <= 1_000_000_000_000
            or type(budget["reservation_tokens"]) is not int
            or not 1 <= budget["reservation_tokens"] <= budget["daily_token_limit"]
        ):
            raise ForestError(
                "MEMORY_CONFIGURATION_INVALID", "Memory fusion configuration is invalid", 500
            )
        return FrozenMemoryProfile(
            profile,
            fusion,
            budget["policy_revision"],
            budget["daily_request_limit"],
            budget["daily_token_limit"],
            budget["reservation_tokens"],
        )

    async def prepare_generation(self, context) -> PreparedGenerationBinding:
        desired = await self.module_configuration.desired(include_value=True)
        profile = self._profile(desired.value)
        await self.repository.initialize_domain(
            operation_id="bootstrap:memory.graph:domain",
            space_id=profile.embedding.space_id,
            embedding_model_kind=profile.embedding.model,
            model_revision_digest=digest_json(profile.embedding.as_dict()),
            dimensions=profile.embedding.dimensions,
            normalization="L2",
            fusion_policy_revision=profile.fusion_policy_revision,
        )
        self.profile = profile
        self.configuration_revision = desired.revision
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
            instance_id=f"instance:memory.graph:{context.activation_id}",
            worker_incarnation=f"worker:memory.graph:{context.activation_id}",
            state_schema=context.state_schema,
            state_contract_digest=context.state_contract_digest,
            capability_grants_digest=self.grants.grant_set_digest,
            data_admission=True,
            control_admission=True,
            storage=context.storage,
        )

    async def bind_generation(self, identity: BoundGenerationIdentity) -> None:
        if identity.component_id != "memory.graph":
            raise ForestError(
                "RUNTIME_GENERATION_BINDING_MISMATCH",
                "Memory received another component's Generation identity",
                409,
            )
        if self.generation_identity is not None and self.generation_identity != identity:
            raise ForestError(
                "RUNTIME_GENERATION_BINDING_MISMATCH",
                "Memory Generation identity is immutable",
                409,
            )
        self.generation_identity = identity

    async def start(self) -> None:
        if self.generation_identity is None or self.profile is None:
            raise ForestError(
                "RUNTIME_GENERATION_IDENTITY_UNAVAILABLE",
                "Memory Generation was not prepared and bound",
                503,
            )
        route = self.embedding_provider.route
        if route.provider_component_id != "embedding.multimodal":
            raise ForestError(
                "EMBEDDING_ROUTE_MISMATCH", "Memory embedding route is invalid", 409
            )
        self.embedding_service = CleanMemoryEmbeddingService(
            retrieval=self.retrieval,
            payloads=self.payloads,
            provider=self.embedding_provider,
            profile=self.profile.embedding,
            component_id="memory.graph",
            generation_id=self.generation_identity.generation_id,
            route_revision=route.graph_id,
            budget=MemoryEmbeddingBudgetRepository(
                self.repository.store, namespace=self.repository.namespace
            ),
            request_limit=self.profile.daily_request_limit,
            token_limit=self.profile.daily_token_limit,
            reserved_tokens=self.profile.reservation_tokens,
            policy_revision=self.profile.budget_policy_revision,
        )
        self.embedding_service.start()

    def activate_generation(self) -> None:
        self.active = True
        if self.embedding_service is not None:
            self.embedding_service.activate()

    async def quiesce(self) -> None:
        if self.embedding_service is not None:
            await self.embedding_service.quiesce()
        self.active = False

    async def close(self) -> None:
        if self.embedding_service is not None:
            await self.embedding_service.close()
        self.active = False

    def notify(self) -> None:
        if self.embedding_service is not None:
            self.embedding_service.wake()

    def owned_tasks(self) -> tuple:
        return self.embedding_service.owned_tasks() if self.embedding_service is not None else ()

    async def _admit(self) -> None:
        if not self.active or self.generation_identity is None:
            raise ForestError(
                "RUNTIME_GENERATION_NOT_ACTIVE",
                "Memory Generation is not accepting calls",
                409,
            )
        await self.generation_verifier(
            component_id="memory.graph",
            generation_id=self.generation_identity.generation_id,
            admission_lane="DATA",
        )

    async def list_trunks(self):
        await self._admit()
        return await self.repository.list_trunks()

    async def query_sources(self, **filters):
        await self._admit()
        return await self.repository.query_sources(**filters)

    async def query_relations(self, **filters):
        await self._admit()
        return await self.repository.query_relations(**filters)

    @staticmethod
    def _canonical(value: object) -> bytes:
        def default(item):
            if isinstance(item, datetime):
                return item.isoformat()
            raise TypeError(type(item).__name__)

        return json.dumps(
            value,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=False,
            allow_nan=False,
            default=default,
        ).encode()

    async def _read_payload_at_epoch(self, payload_id: str, deletion_epoch: int) -> bytes:
        async with self.payloads.store.pool.connection() as conn, conn.transaction():
            return await self.payloads.read_bytes_in(
                conn,
                payload_id,
                expected_deletion_epoch=deletion_epoch,
            )

    async def _render_bundle(
        self,
        bundle: dict,
        *,
        conversation_id: str,
        input_cut: int,
    ) -> MemoryRecallResult:
        if self.profile is None or self.configuration_revision is None:
            raise ForestError("RUNTIME_GENERATION_NOT_ACTIVE", "Memory is not prepared", 409)
        rows = []
        media = {}
        parents = []
        sources = []
        for item in bundle["evidence"]:
            parts = []
            for part in item["parts"]:
                content = await self._read_payload_at_epoch(
                    part["payload_id"],
                    bundle["accepted_deletion_epoch"],
                )
                parents.append(part["payload_id"])
                if part["part_kind"] == "TEXT":
                    try:
                        text = content.decode("utf-8")
                    except UnicodeDecodeError as exc:
                        raise ForestError(
                            "MEMORY_TEXT_INVALID", "Recalled Memory text is not UTF-8", 422
                        ) from exc
                    parts.append(
                        {"part_id": part["part_id"], "type": "text", "text": text}
                    )
                else:
                    media_ref = part["payload_id"]
                    parts.append(
                        {
                            "part_id": part["part_id"],
                            "type": "image",
                            "media_ref": media_ref,
                            "media_revision": item["source_revision"],
                        }
                    )
                    snapshot = MediaSnapshot(
                        self.repository.store.bot_id,
                        media_ref,
                        item["source_revision"],
                        item["source_unit_id"],
                        part["part_id"],
                        part["media_type"],
                        content,
                        part["payload_digest"],
                        part["width"],
                        part["height"],
                    )
                    media[(item["source_unit_id"], part["part_id"])] = snapshot
                    media[media_ref] = snapshot
            if item["source_kind"] == "GROUP":
                source_id = f"onebot:group:{item['external_group_id']}"
                channel_kind = "group"
                channel_id = item["external_group_id"]
            elif item["source_kind"] == "DIRECT":
                source_id = item["source_id"]
                channel_kind = "private"
                channel_id = item["source_id"]
            else:
                source_id = item["source_id"]
                channel_kind = "system"
                channel_id = item["source_id"]
            source = {
                "source_id": source_id,
                "source_kind": item["source_kind"],
                "realm_id": item["realm_id"],
                "platform": item["platform"],
                "channel_kind": channel_kind,
                "channel_id": channel_id,
                "identity_schema": 1,
            }
            if item["source_kind"] == "GROUP":
                source["group_source_id"] = source_id
            sources.append(
                PayloadSource(
                    "memory.graph",
                    "retrieval.evidence",
                    item["source_unit_id"],
                    item["source_revision"],
                    source_trunk_id=item["trunk_id"],
                    author_subject_id=item["author_subject_ref"],
                    occurred_at=item["occurred_at"],
                )
            )
            rows.append(
                {
                    "unit_id": "evidence:" + item["source_unit_id"],
                    "source_unit_id": item["source_unit_id"],
                    "source_revision": item["source_revision"],
                    "source": source,
                    "author": {
                        "principal_ref": item["author_subject_ref"],
                        "identity_origin": "memory_verified",
                    },
                    "parts": parts,
                }
            )
        projection = {
            "protocol": "forest.memory-evidence/1",
            "bundle_id": bundle["bundle_id"],
            "query_id": bundle["query_id"],
            "conversation_id": conversation_id,
            "input_cut": input_cut,
            "space_id": self.profile.embedding.space_id,
            "config_revision": self.configuration_revision,
            "deletion_epoch": bundle["accepted_deletion_epoch"],
            "status": "READY",
            "completeness": bundle["completeness"],
            "evidence": rows,
        }
        epoch = await self.payloads.epoch()
        saved = await self.payloads.put_bytes(
            owner_component_id="memory.graph",
            owner_kind="memory.evidence",
            owner_id=bundle["bundle_id"],
            media_type="application/json",
            content=self._canonical(projection),
            sources=tuple(sources),
            parents=tuple(parents),
            expected_deletion_epoch=epoch,
        )
        return MemoryRecallResult(
            evidence={
                **projection,
                "payload_id": saved.payload_id,
                "digest": saved.content_digest,
            },
            media=media,
        )

    async def recall(
        self,
        *,
        recall_id: str,
        conversation_id: str,
        input_batch_ref: str,
        context_revision: int,
        input_cut: int,
        timeout_seconds: float = 15,
    ) -> MemoryRecallResult | None:
        await self._admit()
        resolved = await self.retrieval.resolve_query_input(
            conversation_ref=conversation_id,
            input_batch_ref=input_batch_ref,
            context_revision=context_revision,
        )
        if resolved is None:
            return None
        identity = hashlib.sha256(recall_id.encode()).hexdigest()
        query_id = "memory-query:" + identity
        epoch = await self.payloads.epoch()
        query = await self.retrieval.register_query(
            operation_id="memory-query-register:" + identity,
            query_id=query_id,
            current_source_id=resolved["current_source_id"],
            input_batch_ref=input_batch_ref,
            context_revision=context_revision,
            parts=resolved["parts"],
            expected_deletion_epoch=epoch,
        )
        self.notify()
        try:
            async with asyncio.timeout(timeout_seconds):
                while True:
                    states = {item["state"] for item in query["facets"]}
                    if states == {"READY"}:
                        break
                    if states & {"FAILED", "UNKNOWN", "SUPPRESSED"}:
                        return None
                    await asyncio.sleep(0.05)
                    query = await self.retrieval.query_view(query_id)
        except TimeoutError:
            return None
        bundle = await self.retrieval.freeze_bundle(
            operation_id="memory-bundle-freeze:" + identity,
            bundle_id="memory-bundle:" + identity,
            query_id=query_id,
            expected_deletion_epoch=epoch,
        )
        return await self._render_bundle(
            bundle,
            conversation_id=conversation_id,
            input_cut=input_cut,
        )

    async def capture_ingress_source(
        self,
        *,
        operation_id: str,
        identity,
        source_unit_id: str,
        source_revision: int,
        unit_kind: str,
        author_subject_ref: str,
        author,
        parts,
        occurrence,
        occurred_at,
    ) -> dict:
        await self._admit()
        async with self.repository.store.pool.connection() as conn, conn.transaction():
            source = await self.repository.register_source_in(
                conn,
                operation_id=operation_id + ":source",
                identity=identity,
            )
            epoch = await self.payloads.epoch_in(conn)
            captured = await self.capture_source_unit_in(
                conn,
                operation_id=operation_id + ":unit",
                source_unit_id=source_unit_id,
                source_id=identity.source_id,
                source_revision=source_revision,
                unit_kind=unit_kind,
                author_subject_ref=author_subject_ref,
                author=author,
                parts=parts,
                occurrence=occurrence,
                occurred_at=occurred_at,
                expected_deletion_epoch=epoch,
            )
        self.notify()
        return {"source": source, "capture": captured}

    async def mark_source_unavailable(
        self,
        *,
        conversation_ref: str,
        event_ref: str,
        error_code: str,
    ) -> dict | None:
        await self._admit()
        return await self.repository.resolve_pending_unavailable(
            conversation_ref=conversation_ref,
            event_ref=event_ref,
            error_code=error_code,
        )

    async def _admit_in(
        self, conn, *, owned_work: bool, admission_lane: str = "DATA"
    ) -> None:
        if not self.active or self.generation_identity is None:
            raise ForestError(
                "RUNTIME_GENERATION_NOT_ACTIVE",
                "Memory Generation is not accepting transaction work",
                409,
            )
        if owned_work:
            await self.generation_member_verifier(
                conn,
                component_id="memory.graph",
                generation_id=self.generation_identity.generation_id,
            )
        else:
            await self.generation_admission_verifier(
                conn,
                component_id="memory.graph",
                generation_id=self.generation_identity.generation_id,
                admission_lane=admission_lane,
            )

    async def qualify_input_batch_in(self, conn, **values):
        await self._admit_in(conn, owned_work=True)
        qualifications = await self.repository.qualify_input_batch_in(conn, **values)
        expected_epoch = values["expected_deletion_epoch"]
        for item in qualifications:
            source_unit_id = item.get("source_unit_id")
            if source_unit_id is None or item.get("state") == "PENDING_CAPTURE":
                continue
            await self._prepare_source_facets_in(
                conn,
                source_unit_id=source_unit_id,
                expected_deletion_epoch=expected_epoch,
            )
        return qualifications

    async def register_source_in(self, conn, **values):
        admission_lane = values.pop("admission_lane", "DATA")
        await self._admit_in(conn, owned_work=False, admission_lane=admission_lane)
        return await self.repository.register_source_in(conn, **values)

    async def capture_source_unit_in(self, conn, **values):
        admission_lane = values.pop("admission_lane", "DATA")
        await self._admit_in(conn, owned_work=False, admission_lane=admission_lane)
        captured = await self.repository.capture_source_unit_in(conn, **values)
        if captured["eligibility_state"] == "ELIGIBLE":
            await self._prepare_source_facets_in(
                conn,
                source_unit_id=values["source_unit_id"],
                expected_deletion_epoch=values["expected_deletion_epoch"],
            )
        return captured

    async def _prepare_source_facets_in(
        self, conn, *, source_unit_id: str, expected_deletion_epoch: int
    ) -> dict:
        operation_id = "source-facets:" + hashlib.sha256(
            (self.repository.store.bot_id + "\0" + source_unit_id).encode("utf-8")
        ).hexdigest()
        return await self.retrieval.prepare_source_facets(
            operation_id=operation_id,
            source_unit_id=source_unit_id,
            expected_deletion_epoch=expected_deletion_epoch,
            _connection=conn,
        )

    async def deletion_epoch_in(self, conn) -> int:
        await self._admit_in(conn, owned_work=True)
        if self.repository.deletion_epoch_guard is None:
            raise ForestError(
                "PAYLOAD_AUTHORITY_MISSING",
                "Memory deletion epoch authority is unavailable",
                503,
            )
        return await self.repository.deletion_epoch_guard(conn)
