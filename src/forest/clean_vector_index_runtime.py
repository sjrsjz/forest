"""Generation-bound Milvus adapter for rebuildable Memory vector artifacts."""

from __future__ import annotations

import asyncio
import json
import math
import re
from collections.abc import Mapping

from pymilvus import AsyncMilvusClient, DataType
from pymilvus.exceptions import MilvusException

from forest.errors import ForestError
from forest.runtime_generation import BoundGenerationIdentity, PreparedGenerationBinding

_STATE_CONTRACT = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "type": "object",
    "properties": {},
    "additionalProperties": False,
}
_REF = re.compile(r"[^\x00]{1,256}\Z")


class CleanMilvusVectorIndexRuntime:
    """Own only derived vectors; authoritative content remains in Memory/PostgreSQL."""

    runtime_contract = {
        "protocol": "forest.runtime-services/1",
        "services": [
            {"service_id": "vector.index.upsert", "version": 1},
            {"service_id": "vector.index.search", "version": 1},
            {"service_id": "vector.index.delete", "version": 1},
        ],
    }
    state_schema = 1
    state_contract = _STATE_CONTRACT

    def __init__(
        self,
        *,
        repository,
        module_configuration,
        grants,
        generation_verifier,
        token_resolver,
        client_factory=AsyncMilvusClient,
    ) -> None:
        self.repository = repository
        self.module_configuration = module_configuration
        self.grants = grants
        self.generation_verifier = generation_verifier
        self.token_resolver = token_resolver
        self.client_factory = client_factory
        self.generation_identity: BoundGenerationIdentity | None = None
        self.configuration: dict | None = None
        self.configuration_revision: int | None = None
        self.identity: dict | None = None
        self.client = None
        self.active = False

    def runtime_service_handlers(self) -> dict:
        return {
            ("vector.index.upsert", 1): self.upsert,
            ("vector.index.search", 1): self.search,
            ("vector.index.delete", 1): self.delete,
        }

    @staticmethod
    def _parse_configuration(value: object) -> dict:
        expected = {
            "protocol",
            "endpoint",
            "credential_ref",
            "timeout_seconds",
            "space_id",
            "dimensions",
            "metric",
        }
        if not isinstance(value, dict) or set(value) != expected:
            raise ForestError("VECTOR_INDEX_CONFIGURATION_INVALID", "Vector index configuration differs", 422)
        if (
            value["protocol"] != "forest.vector-index.milvus/1"
            or not isinstance(value["endpoint"], str)
            or not value["endpoint"]
            or not isinstance(value["credential_ref"], str)
            or type(value["timeout_seconds"]) not in {int, float}
            or not 1 <= value["timeout_seconds"] <= 300
            or not isinstance(value["space_id"], str)
            or not value["space_id"]
            or type(value["dimensions"]) is not int
            or not 1 <= value["dimensions"] <= 4096
            or value["metric"] != "COSINE"
        ):
            raise ForestError("VECTOR_INDEX_CONFIGURATION_INVALID", "Vector index configuration is invalid", 422)
        return value

    async def prepare_generation(self, context) -> PreparedGenerationBinding:
        desired = await self.module_configuration.desired(include_value=True)
        configuration = self._parse_configuration(desired.value)
        identity = await self.repository.initialize(
            operation_id="bootstrap:vector.index:identity",
            space_id=configuration["space_id"],
            dimensions=configuration["dimensions"],
            metric=configuration["metric"],
        )
        self.configuration = configuration
        self.configuration_revision = desired.revision
        self.identity = identity
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
            instance_id=f"instance:vector.index:{context.activation_id}",
            worker_incarnation=f"worker:vector.index:{context.activation_id}",
            state_schema=context.state_schema,
            state_contract_digest=context.state_contract_digest,
            capability_grants_digest=self.grants.grant_set_digest,
            data_admission=True,
            control_admission=True,
            storage=context.storage,
        )

    async def bind_generation(self, identity: BoundGenerationIdentity) -> None:
        if identity.component_id != "vector.index":
            raise ForestError("RUNTIME_GENERATION_BINDING_MISMATCH", "Vector index binding differs", 409)
        if self.generation_identity is not None and self.generation_identity != identity:
            raise ForestError("RUNTIME_GENERATION_BINDING_MISMATCH", "Vector index Generation is immutable", 409)
        self.generation_identity = identity

    async def start(self) -> None:
        if self.configuration is None or self.identity is None or self.generation_identity is None:
            raise ForestError("RUNTIME_GENERATION_IDENTITY_UNAVAILABLE", "Vector index was not prepared", 503)
        token = await self.token_resolver(
            self.configuration["credential_ref"], self.configuration_revision
        )
        self.client = self.client_factory(
            uri=self.configuration["endpoint"],
            token=token or "",
            timeout=float(self.configuration["timeout_seconds"]),
        )
        await self._ensure_collection()

    def activate_generation(self) -> None:
        self.active = True

    async def quiesce(self) -> None:
        self.active = False

    async def close(self) -> None:
        self.active = False
        if self.client is not None:
            await self.client.close()
            self.client = None

    def owned_tasks(self) -> tuple:
        return ()

    async def _admit(self) -> None:
        if not self.active or self.generation_identity is None:
            raise ForestError("RUNTIME_GENERATION_NOT_ACTIVE", "Vector index is not accepting calls", 409)
        await self.generation_verifier(
            component_id="vector.index",
            generation_id=self.generation_identity.generation_id,
            admission_lane="DATA",
        )

    async def _ensure_collection(self) -> None:
        assert self.client is not None and self.identity is not None
        name = self.identity["collection_name"]
        try:
            async with asyncio.timeout(float(self.configuration["timeout_seconds"])):
                if await self.client.has_collection(collection_name=name):
                    description = await self.client.describe_collection(collection_name=name)
                    fields = {item["name"]: item for item in description.get("fields", [])}
                    vector = fields.get("embedding", {})
                    if int(vector.get("params", {}).get("dim", -1)) != self.identity["dimensions"]:
                        raise ForestError("VECTOR_INDEX_SPACE_IMMUTABLE", "Milvus collection dimension differs", 409)
                    return
                schema = self.client.create_schema(auto_id=False, enable_dynamic_field=False)
                schema.add_field("ref_id", DataType.VARCHAR, is_primary=True, max_length=256)
                schema.add_field("bot_id", DataType.VARCHAR, max_length=256)
                schema.add_field("space_id", DataType.VARCHAR, max_length=256)
                schema.add_field("purpose", DataType.VARCHAR, max_length=16)
                schema.add_field("source_unit_id", DataType.VARCHAR, max_length=256)
                schema.add_field("source_revision", DataType.INT64)
                schema.add_field("modality", DataType.VARCHAR, max_length=16)
                schema.add_field(
                    "embedding", DataType.FLOAT_VECTOR, dim=self.identity["dimensions"]
                )
                indexes = self.client.prepare_index_params()
                indexes.add_index(
                    field_name="embedding", index_type="AUTOINDEX", metric_type="COSINE"
                )
                await self.client.create_collection(
                    collection_name=name,
                    schema=schema,
                    index_params=indexes,
                    consistency_level="Strong",
                )
        except ForestError:
            raise
        except (TimeoutError, MilvusException, OSError, KeyError, TypeError, ValueError) as exc:
            raise ForestError("VECTOR_INDEX_UNAVAILABLE", "Milvus collection is unavailable", 503) from exc

    def _check_space(self, space_id: str) -> None:
        if self.identity is None or space_id != self.identity["space_id"]:
            raise ForestError("VECTOR_INDEX_SPACE_MISMATCH", "Vector belongs to another index space", 409)

    async def upsert(
        self,
        *,
        ref_id: str,
        space_id: str,
        purpose: str,
        source_unit_id: str,
        source_revision: int,
        modality: str,
        vector,
    ) -> dict:
        await self._admit()
        self._check_space(space_id)
        values = tuple(float(item) for item in vector)
        if (
            _REF.fullmatch(ref_id) is None
            or _REF.fullmatch(source_unit_id) is None
            or purpose not in {"DOCUMENT", "QUERY"}
            or modality not in {"TEXT", "IMAGE"}
            or type(source_revision) is not int
            or source_revision <= 0
            or len(values) != self.identity["dimensions"]
            or any(not math.isfinite(item) for item in values)
        ):
            raise ForestError("VECTOR_INDEX_INPUT_INVALID", "Vector index row is invalid", 422)
        row = {
            "ref_id": ref_id,
            "bot_id": self.repository.store.bot_id,
            "space_id": space_id,
            "purpose": purpose,
            "source_unit_id": source_unit_id,
            "source_revision": source_revision,
            "modality": modality,
            "embedding": list(values),
        }
        try:
            async with asyncio.timeout(float(self.configuration["timeout_seconds"])):
                await self.client.upsert(
                    collection_name=self.identity["collection_name"], data=[row]
                )
        except (TimeoutError, MilvusException, OSError) as exc:
            raise ForestError("VECTOR_INDEX_UPSERT_UNAVAILABLE", "Milvus upsert failed", 503) from exc
        return {"ref_id": ref_id, "space_id": space_id}

    async def search(self, *, query_ref_id: str, space_id: str, limit: int) -> tuple[dict, ...]:
        await self._admit()
        self._check_space(space_id)
        if _REF.fullmatch(query_ref_id) is None or type(limit) is not int or not 1 <= limit <= 256:
            raise ForestError("VECTOR_INDEX_INPUT_INVALID", "Vector search input is invalid", 422)
        try:
            async with asyncio.timeout(float(self.configuration["timeout_seconds"])):
                query_rows = await self.client.get(
                    collection_name=self.identity["collection_name"],
                    ids=[query_ref_id],
                    output_fields=["embedding", "purpose", "space_id"],
                    consistency_level="Strong",
                )
                if len(query_rows) != 1 or query_rows[0].get("purpose") != "QUERY":
                    raise ForestError("VECTOR_INDEX_QUERY_REF_MISSING", "Query vector ref is unavailable", 409)
                bot_literal = json.dumps(self.repository.store.bot_id, ensure_ascii=False)
                space_literal = json.dumps(space_id, ensure_ascii=False)
                raw = await self.client.search(
                    collection_name=self.identity["collection_name"],
                    data=[query_rows[0]["embedding"]],
                    anns_field="embedding",
                    filter=(
                        f"bot_id == {bot_literal} && space_id == {space_literal} "
                        '&& purpose == "DOCUMENT"'
                    ),
                    limit=limit,
                    output_fields=["source_unit_id", "source_revision", "modality"],
                    search_params={"metric_type": "COSINE", "params": {}},
                    consistency_level="Strong",
                )
        except ForestError:
            raise
        except (TimeoutError, MilvusException, OSError, KeyError, TypeError) as exc:
            raise ForestError("VECTOR_INDEX_SEARCH_UNAVAILABLE", "Milvus search failed", 503) from exc
        hits = raw[0] if isinstance(raw, list) and raw else []
        result = []
        for hit in hits:
            entity = hit.get("entity", {}) if isinstance(hit, Mapping) else {}
            ref_id = (
                hit.get("ref_id", hit.get("id")) if isinstance(hit, Mapping) else None
            )
            score = hit.get("distance") if isinstance(hit, Mapping) else None
            if not isinstance(ref_id, str) or type(score) not in {int, float}:
                raise ForestError("VECTOR_INDEX_RESPONSE_INVALID", "Milvus returned an invalid hit", 502)
            result.append(
                {
                    "ref_id": ref_id,
                    "score": float(score),
                    "source_unit_id": entity.get("source_unit_id"),
                    "source_revision": entity.get("source_revision"),
                    "modality": entity.get("modality"),
                }
            )
        return tuple(result)

    async def delete(self, *, ref_ids, space_id: str) -> None:
        await self._admit()
        self._check_space(space_id)
        refs = tuple(dict.fromkeys(ref_ids))
        if not refs:
            return
        if len(refs) > 1024 or any(not isinstance(item, str) or _REF.fullmatch(item) is None for item in refs):
            raise ForestError("VECTOR_INDEX_INPUT_INVALID", "Vector deletion refs are invalid", 422)
        try:
            async with asyncio.timeout(float(self.configuration["timeout_seconds"])):
                await self.client.delete(
                    collection_name=self.identity["collection_name"], ids=list(refs)
                )
        except (TimeoutError, MilvusException, OSError) as exc:
            raise ForestError("VECTOR_INDEX_DELETE_UNAVAILABLE", "Milvus deletion failed", 503) from exc
