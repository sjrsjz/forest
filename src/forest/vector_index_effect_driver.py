"""Generation-owned Milvus drivers for the builtin derived vector index."""

from __future__ import annotations

import asyncio
import inspect
import json
import math
import re
from collections.abc import Mapping

from pymilvus import AsyncMilvusClient, DataType
from pymilvus.exceptions import MilvusException

from forest.errors import ForestError
from forest.storage.vector_index_authority import VectorIndexIdentityRepository
from forest.vector_index_builtin import validate_vector_index_configuration

_REF = re.compile(r"[^\x00]{1,256}\Z")
_SERVICES = frozenset(
    {"vector.index.upsert", "vector.index.search", "vector.index.delete"}
)


async def _call(port, *args):
    result = port(*args)
    return await result if inspect.isawaitable(result) else result


class MilvusVectorIndexEffectDriver:
    def __init__(
        self,
        configuration,
        configuration_revision,
        identity,
        declaration,
        *,
        bot_id,
        runtime,
        token_resolver,
        client_factory=AsyncMilvusClient,
    ) -> None:
        if (
            declaration.get("kind") != "vector-index.milvus"
            or declaration.get("name") not in _SERVICES
        ):
            raise ForestError(
                "PACKAGE_EFFECT_DRIVER_BINDING_INVALID",
                "Milvus driver received another effect service",
                500,
            )
        self.configuration = validate_vector_index_configuration(configuration)
        self.configuration_revision = configuration_revision
        self.identity = identity
        self.service_id = declaration["name"]
        self.bot_id = bot_id
        self.runtime = runtime
        self.token_resolver = token_resolver
        self.client_factory = client_factory
        self.collection_name = VectorIndexIdentityRepository.collection_name(
            bot_id, configuration["space_id"]
        )
        self.client = None
        self.accepting = False

    async def start(self) -> None:
        token = await _call(
            self.token_resolver,
            self.configuration["credential_ref"],
            self.configuration_revision,
        )
        if not isinstance(token, str):
            raise ForestError(
                "VECTOR_INDEX_CREDENTIAL_UNAVAILABLE",
                "Vector index credential is unavailable",
                503,
            )
        self.client = self.client_factory(
            uri=self.configuration["endpoint"],
            token=token,
            timeout=float(self.configuration["timeout_seconds"]),
        )
        await self._ensure_collection()
        self.accepting = True

    async def _ensure_collection(self) -> None:
        try:
            async with asyncio.timeout(float(self.configuration["timeout_seconds"])):
                if await self.client.has_collection(collection_name=self.collection_name):
                    description = await self.client.describe_collection(
                        collection_name=self.collection_name
                    )
                    fields = {item["name"]: item for item in description.get("fields", [])}
                    vector = fields.get("embedding", {})
                    if int(vector.get("params", {}).get("dim", -1)) != self.configuration[
                        "dimensions"
                    ]:
                        raise ForestError(
                            "VECTOR_INDEX_SPACE_IMMUTABLE",
                            "Milvus collection dimension differs",
                            409,
                        )
                    return
                schema = self.client.create_schema(
                    auto_id=False, enable_dynamic_field=False
                )
                schema.add_field("ref_id", DataType.VARCHAR, is_primary=True, max_length=256)
                schema.add_field("bot_id", DataType.VARCHAR, max_length=256)
                schema.add_field("space_id", DataType.VARCHAR, max_length=256)
                schema.add_field("purpose", DataType.VARCHAR, max_length=16)
                schema.add_field("source_unit_id", DataType.VARCHAR, max_length=256)
                schema.add_field("source_revision", DataType.INT64)
                schema.add_field("modality", DataType.VARCHAR, max_length=16)
                schema.add_field(
                    "embedding",
                    DataType.FLOAT_VECTOR,
                    dim=self.configuration["dimensions"],
                )
                indexes = self.client.prepare_index_params()
                indexes.add_index(
                    field_name="embedding", index_type="AUTOINDEX", metric_type="COSINE"
                )
                await self.client.create_collection(
                    collection_name=self.collection_name,
                    schema=schema,
                    index_params=indexes,
                    consistency_level="Strong",
                )
        except ForestError:
            raise
        except (TimeoutError, MilvusException, OSError, KeyError, TypeError, ValueError) as exc:
            raise ForestError(
                "VECTOR_INDEX_UNAVAILABLE", "Milvus collection is unavailable", 503
            ) from exc

    async def _admit(self, scope) -> None:
        if (
            not self.accepting
            or self.client is None
            or scope.component_id != self.identity.component_id
            or scope.generation_id != self.identity.generation_id
        ):
            raise ForestError(
                "RUNTIME_GENERATION_NOT_ACTIVE",
                "Vector index Generation is not accepting calls",
                409,
            )
        await self.runtime.verify_generation_member(
            component_id=self.identity.component_id,
            generation_id=self.identity.generation_id,
        )

    def _check_space(self, space_id: str) -> None:
        if space_id != self.configuration["space_id"]:
            raise ForestError(
                "VECTOR_INDEX_SPACE_MISMATCH",
                "Vector belongs to another index space",
                409,
            )

    async def invoke(self, payload, *, scope, invocation):
        await self._admit(scope)
        self._check_space(payload["space_id"])
        if self.service_id == "vector.index.upsert":
            return await self._upsert(payload)
        if self.service_id == "vector.index.search":
            return await self._search(payload)
        return await self._delete(payload)

    async def _upsert(self, payload):
        values = tuple(float(item) for item in payload["vector"])
        if (
            _REF.fullmatch(payload["ref_id"]) is None
            or _REF.fullmatch(payload["source_unit_id"]) is None
            or len(values) != self.configuration["dimensions"]
            or any(not math.isfinite(item) for item in values)
        ):
            raise ForestError(
                "VECTOR_INDEX_INPUT_INVALID", "Vector index row is invalid", 422
            )
        row = {
            "ref_id": payload["ref_id"],
            "bot_id": self.bot_id,
            "space_id": payload["space_id"],
            "purpose": payload["purpose"],
            "source_unit_id": payload["source_unit_id"],
            "source_revision": payload["source_revision"],
            "modality": payload["modality"],
            "embedding": list(values),
        }
        try:
            async with asyncio.timeout(float(self.configuration["timeout_seconds"])):
                await self.client.upsert(collection_name=self.collection_name, data=[row])
        except (TimeoutError, MilvusException, OSError) as exc:
            raise ForestError(
                "VECTOR_INDEX_UPSERT_UNAVAILABLE", "Milvus upsert failed", 503
            ) from exc
        return {"ref_id": payload["ref_id"], "space_id": payload["space_id"]}

    async def _search(self, payload):
        try:
            async with asyncio.timeout(float(self.configuration["timeout_seconds"])):
                query_rows = await self.client.get(
                    collection_name=self.collection_name,
                    ids=[payload["query_ref_id"]],
                    output_fields=["embedding", "purpose", "space_id"],
                    consistency_level="Strong",
                )
                if len(query_rows) != 1 or query_rows[0].get("purpose") != "QUERY":
                    raise ForestError(
                        "VECTOR_INDEX_QUERY_REF_MISSING",
                        "Query vector ref is unavailable",
                        409,
                    )
                bot_literal = json.dumps(self.bot_id, ensure_ascii=False)
                space_literal = json.dumps(payload["space_id"], ensure_ascii=False)
                raw = await self.client.search(
                    collection_name=self.collection_name,
                    data=[query_rows[0]["embedding"]],
                    anns_field="embedding",
                    filter=(
                        f"bot_id == {bot_literal} && space_id == {space_literal} "
                        '&& purpose == "DOCUMENT"'
                    ),
                    limit=payload["limit"],
                    output_fields=["source_unit_id", "source_revision", "modality"],
                    search_params={"metric_type": "COSINE", "params": {}},
                    consistency_level="Strong",
                )
        except ForestError:
            raise
        except (TimeoutError, MilvusException, OSError, KeyError, TypeError) as exc:
            raise ForestError(
                "VECTOR_INDEX_SEARCH_UNAVAILABLE", "Milvus search failed", 503
            ) from exc
        hits = raw[0] if isinstance(raw, list) and raw else []
        result = []
        for hit in hits:
            entity = hit.get("entity", {}) if isinstance(hit, Mapping) else {}
            ref_id = hit.get("ref_id", hit.get("id")) if isinstance(hit, Mapping) else None
            score = hit.get("distance") if isinstance(hit, Mapping) else None
            if (
                not isinstance(ref_id, str)
                or type(score) not in {int, float}
                or not math.isfinite(float(score))
                or not isinstance(entity.get("source_unit_id"), str)
                or type(entity.get("source_revision")) is not int
                or entity.get("modality") not in {"TEXT", "IMAGE"}
            ):
                raise ForestError(
                    "VECTOR_INDEX_RESPONSE_INVALID", "Milvus returned an invalid hit", 502
                )
            result.append(
                {
                    "ref_id": ref_id,
                    "score": float(score),
                    "source_unit_id": entity["source_unit_id"],
                    "source_revision": entity["source_revision"],
                    "modality": entity["modality"],
                }
            )
        return {
            "query_ref_id": payload["query_ref_id"],
            "space_id": payload["space_id"],
            "hits": result,
        }

    async def _delete(self, payload):
        refs = tuple(payload["ref_ids"])
        if any(_REF.fullmatch(item) is None for item in refs):
            raise ForestError(
                "VECTOR_INDEX_INPUT_INVALID", "Vector deletion refs are invalid", 422
            )
        try:
            async with asyncio.timeout(float(self.configuration["timeout_seconds"])):
                await self.client.delete(
                    collection_name=self.collection_name, ids=list(refs)
                )
        except (TimeoutError, MilvusException, OSError) as exc:
            raise ForestError(
                "VECTOR_INDEX_DELETE_UNAVAILABLE", "Milvus deletion failed", 503
            ) from exc
        return {"ref_ids": list(refs), "space_id": payload["space_id"]}

    async def quiesce(self) -> None:
        self.accepting = False

    async def resume(self) -> None:
        if self.client is None:
            await self.start()
        else:
            self.accepting = True

    async def close(self) -> None:
        self.accepting = False
        client, self.client = self.client, None
        if client is not None:
            await client.close()


def milvus_vector_index_effect_driver_factory(
    *, bot_id, runtime, token_resolver, client_factory=AsyncMilvusClient
):
    def create(configuration, configuration_revision, identity, declaration):
        return MilvusVectorIndexEffectDriver(
            configuration,
            configuration_revision,
            identity,
            declaration,
            bot_id=bot_id,
            runtime=runtime,
            token_resolver=token_resolver,
            client_factory=client_factory,
        )

    return create
