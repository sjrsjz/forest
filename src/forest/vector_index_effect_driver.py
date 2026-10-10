"""Generation-owned Milvus drivers for the builtin derived vector index."""

from __future__ import annotations

import asyncio
import hashlib
import inspect
import json
import logging
import math
import re
from collections.abc import Mapping

import psycopg
from pymilvus import AsyncMilvusClient, DataType
from pymilvus.exceptions import MilvusException

from forest.errors import ForestError
from forest.packages import digest_json
from forest.schema_bundles import module_schema_namespace
from forest.storage.vector_index_authority import VectorIndexRefRepository

_REF = re.compile(r"[^\x00]{1,256}\Z")
_LOG = logging.getLogger(__name__)
_SERVICES = frozenset(
    {"vector.transport.upsert", "vector.transport.search",
     "vector.transport.delete", "vector.transport.prune"}
)


async def _call(port, *args):
    result = port(*args)
    return await result if inspect.isawaitable(result) else result


def _valid_ref(value: str) -> bool:
    try:
        return _REF.fullmatch(value) is not None and len(value.encode("utf-8")) <= 256
    except UnicodeError:
        return False


def vector_collection_name(bot_id: str, space_id: str) -> str:
    suffix = hashlib.sha256((bot_id + "\0" + space_id).encode()).hexdigest()[:32]
    return "forest_memory_ref2_" + suffix


class MilvusVectorIndexEffectDriver:
    def __init__(
        self,
        configuration,
        configuration_revision,
        identity,
        declaration,
        *,
        bot_id,
        store,
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
        # The package Generation validates configuration before effect binding.
        self.configuration = configuration
        self.configuration_revision = configuration_revision
        self.identity = identity
        self.service_id = declaration["name"]
        self.bot_id = bot_id
        self.refs = VectorIndexRefRepository(
            store, namespace=module_schema_namespace("forest.vector.index")
        )
        self.runtime = runtime
        self.token_resolver = token_resolver
        self.client_factory = client_factory
        self.collection_name = vector_collection_name(bot_id, configuration["space_id"])
        self.client = None
        self.accepting = False
        self.rebuild_required = False

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
        self.rebuild_required = False
        self.client = self.client_factory(
            uri=self.configuration["endpoint"],
            token=token,
            timeout=float(self.configuration["timeout_seconds"]),
        )
        try:
            await self._ensure_collection()
        except BaseException:
            client, self.client = self.client, None
            try:
                await client.close()
            except Exception:
                _LOG.warning("Vector index client close failed after startup error")
            raise
        self.accepting = True

    def _collection_id(self, description) -> int:
        collection_id = description.get("collection_id")
        if type(collection_id) is not int or collection_id <= 0:
            raise ForestError(
                "VECTOR_INDEX_IDENTITY_INVALID",
                "Milvus did not provide a valid collection identity",
                502,
            )
        return collection_id

    def _require_rebuild(self, reason: str) -> None:
        self.rebuild_required = True
        _LOG.error(
            "Vector collection rebuild required: %s (bot_id=%s, space_id=%s)",
            reason, self.bot_id, self.configuration["space_id"],
        )

    async def _ensure_collection(self) -> None:
        try:
            async with asyncio.timeout(float(self.configuration["timeout_seconds"])):
                async with self.refs.store.pool.connection() as conn, conn.transaction():
                    binding, active_refs = await self.refs.lock_collection_state(conn)
                    if binding is not None and (
                        binding["space_id"] != self.configuration["space_id"]
                        or binding["collection_name"] != self.collection_name
                    ):
                        raise ForestError(
                            "VECTOR_INDEX_SPACE_IMMUTABLE",
                            "Collection binding belongs to another vector space",
                            409,
                        )
                    if await self.client.has_collection(
                        collection_name=self.collection_name
                    ):
                        description = await self.client.describe_collection(
                            collection_name=self.collection_name
                        )
                        fields = {
                            item["name"]: item for item in description.get("fields", [])
                        }
                        vector = fields.get("embedding", {})
                        if (
                            set(fields) != {
                                "ref_id", "bot_id", "space_id", "purpose",
                                "input_digest", "embedding"
                            }
                            or int(vector.get("params", {}).get("dim", -1))
                            != self.configuration["dimensions"]
                        ):
                            raise ForestError(
                                "VECTOR_INDEX_SPACE_IMMUTABLE",
                                "Milvus collection layout differs",
                                409,
                            )
                        collection_id = self._collection_id(description)
                        if binding is None:
                            self._require_rebuild("unbound collection")
                        elif binding["collection_id"] != collection_id:
                            self._require_rebuild("collection identity changed")
                        return
                    if active_refs:
                        self._require_rebuild("collection missing with active refs")
                        return
                    schema = self.client.create_schema(
                        auto_id=False, enable_dynamic_field=False
                    )
                    schema.add_field(
                        "ref_id", DataType.VARCHAR, is_primary=True, max_length=256
                    )
                    schema.add_field("bot_id", DataType.VARCHAR, max_length=256)
                    schema.add_field("space_id", DataType.VARCHAR, max_length=256)
                    schema.add_field("purpose", DataType.VARCHAR, max_length=16)
                    schema.add_field("input_digest", DataType.VARCHAR, max_length=71)
                    schema.add_field(
                        "embedding", DataType.FLOAT_VECTOR,
                        dim=self.configuration["dimensions"],
                    )
                    indexes = self.client.prepare_index_params()
                    indexes.add_index(
                        field_name="embedding", index_type="AUTOINDEX",
                        metric_type="COSINE",
                    )
                    await self.client.create_collection(
                        collection_name=self.collection_name,
                        schema=schema,
                        index_params=indexes,
                        consistency_level="Strong",
                    )
                    description = await self.client.describe_collection(
                        collection_name=self.collection_name
                    )
                    await self.refs.bind_collection(
                        conn,
                        revision=1 if binding is None else binding["binding_revision"] + 1,
                        space_id=self.configuration["space_id"],
                        collection_name=self.collection_name,
                        collection_id=self._collection_id(description),
                    )
        except ForestError:
            raise
        except (
            TimeoutError, MilvusException, OSError, KeyError, TypeError,
            ValueError, psycopg.Error,
        ) as exc:
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
        if self.rebuild_required and self.service_id != "vector.transport.delete":
            raise ForestError(
                "VECTOR_INDEX_REBUILD_REQUIRED",
                "Published vector refs require index reconstruction",
                503,
            )
        if self.service_id == "vector.transport.upsert":
            return await self._upsert(payload)
        if self.service_id == "vector.transport.search":
            return await self._search(payload)
        if self.service_id == "vector.transport.prune":
            return await self._prune(payload)
        return await self._delete(payload)

    async def _upsert(self, payload):
        values = tuple(float(item) for item in payload["vector"])
        if (
            not _valid_ref(payload["ref_id"])
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
            "embedding": list(values),
        }
        input_digest = digest_json(row)
        row["input_digest"] = input_digest
        replay = payload["replay"]
        try:
            async with self.refs.store.pool.connection() as conn, conn.transaction():
                await self.refs.lock_collection_mutation(conn)
                existing = await self.refs.lock(conn, payload["ref_id"])
                if existing is not None:
                    if existing["state"] == "TOMBSTONED":
                        raise ForestError(
                            "VECTOR_INDEX_REF_DELETED", "Deleted vector ref cannot be published", 409
                        )
                    if (
                        existing["input_digest"] != input_digest
                        or existing["space_id"] != payload["space_id"]
                        or existing["purpose"] != payload["purpose"]
                        or existing["replay_locator"] != replay
                        or existing["vector_digest"] != replay["vector_digest"]
                    ):
                        raise ForestError(
                            "VECTOR_INDEX_REF_CONFLICT", "Vector ref was bound to other data", 409
                        )
                async with asyncio.timeout(float(self.configuration["timeout_seconds"])):
                    await self.client.upsert(collection_name=self.collection_name, data=[row])
                if existing is None:
                    await self.refs.record_active(
                        conn, ref_id=payload["ref_id"],
                        space_id=payload["space_id"], purpose=payload["purpose"],
                        input_digest=input_digest,
                        replay_locator=replay, vector_digest=replay["vector_digest"],
                    )
        except ForestError:
            raise
        except (TimeoutError, MilvusException, OSError, psycopg.Error) as exc:
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
                    output_fields=[],
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
            ref_id = hit.get("ref_id", hit.get("id")) if isinstance(hit, Mapping) else None
            score = hit.get("distance") if isinstance(hit, Mapping) else None
            if (
                not isinstance(ref_id, str)
                or type(score) not in {int, float}
                or not math.isfinite(float(score))
            ):
                raise ForestError(
                    "VECTOR_INDEX_RESPONSE_INVALID", "Milvus returned an invalid hit", 502
                )
            result.append(
                {
                    "ref_id": ref_id,
                    "score": float(score),
                }
            )
        return {
            "query_ref_id": payload["query_ref_id"],
            "space_id": payload["space_id"],
            "hits": result,
        }

    async def _delete(self, payload):
        refs = tuple(payload["ref_ids"])
        if any(not _valid_ref(item) for item in refs):
            raise ForestError(
                "VECTOR_INDEX_INPUT_INVALID", "Vector deletion refs are invalid", 422
            )
        try:
            async with self.refs.store.pool.connection() as conn, conn.transaction():
                await self.refs.lock_collection_mutation(conn)
                for ref_id in sorted(set(refs)):
                    existing = await self.refs.lock(conn, ref_id)
                    await self.refs.record_tombstone(
                        conn, ref_id=ref_id, space_id=payload["space_id"],
                        existing=existing,
                    )
                # With no collection, the durable tombstone is the deletion result.
                if not self.rebuild_required:
                    await self._prune(payload)
        except ForestError:
            raise
        except (TimeoutError, MilvusException, OSError, psycopg.Error) as exc:
            raise ForestError(
                "VECTOR_INDEX_DELETE_UNAVAILABLE", "Milvus deletion failed", 503
            ) from exc
        return {"ref_ids": list(refs), "space_id": payload["space_id"]}

    async def _prune(self, payload):
        refs = tuple(payload["ref_ids"])
        if any(not _valid_ref(item) for item in refs):
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
    *, bot_id, store, runtime, token_resolver, client_factory=AsyncMilvusClient
):
    def create(configuration, configuration_revision, identity, declaration):
        return MilvusVectorIndexEffectDriver(
            configuration,
            configuration_revision,
            identity,
            declaration,
            bot_id=bot_id,
            store=store,
            runtime=runtime,
            token_resolver=token_resolver,
            client_factory=client_factory,
        )

    return create
