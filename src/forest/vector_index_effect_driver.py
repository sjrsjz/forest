"""Generation-owned Milvus drivers for the builtin derived vector index."""

from __future__ import annotations

import asyncio
import hashlib
import inspect
import json
import logging
import math
import re
import struct
from collections.abc import Mapping

from pymilvus import AsyncMilvusClient, DataType, MilvusClient
from pymilvus.exceptions import MilvusException

from forest.errors import ForestError
from forest.packages import digest_json

_REF = re.compile(r"[^\x00]{1,256}\Z")
_LOG = logging.getLogger(__name__)
_SERVICES = frozenset(
    {"vector.transport.upsert", "vector.transport.search",
     "vector.transport.prune", "vector.transport.audit"}
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
        runtime,
        token_resolver,
        client_factory=AsyncMilvusClient,
        audit_client_factory=MilvusClient,
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
        self._startup_command = None
        self._runtime_command = None
        self._startup_operation_id = "vector-collection-start:" + hashlib.sha256(
            (identity.generation_id + "\0" + self.service_id).encode("utf-8")
        ).hexdigest()
        self.runtime = runtime
        self.token_resolver = token_resolver
        self.client_factory = client_factory
        self.audit_client_factory = audit_client_factory
        self._token = None
        self.collection_name = vector_collection_name(bot_id, configuration["space_id"])
        self.client = None
        self.accepting = False
        self.rebuild_required = False
        self.binding_revision = None
        self.collection_id = None

    def bind_startup_commands(self, command) -> None:
        if self._startup_command is not None or not callable(command):
            raise ForestError(
                "PACKAGE_EFFECT_STARTUP_BINDING_INVALID",
                "Vector startup command port was bound more than once", 500,
            )
        self._startup_command = command

    def bind_runtime_commands(self, command) -> None:
        if self._runtime_command is not None or not callable(command):
            raise ForestError(
                "PACKAGE_EFFECT_RUNTIME_BINDING_INVALID",
                "Vector runtime command port was bound more than once", 500,
            )
        self._runtime_command = command

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
        self.binding_revision = None
        self.collection_id = None
        self._token = token
        self.client = self.client_factory(
            uri=self.configuration["endpoint"],
            token=token,
            timeout=float(self.configuration["timeout_seconds"]),
        )
        try:
            await self._ensure_collection()
            if not self.rebuild_required:
                await self._verify_physical_collection()
        except BaseException:
            client, self.client = self.client, None
            self._token = None
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

    def _remember_binding(self, decision) -> None:
        revision = decision.get("binding_revision")
        collection_id = decision.get("collection_id")
        if type(revision) is not int or revision <= 0 or type(collection_id) is not int or collection_id <= 0:
            raise ForestError(
                "VECTOR_INDEX_BINDING_INVALID",
                "Package returned an invalid collection binding", 502,
            )
        self.binding_revision = revision
        self.collection_id = collection_id

    async def _quarantine(self, reason: str) -> None:
        self._require_rebuild(reason)
        if self.binding_revision is not None:
            command = self._runtime_command if self.accepting else self._startup_command
            if command is None:
                raise ForestError(
                    "PACKAGE_EFFECT_RUNTIME_BINDING_INVALID",
                    "Vector quarantine has no package command port", 500,
                )
            await command(
                "vector.collection.quarantine",
                {"binding_revision": self.binding_revision,
                 "collection_id": self.collection_id, "reason": reason},
                operation_id=self._startup_operation_id + ":quarantine:" + reason,
            )

    async def _verify_physical_collection(self) -> None:
        try:
            async with asyncio.timeout(float(self.configuration["timeout_seconds"])):
                exists = await self.client.has_collection(
                    collection_name=self.collection_name
                )
                description = await self.client.describe_collection(
                    collection_name=self.collection_name
                ) if exists else None
        except (TimeoutError, MilvusException, OSError) as exc:
            raise ForestError(
                "VECTOR_INDEX_UNAVAILABLE", "Milvus collection is unavailable", 503
            ) from exc
        if description is None:
            reason = "collection missing"
        elif self._collection_id(description) != self.collection_id:
            reason = "collection identity changed"
        else:
            try:
                self._validate_collection_description(description)
            except ForestError as exc:
                if exc.code != "VECTOR_INDEX_SPACE_IMMUTABLE":
                    raise
                reason = "collection layout changed"
            else:
                return
        await self._quarantine(reason)
        raise ForestError(
            "VECTOR_INDEX_REBUILD_REQUIRED",
            "Published vector refs require index reconstruction", 503,
        )

    def _validate_collection_description(self, description) -> int:
        fields = {item["name"]: item for item in description.get("fields", [])}
        vector = fields.get("embedding", {})
        if (
            set(fields) != {
                "ref_id", "bot_id", "space_id", "purpose",
                "input_digest", "embedding",
            }
            or int(vector.get("params", {}).get("dim", -1))
            != self.configuration["dimensions"]
        ):
            raise ForestError(
                "VECTOR_INDEX_SPACE_IMMUTABLE",
                "Milvus collection layout differs", 409,
            )
        return self._collection_id(description)

    async def _create_collection(self) -> int:
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
            field_name="embedding", index_type="AUTOINDEX", metric_type="COSINE"
        )
        await self.client.create_collection(
            collection_name=self.collection_name, schema=schema,
            index_params=indexes, consistency_level="Strong",
        )
        description = await self.client.describe_collection(
            collection_name=self.collection_name
        )
        return self._validate_collection_description(description)

    async def _confirm_collection(self, token: str, collection_id: int) -> None:
        shape = {
            "space_id": self.configuration["space_id"],
            "collection_name": self.collection_name,
            "dimensions": self.configuration["dimensions"],
            "metric": self.configuration["metric"],
        }
        confirmed = await self._startup_command(
            "vector.collection.confirm",
            {**shape, "collection_id": collection_id, "claim_token": token},
            operation_id=self._startup_operation_id + ":confirm",
        )
        if confirmed != {
            "state": "READY", "reason": None, "claim_token": None,
            "binding_revision": 1, "collection_id": collection_id,
        }:
            raise ForestError(
                "VECTOR_INDEX_BINDING_INVALID",
                "Package did not confirm the physical collection identity", 502,
            )
        self._remember_binding(confirmed)

    async def _ensure_collection(self) -> None:
        if self._startup_command is None:
            raise ForestError(
                "PACKAGE_EFFECT_STARTUP_BINDING_INVALID",
                "Vector collection startup has no package command port", 500,
            )
        shape = {
            "space_id": self.configuration["space_id"],
            "collection_name": self.collection_name,
            "dimensions": self.configuration["dimensions"],
            "metric": self.configuration["metric"],
        }
        try:
            async with asyncio.timeout(float(self.configuration["timeout_seconds"])):
                while True:
                    exists = await self.client.has_collection(
                        collection_name=self.collection_name
                    )
                    collection_id = None
                    if exists:
                        description = await self.client.describe_collection(
                            collection_name=self.collection_name
                        )
                        collection_id = self._collection_id(description)
                    decision = await self._startup_command(
                        "vector.collection.plan",
                        {**shape, "exists": exists, "collection_id": collection_id},
                        operation_id=self._startup_operation_id,
                    )
                    state = decision["state"]
                    if state == "READY":
                        if not exists or collection_id != decision["collection_id"]:
                            continue
                        self._remember_binding(decision)
                        try:
                            self._validate_collection_description(description)
                        except ForestError as exc:
                            if exc.code != "VECTOR_INDEX_SPACE_IMMUTABLE":
                                raise
                            await self._quarantine("collection layout changed")
                        return
                    if state == "WAIT":
                        await asyncio.sleep(0.2)
                        continue
                    if state == "REBUILD_REQUIRED":
                        # Another startup may have published a binding between
                        # the physical observation and the package decision.
                        if not exists and await self.client.has_collection(
                            collection_name=self.collection_name
                        ):
                            continue
                        if decision["binding_revision"] is not None:
                            self._remember_binding(decision)
                            if decision["reason"] in (
                                "collection identity changed",
                                "collection missing with binding",
                            ):
                                await self._quarantine(
                                    "collection missing" if not exists
                                    else "collection identity changed"
                                )
                            else:
                                self._require_rebuild(decision["reason"])
                        else:
                            self._require_rebuild(decision["reason"])
                        return
                    token = decision["claim_token"]
                    if not isinstance(token, str) or not token:
                        raise ForestError(
                            "VECTOR_INDEX_BINDING_INVALID",
                            "Package returned a creation action without a claim", 502,
                        )
                    if state == "CREATE":
                        collection_id = await self._create_collection()
                    elif state != "CONFIRM" or collection_id is None:
                        raise ForestError(
                            "VECTOR_INDEX_BINDING_INVALID",
                            "Package returned an invalid collection action", 502,
                        )
                    await self._confirm_collection(token, collection_id)
                    return
        except ForestError:
            raise
        except (TimeoutError, MilvusException, OSError, KeyError, TypeError,
                ValueError) as exc:
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
        if self.rebuild_required:
            raise ForestError(
                "VECTOR_INDEX_REBUILD_REQUIRED",
                "Published vector refs require index reconstruction",
                503,
            )
        await self._verify_physical_collection()
        if self.service_id == "vector.transport.upsert":
            return await self._upsert(payload)
        if self.service_id == "vector.transport.search":
            return await self._search(payload)
        if self.service_id == "vector.transport.audit":
            return await self._audit(payload)
        return await self._prune(payload)

    async def _upsert(self, payload):
        try:
            values = tuple(float(item) for item in payload["vector"])
        except (TypeError, ValueError) as exc:
            raise ForestError(
                "VECTOR_INDEX_INPUT_INVALID", "Vector index row is invalid", 422
            ) from exc
        if (
            not _valid_ref(payload["ref_id"])
            or len(values) != self.configuration["dimensions"]
            or any(not math.isfinite(item) for item in values)
            or payload["purpose"] not in ("DOCUMENT", "QUERY")
        ):
            raise ForestError(
                "VECTOR_INDEX_INPUT_INVALID", "Vector index row is invalid", 422
            )
        request = {
            key: payload[key]
            for key in ("ref_id", "space_id", "purpose", "vector", "replay")
        }
        input_digest = payload["input_digest"]
        if input_digest != digest_json(request):
            raise ForestError(
                "VECTOR_INDEX_INPUT_INVALID", "Vector input digest differs", 422
            )
        row = {
            "ref_id": payload["ref_id"],
            "bot_id": self.bot_id,
            "space_id": payload["space_id"],
            "purpose": payload["purpose"],
            "embedding": list(values),
            "input_digest": input_digest,
        }
        try:
            async with asyncio.timeout(float(self.configuration["timeout_seconds"])):
                await self.client.upsert(collection_name=self.collection_name, data=[row])
        except (TimeoutError, MilvusException, OSError) as exc:
            raise ForestError(
                "VECTOR_INDEX_UPSERT_UNAVAILABLE", "Milvus upsert failed", 503
            ) from exc
        await self._verify_physical_collection()
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
        await self._verify_physical_collection()
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

    def _audit_row(self, raw):
        ref_id = raw.get("ref_id")
        if not isinstance(ref_id, str) or not _valid_ref(ref_id):
            raise ForestError(
                "VECTOR_INDEX_RESPONSE_INVALID",
                "Milvus returned a row without a valid primary key", 502,
            )
        vector = raw.get("embedding")
        physical_digest = None
        if (
            isinstance(vector, list)
            and len(vector) == self.configuration["dimensions"]
            and all(type(value) in (int, float) and math.isfinite(value)
                    for value in vector)
        ):
            try:
                packed = struct.pack("!" + "f" * len(vector), *vector)
            except (OverflowError, struct.error):
                pass
            else:
                physical_digest = "f32-sha256:" + hashlib.sha256(packed).hexdigest()
        return {
            "ref_id": ref_id,
            **{
                key: raw.get(key) if isinstance(raw.get(key), str) else None
                for key in ("bot_id", "space_id", "purpose", "input_digest")
            },
            "physical_digest": physical_digest,
        }

    def _scan_page(self, after_ref, limit):
        client = self.audit_client_factory(
            uri=self.configuration["endpoint"], token=self._token,
            timeout=float(self.configuration["timeout_seconds"]),
        )
        iterator = None
        try:
            filter_expr = (
                "ref_id > " + json.dumps(after_ref, ensure_ascii=False)
                if after_ref is not None else ""
            )
            iterator = client.query_iterator(
                collection_name=self.collection_name,
                batch_size=limit, limit=limit, filter=filter_expr,
                output_fields=[
                    "ref_id", "bot_id", "space_id", "purpose",
                    "input_digest", "embedding",
                ],
                consistency_level="Strong",
            )
            return iterator.next() or []
        finally:
            try:
                if iterator is not None:
                    iterator.close()
            finally:
                client.close()

    async def _audit(self, payload):
        mode = payload["mode"]
        try:
            async with asyncio.timeout(float(self.configuration["timeout_seconds"])):
                if mode == "GET":
                    raw = await self.client.get(
                        collection_name=self.collection_name,
                        ids=payload["ref_ids"],
                        output_fields=[
                            "ref_id", "bot_id", "space_id", "purpose",
                            "input_digest", "embedding",
                        ],
                        consistency_level="Strong",
                    )
                elif mode == "SCAN":
                    raw = await asyncio.to_thread(
                        self._scan_page, payload["after_ref"], payload["limit"]
                    )
                else:
                    raise ForestError(
                        "VECTOR_INDEX_INPUT_INVALID", "Unknown audit mode", 422,
                    )
        except ForestError:
            raise
        except (TimeoutError, MilvusException, OSError, ValueError, TypeError) as exc:
            raise ForestError(
                "VECTOR_INDEX_AUDIT_UNAVAILABLE", "Milvus audit read failed", 503,
            ) from exc
        await self._verify_physical_collection()
        rows = [self._audit_row(item) for item in raw]
        if mode == "GET":
            allowed = set(payload["ref_ids"])
            if len(rows) > len(allowed) or any(item["ref_id"] not in allowed for item in rows):
                raise ForestError(
                    "VECTOR_INDEX_RESPONSE_INVALID", "Milvus returned unrelated refs", 502,
                )
            return {
                "space_id": payload["space_id"], "mode": mode, "rows": rows,
                "next_ref": None, "done": True,
            }
        refs = [item["ref_id"] for item in rows]
        if (
            len(refs) > payload["limit"]
            or refs != sorted(set(refs))
            or (payload["after_ref"] is not None
                and any(ref <= payload["after_ref"] for ref in refs))
        ):
            raise ForestError(
                "VECTOR_INDEX_RESPONSE_INVALID", "Milvus audit cursor is invalid", 502,
            )
        return {
            "space_id": payload["space_id"], "mode": mode, "rows": rows,
            "next_ref": refs[-1] if refs else payload["after_ref"],
            "done": len(refs) < payload["limit"],
        }

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
        await self._verify_physical_collection()
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
        self._token = None
        if client is not None:
            await client.close()


def milvus_vector_index_effect_driver_factory(
    *, bot_id, runtime, token_resolver, client_factory=AsyncMilvusClient,
    audit_client_factory=MilvusClient,
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
            audit_client_factory=audit_client_factory,
        )

    return create
