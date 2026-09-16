"""Supervised, independently configured image capture and memory indexing."""

from __future__ import annotations

import asyncio
from uuid import uuid4

import psycopg
from psycopg_pool import PoolTimeout

from forest.embeddings import EmbeddingCancelled, EmbeddingClient, EmbeddingResult
from forest.errors import ForestError
from forest.memory_index import profile
from forest.storage.agent_memory import AgentMemoryStore
from forest.storage.control import BotControl
from forest.storage.memory_administration import MemoryAdministration
from forest.storage.memory_associations import MemoryAssociationAdministration
from forest.storage.memory_config import MemoryConfig
from forest.storage.memory_graph import GraphStore
from forest.storage.memory_index import MemoryIndexStore
from forest.storage.memory_media import MemoryMediaStore
from forest.storage.memory_queries import MemoryQueryStore
from forest.storage.memory_semantic import MemorySemanticAssociationStore
from forest.storage.memory_sources import MemorySourceStore
from forest.storage.payloads import PayloadStore

_LOST_AUTHORITY = {"STALE_HOST", "STORAGE_SEALED", "KERNEL_UNAVAILABLE", "BOT_SCOPE_MISMATCH"}


class MemoryRuntime:
    def __init__(self, platform, *, messages):
        self.platform, self.store = platform, platform.bot.store
        self.config = MemoryConfig(self.store, platform.vault)
        self.media = MemoryMediaStore(messages, config=self.config)
        self.index = MemoryIndexStore(messages, self.media)
        self.queries = MemoryQueryStore(self.index)
        self.graph = GraphStore(messages)
        self.administration = MemoryAdministration(messages)
        self.associations = MemoryAssociationAdministration(messages)
        self.semantic = MemorySemanticAssociationStore(messages)
        self.sources = MemorySourceStore(messages)
        self.payloads = PayloadStore(self.store)
        self.task = None
        self.backfill_task = None
        self.workers, self.errors, self.clients = {}, {}, set()
        self.executions = set()
        self.closed = False
        self._wake = asyncio.Event()
        self._graph_cursor = {}
        self._close_task = None

    def notify(self):
        self._wake.set()

    async def start(self):
        if self.closed:
            raise ForestError("MEMORY_RUNTIME_CLOSED", "Memory runtime is closed", 503)
        if self.task is None:
            self.task = asyncio.create_task(
                self._loop(), name="memory-runtime:" + self.store.bot_id
            )
        if self.backfill_task is None:
            self.backfill_task = asyncio.create_task(
                self._backfill_loop(), name="memory-source-backfill"
            )

    def owned_tasks(self):
        return tuple(task for task in (self.task, self.backfill_task) if task is not None)

    async def _backfill_loop(self):
        """Recover legacy source rows as bounded work owned by this module."""
        while not self.closed:
            delay = 10
            try:
                async with asyncio.timeout(5):
                    result = await self.sources.backfill(
                        self.platform.kernel._authority(), limit=32
                    )
                self.errors.pop("memory_sources", None)
                delay = 0.25 if result["more_possible"] else 5
            except asyncio.CancelledError:
                raise
            except ForestError as exc:
                if exc.code in _LOST_AUTHORITY:
                    raise
                self.errors["memory_sources"] = exc.code[:128]
            except (TimeoutError, psycopg.Error, PoolTimeout):
                self.errors["memory_sources"] = "MEMORY_SOURCE_BACKFILL_UNAVAILABLE"
            except Exception:
                self.errors["memory_sources"] = "MEMORY_SOURCE_BACKFILL_FAILED"
            await asyncio.sleep(delay)

    def _spawn(self, key, factory):
        if self.closed or key in self.workers:
            return
        task = asyncio.create_task(factory(), name=key)
        self.workers[key] = task

        def finished(task):
            self.workers.pop(key, None)
            if not task.cancelled() and task.exception() is not None:
                if len(self.errors) >= 32:
                    self.errors.pop(next(iter(self.errors)))
                self.errors[key.split(":", 1)[0]] = getattr(
                    task.exception(), "code", "MEMORY_WORK_FAILED"
                )

        task.add_done_callback(finished)

    async def _loop(self):
        while not self.closed:
            delay = 2
            self._wake.clear()
            try:
                lease = self.platform.kernel._authority()
                if "scrub:payloads" not in self.workers:
                    self._spawn("scrub:payloads", lambda: self._scrub(lease))
                # Attempt identities carry enough accounting facts to recover
                # without reopening a deleted/disabled configuration body.
                await self.index.recover(lease)
                await self.media.recover(lease)
                # Manual association publication is independent of embedding
                # configuration, including a Bot that has never enabled memory
                # indexing. This pending-only worker runs before config lookup.
                if "graph:refresh" not in self.workers:
                    pending_graph = await self.graph.pending(lease, limit=1)
                    if pending_graph["scopes"]:
                        self._spawn(
                            "graph:refresh",
                            lambda: self._graph(lease, discover=False),
                        )
                cfg = await self.config.runtime(lease)
                if self.closed:
                    break
                if cfg["values"]["enabled"]:
                    await self.index.enroll(lease, cfg["revision"], limit=32)
                    captures = await self.media.scan(lease, limit=32)
                    for capture in captures:
                        if (
                            sum(key.startswith("capture:") for key in self.workers)
                            >= cfg["values"]["capture_concurrency"]
                        ):
                            break
                        key = "capture:" + capture["capture_id"]
                        self._spawn(
                            key,
                            lambda capture=capture: self.media.capture_once(
                                lease, capture["capture_id"]
                            ),
                        )
                    if (
                        sum(key.startswith("index:") for key in self.workers)
                        < cfg["values"]["index_concurrency"]
                    ):
                        retry = await self.index.retryable(lease, cfg["revision"])
                        if retry:
                            self._spawn(
                                "index:" + retry["request_id"],
                                lambda retry=retry: self.embed(
                                    lease, cfg, retry["facet_ids"], request_id=retry["request_id"]
                                ),
                            )
                        pending = await self.index.pending(lease, cfg["revision"], limit=32)
                        selected, images = [], 0
                        for facet in pending:
                            if len(selected) >= cfg["values"]["batch_size"]:
                                break
                            if facet["modality"] == "image":
                                if images >= 2:  # Driver's 8 MiB total image bound.
                                    continue
                                images += 1
                            selected.append(facet["facet_id"])
                        if (
                            selected
                            and sum(key.startswith("index:") for key in self.workers)
                            < cfg["values"]["index_concurrency"]
                        ):
                            self._spawn(
                                "index:" + uuid4().hex,
                                lambda selected=selected: self.embed(lease, cfg, selected),
                            )
                    if (
                        cfg["values"]["semantic_associations_enabled"]
                        and not self.platform.module(
                            "conversation.scheduler"
                        ).active_conversations
                        and "semantic:reconcile" not in self.workers
                    ):
                        self._spawn(
                            "semantic:reconcile",
                            lambda: self._semantic(lease),
                        )
                    self.errors.pop("scan", None)
                # Ordinary trunk discovery remains tied to enabled indexing.
                if cfg["values"]["enabled"] and "graph:refresh" not in self.workers:
                    self._spawn(
                        "graph:refresh",
                        lambda: self._graph(lease),
                    )
            except asyncio.CancelledError:
                raise
            except ForestError as exc:
                if exc.code in {"STALE_HOST", "STORAGE_SEALED", "BOT_SCOPE_MISMATCH"}:
                    raise
                if exc.code not in {"MEMORY_NOT_CONFIGURED", "MEMORY_CONFIGURATION_REVOKED", "CONTROL_CONTENT_REVOKED"}:
                    self.errors["scan"] = exc.code
                delay = 5
            except Exception:
                self.errors["scan"] = "MEMORY_SCAN_FAILED"
                delay = 5
            if self.closed:
                break
            try:
                await asyncio.wait_for(self._wake.wait(), delay)
            except TimeoutError:
                pass

    async def _scrub(self, lease):
        result = await self.payloads.scrub(lease, limit=64)
        if result["remaining"]:
            self.notify()
        return result

    async def _semantic(self, lease):
        await self.semantic.repair_stale(lease)
        result = await self.semantic.process_one(lease)
        if result is None:
            self.errors.pop("semantic", None)
            return
        if result["affected"]:
            await self.semantic.enqueue_sources(
                lease, result["space_id"], result["affected"]
            )
        self.errors.pop("semantic", None)
        self.notify()

    async def _graph(self, lease, *, discover=True):
        pending = await self.graph.pending(lease, limit=1)
        if pending["scopes"]:
            await self.graph.refresh(lease, pending["scopes"][0])
            self.errors.pop("graph", None)
            return
        if not discover:
            self.errors.pop("graph", None)
            return
        page = await self.graph.discover(lease, **self._graph_cursor, limit=1)
        for scope in page["scopes"]:
            if self.closed:
                return
            await self.graph.refresh(lease, scope)
        self._graph_cursor = page["next_after"] or {}
        self.errors.pop("graph", None)

    async def embed(
        self, lease, cfg, facet_ids, *, request_id=None, conversation_id=None, input_cut=None
    ):
        if self.closed:
            raise ForestError("MEMORY_RUNTIME_CLOSED", "Memory runtime is closed", 503)
        task = asyncio.create_task(
            self._embed(
                lease,
                cfg,
                facet_ids,
                request_id=request_id,
                conversation_id=conversation_id,
                input_cut=input_cut,
            ),
            name="memory-embedding",
        )
        self.executions.add(task)
        task.add_done_callback(self.executions.discard)
        return await task

    async def _embed(
        self, lease, cfg, facet_ids, *, request_id=None, conversation_id=None, input_cut=None
    ):
        admission = await self.index.prepare(
            lease,
            cfg["revision"],
            facet_ids,
            request_id=request_id,
            purpose="query" if conversation_id else "document",
            conversation_id=conversation_id,
            input_cut=input_cut,
        )
        if "prepared" not in admission:
            return admission
        prepared = admission["prepared"]
        if self.closed or asyncio.current_task().cancelling():
            result = EmbeddingResult(
                "NOT_SENT",
                prepared.request_id,
                prepared.transport_attempt_id,
                prepared.payload_digest,
                prepared.space_id,
                error_code="MEMORY_RUNTIME_CLOSED",
            )
            await self.index.complete(lease, prepared, result)
            raise ForestError("MEMORY_RUNTIME_CLOSED", "Memory runtime closed before dispatch", 503)
        client = EmbeddingClient(
            cfg["values"]["endpoint"],
            self.platform.vault.decrypt(cfg["secret_ciphertext"]),
            profile(cfg["values"]),
            timeout_seconds=min(cfg["values"]["request_timeout"], cfg["values"]["query_timeout"])
            if conversation_id
            else cfg["values"]["request_timeout"],
            max_connections=1,
        )
        self.clients.add(client)
        token, permitted = uuid4().hex, False

        async def check(request):
            nonlocal permitted
            if request != prepared:
                raise ForestError(
                    "MEMORY_REQUEST_CHANGED", "Embedding transport changed its input", 409
                )
            await self.index.check_permit(lease, request, dispatch_token=token)
            permitted = True

        try:
            try:
                result = await client.dispatch(prepared, permitcheck=check)
            except EmbeddingCancelled as exc:
                try:
                    await self.index.complete(
                        lease, prepared, exc.result, dispatch_token=token if permitted else None
                    )
                except Exception:
                    pass  # Durable recovery keeps uncertain cost reserved.
                raise
            except Exception:
                result = EmbeddingResult(
                    "UNKNOWN" if permitted else "NOT_SENT",
                    prepared.request_id,
                    prepared.transport_attempt_id,
                    prepared.payload_digest,
                    prepared.space_id,
                    error_code="MEMORY_DISPATCH_INTERRUPTED",
                )
            return await self.index.complete(
                lease, prepared, result, dispatch_token=token if permitted else None
            )
        finally:
            await client.aclose()
            self.clients.discard(client)

    async def retrieve_for_step(self, actor, snapshot, state):
        if self.closed:
            raise ForestError("MEMORY_RUNTIME_CLOSED", "Memory runtime is closed", 503)
        task = asyncio.create_task(
            self._retrieve_for_step(actor, snapshot, state), name="memory-agent-query"
        )
        self.executions.add(task)
        task.add_done_callback(self.executions.discard)
        return await task

    async def _retrieve_for_step(self, actor, snapshot, state):
        memory = AgentMemoryStore(state, self.queries)
        decision = await memory.begin(actor, snapshot)
        if decision["status"] in {"DISABLED", "BYPASSED"}:
            return await memory.finish(actor, snapshot)
        try:
            cfg = await self.config.runtime(actor.host, revision=decision["config_revision"])
        except ForestError as exc:
            if exc.code not in {"CONTROL_CONTENT_REVOKED", "MEMORY_CONFIGURATION_REVOKED"}:
                raise
            # finish refuses to replace an already BOUND bundle. Pending query
            # decisions may commit a metadata-only failure without config text.
            try:
                async with asyncio.timeout(1):
                    return await memory.finish(actor, snapshot, error_code="MEMORY_CONFIGURATION_REVOKED")
            except TimeoutError:
                raise ForestError(
                    "MEMORY_QUERY_RECOVERY_REQUIRED",
                    "Configuration was revoked before the retrieval decision could commit",
                    503,
                ) from None
        deadline = asyncio.timeout(cfg["values"]["query_timeout"])
        try:
            async with deadline:
                return await self._retrieve_pending(actor, snapshot, memory, decision, cfg)
        except (TimeoutError, asyncio.CancelledError) as exc:
            # Python 3.11 timeout does not translate a CancelledError subclass
            # (the embedding driver's cancellation carries a cost receipt).
            # A separate external cancellation still takes precedence.
            if isinstance(exc, asyncio.CancelledError) and (
                not deadline.expired() or asyncio.current_task().cancelling()
            ):
                raise
            # One short metadata-only cleanup window. A failed commit remains
            # a recovery case; it must not masquerade as a frozen fallback.
            try:
                async with asyncio.timeout(1):
                    return await memory.finish(actor, snapshot, error_code="MEMORY_QUERY_TIMEOUT")
            except TimeoutError:
                raise ForestError(
                    "MEMORY_QUERY_RECOVERY_REQUIRED",
                    "Timed out before the retrieval decision could commit",
                    503,
                ) from None

    async def _retrieve_pending(self, actor, snapshot, memory, decision, cfg):
        # Reuse the same bundle at an unchanged input cut, including recovery.
        try:
            return await memory.finish(actor, snapshot)
        except ForestError as exc:
            if exc.code not in {"MEMORY_QUERY_NOT_FOUND", "MEMORY_EVIDENCE_NOT_READY"}:
                raise
        query, failure = None, None
        try:
            query = await self.queries.prepare(
                actor.host,
                decision["config_revision"],
                query_id=decision["query_id"],
                conversation_id=actor.conversation_id,
                input_cut=decision["input_cut"],
                source_unit_ids=decision["source_unit_ids"],
            )
            captures = await self.media.ensure(actor.host, query["source_unit_ids"])
            for capture in captures:
                if self.closed:
                    raise ForestError("MEMORY_RUNTIME_CLOSED", "Memory runtime is closing", 503)
                await self.media.capture_once(
                    actor.host, capture["capture_id"], owner_id=query["query_id"]
                )
            await self.embed(
                actor.host,
                cfg,
                query["facet_ids"],
                request_id=query["query_id"],
                conversation_id=actor.conversation_id,
                input_cut=decision["input_cut"],
            )
        except ForestError as exc:
            if exc.code in {
                "STALE_HOST",
                "STALE_BINDING",
                "STORAGE_SEALED",
                "MEMORY_RUNTIME_CLOSED",
            }:
                raise
            failure = exc.code
        if query is None:
            return await memory.finish(
                actor, snapshot, error_code=failure or "MEMORY_QUERY_UNAVAILABLE"
            )
        bundle = await self.queries.retrieve(actor.host, query["query_id"], error_code=failure)
        if bundle["payload_id"] is None:
            return await memory.finish(
                actor, snapshot, error_code=bundle["reason_code"] or "MEMORY_QUERY_UNAVAILABLE"
            )
        return await memory.finish(actor, snapshot)

    async def inspect(self, ticket, operation_id, payload):
        if payload != {}:
            raise ForestError("MEMORY_INSPECT_INVALID", "Memory status takes no selectors", 422)
        async with self.store.pool.connection() as conn, conn.transaction():
            control = BotControl(self.store)
            claims = await control._guard(conn, ticket, "memory.read", operation_id, payload)
            cfg = await (
                await conn.execute(
                    "SELECT r.* FROM public.bot_memory_config_heads h JOIN public.bot_memory_config_revisions r ON (r.bot_id,r.revision)=(h.bot_id,h.revision) WHERE h.bot_id=%s FOR SHARE OF h",
                    (self.store.bot_id,),
                )
            ).fetchone()
            cfg = await MemoryConfig.hydrate_in(conn, self.store, cfg, required=False)
            counts = {}
            for name, table, field in (
                ("facets", "bot_memory_index_facets", "state"),
                ("captures", "bot_memory_media_heads", "status"),
                ("attempts", "bot_memory_embedding_attempts", "state"),
            ):
                rows = await (
                    await conn.execute(
                        f"SELECT {field} AS status,count(*) AS count FROM public.{table} WHERE bot_id=%s GROUP BY {field}",
                        (self.store.bot_id,),
                    )
                ).fetchall()
                counts[name] = {row["status"]: row["count"] for row in rows}
            budget = await (
                await conn.execute(
                    "SELECT budget_day,reserved_tokens,charged_tokens,requests FROM public.bot_memory_embedding_budget_days WHERE bot_id=%s ORDER BY budget_day DESC LIMIT 1",
                    (self.store.bot_id,),
                )
            ).fetchone()
            await control._time(conn, claims)
            return {
                "configured": cfg is not None,
                "enabled": bool(cfg and cfg["values_payload_state"] == "ACTIVE" and cfg["values"]["enabled"]),
                "values_payload_state": cfg["values_payload_state"] if cfg else "UNCONFIGURED",
                "revision": cfg["revision"] if cfg else 0,
                "index_consistency": "eventual",
                "counts": counts,
                "budget": budget,
                "errors": dict(self.errors),
                "active_workers": len(self.workers),
                "running": bool(self.task and not self.task.done()),
                "backfill_running": bool(
                    self.backfill_task and not self.backfill_task.done()
                ),
            }

    async def close(self):
        self.closed = True
        self.notify()
        if self._close_task is None or (
            self._close_task.done()
            and (self._close_task.cancelled() or self._close_task.exception())
        ):
            self._close_task = asyncio.create_task(self._close(), name="close:memory")
        await asyncio.shield(self._close_task)

    async def _close(self):
        # Fence scan results before snapshotting owned workers.
        owned = tuple(task for task in (self.task, self.backfill_task) if task is not None)
        for task in owned:
            task.cancel()
        await asyncio.gather(*owned, return_exceptions=True)
        tasks = (*self.workers.values(), *self.executions)
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        for client in tuple(self.clients):
            await client.aclose()
            self.clients.discard(client)
        self.workers.clear()
