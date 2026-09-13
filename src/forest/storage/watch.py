"""Persistent development intent, authorized by the current local Bot host.

No method publishes a route. The SQL publication trigger joins each immutable
activation to the latest accepted edit inside Kernel.publish's transaction.
Lock order: runtime -> host -> watch business head; no filesystem/network I/O.
"""

from __future__ import annotations

import json

from psycopg.errors import UniqueViolation

from forest.drivers.protocol import validate_json
from forest.errors import ForestError
from forest.packages import digest_json
from forest.storage.bot import BotStore, HostLease
from forest.storage.config_payloads import ConfigPayloadStore
from forest.storage.control_payloads import ControlPayloadStore


def _freeze(value: dict) -> dict:
    try:
        validate_json(value)
        raw = json.dumps(value, allow_nan=False, ensure_ascii=False)
        if len(raw.encode("utf-8")) > 262144:
            raise ValueError("Too large")
        return json.loads(raw)
    except (ForestError, ValueError, TypeError, UnicodeError, RecursionError):
        raise ForestError(
            "WATCH_INVALID", "Invalid bounded development configuration", 422
        ) from None


def _text(value: str, maximum: int = 256) -> str:
    if not isinstance(value, str) or not 0 < len(value) <= maximum:
        raise ForestError("WATCH_INVALID", "Invalid development identity", 422)
    try:
        value.encode("utf-8")
    except UnicodeError:
        raise ForestError("WATCH_INVALID", "Invalid development identity", 422) from None
    return value


class WatchStore:
    def __init__(self, store: BotStore):
        self.store = store

    async def hydrate_in(self, conn, head, *, required=True):
        content = ConfigPayloadStore(self.store)
        hydrated = await content.hydrate_values_in(
            conn, {"values_payload_id": head["spec_payload_id"], "values_payload_digest": head["spec_payload_digest"]},
            family="watch", kind="INITIAL", resource=head["watch_id"], identity=head["watch_id"],
            epoch=(await content.payloads.lock_in(conn))["deletion_epoch"], required=required,
        )
        result = {**head, "spec_payload_state": hydrated["values_payload_state"]}
        if hydrated["values_payload_state"] == "ACTIVE":
            spec = hydrated["values"]
            if (not isinstance(spec, dict) or set(spec) != {"source_identity", "config", "grants"}
                    or digest_json({"source": spec["source_identity"]}) != head["watch_id"]):
                raise ForestError("WATCH_AUTHORIZATION_CONFLICT", "Watch source identity differs", 409)
            result.update(spec)
        return result

    async def _guard(self, conn, lease: HostLease, watch_id: str) -> tuple[dict, dict]:
        runtime = await self.store._runtime(conn)
        await self.store._host(conn, lease, runtime)
        head = await (
            await conn.execute(
                "SELECT * FROM public.bot_dev_watch_heads WHERE bot_id=%s AND watch_id=%s FOR UPDATE",
                (self.store.bot_id, watch_id),
            )
        ).fetchone()
        if not head:
            raise ForestError("WATCH_NOT_FOUND", "Development watch is not registered", 404)
        if (
            head["policy_revision"] != runtime["policy_revision"]
            or not runtime["control_admission"]
        ):
            raise ForestError(
                "WATCH_AUTHORITY_REVOKED", "Development authorization is no longer current", 403
            )
        return head, runtime

    async def initialize(
        self, lease: HostLease, source_identity: str, config: dict, grants: frozenset[str]
    ) -> dict:
        source_identity = _text(source_identity, 4096)
        frozen = _freeze({"config": config, "grants": sorted(grants)})
        if not isinstance(config, dict) or any(
            not isinstance(g, str) or not g or len(g) > 128 for g in frozen["grants"]
        ):
            raise ForestError("WATCH_INVALID", "Configuration and grants are invalid", 422)
        watch_id = digest_json({"source": source_identity})
        async with self.store.pool.connection() as conn, conn.transaction():
            runtime = await self.store._runtime(conn)
            await self.store._host(conn, lease, runtime)
            head = await (await conn.execute(
                "SELECT * FROM public.bot_dev_watch_heads WHERE bot_id=%s AND watch_id=%s FOR UPDATE",
                (self.store.bot_id, watch_id),
            )).fetchone()
            if head is None:
                # The Host runtime lock serializes first registration; an old
                # erased identity is never treated as a new initial input.
                content = ConfigPayloadStore(self.store)
                epoch = (await content.payloads.lock_in(conn))["deletion_epoch"]
                spec = {"source_identity": source_identity, **frozen}
                original = await ControlPayloadStore(self.store).put_input_in(
                    conn, "watch-initialize:" + watch_id, spec, action="watch.initialize", epoch=epoch,
                )
                ref = await content.put_values_in(
                    conn, family="watch", kind="INITIAL", resource=watch_id, identity=watch_id,
                    values=spec, parents=(original.payload_id,), epoch=epoch,
                )
                await conn.execute(
                    "INSERT INTO public.bot_dev_watch_heads(bot_id,watch_id,source_identity,config,grants,policy_revision,spec_payload_id,spec_payload_digest) "
                    "VALUES(%s,%s,NULL,NULL,NULL,%s,%s,%s)",
                    (self.store.bot_id, watch_id, runtime["policy_revision"], ref.payload_id, ref.digest),
                )
            head, runtime = await self._guard(conn, lease, watch_id)
            head = await self.hydrate_in(conn, head)
            if (
                head["source_identity"] != source_identity
                or head["config"] != frozen["config"]
                or head["grants"] != frozen["grants"]
            ):
                raise ForestError(
                    "WATCH_AUTHORIZATION_CONFLICT",
                    "The first development configuration and grants are frozen",
                    409,
                )
            await self.store._host(conn, lease, runtime)
            return head

    async def current(self, lease: HostLease, watch_id: str) -> dict:
        async with self.store.pool.connection() as conn, conn.transaction():
            head, runtime = await self._guard(conn, lease, watch_id)
            if head["good_request_id"]:
                head["last_good"] = await (
                    await conn.execute(
                        "SELECT b.*,r.source_digest FROM public.bot_dev_watch_builds b "
                        "JOIN public.bot_dev_watch_requests r USING(bot_id,request_id) "
                        "WHERE b.bot_id=%s AND b.request_id=%s",
                        (self.store.bot_id, head["good_request_id"]),
                    )
                ).fetchone()
            else:
                head["last_good"] = None
            head = await self.hydrate_in(conn, head)
            await self.store._host(conn, lease, runtime)
            return head

    async def accept(
        self, lease: HostLease, watch_id: str, request_id: str, source_digest: str
    ) -> dict:
        _text(request_id)
        _text(source_digest)
        async with self.store.pool.connection() as conn, conn.transaction():
            head, runtime = await self._guard(conn, lease, watch_id)
            await self.hydrate_in(conn, head)
            previous = await (
                await conn.execute(
                    "SELECT * FROM public.bot_dev_watch_requests WHERE bot_id=%s AND request_id=%s",
                    (self.store.bot_id, request_id),
                )
            ).fetchone()
            if previous:
                if previous["watch_id"] != watch_id or previous["source_digest"] != source_digest:
                    raise ForestError(
                        "IDEMPOTENCY_CONFLICT", "Development request has different input"
                    )
                await self.store._host(conn, lease, runtime)
                return previous
            request = await (
                await conn.execute(
                    "INSERT INTO public.bot_dev_watch_requests(bot_id,watch_id,request_id,sequence,source_digest,host_incarnation) "
                    "VALUES(%s,%s,%s,%s,%s,%s) RETURNING *",
                    (
                        self.store.bot_id,
                        watch_id,
                        request_id,
                        head["desired_sequence"] + 1,
                        source_digest,
                        lease.incarnation,
                    ),
                )
            ).fetchone()
            await conn.execute(
                "UPDATE public.bot_dev_watch_heads SET desired_sequence=%s,desired_request_id=%s WHERE bot_id=%s AND watch_id=%s",
                (request["sequence"], request_id, self.store.bot_id, watch_id),
            )
            if head["desired_request_id"]:
                await conn.execute(
                    "INSERT INTO public.bot_dev_watch_results(bot_id,request_id,status,error_code) "
                    "VALUES(%s,%s,'SUPERSEDED','WATCH_SUPERSEDED') ON CONFLICT DO NOTHING",
                    (self.store.bot_id, head["desired_request_id"]),
                )
            await self.store._event(
                conn,
                "dev.desired",
                request_id,
                {
                    "watch_id": watch_id,
                    "sequence": request["sequence"],
                    "source_digest": source_digest,
                },
            )
            await self.store._host(conn, lease, runtime)
            return request

    async def built(
        self,
        lease: HostLease,
        watch_id: str,
        request_id: str,
        *,
        source_digest: str,
        package_digest: str,
        artifact: str,
        component_id: str,
    ) -> None:
        values = [
            _text(value, 4096 if key == 2 else 256)
            for key, value in enumerate((source_digest, package_digest, artifact, component_id))
        ]
        source_digest, package_digest, artifact, component_id = values
        try:
            async with self.store.pool.connection() as conn, conn.transaction():
                head, runtime = await self._guard(conn, lease, watch_id)
                await self.hydrate_in(conn, head)
                request = await (
                    await conn.execute(
                        "SELECT * FROM public.bot_dev_watch_requests WHERE bot_id=%s AND request_id=%s",
                        (self.store.bot_id, request_id),
                    )
                ).fetchone()
                if (
                    not request
                    or request["watch_id"] != watch_id
                    or request["source_digest"] != source_digest
                    or head["desired_request_id"] != request_id
                ):
                    raise ForestError(
                        "WATCH_SUPERSEDED", "A newer development edit superseded this build"
                    )
                if head["component_id"] not in {None, component_id}:
                    raise ForestError(
                        "WATCH_COMPONENT_CHANGED", "A watch cannot change component identity", 409
                    )
                existing = await (
                    await conn.execute(
                        "SELECT * FROM public.bot_dev_watch_builds WHERE bot_id=%s AND request_id=%s",
                        (self.store.bot_id, request_id),
                    )
                ).fetchone()
                if existing:
                    if any(
                        existing[key] != value
                        for key, value in (
                            ("package_digest", package_digest),
                            ("artifact", artifact),
                            ("component_id", component_id),
                        )
                    ):
                        raise ForestError(
                            "IDEMPOTENCY_CONFLICT", "Development build artifact is already frozen"
                        )
                else:
                    await conn.execute(
                        "INSERT INTO public.bot_dev_watch_builds(bot_id,request_id,package_digest,artifact,component_id) VALUES(%s,%s,%s,%s,%s)",
                        (self.store.bot_id, request_id, package_digest, artifact, component_id),
                    )
                await conn.execute(
                    "UPDATE public.bot_dev_watch_heads SET component_id=%s WHERE bot_id=%s AND watch_id=%s",
                    (component_id, self.store.bot_id, watch_id),
                )
                await self.store._host(conn, lease, runtime)
        except UniqueViolation:
            raise ForestError(
                "WATCH_COMPONENT_CONFLICT", "Another development watch owns this component", 409
            ) from None

    async def activate(
        self,
        lease: HostLease,
        watch_id: str,
        request_id: str,
        operation_id: str,
        *,
        recovering: bool = False,
    ) -> None:
        _text(operation_id)
        async with self.store.pool.connection() as conn, conn.transaction():
            head, runtime = await self._guard(conn, lease, watch_id)
            await self.hydrate_in(conn, head)
            expected = head["good_request_id"] if recovering else head["desired_request_id"]
            if expected != request_id:
                raise ForestError(
                    "WATCH_SUPERSEDED", "The requested development activation is no longer current"
                )
            values = {
                "watch_id": watch_id,
                "request_id": request_id,
                "expected_desired_request_id": head["desired_request_id"],
                "recovering": recovering,
                "host_incarnation": lease.incarnation,
            }
            previous = await (
                await conn.execute(
                    "SELECT * FROM public.bot_dev_watch_activations WHERE bot_id=%s AND operation_id=%s",
                    (self.store.bot_id, operation_id),
                )
            ).fetchone()
            if previous:
                if any(previous[key] != value for key, value in values.items()):
                    raise ForestError(
                        "IDEMPOTENCY_CONFLICT", "Development operation already has an activation"
                    )
            else:
                await conn.execute(
                    "INSERT INTO public.bot_dev_watch_activations(bot_id,operation_id,watch_id,request_id,expected_desired_request_id,recovering,host_incarnation) "
                    "VALUES(%s,%s,%s,%s,%s,%s,%s)",
                    (self.store.bot_id, operation_id, *values.values()),
                )
            await self.store._host(conn, lease, runtime)

    async def failed(
        self, lease: HostLease, watch_id: str, request_id: str, error_code: str
    ) -> None:
        error_code = _text(error_code)
        async with self.store.pool.connection() as conn, conn.transaction():
            head, runtime = await self._guard(conn, lease, watch_id)
            await conn.execute(
                "INSERT INTO public.bot_dev_watch_results(bot_id,request_id,status,error_code) "
                "SELECT bot_id,request_id,%s,%s FROM public.bot_dev_watch_requests "
                "WHERE bot_id=%s AND watch_id=%s AND request_id=%s ON CONFLICT DO NOTHING",
                (
                    "SUPERSEDED" if head["desired_request_id"] != request_id else "FAILED",
                    error_code,
                    self.store.bot_id,
                    watch_id,
                    request_id,
                ),
            )
            await self.store._host(conn, lease, runtime)
