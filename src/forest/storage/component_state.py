"""Kernel-owned query and command ports for bounded plugin component state."""

from __future__ import annotations

import json

from psycopg import sql

from forest.errors import ForestError
from forest.packages import digest_json


def _bounded(value, field: str, limit: int) -> str:
    if (
        not isinstance(value, str)
        or not 1 <= len(value.encode("utf-8")) <= limit
        or any(ord(char) < 32 or ord(char) == 127 for char in value)
    ):
        raise ForestError("COMPONENT_STATE_INPUT_INVALID", f"Invalid {field}", 422)
    return value


class ComponentStateQueries:
    """Read component state without acquiring write locks or creating defaults."""

    def __init__(self, runtime, payloads) -> None:
        self.runtime = runtime
        self.payloads = payloads

    async def get(
        self,
        *,
        component_id: str,
        generation_id: str,
        state_key: str,
    ) -> dict:
        _bounded(state_key, "state_key", 128)
        async with self.runtime.store.pool.connection() as conn, conn.transaction():
            await self.runtime.verify_generation_member_in(
                conn,
                component_id=component_id,
                generation_id=generation_id,
            )
            row = await (
                await conn.execute(
                    sql.SQL(
                        "SELECT revision,deleted,value_payload_id,value_digest,value_owner_id "
                        "FROM {} WHERE bot_id=%s AND component_id=%s AND state_key=%s"
                    ).format(self.runtime._table("state_heads")),
                    (self.runtime.store.bot_id, component_id, state_key),
                )
            ).fetchone()
            if row is None or row["deleted"]:
                return {"value": None, "revision": 0 if row is None else row["revision"]}
            reference = await self.payloads.verify_in(
                conn,
                payload_id=row["value_payload_id"],
                digest=row["value_digest"],
                owner_component_id=component_id,
                owner_kind="component.state",
                owner_id=row["value_owner_id"],
            )
            body = await self.payloads.read_bytes_in(
                conn,
                reference.payload_id,
            )
            return {"value": json.loads(body), "revision": row["revision"]}


class ComponentStateCommands:
    """Commit one immutable, revision-checked component state operation."""

    def __init__(self, runtime, payloads) -> None:
        self.runtime = runtime
        self.payloads = payloads

    async def commit(
        self,
        *,
        component_id: str,
        generation_id: str,
        call_id: str,
        state_key: str,
        value,
        expected_revision: int,
        operation_id: str,
    ) -> dict:
        _bounded(call_id, "call_id", 128)
        _bounded(state_key, "state_key", 128)
        _bounded(operation_id, "operation_id", 256)
        if type(expected_revision) is not int or expected_revision < 0:
            raise ForestError(
                "COMPONENT_STATE_INPUT_INVALID", "Invalid expected_revision", 422
            )
        input_digest = digest_json(
            {
                "state_key": state_key,
                "value": value,
                "expected_revision": expected_revision,
            }
        )
        async with self.runtime.store.pool.connection() as conn, conn.transaction():
            await self.runtime.verify_generation_member_in(
                conn,
                component_id=component_id,
                generation_id=generation_id,
            )
            for lock_id in (
                "forest.kernel.runtime/state-operation/"
                f"{self.runtime.store.bot_id}/{component_id}/{operation_id}",
                "forest.kernel.runtime/state/"
                f"{self.runtime.store.bot_id}/{component_id}/{state_key}",
            ):
                await conn.execute(
                    "SELECT pg_advisory_xact_lock(hashtextextended(%s,0))",
                    (lock_id,),
                )
            replay = await (
                await conn.execute(
                    sql.SQL(
                        "SELECT input_digest,revision,value_digest FROM {} "
                        "WHERE bot_id=%s AND component_id=%s AND operation_id=%s"
                    ).format(self.runtime._table("state_steps")),
                    (self.runtime.store.bot_id, component_id, operation_id),
                )
            ).fetchone()
            if replay is not None:
                if replay["input_digest"] != input_digest:
                    raise ForestError(
                        "COMPONENT_STATE_OPERATION_CONFLICT",
                        "State operation input differs",
                        409,
                    )
                return {
                    "value": value,
                    "revision": replay["revision"],
                    "value_digest": replay["value_digest"],
                }
            head = await (
                await conn.execute(
                    sql.SQL(
                        "SELECT revision FROM {} WHERE bot_id=%s AND component_id=%s "
                        "AND state_key=%s FOR UPDATE"
                    ).format(self.runtime._table("state_heads")),
                    (self.runtime.store.bot_id, component_id, state_key),
                )
            ).fetchone()
            current_revision = 0 if head is None else head["revision"]
            if current_revision != expected_revision:
                raise ForestError(
                    "COMPONENT_STATE_REVISION_CONFLICT",
                    "Component state revision changed",
                    409,
                    details={"current_revision": current_revision},
                )
            deletion_epoch = await self.payloads.epoch_in(conn)
            saved = await self.payloads.put_json_in(
                conn,
                owner_component_id=component_id,
                owner_kind="component.state",
                owner_id=operation_id,
                value=value,
                expected_deletion_epoch=deletion_epoch,
            )
            revision = expected_revision + 1
            await conn.execute(
                sql.SQL(
                    "INSERT INTO {}(bot_id,component_id,operation_id,generation_id,call_id,"
                    "state_key,expected_revision,revision,input_digest,value_payload_id,"
                    "value_digest) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
                ).format(self.runtime._table("state_steps")),
                (
                    self.runtime.store.bot_id,
                    component_id,
                    operation_id,
                    generation_id,
                    call_id,
                    state_key,
                    expected_revision,
                    revision,
                    input_digest,
                    saved.payload_id,
                    saved.content_digest,
                ),
            )
            if head is None:
                await conn.execute(
                    sql.SQL(
                        "INSERT INTO {}(bot_id,component_id,state_key,revision,deleted,"
                        "value_payload_id,value_digest,value_owner_id) "
                        "VALUES (%s,%s,%s,%s,false,%s,%s,%s)"
                    ).format(self.runtime._table("state_heads")),
                    (
                        self.runtime.store.bot_id,
                        component_id,
                        state_key,
                        revision,
                        saved.payload_id,
                        saved.content_digest,
                        operation_id,
                    ),
                )
            else:
                updated = await (
                    await conn.execute(
                        sql.SQL(
                            "UPDATE {} SET revision=%s,deleted=false,value_payload_id=%s,"
                            "value_digest=%s,value_owner_id=%s WHERE bot_id=%s "
                            "AND component_id=%s AND state_key=%s AND revision=%s "
                            "RETURNING revision"
                        ).format(self.runtime._table("state_heads")),
                        (
                            revision,
                            saved.payload_id,
                            saved.content_digest,
                            operation_id,
                            self.runtime.store.bot_id,
                            component_id,
                            state_key,
                            expected_revision,
                        ),
                    )
                ).fetchone()
                if updated is None:
                    raise ForestError(
                        "COMPONENT_STATE_REVISION_CONFLICT",
                        "Component state revision changed",
                        409,
                    )
            return {
                "value": value,
                "revision": revision,
                "value_digest": saved.content_digest,
            }
