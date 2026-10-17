"""Bot-scoped, revocable authority for the small independent Core rescue path."""

from __future__ import annotations

import re

from psycopg import sql

from forest.errors import ForestError
from forest.packages import digest_json

_GROUP = re.compile(r"[1-9][0-9]{0,19}\Z")
_PRINCIPAL = re.compile(r"qq:[1-9][0-9]{0,19}\Z")
_ACTIONS = frozenset({"status", "cancel", "disable", "reload"})
_ACTION_COLUMNS = {
    "status": "allow_status",
    "cancel": "allow_cancel",
    "disable": "allow_disable",
    "reload": "allow_reload",
}


class KernelRescuePolicyAuthority:
    """Store only rescue grants; ordinary Shell policy stays with its package."""

    def __init__(self, store, namespace: str) -> None:
        self.store = store
        self.namespace = namespace

    def _table(self, name: str):
        return sql.Identifier(self.namespace, name)

    @staticmethod
    def _key(binding_ref, group_ref, principal_ref) -> tuple[str, str, str]:
        if (
            not isinstance(binding_ref, str)
            or not 1 <= len(binding_ref.encode("utf-8")) <= 256
            or not isinstance(group_ref, str)
            or _GROUP.fullmatch(group_ref) is None
            or not isinstance(principal_ref, str)
            or _PRINCIPAL.fullmatch(principal_ref) is None
        ):
            raise ForestError("RESCUE_POLICY_INPUT_INVALID", "Invalid rescue identity", 422)
        return binding_ref, group_ref, principal_ref

    @staticmethod
    def _actions(values) -> tuple[str, ...]:
        if (
            not isinstance(values, list)
            or len(values) > len(_ACTIONS)
            or any(not isinstance(item, str) or item not in _ACTIONS for item in values)
            or len(values) != len(set(values))
        ):
            raise ForestError("RESCUE_POLICY_INPUT_INVALID", "Invalid rescue actions", 422)
        return tuple(sorted(values))

    async def read(self) -> dict:
        async with self.store.pool.connection() as conn, conn.transaction():
            await conn.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY")
            revision = await (
                await conn.execute(
                    sql.SQL("SELECT revision FROM {} WHERE bot_id=%s").format(
                        self._table("rescue_policy_state")
                    ),
                    (self.store.bot_id,),
                )
            ).fetchone()
            if revision is None:
                raise ForestError("RESCUE_POLICY_UNAVAILABLE", "Rescue policy is unavailable", 503)
            rows = await (
                await conn.execute(
                    sql.SQL(
                        "SELECT binding_ref,group_ref,principal_ref,allow_status,allow_cancel,"
                        "allow_disable,allow_reload,changed_revision FROM {} WHERE bot_id=%s "
                        "ORDER BY binding_ref,group_ref,principal_ref LIMIT 33"
                    ).format(self._table("rescue_grants")),
                    (self.store.bot_id,),
                )
            ).fetchall()
            if len(rows) > 32:
                raise ForestError("RESCUE_POLICY_INVALID", "Rescue policy exceeds its bound", 409)
        return {
            "protocol": "forest.core-rescue-policy/1",
            "revision": revision["revision"],
            "grants": [
                {
                    "binding_ref": row["binding_ref"],
                    "group_ref": row["group_ref"],
                    "principal_ref": row["principal_ref"],
                    "actions": [
                        action for action, column in _ACTION_COLUMNS.items() if row[column]
                    ],
                    "changed_revision": row["changed_revision"],
                }
                for row in rows
            ],
        }

    async def put(
        self,
        *,
        operation_id: str,
        expected_revision: int,
        binding_ref: str,
        group_ref: str,
        principal_ref: str,
        actions: list[str],
        actor_ref: str,
        authorization,
        authorization_guard,
    ) -> dict:
        binding_ref, group_ref, principal_ref = self._key(
            binding_ref, group_ref, principal_ref
        )
        selected = self._actions(actions)
        if (
            not isinstance(operation_id, str)
            or not 1 <= len(operation_id.encode("utf-8")) <= 256
            or type(expected_revision) is not int
            or expected_revision < 1
            or not isinstance(actor_ref, str)
            or not 1 <= len(actor_ref.encode("utf-8")) <= 256
            or not callable(authorization_guard)
        ):
            raise ForestError("RESCUE_POLICY_INPUT_INVALID", "Invalid rescue mutation", 422)
        body = {
            "expected_revision": expected_revision,
            "binding_ref": binding_ref,
            "group_ref": group_ref,
            "principal_ref": principal_ref,
            "actions": actions,
        }
        input_digest = digest_json(body)
        async with self.store.pool.connection() as conn, conn.transaction():
            await authorization_guard(
                conn,
                ticket=authorization,
                component_id="kernel.runtime",
                operation_id=operation_id,
                action_key="rescue.grant.put",
                input_digest=input_digest,
                plan_digest=None,
            )
            state = await (
                await conn.execute(
                    sql.SQL("SELECT revision FROM {} WHERE bot_id=%s FOR UPDATE").format(
                        self._table("rescue_policy_state")
                    ),
                    (self.store.bot_id,),
                )
            ).fetchone()
            if state is None:
                raise ForestError("RESCUE_POLICY_UNAVAILABLE", "Rescue policy is unavailable", 503)
            existing = await (
                await conn.execute(
                    sql.SQL(
                        "SELECT input_digest,actor_ref,resulting_revision,changed FROM {} "
                        "WHERE bot_id=%s AND operation_id=%s"
                    ).format(self._table("rescue_policy_operations")),
                    (self.store.bot_id, operation_id),
                )
            ).fetchone()
            if existing is not None:
                if existing["input_digest"] != input_digest or existing["actor_ref"] != actor_ref:
                    raise ForestError("RESCUE_OPERATION_CONFLICT", "Rescue operation differs", 409)
                return self._receipt(
                    operation_id, existing["resulting_revision"], existing["changed"]
                )
            if state["revision"] != expected_revision:
                raise ForestError("RESCUE_POLICY_CONFLICT", "Rescue policy changed", 409)
            previous = await (
                await conn.execute(
                    sql.SQL(
                        "SELECT allow_status,allow_cancel,allow_disable,allow_reload FROM {} "
                        "WHERE bot_id=%s AND binding_ref=%s AND group_ref=%s AND principal_ref=%s"
                    ).format(self._table("rescue_grants")),
                    (self.store.bot_id, binding_ref, group_ref, principal_ref),
                )
            ).fetchone()
            requested = {
                column: action in selected for action, column in _ACTION_COLUMNS.items()
            }
            changed = previous is not None if not selected else (
                previous is None
                or any(previous[column] != value for column, value in requested.items())
            )
            next_revision = expected_revision + 1
            if not selected:
                await conn.execute(
                    sql.SQL(
                        "DELETE FROM {} WHERE bot_id=%s AND binding_ref=%s "
                        "AND group_ref=%s AND principal_ref=%s"
                    ).format(self._table("rescue_grants")),
                    (self.store.bot_id, binding_ref, group_ref, principal_ref),
                )
            elif changed:
                if previous is None:
                    count = await (
                        await conn.execute(
                            sql.SQL("SELECT count(*) AS count FROM {} WHERE bot_id=%s").format(
                                self._table("rescue_grants")
                            ),
                            (self.store.bot_id,),
                        )
                    ).fetchone()
                    if count["count"] >= 32:
                        raise ForestError("RESCUE_POLICY_FULL", "Rescue grant limit reached", 409)
                await conn.execute(
                    sql.SQL(
                        "INSERT INTO {}(bot_id,binding_ref,group_ref,principal_ref,"
                        "allow_status,allow_cancel,allow_disable,allow_reload,changed_revision) "
                        "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s) ON CONFLICT "
                        "(bot_id,binding_ref,group_ref,principal_ref) DO UPDATE SET "
                        "allow_status=EXCLUDED.allow_status,allow_cancel=EXCLUDED.allow_cancel,"
                        "allow_disable=EXCLUDED.allow_disable,allow_reload=EXCLUDED.allow_reload,"
                        "changed_revision=EXCLUDED.changed_revision"
                    ).format(self._table("rescue_grants")),
                    (
                        self.store.bot_id, binding_ref, group_ref, principal_ref,
                        requested["allow_status"], requested["allow_cancel"],
                        requested["allow_disable"], requested["allow_reload"], next_revision,
                    ),
                )
            await conn.execute(
                sql.SQL("UPDATE {} SET revision=%s WHERE bot_id=%s").format(
                    self._table("rescue_policy_state")
                ),
                (next_revision, self.store.bot_id),
            )
            await conn.execute(
                sql.SQL(
                    "INSERT INTO {}(bot_id,operation_id,input_digest,actor_ref,"
                    "resulting_revision,changed) VALUES (%s,%s,%s,%s,%s,%s)"
                ).format(self._table("rescue_policy_operations")),
                (
                    self.store.bot_id, operation_id, input_digest, actor_ref,
                    next_revision, changed,
                ),
            )
        return self._receipt(operation_id, next_revision, changed)

    async def authorize_in(
        self, conn, *, binding_ref: str, group_ref: str, principal_ref: str, action: str
    ) -> int:
        binding_ref, group_ref, principal_ref = self._key(
            binding_ref, group_ref, principal_ref
        )
        if action not in _ACTIONS:
            raise ForestError("RESCUE_ACTION_INVALID", "Unknown Core rescue action", 422)
        state = await (
            await conn.execute(
                sql.SQL("SELECT revision FROM {} WHERE bot_id=%s FOR SHARE").format(
                    self._table("rescue_policy_state")
                ),
                (self.store.bot_id,),
            )
        ).fetchone()
        if state is None:
            raise ForestError("RESCUE_POLICY_UNAVAILABLE", "Rescue policy is unavailable", 503)
        grant = await (
            await conn.execute(
                sql.SQL(
                    "SELECT allow_status,allow_cancel,allow_disable,allow_reload FROM {} "
                    "WHERE bot_id=%s AND binding_ref=%s AND group_ref=%s AND principal_ref=%s"
                ).format(self._table("rescue_grants")),
                (self.store.bot_id, binding_ref, group_ref, principal_ref),
            )
        ).fetchone()
        if grant is None or not grant[_ACTION_COLUMNS[action]]:
            raise ForestError("RESCUE_PRINCIPAL_DENIED", "Core rescue action denied", 403)
        return state["revision"]

    @staticmethod
    def _receipt(operation_id: str, revision: int, changed: bool) -> dict:
        return {
            "protocol": "forest.core-rescue-policy-receipt/1",
            "operation_id": operation_id,
            "revision": revision,
            "changed": changed,
        }
