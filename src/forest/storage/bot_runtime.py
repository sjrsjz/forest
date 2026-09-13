"""Reviewed Bot data-admission control shared by Web and group Shell."""

from __future__ import annotations

from forest.errors import ForestError
from forest.storage.bot import BotStore, HostLease
from forest.storage.control import BotControl, _normalized_input


class BotRuntimeAdministration:
    def __init__(self, store: BotStore, *, control: BotControl | None = None):
        self.store = store
        self.control = control or BotControl(store)

    @staticmethod
    def _payload(payload):
        payload = _normalized_input(payload)
        if set(payload) != {"expected_maintenance_epoch", "data_admission", "reason"}:
            raise ForestError("BOT_MAINTENANCE_INVALID", "Maintenance input fields differ", 422)
        epoch, admission, reason = (
            payload["expected_maintenance_epoch"], payload["data_admission"], payload["reason"]
        )
        if type(epoch) is not int or not 0 <= epoch < 10**18:
            raise ForestError("BOT_MAINTENANCE_INVALID", "Maintenance epoch is invalid", 422)
        if type(admission) is not bool:
            raise ForestError("BOT_MAINTENANCE_INVALID", "Data admission state is invalid", 422)
        if (not isinstance(reason, str) or not reason.strip() or len(reason) > 256
                or any(ord(character) < 32 for character in reason)):
            raise ForestError("BOT_MAINTENANCE_INVALID", "Maintenance reason is invalid", 422)
        return payload

    async def read(self, ticket: dict, operation_id: str, payload: dict) -> dict:
        payload = _normalized_input(payload)
        if payload:
            raise ForestError("BOT_RUNTIME_READ_INVALID", "Runtime read takes no input", 422)
        async with self.store.pool.connection() as conn, conn.transaction():
            claims = await self.control._guard(
                conn, ticket, "bot.runtime.read", operation_id, payload
            )
            runtime = await self.store._runtime(conn)
            await self.control._time(conn, claims)
            return {key: runtime[key] for key in (
                "status", "storage_generation", "maintenance_epoch", "policy_revision",
                "data_admission", "control_admission",
            )}

    async def set_maintenance(
        self, ticket: dict, operation_id: str, payload: dict, *, lease: HostLease
    ) -> dict:
        payload = self._payload(payload)
        async with self.store.pool.connection() as conn, conn.transaction():
            # Maintenance has the same route -> runtime -> host lock order as
            # BotStore.set_maintenance. Shell adds its origin Binding lock between
            # route and runtime; no external wait occurs in this transaction.
            await (
                await conn.execute(
                    "SELECT component_id FROM public.bot_component_routes "
                    "WHERE bot_id=%s ORDER BY component_id FOR UPDATE",
                    (self.store.bot_id,),
                )
            ).fetchall()
            claims = await self.control._guard(
                conn, ticket, "bot.maintenance", operation_id, payload
            )
            runtime = await self.store._runtime(conn)
            replay = await self.control._replay(conn, claims)
            if self.control.is_replay(replay):
                return replay
            if runtime["maintenance_epoch"] != payload["expected_maintenance_epoch"]:
                raise ForestError("MAINTENANCE_CONFLICT", "Maintenance intent changed", 409)
            await self.store._host(conn, lease, runtime)
            await self.control._time(conn, claims)
            row = await (
                await conn.execute(
                    "UPDATE public.bot_runtime_state SET maintenance_epoch=maintenance_epoch+1,"
                    "data_admission=%s WHERE bot_id=%s RETURNING *",
                    (payload["data_admission"], self.store.bot_id),
                )
            ).fetchone()
            if payload["data_admission"]:
                await conn.execute(
                    "UPDATE public.bot_component_routes r SET admission=true "
                    "FROM public.bot_generations g WHERE r.bot_id=%s "
                    "AND g.bot_id=r.bot_id AND g.generation_id=r.generation_id "
                    "AND g.status='ACTIVE' AND g.host_incarnation=%s "
                    "AND g.host_owner_epoch=%s",
                    (self.store.bot_id, lease.incarnation, lease.owner_epoch),
                )
            await self.store._host(conn, lease, row)
            result = {
                "status": row["status"],
                "maintenance_epoch": row["maintenance_epoch"],
                "data_admission": row["data_admission"],
                "control_admission": row["control_admission"],
            }
            await self.control._record(conn, claims, result)
            return result
