"""Durable effective business-command registry bound to component route publication."""

from __future__ import annotations

import json
import keyword

from forest.command_contract import (
    IDENTIFIER,
    normalize_command_name,
    validate_argument_mode,
    validate_command_permission,
    validate_command_priority,
)
from forest.errors import ForestError
from forest.packages import digest_json


class CommandRegistryStore:
    def __init__(self, store):
        self.store = store

    @staticmethod
    def _normalize(definitions) -> list[dict]:
        try:
            copied = json.loads(json.dumps(definitions or [], allow_nan=False))
        except (TypeError, ValueError, UnicodeError):
            raise ForestError(
                "COMMAND_DECLARATION_INVALID", "Command declarations must be finite JSON", 422
            ) from None
        if not isinstance(copied, list) or len(copied) > 64:
            raise ForestError(
                "COMMAND_DECLARATION_INVALID", "At most 64 command declarations are allowed", 422
            )
        result = []
        ids = set()
        names = set()
        for item in copied:
            if not isinstance(item, dict) or set(item) != {
                "command_id", "name", "handler", "permission", "argument_mode",
                "input_schema", "schema_digest", "priority",
            }:
                raise ForestError(
                    "COMMAND_DECLARATION_INVALID", "Command declaration fields differ", 422
                )
            try:
                name = normalize_command_name(item["name"], require_normalized=True)
                permission = validate_command_permission(item["permission"])
                argument_mode = validate_argument_mode(item["argument_mode"])
                priority = validate_command_priority(item["priority"])
            except ValueError as exc:
                raise ForestError("COMMAND_DECLARATION_INVALID", str(exc), 422) from None
            if (
                not isinstance(item["command_id"], str)
                or not IDENTIFIER.fullmatch(item["command_id"])
                or item["command_id"] in ids
                or name in names
                or not isinstance(item["handler"], str)
                or not item["handler"].isidentifier()
                or keyword.iskeyword(item["handler"])
                or not isinstance(item["input_schema"], str)
                or not 1 <= len(item["input_schema"]) <= 256
                or not isinstance(item["schema_digest"], str)
                or not item["schema_digest"].startswith("sha256:")
                or len(item["schema_digest"]) != 71
                or any(character not in "0123456789abcdef" for character in item["schema_digest"][7:])
            ):
                raise ForestError(
                    "COMMAND_DECLARATION_INVALID", "Command declaration identity is invalid", 422
                )
            ids.add(item["command_id"])
            names.add(name)
            normalized = {
                **item,
                "name": name,
                "permission": permission,
                "argument_mode": argument_mode,
                "priority": priority,
            }
            normalized["spec_digest"] = digest_json(normalized)
            result.append(normalized)
        return sorted(result, key=lambda item: item["command_id"])

    async def _clock_in(self, conn) -> dict:
        await conn.execute(
            "INSERT INTO public.bot_command_clocks(bot_id) VALUES (%s) ON CONFLICT DO NOTHING",
            (self.store.bot_id,),
        )
        return await (
            await conn.execute(
                "SELECT * FROM public.bot_command_clocks WHERE bot_id=%s FOR UPDATE",
                (self.store.bot_id,),
            )
        ).fetchone()

    async def register_generation_in(
        self,
        conn,
        generation_id: str,
        component_id: str,
        package_digest: str,
        definitions: list[dict],
        *,
        existing_generation: bool = False,
    ) -> None:
        definitions = self._normalize(definitions)
        set_digest = digest_json(definitions)
        current = await (
            await conn.execute(
                "SELECT * FROM public.bot_command_generations WHERE bot_id=%s AND generation_id=%s",
                (self.store.bot_id, generation_id),
            )
        ).fetchone()
        if current is not None:
            if any(
                current[key] != expected
                for key, expected in (
                    ("component_id", component_id),
                    ("package_digest", package_digest),
                    ("spec_set_digest", set_digest),
                    ("command_count", len(definitions)),
                )
            ):
                raise ForestError(
                    "GENERATION_CONFLICT", "Generation command declarations cannot be reused", 409
                )
            return
        if existing_generation:
            raise ForestError(
                "GENERATION_CONFLICT", "Generation command declarations cannot be added later", 409
            )
        await conn.execute(
            "INSERT INTO public.bot_command_generations("
            "bot_id,generation_id,component_id,package_digest,spec_set_digest,command_count) "
            "VALUES (%s,%s,%s,%s,%s,%s)",
            (
                self.store.bot_id, generation_id, component_id, package_digest,
                set_digest, len(definitions),
            ),
        )
        for item in definitions:
            await conn.execute(
                "INSERT INTO public.bot_generation_command_definitions("
                "bot_id,generation_id,component_id,command_id,name,handler,permission,"
                "argument_mode,input_schema,schema_digest,priority,spec_digest) "
                "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
                (
                    self.store.bot_id, generation_id, component_id, item["command_id"],
                    item["name"], item["handler"], item["permission"],
                    item["argument_mode"], item["input_schema"], item["schema_digest"],
                    item["priority"], item["spec_digest"],
                ),
            )

    async def publish_generation_in(
        self, conn, generation_id: str, component_id: str, route_epoch: int
    ) -> int:
        clock = await self._clock_in(conn)
        command_set = await (
            await conn.execute(
                "SELECT * FROM public.bot_command_generations WHERE bot_id=%s "
                "AND generation_id=%s AND component_id=%s",
                (self.store.bot_id, generation_id, component_id),
            )
        ).fetchone()
        if command_set is None:
            raise ForestError(
                "COMMAND_GENERATION_MISSING", "Candidate has no immutable command set", 409
            )
        definitions = await (
            await conn.execute(
                "SELECT * FROM public.bot_generation_command_definitions WHERE bot_id=%s "
                "AND generation_id=%s ORDER BY command_id",
                (self.store.bot_id, generation_id),
            )
        ).fetchall()
        heads = await (
            await conn.execute(
                "SELECT * FROM public.bot_command_heads WHERE bot_id=%s ORDER BY command_id FOR UPDATE",
                (self.store.bot_id,),
            )
        ).fetchall()
        by_id = {row["command_id"]: row for row in heads}
        wanted = {row["command_id"] for row in definitions}
        for head in heads:
            if (
                head["component_id"] == component_id
                and not head["retired"]
                and head["command_id"] not in wanted
            ):
                raise ForestError(
                    "COMMAND_REMOVAL_REQUIRES_RETIREMENT",
                    "Business commands require explicit retirement before removal",
                    409,
                )
        for definition in definitions:
            current = by_id.get(definition["command_id"])
            if current is not None and (
                current["component_id"] != component_id or current["name"] != definition["name"]
            ):
                raise ForestError(
                    "COMMAND_IDENTITY_CONFLICT", "Command ID changed owner or name", 409
                )
            if current is not None and current["retired"]:
                raise ForestError(
                    "COMMAND_REACTIVATION_REQUIRES_MANAGEMENT",
                    "A retired command identity cannot be restored by deployment",
                    409,
                )
            owner = next(
                (
                    head for head in heads
                    if head["active"] and head["name"] == definition["name"]
                    and head["command_id"] != definition["command_id"]
                ),
                None,
            )
            if owner is not None:
                raise ForestError(
                    "COMMAND_NAME_CONFLICT", "Another effective component owns this command name", 409
                )
        revision = clock["revision"] + 1
        await conn.execute(
            "INSERT INTO public.bot_command_publications("
            "bot_id,registry_revision,component_id,generation_id,route_epoch,operation_kind,spec_set_digest) "
            "VALUES (%s,%s,%s,%s,%s,'INSTALL',%s)",
            (
                self.store.bot_id, revision, component_id, generation_id, route_epoch,
                command_set["spec_set_digest"],
            ),
        )
        for definition in definitions:
            await conn.execute(
                "INSERT INTO public.bot_command_heads("
                "bot_id,command_id,component_id,generation_id,route_epoch,registry_revision,"
                "name,handler,permission,argument_mode,input_schema,schema_digest,priority,"
                "spec_digest,active) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,true) "
                "ON CONFLICT(bot_id,command_id) DO UPDATE SET generation_id=EXCLUDED.generation_id,"
                "route_epoch=EXCLUDED.route_epoch,registry_revision=EXCLUDED.registry_revision,"
                "handler=EXCLUDED.handler,permission=EXCLUDED.permission,"
                "argument_mode=EXCLUDED.argument_mode,input_schema=EXCLUDED.input_schema,"
                "schema_digest=EXCLUDED.schema_digest,priority=EXCLUDED.priority,"
                "spec_digest=EXCLUDED.spec_digest,active=true,updated_at=clock_timestamp()",
                (
                    self.store.bot_id, definition["command_id"], component_id, generation_id,
                    route_epoch, revision, definition["name"], definition["handler"],
                    definition["permission"], definition["argument_mode"],
                    definition["input_schema"], definition["schema_digest"],
                    definition["priority"], definition["spec_digest"],
                ),
            )
        await conn.execute(
            "UPDATE public.bot_command_clocks SET revision=%s,updated_at=clock_timestamp() "
            "WHERE bot_id=%s",
            (revision, self.store.bot_id),
        )
        return revision

    async def disable_component_in(
        self, conn, component_id: str, route_epoch: int
    ) -> int:
        clock = await self._clock_in(conn)
        await (
            await conn.execute(
                "SELECT command_id FROM public.bot_command_heads WHERE bot_id=%s "
                "ORDER BY command_id FOR UPDATE",
                (self.store.bot_id,),
            )
        ).fetchall()
        revision = clock["revision"] + 1
        await conn.execute(
            "INSERT INTO public.bot_command_publications("
            "bot_id,registry_revision,component_id,generation_id,route_epoch,operation_kind,spec_set_digest) "
            "VALUES (%s,%s,%s,NULL,%s,'DISABLE',%s)",
            (self.store.bot_id, revision, component_id, route_epoch, digest_json([])),
        )
        await conn.execute(
            "UPDATE public.bot_command_heads SET generation_id=NULL,route_epoch=%s,"
            "registry_revision=%s,active=false,updated_at=clock_timestamp() "
            "WHERE bot_id=%s AND component_id=%s AND active=true",
            (route_epoch, revision, self.store.bot_id, component_id),
        )
        await conn.execute(
            "UPDATE public.bot_command_clocks SET revision=%s,updated_at=clock_timestamp() "
            "WHERE bot_id=%s",
            (revision, self.store.bot_id),
        )
        return revision

    async def snapshot(self) -> dict:
        async with self.store.pool.connection() as conn, conn.transaction():
            await self.store._identity(conn)
            clock = await (
                await conn.execute(
                    "SELECT * FROM public.bot_command_clocks WHERE bot_id=%s FOR SHARE",
                    (self.store.bot_id,),
                )
            ).fetchone()
            rows = await (
                await conn.execute(
                    "SELECT * FROM public.bot_command_heads WHERE bot_id=%s AND active=true "
                    "ORDER BY name,command_id",
                    (self.store.bot_id,),
                )
            ).fetchall()
            return {"revision": clock["revision"] if clock else 0, "commands": rows}

    async def retire(
        self,
        lease,
        *,
        component_id: str,
        command_id: str,
        operation_id: str,
        expected_management_revision: int,
    ) -> dict:
        if (
            not all(
                isinstance(value, str) and 1 <= len(value) <= 256
                for value in (component_id, command_id, operation_id)
            )
            or type(expected_management_revision) is not int
            or expected_management_revision < 0
        ):
            raise ForestError("COMMAND_RETIREMENT_INVALID", "Retirement fields are invalid", 422)
        async with self.store.pool.connection() as conn, conn.transaction():
            clock = await self._clock_in(conn)
            runtime = await self.store._runtime(conn)
            await self.store._host(conn, lease, runtime)
            replay = await (
                await conn.execute(
                    "SELECT * FROM public.bot_command_retirements WHERE bot_id=%s AND operation_id=%s",
                    (self.store.bot_id, operation_id),
                )
            ).fetchone()
            if replay is not None:
                if any(
                    replay[key] != value
                    for key, value in (
                        ("component_id", component_id),
                        ("command_id", command_id),
                        ("expected_management_revision", expected_management_revision),
                    )
                ):
                    raise ForestError("IDEMPOTENCY_CONFLICT", "Retirement operation differs", 409)
                return replay
            head = await (
                await conn.execute(
                    "SELECT * FROM public.bot_command_heads WHERE bot_id=%s AND command_id=%s FOR UPDATE",
                    (self.store.bot_id, command_id),
                )
            ).fetchone()
            if head is None or head["component_id"] != component_id:
                raise ForestError("COMMAND_NOT_FOUND", "Command identity does not exist", 404)
            if head["retired"]:
                raise ForestError("COMMAND_ALREADY_RETIRED", "Command is already retired", 409)
            if head["management_revision"] != expected_management_revision:
                raise ForestError("COMMAND_RETIREMENT_CONFLICT", "Command revision changed", 409)
            revision = clock["revision"] + 1
            resulting = expected_management_revision + 1
            row = await (
                await conn.execute(
                    "INSERT INTO public.bot_command_retirements("
                    "bot_id,operation_id,component_id,command_id,expected_management_revision,"
                    "resulting_management_revision,registry_revision,host_incarnation,host_owner_epoch) "
                    "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s) RETURNING *",
                    (
                        self.store.bot_id, operation_id, component_id, command_id,
                        expected_management_revision, resulting, revision,
                        lease.incarnation, lease.owner_epoch,
                    ),
                )
            ).fetchone()
            await conn.execute(
                "UPDATE public.bot_command_heads SET generation_id=NULL,active=false,retired=true,"
                "management_revision=%s,registry_revision=%s,retired_at=clock_timestamp(),"
                "updated_at=clock_timestamp() WHERE bot_id=%s AND command_id=%s",
                (resulting, revision, self.store.bot_id, command_id),
            )
            await conn.execute(
                "UPDATE public.bot_command_clocks SET revision=%s,updated_at=clock_timestamp() "
                "WHERE bot_id=%s",
                (revision, self.store.bot_id),
            )
            assert row is not None
            return row
