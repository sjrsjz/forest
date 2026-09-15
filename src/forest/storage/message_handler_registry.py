"""Durable effective registry for ordinary group-message handlers."""

from __future__ import annotations

import json
import keyword
import re

from forest.errors import ForestError
from forest.packages import digest_json

IDENTIFIER = re.compile(r"[a-zA-Z][a-zA-Z0-9_.-]{0,127}\Z")


class MessageHandlerRegistryStore:
    def __init__(self, store):
        self.store = store

    @staticmethod
    def _normalize(definitions) -> list[dict]:
        try:
            copied = json.loads(json.dumps(definitions or [], allow_nan=False))
        except (TypeError, ValueError, UnicodeError):
            raise ForestError(
                "MESSAGE_HANDLER_DECLARATION_INVALID",
                "Message handler declarations must be finite JSON",
                422,
            ) from None
        if not isinstance(copied, list) or len(copied) > 64:
            raise ForestError(
                "MESSAGE_HANDLER_DECLARATION_INVALID",
                "At most 64 message handlers are allowed per component",
                422,
            )
        result = []
        identities = set()
        methods = set()
        for item in copied:
            if not isinstance(item, dict) or set(item) != {
                "handler_id", "handler", "priority"
            }:
                raise ForestError(
                    "MESSAGE_HANDLER_DECLARATION_INVALID",
                    "Message handler declaration fields differ",
                    422,
                )
            if (
                not isinstance(item["handler_id"], str)
                or not IDENTIFIER.fullmatch(item["handler_id"])
                or item["handler_id"] in identities
                or not isinstance(item["handler"], str)
                or not item["handler"].isidentifier()
                or keyword.iskeyword(item["handler"])
                or item["handler"] in methods
                or type(item["priority"]) is not int
                or not -100 <= item["priority"] <= 100
            ):
                raise ForestError(
                    "MESSAGE_HANDLER_DECLARATION_INVALID",
                    "Message handler identity, method, or priority is invalid",
                    422,
                )
            identities.add(item["handler_id"])
            methods.add(item["handler"])
            normalized = dict(item)
            normalized["spec_digest"] = digest_json(normalized)
            result.append(normalized)
        return sorted(result, key=lambda item: item["handler_id"])

    async def _clock_in(self, conn) -> dict:
        await conn.execute(
            "INSERT INTO public.bot_message_handler_clocks(bot_id) VALUES (%s) "
            "ON CONFLICT DO NOTHING",
            (self.store.bot_id,),
        )
        return await (
            await conn.execute(
                "SELECT * FROM public.bot_message_handler_clocks "
                "WHERE bot_id=%s FOR UPDATE",
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
                "SELECT * FROM public.bot_message_handler_generations "
                "WHERE bot_id=%s AND generation_id=%s",
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
                    ("handler_count", len(definitions)),
                )
            ):
                raise ForestError(
                    "GENERATION_CONFLICT",
                    "Generation message handler declarations cannot be reused",
                    409,
                )
            return
        if existing_generation:
            raise ForestError(
                "GENERATION_CONFLICT",
                "Generation message handler declarations cannot be added later",
                409,
            )
        await conn.execute(
            "INSERT INTO public.bot_message_handler_generations("
            "bot_id,generation_id,component_id,package_digest,spec_set_digest,handler_count) "
            "VALUES (%s,%s,%s,%s,%s,%s)",
            (
                self.store.bot_id,
                generation_id,
                component_id,
                package_digest,
                set_digest,
                len(definitions),
            ),
        )
        for item in definitions:
            await conn.execute(
                "INSERT INTO public.bot_generation_message_handler_definitions("
                "bot_id,generation_id,component_id,handler_id,handler,priority,spec_digest) "
                "VALUES (%s,%s,%s,%s,%s,%s,%s)",
                (
                    self.store.bot_id,
                    generation_id,
                    component_id,
                    item["handler_id"],
                    item["handler"],
                    item["priority"],
                    item["spec_digest"],
                ),
            )

    async def publish_generation_in(
        self, conn, generation_id: str, component_id: str, route_epoch: int
    ) -> int:
        clock = await self._clock_in(conn)
        handler_set = await (
            await conn.execute(
                "SELECT * FROM public.bot_message_handler_generations WHERE bot_id=%s "
                "AND generation_id=%s AND component_id=%s",
                (self.store.bot_id, generation_id, component_id),
            )
        ).fetchone()
        if handler_set is None:
            raise ForestError(
                "MESSAGE_HANDLER_GENERATION_MISSING",
                "Candidate has no immutable message handler set",
                409,
            )
        definitions = await (
            await conn.execute(
                "SELECT * FROM public.bot_generation_message_handler_definitions "
                "WHERE bot_id=%s AND generation_id=%s ORDER BY handler_id",
                (self.store.bot_id, generation_id),
            )
        ).fetchall()
        heads = await (
            await conn.execute(
                "SELECT * FROM public.bot_message_handler_heads WHERE bot_id=%s "
                "ORDER BY component_id,handler_id FOR UPDATE",
                (self.store.bot_id,),
            )
        ).fetchall()
        by_identity = {
            (row["component_id"], row["handler_id"]): row for row in heads
        }
        wanted = {row["handler_id"] for row in definitions}
        for head in heads:
            if (
                head["component_id"] == component_id
                and not head["retired"]
                and head["handler_id"] not in wanted
            ):
                raise ForestError(
                    "MESSAGE_HANDLER_REMOVAL_REQUIRES_RETIREMENT",
                    "Message handlers require explicit retirement before removal",
                    409,
                )
        for definition in definitions:
            current = by_identity.get((component_id, definition["handler_id"]))
            if current is not None and current["retired"]:
                raise ForestError(
                    "MESSAGE_HANDLER_REACTIVATION_REQUIRES_MANAGEMENT",
                    "A retired message handler identity cannot be restored by deployment",
                    409,
                )
        revision = clock["revision"] + 1
        await conn.execute(
            "INSERT INTO public.bot_message_handler_publications("
            "bot_id,registry_revision,component_id,generation_id,route_epoch,"
            "operation_kind,spec_set_digest) VALUES (%s,%s,%s,%s,%s,'INSTALL',%s)",
            (
                self.store.bot_id,
                revision,
                component_id,
                generation_id,
                route_epoch,
                handler_set["spec_set_digest"],
            ),
        )
        for definition in definitions:
            await conn.execute(
                "INSERT INTO public.bot_message_handler_heads("
                "bot_id,component_id,handler_id,generation_id,route_epoch,registry_revision,"
                "handler,priority,spec_digest,active) "
                "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,true) "
                "ON CONFLICT(bot_id,component_id,handler_id) DO UPDATE SET "
                "generation_id=EXCLUDED.generation_id,route_epoch=EXCLUDED.route_epoch,"
                "registry_revision=EXCLUDED.registry_revision,handler=EXCLUDED.handler,"
                "priority=EXCLUDED.priority,spec_digest=EXCLUDED.spec_digest,active=true,"
                "updated_at=clock_timestamp()",
                (
                    self.store.bot_id,
                    component_id,
                    definition["handler_id"],
                    generation_id,
                    route_epoch,
                    revision,
                    definition["handler"],
                    definition["priority"],
                    definition["spec_digest"],
                ),
            )
        await conn.execute(
            "UPDATE public.bot_message_handler_clocks SET revision=%s,"
            "updated_at=clock_timestamp() WHERE bot_id=%s",
            (revision, self.store.bot_id),
        )
        return revision

    async def disable_component_in(
        self, conn, component_id: str, route_epoch: int
    ) -> int:
        clock = await self._clock_in(conn)
        await (
            await conn.execute(
                "SELECT handler_id FROM public.bot_message_handler_heads WHERE bot_id=%s "
                "ORDER BY component_id,handler_id FOR UPDATE",
                (self.store.bot_id,),
            )
        ).fetchall()
        revision = clock["revision"] + 1
        await conn.execute(
            "INSERT INTO public.bot_message_handler_publications("
            "bot_id,registry_revision,component_id,generation_id,route_epoch,"
            "operation_kind,spec_set_digest) VALUES (%s,%s,%s,NULL,%s,'DISABLE',%s)",
            (self.store.bot_id, revision, component_id, route_epoch, digest_json([])),
        )
        await conn.execute(
            "UPDATE public.bot_message_handler_heads SET generation_id=NULL,route_epoch=%s,"
            "registry_revision=%s,active=false,updated_at=clock_timestamp() "
            "WHERE bot_id=%s AND component_id=%s AND active=true",
            (route_epoch, revision, self.store.bot_id, component_id),
        )
        await conn.execute(
            "UPDATE public.bot_message_handler_clocks SET revision=%s,"
            "updated_at=clock_timestamp() WHERE bot_id=%s",
            (revision, self.store.bot_id),
        )
        return revision

    async def snapshot(self) -> dict:
        async with self.store.pool.connection() as conn, conn.transaction():
            await self.store._identity(conn)
            clock = await (
                await conn.execute(
                    "SELECT * FROM public.bot_message_handler_clocks "
                    "WHERE bot_id=%s FOR SHARE",
                    (self.store.bot_id,),
                )
            ).fetchone()
            rows = await (
                await conn.execute(
                    "SELECT * FROM public.bot_message_handler_heads "
                    "WHERE bot_id=%s AND active=true "
                    "ORDER BY priority DESC,component_id,handler_id",
                    (self.store.bot_id,),
                )
            ).fetchall()
            return {"revision": clock["revision"] if clock else 0, "handlers": rows}

    async def retire(
        self,
        lease,
        *,
        component_id: str,
        handler_id: str,
        operation_id: str,
        expected_management_revision: int,
    ) -> dict:
        if (
            not all(
                isinstance(value, str) and 1 <= len(value) <= 256
                for value in (component_id, handler_id, operation_id)
            )
            or type(expected_management_revision) is not int
            or expected_management_revision < 0
        ):
            raise ForestError(
                "MESSAGE_HANDLER_RETIREMENT_INVALID",
                "Message handler retirement fields are invalid",
                422,
            )
        async with self.store.pool.connection() as conn, conn.transaction():
            clock = await self._clock_in(conn)
            runtime = await self.store._runtime(conn)
            await self.store._host(conn, lease, runtime)
            replay = await (
                await conn.execute(
                    "SELECT * FROM public.bot_message_handler_retirements "
                    "WHERE bot_id=%s AND operation_id=%s",
                    (self.store.bot_id, operation_id),
                )
            ).fetchone()
            if replay is not None:
                if any(
                    replay[key] != value
                    for key, value in (
                        ("component_id", component_id),
                        ("handler_id", handler_id),
                        ("expected_management_revision", expected_management_revision),
                    )
                ):
                    raise ForestError(
                        "IDEMPOTENCY_CONFLICT", "Handler retirement operation differs", 409
                    )
                return replay
            head = await (
                await conn.execute(
                    "SELECT * FROM public.bot_message_handler_heads WHERE bot_id=%s "
                    "AND component_id=%s AND handler_id=%s FOR UPDATE",
                    (self.store.bot_id, component_id, handler_id),
                )
            ).fetchone()
            if head is None:
                raise ForestError("MESSAGE_HANDLER_NOT_FOUND", "Handler does not exist", 404)
            if head["retired"]:
                raise ForestError(
                    "MESSAGE_HANDLER_ALREADY_RETIRED", "Handler is already retired", 409
                )
            if head["management_revision"] != expected_management_revision:
                raise ForestError(
                    "MESSAGE_HANDLER_RETIREMENT_CONFLICT", "Handler revision changed", 409
                )
            revision = clock["revision"] + 1
            resulting = expected_management_revision + 1
            row = await (
                await conn.execute(
                    "INSERT INTO public.bot_message_handler_retirements("
                    "bot_id,operation_id,component_id,handler_id,expected_management_revision,"
                    "resulting_management_revision,registry_revision,host_incarnation,"
                    "host_owner_epoch) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s) RETURNING *",
                    (
                        self.store.bot_id,
                        operation_id,
                        component_id,
                        handler_id,
                        expected_management_revision,
                        resulting,
                        revision,
                        lease.incarnation,
                        lease.owner_epoch,
                    ),
                )
            ).fetchone()
            await conn.execute(
                "UPDATE public.bot_message_handler_heads SET generation_id=NULL,active=false,"
                "retired=true,management_revision=%s,registry_revision=%s,"
                "retired_at=clock_timestamp(),updated_at=clock_timestamp() "
                "WHERE bot_id=%s AND component_id=%s AND handler_id=%s",
                (
                    resulting,
                    revision,
                    self.store.bot_id,
                    component_id,
                    handler_id,
                ),
            )
            await conn.execute(
                "UPDATE public.bot_message_handler_clocks SET revision=%s,"
                "updated_at=clock_timestamp() WHERE bot_id=%s",
                (revision, self.store.bot_id),
            )
            assert row is not None
            return row
