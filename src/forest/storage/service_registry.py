"""Durable service contracts and dependency requirements for the RouteGraph."""

from __future__ import annotations

import json
import re

from forest.errors import ForestError
from forest.packages import contract_matches, contract_range, digest_json

_NAME = re.compile(r"[a-zA-Z][a-zA-Z0-9_.-]{0,127}\Z")


class ServiceRegistryStore:
    """Publish service heads in the same transaction as a component route."""

    def __init__(self, store):
        self.store = store

    @staticmethod
    def _normalize_services(definitions) -> list[dict]:
        try:
            copied = json.loads(json.dumps(definitions or [], allow_nan=False))
        except (TypeError, ValueError, UnicodeError):
            raise ForestError("SERVICE_DECLARATION_INVALID", "Service declarations must be finite JSON", 422) from None
        if not isinstance(copied, list) or len(copied) > 64:
            raise ForestError("SERVICE_DECLARATION_INVALID", "At most 64 services are allowed", 422)
        result, keys = [], set()
        for item in copied:
            if not isinstance(item, dict) or set(item) != {
                "name", "contract", "input_schema_digest", "output_schema_digest"
            }:
                raise ForestError("SERVICE_DECLARATION_INVALID", "Service declaration fields differ", 422)
            try:
                lower, upper = contract_range(item["contract"])
            except ForestError as exc:
                raise ForestError("SERVICE_DECLARATION_INVALID", str(exc), 422) from None
            if upper is not None or not _NAME.fullmatch(item["name"]):
                raise ForestError("SERVICE_DECLARATION_INVALID", "Provided services require an exact contract", 422)
            for field in ("input_schema_digest", "output_schema_digest"):
                value = item[field]
                if not isinstance(value, str) or not re.fullmatch(r"sha256:[0-9a-f]{64}", value):
                    raise ForestError("SERVICE_DECLARATION_INVALID", "Service schema digest is invalid", 422)
            key = item["name"], lower[0]
            if key in keys:
                raise ForestError("SERVICE_DECLARATION_INVALID", "Service name and major must be unique", 422)
            keys.add(key)
            result.append({
                **item,
                "contract_major": lower[0],
                "contract_minor": lower[1],
            })
        return sorted(result, key=lambda item: (item["name"], item["contract_major"]))

    @staticmethod
    def _normalize_requirements(definitions) -> list[dict]:
        try:
            copied = json.loads(json.dumps(definitions or [], allow_nan=False))
        except (TypeError, ValueError, UnicodeError):
            raise ForestError("DEPENDENCY_DECLARATION_INVALID", "Dependency declarations must be finite JSON", 422) from None
        if not isinstance(copied, list) or len(copied) > 64:
            raise ForestError("DEPENDENCY_DECLARATION_INVALID", "At most 64 dependencies are allowed", 422)
        result, names = [], set()
        for item in copied:
            if not isinstance(item, dict) or set(item) != {
                "name", "contract", "required", "startup_dependency"
            }:
                raise ForestError("DEPENDENCY_DECLARATION_INVALID", "Dependency declaration fields differ", 422)
            try:
                contract_range(item["contract"])
            except ForestError as exc:
                raise ForestError("DEPENDENCY_DECLARATION_INVALID", str(exc), 422) from None
            if (
                not _NAME.fullmatch(item["name"])
                or type(item["required"]) is not bool
                or type(item["startup_dependency"]) is not bool
                or item["name"] in names
            ):
                raise ForestError("DEPENDENCY_DECLARATION_INVALID", "Dependency identity is invalid", 422)
            names.add(item["name"])
            result.append(dict(item))
        return sorted(result, key=lambda item: item["name"])

    async def _clock_in(self, conn) -> dict:
        await conn.execute(
            "INSERT INTO public.bot_service_clock(bot_id) VALUES (%s) ON CONFLICT DO NOTHING",
            (self.store.bot_id,),
        )
        return await (
            await conn.execute(
                "SELECT * FROM public.bot_service_clock WHERE bot_id=%s FOR UPDATE",
                (self.store.bot_id,),
            )
        ).fetchone()

    async def register_generation_in(
        self,
        conn,
        generation_id: str,
        component_id: str,
        services: list[dict],
        requirements: list[dict],
        *,
        existing_generation: bool = False,
    ) -> None:
        services = self._normalize_services(services)
        requirements = self._normalize_requirements(requirements)
        set_digest = digest_json({"services": services, "requirements": requirements})
        current = await (
            await conn.execute(
                "SELECT * FROM public.bot_service_generations WHERE bot_id=%s AND generation_id=%s",
                (self.store.bot_id, generation_id),
            )
        ).fetchone()
        if current is not None:
            if any(current[key] != value for key, value in (
                ("component_id", component_id),
                ("spec_set_digest", set_digest),
                ("service_count", len(services)),
                ("requirement_count", len(requirements)),
            )):
                raise ForestError("GENERATION_CONFLICT", "Generation service declarations cannot be reused", 409)
            return
        if existing_generation:
            raise ForestError("GENERATION_CONFLICT", "Generation service declarations cannot be added later", 409)
        await conn.execute(
            "INSERT INTO public.bot_service_generations("
            "bot_id,generation_id,component_id,spec_set_digest,service_count,requirement_count) "
            "VALUES (%s,%s,%s,%s,%s,%s)",
            (self.store.bot_id, generation_id, component_id, set_digest, len(services), len(requirements)),
        )
        for item in services:
            await conn.execute(
                "INSERT INTO public.bot_generation_service_definitions("
                "bot_id,generation_id,component_id,service_name,contract_major,contract_minor,"
                "input_schema_digest,output_schema_digest) VALUES (%s,%s,%s,%s,%s,%s,%s,%s)",
                (
                    self.store.bot_id, generation_id, component_id, item["name"],
                    item["contract_major"], item["contract_minor"],
                    item["input_schema_digest"], item["output_schema_digest"],
                ),
            )
        for item in requirements:
            await conn.execute(
                "INSERT INTO public.bot_generation_service_requirements("
                "bot_id,generation_id,component_id,service_name,contract_range,required,startup_dependency) "
                "VALUES (%s,%s,%s,%s,%s,%s,%s)",
                (
                    self.store.bot_id, generation_id, component_id, item["name"],
                    item["contract"], item["required"], item["startup_dependency"],
                ),
            )

    async def publish_generation_in(
        self, conn, generation_id: str, component_id: str, route_epoch: int
    ) -> int:
        clock = await self._clock_in(conn)
        declaration_set = await (
            await conn.execute(
                "SELECT * FROM public.bot_service_generations WHERE bot_id=%s "
                "AND generation_id=%s AND component_id=%s",
                (self.store.bot_id, generation_id, component_id),
            )
        ).fetchone()
        if declaration_set is None:
            raise ForestError("SERVICE_GENERATION_MISSING", "Candidate has no immutable service set", 409)
        definitions = await (
            await conn.execute(
                "SELECT * FROM public.bot_generation_service_definitions WHERE bot_id=%s "
                "AND generation_id=%s ORDER BY service_name,contract_major",
                (self.store.bot_id, generation_id),
            )
        ).fetchall()
        heads = await (
            await conn.execute(
                "SELECT * FROM public.bot_service_heads WHERE bot_id=%s "
                "ORDER BY service_name,contract_major FOR UPDATE",
                (self.store.bot_id,),
            )
        ).fetchall()
        by_key = {(row["service_name"], row["contract_major"]): row for row in heads}
        for definition in definitions:
            current = by_key.get((definition["service_name"], definition["contract_major"]))
            if current is not None and current["active"] and current["component_id"] != component_id:
                raise ForestError("DUPLICATE_SERVICE", "Another active component provides this service major", 409)
        revision = clock["revision"] + 1
        await conn.execute(
            "UPDATE public.bot_service_heads SET generation_id=NULL,route_epoch=%s,"
            "registry_revision=%s,active=false,updated_at=clock_timestamp() "
            "WHERE bot_id=%s AND component_id=%s AND active=true "
            "AND (service_name,contract_major) NOT IN ("
            "SELECT service_name,contract_major FROM public.bot_generation_service_definitions "
            "WHERE bot_id=%s AND generation_id=%s)",
            (route_epoch, revision, self.store.bot_id, component_id, self.store.bot_id, generation_id),
        )
        for definition in definitions:
            await conn.execute(
                "INSERT INTO public.bot_service_heads("
                "bot_id,service_name,contract_major,component_id,generation_id,route_epoch,"
                "registry_revision,contract_minor,input_schema_digest,output_schema_digest,active) "
                "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,true) "
                "ON CONFLICT(bot_id,service_name,contract_major) DO UPDATE SET "
                "component_id=EXCLUDED.component_id,generation_id=EXCLUDED.generation_id,"
                "route_epoch=EXCLUDED.route_epoch,registry_revision=EXCLUDED.registry_revision,"
                "contract_minor=EXCLUDED.contract_minor,input_schema_digest=EXCLUDED.input_schema_digest,"
                "output_schema_digest=EXCLUDED.output_schema_digest,active=true,updated_at=clock_timestamp()",
                (
                    self.store.bot_id, definition["service_name"], definition["contract_major"],
                    component_id, generation_id, route_epoch, revision,
                    definition["contract_minor"], definition["input_schema_digest"],
                    definition["output_schema_digest"],
                ),
            )
        await conn.execute(
            "INSERT INTO public.bot_service_publications("
            "bot_id,registry_revision,component_id,generation_id,route_epoch,operation_kind,spec_set_digest) "
            "VALUES (%s,%s,%s,%s,%s,'INSTALL',%s)",
            (
                self.store.bot_id, revision, component_id, generation_id, route_epoch,
                declaration_set["spec_set_digest"],
            ),
        )
        await conn.execute(
            "UPDATE public.bot_service_clock SET revision=%s,updated_at=clock_timestamp() WHERE bot_id=%s",
            (revision, self.store.bot_id),
        )
        return revision

    async def disable_component_in(self, conn, component_id: str, route_epoch: int) -> int:
        clock = await self._clock_in(conn)
        await (
            await conn.execute(
                "SELECT service_name FROM public.bot_service_heads WHERE bot_id=%s "
                "ORDER BY service_name,contract_major FOR UPDATE",
                (self.store.bot_id,),
            )
        ).fetchall()
        revision = clock["revision"] + 1
        await conn.execute(
            "UPDATE public.bot_service_heads SET generation_id=NULL,route_epoch=%s,"
            "registry_revision=%s,active=false,updated_at=clock_timestamp() "
            "WHERE bot_id=%s AND component_id=%s AND active=true",
            (route_epoch, revision, self.store.bot_id, component_id),
        )
        await conn.execute(
            "INSERT INTO public.bot_service_publications("
            "bot_id,registry_revision,component_id,generation_id,route_epoch,operation_kind,spec_set_digest) "
            "VALUES (%s,%s,%s,NULL,%s,'DISABLE',%s)",
            (self.store.bot_id, revision, component_id, route_epoch, digest_json({"services": [], "requirements": []})),
        )
        await conn.execute(
            "UPDATE public.bot_service_clock SET revision=%s,updated_at=clock_timestamp() WHERE bot_id=%s",
            (revision, self.store.bot_id),
        )
        return revision

    async def resolve(self, service_name: str, contract_major: int, *, caller_generation_id=None) -> dict:
        if not _NAME.fullmatch(service_name) or type(contract_major) is not int or contract_major < 1:
            raise ForestError("INVALID_SERVICE_REQUEST", "Service identity is invalid", 422)
        async with self.store.pool.connection() as conn, conn.transaction():
            await self.store._identity(conn)
            if caller_generation_id is not None:
                requirement = await (
                    await conn.execute(
                        "SELECT * FROM public.bot_generation_service_requirements WHERE bot_id=%s "
                        "AND generation_id=%s AND service_name=%s",
                        (self.store.bot_id, caller_generation_id, service_name),
                    )
                ).fetchone()
                if requirement is None:
                    raise ForestError("DEPENDENCY_UNAVAILABLE", "Service is not declared by the caller", 409)
            else:
                requirement = None
            row = await (
                await conn.execute(
                    "SELECT h.*,r.admission,g.status AS generation_status "
                    "FROM public.bot_service_heads h "
                    "JOIN public.bot_component_routes r ON r.bot_id=h.bot_id "
                    "AND r.component_id=h.component_id AND r.generation_id=h.generation_id "
                    "JOIN public.bot_generations g ON g.bot_id=h.bot_id AND g.generation_id=h.generation_id "
                    "WHERE h.bot_id=%s AND h.service_name=%s AND h.contract_major=%s AND h.active=true",
                    (self.store.bot_id, service_name, contract_major),
                )
            ).fetchone()
            if row is None:
                raise ForestError("DEPENDENCY_UNAVAILABLE", "Service route is not published", 409)
            exact = f'{row["contract_major"]}.{row["contract_minor"]}'
            if requirement is not None and not contract_matches(requirement["contract_range"], exact):
                raise ForestError("CONTRACT_INCOMPATIBLE", "Active service does not match the caller contract", 422)
            return row
