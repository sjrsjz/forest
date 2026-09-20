"""Immutable per-component bootstrap manifests for one Bot provisioning operation."""

from __future__ import annotations

import json
import re
from dataclasses import dataclass

from forest.deployment_action_authorization import authorize_deployment_action_in
from forest.errors import ForestError
from forest.packages import digest_json

_IDENTIFIER = re.compile(r"[a-z0-9][a-z0-9._-]{0,127}\Z")
_PROFILE_REF = re.compile(r"bootstrap-profile/[A-Za-z0-9][A-Za-z0-9._/-]{0,493}\Z")
_PLAINTEXT_SECRET_FIELDS = {
    "access_token",
    "api_key",
    "credential",
    "dsn",
    "password",
    "secret",
    "token",
}


@dataclass(frozen=True)
class BootstrapComponent:
    component_id: str
    schema_digest: str
    config_digest: str
    config: dict


@dataclass(frozen=True)
class BootstrapProfile:
    profile_ref: str
    bot_id: str
    storage_profile_ref: str
    storage_generation: int
    manifest_digest: str
    components: tuple[BootstrapComponent, ...]


class DeploymentBootstrapProfileAuthority:
    """Freeze non-secret module seeds before a durable provisioning Job is admitted."""

    def __init__(self, store, *, secrets) -> None:
        self.store = store
        self.secrets = secrets

    async def create(
        self,
        *,
        operation_id: str,
        profile_ref: str,
        bot_id: str,
        storage_profile_ref: str,
        storage_generation: int,
        components: dict[str, dict],
        schema_digests: dict[str, str],
        authorization=None,
    ) -> BootstrapProfile:
        self._text(operation_id, "operation_id", 256)
        self._text(bot_id, "bot_id", 128)
        self._text(storage_profile_ref, "storage_profile_ref", 512)
        if not isinstance(profile_ref, str) or _PROFILE_REF.fullmatch(profile_ref) is None:
            raise ForestError("BOOTSTRAP_PROFILE_INVALID", "Bootstrap profile reference is invalid", 422)
        if type(storage_generation) is not int or storage_generation < 1:
            raise ForestError("BOOTSTRAP_PROFILE_INVALID", "Storage generation is invalid", 422)
        if (
            not isinstance(components, dict)
            or not 1 <= len(components) <= 64
            or set(components) != set(schema_digests)
        ):
            raise ForestError("BOOTSTRAP_PROFILE_INVALID", "Component manifest is invalid", 422)
        facts = []
        secret_refs = set()
        for component_id in sorted(components):
            config = components[component_id]
            schema_digest = schema_digests[component_id]
            if (
                _IDENTIFIER.fullmatch(component_id) is None
                or not isinstance(config, dict)
                or not isinstance(schema_digest, str)
                or not re.fullmatch(r"sha256:[a-f0-9]{64}", schema_digest)
            ):
                raise ForestError("BOOTSTRAP_PROFILE_INVALID", "Component seed is invalid", 422)
            self._inspect(config, secret_refs=secret_refs)
            try:
                encoded = json.dumps(
                    config, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False
                ).encode("utf-8")
            except (TypeError, ValueError, UnicodeError) as exc:
                raise ForestError(
                    "BOOTSTRAP_PROFILE_INVALID", "Component seed is not canonical JSON", 422
                ) from exc
            if len(encoded) > 262_144:
                raise ForestError("BOOTSTRAP_PROFILE_INVALID", "Component seed is too large", 413)
            facts.append(
                {
                    "component_id": component_id,
                    "schema_digest": schema_digest,
                    "config_digest": digest_json(config),
                    "config": config,
                }
            )
        for secret_ref in sorted(secret_refs):
            await self.secrets.metadata(secret_ref)
        manifest_digest = digest_json(
            {
                "bot_id": bot_id,
                "storage_profile_ref": storage_profile_ref,
                "storage_generation": storage_generation,
                "components": facts,
            }
        )
        input_digest = digest_json([profile_ref, manifest_digest])
        async with self.store.pool.connection() as conn, conn.transaction():
            await authorize_deployment_action_in(
                conn, authorization, operation_id=operation_id
            )
            await conn.execute(
                "SELECT pg_advisory_xact_lock(hashtextextended(%s,0))",
                (f"forest.deployment.bootstrap-profile/{profile_ref}",),
            )
            replay = await (
                await conn.execute(
                    "SELECT p.*,o.input_digest FROM forest_deployment.bootstrap_profile_operations o "
                    "JOIN forest_deployment.bootstrap_profiles p USING(profile_ref) "
                    "WHERE o.operation_id=%s",
                    (operation_id,),
                )
            ).fetchone()
            if replay is not None:
                if replay["input_digest"] != input_digest:
                    raise ForestError(
                        "BOOTSTRAP_PROFILE_OPERATION_CONFLICT",
                        "Bootstrap profile operation differs",
                        409,
                    )
                return await self._load_in(conn, replay["profile_ref"])
            exists = await (
                await conn.execute(
                    "SELECT 1 FROM forest_deployment.bootstrap_profiles WHERE profile_ref=%s",
                    (profile_ref,),
                )
            ).fetchone()
            if exists is not None:
                raise ForestError(
                    "BOOTSTRAP_PROFILE_EXISTS", "Bootstrap profile reference cannot be reused", 409
                )
            await conn.execute(
                "INSERT INTO forest_deployment.bootstrap_profiles(profile_ref,bot_id,"
                "storage_profile_ref,storage_generation,manifest_digest) VALUES (%s,%s,%s,%s,%s)",
                (profile_ref, bot_id, storage_profile_ref, storage_generation, manifest_digest),
            )
            for fact in facts:
                await conn.execute(
                    "INSERT INTO forest_deployment.bootstrap_profile_components("
                    "profile_ref,component_id,schema_digest,config_digest,config) "
                    "VALUES (%s,%s,%s,%s,%s::jsonb)",
                    (
                        profile_ref,
                        fact["component_id"],
                        fact["schema_digest"],
                        fact["config_digest"],
                        json.dumps(fact["config"], ensure_ascii=False),
                    ),
                )
            await conn.execute(
                "INSERT INTO forest_deployment.bootstrap_profile_operations("
                "operation_id,input_digest,profile_ref) VALUES (%s,%s,%s)",
                (operation_id, input_digest, profile_ref),
            )
            return await self._load_in(conn, profile_ref)

    async def load(self, profile_ref: str) -> BootstrapProfile:
        async with self.store.pool.connection() as conn:
            return await self._load_in(conn, profile_ref)

    async def _load_in(self, conn, profile_ref: str) -> BootstrapProfile:
        if not isinstance(profile_ref, str) or _PROFILE_REF.fullmatch(profile_ref) is None:
            raise ForestError("BOOTSTRAP_PROFILE_INVALID", "Bootstrap profile reference is invalid", 422)
        profile = await (
            await conn.execute(
                "SELECT * FROM forest_deployment.bootstrap_profiles WHERE profile_ref=%s",
                (profile_ref,),
            )
        ).fetchone()
        if profile is None:
            raise ForestError("BOOTSTRAP_PROFILE_NOT_FOUND", "Bootstrap profile does not exist", 404)
        rows = await (
            await conn.execute(
                "SELECT * FROM forest_deployment.bootstrap_profile_components "
                "WHERE profile_ref=%s ORDER BY component_id",
                (profile_ref,),
            )
        ).fetchall()
        components = tuple(
            BootstrapComponent(
                row["component_id"], row["schema_digest"], row["config_digest"], row["config"]
            )
            for row in rows
        )
        fact = BootstrapProfile(
            profile["profile_ref"],
            profile["bot_id"],
            profile["storage_profile_ref"],
            profile["storage_generation"],
            profile["manifest_digest"],
            components,
        )
        expected = digest_json(
            {
                "bot_id": fact.bot_id,
                "storage_profile_ref": fact.storage_profile_ref,
                "storage_generation": fact.storage_generation,
                "components": [
                    {
                        "component_id": item.component_id,
                        "schema_digest": item.schema_digest,
                        "config_digest": item.config_digest,
                        "config": item.config,
                    }
                    for item in components
                ],
            }
        )
        if expected != fact.manifest_digest:
            raise ForestError("BOOTSTRAP_PROFILE_TAMPERED", "Bootstrap manifest digest differs", 409)
        return fact

    @classmethod
    def _inspect(cls, value, *, secret_refs: set[str], path: tuple[str, ...] = ()) -> None:
        if isinstance(value, dict):
            for key, nested in value.items():
                if not isinstance(key, str):
                    raise ForestError("BOOTSTRAP_PROFILE_INVALID", "Configuration key is invalid", 422)
                lowered = key.lower()
                sensitive = lowered in _PLAINTEXT_SECRET_FIELDS or any(
                    lowered.endswith("_" + name) for name in _PLAINTEXT_SECRET_FIELDS
                )
                if sensitive and not lowered.endswith(("_ref", "_digest")):
                    raise ForestError(
                        "BOOTSTRAP_SECRET_PLAINTEXT_FORBIDDEN",
                        "Bootstrap profile contains a plaintext secret field",
                        422,
                        details={"path": ".".join((*path, key))},
                    )
                if lowered.endswith("_ref") and isinstance(nested, str) and nested.startswith("secret://"):
                    secret_refs.add(nested)
                cls._inspect(nested, secret_refs=secret_refs, path=(*path, key))
        elif isinstance(value, list):
            for index, nested in enumerate(value):
                cls._inspect(nested, secret_refs=secret_refs, path=(*path, str(index)))
        elif (
            isinstance(value, str)
            and value.startswith("secret://")
            and (not path or not path[-1].lower().endswith("_ref"))
        ):
            raise ForestError(
                "BOOTSTRAP_SECRET_REFERENCE_INVALID",
                "Secret reference is stored outside a reference field",
                422,
                details={"path": ".".join(path)},
            )

    @staticmethod
    def _text(value: object, field_name: str, limit: int) -> str:
        try:
            size = len(value.encode("utf-8")) if isinstance(value, str) else 0
        except UnicodeError:
            size = 0
        if not 1 <= size <= limit or any(ord(char) < 32 or ord(char) == 127 for char in value):
            raise ForestError("BOOTSTRAP_PROFILE_INVALID", f"Invalid {field_name}", 422)
        return value
