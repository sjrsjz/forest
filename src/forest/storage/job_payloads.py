"""Removable Job settings and delegated authority under frozen identities."""

from __future__ import annotations

import json
from collections.abc import Mapping

from forest.errors import ForestError
from forest.schedules import JobSettings, aware
from forest.storage.config_payloads import ConfigPayloadStore

SETTINGS_METADATA_KEYS = (
    "enabled",
    "kind",
    "conversation_id",
    "schedule",
    "misfire",
    "misfire_grace_seconds",
    "catch_up_limit",
    "max_overlap",
    "delegation_expires_at",
    "max_runs",
)
DELEGATION_METADATA_KEYS = (
    "issuer_id", "key_revision", "policy_revision", "delegation_epoch", "expires_at",
)


def _clone(value):
    try:
        return json.loads(json.dumps(value, ensure_ascii=False, allow_nan=False))
    except (TypeError, ValueError, OverflowError, RecursionError, UnicodeError) as exc:
        raise ForestError("JOB_DEFINITION_INVALID", "Job definition must be bounded JSON", 422) from exc


def normalize_settings(values):
    try:
        return JobSettings.model_validate(values).model_dump()
    except ValueError:
        raise ForestError(
            "JOB_INVALID", "Job schedule and delegation settings are invalid", 422
        ) from None


def settings_metadata(values):
    normalized = normalize_settings(values)
    return {key: _clone(normalized[key]) for key in SETTINGS_METADATA_KEYS}


def delegation_metadata(value):
    value = _clone(value)
    if not isinstance(value, dict):
        raise ForestError("JOB_DELEGATION_INVALID", "Job delegation is not an object", 422)
    for key in ("issuer_id", "expires_at"):
        if not isinstance(value.get(key), str) or not 1 <= len(value[key].encode()) <= 4096:
            raise ForestError("JOB_DELEGATION_INVALID", "Job delegation fields are invalid", 422)
    for key in ("key_revision", "policy_revision"):
        if type(value.get(key)) is not int or not 1 <= value[key] < 2**63:
            raise ForestError("JOB_DELEGATION_INVALID", "Job delegation fields are invalid", 422)
    epoch = value.get("delegation_epoch")
    if epoch is not None and (type(epoch) is not int or not 1 <= epoch < 2**63):
        raise ForestError("JOB_DELEGATION_INVALID", "Job delegation epoch is invalid", 422)
    try:
        expires_at = aware(value["expires_at"]).isoformat()
    except (TypeError, ValueError):
        raise ForestError("JOB_DELEGATION_INVALID", "Job delegation expiry is invalid", 422) from None
    return {
        "issuer_id": value["issuer_id"],
        "key_revision": value["key_revision"],
        "policy_revision": value["policy_revision"],
        "delegation_epoch": epoch,
        "expires_at": expires_at,
    }


class JobPayloadStore:
    def __init__(self, store):
        self.store = store
        self.content = ConfigPayloadStore(store)

    async def put_settings_in(self, conn, *, kind, job_id, identity, values, parents, epoch):
        normalized = normalize_settings(values)
        return await self.content.put_values_in(
            conn,
            family="job",
            kind=kind,
            resource=job_id,
            identity=identity,
            values=normalized,
            parents=parents,
            epoch=epoch,
        )

    async def put_delegation_in(
        self, conn, *, job_id, revision, delegation, parents, epoch,
    ):
        # Validate operational fields before freezing the complete authority.
        delegation_metadata(delegation)
        return await self.content.put_values_in(
            conn,
            family="job.delegation",
            kind="REVISION",
            resource=job_id,
            identity=str(revision),
            values=delegation,
            parents=parents,
            epoch=epoch,
        )

    async def hydrate_settings_in(self, conn, row, *, kind, job_id, identity, epoch, required=True):
        saved = await self.content.hydrate_values_in(
            conn,
            row,
            family="job",
            kind=kind,
            resource=job_id,
            identity=identity,
            epoch=epoch,
            required=required,
        )
        if saved["values_payload_state"] == "ACTIVE":
            normalized = normalize_settings(saved["values"])
            if normalized != saved["values"] or settings_metadata(normalized) != row.get("values_metadata"):
                raise ForestError(
                    "JOB_PAYLOAD_IDENTITY_CONFLICT", "Job settings differ from their metadata", 409
                )
            saved["values"] = normalized
        return saved

    async def hydrate_delegation_in(self, conn, row, *, job_id, revision, epoch, required=True):
        projected = {
            "bot_id": row.get("bot_id"),
            "values_payload_id": row.get("delegation_payload_id"),
            "values_payload_digest": row.get("delegation_payload_digest"),
        }
        saved = await self.content.hydrate_values_in(
            conn,
            projected,
            family="job.delegation",
            kind="REVISION",
            resource=job_id,
            identity=str(revision),
            epoch=epoch,
            required=required,
        )
        if saved["values_payload_state"] == "ACTIVE":
            metadata = delegation_metadata(saved["values"])
            if metadata != row.get("delegation_metadata"):
                raise ForestError(
                    "JOB_PAYLOAD_IDENTITY_CONFLICT", "Job delegation differs from its metadata", 409
                )
        return {
            "delegation_payload_id": saved["values_payload_id"],
            "delegation_payload_digest": saved["values_payload_digest"],
            "delegation_payload_state": saved["values_payload_state"],
            "delegation": saved["values"],
        }

    async def hydrate_plan_in(self, conn, row, *, epoch, required=True):
        if not isinstance(row, Mapping):
            raise ForestError("JOB_NOT_CONFIGURED", "Job plan is unavailable", 409)
        settings = await self.hydrate_settings_in(
            conn,
            row,
            kind="PLAN",
            job_id=row["job_id"],
            identity=row["plan_id"],
            epoch=epoch,
            required=required,
        )
        return {**row, **settings}

    async def hydrate_definition_in(self, conn, row, *, epoch, required=True):
        if not isinstance(row, Mapping):
            raise ForestError("JOB_NOT_CONFIGURED", "Job definition is unavailable", 409)
        settings = await self.hydrate_settings_in(
            conn,
            row,
            kind="REVISION",
            job_id=row["job_id"],
            identity=str(row["revision"]),
            epoch=epoch,
            required=required,
        )
        delegation = await self.hydrate_delegation_in(
            conn,
            row,
            job_id=row["job_id"],
            revision=row["revision"],
            epoch=epoch,
            required=required,
        )
        return {**row, **settings, **delegation}
