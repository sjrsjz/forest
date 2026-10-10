"""Read-only storage identity admission shared by every RuntimeGraph scope."""

from __future__ import annotations

from forest.errors import ForestError

_LEDGERS = {
    "bot": ("forest_kernel", "bot_id"),
    "deployment": ("forest_deployment", "deployment_id"),
    "host": ("forest_host", "host_id"),
}


async def require_installable_storage_in(
    conn, *, scope: str, scope_id: str, storages,
) -> None:
    """Reject a candidate whose existing SQL bundle cannot be installed exactly."""

    ledger = _LEDGERS.get(scope)
    if ledger is None:
        raise ValueError("Unknown RuntimeGraph scope")
    expected = {}
    for storage in storages:
        spec = storage.spec
        if spec.scope != scope:
            raise ForestError(
                "SCHEMA_BUNDLE_SCOPE_MISMATCH",
                "Selected component storage belongs to another scope", 409,
            )
        identity = {
            "component_id": spec.component_id,
            "schema_version": spec.schema_version,
            "scope": spec.scope,
            "namespace": spec.namespace,
            "baseline_sha256": spec.baseline_sha256,
            "package_digest": storage.package_digest,
            "repository_contract": spec.repository_contract,
            "execution_profile": storage.execution_profile,
            "state": "READY",
        }
        prior = expected.setdefault(spec.schema_id, identity)
        if prior != identity:
            raise ForestError(
                "SCHEMA_BUNDLE_IDENTITY_CONFLICT",
                "Selected components claim incompatible storage identities", 409,
            )
    if not expected:
        return
    schema, id_column = ledger
    rows = await (
        await conn.execute(
            "SELECT schema_id,component_id,schema_version,scope,namespace,"
            "baseline_sha256,package_digest,repository_contract,"
            f"execution_profile,state FROM {schema}.schema_bundles "
            f"WHERE {id_column}=%s AND schema_id=ANY(%s)",
            (scope_id, list(expected)),
        )
    ).fetchall()
    for row in rows:
        if any(str(row[key]) != value for key, value in expected[row["schema_id"]].items()):
            raise ForestError(
                "SCHEMA_BUNDLE_UPGRADE_REQUIRED",
                "Selected package storage differs from the installed schema",
                409,
                details={"schema_id": row["schema_id"]},
            )
