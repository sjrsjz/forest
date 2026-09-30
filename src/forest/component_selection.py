"""Host-side read fence for the Worker Host's Bot-scoped selection snapshot."""

from __future__ import annotations

from contextlib import asynccontextmanager
from dataclasses import dataclass

from psycopg import sql
from psycopg.types.json import Jsonb

from forest.errors import ForestError
from forest.packages import digest_json
from forest.schema_bundles import module_schema_namespace


@dataclass(frozen=True)
class ComponentSelectionEntry:
    component_id: str
    source_kind: str
    package_digest: str | None
    manifest_digest: str | None
    approved_capabilities: tuple[str, ...]
    desired_enabled: bool
    desired_revision: int

    @classmethod
    def from_dict(cls, value: object) -> ComponentSelectionEntry:
        if (
            not isinstance(value, dict)
            or set(value) != {
                "component_id", "source_kind", "package_digest", "manifest_digest",
                "approved_capabilities", "desired_enabled", "desired_revision",
            }
            or not isinstance(value["component_id"], str)
            or not isinstance(value["source_kind"], str)
            or value["source_kind"] not in {"builtin", "installed"}
            or not isinstance(value["approved_capabilities"], list)
            or any(not isinstance(item, str) for item in value["approved_capabilities"])
            or type(value["desired_enabled"]) is not bool
            or type(value["desired_revision"]) is not int
            or value["desired_revision"] < 1
        ):
            raise ForestError(
                "COMPONENT_SELECTION_SNAPSHOT_INVALID",
                "Worker Host selection entry is invalid",
                503,
            )
        if value["source_kind"] == "builtin":
            valid_source = (
                value["package_digest"] is None
                and value["manifest_digest"] is None
                and not value["approved_capabilities"]
            )
        else:
            valid_source = (
                isinstance(value["package_digest"], str)
                and isinstance(value["manifest_digest"], str)
            )
        if not valid_source:
            raise ForestError(
                "COMPONENT_SELECTION_SNAPSHOT_INVALID",
                "Worker Host selection source is invalid",
                503,
            )
        return cls(
            value["component_id"], value["source_kind"], value["package_digest"],
            value["manifest_digest"], tuple(value["approved_capabilities"]),
            value["desired_enabled"], value["desired_revision"],
        )

    def as_dict(self) -> dict:
        return {
            "component_id": self.component_id,
            "source_kind": self.source_kind,
            "package_digest": self.package_digest,
            "manifest_digest": self.manifest_digest,
            "approved_capabilities": list(self.approved_capabilities),
            "desired_enabled": self.desired_enabled,
            "desired_revision": self.desired_revision,
        }


@dataclass(frozen=True)
class ComponentSelectionSnapshot:
    revision: int
    entries: tuple[ComponentSelectionEntry, ...]
    digest: str


class ComponentSelectionReader:
    """Read the package-owned projection without a Worker process at cold start."""

    def __init__(self, store) -> None:
        self.store = store
        self.namespace = module_schema_namespace("forest.component.worker-host")

    @asynccontextmanager
    async def publication_fence(
        self, *, expected_revision: int | None = None, expected_digest: str | None = None
    ):
        """Hold a shared Bot selection lock through Graph publication."""
        async with self.store.pool.connection() as conn, conn.transaction():
            await conn.execute(
                "SELECT pg_advisory_xact_lock_shared(hashtextextended(%s,0))",
                ("forest.component.selection/" + self.store.bot_id,),
            )
            result = await conn.execute(
                sql.SQL("SELECT {}(%s::jsonb,%s::jsonb) AS result").format(
                    sql.Identifier(self.namespace, "component_selection_snapshot")
                ),
                (Jsonb({"bot_id": self.store.bot_id}), Jsonb({})),
            )
            envelope = (await result.fetchone())["result"]
            if (
                not isinstance(envelope, dict)
                or envelope.get("protocol") != "forest.domain-result/1"
                or envelope.get("ok") is not True
            ):
                raise ForestError(
                    "COMPONENT_SELECTION_SNAPSHOT_INVALID",
                    "Worker Host selection snapshot is invalid",
                    503,
                )
            value = envelope.get("value")
            if (
                not isinstance(value, dict)
                or set(value) != {"protocol", "revision", "entries"}
                or value.get("protocol") != "forest.component-selection-snapshot/1"
                or type(value.get("revision")) is not int
                or value["revision"] < 0
                or not isinstance(value.get("entries"), list)
                or len(value["entries"]) > 128
            ):
                raise ForestError(
                    "COMPONENT_SELECTION_SNAPSHOT_INVALID",
                    "Worker Host selection snapshot is invalid",
                    503,
                )
            entries = tuple(ComponentSelectionEntry.from_dict(item) for item in value["entries"])
            if tuple(item.component_id for item in entries) != tuple(
                sorted({item.component_id for item in entries})
            ):
                raise ForestError(
                    "COMPONENT_SELECTION_SNAPSHOT_INVALID",
                    "Worker Host selection order is invalid",
                    503,
                )
            frozen = {
                "protocol": "forest.component-selection-snapshot/1",
                "revision": value["revision"],
                "entries": [item.as_dict() for item in entries],
            }
            snapshot = ComponentSelectionSnapshot(
                value["revision"], entries, digest_json(frozen)
            )
            if (
                expected_revision is not None and snapshot.revision != expected_revision
            ) or (expected_digest is not None and snapshot.digest != expected_digest):
                raise ForestError(
                    "COMPONENT_SELECTION_CHANGED",
                    "Worker Host selection changed before Graph publication",
                    409,
                )
            yield snapshot

    async def snapshot(self) -> ComponentSelectionSnapshot:
        async with self.publication_fence() as snapshot:
            return snapshot
