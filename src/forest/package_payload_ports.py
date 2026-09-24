"""Capability-scoped payload port for isolated package Workers."""

from __future__ import annotations

from forest.errors import ForestError
from forest.package_capabilities import PackageCallScope


class _ExactPayloadRef:
    __slots__ = (
        "payload_id",
        "digest",
        "owner_component_id",
        "owner_kind",
        "owner_id",
    )

    def __init__(self, value: dict) -> None:
        expected = {
            "payload_id",
            "digest",
            "owner_component_id",
            "owner_kind",
            "owner_id",
        }
        if not isinstance(value, dict) or set(value) != expected:
            raise ForestError(
                "PAYLOAD_REFERENCE_INVALID",
                "Payload access requires one complete exact reference",
                422,
            )
        for field in expected - {"digest"}:
            item = value[field]
            limit = 512 if field == "owner_id" else 256
            if not isinstance(item, str) or not 1 <= len(item.encode("utf-8")) <= limit:
                raise ForestError(
                    "PAYLOAD_REFERENCE_INVALID",
                    "Payload reference identity is invalid",
                    422,
                )
            setattr(self, field, item)
        digest = value["digest"]
        if (
            not isinstance(digest, str)
            or len(digest) != 71
            or not digest.startswith("sha256:")
            or any(char not in "0123456789abcdef" for char in digest[7:])
        ):
            raise ForestError(
                "PAYLOAD_REFERENCE_INVALID",
                "Payload reference digest is invalid",
                422,
            )
        self.digest = digest


class KernelPackagePayloadAuthority:
    """Expose payload bytes without exposing a repository or database locator."""

    def __init__(self, payloads) -> None:
        self._payloads = payloads
        self._bot_id = payloads.store.bot_id

    def _verify_scope(self, scope: PackageCallScope) -> None:
        if scope.bot_id != self._bot_id:
            raise ForestError(
                "PACKAGE_CAPABILITY_SCOPE_MISMATCH",
                "Package capability request belongs to another Bot",
                403,
            )

    async def read_chunk(
        self,
        scope: PackageCallScope,
        *,
        reference: dict,
        offset: int,
        max_bytes: int,
    ) -> dict:
        self._verify_scope(scope)
        exact = _ExactPayloadRef(reference)
        content, total = await self._payloads.read_verified_chunk(
            exact,
            offset=offset,
            max_bytes=max_bytes,
        )
        return {
            "content": content,
            "offset": offset,
            "byte_length": total,
            "eof": offset + len(content) == total,
        }

    async def commit_bytes(
        self,
        scope: PackageCallScope,
        *,
        owner_kind: str,
        owner_id: str,
        media_type: str,
        content: bytes,
        parents: tuple[dict, ...],
    ) -> dict:
        self._verify_scope(scope)
        exact_parents = tuple(_ExactPayloadRef(item) for item in parents)
        async with self._payloads.store.pool.connection() as conn, conn.transaction():
            epoch = await self._payloads.epoch_in(conn)
            for parent in exact_parents:
                await self._payloads.verify_in(
                    conn,
                    payload_id=parent.payload_id,
                    digest=parent.digest,
                    owner_component_id=parent.owner_component_id,
                    owner_kind=parent.owner_kind,
                    owner_id=parent.owner_id,
                )
            result = await self._payloads.put_bytes_in(
                conn,
                owner_component_id=scope.component_id,
                owner_kind=owner_kind,
                owner_id=owner_id,
                media_type=media_type,
                content=content,
                parents=tuple(item.payload_id for item in exact_parents),
                expected_deletion_epoch=epoch,
            )
        return {
            "payload_id": result.payload_id,
            "digest": result.content_digest,
            "owner_component_id": scope.component_id,
            "owner_kind": owner_kind,
            "owner_id": owner_id,
        }
