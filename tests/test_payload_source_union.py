"""Actual PostgreSQL parent flattening uses a bounded unique source union."""

import pytest
from test_bot_store import database as database
from test_bot_store import store as store
from test_payloads import payloads as payloads

from forest.errors import ForestError
from forest.memory_types import SourceRef
from forest.storage.kernel_payloads import KernelPayloadStore
from forest.storage.payloads import MAX_SOURCES, PayloadStore

pytestmark = pytest.mark.postgres


async def write(repo, lease, owner, *, sources=(), parents=(), epoch=0):
    return await repo.put_json(
        lease,
        owner_kind="kernel.result",
        owner_id=owner,
        value={"owner": owner},
        sources=sources,
        parents=parents,
        expected_deletion_epoch=epoch,
    )


async def test_overlapping_parent_manifests_merge_before_inherited_source_limit(payloads):
    repo, lease = payloads
    shared = tuple(SourceRef(f"shared-{index:04}") for index in range(600))
    parents = [await write(repo, lease, f"parent-{index}", sources=shared) for index in range(8)]
    assert len(shared) * len(parents) > MAX_SOURCES
    result = await write(
        repo, lease, "result", sources=shared[:100], parents=tuple(p.payload_id for p in parents)
    )
    # Parent order/repetition and direct/inherited overlap cannot change identity.
    repeated = await write(
        repo,
        lease,
        "result",
        sources=shared[:100],
        parents=tuple(p.payload_id for p in reversed(parents)) * 2,
    )
    assert result == repeated
    async with repo.store.pool.connection() as conn:
        header = await (
            await conn.execute(
                "SELECT * FROM public.bot_payload_headers WHERE payload_id=%s", (result.payload_id,)
            )
        ).fetchone()
        assert len(header["manifest"]["sources"]) == len(shared)
        assert tuple(SourceRef(**source) for source in header["manifest"]["sources"]) == shared
        assert len(header["manifest"]["parents"]) == 8
    await repo.forget_sources(lease, "forget-one-source", (shared[-1],), expected_deletion_epoch=0)
    await repo.scrub(lease)
    with pytest.raises(ForestError) as error:
        await repo.read_json(lease, result.payload_id, expected_deletion_epoch=1)
    assert error.value.code == "PAYLOAD_ERASED"
    erased = await write(
        repo,
        lease,
        "result",
        sources=shared[:100],
        parents=tuple(p.payload_id for p in parents),
        epoch=1,
    )
    assert erased.payload_id == result.payload_id and erased.digest == result.digest
    assert erased.state == "SCRUBBED"


async def test_true_unique_parent_union_over_4096_is_typed_and_atomic(payloads):
    repo, lease = payloads
    left = await write(
        repo, lease, "left", sources=tuple(SourceRef(f"source-{index:04}") for index in range(2048))
    )
    right = await write(
        repo,
        lease,
        "right",
        sources=tuple(SourceRef(f"source-{index:04}") for index in range(2048, 4097)),
    )
    for ref in (left, right):
        assert await repo.read_json(lease, ref.payload_id, expected_deletion_epoch=0)
    with pytest.raises(ForestError) as error:
        await write(repo, lease, "over-limit", parents=(left.payload_id, right.payload_id))
    assert error.value.code == "SOURCE_LINK_SCOPE_LIMIT"
    helper = KernelPayloadStore(repo.store)
    async with repo.store.pool.connection() as conn, conn.transaction():
        await repo.store._host(conn, lease)
        with pytest.raises(ForestError) as error:
            await helper.put_call_result_in(
                conn, "typed-over-limit", None, parents=(left.payload_id, right.payload_id), epoch=0
            )
        assert error.value.code == "KERNEL_CONTENT_REVOKED"
        assert not await (
            await conn.execute(
                "SELECT 1 FROM public.bot_payload_headers WHERE owner_id=ANY(%s)",
                (["over-limit", helper._owner("result", "typed-over-limit")],),
            )
        ).fetchone()


def test_direct_manifest_raw_length_limit_still_rejects_excessive_duplicates():
    # Untrusted direct input remains bounded before deduplication. Only the
    # framework's inherited parent flattening uses the unique source budget.
    with pytest.raises(ForestError) as error:
        PayloadStore._sources([SourceRef("duplicate")] * (MAX_SOURCES + 1))
    assert error.value.code == "INVALID_SOURCE_MANIFEST"
