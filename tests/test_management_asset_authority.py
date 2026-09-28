import hashlib
import io
import random

import pytest
from PIL import Image
from psycopg import sql

from forest.core_runtime import bootstrap_clean_kernel
from forest.errors import ForestError
from forest.storage.management_asset_authority import (
    CHUNK_BYTES,
    ManagementAssetAuthority,
)
from test_schema_bundles import clean_authority as clean_authority


@pytest.mark.postgres
async def test_management_asset_chunks_publish_one_bound_payload(clean_authority):
    store, installer = clean_authority
    kernel = await bootstrap_clean_kernel(store, installer)
    authority = ManagementAssetAuthority(kernel.payload)
    pixels = random.Random(7).randbytes(512 * 512 * 3)
    image = Image.frombytes("RGB", (512, 512), pixels)
    output = io.BytesIO()
    image.save(output, format="PNG")
    content = output.getvalue()
    assert len(content) > CHUNK_BYTES
    upload_id = "trial-image-1"
    target_ref, part_id = "query/asset-1", "image/0"
    owner_id = "trial:" + hashlib.sha256(
        (target_ref + "\0" + part_id).encode()
    ).hexdigest()
    digest = "sha256:" + hashlib.sha256(content).hexdigest()
    chunks = [content[offset:offset + CHUNK_BYTES]
              for offset in range(0, len(content), CHUNK_BYTES)]
    begin = dict(
        upload_id=upload_id, component_id="memory.graph", asset_id="trial.input",
        actor_subject_ref="admin-1", generation_id="generation-1",
        target_ref=target_ref, part_id=part_id, owner_kind="trial.input",
        owner_id=owner_id, media_type="image/png", byte_length=len(content),
        content_digest=digest, chunk_count=len(chunks),
        allowed_media_types=("text/plain", "image/png"), max_bytes=8 * 1024 * 1024,
    )
    async with store.pool.connection() as conn, conn.transaction():
        started = await authority.begin_in(conn, **begin)
        assert started["state"] == "OPEN"
        assert await authority.begin_in(conn, **begin) == started
        window = (await (await conn.execute(
            sql.SQL("SELECT admission_count,bytes_reserved FROM {} "
                    "WHERE bot_id=%s AND actor_subject_ref='admin-1'").format(
                kernel.payload._table("management_asset_admission_windows")
            ), (store.bot_id,),
        )).fetchone())
        assert (window["admission_count"], window["bytes_reserved"]) == (1, len(content))
        with pytest.raises(ForestError, match="Upload ID has another input"):
            await authority.begin_in(conn, **{**begin, "part_id": "image/other"})
        with pytest.raises(ForestError, match="Upload chunks are incomplete"):
            await authority.finalize_in(
                conn, upload_id=upload_id, component_id="memory.graph",
                asset_id="trial.input", actor_subject_ref="admin-1",
                generation_id="generation-1",
            )
        for index in reversed(range(len(chunks))):
            part = chunks[index]
            values = dict(
                upload_id=upload_id, component_id="memory.graph",
                asset_id="trial.input", actor_subject_ref="admin-1",
                generation_id="generation-1", chunk_index=index, content=part,
                content_digest="sha256:" + hashlib.sha256(part).hexdigest(),
            )
            await authority.put_chunk_in(conn, **values)
            assert await authority.put_chunk_in(conn, **values) == {
                "upload_id": upload_id, "chunk_index": index,
                "digest": values["content_digest"],
            }
        with pytest.raises(ForestError, match="another authority"):
            await authority.finalize_in(
                conn, upload_id=upload_id, component_id="memory.graph",
                asset_id="trial.input", actor_subject_ref="admin-2",
                generation_id="generation-1",
            )
        values = dict(
            upload_id=upload_id, component_id="memory.graph",
            asset_id="trial.input", actor_subject_ref="admin-1",
            generation_id="generation-1",
        )
        finished = await authority.finalize_in(conn, **values)
        assert finished["state"] == "FINALIZED"
        assert (finished["width"], finished["height"]) == (512, 512)
        assert finished["payload"]["owner_id"] == owner_id
        assert await authority.finalize_in(conn, **values) == finished
        assert await authority.verify_asset_in(
            conn, payload_id=finished["payload"]["payload_id"], digest=digest,
            component_id="memory.graph", asset_id="trial.input",
            actor_subject_ref="admin-1", generation_id="generation-1",
            target_ref=target_ref, part_id=part_id,
        ) == finished
        with pytest.raises(ForestError, match="not bound"):
            await authority.verify_asset_in(
                conn, payload_id=finished["payload"]["payload_id"], digest=digest,
                component_id="memory.graph", asset_id="trial.input",
                actor_subject_ref="admin-2", generation_id="generation-1",
                target_ref=target_ref, part_id=part_id,
            )
        assert await kernel.payload.read_bytes_in(
            conn, finished["payload"]["payload_id"]
        ) == content
        chunk_count = (await (await conn.execute(
            sql.SQL("SELECT count(*) AS count FROM {} WHERE bot_id=%s AND upload_id=%s").format(
                kernel.payload._table("management_asset_chunks")
            ), (store.bot_id, upload_id),
        )).fetchone())["count"]
        assert chunk_count == 0


@pytest.mark.postgres
async def test_management_asset_expiry_revokes_payload(clean_authority):
    store, installer = clean_authority
    kernel = await bootstrap_clean_kernel(store, installer)
    authority = ManagementAssetAuthority(kernel.payload)
    ref = await kernel.payload.put_bytes(
        owner_component_id="memory.graph", owner_kind="trial.input",
        owner_id="trial:expired-upload", media_type="text/plain", content=b"test",
        expected_deletion_epoch=await kernel.payload.epoch(),
    )
    namespace = kernel.payload._table("management_asset_uploads")
    async with store.pool.connection() as conn, conn.transaction():
        await conn.execute(
            sql.SQL(
                "INSERT INTO {}(bot_id,upload_id,input_digest,component_id,asset_id,"
                "actor_subject_ref,generation_id,target_ref,part_id,owner_kind,owner_id,"
                "media_type,byte_length,content_digest,chunk_count,opened_at,upload_expires_at,"
                "state,payload_id,finalized_at,payload_expires_at) VALUES "
                "(%s,'expired-upload',%s,'memory.graph','trial.input','admin-1',"
                "'generation-1','query/expired','text/0','trial.input',"
                "'trial:expired-upload','text/plain',4,%s,1,"
                "clock_timestamp()-interval '35 minutes',"
                "clock_timestamp()-interval '31 minutes','FINALIZED',%s,"
                "clock_timestamp()-interval '31 minutes',"
                "clock_timestamp()-interval '2 minutes')"
            ).format(namespace),
            (store.bot_id, "sha256:" + "a" * 64, ref.content_digest, ref.payload_id),
        )
    assert await authority.reap_expired() == {"reaped": 1}
    with pytest.raises(ForestError) as revoked:
        await kernel.payload.read_bytes(ref.payload_id)
    assert revoked.value.code == "PAYLOAD_REVOKED"
