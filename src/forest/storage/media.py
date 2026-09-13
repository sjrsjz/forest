"""Bot-bound immutable image acquisition; no network URI enters model context.

Pillow validation follows https://pillow.readthedocs.io/en/stable/reference/Image.html:
identify and verify, reopen and decode every bounded frame, retain original bytes.
"""

from __future__ import annotations

import asyncio
import hashlib
import io
import threading
import warnings
from urllib.parse import urlsplit
from uuid import uuid4

import httpx
from PIL import Image

from forest.errors import ForestError
from forest.models import MediaSnapshot
from forest.packages import digest_json
from forest.storage.input_payloads import InputContentStore

MAX_MEDIA_BYTES = 8 * 1024 * 1024
MAX_MEDIA_PIXELS = 16777216
MAX_MEDIA_FRAMES = 100
MAX_CONTEXT_MEDIA = 32
MAX_CONTEXT_MEDIA_BYTES = 32 * 1024 * 1024
MAX_CONTEXT_CAPTURE_SECONDS = 15
MAX_BOT_MEDIA_BYTES = 512 * 1024 * 1024
_IMAGE_LOCK = threading.Lock()
_FORMATS = {"PNG": "image/png", "JPEG": "image/jpeg", "WEBP": "image/webp", "GIF": "image/gif"}


def _failure(code):
    return ForestError(code, "Managed image acquisition is unavailable", 422)


def _url(value, *, origin_only=False):
    """Strict origin equality, never prefix matching or inherited URL credentials."""
    try:
        if (
            not isinstance(value, str)
            or not 1 <= len(value) <= 4096
            or "\\" in value
            or any(ord(c) < 33 or ord(c) == 127 for c in value)
        ):
            raise ValueError()
        parsed = urlsplit(value)
        if (
            parsed.scheme not in {"http", "https"}
            or not parsed.hostname
            or parsed.username is not None
            or parsed.password is not None
            or parsed.fragment
            or "%" in parsed.hostname
            or (origin_only and (parsed.path not in {"", "/"} or parsed.query))
        ):
            raise ValueError()
        port = parsed.port or (443 if parsed.scheme == "https" else 80)
        if not 1 <= port <= 65535:
            raise ValueError()
        hostname = parsed.hostname.encode("idna").decode("ascii").lower()
        if ":" in hostname:
            hostname = "[" + hostname + "]"
        origin = f"{parsed.scheme}://{hostname}:{port}"
        actual = httpx.URL(value)
        if actual.host.lower() != parsed.hostname.lower() and actual.host.lower() != hostname:
            raise ValueError()
        return str(actual), origin
    except (ValueError, UnicodeError, httpx.InvalidURL):
        raise _failure("MEDIA_URL_INVALID") from None


def _validate_image(content: bytes):
    if not content or len(content) > MAX_MEDIA_BYTES:
        raise _failure("MEDIA_SIZE_EXCEEDED")
    # Python 3.11 warning filters are process-global. Serialize this brief codec
    # section so concurrent captures cannot accidentally suppress bomb warnings.
    with _IMAGE_LOCK, warnings.catch_warnings():
        warnings.simplefilter("error", Image.DecompressionBombWarning)
        try:
            with Image.open(io.BytesIO(content), formats=list(_FORMATS)) as image:
                width, height = image.size
                if (
                    not 0 < width <= 16384
                    or not 0 < height <= 16384
                    or width * height > MAX_MEDIA_PIXELS
                ):
                    raise _failure("MEDIA_PIXELS_EXCEEDED")
                mime = _FORMATS[image.format]
                image.verify()
            frames, pixels = 0, 0
            with Image.open(io.BytesIO(content), formats=list(_FORMATS)) as image:
                while True:
                    try:
                        image.seek(frames)
                    except EOFError:
                        break
                    frames += 1
                    pixels += image.width * image.height
                    if frames > MAX_MEDIA_FRAMES or pixels > MAX_MEDIA_PIXELS:
                        raise _failure("MEDIA_PIXELS_EXCEEDED")
                    image.load()
            return {
                "mime": mime,
                "width": width,
                "height": height,
                "frame_count": frames,
                "decoded_pixels": pixels,
            }
        except ForestError:
            raise
        except Exception:
            raise _failure("MEDIA_IMAGE_INVALID") from None


async def _validate_owned(content):
    task = asyncio.create_task(asyncio.to_thread(_validate_image, content))
    try:
        return await asyncio.shield(task)
    finally:
        # Cancelling the awaiting Task does not terminate its decoder thread.
        # Keep that thread owned until it really completes.
        if not task.done():
            while not task.done():
                try:
                    await asyncio.shield(task)
                except asyncio.CancelledError:
                    continue
                except Exception:
                    break
        if task.done() and not task.cancelled():
            task.exception()


class MediaStore:
    def __init__(self, messages, *, timeout_seconds=15):
        if not isinstance(timeout_seconds, (int, float)) or not 0 < timeout_seconds <= 30:
            raise ValueError("Media timeout must be between zero and 30 seconds")
        self.messages, self.store = messages, messages.store
        self.contents = InputContentStore(self.store)
        self.timeout_seconds = timeout_seconds

    async def _origins_in(self, conn, allowed_origins):
        frozen = {_url(value, origin_only=True)[1] for value in allowed_origins}
        row = await (
            await conn.execute(
                "SELECT r.* FROM public.bot_agent_config_heads h JOIN public.bot_agent_config_revisions r "
                "ON (r.bot_id,r.revision)=(h.bot_id,h.revision) WHERE h.bot_id=%s FOR SHARE OF h",
                (self.store.bot_id,),
            )
        ).fetchone()
        if not row:
            return set()
        from forest.storage.agent_config import AgentConfig

        saved = await AgentConfig(self.store)._hydrate(conn, row, kind="REVISION", required=False)
        if saved["values_payload_state"] != "ACTIVE" or not saved["values"].get("enabled"):
            return set()
        return frozen & {
            _url(value, origin_only=True)[1]
            for value in saved["values"].get("media_allowed_origins", [])
        }

    async def _source(self, conn, actor, request, cut):
        clock = await self.contents.payloads.lock_in(conn)
        row = await (
            await conn.execute(
                "SELECT m.*,u.unit_id,i.conversation_id,i.ingress_seq,i.source_event_id,"
                "i.context_disposition,c.group_source_id,c.platform,c.channel_kind,c.channel_id,c.thread_id "
                "FROM public.bot_inbound_media_refs m JOIN public.bot_message_inbox i "
                "ON (i.bot_id,i.event_id)=(m.bot_id,m.event_id) JOIN public.bot_context_units u "
                "ON (u.bot_id,u.event_id)=(i.bot_id,i.event_id) JOIN public.bot_conversations c "
                "ON (c.bot_id,c.conversation_id)=(i.bot_id,i.conversation_id) "
                "WHERE m.bot_id=%s AND m.media_ref=%s AND u.unit_id=%s AND i.conversation_id=%s FOR UPDATE OF m",
                (
                    self.store.bot_id,
                    request["media_ref"],
                    request["unit_id"],
                    actor.conversation_id,
                ),
            )
        ).fetchone()
        if row is None or row["ingress_seq"] > cut:
            raise _failure("MEDIA_SOURCE_MISMATCH")
        inbox = await self.contents.read_input_in(
            conn, row["event_id"], expected_deletion_epoch=clock["deletion_epoch"], required=True
        )
        media = await self.contents.read_media_source_in(
            conn, row["media_ref"], expected_deletion_epoch=clock["deletion_epoch"]
        )
        row = {
            **row,
            "author": inbox["author"],
            "segments": inbox["segments"],
            "source": media["source"],
            "deletion_epoch": clock["deletion_epoch"],
        }
        if (
            str(row["part_index"]) != request["part_id"]
            or row["part_index"] >= len(row["segments"])
            or row["segments"][row["part_index"]].get("media_ref") != request["media_ref"]
        ):
            raise _failure("MEDIA_SOURCE_MISMATCH")
        return row

    @staticmethod
    def _snapshot(row, source):
        return MediaSnapshot(
            row["bot_id"],
            row["media_ref"],
            row["media_revision"],
            source["unit_id"],
            str(source["part_index"]),
            row["mime"],
            bytes(row["content"]),
            row["blob_digest"],
            row["width"],
            row["height"],
        )

    async def _claim(self, actor, request, cut, allowed_origins):
        async with self.store.pool.connection() as conn, conn.transaction():
            binding, _ = await self.messages._actor(conn, actor)
            origins = await self._origins_in(conn, allowed_origins)
            source = await self._source(conn, actor, request, cut)
            if source["context_disposition"] != "READABLE":
                await self.messages._actor_final(conn, actor, binding)
                return None
            saved = await self.contents.read_capture_in(
                conn, source["media_ref"], expected_deletion_epoch=source["deletion_epoch"]
            )
            if saved:
                await self.messages._actor_final(conn, actor, binding)
                return self._snapshot(saved, source)
            now = await self.store._now(conn)
            if (source["capture_expires_at"] and source["capture_expires_at"] > now) or (
                source["retry_after"] and source["retry_after"] > now
            ):
                await self.messages._actor_final(conn, actor, binding)
                return None
            try:
                url, origin = _url(source["source"].get("uri"))
            except ForestError:
                await self.messages._actor_final(conn, actor, binding)
                return None
            if origin not in origins:
                await self.messages._actor_final(conn, actor, binding)
                return None
            capture_id, epoch = str(uuid4()), source["capture_epoch"] + 1
            await conn.execute(
                "UPDATE public.bot_inbound_media_refs SET status='FETCHING',capture_id=%s,capture_epoch=%s,"
                "capture_expires_at=clock_timestamp()+interval '45 seconds',retry_after=NULL,error_code=NULL "
                "WHERE bot_id=%s AND media_ref=%s",
                (capture_id, epoch, self.store.bot_id, source["media_ref"]),
            )
            await self.messages._actor_final(conn, actor, binding)
            return {
                "url": url,
                "origin": origin,
                "capture_id": capture_id,
                "capture_epoch": epoch,
                "source_digest": digest_json(source["source"]),
                "deletion_epoch": source["deletion_epoch"],
            }

    async def _download(self, url, *, timeout_seconds=None):
        # A new client per GET cannot carry a preceding response's Set-Cookie.
        timeout_seconds = self.timeout_seconds if timeout_seconds is None else timeout_seconds
        async with (
            httpx.AsyncClient(
                timeout=timeout_seconds,
                trust_env=False,
                follow_redirects=False,
                transport=httpx.AsyncHTTPTransport(retries=0, trust_env=False),
            ) as client,
            asyncio.timeout(timeout_seconds),
        ):
            try:
                async with client.stream(
                    "GET", url, headers={"Accept-Encoding": "identity", "Accept": "image/*"}
                ) as response:
                    if response.status_code != 200:
                        raise _failure("MEDIA_HTTP_REJECTED")
                    if response.headers.get("Content-Encoding", "identity").lower() not in {
                        "",
                        "identity",
                    }:
                        raise _failure("MEDIA_ENCODING_UNSUPPORTED")
                    length = response.headers.get("Content-Length")
                    if length is not None and (
                        not length.isdigit() or int(length) > MAX_MEDIA_BYTES
                    ):
                        raise _failure("MEDIA_SIZE_EXCEEDED")
                    content = bytearray()
                    async for chunk in response.aiter_raw():
                        if len(content) + len(chunk) > MAX_MEDIA_BYTES:
                            raise _failure("MEDIA_SIZE_EXCEEDED")
                        content.extend(chunk)
                    return bytes(content)
            except (httpx.HTTPError, TimeoutError):
                raise _failure("MEDIA_TRANSPORT_FAILED") from None

    async def _finish(
        self,
        actor,
        request,
        cut,
        allowed_origins,
        claim,
        *,
        content=None,
        properties=None,
        error=None,
    ):
        async with self.store.pool.connection() as conn, conn.transaction():
            binding, _ = await self.messages._actor(conn, actor)
            origins = await self._origins_in(conn, allowed_origins)
            source = await self._source(conn, actor, request, cut)
            if (
                source["capture_id"] != claim["capture_id"]
                or source["capture_epoch"] != claim["capture_epoch"]
                or source["deletion_epoch"] != claim["deletion_epoch"]
                or source["source"] is None
                or digest_json(source["source"]) != claim["source_digest"]
                or source["capture_expires_at"] <= await self.store._now(conn)
            ):
                raise _failure("MEDIA_CAPTURE_STALE")
            if claim["origin"] not in origins or source["context_disposition"] != "READABLE":
                error = "MEDIA_PERMISSION_REVOKED"
            saved = None
            if error is None:
                await conn.execute(
                    "INSERT INTO public.bot_media_quota_heads(bot_id) VALUES (%s) ON CONFLICT DO NOTHING",
                    (self.store.bot_id,),
                )
                await (
                    await conn.execute(
                        "SELECT retained_bytes FROM public.bot_media_quota_heads WHERE bot_id=%s FOR UPDATE",
                        (self.store.bot_id,),
                    )
                ).fetchone()
                # Revoked bodies still consume storage until the scrub transaction
                # removes them. The memory clock serializes that physical deletion
                # with this quota reconciliation and the new capture publication.
                quota = await (
                    await conn.execute(
                        "UPDATE public.bot_media_quota_heads SET retained_bytes=(SELECT COALESCE(sum(h.byte_length),0) "
                        "FROM public.bot_media_captures c JOIN public.bot_payload_headers h ON (h.bot_id,h.payload_id)="
                        "(c.bot_id,c.payload_id) JOIN public.bot_payload_bodies b ON (b.bot_id,b.payload_id)="
                        "(h.bot_id,h.payload_id) WHERE c.bot_id=%s) WHERE bot_id=%s RETURNING retained_bytes",
                        (self.store.bot_id, self.store.bot_id),
                    )
                ).fetchone()
                if quota["retained_bytes"] + len(content) > MAX_BOT_MEDIA_BYTES:
                    error = "MEDIA_BOT_QUOTA_EXCEEDED"
            if error is None:
                manifest = {
                    key: source[key]
                    for key in (
                        "event_id",
                        "source_event_id",
                        "unit_id",
                        "part_index",
                        "conversation_id",
                        "group_source_id",
                        "platform",
                        "channel_kind",
                        "channel_id",
                        "thread_id",
                        "author",
                    )
                }
                refs = (
                    self.contents.source_ref(source["source_event_id"], str(source["part_index"])),
                )
                body = await self.contents.payloads.put_bytes_in(
                    conn,
                    owner_kind="media.capture",
                    owner_id="input-capture:" + source["media_ref"],
                    value=content,
                    sources=refs,
                    expected_deletion_epoch=claim["deletion_epoch"],
                )
                manifest_body = await self.contents.payloads.put_json_in(
                    conn,
                    owner_kind="media.source",
                    owner_id="capture-manifest:" + source["media_ref"],
                    value=manifest,
                    sources=refs,
                    expected_deletion_epoch=claim["deletion_epoch"],
                )
                if body.state != "ACTIVE" or manifest_body.state != "ACTIVE":
                    raise _failure("MEDIA_SOURCE_REVOKED")
                saved = await (
                    await conn.execute(
                        "INSERT INTO public.bot_media_captures(bot_id,media_ref,media_revision,event_id,part_index,payload_id,"
                        "blob_digest,mime,width,height,frame_count,decoded_pixels,manifest_payload_id,source_digest,capture_id,capture_epoch) "
                        "VALUES (%s,%s,1,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) RETURNING *",
                        (
                            self.store.bot_id,
                            source["media_ref"],
                            source["event_id"],
                            source["part_index"],
                            body.payload_id,
                            "sha256:" + hashlib.sha256(content).hexdigest(),
                            properties["mime"],
                            properties["width"],
                            properties["height"],
                            properties["frame_count"],
                            properties["decoded_pixels"],
                            manifest_body.payload_id,
                            claim["source_digest"],
                            claim["capture_id"],
                            claim["capture_epoch"],
                        ),
                    )
                ).fetchone()
                saved = {**saved, "content": content, "source_manifest": manifest}
                await conn.execute(
                    "UPDATE public.bot_media_quota_heads SET retained_bytes=retained_bytes+%s WHERE bot_id=%s",
                    (len(content), self.store.bot_id),
                )
            await conn.execute(
                "UPDATE public.bot_inbound_media_refs SET status=%s,error_code=%s,capture_expires_at=NULL,"
                "retry_after=CASE WHEN %s THEN clock_timestamp()+interval '60 seconds' ELSE NULL END "
                "WHERE bot_id=%s AND media_ref=%s",
                (
                    "CAPTURED" if saved else "FETCH_FAILED",
                    error,
                    saved is None,
                    self.store.bot_id,
                    source["media_ref"],
                ),
            )
            await self.messages._actor_final(conn, actor, binding)
            return self._snapshot(saved, source) if saved else None

    async def materialize(self, actor, snapshot, *, allowed_origins):
        """Return {(canonical unit ID, part ID): pixels}; never modify snapshot.

        Only direct inbox images currently have capture authority. Quoted or tool
        media need their own future validated binding; arbitrary refs are refused.
        A bounded subset is captured per call; unresolved refs remain canonical.
        """
        if (
            not isinstance(snapshot, dict)
            or snapshot.get("bot_id") != self.store.bot_id
            or actor.host.bot_id != self.store.bot_id
            or not isinstance(allowed_origins, (tuple, list))
            or len(allowed_origins) > 32
        ):
            raise _failure("MEDIA_SOURCE_MISMATCH")
        cut, units = snapshot.get("selected_input_end_seq"), snapshot.get("units")
        if type(cut) is not int or cut < 0 or not isinstance(units, list) or len(units) > 2048:
            raise _failure("MEDIA_SOURCE_MISMATCH")
        requests, seen = [], set()
        for unit in units:
            if not isinstance(unit, dict) or not isinstance(unit.get("parts"), list):
                raise _failure("MEDIA_SOURCE_MISMATCH")
            if unit.get("role") != "user":
                continue
            for index, part in enumerate(unit["parts"]):
                if not isinstance(part, dict) or part.get("type") != "image":
                    continue
                key = (unit.get("unit_id"), part.get("part_id", str(index)))
                if (
                    any(not isinstance(v, str) or not 1 <= len(v) <= 256 for v in key)
                    or key in seen
                    or not isinstance(part.get("media_ref"), str)
                ):
                    raise _failure("MEDIA_SOURCE_MISMATCH")
                seen.add(key)
                requests.append(
                    {"unit_id": key[0], "part_id": key[1], "media_ref": part["media_ref"]}
                )
        result, total = {}, 0
        remaining = MAX_CONTEXT_CAPTURE_SECONDS
        if requests:
            async with self.store.pool.connection() as conn, conn.transaction():
                binding, _ = await self.messages._actor(conn, actor)
                run = await (
                    await conn.execute(
                        "SELECT conversation_id,deadline_at FROM public.bot_agent_runs WHERE bot_id=%s AND run_id=%s",
                        (self.store.bot_id, snapshot.get("run_id")),
                    )
                ).fetchone()
                if run:
                    if run["conversation_id"] != actor.conversation_id:
                        raise _failure("MEDIA_SOURCE_MISMATCH")
                    remaining = min(
                        remaining,
                        (run["deadline_at"] - await self.store._now(conn)).total_seconds(),
                    )
                await self.messages._actor_final(conn, actor, binding)
        deadline = asyncio.get_running_loop().time() + remaining
        for request in requests[:MAX_CONTEXT_MEDIA]:
            if asyncio.get_running_loop().time() >= deadline:
                break
            claim = await self._claim(actor, request, cut, allowed_origins)
            if isinstance(claim, MediaSnapshot):
                image = claim
            elif claim is None:
                continue
            else:
                content = properties = None
                error = None
                try:
                    duration = min(
                        self.timeout_seconds,
                        max(0.001, deadline - asyncio.get_running_loop().time()),
                    )
                    content = await self._download(claim["url"], timeout_seconds=duration)
                    properties = await _validate_owned(content)
                except (ForestError, TimeoutError) as exc:
                    error = exc.code if isinstance(exc, ForestError) else "MEDIA_TRANSPORT_FAILED"
                # Cancellation propagates. The durable claim expires; no half
                # image or invented failed outcome is committed by a stale owner.
                image = await self._finish(
                    actor,
                    request,
                    cut,
                    allowed_origins,
                    claim,
                    content=content,
                    properties=properties,
                    error=error,
                )
            if image is not None:
                total += len(image.content)
                if total > MAX_CONTEXT_MEDIA_BYTES:
                    break
                result[(request["unit_id"], request["part_id"])] = image
        return result
