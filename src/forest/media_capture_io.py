"""Bounded public-network acquisition and image validation for media.capture."""

from __future__ import annotations

import asyncio
import io
import ipaddress
import socket
import threading
import warnings
from dataclasses import dataclass
from urllib.parse import urlsplit

import httpx
from PIL import Image

from forest.errors import ForestError

MAX_MEDIA_BYTES = 8 * 1024 * 1024
MAX_MEDIA_PIXELS = 16_777_216
MAX_MEDIA_FRAMES = 100
_FORMATS = {"PNG": "image/png", "JPEG": "image/jpeg", "WEBP": "image/webp", "GIF": "image/gif"}
_IMAGE_LOCK = threading.Lock()


def _error(code: str) -> ForestError:
    return ForestError(code, "Managed image acquisition failed", 422)


@dataclass(frozen=True)
class CapturedImage:
    content: bytes
    media_type: str
    width: int
    height: int
    decoded_pixels: int


@dataclass(frozen=True)
class PinnedPublicURL:
    connect_url: str
    authority: str
    tls_hostname: str


def _validate(content: bytes) -> CapturedImage:
    if not content or len(content) > MAX_MEDIA_BYTES:
        raise _error("MEDIA_SIZE_EXCEEDED")
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
                    raise _error("MEDIA_PIXELS_EXCEEDED")
                media_type = _FORMATS[image.format]
                image.verify()
            frames = pixels = 0
            with Image.open(io.BytesIO(content), formats=list(_FORMATS)) as image:
                while True:
                    try:
                        image.seek(frames)
                    except EOFError:
                        break
                    frames += 1
                    pixels += image.width * image.height
                    if frames > MAX_MEDIA_FRAMES or pixels > MAX_MEDIA_PIXELS:
                        raise _error("MEDIA_PIXELS_EXCEEDED")
                    image.load()
            return CapturedImage(content, media_type, width, height, pixels)
        except ForestError:
            raise
        except Exception:
            raise _error("MEDIA_IMAGE_INVALID") from None


async def _validate_owned(content: bytes) -> CapturedImage:
    task = asyncio.create_task(asyncio.to_thread(_validate, content))
    try:
        return await asyncio.shield(task)
    finally:
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


class PublicImageCaptureClient:
    """Fetch one non-redirecting public HTTP(S) image with fixed resource bounds."""

    def __init__(self, *, timeout_seconds: float = 15, max_connections: int = 4) -> None:
        self.timeout_seconds = timeout_seconds
        self._client = httpx.AsyncClient(
            transport=httpx.AsyncHTTPTransport(
                retries=0,
                limits=httpx.Limits(
                    max_connections=max_connections,
                    max_keepalive_connections=0,
                ),
            ),
            follow_redirects=False,
            trust_env=False,
            timeout=httpx.Timeout(timeout_seconds),
            headers={"Accept-Encoding": "identity"},
        )

    @staticmethod
    async def _public_url(value: object) -> PinnedPublicURL:
        if (
            not isinstance(value, str)
            or not 1 <= len(value.encode()) <= 8192
            or "\\" in value
            or any(ord(char) <= 32 or ord(char) == 127 for char in value)
        ):
            raise _error("MEDIA_URL_INVALID")
        try:
            parsed = urlsplit(value)
            if (
                parsed.scheme not in {"http", "https"}
                or parsed.hostname is None
                or parsed.username is not None
                or parsed.password is not None
                or parsed.fragment
                or (parsed.port is not None and not 1 <= parsed.port <= 65535)
            ):
                raise ValueError
            hostname = parsed.hostname.encode("idna").decode("ascii")
            port = parsed.port or (443 if parsed.scheme == "https" else 80)
            addresses = await asyncio.get_running_loop().run_in_executor(
                None,
                lambda: socket.getaddrinfo(hostname, port, type=socket.SOCK_STREAM),
            )
            if not addresses or any(
                not ipaddress.ip_address(item[4][0]).is_global for item in addresses
            ):
                raise ValueError
            canonical = httpx.URL(value)
            return PinnedPublicURL(
                connect_url=str(canonical.copy_with(host=addresses[0][4][0])),
                authority=canonical.netloc.decode("ascii"),
                tls_hostname=hostname,
            )
        except (ValueError, UnicodeError, OSError, httpx.InvalidURL):
            raise _error("MEDIA_URL_FORBIDDEN") from None

    async def fetch(self, locator: dict) -> CapturedImage:
        target = await self._public_url(locator.get("uri"))
        try:
            async with asyncio.timeout(self.timeout_seconds):
                async with self._client.stream(
                    "GET", target.connect_url,
                    headers={"Host": target.authority},
                    extensions={"sni_hostname": target.tls_hostname},
                ) as response:
                    if response.status_code != 200:
                        raise _error("MEDIA_HTTP_REJECTED")
                    if response.headers.get("content-encoding", "identity").lower() != "identity":
                        raise _error("MEDIA_ENCODING_UNSUPPORTED")
                    length = response.headers.get("content-length")
                    if length is not None:
                        try:
                            declared = int(length)
                        except ValueError:
                            raise _error("MEDIA_SIZE_INVALID") from None
                        if not 1 <= declared <= MAX_MEDIA_BYTES:
                            raise _error("MEDIA_SIZE_EXCEEDED")
                    content = bytearray()
                    async for chunk in response.aiter_raw(chunk_size=65536):
                        content.extend(chunk)
                        if len(content) > MAX_MEDIA_BYTES:
                            raise _error("MEDIA_SIZE_EXCEEDED")
            return await _validate_owned(bytes(content))
        except ForestError:
            raise
        except (TimeoutError, httpx.HTTPError, OSError):
            raise _error("MEDIA_TRANSPORT_FAILED") from None

    async def aclose(self) -> None:
        await self._client.aclose()
