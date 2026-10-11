from __future__ import annotations

import io
import socket

import httpx
import pytest
from PIL import Image

from forest.errors import ForestError
from forest.media_capture_io import PublicImageCaptureClient, _validate


@pytest.mark.parametrize(
    "url",
    (
        "file:///tmp/image.png",
        "http://user@example.com/image.png",
        "http://127.0.0.1/image.png",
        "http://[::1]/image.png",
    ),
)
async def test_media_capture_rejects_non_public_or_credentialed_urls(url):
    with pytest.raises(ForestError) as caught:
        await PublicImageCaptureClient._public_url(url)
    assert caught.value.code == "MEDIA_URL_FORBIDDEN"


async def test_media_capture_rejects_host_when_any_dns_answer_is_private(monkeypatch):
    def mixed_answers(_host, port, *, type):
        assert type == socket.SOCK_STREAM
        return (
            (socket.AF_INET, socket.SOCK_STREAM, 6, "", ("1.1.1.1", port)),
            (socket.AF_INET, socket.SOCK_STREAM, 6, "", ("10.0.0.1", port)),
        )

    monkeypatch.setattr(socket, "getaddrinfo", mixed_answers)
    with pytest.raises(ForestError) as caught:
        await PublicImageCaptureClient._public_url("https://example.invalid/image.png")
    assert caught.value.code == "MEDIA_URL_FORBIDDEN"


async def test_media_capture_connects_only_to_validated_dns_answer(monkeypatch):
    calls = 0

    def resolve(_host, port, *, type):
        nonlocal calls
        calls += 1
        assert type == socket.SOCK_STREAM
        address = "1.1.1.1" if calls == 1 else "127.0.0.1"
        return ((socket.AF_INET, socket.SOCK_STREAM, 6, "", (address, port)),)

    monkeypatch.setattr(socket, "getaddrinfo", resolve)
    image = io.BytesIO()
    Image.new("RGB", (3, 2), (12, 34, 56)).save(image, format="PNG")

    def transport(request):
        assert request.url.host == "1.1.1.1"
        assert request.headers["Host"] == "example.invalid:8443"
        assert request.extensions["sni_hostname"] == "example.invalid"
        return httpx.Response(200, stream=httpx.ByteStream(image.getvalue()))

    capture = PublicImageCaptureClient()
    await capture.aclose()
    capture._client = httpx.AsyncClient(transport=httpx.MockTransport(transport))
    try:
        result = await capture.fetch({"uri": "https://example.invalid:8443/image.png"})
        assert result.content == image.getvalue()
        assert calls == 1
    finally:
        await capture.aclose()


def test_media_capture_validates_and_identifies_decoded_image():
    body = io.BytesIO()
    Image.new("RGB", (3, 2), (12, 34, 56)).save(body, format="PNG")

    image = _validate(body.getvalue())

    assert image.media_type == "image/png"
    assert (image.width, image.height) == (3, 2)
    assert image.content == body.getvalue()


def test_media_capture_rejects_invalid_image_bytes():
    with pytest.raises(ForestError) as caught:
        _validate(b"not an image")
    assert caught.value.code == "MEDIA_IMAGE_INVALID"
