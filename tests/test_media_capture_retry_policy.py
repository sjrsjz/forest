import asyncio

from forest.builtin.media_capture.python.media_capture import MediaCapture
from forest.errors import ForestError


class _CaptureContext:
    def __init__(self, error_code):
        self.error_code = error_code
        self.part_state = "PENDING"
        self.fetches = 0
        self.unavailable = 0
        self.services = self
        self.commands = self
        self.queries = self
        self.operations = self

    def stable(self, value):
        return value

    async def call(self, name, payload, **_kwargs):
        if name == "media.image.fetch":
            self.fetches += 1
            raise ForestError(self.error_code, "image acquisition failed", 422)
        if name == "media.fail-part":
            self.part_state = "FAILED" if payload["final"] else "PENDING"
            return {}
        if name == "media.capture.current":
            return {
                "parts": [{"part_kind": "IMAGE", "state": self.part_state}],
                "conversation_ref": "conversation/test",
                "event_ref": "event/test",
            }
        if name == "memory.source.unavailable":
            self.unavailable += 1
            return {}
        raise AssertionError(name)


def _run_capture(error_code):
    worker = MediaCapture()
    worker.max_attempts = 3
    context = _CaptureContext(error_code)
    task = {
        "attempt": 1,
        "payload": {
            "capture_id": "capture/test",
            "claim_token": "claim/test",
            "identity": {"trunk_id": "trunk/test"},
            "conversation_ref": "conversation/test",
            "event_ref": "event/test",
            "binding_ref": "binding/test",
            "author_subject_ref": "author/test",
            "occurred_at": "2026-10-09T00:00:00+00:00",
            "parts": [{
                "part_kind": "IMAGE",
                "state": "PENDING",
                "media_ref": "media/test",
                "locator": {},
                "part_id": "part:image:0",
            }],
        },
    }
    result = asyncio.run(worker.capture(task, context))
    return result, context


def test_oversized_image_is_marked_unavailable_without_retry():
    result, context = _run_capture("MEDIA_PIXELS_EXCEEDED")
    assert result == {"disposition": "FAILED", "memory_published": True}
    assert context.fetches == 1
    assert context.unavailable == 1


def test_transport_failure_retains_bounded_retry():
    result, context = _run_capture("MEDIA_TRANSPORT_FAILED")
    assert result == {"disposition": "RETRY", "memory_published": False}
    assert context.fetches == 1
    assert context.unavailable == 0
