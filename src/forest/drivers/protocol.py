"""Bounded, versioned JSON frames. Neither side accepts executable objects."""

from __future__ import annotations

import asyncio
import json
import math
import struct
from typing import Any

from forest.errors import ForestError
from forest.worker_limits import MAX_FRAME_BYTES

WIRE_VERSION = 1
MAX_DEPTH = 32
MAX_ITEMS = 16384


def validate_json(value: Any) -> None:
    pending = [(value, 0)]
    items = 0
    while pending:
        item, depth = pending.pop()
        items += 1
        if items > MAX_ITEMS or depth > MAX_DEPTH:
            raise ForestError("IPC_FRAME_INVALID", "DTO structure exceeds protocol limits", 400)
        if item is None or isinstance(item, bool):
            continue
        if isinstance(item, str):
            if len(item.encode("utf-8")) > MAX_FRAME_BYTES:
                raise ForestError("IPC_FRAME_INVALID", "DTO field exceeds protocol limit", 400)
        elif isinstance(item, int):
            if not -(2**63) <= item < 2**63:
                raise ForestError(
                    "IPC_FRAME_INVALID", "DTO integer exceeds signed 64-bit range", 400
                )
        elif isinstance(item, float):
            if not math.isfinite(item):
                raise ForestError("IPC_FRAME_INVALID", "DTO number must be finite", 400)
        elif isinstance(item, list):
            pending.extend((child, depth + 1) for child in item)
        elif isinstance(item, dict):
            if any(not isinstance(key, str) or len(key) > 256 for key in item):
                raise ForestError("IPC_FRAME_INVALID", "DTO keys must be bounded text", 400)
            pending.extend((child, depth + 1) for child in item.values())
        else:
            raise ForestError("IPC_FRAME_INVALID", "DTO contains an unsupported Python object", 400)


def _unique_object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("Duplicate JSON key")
        result[key] = value
    return result


def encode_frame(message: dict[str, Any], limit: int = MAX_FRAME_BYTES) -> bytes:
    try:
        validate_json(message)
        data = json.dumps(
            message, ensure_ascii=False, allow_nan=False, separators=(",", ":")
        ).encode("utf-8")
    except (ValueError, TypeError, UnicodeError, RecursionError):
        raise ForestError("IPC_FRAME_INVALID", "Invalid JSON DTO", 400) from None
    if not 0 < len(data) <= limit:
        raise ForestError("IPC_FRAME_TOO_LARGE", "IPC frame exceeds configured byte limit", 413)
    return struct.pack("!I", len(data)) + data


async def read_frame(reader: asyncio.StreamReader, limit: int = MAX_FRAME_BYTES) -> dict[str, Any]:
    header = await reader.readexactly(4)
    size = struct.unpack("!I", header)[0]
    if not 0 < size <= limit:
        raise ForestError("IPC_FRAME_TOO_LARGE", "IPC frame exceeds configured byte limit", 413)
    data = await asyncio.wait_for(reader.readexactly(size), timeout=15)
    try:
        result = json.loads(data, object_pairs_hook=_unique_object)
        if not isinstance(result, dict) or result.get("wire_version") != WIRE_VERSION:
            raise ValueError("Invalid frame envelope")
        validate_json(result)
    except (ValueError, TypeError, UnicodeError, RecursionError):
        raise ForestError("IPC_FRAME_INVALID", "Invalid versioned JSON frame", 400) from None
    return result


class FrameWriter:
    def __init__(self, writer: asyncio.StreamWriter, limit: int = MAX_FRAME_BYTES):
        self.writer = writer
        self.limit = limit
        self.lock = asyncio.Lock()

    async def send(self, message: dict[str, Any]) -> None:
        frame = encode_frame({"wire_version": WIRE_VERSION, **message}, self.limit)
        async with self.lock:
            self.writer.write(frame)
            await asyncio.wait_for(self.writer.drain(), timeout=5)
