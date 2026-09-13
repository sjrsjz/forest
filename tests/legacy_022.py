"""Execute checksum-pinned, released repositories only to seed the old schema.

The fixtures are exact Git blobs from 394c7cb, compressed without timestamps.
They never participate in current-schema recovery or application execution.
"""

import gzip
import hashlib
import sys
from pathlib import Path
from types import ModuleType

_CHECKSUMS = {
    "messages": "35f4207c96559db6f06ddaaa29fb0da7200e0cb04261597dc13fecd14c0abba1",
    "memory_sources": "a6a9d4eb2b05983ebc038ea40d039e7f7372bc19af4f759010a82b6634703033",
    "delivery": "7b90079c703fb876c3587886aac9601a79a6cfd698d35b9f52801cc866c3a8a7",
    "payloads": "7212a701efe48b1dfdaac0f8d656c46a91ef7e998fbe8f4ba71b8a9d14f09075",
}


def _load(name):
    path = Path(__file__).parent / "fixtures" / "legacy_022" / (name + ".py.gz")
    source = gzip.decompress(path.read_bytes())
    assert hashlib.sha256(source).hexdigest() == _CHECKSUMS[name]
    module = ModuleType("_forest_release_022_" + name)
    module.__file__ = str(path)
    sys.modules[module.__name__] = module
    exec(compile(source, str(path), "exec"), module.__dict__)
    return module


FrozenPayloadStore = _load("payloads").PayloadStore
_memory_sources = _load("memory_sources")
# Its historical class imports this dependency from the installed package.
# Pin that import too: current payload admissions require the current schema.
_memory_sources.PayloadStore = FrozenPayloadStore
MemorySourceStore = _memory_sources.MemorySourceStore
DeliveryStore = _load("delivery").DeliveryStore
_ReleasedMessageStore = _load("messages").MessageStore


class MessageStore(_ReleasedMessageStore):
    def _memory_sources(self):
        return MemorySourceStore(self)
