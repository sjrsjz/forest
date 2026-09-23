import base64
import hashlib
import json
from datetime import datetime, timedelta

from forest.errors import ForestError
from forest.sdk import Plugin, service


def _plain(value):
    if hasattr(value, "items"):
        return {key: _plain(item) for key, item in value.items()}
    if isinstance(value, (tuple, list)):
        return [_plain(item) for item in value]
    return value


def _canonical(value):
    return json.dumps(
        value, sort_keys=True, separators=(",", ":"), ensure_ascii=False
    ).encode("utf-8")


def _digest(value):
    return "sha256:" + hashlib.sha256(_canonical(value)).hexdigest()


def _connection_epoch(generation_id, binding_ref):
    value = hashlib.sha256((generation_id + "\0" + binding_ref).encode()).hexdigest()
    return int(value[:15], 16) + 1


def _client_incarnation(generation_id, binding_ref):
    value = hashlib.sha256((generation_id + "\0" + binding_ref).encode()).hexdigest()
    return "onebot-client:" + value


def _transport_identity(ctx, binding):
    generation_id = ctx.identity["generation_id"]
    identity = {
        "binding_ref": binding["binding_ref"],
        "binding_epoch": binding["binding_epoch"],
        "connection_epoch": _connection_epoch(generation_id, binding["binding_ref"]),
        "client_incarnation": _client_incarnation(generation_id, binding["binding_ref"]),
        "adapter_generation_id": generation_id,
        "graph_id": ctx.identity["graph_id"],
        "graph_revision": ctx.identity["graph_revision"],
    }
    fence = {"protocol": "forest.onebot-transport-fence/2", **identity}
    return {**identity, "transport_digest": _digest(fence)}


class AdapterOneBot(Plugin):
    async def prepare(self, ctx, config, restored_state=None):
        await super().prepare(ctx, config, restored_state)
        value = _plain(config)
        if value.get("protocol") != "forest.adapter.onebot/2":
            raise ForestError(
                "ONEBOT_CONFIGURATION_INVALID", "OneBot configuration is invalid", 422
            )
        bindings = value.get("bindings")
        refs = [item.get("binding_ref") for item in bindings]
        accounts = [item.get("self_id") for item in bindings]
        if len(refs) != len(set(refs)) or len(accounts) != len(set(accounts)):
            raise ForestError(
                "ONEBOT_CONFIGURATION_INVALID",
                "OneBot Binding or account identities are duplicated",
                422,
            )
        if ctx.identity.get("graph_id") is None or ctx.identity.get("graph_revision") is None:
            raise ForestError(
                "RUNTIME_GENERATION_IDENTITY_UNAVAILABLE",
                "OneBot Worker has no RuntimeGraph identity",
                503,
            )
        self.bindings = {item["binding_ref"]: item for item in bindings}

    def _binding(self, binding_ref, *, enabled=True):
        binding = self.bindings.get(binding_ref)
        if binding is None or (enabled and not binding["enabled"]):
            raise ForestError("ONEBOT_BINDING_UNAVAILABLE", "OneBot Binding is unavailable", 404)
        return binding

    async def _write_payload(
        self, ctx, *, owner_kind, owner_id, media_type, content, source, parents=()
    ):
        return await ctx.services.call(
            "kernel.payload.bytes-write",
            {
                "protocol": "forest.payload-bytes-write/1",
                "owner_kind": owner_kind,
                "owner_id": owner_id,
                "media_type": media_type,
                "content_base64": base64.b64encode(content).decode("ascii"),
                "parents": list(parents),
                "sources": [source],
            },
            operation_id=ctx.operations.stable("payload:" + owner_kind + ":" + owner_id[-24:]),
        )

    @service("adapter.onebot.delivery.prepare", semantics="idempotent")
    async def prepare_delivery(self, payload, ctx):
        value = _plain(payload)
        binding = self._binding(value["binding_ref"])
        prepared = await ctx.services.call(
            "onebot.codec.prepare-send",
            {
                "channel_kind": value["channel_kind"].lower(),
                "channel_id": value["channel_ref"],
                "segments": value["parts"],
            },
            operation_id=ctx.operations.stable("prepare-wire"),
        )
        return {
            "protocol": "forest.onebot-delivery-prepared/2",
            **_transport_identity(ctx, binding),
            "prepared": _plain(prepared),
        }

    @service("adapter.onebot.delivery.dispatch", semantics="at-most-once")
    async def dispatch_delivery(self, payload, ctx):
        return await ctx.services.call(
            "adapter.onebot.transport.send",
            _plain(payload),
            operation_id=ctx.operations.stable("transport-send"),
            timeout=payload["timeout_seconds"] + 5,
            contract_major=2,
        )

    @service("adapter.onebot.transport.frame", semantics="at-most-once")
    async def ingest_frame(self, payload, ctx):
        frame = _plain(payload)
        binding = self._binding(frame["binding_ref"])
        expected = _transport_identity(ctx, binding)
        if any(frame[key] != value for key, value in expected.items()):
            raise ForestError(
                "ONEBOT_TRANSPORT_FENCE_MISMATCH",
                "OneBot frame belongs to another transport Generation",
                409,
            )
        normalized_result = await ctx.services.call(
            "onebot.codec.normalize",
            {"body_b64": frame["body_base64"], "self_id": binding["self_id"]},
            operation_id=ctx.operations.stable("normalize"),
        )
        normalized = _plain(normalized_result["message"])
        if normalized is None:
            return {
                "protocol": "forest.onebot-frame-result/2",
                "frame_id": frame["frame_id"],
                "status": "IGNORED",
                "event_id": None,
            }
        return await self._admit(frame, binding, normalized, ctx)

    async def _admit(self, frame, binding, normalized, ctx):
        message_ref = normalized["platform_message_id"]
        event_hash = hashlib.sha256(
            (binding["binding_ref"] + "\0" + message_ref).encode()
        ).hexdigest()
        event_id = "onebot-event:" + event_hash
        channel_kind = normalized["channel_kind"]
        channel_ref = normalized["channel_id"]
        conversation_ref = f"onebot:{channel_kind}:{channel_ref}"
        author = normalized["author"]
        anonymous = author.get("anonymous")
        author_subject_ref = (
            f"onebot:anonymous:{anonymous.get('id') or event_hash[:20]}"
            if anonymous is not None
            else "qq:" + author["external_user_id"]
        )
        sent_at = normalized["sent_at"].replace("Z", "+00:00")
        received_at = datetime.now().astimezone()
        source_hash = hashlib.sha256(
            (
                normalized["platform"]
                + "\0"
                + binding["self_id"]
                + "\0"
                + channel_kind
                + "\0"
                + channel_ref
            ).encode()
        ).hexdigest()
        trunk_id = "onebot-trunk:" + source_hash if channel_kind == "group" else None
        source = {
            "source_component_id": "adapter.onebot",
            "source_kind": "onebot.platform-event",
            "source_id": event_id,
            "source_revision": 1,
            "part_id": "*",
            "source_trunk_id": trunk_id,
            "observation_conversation_id": conversation_ref,
            "observation_binding_id": binding["binding_ref"],
            "author_subject_id": author_subject_ref,
            "occurred_at": sent_at,
        }
        raw_body = base64.b64decode(frame["body_base64"])
        raw = await self._write_payload(
            ctx,
            owner_kind="onebot.raw-event",
            owner_id=event_id,
            media_type="application/json",
            content=raw_body,
            source=source,
        )
        author_saved = await self._write_payload(
            ctx,
            owner_kind="onebot.author-snapshot",
            owner_id=event_id,
            media_type="application/json",
            content=_canonical(author),
            source=source,
        )
        context_parts, memory_texts, images = [], [], []
        for index, part in enumerate(normalized["segments"]):
            kind = part["type"]
            if kind == "image":
                media_ref = "media:" + hashlib.sha256(
                    (event_id + "\0" + str(index)).encode()
                ).hexdigest()
                context_parts.append(
                    {
                        "type": "image",
                        "media_ref": media_ref,
                        "media_revision": 1,
                        "state": "PENDING_CAPTURE",
                    }
                )
                locator = part["media_ref"]
                images.append(
                    {
                        "part_id": "part:image:" + str(index),
                        "part_index": index,
                        "media_ref": media_ref,
                        "locator": {
                            "kind": "onebot-source",
                            "uri": locator.get("uri"),
                            "platform_file": locator.get("platform_file"),
                        },
                    }
                )
                continue
            context_parts.append(part)
            if kind == "text":
                memory_texts.append((index, part["text"]))
            elif kind == "mention":
                memory_texts.append((index, "[mention:" + part["external_user_id"] + "]"))
            elif kind == "reply":
                memory_texts.append((index, "[reply:" + part["platform_message_id"] + "]"))
            else:
                memory_texts.append((index, "[unsupported:" + part.get("platform_type", kind) + "]"))
        normalized_saved = await self._write_payload(
            ctx,
            owner_kind="onebot.normalized-event",
            owner_id=event_id,
            media_type="application/json",
            content=_canonical({"parts": context_parts}),
            source=source,
        )
        if not memory_texts and not images:
            memory_texts.append((0, "[empty message]"))
        memory_parts = []
        for part_index, text in memory_texts:
            owner_id = event_id + ":memory-text:" + str(part_index)
            encoded = text.encode("utf-8")
            saved = await self._write_payload(
                ctx,
                owner_kind="onebot.memory-text",
                owner_id=owner_id,
                media_type="text/plain; charset=utf-8",
                content=encoded,
                source={**source, "part_id": "part:text:" + str(part_index)},
                parents=(normalized_saved["payload"]["payload_id"],),
            )
            memory_parts.append(
                {
                    "part_id": "part:text:" + str(part_index),
                    "part_index": part_index,
                    "part_kind": "TEXT",
                    "media_type": "text/plain; charset=utf-8",
                    "payload": saved["payload"],
                    "width": None,
                    "height": None,
                    "byte_length": len(encoded),
                }
            )
        identity = {
            "source_id": "onebot-source:" + source_hash,
            "source_kind": "GROUP" if channel_kind == "group" else "DIRECT",
            "identity_incarnation": 1,
            "platform": normalized["platform"],
            "realm_id": "qq:" + binding["self_id"],
            "external_group_id": channel_ref if channel_kind == "group" else None,
            "trunk_id": trunk_id,
        }
        capture = {
            "identity": identity,
            "source_unit_id": "onebot-unit:" + event_hash,
            "source_revision": 1,
            "unit_kind": "GROUP_MESSAGE" if channel_kind == "group" else "DIRECT_MESSAGE",
            "author": author_saved["payload"],
            "occurred_at": sent_at,
        }
        if images:
            media_capture = {
                **capture,
                "capture_id": "media-capture:" + event_hash,
                "text_parts": memory_parts,
                "image_parts": images,
            }
            memory_capture = None
        else:
            memory_capture = {**capture, "parts": memory_parts}
            media_capture = None
        projected = {**normalized, "segments": context_parts}
        shell = await ctx.services.call(
            "shell.ingress.inspect",
            {
                "normalized": projected,
                "binding_ref": binding["binding_ref"],
                "binding_epoch": binding["binding_epoch"],
                "connection_epoch": frame["connection_epoch"],
                "client_incarnation": frame["client_incarnation"],
                "adapter_generation_id": frame["adapter_generation_id"],
                "executor_fence_digest": frame["transport_digest"],
                "conversation_ref": conversation_ref,
                "event_ref": event_id,
                "principal_ref": author_subject_ref,
            },
            operation_id=ctx.operations.stable("shell-inspect"),
        )
        classification = None
        if shell is None:
            classification = await ctx.services.call(
                "conversation.trigger.classify",
                {"normalized": projected},
                operation_id=ctx.operations.stable("trigger-classify"),
            )
        event = {
            "event_id": event_id,
            "platform_message_ref": message_ref,
            "event_kind": channel_kind + ".message",
            "channel_kind": "GROUP" if channel_kind == "group" else "PRIVATE",
            "channel_ref": channel_ref,
            "conversation_ref": conversation_ref,
            "author_subject_ref": author_subject_ref,
            "raw": raw["payload"],
            "normalized": normalized_saved["payload"],
            "author": author_saved["payload"],
            "received_at": received_at.isoformat(),
            "classification_deadline": (received_at + timedelta(minutes=2)).isoformat(),
            "sent_at": sent_at,
        }
        await ctx.services.call(
            "kernel.uow.onebot.ingress",
            {
                "protocol": "forest.onebot-ingress-plan/2",
                "operation_id": "onebot-admit:" + event_hash,
                "binding_ref": binding["binding_ref"],
                "binding_epoch": binding["binding_epoch"],
                "connection_epoch": frame["connection_epoch"],
                "client_incarnation": frame["client_incarnation"],
                "adapter_generation_id": frame["adapter_generation_id"],
                "transport_digest": frame["transport_digest"],
                "event": event,
                "memory_capture": memory_capture,
                "media_capture": media_capture,
                "classification": _plain(classification),
                "shell_command": _plain(shell),
            },
            operation_id=ctx.operations.stable("ingress-uow"),
            timeout=30,
        )
        return {
            "protocol": "forest.onebot-frame-result/2",
            "frame_id": frame["frame_id"],
            "status": "ADMITTED",
            "event_id": event_id,
        }
