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

    @service(
        "adapter.onebot.groups.page", semantics="read", contract="1.0",
        input_schema="schemas/groups-page-input.json", output_schema="schemas/groups-page-output.json",
    )
    async def groups_page(self, payload, ctx):
        accounts = [{"binding_ref": row["binding_ref"], "self_id": row["self_id"]}
                    for row in self.bindings.values() if row["enabled"]]
        binding_ref = payload["binding_ref"]
        if binding_ref is None:
            return {"accounts": accounts, "binding_ref": None, "items": [],
                    "total": 0, "next_after_group_id": None}
        binding = self._binding(binding_ref)
        page = await ctx.services.call(
            "adapter.onebot.transport.groups-page",
            {**_transport_identity(ctx, binding), "query": payload["query"],
             "after_group_id": payload["after_group_id"]},
            timeout=11, contract_major=1,
            operation_id=ctx.operations.stable("transport-groups-page"),
        )
        return {"accounts": accounts, "binding_ref": binding_ref, **_plain(page)}

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

    @service("adapter.onebot.api.query", semantics="read", input_schema="schemas/api-query-input.json",
             output_schema="schemas/api-query-output.json")
    async def api_query(self, payload, ctx):
        binding = self._binding(payload["binding_ref"])
        return await ctx.services.call(
            "adapter.onebot.transport.api-query", {**_plain(payload), **_transport_identity(ctx, binding)},
            operation_id=ctx.operations.stable("onebot-api-query"), timeout=12,
        )

    @service("adapter.onebot.api.command", semantics="at-most-once", input_schema="schemas/api-command-input.json",
             output_schema="schemas/api-command-output.json")
    async def api_command(self, payload, ctx):
        binding = self._binding(payload["binding_ref"])
        return await ctx.services.call(
            "adapter.onebot.transport.api-command", {**_plain(payload), **_transport_identity(ctx, binding)},
            operation_id=ctx.operations.stable("onebot-api-command"), timeout=12,
        )

    @service("adapter.onebot.delivery.prepare", semantics="idempotent")
    async def prepare_delivery(self, payload, ctx):
        value = _plain(payload)
        binding = self._binding(value["binding_ref"])
        try:
            return await self._prepare_delivery(value, binding, ctx)
        except ForestError as error:
            if error.code not in {
                "ONEBOT_INVALID", "ONEBOT_MEDIA_UNAVAILABLE", "ONEBOT_MEDIA_SIZE_EXCEEDED",
                "ONEBOT_MEDIA_REFERENCE_INVALID", "PAYLOAD_REVOKED", "PAYLOAD_DIGEST_MISMATCH",
            }:
                raise
            return {"protocol": "forest.onebot-delivery-prepared/3", **_transport_identity(ctx, binding),
                    "status": "REJECTED", "prepared": None, "error_code": error.code}

    async def _prepare_delivery(self, value, binding, ctx):
        prepared = await ctx.services.call(
            "onebot.codec.prepare-send",
            {
                "channel_kind": value["channel_kind"].lower(),
                "channel_id": value["channel_ref"],
                "segments": value["parts"],
            },
            operation_id=ctx.operations.stable("prepare-wire"),
            contract_major=2,
        )
        template = _plain(prepared)
        images = [part for part in template["body"]["message"] if part["type"] == "image"]
        if images:
            references = [part["data"]["file"]["media_reference"] for part in images]
            resolved = await ctx.services.call(
                "media.context.resolve", {"items": references},
                operation_id=ctx.operations.stable("resolve-output-images"), timeout=15,
            )
            assets = {(item["media_ref"], item["media_revision"]): item for item in resolved["assets"]}
            for part, reference in zip(images, references, strict=True):
                asset = assets.get((reference["media_ref"], reference["media_revision"]))
                if asset is None:
                    raise ForestError("ONEBOT_MEDIA_UNAVAILABLE", "Output image is not an available managed asset", 409)
                part["data"]["file"] = {"payload": _plain(asset["payload"])}
        wire = await ctx.services.call(
            "adapter.onebot.wire.materialize",
            {**template, "wire_owner_id": value["wire_owner_id"]},
            operation_id=ctx.operations.stable("materialize-wire"),
        )
        return {
            "protocol": "forest.onebot-delivery-prepared/3",
            "status": "READY", "error_code": None,
            **_transport_identity(ctx, binding),
            "prepared": _plain(wire),
        }

    @service("adapter.onebot.delivery.dispatch", semantics="at-most-once")
    async def dispatch_delivery(self, payload, ctx):
        return await ctx.services.call(
            "adapter.onebot.transport.send",
            _plain(payload),
            operation_id=ctx.operations.stable("transport-send"),
            timeout=payload["timeout_seconds"] + 5,
            contract_major=3,
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
        context_parts, memory_segments, images = [], [], []
        for index, part in enumerate(normalized["segments"]):
            kind = part["type"]
            if kind == "image":
                image_index = len(images) + 1
                media_ref = "media:" + hashlib.sha256(
                    (event_id + "\0" + str(image_index)).encode()
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
                        "part_id": "part:image:" + str(image_index),
                        "part_index": image_index,
                        "media_ref": media_ref,
                        "locator": {
                            "kind": "onebot-source",
                            "uri": locator.get("uri"),
                            "platform_file": locator.get("platform_file"),
                        },
                    }
                )
                memory_segments.append({
                    "type": "image", "media_ref": media_ref, "media_revision": 1,
                    "source_unit_id": "onebot-unit:" + event_hash,
                    "part_id": "part:image:" + str(image_index),
                })
                continue
            context_parts.append(part)
            memory_segments.append(
                {"type": "unsupported", "platform_type": part["platform_type"]}
                if kind == "unsupported" else part
            )
        cq_message = await ctx.services.call(
            "onebot.codec.memory-message", {"segments": memory_segments},
            operation_id=ctx.operations.stable("memory-message"),
        )
        normalized_saved = await self._write_payload(
            ctx,
            owner_kind="onebot.normalized-event",
            owner_id=event_id,
            media_type="application/json",
            content=_canonical({
                "parts": [
                    {"type": "text", "text": cq_message["text"]},
                    *(part for part in context_parts if part["type"] == "image"),
                ],
                "platform_message_id": message_ref,
                "output_format": {
                    "syntax": "cq-code",
                    "instructions": "Your final assistant text may use [CQ:at,qq=<user_id>], [CQ:reply,id=<platform_message_id>], [CQ:face,id=<face_id>], and [CQ:image,file=<media_ref>,revision=<media_revision>,source_unit_id=<source_unit_id>,part_id=<part_id>]. Use only supplied managed image references, not URLs, paths or base64. To print literal CQ, escape & as &amp;, [ as &#91;, ] as &#93;. In CQ parameter values also escape comma as &#44;. Other unsupported CQ is printed literally.",
                },
                "receiving_account": {
                    "bot_id": ctx.identity["bot_id"],
                    "binding_ref": binding["binding_ref"],
                    "platform": normalized["platform"],
                    "external_user_id": binding["self_id"],
                },
            }),
            source=source,
        )
        memory_texts = [(0, cq_message["text"])]
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
            memory_capture = {**capture, "parts": [
                {**part, "decoded_pixels": None} for part in memory_parts
            ]}
            media_capture = None
        projected = {**normalized, "segments": context_parts}
        gate = await ctx.services.call(
            "shell.gate.inspect",
            {"protocol": "forest.shell-gate-request/1", "normalized": projected},
            operation_id=ctx.operations.stable("shell-gate"),
        )
        if (
            not isinstance(gate, dict)
            or gate.get("protocol") != "forest.shell-gate/1"
            or type(gate.get("claimed")) is not bool
        ):
            raise ForestError("SHELL_GATE_RESULT_INVALID", "Shell gate result is invalid", 503)
        shell = None
        core_rescue = None
        if gate["claimed"]:
            core_rescue = await ctx.services.call(
                "core.rescue.inspect",
                {"protocol": "forest.core-rescue-inspect-request/1", "gate": gate},
                operation_id=ctx.operations.stable("core-rescue-inspect"),
            )
            if (
                not isinstance(core_rescue, dict)
                or core_rescue.get("protocol") != "forest.core-rescue-inspection/1"
                or type(core_rescue.get("claimed")) is not bool
            ):
                raise ForestError(
                    "CORE_RESCUE_RESULT_INVALID", "Core rescue inspection is invalid", 503,
                )
            if not core_rescue["claimed"]:
                shell = await ctx.services.call(
                    "shell.ingress.inspect",
                    {
                        "gate": gate,
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
                if not isinstance(shell, dict) or shell.get("protocol") != "forest.shell-ingress/2":
                    raise ForestError(
                        "SHELL_INGRESS_RESULT_INVALID", "Claimed Shell ingress is invalid", 503,
                    )
        classification = None
        if not gate["claimed"]:
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
        conversation_event_id = "onebot:" + event_id
        plan = {
            "adapter": {
                "operation_id": "onebot-admit:" + event_hash,
                "binding_ref": binding["binding_ref"],
                "binding_epoch": binding["binding_epoch"],
                "client_incarnation": frame["client_incarnation"],
                "event": event,
            },
            "conversation": {
                "conversation_id": conversation_ref,
                "source_component_id": "adapter.onebot",
                "binding_ref": binding["binding_ref"],
                "platform": "onebot-v11",
                "channel_kind": event["channel_kind"],
                "channel_ref": channel_ref,
                "thread_ref": None,
                "event_id": conversation_event_id,
                "source_event_ref": event_id,
                "event_kind": event["event_kind"],
                "platform_message_ref": message_ref,
                "author_subject_ref": author_subject_ref,
                "content": normalized_saved["payload"],
                "content_byte_length": normalized_saved["byte_length"],
                "author": author_saved["payload"],
                "received_at": event["received_at"],
                "classification_deadline": event["classification_deadline"],
                "sent_at": sent_at,
                "context_disposition": "MATERIALIZE",
            },
        }
        plan["position"] = {
            "identity": identity,
            "source_unit_id": capture["source_unit_id"],
            "conversation_ref": conversation_ref,
            "event_ref": conversation_event_id,
            "conversation_step_id": "conversation",
        }
        if memory_capture is not None:
            plan["payload_epoch"] = {"protocol": "forest.payload-epoch-capture/1"}
            plan["memory"] = {
                "capture": {
                    **memory_capture,
                    "author_subject_ref": author_subject_ref,
                    "occurrence": {
                        "occurrence_id": "onebot-memory-occurrence:" + event_hash,
                        "binding_ref": binding["binding_ref"],
                        "conversation_ref": conversation_ref,
                        "event_ref": conversation_event_id,
                        "ingress_seq": None,
                        "observed_at": event["received_at"],
                    },
                },
                "conversation_step_id": "conversation",
                "deletion_epoch_step_id": "payload_epoch",
            }
        else:
            plan["media"] = {
                "capture": {
                    **media_capture,
                    "author_subject_ref": author_subject_ref,
                    "binding_ref": binding["binding_ref"],
                    "conversation_ref": conversation_ref,
                    "event_ref": conversation_event_id,
                    "observed_at": event["received_at"],
                },
                "conversation_step_id": "conversation",
            }
        if not gate["claimed"]:
            is_trigger = classification["decision"] == "TRIGGER"
            plan["classification"] = {
                "conversation_id": conversation_ref,
                "event_id": conversation_event_id,
                "decision": classification["decision"],
                "classifier_component_id": classification["classifier_component_id"],
                "classifier_generation_id": classification["classifier_generation_id"],
                "rule_revision": classification["rule_revision"],
                "reason_code": classification["reason_code"],
                "route_ref": None,
                "trigger_id": "onebot-trigger:" + event_hash if is_trigger else None,
                "principal_ref": author_subject_ref if is_trigger else None,
            }
            plan_name = (
                "adapter.ingress-data-memory"
                if memory_capture is not None
                else "adapter.ingress-data-media"
            )
        else:
            plan["gate"] = {"self_id": binding["self_id"], "inspection": gate}
            plan["shell_origin"] = {"raw_frame": json.loads(raw_body.decode("utf-8"))}
            if core_rescue["claimed"]:
                plan["rescue"] = {
                    "protocol": "forest.core-rescue-admit/1",
                    "binding_ref": binding["binding_ref"],
                    "binding_epoch": binding["binding_epoch"],
                    "connection_epoch": frame["connection_epoch"],
                    "client_incarnation": frame["client_incarnation"],
                    "adapter_generation_id": frame["adapter_generation_id"],
                    "executor_fence_digest": frame["transport_digest"],
                    "conversation_ref": conversation_ref,
                    "event_ref": event_id,
                    "principal_ref": author_subject_ref,
                    "group_ref": channel_ref,
                    "received_at": event["received_at"],
                    "admission_deadline": event["classification_deadline"],
                    "inspection": core_rescue,
                }
                plan["rescue_classification"] = {
                    "conversation_id": conversation_ref,
                    "event_id": conversation_event_id,
                    "conversation_step_id": "conversation",
                    "rescue_step_id": "rescue",
                }
                plan_name = (
                    "adapter.ingress-rescue-memory"
                    if memory_capture is not None
                    else "adapter.ingress-rescue-media"
                )
            else:
                plan["shell"] = {
                    "protocol": "forest.shell-admission/2",
                    "request_id": shell["request_id"],
                    "shell_generation_id": shell["shell_generation_id"],
                    "binding_ref": binding["binding_ref"],
                    "binding_epoch": binding["binding_epoch"],
                    "connection_epoch": frame["connection_epoch"],
                    "client_incarnation": frame["client_incarnation"],
                    "adapter_generation_id": frame["adapter_generation_id"],
                    "executor_fence_digest": frame["transport_digest"],
                    "conversation_ref": conversation_ref,
                    "event_ref": event_id,
                    "principal_ref": author_subject_ref,
                    "command_id": shell["command_id"],
                    "command_version": shell["command_version"],
                    "arguments": shell["arguments"],
                    "rejection": shell["rejection"],
                    "received_at": event["received_at"],
                    "admission_deadline": event["classification_deadline"],
                }
                plan["shell_classification"] = {
                    "conversation_id": conversation_ref,
                    "event_id": conversation_event_id,
                    "conversation_step_id": "conversation",
                    "shell_step_id": "shell",
                }
                plan_name = (
                    "adapter.ingress-shell-memory"
                    if memory_capture is not None
                    else "adapter.ingress-shell-media"
                )
        await ctx.uow.call(
            plan_name,
            plan,
            operation_id=ctx.operations.stable("ingress-uow"),
            timeout=30,
        )
        return {
            "protocol": "forest.onebot-frame-result/2",
            "frame_id": frame["frame_id"],
            "status": "ADMITTED",
            "event_id": event_id,
        }
