"""Durable, ordered group input and fenced conversation ownership.

Inbox reception, classification and context consumption are separate facts. The
host calls select_input only at a closed model/tool boundary. No method here
calls a model or summarizes content: compaction retains immutable source refs.
"""

from __future__ import annotations

import hashlib
import json
import unicodedata
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any
from uuid import uuid4

from psycopg.types.json import Jsonb

from forest.errors import ForestError
from forest.packages import digest_json
from forest.shell_authority import P0_PRIORITY_POLICY, qualify_priority
from forest.shell_commands import parse_command
from forest.shell_ingress import ShellPriorityProof
from forest.storage.bot import BotStore, HostLease
from forest.storage.business_command_payloads import BusinessCommandPayloadStore
from forest.storage.command_limits import CommandLimitStore
from forest.storage.command_policies import CommandPolicyStore
from forest.storage.input_payloads import InputContentStore, identity
from forest.storage.message_handler_chains import MessageHandlerChainStore
from forest.storage.shell_payloads_v12 import (
    UNAVAILABLE as SHELL_UNAVAILABLE,
)
from forest.storage.shell_payloads_v12 import (
    ShellPayloadStore,
    arguments_metadata,
    delivery_metadata,
    outcome_metadata,
)


@dataclass(frozen=True)
class ActorLease:
    host: HostLease
    conversation_id: str
    binding_id: str
    binding_epoch: int
    actor_id: str
    actor_epoch: int
    expires_at: datetime
    policy_revision: int
    maintenance_epoch: int
    storage_generation: int


@dataclass(frozen=True)
class StagedPlatformInput:
    kind: str
    event: dict[str, Any] | None = None
    origin_payload_id: str | None = None
    receipt: dict[str, Any] | None = None


def _text(value: Any, name: str, maximum: int = 256, *, empty: bool = False) -> str:
    if not isinstance(value, str) or len(value) > maximum or (not value and not empty):
        raise ForestError("INVALID_MESSAGE", f"Invalid {name}", 422)
    return value


def _json(value: Any, maximum: int = 262144) -> Any:
    try:
        text = json.dumps(value, ensure_ascii=False, allow_nan=False, separators=(",", ":"))
        if len(text.encode()) > maximum:
            raise ValueError("size")
        return json.loads(text)
    except (ValueError, TypeError, RecursionError) as exc:
        raise ForestError("INVALID_MESSAGE", "Message exceeds bounded JSON limits", 422) from exc


def _tokens(value: Any) -> int:
    # Conservative local upper bound. CJK and image placeholders remain charged;
    # the eventual Provider must use its own exact request tokenizer/budget.
    return max(1, len(json.dumps(value, ensure_ascii=False).encode("utf-8")))


def _shell_command(value: dict) -> str | None:
    """Compatibility entry point for the versioned typed control parser."""
    parsed = parse_command(value)
    return parsed.name if parsed else None


class MessageStore:
    def __init__(self, store: BotStore, bindings=None, *, context_token_budget: int = 32768):
        if bindings is None:
            from forest.storage.bindings import BindingStore

            bindings = BindingStore(store)
        if type(context_token_budget) is not int or not 512 <= context_token_budget <= 1048576:
            raise ValueError("Context budget must be between 512 and 1048576")
        self.store = store
        self.bindings = bindings
        self.shell_payloads = ShellPayloadStore(store)
        self.business_payloads = BusinessCommandPayloadStore(store)
        self.handler_chains = MessageHandlerChainStore(store, bindings)
        self.context_token_budget = context_token_budget

    @staticmethod
    def _business_token(value: dict) -> tuple[str, str] | None:
        """Read one token from one real text segment; other parts never join it."""
        for part in value["segments"]:
            if part["type"] != "text":
                continue
            text = part["text"].lstrip()
            if not text:
                continue
            pieces = text.split(maxsplit=1)
            token = unicodedata.normalize("NFC", pieces[0])
            if not token.startswith("/"):
                return None
            return token, pieces[1].lstrip() if len(pieces) == 2 else ""
        return None

    async def _business_plan_in(self, conn, binding, value) -> dict | None:
        token = self._business_token(value)
        if token is None or value["author"].get("is_self"):
            return None
        name, argument_text = token
        await conn.execute(
            "INSERT INTO public.bot_command_clocks(bot_id) VALUES (%s) ON CONFLICT DO NOTHING",
            (self.store.bot_id,),
        )
        clock = await (
            await conn.execute(
                "SELECT * FROM public.bot_command_clocks WHERE bot_id=%s FOR SHARE",
                (self.store.bot_id,),
            )
        ).fetchone()
        command = await (
            await conn.execute(
                "SELECT h.*,g.package_digest FROM public.bot_command_heads h "
                "JOIN public.bot_command_generations g ON "
                "(g.bot_id,g.generation_id)=(h.bot_id,h.generation_id) "
                "WHERE h.bot_id=%s AND h.name=%s AND h.active=true AND h.retired=false",
                (self.store.bot_id, name),
            )
        ).fetchone()
        if command is None:
            return None
        policy = await (
            await conn.execute(
                "SELECT r.* FROM public.bot_command_policy_heads p "
                "JOIN public.bot_command_policy_revisions r ON "
                "(r.bot_id,r.binding_id,r.command_id,r.revision)="
                "(p.bot_id,p.binding_id,p.command_id,p.revision) "
                "WHERE p.bot_id=%s AND p.binding_id=%s AND p.command_id=%s "
                "FOR SHARE OF p,r",
                (self.store.bot_id, binding["binding_id"], command["command_id"]),
            )
        ).fetchone()
        if policy is None or policy["policy_mode"] == "DISABLED":
            return None
        hydrated = await CommandPolicyStore(self.store)._hydrate_in(
            conn, policy, kind="REVISION", required=False
        )
        if hydrated["values_payload_state"] != "ACTIVE":
            return {"failed": True, "reason": "BUSINESS_COMMAND_POLICY_UNAVAILABLE"}
        values = hydrated["values"]
        author = value["author"]
        verified = (
            author.get("identity_origin") == "adapter_verified"
            and author.get("anonymous") is None
            and isinstance(author.get("external_user_id"), str)
        )
        binding_config_revision = None
        allowed = False
        if values["mode"] == "EVERYONE":
            allowed = verified
        elif values["mode"] == "USER_SET":
            allowed = verified and author["external_user_id"] in values["user_ids"]
        elif values["mode"] == "ADMINS":
            current = await self.bindings.current_config_in(
                conn,
                self.store.bot_id,
                binding["binding_id"],
                expected_binding_epoch=binding["binding_epoch"],
            )
            binding_config_revision = current["effective_revision"]
            allowed = verified and author["external_user_id"] in current["config"][
                "admin_user_ids"
            ]
        return {
            "failed": False,
            "allowed": allowed,
            "argument_text": argument_text,
            "registry_revision": clock["revision"],
            "command": command,
            "policy": policy,
            "binding_config_revision": binding_config_revision,
        }

    async def _put_notice_in(
        self,
        conn,
        *,
        event_id: str,
        request_id: str | None,
        conversation_id: str,
        ingress_seq: int,
        input_payload_id: str,
        kind: str,
        error_code: str,
    ) -> dict:
        notice_id = "conversation-notice:" + digest_json([event_id, kind])
        return await (
            await conn.execute(
                "INSERT INTO public.bot_conversation_notices("
                "bot_id,notice_id,event_id,request_id,conversation_id,ingress_seq,"
                "input_payload_id,kind,error_code) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s) "
                "RETURNING *",
                (
                    self.store.bot_id,
                    notice_id,
                    event_id,
                    request_id,
                    conversation_id,
                    ingress_seq,
                    input_payload_id,
                    kind,
                    error_code,
                ),
            )
        ).fetchone()

    async def _persist_business_request_in(
        self,
        conn,
        *,
        event_id: str,
        conversation_id: str,
        binding_id: str,
        ingress_seq: int,
        author: dict,
        input_payload_id: str,
        deletion_epoch: int,
        business_plan: dict,
    ) -> tuple[str, str, str]:
        request_id = "business-command:" + digest_json(event_id)
        command, policy = business_plan["command"], business_plan["policy"]
        arguments_ref, arguments_digest = await self.business_payloads.put_arguments_in(
            conn,
            request_id=request_id,
            command_id=command["command_id"],
            name=command["name"],
            argument_mode=command["argument_mode"],
            argument_text=business_plan["argument_text"],
            input_payload_id=input_payload_id,
            epoch=deletion_epoch,
        )
        status = "PENDING" if business_plan["allowed"] else "DENIED"
        terminal_reason = None if business_plan["allowed"] else "BUSINESS_COMMAND_DENIED"
        admission_limit_revision = None
        if business_plan["allowed"]:
            admission_limit_revision, terminal_reason = await CommandLimitStore(
                self.store
            ).admit_in(
                conn,
                binding_id=binding_id,
                principal_ref=author["principal_ref"],
                argument_bytes=len(business_plan["argument_text"].encode("utf-8")),
            )
            if terminal_reason is not None:
                status = "FAILED"
        request_fact = {
            "event_id": event_id,
            "conversation_id": conversation_id,
            "binding_id": binding_id,
            "ingress_seq": ingress_seq,
            "principal_ref": author["principal_ref"],
            "external_user_id": author["external_user_id"],
            "command_id": command["command_id"],
            "command_name": command["name"],
            "component_id": command["component_id"],
            "generation_id": command["generation_id"],
            "route_epoch": command["route_epoch"],
            "registry_revision": business_plan["registry_revision"],
            "command_spec_digest": command["spec_digest"],
            "handler": command["handler"],
            "permission": command["permission"],
            "argument_mode": command["argument_mode"],
            "input_schema": command["input_schema"],
            "schema_digest": command["schema_digest"],
            "command_policy_revision": policy["revision"],
            "policy_mode": policy["policy_mode"],
            "policy_payload_id": policy["values_payload_id"],
            "policy_payload_digest": policy["values_payload_digest"],
            "binding_config_revision": business_plan["binding_config_revision"],
            "admission_limit_revision": admission_limit_revision,
            "input_payload_id": input_payload_id,
            "arguments_payload_id": arguments_ref.payload_id,
            "arguments_payload_digest": arguments_ref.digest,
            "arguments_digest": arguments_digest,
            "status": status,
            "terminal_reason": terminal_reason,
        }
        request_digest = digest_json(request_fact)
        await conn.execute(
            "INSERT INTO public.bot_business_command_requests("
            "bot_id,request_id,event_id,conversation_id,binding_id,ingress_seq,"
            "principal_ref,external_user_id,command_id,command_name,component_id,"
            "generation_id,route_epoch,registry_revision,command_spec_digest,handler,"
            "permission,argument_mode,input_schema,schema_digest,command_policy_revision,"
            "policy_mode,policy_payload_id,policy_payload_digest,binding_config_revision,"
            "admission_limit_revision,"
            "input_payload_id,arguments_payload_id,arguments_payload_digest,arguments_digest,"
            "request_digest,status,terminal_reason,completed_at) VALUES ("
            "%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,"
            "%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,"
            "CASE WHEN %s IN ('DENIED','FAILED') THEN clock_timestamp() END)",
            (
                self.store.bot_id,
                request_id,
                *(request_fact[key] for key in (
                    "event_id", "conversation_id", "binding_id", "ingress_seq",
                    "principal_ref", "external_user_id", "command_id", "command_name",
                    "component_id", "generation_id", "route_epoch", "registry_revision",
                    "command_spec_digest", "handler", "permission", "argument_mode",
                    "input_schema", "schema_digest", "command_policy_revision", "policy_mode",
                    "policy_payload_id", "policy_payload_digest", "binding_config_revision",
                    "admission_limit_revision",
                    "input_payload_id", "arguments_payload_id", "arguments_payload_digest",
                    "arguments_digest",
                )),
                request_digest,
                status,
                terminal_reason,
                status,
            ),
        )
        if status in {"DENIED", "FAILED"}:
            await self._put_notice_in(
                conn,
                event_id=event_id,
                request_id=request_id,
                conversation_id=conversation_id,
                ingress_seq=ingress_seq,
                input_payload_id=input_payload_id,
                kind=(
                    "BUSINESS_COMMAND_DENIED"
                    if status == "DENIED"
                    else "BUSINESS_COMMAND_FAILED"
                ),
                error_code=terminal_reason,
            )
        return request_id, request_digest, status

    def _memory_sources(self):
        from forest.storage.memory_sources import MemorySourceStore

        return MemorySourceStore(self)

    async def _register_memory_input(self, conn, conversation, event_id, *, reconstructed=False, body=None):
        inbox = await (await conn.execute(
            "SELECT * FROM public.bot_message_inbox WHERE bot_id=%s AND event_id=%s",
            (self.store.bot_id, event_id),
        )).fetchone()
        return await self._memory_sources().register_input_in(conn, conversation, inbox,
            reconstructed=reconstructed, body=body)

    def _normalized(self, raw: dict) -> dict:
        value = _json(raw)
        if not isinstance(value, dict):
            raise ForestError("INVALID_MESSAGE", "Message must be an object", 422)
        if value.get("platform") != "onebot-v11" or value.get("channel_kind") not in {
            "group", "private"
        }:
            raise ForestError("INVALID_MESSAGE", "Unsupported message identity", 422)
        for key in ("self_id", "channel_id", "platform_message_id"):
            _text(value.get(key), key)
        value["thread_id"] = _text(value.get("thread_id", ""), "thread_id", empty=True)
        try:
            sent = datetime.fromisoformat(value["sent_at"].replace("Z", "+00:00"))
            if sent.tzinfo is None:
                raise ValueError("timezone required")
        except (KeyError, ValueError, TypeError, AttributeError) as exc:
            raise ForestError("INVALID_MESSAGE", "sent_at must be an aware ISO timestamp", 422) from exc
        author = value.get("author")
        if not isinstance(author, dict):
            raise ForestError("INVALID_MESSAGE", "Message author is required", 422)
        user_id = _text(author.get("external_user_id"), "external_user_id")
        _text(author.get("display_name", ""), "display_name", 512, empty=True)
        if type(author.get("is_self")) is not bool:
            raise ForestError("INVALID_MESSAGE", "is_self must be a trusted Boolean", 422)
        if author["is_self"] != (user_id == value["self_id"]):
            raise ForestError("INVALID_MESSAGE", "Self identity does not match the binding", 422)
        # The trusted host issues this identity, never a message's text/nickname.
        anonymous = author.get("anonymous")
        if anonymous is not None:
            if not isinstance(anonymous, dict):
                raise ForestError("INVALID_MESSAGE", "Malformed anonymous identity", 422)
            # QQ's shared anonymous user ID is never a normal account principal.
            # An absent pseudonym ID is scoped to this event, not merged by name.
            author["principal_ref"] = "anonymous:" + digest_json([
                self.store.bot_id, value["platform"], value["self_id"], value["channel_id"],
                anonymous.get("id") or value["platform_message_id"],
            ])
            author["identity_origin"] = "adapter_anonymous"
        else:
            author["principal_ref"] = "principal:" + digest_json(
                [self.store.bot_id, value["platform"], user_id]
            )
            author["identity_origin"] = "adapter_verified"
        parts = value.get("segments")
        if not isinstance(parts, list) or len(parts) > 256:
            raise ForestError("INVALID_MESSAGE", "Message needs 0 to 256 content parts", 422)
        if not parts:
            # Empty platform messages still carry a real source, author and
            # ingress position. Normalize representation, not trigger semantics.
            parts = value["segments"] = [{"type": "text", "text": ""}]
        for part in parts:
            if not isinstance(part, dict):
                raise ForestError("INVALID_MESSAGE", "Malformed content part", 422)
            kind = part.get("type")
            if kind == "text":
                _text(part.get("text"), "text", 131072, empty=True)
            elif kind == "image":
                if not isinstance(part.get("media_ref"), dict):
                    raise ForestError("INVALID_MESSAGE", "Image needs a typed source reference", 422)
            elif kind == "mention":
                _text(part.get("external_user_id"), "mention")
            elif kind == "reply":
                _text(part.get("platform_message_id"), "reply")
            elif kind == "unsupported":
                _text(part.get("platform_type"), "platform_type")
            else:
                raise ForestError("INVALID_MESSAGE", "Unsupported content part", 422)
        return value

    async def _conversation(self, conn, conversation_id: str) -> dict:
        row = await (await conn.execute(
            "SELECT * FROM public.bot_conversations WHERE bot_id=%s AND conversation_id=%s FOR UPDATE",
            (self.store.bot_id, conversation_id),
        )).fetchone()
        if row is None:
            raise ForestError("CONVERSATION_NOT_FOUND", "Conversation does not exist", 404)
        return row

    async def _locate(self, conn, conversation_id: str) -> dict:
        row = await (await conn.execute(
            "SELECT binding_id FROM public.bot_conversations WHERE bot_id=%s AND conversation_id=%s",
            (self.store.bot_id, conversation_id),
        )).fetchone()
        if row is None:
            raise ForestError("CONVERSATION_NOT_FOUND", "Conversation does not exist", 404)
        return row

    def platform_source_event_id(self, value):
        group_source = "source:" + digest_json([
            self.store.bot_id, value["platform"], "qq-global", value["channel_kind"],
            value["channel_id"], 1,
        ])
        return "source-event:" + digest_json([
            group_source, value["self_id"], value["platform_message_id"],
        ])

    def _platform_ids(self, binding_id, value):
        conversation_id = "conversation:" + digest_json([
            self.store.bot_id,
            binding_id,
            value["platform"],
            value["channel_kind"],
            value["channel_id"],
            value["thread_id"],
        ])
        group_source_id = "source:" + digest_json([
            self.store.bot_id,
            value["platform"],
            "qq-global",
            value["channel_kind"],
            value["channel_id"],
            1,
        ])
        source_event_id = self.platform_source_event_id(value)
        event_id = "event:" + digest_json([conversation_id, value["platform_message_id"]])
        occurrence_id = "source-occurrence:" + digest_json(
            [self.store.bot_id, "INPUT", event_id]
        )
        return {
            "conversation_id": conversation_id,
            "group_source_id": group_source_id,
            "source_event_id": source_event_id,
            "source_unit_id": self._memory_sources().unit_id(source_event_id),
            "event_id": event_id,
            "occurrence_id": occurrence_id,
            "input_digest": digest_json(value),
        }

    async def _suppressed_receipt_in(self, conn, binding, value, identifiers, basis):
        receipt_id = "suppressed-input:" + digest_json(
            [self.store.bot_id, "SUPPRESSED_OCCURRENCE", identifiers["occurrence_id"]]
        )
        observed_at = datetime.fromisoformat(value["sent_at"].replace("Z", "+00:00"))
        fields = {
            "receipt_id": receipt_id,
            "event_id": identifiers["event_id"],
            "occurrence_id": identifiers["occurrence_id"],
            "source_event_id": identifiers["source_event_id"],
            "source_unit_id": identifiers["source_unit_id"],
            "conversation_id": identifiers["conversation_id"],
            "binding_id": binding["binding_id"],
            "platform": value["platform"],
            "channel_kind": value["channel_kind"],
            "channel_id": value["channel_id"],
            "thread_id": value["thread_id"],
            "platform_message_id": value["platform_message_id"],
            "input_digest": identifiers["input_digest"],
            "observed_at": observed_at,
        }
        inserted = await (
            await conn.execute(
                "INSERT INTO public.bot_suppressed_input_occurrences("
                "bot_id,receipt_id,event_id,occurrence_id,source_event_id,source_unit_id,"
                "conversation_id,binding_id,first_binding_epoch,platform,channel_kind,channel_id,"
                "thread_id,platform_message_id,input_digest,observed_at,"
                "suppression_source_unit_id,suppression_part_id,suppression_operation_id,"
                "suppression_deletion_epoch) "
                "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) "
                "ON CONFLICT DO NOTHING RETURNING *",
                (
                    self.store.bot_id,
                    receipt_id,
                    identifiers["event_id"],
                    identifiers["occurrence_id"],
                    identifiers["source_event_id"],
                    identifiers["source_unit_id"],
                    identifiers["conversation_id"],
                    binding["binding_id"],
                    binding["binding_epoch"],
                    value["platform"],
                    value["channel_kind"],
                    value["channel_id"],
                    value["thread_id"],
                    value["platform_message_id"],
                    identifiers["input_digest"],
                    observed_at,
                    basis["source_unit_id"],
                    basis["part_id"],
                    basis["operation_id"],
                    basis["deletion_epoch"],
                ),
            )
        ).fetchone()
        row = inserted or await (
            await conn.execute(
                "SELECT * FROM public.bot_suppressed_input_occurrences "
                "WHERE bot_id=%s AND event_id=%s",
                (self.store.bot_id, identifiers["event_id"]),
            )
        ).fetchone()
        if row is None or any(row[key] != expected for key, expected in fields.items()):
            raise ForestError("INGEST_CONFLICT", "Platform message identity was reused", 409)
        return {
            "kind": "SUPPRESSED_OCCURRENCE",
            "receipt_id": row["receipt_id"],
            "event_id": row["event_id"],
            "occurrence_id": row["occurrence_id"],
            "duplicate": inserted is None,
            "suppression_deletion_epoch": row["suppression_deletion_epoch"],
        }

    async def stage_platform_input(self, lease, binding_id, body, *, expected_binding_epoch):
        """Bind authenticated platform bytes to their real source before Worker IPC.

        The host's fixed OneBot parser owns identity and accepted message data.
        A reloadable codec must reproduce that complete projection exactly.
        """
        from forest.adapters.onebot import parse_event
        from forest.storage.input_payloads import InputContentStore

        async with self.store.pool.connection() as conn, conn.transaction():
            binding = await self.bindings._guard(
                conn, lease, binding_id, expected_binding_epoch, data=False, require_receive=True,
            )
            parsed = parse_event(body, self_id=binding["self_id"])
            if parsed is None:
                await self.bindings._final_guard(conn, lease, binding, data=False, require_receive=True, require_config=True)
                return StagedPlatformInput("NON_MESSAGE")
            event = self._normalized(parsed.as_dict())
            identifiers = self._platform_ids(binding_id, event)
            existing = await (
                await conn.execute(
                    "SELECT * FROM public.bot_message_inbox WHERE bot_id=%s AND event_id=%s",
                    (self.store.bot_id, identifiers["event_id"]),
                )
            ).fetchone()
            if existing is not None:
                if existing["input_digest"] != identifiers["input_digest"]:
                    raise ForestError("INGEST_CONFLICT", "Platform message identity was reused", 409)
                conversation = await self._conversation(conn, identifiers["conversation_id"])
                if event["author"].get("is_self"):
                    await self._reconcile_exact_echo_in(
                        conn, lease, binding, conversation, identifiers["event_id"]
                    )
                await self.bindings._final_guard(
                    conn, lease, binding, data=False, require_receive=True
                )
                return StagedPlatformInput(
                    "DUPLICATE",
                    receipt={
                        "conversation_id": identifiers["conversation_id"],
                        "event_id": identifiers["event_id"],
                        "ingress_seq": existing["ingress_seq"],
                        "graph_digest": existing["graph_digest"],
                        "duplicate": True,
                        "command_id": (
                            "command:" + digest_json(identifiers["event_id"])
                            if parse_command(event) is not None
                            else None
                        ),
                        "business_request_id": (
                            "business-command:" + digest_json(identifiers["event_id"])
                            if existing["trigger_decision"] == "BUSINESS_COMMAND"
                            else None
                        ),
                        "trigger_decision": existing["trigger_decision"],
                    },
                )
            contents = InputContentStore(self.store)
            clock = await contents.payloads.lock_in(conn)
            source_ref = contents.source_ref(identifiers["source_event_id"])
            basis = await contents.payloads.suppression_basis_in(conn, (source_ref,))
            if basis is not None:
                receipt = await self._suppressed_receipt_in(
                    conn, binding, event, identifiers, basis
                )
                await self.bindings._final_guard(
                    conn, lease, binding, data=False, require_receive=True
                )
                return StagedPlatformInput("SUPPRESSED_OCCURRENCE", receipt=receipt)
            ref = await contents.payloads.put_bytes_in(
                conn, owner_kind="context.parts",
                owner_id="ingress:"
                + digest_json(
                    [identifiers["source_event_id"], hashlib.sha256(body).hexdigest()]
                ),
                value=body,
                sources=(source_ref,),
                expected_deletion_epoch=clock["deletion_epoch"],
            )
            await contents.payloads.require_available_in(
                conn, [ref.payload_id], expected_deletion_epoch=clock["deletion_epoch"],
            )
            await self.bindings._final_guard(conn, lease, binding, data=False, require_receive=True, require_config=True)
            return StagedPlatformInput("NEW", event=event, origin_payload_id=ref.payload_id)

    async def ingest(
        self, lease: HostLease, binding_id: str, event: dict, *,
        expected_binding_epoch: int | None = None,
        decision: str | None = None, rule_revision: str = "ingress-v1", reason: str = "",
        priority_proof: ShellPriorityProof | None = None,
    ) -> dict:
        value = self._normalized(event)
        if decision not in {None, "TRIGGER", "NO_TRIGGER", "CONTROL_COMMAND", "FAILED"}:
            raise ForestError("INVALID_CLASSIFICATION", "Unknown classification", 422)
        if value["author"]["is_self"] and decision == "TRIGGER":
            raise ForestError("SELF_TRIGGER", "A binding cannot trigger its own response", 422)
        parsed_command = parse_command(value)
        shell_command = parsed_command.name if parsed_command else None
        if priority_proof is not None and parsed_command is None:
            raise ForestError(
                "SHELL_PRIORITY_PROOF_INVALID",
                "Shell priority proof does not match this message",
                409,
            )
        if shell_command is not None:
            decision, rule_revision, reason = (
                "CONTROL_COMMAND", parsed_command.gate_version, "reserved_namespace"
            )
        if decision is not None:
            _text(rule_revision, "rule_revision")
            _text(reason, "reason", 1024, empty=True)
        fallback_decision = decision or "PENDING"
        fallback_rule_revision = rule_revision
        fallback_reason = reason
        identifiers = self._platform_ids(binding_id, value)
        conversation_id = identifiers["conversation_id"]
        group_source_id = identifiers["group_source_id"]
        source_event_id = identifiers["source_event_id"]
        input_digest = identifiers["input_digest"]
        event_id = identifiers["event_id"]
        event_bytes = len(
            json.dumps(
                value,
                ensure_ascii=False,
                allow_nan=False,
                sort_keys=True,
                separators=(",", ":"),
            ).encode("utf-8")
        )
        command_id = "command:" + digest_json(event_id) if shell_command is not None else None
        async with self.store.pool.connection() as conn, conn.transaction():
            binding = await self.bindings._guard(
                conn, lease, binding_id, expected_binding_epoch,
                data=False, require_receive=True,
            )
            if binding["self_id"] != value["self_id"] or binding["platform"] != value["platform"]:
                raise ForestError("BINDING_IDENTITY_MISMATCH", "Message belongs to another account", 403)
            handler_snapshot = None
            handler_limit_snapshot = None
            if parsed_command is None and value["channel_kind"] == "group":
                # Lifecycle publication locks the command registry before the
                # ordinary-handler registry. Establish the same order before
                # either policy family reads removable payload content.
                await conn.execute(
                    "INSERT INTO public.bot_command_clocks(bot_id) VALUES (%s) "
                    "ON CONFLICT DO NOTHING",
                    (self.store.bot_id,),
                )
                await conn.execute(
                    "SELECT revision FROM public.bot_command_clocks "
                    "WHERE bot_id=%s FOR SHARE",
                    (self.store.bot_id,),
                )
                handler_snapshot = await self.handler_chains.snapshot_in(conn, binding)
                if handler_snapshot[1]:
                    handler_limit_snapshot = await self.handler_chains.limits.snapshot_in(conn)
            business_plan = None
            business_classification_error = None
            if parsed_command is None:
                business_plan = await self._business_plan_in(conn, binding, value)
                if business_plan is not None:
                    if business_plan.get("failed"):
                        decision = None
                        business_classification_error = business_plan["reason"]
                    else:
                        decision = "BUSINESS_COMMAND"
                        rule_revision = "business-command-v1"
            priority_admission = None
            if parsed_command is not None:
                if type(priority_proof) is ShellPriorityProof:
                    expected = (
                        self.store.bot_id,
                        binding_id,
                        binding["binding_epoch"],
                        binding["effective_revision"],
                        binding["config_payload_digest"],
                        value["author"]["principal_ref"],
                        parsed_command.name,
                        parsed_command.gate_version,
                        parsed_command.priority,
                        P0_PRIORITY_POLICY,
                    )
                    actual = (
                        priority_proof.bot_id,
                        priority_proof.binding_id,
                        priority_proof.binding_epoch,
                        priority_proof.config_revision,
                        priority_proof.config_payload_digest,
                        priority_proof.principal_ref,
                        priority_proof.command_name,
                        priority_proof.gate_version,
                        priority_proof.requested_priority,
                        priority_proof.priority_policy,
                    )
                    valid_result = (
                        type(priority_proof.binding_epoch) is int
                        and type(priority_proof.config_revision) is int
                        and type(priority_proof.requested_priority) is int
                        and type(priority_proof.priority) is int
                        and 0 <= priority_proof.priority <= 2
                        and type(priority_proof.priority_eligible) is bool
                        and priority_proof.priority_eligible
                        == (
                            priority_proof.requested_priority == 0
                            and priority_proof.priority == 0
                        )
                        and priority_proof.priority
                        == (
                            0
                            if priority_proof.priority_eligible
                            and priority_proof.requested_priority == 0
                            else max(1, priority_proof.requested_priority)
                        )
                    )
                    if actual != expected or not valid_result:
                        raise ForestError(
                            "SHELL_PRIORITY_PROOF_INVALID",
                            "Shell priority proof no longer matches the Binding or command",
                            409,
                        )
                    priority_admission = priority_proof.persistence()
                elif priority_proof is not None:
                    raise ForestError(
                        "SHELL_PRIORITY_PROOF_INVALID",
                        "Shell priority proof has an invalid type",
                        409,
                    )
                else:
                    settings = await self.bindings.current_config_in(
                        conn,
                        self.store.bot_id,
                        binding_id,
                        expected_binding_epoch=binding["binding_epoch"],
                    )
                    priority_admission = {
                        **qualify_priority(
                            parsed_command.priority, value["author"], settings["config"]
                        ),
                        "priority_basis_binding_epoch": binding["binding_epoch"],
                        "priority_basis_config_revision": settings["effective_revision"],
                    }
            created = await (
                await conn.execute(
                    "INSERT INTO public.bot_conversations(bot_id,conversation_id,binding_id,platform,"
                    "channel_kind,channel_id,thread_id,group_source_id) "
                    "VALUES (%s,%s,%s,%s,%s,%s,%s,%s) ON CONFLICT DO NOTHING "
                    "RETURNING conversation_id",
                    (
                        self.store.bot_id,
                        conversation_id,
                        binding_id,
                        value["platform"],
                        value["channel_kind"],
                        value["channel_id"],
                        value["thread_id"],
                        group_source_id,
                    ),
                )
            ).fetchone()
            conversation = await self._conversation(conn, conversation_id)
            existing = await (await conn.execute(
                "SELECT * FROM public.bot_message_inbox WHERE bot_id=%s AND event_id=%s",
                (self.store.bot_id, event_id),
            )).fetchone()
            if existing:
                if existing["input_digest"] != input_digest:
                    raise ForestError("INGEST_CONFLICT", "Platform message identity was reused", 409)
                registered = await (
                    await conn.execute(
                        "SELECT 1 FROM public.bot_memory_source_occurrences "
                        "WHERE bot_id=%s AND event_id=%s",
                        (self.store.bot_id, event_id),
                    )
                ).fetchone()
                if registered is None:
                    contents = InputContentStore(self.store)
                    await contents.payloads.lock_in(conn)
                    basis = await contents.payloads.suppression_basis_in(
                        conn, (contents.source_ref(source_event_id),)
                    )
                else:
                    basis = None
                if registered is None and basis is None:
                    await self._register_memory_input(
                        conn, conversation, event_id, reconstructed=True
                    )
                if value["author"].get("is_self"):
                    await self._reconcile_exact_echo_in(conn, lease, binding, conversation, event_id)
                handler_chain = await (await conn.execute(
                    "SELECT chain_id FROM public.bot_message_handler_chains "
                    "WHERE bot_id=%s AND event_id=%s",
                    (self.store.bot_id, event_id),
                )).fetchone()
                await self.bindings._final_guard(conn, lease, binding, data=False, require_receive=True)
                return {"conversation_id": conversation_id, "event_id": event_id,
                        "ingress_seq": existing["ingress_seq"],
                        "graph_digest": existing["graph_digest"], "duplicate": True,
                        "command_id": command_id,
                        "business_request_id": (
                            "business-command:" + digest_json(event_id)
                            if existing["trigger_decision"] == "BUSINESS_COMMAND" else None
                        ),
                        "message_handler_chain_id": (
                            handler_chain["chain_id"] if handler_chain else None
                        ),
                        "trigger_decision": existing["trigger_decision"]}
            contents = InputContentStore(self.store)
            clock = await contents.payloads.lock_in(conn)
            basis = await contents.payloads.suppression_basis_in(
                conn, (contents.source_ref(source_event_id),)
            )
            if basis is not None:
                receipt = await self._suppressed_receipt_in(
                    conn, binding, value, identifiers, basis
                )
                if created is not None:
                    await conn.execute(
                        "DELETE FROM public.bot_conversations "
                        "WHERE bot_id=%s AND conversation_id=%s",
                        (self.store.bot_id, conversation_id),
                    )
                await self.bindings._final_guard(
                    conn, lease, binding, data=False, require_receive=True
                )
                return receipt
            if conversation["next_ingress_seq"] - conversation["context_applied_seq"] > 10000:
                raise ForestError("INBOX_BACKPRESSURE", "Conversation input backlog is full", 429)
            segments, media = [], []
            input_parts = value["segments"] if shell_command is None else [{
                "type": "text",
                "text": ("用户提交了一条管理命令，参数未进入对话上下文。" if shell_command != "unknown"
                         else "用户提交了一条未成功解析的管理命令。"),
                "command_id": command_id,
                "command_name": shell_command,
            }]
            for index, part in enumerate(input_parts):
                if part["type"] == "image":
                    media_ref = "media:" + digest_json([event_id, index])
                    media.append((media_ref, index, part["media_ref"]))
                    segments.append({"type": "image", "media_ref": media_ref,
                                     "fetch_status": "PENDING_FETCH"})
                elif part["type"] == "unsupported":
                    segments.append({"type": "unsupported", "platform_type": part["platform_type"]})
                else:
                    segments.append(part)
            seq = conversation["next_ingress_seq"]
            from forest.routing_graph import RouteGraphStore

            route_graph = await RouteGraphStore(self.store).freeze_in(conn)
            payload = await contents.put_input_in(conn, event_id=event_id, source_event_id=source_event_id,
                author=value["author"], segments=segments, expected_deletion_epoch=clock["deletion_epoch"])
            inbox = await (await conn.execute(
                "INSERT INTO public.bot_message_inbox(bot_id,conversation_id,event_id,ingress_seq,"
                "platform_message_id,source_event_id,input_digest,author,payload_id,sent_at,estimated_tokens,graph_digest,"
                "business_classification_pending,business_fallback_decision,"
                "business_fallback_rule_revision,business_fallback_reason,"
                "business_classification_next_attempt_at,business_classification_error_code) "
                "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,"
                "CASE WHEN %s THEN clock_timestamp() END,%s) RETURNING *",
                (self.store.bot_id, conversation_id, event_id, seq, value["platform_message_id"],
                 source_event_id, input_digest, Jsonb(identity(value["author"])), payload.payload_id,
                 value["sent_at"], _tokens({"author": value["author"], "segments": segments}),
                 route_graph["graph_digest"],
                 business_classification_error is not None,
                 fallback_decision if business_classification_error is not None else None,
                 fallback_rule_revision if business_classification_error is not None else None,
                 fallback_reason if business_classification_error is not None else None,
                 business_classification_error is not None,
                business_classification_error),
            )).fetchone()
            for media_ref, index, source in media:
                source_payload = await contents.put_media_source_in(conn, media_ref=media_ref,
                    source_event_id=source_event_id, part_index=index, source=source,
                    expected_deletion_epoch=clock["deletion_epoch"])
                await conn.execute(
                    "INSERT INTO public.bot_inbound_media_refs(bot_id,media_ref,event_id,part_index,source_payload_id) "
                    "VALUES (%s,%s,%s,%s,%s)",
                    (self.store.bot_id, media_ref, event_id, index, source_payload.payload_id),
                )
            if command_id is not None:
                arguments_ref = await self.shell_payloads.put_arguments_in(
                    conn,
                    command_id=command_id,
                    command_name=shell_command,
                    gate_version=parsed_command.gate_version,
                    arguments=parsed_command.payload,
                    parent=payload.payload_id,
                    epoch=clock["deletion_epoch"],
                )
                await conn.execute(
                    "INSERT INTO public.bot_shell_requests(bot_id,command_id,event_id,conversation_id,"
                    "binding_id,command_name,raw_digest,arguments,arguments_payload_id,"
                    "arguments_payload_digest,arguments_metadata,priority,gate_version,"
                    "requested_priority,priority_eligible,priority_policy,"
                    "priority_basis_binding_epoch,priority_basis_config_revision) "
                    "VALUES (%s,%s,%s,%s,%s,%s,%s,NULL,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
                    (self.store.bot_id, command_id, event_id, conversation_id, binding_id,
                     shell_command, digest_json(value["segments"]),
                     arguments_ref.payload_id, arguments_ref.digest,
                     Jsonb(arguments_metadata(
                         shell_command, parsed_command.gate_version, parsed_command.payload
                     )), priority_admission["priority"], parsed_command.gate_version,
                     priority_admission["requested_priority"],
                     priority_admission["priority_eligible"],
                     priority_admission["priority_policy"],
                     priority_admission["priority_basis_binding_epoch"],
                     priority_admission["priority_basis_config_revision"]),
                )
            business_request_id = None
            if business_plan is not None and not business_plan.get("failed"):
                business_request_id, request_digest, _ = (
                    await self._persist_business_request_in(
                        conn,
                        event_id=event_id,
                        conversation_id=conversation_id,
                        binding_id=binding_id,
                        ingress_seq=seq,
                        author=value["author"],
                        input_payload_id=payload.payload_id,
                        deletion_epoch=clock["deletion_epoch"],
                        business_plan=business_plan,
                    )
                )
                reason = business_request_id + ":" + request_digest
            handler_chain = None
            if (
                shell_command is None
                and value["channel_kind"] == "group"
                and (business_plan is None or business_plan.get("failed"))
            ):
                handler_chain = await self.handler_chains.freeze_in(
                    conn,
                    binding=binding,
                    inbox=inbox,
                    author=value["author"],
                    fallback_decision=fallback_decision,
                    fallback_rule_revision=fallback_rule_revision,
                    fallback_reason=fallback_reason,
                    event_bytes=event_bytes,
                    snapshot=handler_snapshot,
                    limit_snapshot=handler_limit_snapshot,
                    admission_exempt=business_classification_error is not None,
                )
                if handler_chain["status"] == "FAILED":
                    decision = "FAILED"
                    rule_revision = "message-handler-limit-v1"
                    reason = handler_chain["terminal_reason"]
                    await self._put_notice_in(
                        conn,
                        event_id=event_id,
                        request_id=None,
                        conversation_id=conversation_id,
                        ingress_seq=seq,
                        input_payload_id=payload.payload_id,
                        kind="MESSAGE_HANDLER_REJECTED",
                        error_code=reason,
                    )
            await conn.execute(
                "UPDATE public.bot_conversations SET next_ingress_seq=next_ingress_seq+1,"
                "mailbox_revision=mailbox_revision+1,updated_at=clock_timestamp() "
                "WHERE bot_id=%s AND conversation_id=%s",
                (self.store.bot_id, conversation_id),
            )
            if decision is not None and not (
                handler_chain is not None and handler_chain["status"] == "PENDING"
            ):
                await self._classify_in(conn, conversation_id, event_id, decision, rule_revision, reason)
            await self._register_memory_input(conn, conversation, event_id,
                body={"author": value["author"], "segments": segments})
            if value["author"].get("is_self"):
                await self._reconcile_exact_echo_in(conn, lease, binding, conversation, event_id)
            await self.store._event(conn, "conversation.input", event_id,
                                    {"conversation_id": conversation_id, "ingress_seq": seq})
            await self.bindings._final_guard(conn, lease, binding, data=False, require_receive=True, require_config=True)
            effective_decision = (
                "PENDING"
                if handler_chain is not None and handler_chain["status"] == "PENDING"
                else decision or "PENDING"
            )
            return {"conversation_id": conversation_id, "event_id": event_id,
                    "ingress_seq": seq, "duplicate": False, "command_id": command_id,
                    "graph_digest": route_graph["graph_digest"],
                    "business_request_id": business_request_id,
                    "message_handler_chain_id": (
                        handler_chain["chain_id"] if handler_chain is not None else None
                    ),
                    "trigger_decision": effective_decision}

    async def enqueue_job_trigger_in(self, conn, lease, occurrence, binding, definition):
        """Host-only, in the caller's authorized Job dispatch transaction.

        Content and identity come from an immutable definition. The internal
        event shares the conversation ordering, without claiming to be QQ input.
        JobStore commits its receipt and final delegation/lease fence together.
        """
        values, grant = definition["values"], definition["delegation"]
        conversation_id = values["conversation_id"]
        conversation = await self._conversation(conn, conversation_id)
        if (occurrence["bot_id"] != self.store.bot_id
                or occurrence["status"] != "CLAIMED"
                or occurrence["host_incarnation"] != lease.incarnation
                or conversation["binding_id"] != binding["binding_id"]
                or definition["job_id"] != occurrence["job_id"]
                or definition["revision"] != occurrence["job_revision"]):
            raise ForestError("STALE_JOB", "Internal trigger has no current Job claim", 409)
        event_id = "event:job:" + digest_json([self.store.bot_id, occurrence["occurrence_id"]])
        trigger_id = "trigger:" + digest_json(event_id)
        author = {
            "external_user_id": "job:" + occurrence["job_id"], "display_name": values["name"],
            "is_self": False, "identity_origin": "host_job",
            "principal_ref": "delegation:" + grant["delegation_id"],
            "job_id": occurrence["job_id"], "occurrence_id": occurrence["occurrence_id"],
            "delegation_id": grant["delegation_id"], "tool_capabilities": [],
        }
        segments = [{"type": "text", "text": values["prompt"]}]
        digest = digest_json([occurrence["occurrence_id"], definition["definition_digest"], author])
        existing = await (await conn.execute(
            "SELECT * FROM public.bot_message_inbox WHERE bot_id=%s AND event_id=%s",
            (self.store.bot_id, event_id),
        )).fetchone()
        if existing:
            if existing["input_digest"] != digest or existing["origin"] != "job":
                raise ForestError("INGEST_CONFLICT", "Internal Job identity was reused", 409)
            await self._register_memory_input(conn, conversation, event_id, reconstructed=True)
            return {"event_id": event_id, "trigger_id": trigger_id,
                    "conversation_id": conversation_id, "ingress_seq": existing["ingress_seq"],
                    "graph_digest": existing["graph_digest"]}
        if conversation["next_ingress_seq"] - conversation["context_applied_seq"] > 10000:
            raise ForestError("INBOX_BACKPRESSURE", "Conversation input backlog is full", 429)
        seq = conversation["next_ingress_seq"]
        from forest.routing_graph import RouteGraphStore

        route_graph = await RouteGraphStore(self.store).freeze_in(conn)
        contents = InputContentStore(self.store)
        clock = await contents.payloads.lock_in(conn)
        payload = await contents.put_input_in(conn, event_id=event_id,
            source_event_id="source:" + occurrence["occurrence_id"], author=author, segments=segments,
            expected_deletion_epoch=clock["deletion_epoch"])
        await conn.execute(
            "INSERT INTO public.bot_message_inbox(bot_id,conversation_id,event_id,ingress_seq,"
            "platform_message_id,source_event_id,input_digest,author,payload_id,sent_at,estimated_tokens,"
            "graph_digest,origin) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,'job')",
            (self.store.bot_id, conversation_id, event_id, seq,
             "internal:" + occurrence["occurrence_id"], "source:" + occurrence["occurrence_id"],
             digest, Jsonb(identity(author)), payload.payload_id, occurrence["scheduled_for"],
             _tokens({"author": author, "segments": segments}), route_graph["graph_digest"]),
        )
        await conn.execute(
            "UPDATE public.bot_conversations SET next_ingress_seq=next_ingress_seq+1,"
            "mailbox_revision=mailbox_revision+1,updated_at=clock_timestamp() "
            "WHERE bot_id=%s AND conversation_id=%s", (self.store.bot_id, conversation_id),
        )
        await self._classify_in(conn, conversation_id, event_id, "TRIGGER", "job-trigger-v1",
                                occurrence["occurrence_id"])
        await self._register_memory_input(conn, conversation, event_id,
            body={"author": author, "segments": segments})
        await self.store._event(conn, "job.trigger_enqueued", event_id,
                                {"occurrence_id": occurrence["occurrence_id"],
                                 "conversation_id": conversation_id, "ingress_seq": seq})
        return {"event_id": event_id, "trigger_id": trigger_id,
                "conversation_id": conversation_id, "ingress_seq": seq,
                "graph_digest": route_graph["graph_digest"]}

    async def _classify_in(self, conn, conversation_id, event_id, decision, rule_revision, reason):
        row = await (await conn.execute(
            "SELECT * FROM public.bot_message_inbox WHERE bot_id=%s AND event_id=%s FOR UPDATE",
            (self.store.bot_id, event_id),
        )).fetchone()
        if row is None or row["conversation_id"] != conversation_id:
            raise ForestError("EVENT_NOT_FOUND", "Message does not exist", 404)
        value_digest = digest_json([decision, rule_revision, reason])
        if row["trigger_decision"] != "PENDING":
            if row["classification_digest"] != value_digest:
                raise ForestError("CLASSIFICATION_CONFLICT", "Classification is already final", 409)
            return row
        if row["author"]["is_self"] and decision == "TRIGGER":
            raise ForestError("SELF_TRIGGER", "Self messages do not create conversation triggers", 422)
        await conn.execute(
            "UPDATE public.bot_message_inbox SET trigger_decision=%s,classification_digest=%s,"
            "rule_revision=%s,classification_reason=%s WHERE bot_id=%s AND event_id=%s",
            (decision, value_digest, rule_revision, reason, self.store.bot_id, event_id),
        )
        if decision == "TRIGGER":
            await conn.execute(
                "INSERT INTO public.bot_conversation_triggers(bot_id,trigger_id,conversation_id,"
                "event_id,ingress_seq,principal_ref) VALUES (%s,%s,%s,%s,%s,%s)",
                (self.store.bot_id, "trigger:" + digest_json(event_id), conversation_id,
                 event_id, row["ingress_seq"], row["author"]["principal_ref"]),
            )
        await conn.execute(
            "UPDATE public.bot_conversations c SET classified_through_seq=COALESCE("
            "(SELECT min(i.ingress_seq)-1 FROM public.bot_message_inbox i WHERE i.bot_id=c.bot_id "
            "AND i.conversation_id=c.conversation_id AND i.trigger_decision='PENDING'),"
            "c.next_ingress_seq-1),mailbox_revision=mailbox_revision+1,updated_at=clock_timestamp() "
            "WHERE c.bot_id=%s AND c.conversation_id=%s",
            (self.store.bot_id, conversation_id),
        )
        return {**row, "trigger_decision": decision}

    async def classify(self, lease, conversation_id, event_id, decision, *,
                       expected_binding_epoch=None, rule_revision="classification-v1", reason=""):
        if decision not in {"TRIGGER", "NO_TRIGGER", "CONTROL_COMMAND", "FAILED"}:
            raise ForestError("INVALID_CLASSIFICATION", "Unknown classification", 422)
        _text(rule_revision, "rule_revision")
        _text(reason, "reason", 1024, empty=True)
        async with self.store.pool.connection() as conn, conn.transaction():
            identity = await self._locate(conn, conversation_id)
            binding = await self.bindings._guard(
                conn, lease, identity["binding_id"], expected_binding_epoch, data=False,
                require_receive=True,
            )
            await self._conversation(conn, conversation_id)
            chain = await (await conn.execute(
                "SELECT * FROM public.bot_message_handler_chains WHERE bot_id=%s "
                "AND event_id=%s FOR UPDATE",
                (self.store.bot_id, event_id),
            )).fetchone()
            if chain is not None and decision in {"TRIGGER", "NO_TRIGGER"}:
                chain = await self.handler_chains.resolve_fallback_in(
                    conn, chain, decision, rule_revision, reason
                )
                if chain["status"] == "PENDING":
                    row = await (await conn.execute(
                        "SELECT * FROM public.bot_message_inbox WHERE bot_id=%s AND event_id=%s",
                        (self.store.bot_id, event_id),
                    )).fetchone()
                elif chain["status"] == "PASSED":
                    row = await self._classify_in(
                        conn, conversation_id, event_id, decision, rule_revision, reason
                    )
                else:
                    raise ForestError(
                        "MESSAGE_HANDLER_CHAIN_TERMINAL",
                        "A terminal handler chain cannot receive a fallback decision",
                        409,
                    )
            elif chain is not None and not (
                decision == "FAILED"
                and chain["status"] == "PASSED"
                and chain["fallback_decision"] == "PENDING"
            ):
                raise ForestError(
                    "MESSAGE_HANDLER_FALLBACK_INVALID",
                    "This message is governed by its frozen handler fallback",
                    409,
                )
            else:
                row = await self._classify_in(
                    conn, conversation_id, event_id, decision, rule_revision, reason
                )
            await self.store._event(
                conn,
                (
                    "conversation.classified"
                    if row["trigger_decision"] != "PENDING"
                    else "message_handler.fallback_resolved"
                ),
                event_id,
                {"conversation_id": conversation_id},
            )
            await self.bindings._final_guard(conn, lease, binding, data=False, require_receive=True, require_config=True)
            return row

    async def pending(self, lease: HostLease, *, limit: int = 128,
                      after_conversation_id: str | None = None) -> list[dict]:
        if type(limit) is not int or not 1 <= limit <= 1024:
            raise ValueError("limit")
        async with self.store.pool.connection() as conn, conn.transaction():
            await self.store._host(conn, lease)
            rows = await (await conn.execute(
                "SELECT c.*,a.phase AS agent_phase,a.checkpoint_seq AS agent_checkpoint_seq "
                "FROM public.bot_conversations c JOIN public.bot_bindings b "
                "ON b.bot_id=c.bot_id AND b.binding_id=c.binding_id "
                "LEFT JOIN public.bot_conversation_sessions s ON s.bot_id=c.bot_id AND s.session_id=c.active_session_id "
                "LEFT JOIN public.bot_agent_runs a ON a.bot_id=s.bot_id AND a.run_id=s.active_run_id WHERE c.bot_id=%s "
                "AND (%s::text IS NULL OR c.conversation_id>%s) "
                "AND (a.phase IS NULL OR (a.phase<>'BLOCKED_UNKNOWN' AND (a.deadline_at<=clock_timestamp() "
                "OR (a.phase<>'WAIT_APPROVAL' AND (a.phase<>'WAIT_INPUT' OR c.classified_through_seq>a.wait_classified_seq))))) "
                "AND b.enabled AND (c.context_applied_seq<c.classified_through_seq OR "
                "c.mailbox_revision>c.processed_mailbox_revision OR c.active_session_id IS NOT NULL "
                "OR EXISTS (SELECT 1 FROM public.bot_conversation_triggers t WHERE t.bot_id=c.bot_id "
                "AND t.conversation_id=c.conversation_id AND t.status IN ('PENDING','CLAIMED')) "
                "OR EXISTS (SELECT 1 FROM public.bot_business_command_requests r WHERE r.bot_id=c.bot_id "
                "AND r.conversation_id=c.conversation_id AND r.status IN ('PENDING','CLAIMED') "
                "AND (r.status='PENDING' OR r.next_attempt_at<=clock_timestamp() "
                "OR r.execution_deadline_at<=clock_timestamp())) "
                "OR EXISTS (SELECT 1 FROM public.bot_conversation_notices n WHERE n.bot_id=c.bot_id "
                "AND n.conversation_id=c.conversation_id AND n.status IN ('PENDING','CLAIMED')) "
                "OR EXISTS (SELECT 1 FROM public.bot_message_handler_chains h WHERE h.bot_id=c.bot_id "
                "AND h.conversation_id=c.conversation_id AND h.status='PENDING' "
                "AND (h.claim_actor_id IS NOT NULL OR h.next_attempt_at<=clock_timestamp() "
                "OR h.deadline_at<=clock_timestamp()))) "
                "ORDER BY c.conversation_id LIMIT %s",
                (self.store.bot_id, after_conversation_id, after_conversation_id, limit),
            )).fetchall()
            await self.store._host(conn, lease)
            return rows

    async def retry_business_classifications(
        self, lease: HostLease, *, limit: int = 128
    ) -> int:
        """Retry deterministic business classification without opening an ingress gap."""
        if type(limit) is not int or not 1 <= limit <= 1024:
            raise ValueError("limit")
        async with self.store.pool.connection() as conn, conn.transaction():
            await self.store._host(conn, lease)
            rows = await (
                await conn.execute(
                    "SELECT event_id FROM public.bot_message_inbox WHERE bot_id=%s "
                    "AND trigger_decision='PENDING' AND business_classification_pending "
                    "AND business_classification_next_attempt_at<=clock_timestamp() "
                    "AND classification_deadline>clock_timestamp() "
                    "ORDER BY business_classification_next_attempt_at,event_id LIMIT %s",
                    (self.store.bot_id, limit),
                )
            ).fetchall()
        completed = 0
        for candidate in rows:
            async with self.store.pool.connection() as conn, conn.transaction():
                await self.store._host(conn, lease)
                inbox = await (
                    await conn.execute(
                        "SELECT * FROM public.bot_message_inbox WHERE bot_id=%s AND event_id=%s "
                        "FOR UPDATE SKIP LOCKED",
                        (self.store.bot_id, candidate["event_id"]),
                    )
                ).fetchone()
                if (
                    inbox is None
                    or inbox["trigger_decision"] != "PENDING"
                    or not inbox["business_classification_pending"]
                ):
                    continue
                now = await self.store._now(conn)
                if inbox["classification_deadline"] <= now:
                    continue
                conversation = await self._conversation(conn, inbox["conversation_id"])
                binding = await self.bindings._guard(
                    conn,
                    lease,
                    conversation["binding_id"],
                    data=False,
                    require_receive=True,
                )
                barrier_chain = await (
                    await conn.execute(
                        "SELECT * FROM public.bot_message_handler_chains WHERE bot_id=%s "
                        "AND event_id=%s FOR UPDATE",
                        (self.store.bot_id, inbox["event_id"]),
                    )
                ).fetchone()
                handler_limit_snapshot = None
                if barrier_chain is not None and barrier_chain["admission_exempt"]:
                    handler_limit_snapshot = await self.handler_chains.limits.snapshot_in(conn)
                inputs = InputContentStore(self.store)
                clock = await inputs.payloads.lock_in(conn)
                source = await inputs.read_input_in(
                    conn,
                    inbox["event_id"],
                    expected_deletion_epoch=clock["deletion_epoch"],
                    required=True,
                )
                try:
                    plan = await self._business_plan_in(
                        conn,
                        binding,
                        {"author": source["author"], "segments": source["segments"]},
                    )
                except ForestError as exc:
                    candidate = exc.code
                    stable = (
                        isinstance(candidate, str)
                        and candidate.isascii()
                        and 1 <= len(candidate) <= 96
                        and candidate[0].isalpha()
                        and candidate == candidate.upper()
                        and candidate.replace("_", "").isalnum()
                    )
                    plan = {
                        "failed": True,
                        "reason": candidate
                        if stable
                        else "BUSINESS_CLASSIFICATION_UNAVAILABLE",
                    }
                if plan is not None and plan.get("failed"):
                    attempt_no = inbox["business_classification_attempt_count"] + 1
                    delay = min(5.0, 0.25 * (2 ** min(attempt_no - 1, 8)))
                    retry_at = min(
                        now + timedelta(seconds=delay), inbox["classification_deadline"]
                    )
                    operation_id = (
                        "business-classification-attempt:"
                        + inbox["event_id"]
                        + ":"
                        + str(attempt_no)
                    )
                    await conn.execute(
                        "INSERT INTO public.bot_business_classification_attempts("
                        "bot_id,event_id,attempt_no,operation_id,host_incarnation,host_owner_epoch,"
                        "error_code,retry_at) VALUES (%s,%s,%s,%s,%s,%s,%s,%s)",
                        (
                            self.store.bot_id,
                            inbox["event_id"],
                            attempt_no,
                            operation_id,
                            lease.incarnation,
                            lease.owner_epoch,
                            plan["reason"],
                            retry_at,
                        ),
                    )
                    await conn.execute(
                        "UPDATE public.bot_message_inbox SET "
                        "business_classification_attempt_count=%s,"
                        "business_classification_next_attempt_at=%s,"
                        "business_classification_error_code=%s "
                        "WHERE bot_id=%s AND event_id=%s",
                        (
                            attempt_no,
                            retry_at,
                            plan["reason"],
                            self.store.bot_id,
                            inbox["event_id"],
                        ),
                    )
                    await self.store._event(
                        conn,
                        "business_command.classification_retry",
                        operation_id,
                        {
                            "conversation_id": inbox["conversation_id"],
                            "event_id": inbox["event_id"],
                            "attempt_no": attempt_no,
                            "error_code": plan["reason"],
                            "retry_at": retry_at.isoformat(),
                        },
                    )
                    await self.bindings._final_guard(
                        conn,
                        lease,
                        binding,
                        data=False,
                        require_receive=True,
                        require_config=True,
                    )
                    continue
                decision = inbox["business_fallback_decision"]
                rule_revision = inbox["business_fallback_rule_revision"]
                reason = inbox["business_fallback_reason"]
                request_id = None
                if (
                    plan is None
                    and barrier_chain is not None
                    and barrier_chain["admission_exempt"]
                ):
                    barrier_chain = await self.handler_chains.admit_deferred_in(
                        conn, barrier_chain, handler_limit_snapshot
                    )
                    if barrier_chain["status"] == "FAILED":
                        decision = "FAILED"
                        rule_revision = "message-handler-limit-v1"
                        reason = barrier_chain["terminal_reason"]
                        await self._put_notice_in(
                            conn,
                            event_id=inbox["event_id"],
                            request_id=None,
                            conversation_id=inbox["conversation_id"],
                            ingress_seq=inbox["ingress_seq"],
                            input_payload_id=inbox["payload_id"],
                            kind="MESSAGE_HANDLER_REJECTED",
                            error_code=reason,
                        )
                if plan is not None:
                    request_id, request_digest, _ = await self._persist_business_request_in(
                        conn,
                        event_id=inbox["event_id"],
                        conversation_id=inbox["conversation_id"],
                        binding_id=conversation["binding_id"],
                        ingress_seq=inbox["ingress_seq"],
                        author=source["author"],
                        input_payload_id=inbox["payload_id"],
                        deletion_epoch=clock["deletion_epoch"],
                        business_plan=plan,
                    )
                    decision = "BUSINESS_COMMAND"
                    rule_revision = "business-command-v1"
                    reason = request_id + ":" + request_digest
                    await self.handler_chains.cancel_for_business_command_in(
                        conn, inbox["event_id"]
                    )
                await conn.execute(
                    "UPDATE public.bot_message_inbox SET business_classification_pending=false,"
                    "business_classification_next_attempt_at=NULL WHERE bot_id=%s AND event_id=%s",
                    (self.store.bot_id, inbox["event_id"]),
                )
                if decision != "PENDING":
                    chain = await (await conn.execute(
                        "SELECT * FROM public.bot_message_handler_chains WHERE bot_id=%s "
                        "AND event_id=%s FOR UPDATE",
                        (self.store.bot_id, inbox["event_id"]),
                    )).fetchone()
                    if chain is not None and decision in {"TRIGGER", "NO_TRIGGER"}:
                        chain = await self.handler_chains.resolve_fallback_in(
                            conn, chain, decision, rule_revision, reason
                        )
                    if (
                        chain is None
                        or chain["status"] in {"PASSED", "CANCELLED"}
                        or (
                            decision == "FAILED"
                            and chain["status"] == "FAILED"
                            and rule_revision == "message-handler-limit-v1"
                        )
                    ):
                        await self._classify_in(
                            conn,
                            inbox["conversation_id"],
                            inbox["event_id"],
                            decision,
                            rule_revision,
                            reason,
                        )
                    else:
                        decision = "PENDING"
                await self.store._event(
                    conn,
                    "business_command.classification_completed",
                    inbox["event_id"],
                    {
                        "conversation_id": inbox["conversation_id"],
                        "event_id": inbox["event_id"],
                        "decision": decision,
                        "request_id": request_id,
                    },
                )
                await self.bindings._final_guard(
                    conn,
                    lease,
                    binding,
                    data=False,
                    require_receive=True,
                    require_config=True,
                )
                completed += 1
        return completed

    async def _expire_business_classification(
        self, lease: HostLease, conversation_id: str, event_id: str
    ) -> bool:
        async with self.store.pool.connection() as conn, conn.transaction():
            await self.store._host(conn, lease)
            conversation = await self._conversation(conn, conversation_id)
            binding = await self.bindings._guard(
                conn,
                lease,
                conversation["binding_id"],
                data=False,
                require_receive=True,
            )
            inbox = await (
                await conn.execute(
                    "SELECT * FROM public.bot_message_inbox WHERE bot_id=%s AND event_id=%s "
                    "FOR UPDATE",
                    (self.store.bot_id, event_id),
                )
            ).fetchone()
            if (
                inbox is None
                or inbox["conversation_id"] != conversation_id
                or inbox["trigger_decision"] != "PENDING"
                or not inbox["business_classification_pending"]
                or inbox["classification_deadline"] > await self.store._now(conn)
            ):
                return False
            attempt_no = inbox["business_classification_attempt_count"] + 1
            error_code = "BUSINESS_CLASSIFICATION_DEADLINE_EXCEEDED"
            operation_id = (
                "business-classification-attempt:" + event_id + ":" + str(attempt_no)
            )
            await conn.execute(
                "INSERT INTO public.bot_business_classification_attempts("
                "bot_id,event_id,attempt_no,operation_id,host_incarnation,host_owner_epoch,"
                "error_code,retry_at) VALUES (%s,%s,%s,%s,%s,%s,%s,NULL)",
                (
                    self.store.bot_id,
                    event_id,
                    attempt_no,
                    operation_id,
                    lease.incarnation,
                    lease.owner_epoch,
                    error_code,
                ),
            )
            await self._put_notice_in(
                conn,
                event_id=event_id,
                request_id=None,
                conversation_id=conversation_id,
                ingress_seq=inbox["ingress_seq"],
                input_payload_id=inbox["payload_id"],
                kind="BUSINESS_CLASSIFICATION_FAILED",
                error_code=error_code,
            )
            await self.handler_chains.cancel_in(
                conn, event_id, "BUSINESS_CLASSIFICATION_FAILED"
            )
            await conn.execute(
                "UPDATE public.bot_message_inbox SET business_classification_pending=false,"
                "business_classification_attempt_count=%s,business_classification_next_attempt_at=NULL,"
                "business_classification_error_code=%s WHERE bot_id=%s AND event_id=%s",
                (attempt_no, error_code, self.store.bot_id, event_id),
            )
            await self._classify_in(
                conn,
                conversation_id,
                event_id,
                "FAILED",
                "business-command-classification-v1",
                "deadline_exceeded",
            )
            await self.store._event(
                conn,
                "business_command.classification_failed",
                event_id,
                {
                    "conversation_id": conversation_id,
                    "event_id": event_id,
                    "attempt_no": attempt_no,
                    "error_code": error_code,
                },
            )
            await self.bindings._final_guard(
                conn,
                lease,
                binding,
                data=False,
                require_receive=True,
                require_config=True,
            )
            return True

    async def expire_classifications(self, lease: HostLease, *, limit: int = 128) -> int:
        """A dead preprocessing worker cannot leave a permanent ingress gap."""
        async with self.store.pool.connection() as conn, conn.transaction():
            await self.store._host(conn, lease)
            rows = await (await conn.execute(
                "SELECT i.conversation_id,i.event_id,i.business_classification_pending "
                "FROM public.bot_message_inbox i "
                "JOIN public.bot_conversations c ON c.bot_id=i.bot_id AND c.conversation_id=i.conversation_id "
                "JOIN public.bot_bindings b ON b.bot_id=c.bot_id AND b.binding_id=c.binding_id "
                "WHERE i.bot_id=%s AND b.receive_enabled AND b.state IN ('ONLINE','DRAINING') "
                "AND i.trigger_decision='PENDING' AND i.classification_deadline<=clock_timestamp() "
                "AND NOT EXISTS(SELECT 1 FROM public.bot_message_handler_chains h "
                "WHERE h.bot_id=i.bot_id AND h.event_id=i.event_id AND h.status='PENDING') "
                "ORDER BY i.classification_deadline LIMIT %s", (self.store.bot_id, limit),
            )).fetchall()
        count = 0
        for row in rows:
            try:
                if row["business_classification_pending"]:
                    count += bool(
                        await self._expire_business_classification(
                            lease, row["conversation_id"], row["event_id"]
                        )
                    )
                else:
                    await self.classify(
                        lease,
                        row["conversation_id"],
                        row["event_id"],
                        "FAILED",
                        rule_revision="classification-timeout-v1",
                        reason="deadline_exceeded",
                    )
                    count += 1
            except ForestError as exc:
                if exc.code not in {"CLASSIFICATION_CONFLICT", "BINDING_OFFLINE", "RECEIVE_CLOSED", "STALE_BINDING"}:
                    raise
        return count

    async def pending_controls(self, lease: HostLease, *, limit: int = 64,
                               after_command_id: str | None = None) -> list[dict]:
        if type(limit) is not int or not 1 <= limit <= 256:
            raise ValueError("limit")
        async with self.store.pool.connection() as conn, conn.transaction():
            runtime = await self.store._runtime(conn)
            await self.store._host(conn, lease, runtime)
            if not runtime["control_admission"]:
                raise ForestError("CONTROL_CLOSED", "Bot control admission is closed", 409)
            rows = await (await conn.execute(
                "SELECT s.command_id,s.binding_id,s.command_name,s.conversation_id,s.status,s.priority "
                "FROM public.bot_shell_requests s JOIN public.bot_bindings b "
                "ON b.bot_id=s.bot_id AND b.binding_id=s.binding_id WHERE s.bot_id=%s "
                "AND (%s::text IS NULL OR s.command_id>%s) "
                "AND b.receive_enabled AND b.state IN ('ONLINE','DRAINING') "
                "AND (s.status='PENDING' OR (s.status='CLAIMED' AND (s.expires_at<=clock_timestamp() "
                "OR s.host_incarnation<>%s OR s.binding_epoch<>b.binding_epoch OR s.policy_revision<>%s))) "
                "ORDER BY s.command_id LIMIT %s",
                (self.store.bot_id, after_command_id, after_command_id, lease.incarnation,
                 runtime["policy_revision"], limit),
            )).fetchall()
            await self.store._host(conn, lease, runtime)
            return rows

    async def reconcile_echoes(self, lease: HostLease, *, limit: int = 64) -> int:
        """Recover exact late self-output aliases from durable SENT receipts.

        The scan is independent of notification delivery. It records provenance,
        not context layout; only an Actor's later safe point changes the view.
        No text comparison or UNKNOWN-send guess is permitted.
        """
        if type(limit) is not int or not 1 <= limit <= 256:
            raise ValueError("limit")
        async with self.store.pool.connection() as conn, conn.transaction():
            await self.store._host(conn, lease)
            candidates = await (await conn.execute(
                "SELECT DISTINCT i.event_id,i.conversation_id,c.binding_id "
                "FROM public.bot_message_inbox i JOIN public.bot_conversations c "
                "ON c.bot_id=i.bot_id AND c.conversation_id=i.conversation_id "
                "JOIN public.bot_bindings b ON b.bot_id=c.bot_id AND b.binding_id=c.binding_id "
                "JOIN public.bot_send_intents s ON s.bot_id=i.bot_id AND s.binding_id=c.binding_id "
                "AND s.conversation_id=c.conversation_id "
                "AND s.reply_route->>'channel_kind'=c.channel_kind AND s.reply_route->>'channel_id'=c.channel_id "
                "AND COALESCE(s.reply_route->>'thread_id','')=c.thread_id "
                "JOIN public.bot_send_attempts a ON a.bot_id=s.bot_id AND a.intent_id=s.intent_id "
                "AND a.state='SENT' AND a.receipt->>'message_id'=i.platform_message_id "
                "WHERE i.bot_id=%s AND i.author->>'is_self'='true' "
                "AND i.author->>'external_user_id'=b.self_id AND i.origin='platform_input' "
                "AND b.receive_enabled AND b.state IN ('ONLINE','DRAINING') "
                "AND (EXISTS (SELECT 1 FROM public.bot_conversation_replies r WHERE r.bot_id=s.bot_id "
                "AND r.delivery->>'intent_id'=s.intent_id) OR EXISTS "
                "(SELECT 1 FROM public.bot_shell_requests q WHERE q.bot_id=s.bot_id "
                "AND q.command_id=s.command_id AND q.delivery_metadata->>'intent_id'=s.intent_id "
                "AND q.status='COMPLETED')) "
                "ORDER BY i.event_id LIMIT %s", (self.store.bot_id, limit),
            )).fetchall()
        count = 0
        for candidate in candidates:
            try:
                if await self.reconcile_exact_echo(lease, candidate["event_id"]):
                    count += 1
            except ForestError as exc:
                if exc.code not in {"BINDING_OFFLINE", "RECEIVE_CLOSED", "STALE_BINDING", "ECHO_IDENTITY_CONFLICT"}:
                    raise
        return count

    async def reconcile_exact_echo(self, lease: HostLease, event_id: str) -> bool:
        async with self.store.pool.connection() as conn, conn.transaction():
            identity = await (await conn.execute(
                "SELECT c.binding_id,c.conversation_id FROM public.bot_message_inbox i "
                "JOIN public.bot_conversations c ON c.bot_id=i.bot_id AND c.conversation_id=i.conversation_id "
                "WHERE i.bot_id=%s AND i.event_id=%s", (self.store.bot_id, event_id),
            )).fetchone()
            if identity is None:
                return False
            binding = await self.bindings._guard(
                conn, lease, identity["binding_id"], data=False, require_receive=True,
            )
            conversation = await self._conversation(conn, identity["conversation_id"])
            result = await self._reconcile_exact_echo_in(conn, lease, binding, conversation, event_id)
            await self.bindings._final_guard(conn, lease, binding, data=False, require_receive=True)
            return result

    async def _reconcile_exact_echo_in(self, conn, lease, binding, conversation, event_id):
        """Caller owns Binding/Host/Conversation; exact proof and source link commit together."""
        inbox = await (await conn.execute(
            "SELECT * FROM public.bot_message_inbox WHERE bot_id=%s AND event_id=%s FOR UPDATE",
            (self.store.bot_id, event_id),
        )).fetchone()
        if inbox["origin"] in {"chat_output", "control_output"}:
            from forest.storage.source_links import SourceLinkStore

            links = SourceLinkStore(self.store)
            clock = await links.payloads.lock_in(conn)
            await links.link_exact_echo_in(conn, event_id, expected_deletion_epoch=clock["deletion_epoch"])
            return False
        if (not inbox["author"].get("is_self")
                or inbox["author"]["external_user_id"] != binding["self_id"]
                or inbox["origin"] != "platform_input"):
            return False
        matches = await (await conn.execute(
            "SELECT DISTINCT ON (s.intent_id) s.intent_id,s.send_lane,s.command_id,a.attempt_id,"
            "r.reply_id,q.command_id AS canonical_command_id "
            "FROM public.bot_send_intents s JOIN public.bot_send_attempts a "
            "ON a.bot_id=s.bot_id AND a.intent_id=s.intent_id "
            "LEFT JOIN public.bot_conversation_replies r ON r.bot_id=s.bot_id AND r.delivery->>'intent_id'=s.intent_id "
            "LEFT JOIN public.bot_shell_requests q ON q.bot_id=s.bot_id AND q.command_id=s.command_id "
            "AND q.delivery_metadata->>'intent_id'=s.intent_id AND q.status='COMPLETED' "
            "WHERE s.bot_id=%s AND s.binding_id=%s AND s.conversation_id=%s "
            "AND s.reply_route->>'channel_kind'=%s AND s.reply_route->>'channel_id'=%s "
            "AND COALESCE(s.reply_route->>'thread_id','')=%s "
            "AND a.state='SENT' AND a.receipt->>'message_id'=%s ORDER BY s.intent_id,a.attempt_id LIMIT 2",
            (self.store.bot_id, binding["binding_id"], conversation["conversation_id"],
             conversation["channel_kind"], conversation["channel_id"], conversation["thread_id"],
             inbox["platform_message_id"]),
        )).fetchall()
        if len(matches) > 1:
            raise ForestError("ECHO_IDENTITY_CONFLICT", "Platform output identity maps to multiple intents", 409)
        if not matches:
            return False
        match = matches[0]
        origin = "chat_output" if match["send_lane"] == "CHAT" else "control_output"
        if (origin == "chat_output" and not match["reply_id"]) or (
            origin == "control_output" and not match["canonical_command_id"]
        ):
            return False
        await conn.execute(
            "INSERT INTO public.bot_message_echo_aliases(bot_id,event_id,intent_id,attempt_id,platform_message_id,"
            "origin,reply_id,command_id) VALUES (%s,%s,%s,%s,%s,%s,%s,%s)",
            (self.store.bot_id, event_id, match["intent_id"], match["attempt_id"],
             inbox["platform_message_id"], origin, match["reply_id"], match["canonical_command_id"]),
        )
        from forest.storage.source_links import SourceLinkStore

        links = SourceLinkStore(self.store)
        clock = await links.payloads.lock_in(conn)
        await links.link_exact_echo_in(conn, event_id, expected_deletion_epoch=clock["deletion_epoch"])
        await conn.execute(
            "UPDATE public.bot_message_inbox SET origin=%s WHERE bot_id=%s AND event_id=%s",
            (origin, self.store.bot_id, event_id),
        )
        await conn.execute(
            "UPDATE public.bot_conversations SET mailbox_revision=mailbox_revision+1 "
            "WHERE bot_id=%s AND conversation_id=%s", (self.store.bot_id, conversation["conversation_id"]),
        )
        await self.store._event(conn, "conversation.echo_resolved", event_id,
                                {"event_id": event_id, "intent_id": match["intent_id"], "origin": origin})
        await self.bindings._final_guard(conn, lease, binding, data=False, require_receive=True)
        return True

    async def claim_control(self, lease: HostLease, command_id: str, owner_id: str,
                            *, ttl_seconds: float = 30) -> dict:
        """Claim reserved control independently of an Actor or classification gap.

        This claim permits processing; it grants no command capability. A host
        command dispatcher must authorize the concrete action before executing it.
        """
        self.store._ttl(ttl_seconds)
        _text(owner_id, "owner_id")
        async with self.store.pool.connection() as conn, conn.transaction():
            identity = await (await conn.execute(
                "SELECT binding_id FROM public.bot_shell_requests WHERE bot_id=%s AND command_id=%s",
                (self.store.bot_id, command_id),
            )).fetchone()
            if identity is None:
                raise ForestError("COMMAND_NOT_FOUND", "Control command does not exist", 404)
            binding = await self.bindings._guard(conn, lease, identity["binding_id"], data=False)
            current = await (await conn.execute(
                "SELECT * FROM public.bot_shell_requests WHERE bot_id=%s AND command_id=%s FOR UPDATE",
                (self.store.bot_id, command_id),
            )).fetchone()
            if current["status"] == "COMPLETED":
                epoch = (await self.shell_payloads.payloads.lock_in(conn))["deletion_epoch"]
                current = await self.shell_payloads.hydrate_result_in(
                    conn, current, epoch=epoch, required=False
                )
                await self.bindings._final_guard(conn, lease, binding, data=False)
                return current
            if current["status"] == "CANCELLED":
                await self.bindings._final_guard(conn, lease, binding, data=False)
                return current
            epoch = (await self.shell_payloads.payloads.lock_in(conn))["deletion_epoch"]
            try:
                current = await self.shell_payloads.hydrate_arguments_in(
                    conn, current, epoch=epoch
                )
            except ForestError as exc:
                if exc.code != "CONTROL_CONTENT_REVOKED":
                    raise
                cancelled = await (await conn.execute(
                    "UPDATE public.bot_shell_requests SET status='CANCELLED',owner_id=NULL,"
                    "host_incarnation=NULL,expires_at=NULL,terminal_reason='CONTROL_CONTENT_REVOKED',"
                    "completed_at=clock_timestamp() WHERE bot_id=%s AND command_id=%s RETURNING *",
                    (self.store.bot_id, command_id),
                )).fetchone()
                await self.bindings._final_guard(conn, lease, binding, data=False)
                return {**cancelled, "arguments": None, "arguments_payload_state": "UNAVAILABLE"}
            live = (
                current["status"] == "CLAIMED" and current["expires_at"] > await self.store._now(conn)
                and current["host_incarnation"] == lease.incarnation
                and current["binding_epoch"] == binding["binding_epoch"]
                and current["policy_revision"] == binding["_runtime"]["policy_revision"]
            )
            if live and current["owner_id"] != owner_id:
                raise ForestError("COMMAND_BUSY", "Control command has a current owner", 409)
            epoch = current["claim_epoch"] if live else current["claim_epoch"] + 1
            claimed = await (await conn.execute(
                "UPDATE public.bot_shell_requests SET status='CLAIMED',owner_id=%s,claim_epoch=%s,"
                "host_incarnation=%s,binding_epoch=%s,policy_revision=%s,"
                "expires_at=clock_timestamp()+(%s*interval '1 second') "
                "WHERE bot_id=%s AND command_id=%s RETURNING *",
                (owner_id, epoch, lease.incarnation, binding["binding_epoch"],
                 binding["_runtime"]["policy_revision"], ttl_seconds, self.store.bot_id, command_id),
            )).fetchone()
            source = await (await conn.execute(
                "SELECT i.author,i.ingress_seq,i.sent_at,c.platform,c.channel_kind,c.channel_id,c.thread_id,"
                "c.group_source_id FROM public.bot_message_inbox i JOIN public.bot_conversations c "
                "ON c.bot_id=i.bot_id AND c.conversation_id=i.conversation_id WHERE i.bot_id=%s AND i.event_id=%s",
                (self.store.bot_id, claimed["event_id"]),
            )).fetchone()
            await self.bindings._final_guard(conn, lease, binding, data=False)
            return {
                **claimed,
                "arguments": current["arguments"],
                "arguments_payload_state": current["arguments_payload_state"],
                "source": source,
            }

    async def complete_control(self, lease: HostLease, claim: dict, operation_id: str, *,
                               reply_parts: list[list[dict]], outcome: dict, delivery) -> dict:
        """Commit a safe command receipt and CONTROL SendIntent together.

        No conversation Actor is acquired and no active prompt is mutated. The
        original command retains its ingress position for later input admission.
        """
        _text(operation_id, "operation_id")
        parts, result = _json(reply_parts), _json(outcome, 65536)
        if not isinstance(result, dict):
            raise ForestError("INVALID_COMMAND_RESULT", "Control outcome must be an object", 422)
        digest = digest_json([parts, result])
        async with self.store.pool.connection() as conn, conn.transaction():
            binding = await self.bindings._guard(
                conn, lease, claim["binding_id"], claim["binding_epoch"], data=False,
            )
            current = await (await conn.execute(
                "SELECT * FROM public.bot_shell_requests WHERE bot_id=%s AND command_id=%s FOR UPDATE",
                (self.store.bot_id, claim["command_id"]),
            )).fetchone()
            if current is None or current["binding_id"] != binding["binding_id"]:
                raise ForestError("COMMAND_NOT_FOUND", "Command does not belong to this binding", 404)
            if current["status"] == "COMPLETED":
                if current["operation_id"] != operation_id or current["result_digest"] != digest:
                    raise ForestError("COMMAND_RESULT_CONFLICT", "Control result is already committed", 409)
                epoch = (await self.shell_payloads.payloads.lock_in(conn))["deletion_epoch"]
                saved = await self.shell_payloads.hydrate_result_in(
                    conn, current, epoch=epoch, required=False
                )
                await self.bindings._final_guard(conn, lease, binding, data=False)
                return saved
            if (current["status"] != "CLAIMED" or current["owner_id"] != claim["owner_id"]
                    or current["claim_epoch"] != claim["claim_epoch"]
                    or current["host_incarnation"] != lease.incarnation
                    or current["policy_revision"] != binding["_runtime"]["policy_revision"]
                    or current["expires_at"] <= await self.store._now(conn)):
                raise ForestError("STALE_COMMAND", "Control command claim expired or was replaced", 409)
            epoch = (await self.shell_payloads.payloads.lock_in(conn))["deletion_epoch"]
            await self.shell_payloads.hydrate_arguments_in(conn, current, epoch=epoch)
            source = await (await conn.execute(
                "SELECT * FROM public.bot_conversations WHERE bot_id=%s AND conversation_id=%s",
                (self.store.bot_id, current["conversation_id"]),
            )).fetchone()
            route = {"bot_id": self.store.bot_id, "binding_id": binding["binding_id"],
                     "platform": binding["platform"], "self_id": binding["self_id"],
                     "channel_kind": source["channel_kind"], "channel_id": source["channel_id"],
                     "thread_id": source["thread_id"], "binding_epoch": binding["binding_epoch"],
                     "visibility": source["channel_kind"]}
            command_input = await (await conn.execute(
                "SELECT payload_id FROM public.bot_message_inbox WHERE bot_id=%s AND event_id=%s",
                (self.store.bot_id, current["event_id"]),
            )).fetchone()
            if not command_input or not command_input["payload_id"]:
                raise ForestError("INPUT_PAYLOAD_MIGRATION_REQUIRED", "Control origin has no payload", 409)
            if not binding["config_payload_id"]:
                raise ForestError("CONTROL_CONTENT_REVOKED", "Binding configuration is unavailable", 409)
            outcome_parents = [
                command_input["payload_id"],
                current["arguments_payload_id"],
                binding["config_payload_id"],
            ]
            operation = await (await conn.execute(
                "SELECT result_payload_id FROM public.bot_web_operations WHERE bot_id=%s "
                "AND operation_id=%s",
                (self.store.bot_id, "shell-action:" + current["command_id"]),
            )).fetchone()
            if operation and operation["result_payload_id"]:
                try:
                    await self.shell_payloads.payloads.require_available_in(
                        conn, (operation["result_payload_id"],),
                        expected_deletion_epoch=epoch,
                    )
                except ForestError as exc:
                    if exc.code not in SHELL_UNAVAILABLE or result.get("code") != "RECORDED_UNAVAILABLE":
                        raise ForestError(
                            "CONTROL_CONTENT_REVOKED", "Shell operation result is unavailable", 409
                        ) from None
                else:
                    outcome_parents.append(operation["result_payload_id"])
            outcome_ref = await self.shell_payloads.put_outcome_in(
                conn,
                command_id=current["command_id"],
                outcome=result,
                parents=outcome_parents,
                epoch=epoch,
            )
            receipt = await delivery.enqueue_in(
                conn, lease, operation_id="control:" + current["command_id"], route=route, parts=parts,
                conversation_id=current["conversation_id"], lane="CONTROL", command_id=current["command_id"],
                control_seq=1, root_operation_id=current["command_id"],
                expected_policy_revision=current["policy_revision"],
                parents=(outcome_ref.payload_id, binding["config_payload_id"]),
            )
            part_rows = await (await conn.execute(
                "SELECT payload_id FROM public.bot_send_parts WHERE bot_id=%s AND intent_id=%s "
                "ORDER BY part_index",
                (self.store.bot_id, receipt["intent_id"]),
            )).fetchall()
            delivery_ref = await self.shell_payloads.put_delivery_in(
                conn,
                command_id=current["command_id"],
                delivery=receipt,
                parents=(outcome_ref.payload_id, *(row["payload_id"] for row in part_rows)),
                epoch=epoch,
            )
            if current["expires_at"] <= await self.store._now(conn):
                raise ForestError("STALE_COMMAND", "Control claim expired before commit", 409)
            updated = await (await conn.execute(
                "UPDATE public.bot_shell_requests SET status='COMPLETED',operation_id=%s,result_digest=%s,"
                "outcome=NULL,delivery=NULL,outcome_payload_id=%s,outcome_payload_digest=%s,"
                "outcome_metadata=%s,delivery_payload_id=%s,delivery_payload_digest=%s,"
                "delivery_metadata=%s,completed_at=clock_timestamp() "
                "WHERE bot_id=%s AND command_id=%s RETURNING *",
                (
                    operation_id,
                    digest,
                    outcome_ref.payload_id,
                    outcome_ref.digest,
                    Jsonb(outcome_metadata(result)),
                    delivery_ref.payload_id,
                    delivery_ref.digest,
                    Jsonb(delivery_metadata(receipt)),
                    self.store.bot_id,
                    current["command_id"],
                ),
            )).fetchone()
            await self.store._event(conn, "shell.completed", operation_id,
                                    {"command_id": current["command_id"], "conversation_id": current["conversation_id"]})
            await self.bindings._final_guard(conn, lease, binding, data=False, require_config=True)
            return {
                **updated,
                "outcome": result,
                "delivery": receipt,
                "outcome_payload_state": "ACTIVE",
                "delivery_payload_state": "ACTIVE",
            }

    async def acquire_actor(self, lease: HostLease, conversation_id: str, actor_id: str,
                            *, ttl_seconds: float = 30) -> ActorLease:
        _text(actor_id, "actor_id")
        self.store._ttl(ttl_seconds)
        async with self.store.pool.connection() as conn, conn.transaction():
            identity = await self._locate(conn, conversation_id)
            binding = await self.bindings._guard(conn, lease, identity["binding_id"])
            runtime = await self.store._runtime(conn)
            row = await self._conversation(conn, conversation_id)
            now = await self.store._now(conn)
            if (row["actor_expires_at"] and row["actor_expires_at"] > now
                    and row["actor_host_incarnation"] == lease.incarnation
                    and row["actor_binding_epoch"] == binding["binding_epoch"]
                    and all(row["actor_" + key] == runtime[key] for key in
                            ("policy_revision", "maintenance_epoch", "storage_generation"))):
                if row["actor_id"] != actor_id:
                    raise ForestError("ACTOR_BUSY", "Conversation has an active owner", 409)
                epoch = row["actor_epoch"]
            else:
                epoch = row["actor_epoch"] + 1
            row = await (await conn.execute(
                "UPDATE public.bot_conversations SET actor_id=%s,actor_epoch=%s,actor_host_incarnation=%s,"
                "actor_expires_at=clock_timestamp()+(%s*interval '1 second'),actor_binding_epoch=%s,"
                "actor_policy_revision=%s,actor_maintenance_epoch=%s,actor_storage_generation=%s "
                "WHERE bot_id=%s AND conversation_id=%s RETURNING *",
                (actor_id, epoch, lease.incarnation, ttl_seconds, binding["binding_epoch"],
                 runtime["policy_revision"], runtime["maintenance_epoch"], runtime["storage_generation"],
                 self.store.bot_id, conversation_id),
            )).fetchone()
            await self.bindings._final_guard(conn, lease, binding)
            return ActorLease(lease, conversation_id, row["binding_id"], binding["binding_epoch"],
                              actor_id, epoch, row["actor_expires_at"], runtime["policy_revision"],
                              runtime["maintenance_epoch"], runtime["storage_generation"])

    async def _actor(self, conn, actor: ActorLease) -> tuple[dict, dict]:
        binding = await self.bindings._guard(
            conn, actor.host, actor.binding_id, actor.binding_epoch,
        )
        runtime = await self.store._runtime(conn)
        if any(runtime[key] != getattr(actor, key) for key in
               ("policy_revision", "maintenance_epoch", "storage_generation")):
            raise ForestError("STALE_ACTOR", "Conversation authority changed", 409)
        row = await self._conversation(conn, actor.conversation_id)
        if (row["binding_id"] != actor.binding_id or row["actor_id"] != actor.actor_id
                or row["actor_epoch"] != actor.actor_epoch
                or row["actor_host_incarnation"] != actor.host.incarnation
                or not row["actor_expires_at"] or row["actor_expires_at"] <= await self.store._now(conn)):
            raise ForestError("STALE_ACTOR", "Conversation ownership expired or was replaced", 409)
        return binding, row

    async def _actor_final(self, conn, actor, binding):
        await self.bindings._final_guard(conn, actor.host, binding)
        row = await self._conversation(conn, actor.conversation_id)
        if not row["actor_expires_at"] or row["actor_expires_at"] <= await self.store._now(conn):
            raise ForestError("STALE_ACTOR", "Conversation ownership expired before commit", 409)

    async def renew_actor(self, actor: ActorLease, *, ttl_seconds: float = 30) -> ActorLease:
        self.store._ttl(ttl_seconds)
        async with self.store.pool.connection() as conn, conn.transaction():
            binding, _ = await self._actor(conn, actor)
            await self._actor_final(conn, actor, binding)
            row = await (await conn.execute(
                "UPDATE public.bot_conversations SET actor_expires_at=clock_timestamp()+(%s*interval '1 second') "
                "WHERE bot_id=%s AND conversation_id=%s RETURNING actor_expires_at",
                (ttl_seconds, self.store.bot_id, actor.conversation_id),
            )).fetchone()
            return ActorLease(**{**actor.__dict__, "expires_at": row["actor_expires_at"]})

    async def release_actor(self, actor: ActorLease) -> None:
        async with self.store.pool.connection() as conn, conn.transaction():
            # Resource cleanup remains possible after data pause/policy changes;
            # it can only release this exact old owner, never another actor.
            binding = await self.bindings._guard(conn, actor.host, actor.binding_id, data=False)
            row = await self._conversation(conn, actor.conversation_id)
            if (row["actor_id"] != actor.actor_id or row["actor_epoch"] != actor.actor_epoch
                    or row["actor_host_incarnation"] != actor.host.incarnation):
                raise ForestError("STALE_ACTOR", "Actor resource is owned by another execution", 409)
            await self.bindings._final_guard(conn, actor.host, binding, data=False)
            await conn.execute(
                "UPDATE public.bot_conversations SET actor_expires_at=clock_timestamp(),actor_id=NULL "
                "WHERE bot_id=%s AND conversation_id=%s", (self.store.bot_id, actor.conversation_id),
            )

    async def _session(self, conn, conversation: dict) -> dict | None:
        if not conversation["active_session_id"]:
            return None
        return await (await conn.execute(
            "SELECT * FROM public.bot_conversation_sessions WHERE bot_id=%s AND session_id=%s FOR UPDATE",
            (self.store.bot_id, conversation["active_session_id"]),
        )).fetchone()

    async def _run(self, conn, session: dict | None) -> dict | None:
        if not session or not session["active_run_id"]:
            return None
        return await (await conn.execute(
            "SELECT * FROM public.bot_conversation_runs WHERE bot_id=%s AND run_id=%s FOR UPDATE",
            (self.store.bot_id, session["active_run_id"]),
        )).fetchone()

    async def _new_run(self, conn, conversation, session):
        if session is None:
            session_id = str(uuid4())
            session = await (await conn.execute(
                "INSERT INTO public.bot_conversation_sessions(bot_id,session_id,conversation_id,status) "
                "VALUES (%s,%s,%s,'RUNNING') RETURNING *",
                (self.store.bot_id, session_id, conversation["conversation_id"]),
            )).fetchone()
            await conn.execute(
                "UPDATE public.bot_conversations SET active_session_id=%s WHERE bot_id=%s AND conversation_id=%s",
                (session_id, self.store.bot_id, conversation["conversation_id"]),
            )
        run_id = str(uuid4())
        run = await (await conn.execute(
            "INSERT INTO public.bot_conversation_runs(bot_id,run_id,session_id,conversation_id) "
            "VALUES (%s,%s,%s,%s) RETURNING *",
            (self.store.bot_id, run_id, session["session_id"], conversation["conversation_id"]),
        )).fetchone()
        await conn.execute(
            "UPDATE public.bot_conversation_sessions SET status='RUNNING',active_run_id=%s "
            "WHERE bot_id=%s AND session_id=%s", (run_id, self.store.bot_id, session["session_id"]),
        )
        return session, run

    async def _require_legacy_run(self, conn, actor, *, run_id=None):
        _, conversation = await self._actor(conn, actor)
        if run_id is None:
            session = await self._session(conn, conversation)
            run = await self._run(conn, session)
            run_id = run["run_id"] if run else None
        registered = await (await conn.execute(
            "SELECT 1 FROM public.bot_agent_runs WHERE bot_id=%s AND run_id=%s",
            (self.store.bot_id, run_id),
        )).fetchone()
        if registered:
            raise ForestError("AGENT_BOUNDARY_REQUIRED", "Persistent Agent steps own this run boundary", 409)

    async def _business_command_selection_in(
        self, conn, actor, conversation, request, batch, *, replayed
    ) -> dict:
        from forest.storage.payloads import PayloadStore

        payloads = PayloadStore(self.store)
        clock = await payloads.lock_in(conn)
        source = await InputContentStore(self.store).read_input_in(
            conn,
            request["event_id"],
            expected_deletion_epoch=clock["deletion_epoch"],
            required=True,
        )
        arguments = await payloads.read_json_in(
            conn,
            request["arguments_payload_id"],
            expected_deletion_epoch=clock["deletion_epoch"],
        )
        if (
            not isinstance(arguments, dict)
            or arguments.get("request_id") != request["request_id"]
            or arguments.get("command_id") != request["command_id"]
            or arguments.get("name") != request["command_name"]
            or arguments.get("argument_mode") != request["argument_mode"]
            or not isinstance(arguments.get("argument_text"), str)
        ):
            raise ForestError(
                "BUSINESS_COMMAND_ARGUMENTS_INVALID",
                "Command arguments no longer match the admitted request",
                409,
            )
        inbox = await (
            await conn.execute(
                "SELECT source_event_id FROM public.bot_message_inbox "
                "WHERE bot_id=%s AND event_id=%s",
                (self.store.bot_id, request["event_id"]),
            )
        ).fetchone()
        event = {
            "request_id": request["request_id"],
            "command_id": request["command_id"],
            "name": request["command_name"],
            "argument_text": arguments["argument_text"],
            "source_event_id": inbox["source_event_id"],
            "conversation": {
                key: conversation[key]
                for key in (
                    "conversation_id",
                    "group_source_id",
                    "platform",
                    "channel_kind",
                    "channel_id",
                    "thread_id",
                )
            },
            "author": source["author"],
            "segments": source["segments"],
        }
        return {
            "status": "BUSINESS_COMMAND",
            "kind": "BUSINESS_COMMAND_INPUT",
            "conversation_id": actor.conversation_id,
            "input_batch_id": batch["input_batch_id"],
            "context_revision": batch["context_revision"],
            "selected_input_end_seq": batch["selected_input_end_seq"],
            "request": dict(request),
            "event": event,
            "input_parent_ids": sorted(
                {
                    request["input_payload_id"],
                    request["arguments_payload_id"],
                    request["policy_payload_id"],
                }
            ),
            "replayed": replayed,
        }

    async def _claim_business_command_in(
        self, conn, actor, binding, conversation, request, operation_id
    ) -> dict:
        if request["status"] == "CLAIMED":
            batch = await (
                await conn.execute(
                    "SELECT * FROM public.bot_context_input_batches "
                    "WHERE bot_id=%s AND input_batch_id=%s",
                    (self.store.bot_id, request["claim_input_batch_id"]),
                )
            ).fetchone()
            if batch is None or batch["kind"] != "BUSINESS_COMMAND_INPUT":
                raise ForestError(
                    "BUSINESS_COMMAND_CLAIM_INVALID", "Claimed command has no input boundary", 409
                )
            if (
                request["claim_actor_id"] != actor.actor_id
                or request["claim_actor_epoch"] != actor.actor_epoch
                or request["claim_host_incarnation"] != actor.host.incarnation
            ):
                request = await (
                    await conn.execute(
                        "UPDATE public.bot_business_command_requests SET claim_actor_id=%s,"
                        "claim_actor_epoch=%s,claim_host_incarnation=%s "
                        "WHERE bot_id=%s AND request_id=%s AND status='CLAIMED' RETURNING *",
                        (
                            actor.actor_id,
                            actor.actor_epoch,
                            actor.host.incarnation,
                            self.store.bot_id,
                            request["request_id"],
                        ),
                    )
                ).fetchone()
            selected = await self._business_command_selection_in(
                conn, actor, conversation, request, batch, replayed=True
            )
            await self._actor_final(conn, actor, binding)
            return selected

        applied = conversation["context_applied_seq"]
        end = max(applied, request["ingress_seq"])
        revision = conversation["context_revision"] + 1
        batch_id = str(uuid4())
        batch = await (
            await conn.execute(
                "INSERT INTO public.bot_context_input_batches(bot_id,input_batch_id,operation_id,"
                "conversation_id,kind,start_seq,end_seq,selected_input_end_seq,trigger_ids,"
                "context_revision) VALUES (%s,%s,%s,%s,'BUSINESS_COMMAND_INPUT',%s,%s,%s,'[]',%s) "
                "RETURNING *",
                (
                    self.store.bot_id,
                    batch_id,
                    operation_id,
                    actor.conversation_id,
                    applied + 1 if end > applied else None,
                    end if end > applied else None,
                    end,
                    revision,
                ),
            )
        ).fetchone()
        if end > applied:
            await conn.execute(
                "INSERT INTO public.bot_context_units(bot_id,conversation_id,unit_id,"
                "context_revision,unit_index,kind,event_id,input_batch_id,estimated_tokens) "
                "SELECT bot_id,conversation_id,event_id,%s,ingress_seq,'INPUT',event_id,%s,"
                "estimated_tokens FROM public.bot_message_inbox WHERE bot_id=%s "
                "AND conversation_id=%s AND ingress_seq>%s AND ingress_seq<=%s ORDER BY ingress_seq",
                (
                    revision,
                    batch_id,
                    self.store.bot_id,
                    actor.conversation_id,
                    applied,
                    end,
                ),
            )
        request = await (
            await conn.execute(
                "UPDATE public.bot_business_command_requests SET status='CLAIMED',claim_actor_id=%s,"
                "claim_actor_epoch=%s,claim_host_incarnation=%s,claim_input_batch_id=%s "
                "WHERE bot_id=%s AND request_id=%s AND status='PENDING' RETURNING *",
                (
                    actor.actor_id,
                    actor.actor_epoch,
                    actor.host.incarnation,
                    batch_id,
                    self.store.bot_id,
                    request["request_id"],
                ),
            )
        ).fetchone()
        if request is None:
            raise ForestError("BUSINESS_COMMAND_CLAIM_LOST", "Command claim changed", 409)
        await conn.execute(
            "UPDATE public.bot_conversations SET context_applied_seq=%s,context_revision=%s,"
            "mailbox_revision=mailbox_revision+1,updated_at=clock_timestamp() "
            "WHERE bot_id=%s AND conversation_id=%s",
            (end, revision, self.store.bot_id, actor.conversation_id),
        )
        await self._memory_sources().accept_batch_in(conn, conversation, batch)
        await self._compact(conn, actor.conversation_id)
        await self.store._event(
            conn,
            "business_command.claimed",
            request["request_id"],
            {
                "conversation_id": actor.conversation_id,
                "request_id": request["request_id"],
                "input_batch_id": batch_id,
            },
        )
        selected = await self._business_command_selection_in(
            conn, actor, conversation, request, batch, replayed=False
        )
        await self._actor_final(conn, actor, binding)
        return selected

    @staticmethod
    def _notice_selection(actor: ActorLease, notice: dict, batch: dict, *, replayed: bool) -> dict:
        return {
            "status": "NOTICE",
            "kind": "CONVERSATION_NOTICE_INPUT",
            "conversation_id": actor.conversation_id,
            "input_batch_id": batch["input_batch_id"],
            "context_revision": batch["context_revision"],
            "selected_input_end_seq": batch["selected_input_end_seq"],
            "notice": dict(notice),
            "replayed": replayed,
        }

    async def _claim_notice_in(
        self, conn, actor, binding, conversation, notice, operation_id
    ) -> dict:
        if notice["status"] == "CLAIMED":
            batch = await (
                await conn.execute(
                    "SELECT * FROM public.bot_context_input_batches "
                    "WHERE bot_id=%s AND input_batch_id=%s",
                    (self.store.bot_id, notice["claim_input_batch_id"]),
                )
            ).fetchone()
            if batch is None or batch["kind"] != "CONVERSATION_NOTICE_INPUT":
                raise ForestError(
                    "CONVERSATION_NOTICE_CLAIM_INVALID",
                    "Claimed notice has no input boundary",
                    409,
                )
            if (
                notice["claim_actor_id"] != actor.actor_id
                or notice["claim_actor_epoch"] != actor.actor_epoch
                or notice["claim_host_incarnation"] != actor.host.incarnation
            ):
                notice = await (
                    await conn.execute(
                        "UPDATE public.bot_conversation_notices SET claim_actor_id=%s,"
                        "claim_actor_epoch=%s,claim_host_incarnation=%s "
                        "WHERE bot_id=%s AND notice_id=%s AND status='CLAIMED' RETURNING *",
                        (
                            actor.actor_id,
                            actor.actor_epoch,
                            actor.host.incarnation,
                            self.store.bot_id,
                            notice["notice_id"],
                        ),
                    )
                ).fetchone()
            await self._actor_final(conn, actor, binding)
            return self._notice_selection(actor, notice, batch, replayed=True)

        applied = conversation["context_applied_seq"]
        end = max(applied, notice["ingress_seq"])
        revision = conversation["context_revision"] + 1
        batch_id = str(uuid4())
        batch = await (
            await conn.execute(
                "INSERT INTO public.bot_context_input_batches(bot_id,input_batch_id,operation_id,"
                "conversation_id,kind,start_seq,end_seq,selected_input_end_seq,trigger_ids,"
                "context_revision) VALUES "
                "(%s,%s,%s,%s,'CONVERSATION_NOTICE_INPUT',%s,%s,%s,'[]',%s) RETURNING *",
                (
                    self.store.bot_id,
                    batch_id,
                    operation_id,
                    actor.conversation_id,
                    applied + 1 if end > applied else None,
                    end if end > applied else None,
                    end,
                    revision,
                ),
            )
        ).fetchone()
        if end > applied:
            await conn.execute(
                "INSERT INTO public.bot_context_units(bot_id,conversation_id,unit_id,"
                "context_revision,unit_index,kind,event_id,input_batch_id,estimated_tokens) "
                "SELECT bot_id,conversation_id,event_id,%s,ingress_seq,'INPUT',event_id,%s,"
                "estimated_tokens FROM public.bot_message_inbox WHERE bot_id=%s "
                "AND conversation_id=%s AND ingress_seq>%s AND ingress_seq<=%s ORDER BY ingress_seq",
                (
                    revision,
                    batch_id,
                    self.store.bot_id,
                    actor.conversation_id,
                    applied,
                    end,
                ),
            )
        notice = await (
            await conn.execute(
                "UPDATE public.bot_conversation_notices SET status='CLAIMED',claim_actor_id=%s,"
                "claim_actor_epoch=%s,claim_host_incarnation=%s,claim_input_batch_id=%s "
                "WHERE bot_id=%s AND notice_id=%s AND status='PENDING' RETURNING *",
                (
                    actor.actor_id,
                    actor.actor_epoch,
                    actor.host.incarnation,
                    batch_id,
                    self.store.bot_id,
                    notice["notice_id"],
                ),
            )
        ).fetchone()
        if notice is None:
            raise ForestError("CONVERSATION_NOTICE_CLAIM_LOST", "Notice claim changed", 409)
        await conn.execute(
            "UPDATE public.bot_conversations SET context_applied_seq=%s,context_revision=%s,"
            "mailbox_revision=mailbox_revision+1,updated_at=clock_timestamp() "
            "WHERE bot_id=%s AND conversation_id=%s",
            (end, revision, self.store.bot_id, actor.conversation_id),
        )
        await self._memory_sources().accept_batch_in(conn, conversation, batch)
        await self._compact(conn, actor.conversation_id)
        await self.store._event(
            conn,
            "conversation.notice.claimed",
            notice["notice_id"],
            {
                "conversation_id": actor.conversation_id,
                "notice_id": notice["notice_id"],
                "kind": notice["kind"],
                "input_batch_id": batch_id,
            },
        )
        await self._actor_final(conn, actor, binding)
        return self._notice_selection(actor, notice, batch, replayed=False)

    async def _message_handler_authorized_in(self, conn, binding, chain, item) -> bool:
        """Recheck the current policy without changing the frozen execution route."""
        head = await (
            await conn.execute(
                "SELECT * FROM public.bot_message_handler_policy_heads WHERE bot_id=%s "
                "AND binding_id=%s AND component_id=%s AND handler_id=%s FOR SHARE",
                (
                    self.store.bot_id,
                    chain["binding_id"],
                    item["component_id"],
                    item["handler_id"],
                ),
            )
        ).fetchone()
        if head is None or head["revision"] < 1:
            return False
        policy = await (
            await conn.execute(
                "SELECT * FROM public.bot_message_handler_policy_revisions WHERE bot_id=%s "
                "AND binding_id=%s AND component_id=%s AND handler_id=%s AND revision=%s",
                (
                    self.store.bot_id,
                    chain["binding_id"],
                    item["component_id"],
                    item["handler_id"],
                    head["revision"],
                ),
            )
        ).fetchone()
        saved = await self.handler_chains.policies._hydrate_in(
            conn, policy, kind="REVISION", required=False
        )
        if (
            saved is None
            or saved["values_payload_state"] != "ACTIVE"
            or policy["handler_spec_digest"] != item["spec_digest"]
        ):
            return False
        values = saved["values"]
        if values["mode"] == "DISABLED":
            return False
        if values["mode"] == "EVERYONE":
            return True
        if values["mode"] == "USER_SET":
            return chain["external_user_id"] in values["user_ids"]
        current = await self.bindings.current_config_in(
            conn,
            self.store.bot_id,
            binding["binding_id"],
            expected_binding_epoch=binding["binding_epoch"],
        )
        return chain["external_user_id"] in current["config"]["admin_user_ids"]

    async def _message_handler_item_in(self, conn, chain_id, route_index) -> dict | None:
        """Resolve append-only takeover facts without rewriting the frozen route."""

        item = await (
            await conn.execute(
                "SELECT * FROM public.bot_message_handler_chain_items WHERE bot_id=%s "
                "AND chain_id=%s AND route_index=%s FOR SHARE",
                (self.store.bot_id, chain_id, route_index),
            )
        ).fetchone()
        if item is None:
            return None
        takeover = await (
            await conn.execute(
                "SELECT * FROM public.bot_message_handler_item_takeovers WHERE bot_id=%s "
                "AND chain_id=%s AND route_index=%s ORDER BY takeover_no DESC LIMIT 1",
                (self.store.bot_id, chain_id, route_index),
            )
        ).fetchone()
        result = dict(item)
        result.update(
            {
                "frozen_generation_id": item["generation_id"],
                "frozen_package_digest": item["package_digest"],
                "frozen_route_epoch": item["route_epoch"],
                "frozen_route_registry_revision": item["route_registry_revision"],
                "takeover_id": takeover["takeover_id"] if takeover else None,
                "takeover_no": takeover["takeover_no"] if takeover else 0,
            }
        )
        if takeover is not None:
            result.update(
                {
                    "generation_id": takeover["to_generation_id"],
                    "package_digest": takeover["to_package_digest"],
                    "route_epoch": takeover["to_route_epoch"],
                    "route_registry_revision": takeover["to_registry_revision"],
                }
            )
        return result

    async def _message_handler_selection_in(
        self, conn, actor, conversation, chain, item, attempt, *, replayed
    ) -> dict:
        from forest.storage.payloads import PayloadStore

        clock = await PayloadStore(self.store).lock_in(conn)
        source = await InputContentStore(self.store).read_input_in(
            conn,
            chain["event_id"],
            expected_deletion_epoch=clock["deletion_epoch"],
            required=False,
        )
        event = {
            "event_id": chain["event_id"],
            "source_event_id": source["source_event_id"],
            "ingress_seq": source["ingress_seq"],
            "platform_message_id": source["platform_message_id"],
            "sent_at": source["sent_at"].isoformat(),
            "received_at": source["received_at"].isoformat(),
            "origin": source["origin"],
            "payload_state": source["payload_state"],
            "conversation": {
                key: conversation[key]
                for key in (
                    "conversation_id",
                    "group_source_id",
                    "platform",
                    "channel_kind",
                    "channel_id",
                    "thread_id",
                )
            },
            "author": source["author"],
            "segments": source["segments"],
        }
        request = {
            **dict(item),
            "item_id": "message-handler-item:"
            + digest_json([chain["chain_id"], item["route_index"]]),
            "status": "CLAIMED",
            "handler_spec_digest": item["spec_digest"],
        }
        return {
            "status": "MESSAGE_HANDLER",
            "kind": "MESSAGE_HANDLER_INPUT",
            "conversation_id": actor.conversation_id,
            "chain": dict(chain),
            "request": request,
            "attempt": dict(attempt),
            "event": event,
            "input_parent_ids": sorted(
                {chain["input_payload_id"], item["policy_payload_id"]}
            ),
            "preflight_error": (
                None
                if source["payload_state"] == "ACTIVE"
                else source.get("content_error", "MESSAGE_HANDLER_INPUT_UNAVAILABLE")
            ),
            "replayed": replayed,
        }

    async def _claim_message_handler_in(
        self, conn, actor, binding, conversation, chain, operation_id
    ) -> dict:
        item = await self._message_handler_item_in(
            conn, chain["chain_id"], chain["current_index"]
        )
        if item is None:
            raise ForestError(
                "MESSAGE_HANDLER_CHAIN_INCOMPLETE", "Frozen handler item is unavailable", 409
            )
        if not await self._message_handler_authorized_in(conn, binding, chain, item):
            chain = await self.handler_chains.cancel_in(
                conn, chain["event_id"], "POLICY_REVOKED"
            )
            await self._classify_in(
                conn,
                actor.conversation_id,
                chain["event_id"],
                "NO_TRIGGER",
                "message-handler-policy-v1",
                "POLICY_REVOKED",
            )
            await self.store._event(
                conn,
                "message_handler.cancelled",
                chain["chain_id"],
                {"conversation_id": actor.conversation_id, "reason": "POLICY_REVOKED"},
            )
            await self._actor_final(conn, actor, binding)
            return {
                "status": "IDLE",
                "kind": "MESSAGE_HANDLER_POLICY_REVOKED",
                "conversation_id": actor.conversation_id,
            }
        replayed = chain["claim_actor_id"] is not None
        if replayed:
            attempt = await (
                await conn.execute(
                    "SELECT * FROM public.bot_message_handler_attempts WHERE bot_id=%s "
                    "AND chain_id=%s AND attempt_no=%s",
                    (self.store.bot_id, chain["chain_id"], chain["attempt_count"]),
                )
            ).fetchone()
            if attempt is None or attempt["route_index"] != chain["current_index"]:
                raise ForestError(
                    "MESSAGE_HANDLER_ATTEMPT_INVALID", "Claimed handler has no attempt proof", 409
                )
            if (
                chain["claim_actor_id"],
                chain["claim_actor_epoch"],
                chain["claim_host_incarnation"],
            ) != (actor.actor_id, actor.actor_epoch, actor.host.incarnation):
                chain = await (
                    await conn.execute(
                        "UPDATE public.bot_message_handler_chains SET claim_actor_id=%s,"
                        "claim_actor_epoch=%s,claim_host_incarnation=%s,revision=revision+1 "
                        "WHERE bot_id=%s AND chain_id=%s AND status='PENDING' RETURNING *",
                        (
                            actor.actor_id,
                            actor.actor_epoch,
                            actor.host.incarnation,
                            self.store.bot_id,
                            chain["chain_id"],
                        ),
                    )
                ).fetchone()
        else:
            attempt_no = chain["attempt_count"] + 1
            attempt_operation_id = "message-handler-attempt:" + digest_json(
                [chain["chain_id"], chain["current_index"], attempt_no]
            )
            call_operation_id = "message-handler-call:" + digest_json(
                [chain["chain_id"], chain["current_index"], attempt_no]
            )
            attempt = await (
                await conn.execute(
                    "INSERT INTO public.bot_message_handler_attempts("
                    "bot_id,chain_id,attempt_no,route_index,operation_id,call_operation_id,"
                    "actor_id,actor_epoch,host_incarnation) VALUES "
                    "(%s,%s,%s,%s,%s,%s,%s,%s,%s) RETURNING *",
                    (
                        self.store.bot_id,
                        chain["chain_id"],
                        attempt_no,
                        chain["current_index"],
                        attempt_operation_id,
                        call_operation_id,
                        actor.actor_id,
                        actor.actor_epoch,
                        actor.host.incarnation,
                    ),
                )
            ).fetchone()
            chain = await (
                await conn.execute(
                    "UPDATE public.bot_message_handler_chains SET claim_actor_id=%s,"
                    "claim_actor_epoch=%s,claim_host_incarnation=%s,attempt_count=%s,"
                    "revision=revision+1 WHERE bot_id=%s AND chain_id=%s "
                    "AND status='PENDING' AND claim_actor_id IS NULL RETURNING *",
                    (
                        actor.actor_id,
                        actor.actor_epoch,
                        actor.host.incarnation,
                        attempt_no,
                        self.store.bot_id,
                        chain["chain_id"],
                    ),
                )
            ).fetchone()
            if chain is None:
                raise ForestError(
                    "MESSAGE_HANDLER_CLAIM_LOST", "Handler claim changed", 409
                )
            await self.store._event(
                conn,
                "message_handler.claimed",
                attempt["operation_id"],
                {
                    "conversation_id": actor.conversation_id,
                    "chain_id": chain["chain_id"],
                    "route_index": chain["current_index"],
                    "attempt_no": attempt_no,
                },
            )
        selected = await self._message_handler_selection_in(
            conn, actor, conversation, chain, item, attempt, replayed=replayed
        )
        await self._actor_final(conn, actor, binding)
        return selected

    async def _message_handler_outputs_in(
        self, conn, conversation_id: str, start_seq: int, end_seq: int
    ) -> list[dict]:
        if end_seq <= start_seq:
            return []
        return await (
            await conn.execute(
                "SELECT q.* FROM ("
                "SELECT h.ingress_seq,h.chain_id,r.result_id AS output_id,"
                "r.context_payload_id,r.context_payload_digest,r.estimated_tokens,"
                "'RESULT' AS output_kind "
                "FROM public.bot_message_handler_chains h "
                "JOIN public.bot_message_handler_results r ON "
                "(r.bot_id,r.chain_id)=(h.bot_id,h.chain_id) "
                "WHERE h.bot_id=%s AND h.conversation_id=%s AND h.status='REPLIED' "
                "AND r.context_payload_id IS NOT NULL UNION ALL "
                "SELECT h.ingress_seq,h.chain_id,f.failure_id AS output_id,"
                "f.context_payload_id,f.context_payload_digest,f.estimated_tokens,"
                "'FAILURE' AS output_kind "
                "FROM public.bot_message_handler_chains h "
                "JOIN public.bot_message_handler_failures f ON "
                "(f.bot_id,f.chain_id)=(h.bot_id,h.chain_id) "
                "WHERE h.bot_id=%s AND h.conversation_id=%s AND h.status='FAILED' "
                "AND f.context_payload_id IS NOT NULL) q "
                "LEFT JOIN public.bot_context_units u ON u.bot_id=%s "
                "AND u.message_handler_chain_id=q.chain_id "
                "WHERE q.ingress_seq>%s AND q.ingress_seq<=%s AND u.unit_id IS NULL "
                "ORDER BY q.ingress_seq",
                (
                    self.store.bot_id,
                    conversation_id,
                    self.store.bot_id,
                    conversation_id,
                    self.store.bot_id,
                    start_seq,
                    end_seq,
                ),
            )
        ).fetchall()

    async def select_input(self, actor: ActorLease, operation_id: str, *, mode: str = "IDLE") -> dict:
        """Select exactly one immutable cut at a host-owned closed execution boundary.

        IDLE never changes an active Run's input. SAFE_POINT is exclusively for
        the foreground scheduler after the previous call/tool group has closed.
        A resumed run keeps its committed input; callers must inspect checkpoints
        before choosing another safe point in the future Agent implementation.
        """
        _text(operation_id, "operation_id")
        if mode not in {"IDLE", "SAFE_POINT"}:
            raise ValueError("mode must be IDLE or SAFE_POINT")
        async with self.store.pool.connection() as conn, conn.transaction():
            if mode == "SAFE_POINT":
                await self._require_legacy_run(conn, actor)
            return await self._select_input_in(conn, actor, operation_id, mode=mode)

    async def _select_input_in(self, conn, actor: ActorLease, operation_id: str, *, mode: str = "IDLE") -> dict:
        binding, conversation = await self._actor(conn, actor)
        from forest.storage.jobs import JobStore

        await JobStore.cancel_revoked_triggers_in(conn, self.store.bot_id, actor.conversation_id)
        existing = await (await conn.execute(
            "SELECT * FROM public.bot_context_input_batches WHERE bot_id=%s AND conversation_id=%s "
            "AND operation_id=%s", (self.store.bot_id, actor.conversation_id, operation_id),
        )).fetchone()
        if existing:
            if existing["kind"] == "BUSINESS_COMMAND_INPUT":
                request = await (
                    await conn.execute(
                        "SELECT * FROM public.bot_business_command_requests WHERE bot_id=%s "
                        "AND claim_input_batch_id=%s FOR UPDATE",
                        (self.store.bot_id, existing["input_batch_id"]),
                    )
                ).fetchone()
                if request is None or request["status"] != "CLAIMED":
                    raise ForestError(
                        "BUSINESS_COMMAND_CLAIM_INVALID",
                        "Command input boundary has no active request",
                        409,
                    )
                selected = await self._business_command_selection_in(
                    conn, actor, conversation, request, existing, replayed=True
                )
                await self._actor_final(conn, actor, binding)
                return selected
            if existing["kind"] == "CONVERSATION_NOTICE_INPUT":
                notice = await (
                    await conn.execute(
                        "SELECT * FROM public.bot_conversation_notices WHERE bot_id=%s "
                        "AND claim_input_batch_id=%s FOR UPDATE",
                        (self.store.bot_id, existing["input_batch_id"]),
                    )
                ).fetchone()
                if notice is None or notice["status"] != "CLAIMED":
                    raise ForestError(
                        "CONVERSATION_NOTICE_CLAIM_INVALID",
                        "Notice input boundary has no active notice",
                        409,
                    )
                await self._actor_final(conn, actor, binding)
                return self._notice_selection(actor, notice, existing, replayed=True)
            visible = await (await conn.execute(
                "SELECT COALESCE(sum(estimated_tokens),0) AS tokens FROM public.bot_context_units "
                "WHERE bot_id=%s AND conversation_id=%s AND visible",
                (self.store.bot_id, actor.conversation_id),
            )).fetchone()
            await self._actor_final(conn, actor, binding)
            return {"status": "INPUT" if visible["tokens"] <= self.context_token_budget else "INPUT_BUDGET_EXCEEDED",
                    **existing, "replayed": True}
        session = await self._session(conn, conversation)
        run = await self._run(conn, session)
        if run and mode == "SAFE_POINT":
            registered = await (await conn.execute(
                "SELECT phase FROM public.bot_agent_runs WHERE bot_id=%s AND run_id=%s",
                (self.store.bot_id, run["run_id"]),
            )).fetchone()
            if registered:
                unfinished = await (await conn.execute(
                    "SELECT 1 FROM public.bot_agent_steps WHERE bot_id=%s AND run_id=%s "
                    "AND status NOT IN ('CLOSED','FAILED') LIMIT 1", (self.store.bot_id, run["run_id"]),
                )).fetchone()
                if registered["phase"] not in {"READY_MODEL", "WAIT_INPUT"} or unfinished:
                    raise ForestError("AGENT_BOUNDARY_REQUIRED", "The previous model and tool group must close", 409)
        if mode == "SAFE_POINT" or run is None:
            await self._apply_context_requests_in(conn, actor.conversation_id)
        handler_chain = await (
            await conn.execute(
                "SELECT h.* FROM public.bot_message_handler_chains h "
                "JOIN public.bot_message_inbox i ON (i.bot_id,i.event_id)=(h.bot_id,h.event_id) "
                "WHERE h.bot_id=%s AND h.conversation_id=%s AND h.status='PENDING' "
                "AND NOT i.business_classification_pending ORDER BY h.ingress_seq LIMIT 1 FOR UPDATE OF h",
                (self.store.bot_id, actor.conversation_id),
            )
        ).fetchone()
        database_now = await self.store._now(conn)
        available_handler = (
            handler_chain
            if handler_chain is not None
            and conversation["context_applied_seq"] >= handler_chain["ingress_seq"] - 1
            and (
                handler_chain["claim_actor_id"] is not None
                or handler_chain["next_attempt_at"] <= database_now
                or handler_chain["deadline_at"] <= database_now
            )
            else None
        )
        if run and mode == "IDLE":
            if available_handler is not None:
                return await self._claim_message_handler_in(
                    conn, actor, binding, conversation, available_handler, operation_id
                )
            if await self._pending_echo_view(conn, actor.conversation_id):
                await self._compact(conn, actor.conversation_id)
            await self._actor_final(conn, actor, binding)
            agent = await (await conn.execute(
                "SELECT phase FROM public.bot_agent_runs WHERE bot_id=%s AND run_id=%s",
                (self.store.bot_id, run["run_id"]),
            )).fetchone()
            return {**run, "status": "RESUME", "agent_phase": agent["phase"] if agent else None}
        pending = await (await conn.execute(
            "SELECT * FROM public.bot_conversation_triggers WHERE bot_id=%s AND conversation_id=%s "
            "AND status='PENDING' ORDER BY ingress_seq", (self.store.bot_id, actor.conversation_id),
        )).fetchall()
        command = await (
            await conn.execute(
                "SELECT * FROM public.bot_business_command_requests WHERE bot_id=%s "
                "AND conversation_id=%s AND status IN ('PENDING','CLAIMED') "
                "ORDER BY ingress_seq LIMIT 1 FOR UPDATE",
                (self.store.bot_id, actor.conversation_id),
            )
        ).fetchone()
        available_command = (
            command
            if command is not None
            and command["ingress_seq"] <= conversation["classified_through_seq"]
            and (
                command["status"] == "PENDING"
                or command["next_attempt_at"] <= database_now
                or command["execution_deadline_at"] <= database_now
            )
            else None
        )
        notice = await (
            await conn.execute(
                "SELECT * FROM public.bot_conversation_notices WHERE bot_id=%s "
                "AND conversation_id=%s AND status IN ('PENDING','CLAIMED') "
                "ORDER BY ingress_seq LIMIT 1 FOR UPDATE",
                (self.store.bot_id, actor.conversation_id),
            )
        ).fetchone()
        available_notice = (
            notice
            if notice is not None
            and notice["ingress_seq"] <= conversation["classified_through_seq"]
            else None
        )
        business_work = min(
            (
                item
                for item in (available_command, available_notice, available_handler)
                if item is not None
            ),
            key=lambda item: item["ingress_seq"],
            default=None,
        )
        business_barrier = min(
            (
                item
                for item in (command, available_notice, available_handler)
                if item is not None
            ),
            key=lambda item: item["ingress_seq"],
            default=None,
        )
        selected = [
            trigger
            for trigger in pending
            if trigger["ingress_seq"] <= conversation["classified_through_seq"]
            and (
                business_barrier is None
                or trigger["ingress_seq"] < business_barrier["ingress_seq"]
            )
        ]
        if (
            run is not None
            and mode == "SAFE_POINT"
            and not selected
            and business_barrier is not None
            and business_barrier.get("chain_id") is not None
        ):
            await self._actor_final(conn, actor, binding)
            return {
                "status": "WAIT_INPUT",
                "conversation_id": actor.conversation_id,
                "run_id": run["run_id"],
            }
        if business_barrier is not None and not selected and run is None:
            if business_work is None or (
                business_work.get("notice_id") != business_barrier.get("notice_id")
                or business_work.get("request_id") != business_barrier.get("request_id")
            ):
                await conn.execute(
                    "UPDATE public.bot_conversations SET processed_mailbox_revision=mailbox_revision,"
                    "updated_at=clock_timestamp() WHERE bot_id=%s AND conversation_id=%s",
                    (self.store.bot_id, actor.conversation_id),
                )
                await self._actor_final(conn, actor, binding)
                return {
                    "status": "WAIT_INPUT",
                    "conversation_id": actor.conversation_id,
                    "run_id": None,
                }
            if business_barrier.get("notice_id") is not None:
                return await self._claim_notice_in(
                    conn, actor, binding, conversation, business_barrier, operation_id
                )
            if business_barrier.get("chain_id") is not None:
                if mode == "SAFE_POINT":
                    await self._actor_final(conn, actor, binding)
                    return {
                        "status": "WAIT_INPUT",
                        "conversation_id": actor.conversation_id,
                        "run_id": run["run_id"] if run else None,
                    }
                return await self._claim_message_handler_in(
                    conn, actor, binding, conversation, business_barrier, operation_id
                )
            return await self._claim_business_command_in(
                conn, actor, binding, conversation, business_barrier, operation_id
            )
        if pending and not selected and business_barrier is None:
            if session and not run:
                await conn.execute(
                    "UPDATE public.bot_conversation_sessions SET status='WAIT_INPUT' "
                    "WHERE bot_id=%s AND session_id=%s", (self.store.bot_id, session["session_id"]),
                )
            await self._actor_final(conn, actor, binding)
            return {"status": "WAIT_INPUT", "conversation_id": actor.conversation_id,
                    "run_id": run["run_id"] if run else None}
        if not selected and run:
            if await self._pending_echo_view(conn, actor.conversation_id):
                await self._compact(conn, actor.conversation_id)
            await self._actor_final(conn, actor, binding)
            return {**run, "status": "CONTINUE"}
        applied = conversation["context_applied_seq"]
        if selected:
            end = max(applied, selected[-1]["ingress_seq"])
        else:
            # Bounded silent maintenance never drags a frozen cut forward.
            end = min(conversation["classified_through_seq"], applied + 256)
        handler_outputs = await self._message_handler_outputs_in(
            conn, actor.conversation_id, applied, end
        )
        if selected:
            cut = selected[-1]["ingress_seq"]
            if run is None:
                session, run = await self._new_run(conn, conversation, session)
            kind = "TRIGGER_INPUT" if end > applied else "RECOVERY_CLAIM"
        else:
            cut = None
            kind = "SILENT_DRAIN"
        if end == applied and not selected:
            if await self._pending_echo_view(conn, actor.conversation_id):
                await self._compact(conn, actor.conversation_id)
            await self._close_if_drained(conn, conversation, session)
            await self._actor_final(conn, actor, binding)
            return {"status": "IDLE", "conversation_id": actor.conversation_id}
        batch_id = str(uuid4())
        revision = conversation["context_revision"] + 1
        input_revision = run["input_revision"] + 1 if run else None
        trigger_ids = [item["trigger_id"] for item in selected]
        batch = await (await conn.execute(
            "INSERT INTO public.bot_context_input_batches(bot_id,input_batch_id,operation_id,conversation_id,"
            "session_id,run_id,kind,start_seq,end_seq,trigger_cut_seq,selected_input_end_seq,trigger_ids,"
            "context_revision,input_revision) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) RETURNING *",
            (self.store.bot_id, batch_id, operation_id, actor.conversation_id,
             session["session_id"] if session else None, run["run_id"] if run else None, kind,
             applied + 1 if end > applied else None, end if end > applied else None,
             cut, end, Jsonb(trigger_ids), revision, input_revision),
        )).fetchone()
        if end > applied:
            await conn.execute(
                "INSERT INTO public.bot_context_units(bot_id,conversation_id,unit_id,context_revision,unit_index,"
                "kind,event_id,input_batch_id,estimated_tokens) SELECT bot_id,conversation_id,event_id,%s,"
                + (
                    "ingress_seq*2,'INPUT',event_id,%s,estimated_tokens "
                    if handler_outputs
                    else "ingress_seq,'INPUT',event_id,%s,estimated_tokens "
                )
                + "FROM public.bot_message_inbox "
                "WHERE bot_id=%s AND conversation_id=%s AND ingress_seq>%s AND ingress_seq<=%s "
                "ORDER BY ingress_seq",
                (revision, batch_id, self.store.bot_id, actor.conversation_id, applied, end),
            )
        if selected:
            await conn.execute(
                "UPDATE public.bot_conversation_triggers SET status='CLAIMED',claimed_session_id=%s,"
                "claimed_run_id=%s,input_batch_id=%s WHERE bot_id=%s AND trigger_id=ANY(%s) AND status='PENDING'",
                (session["session_id"], run["run_id"], batch_id, self.store.bot_id, trigger_ids),
            )
            # Old recovered triggers may already be archived. Their source
            # is pinned by identity, independently of the new input range.
            await conn.execute(
                "UPDATE public.bot_context_units u SET visible=true FROM public.bot_conversation_triggers t "
                "WHERE t.bot_id=u.bot_id AND t.event_id=u.event_id AND t.bot_id=%s AND t.trigger_id=ANY(%s)",
                (self.store.bot_id, trigger_ids),
            )
            await conn.execute(
                "UPDATE public.bot_conversation_runs SET input_revision=%s,selected_input_end_seq=%s "
                "WHERE bot_id=%s AND run_id=%s",
                (input_revision, end, self.store.bot_id, run["run_id"]),
            )
        for handler_output in handler_outputs:
            await conn.execute(
                "INSERT INTO public.bot_context_units(bot_id,conversation_id,unit_id,"
                "context_revision,unit_index,kind,payload_id,estimated_tokens,"
                "message_handler_chain_id) VALUES "
                "(%s,%s,%s,%s,%s,'MESSAGE_HANDLER_RESULT',%s,%s,%s)",
                (
                    self.store.bot_id,
                    actor.conversation_id,
                    handler_output["output_id"],
                    revision,
                    handler_output["ingress_seq"] * 2 + 1,
                    handler_output["context_payload_id"],
                    handler_output["estimated_tokens"],
                    handler_output["chain_id"],
                ),
            )
        await conn.execute(
            "UPDATE public.bot_conversations SET context_applied_seq=%s,context_revision=%s,"
            "mailbox_revision=mailbox_revision+1,updated_at=clock_timestamp() "
            "WHERE bot_id=%s AND conversation_id=%s",
            (end, revision, self.store.bot_id, actor.conversation_id),
        )
        await self._memory_sources().accept_batch_in(conn, conversation, batch)
        budget_ok = await self._compact(conn, actor.conversation_id)
        await self.store._event(conn, "conversation.input_applied", operation_id,
                                {"conversation_id": actor.conversation_id, "input_batch_id": batch_id})
        await self._actor_final(conn, actor, binding)
        expired_jobs = await (await conn.execute(
            "SELECT 1 FROM public.bot_job_occurrences WHERE bot_id=%s AND trigger_id=ANY(%s) "
            "AND NOT public.forest_job_trigger_authorized(bot_id,trigger_id) LIMIT 1",
            (self.store.bot_id, trigger_ids),
        )).fetchone()
        if expired_jobs:
            raise ForestError("JOB_DELEGATION_EXPIRED", "Job authority expired before input commit", 409)
        return {"status": "INPUT" if budget_ok else "INPUT_BUDGET_EXCEEDED", **batch,
                "replayed": False}

    async def _close_if_drained(self, conn, conversation, session):
        pending = await (await conn.execute(
            "SELECT 1 FROM public.bot_conversation_triggers WHERE bot_id=%s AND conversation_id=%s "
            "AND status IN ('PENDING','CLAIMED') LIMIT 1", (self.store.bot_id, conversation["conversation_id"]),
        )).fetchone()
        context_request = await (await conn.execute(
            "SELECT 1 FROM public.bot_context_management_requests "
            "WHERE bot_id=%s AND conversation_id=%s AND status='PENDING' LIMIT 1",
            (self.store.bot_id, conversation["conversation_id"]),
        )).fetchone()
        command = await (
            await conn.execute(
                "SELECT 1 FROM public.bot_business_command_requests WHERE bot_id=%s "
                "AND conversation_id=%s AND status IN ('PENDING','CLAIMED') LIMIT 1",
                (self.store.bot_id, conversation["conversation_id"]),
            )
        ).fetchone()
        notice = await (
            await conn.execute(
                "SELECT 1 FROM public.bot_conversation_notices WHERE bot_id=%s "
                "AND conversation_id=%s AND status IN ('PENDING','CLAIMED') LIMIT 1",
                (self.store.bot_id, conversation["conversation_id"]),
            )
        ).fetchone()
        handler = await (
            await conn.execute(
                "SELECT 1 FROM public.bot_message_handler_chains WHERE bot_id=%s "
                "AND conversation_id=%s AND status='PENDING' LIMIT 1",
                (self.store.bot_id, conversation["conversation_id"]),
            )
        ).fetchone()
        if pending or command or notice or handler or context_request or (session and session["active_run_id"]):
            return
        if conversation["context_applied_seq"] != conversation["classified_through_seq"]:
            return
        if session:
            await conn.execute(
                "UPDATE public.bot_conversation_sessions SET status='CLOSED',closed_at=clock_timestamp() "
                "WHERE bot_id=%s AND session_id=%s", (self.store.bot_id, session["session_id"]),
            )
        await conn.execute(
            "UPDATE public.bot_conversations SET active_session_id=NULL,processed_mailbox_revision=mailbox_revision "
            "WHERE bot_id=%s AND conversation_id=%s", (self.store.bot_id, conversation["conversation_id"]),
        )

    async def _pending_echo_view(self, conn, conversation_id) -> bool:
        return await (await conn.execute(
            "SELECT 1 FROM public.bot_message_echo_aliases a JOIN public.bot_context_units u "
            "ON u.bot_id=a.bot_id AND u.event_id=a.event_id WHERE a.bot_id=%s "
            "AND u.conversation_id=%s AND a.applied_view_revision IS NULL LIMIT 1",
            (self.store.bot_id, conversation_id),
        )).fetchone() is not None

    async def _apply_context_requests_in(self, conn, conversation_id, *, limit=32) -> int:
        """Let the current Actor publish queued view changes at a safe boundary."""
        requests = await (await conn.execute(
            "SELECT * FROM public.bot_context_management_requests "
            "WHERE bot_id=%s AND conversation_id=%s AND status='PENDING' "
            "ORDER BY requested_at,request_id LIMIT %s FOR UPDATE",
            (self.store.bot_id, conversation_id, limit),
        )).fetchall()
        applied = 0
        for request in requests:
            conversation = await self._conversation(conn, conversation_id)
            if conversation["context_view_revision"] != request["expected_view_revision"]:
                await conn.execute(
                    "UPDATE public.bot_context_management_requests SET status='CONFLICT',"
                    "terminal_reason='context_view_revision_changed',completed_at=clock_timestamp() "
                    "WHERE bot_id=%s AND request_id=%s AND status='PENDING'",
                    (self.store.bot_id, request["request_id"]),
                )
                continue
            if request["kind"] == "COMPACT":
                await self._compact(conn, conversation_id)
            elif request["kind"] == "RESET" and request["mode"] == "SAFE_POINT":
                await self._reset_context(conn, conversation_id)
            else:
                raise ForestError(
                    "CONTEXT_REQUEST_UNSUPPORTED",
                    "The current writer cannot interpret this Context request",
                    409,
                )
            current = await self._conversation(conn, conversation_id)
            await conn.execute(
                "UPDATE public.bot_context_management_requests SET status='APPLIED',"
                "applied_view_revision=%s,completed_at=clock_timestamp() "
                "WHERE bot_id=%s AND request_id=%s AND status='PENDING'",
                (current["context_view_revision"], self.store.bot_id, request["request_id"]),
            )
            applied += 1
        return applied

    async def _apply_echo_view(self, conn, conversation_id) -> list[str]:
        aliases = await (await conn.execute(
            "SELECT a.*,u.unit_id FROM public.bot_message_echo_aliases a JOIN public.bot_context_units u "
            "ON u.bot_id=a.bot_id AND u.event_id=a.event_id WHERE a.bot_id=%s "
            "AND u.conversation_id=%s AND a.applied_view_revision IS NULL ORDER BY u.context_revision,u.unit_index",
            (self.store.bot_id, conversation_id),
        )).fetchall()
        if not aliases:
            return []
        ids = [a["unit_id"] for a in aliases]
        await conn.execute(
            "UPDATE public.bot_context_units SET visible=false WHERE bot_id=%s AND unit_id=ANY(%s)",
            (self.store.bot_id, ids),
        )
        conversation = await self._conversation(conn, conversation_id)
        revision = conversation["context_revision"]
        index = 0
        from forest.storage.context_payloads import UNAVAILABLE_CODES, ContextPayloadStore
        from forest.storage.delivery_payloads import DeliveryContentStore

        content = ContextPayloadStore(self.store)
        clock = await content.payloads.lock_in(conn)
        # CONTROL outputs have no chat ModelTurn. Publish one safe audit/output
        # unit at this Actor boundary instead of retaining a duplicate self INPUT.
        for alias in aliases:
            if alias["origin"] != "control_output":
                continue
            present = await (await conn.execute(
                "SELECT 1 FROM public.bot_context_units WHERE bot_id=%s AND control_command_id=%s",
                (self.store.bot_id, alias["command_id"]),
            )).fetchone()
            if present:
                continue
            origins = await (await conn.execute(
                "SELECT i.payload_id FROM public.bot_message_inbox i WHERE i.bot_id=%s AND "
                "(i.event_id=%s OR i.event_id=(SELECT event_id FROM public.bot_shell_requests "
                "WHERE bot_id=%s AND command_id=%s)) ORDER BY i.event_id",
                (self.store.bot_id, alias["event_id"], self.store.bot_id, alias["command_id"]),
            )).fetchall()
            parents = [row["payload_id"] for row in origins]
            parts = await (await conn.execute(
                "SELECT * FROM public.bot_send_parts WHERE bot_id=%s AND intent_id=%s ORDER BY part_index",
                (self.store.bot_id, alias["intent_id"]),
            )).fetchall()
            parents.extend(part["payload_id"] for part in parts)
            if not parents or any(parent is None for parent in parents):
                raise ForestError("CONTEXT_PAYLOAD_MIGRATION_REQUIRED", "Control origin has no payload", 409)
            unavailable = None
            for parent in parents:
                try:
                    await content.payloads.require_available_in(
                        conn, (parent,), expected_deletion_epoch=clock["deletion_epoch"],
                    )
                except ForestError as exc:
                    if exc.code not in UNAVAILABLE_CODES:
                        raise
                    unavailable = parent
                    break
            if unavailable:
                # Preserve the control position using an already revoked head.
                # A late echo cannot copy the old delivery body into new context.
                payload_id, tokens = unavailable, 32
            else:
                delivery_content = DeliveryContentStore(self.store)
                payload = [await delivery_content.read_part_in(
                    conn, part, expected_deletion_epoch=clock["deletion_epoch"],
                ) for part in parts]
                body = await content.put_control_in(
                    conn, "control-output:" + alias["command_id"], payload, parents=parents,
                    expected_deletion_epoch=clock["deletion_epoch"],
                )
                payload_id, tokens = body.payload_id, _tokens(payload)
            if index == 0:
                revision += 1
            index += 1
            await conn.execute(
                "INSERT INTO public.bot_context_units(bot_id,conversation_id,unit_id,context_revision,unit_index,"
                "kind,control_command_id,payload_id,estimated_tokens) VALUES (%s,%s,%s,%s,%s,'CONTROL',%s,%s,%s)",
                (self.store.bot_id, conversation_id, "control-output:" + alias["command_id"], revision,
                 index, alias["command_id"], payload_id, tokens),
            )
        if index:
            await conn.execute(
                "UPDATE public.bot_conversations SET context_revision=%s WHERE bot_id=%s AND conversation_id=%s",
                (revision, self.store.bot_id, conversation_id),
            )
        await conn.execute(
            "UPDATE public.bot_message_echo_aliases SET applied_view_revision=%s WHERE bot_id=%s AND event_id=ANY(%s)",
            (conversation["context_view_revision"] + 1, self.store.bot_id, [a["event_id"] for a in aliases]),
        )
        return ids

    async def _compact(self, conn, conversation_id) -> bool:
        echo_units = await self._apply_echo_view(conn, conversation_id)
        conversation = await self._conversation(conn, conversation_id)
        rows = await (await conn.execute(
            "SELECT u.*, EXISTS(SELECT 1 FROM public.bot_conversation_triggers t WHERE t.bot_id=u.bot_id "
            "AND t.event_id=u.event_id AND t.status IN ('PENDING','CLAIMED')) OR EXISTS "
            "(SELECT 1 FROM public.bot_context_input_batches b JOIN public.bot_conversation_runs r "
            "ON r.bot_id=b.bot_id AND r.run_id=b.run_id WHERE b.bot_id=u.bot_id "
            "AND b.input_batch_id=u.input_batch_id AND r.status='RUNNING') OR EXISTS "
            "(SELECT 1 FROM public.bot_agent_steps s JOIN public.bot_conversation_runs r "
            "ON r.bot_id=s.bot_id AND r.run_id=s.run_id WHERE s.bot_id=u.bot_id "
            "AND s.logical_step_id=u.logical_step_id AND r.status='RUNNING') AS pinned "
            "FROM public.bot_context_units u WHERE u.bot_id=%s AND u.conversation_id=%s AND u.visible "
            "ORDER BY u.context_revision DESC,u.unit_index DESC", (self.store.bot_id, conversation_id),
        )).fetchall()
        total = sum(row["estimated_tokens"] for row in rows if row["pinned"])
        remove = list(echo_units)
        for row in rows:
            if row["pinned"]:
                continue
            if total + row["estimated_tokens"] <= self.context_token_budget:
                total += row["estimated_tokens"]
            else:
                remove.append(row["unit_id"])
        removed = set(remove)
        if removed:
            await conn.execute(
                "UPDATE public.bot_context_units SET visible=false WHERE bot_id=%s AND unit_id=ANY(%s)",
                (self.store.bot_id, remove),
            )
        view_revision = conversation["context_view_revision"] + 1
        retained = sorted(row["unit_id"] for row in rows if row["unit_id"] not in removed)
        await conn.execute(
            "INSERT INTO public.bot_context_compactions(bot_id,conversation_id,view_revision,context_revision,"
            "policy_revision,token_budget,archive_through_seq,source_manifest_digest,removed_unit_ids,"
            "retained_unit_ids) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
            (self.store.bot_id, conversation_id, view_revision, conversation["context_revision"],
             "byte-upper-bound-v1", self.context_token_budget, conversation["context_applied_seq"],
             digest_json({"removed": sorted(remove), "retained": retained}), Jsonb(sorted(remove)), Jsonb(retained)),
        )
        await conn.execute(
            "UPDATE public.bot_conversations SET context_view_revision=%s WHERE bot_id=%s AND conversation_id=%s",
            (view_revision, self.store.bot_id, conversation_id),
        )
        return total <= self.context_token_budget

    async def _reset_context(self, conn, conversation_id) -> None:
        """Archive every closed unit while retaining the active causal group."""
        echo_units = await self._apply_echo_view(conn, conversation_id)
        conversation = await self._conversation(conn, conversation_id)
        rows = await (await conn.execute(
            "SELECT u.*, EXISTS(SELECT 1 FROM public.bot_conversation_triggers t WHERE t.bot_id=u.bot_id "
            "AND t.event_id=u.event_id AND t.status IN ('PENDING','CLAIMED')) OR EXISTS "
            "(SELECT 1 FROM public.bot_context_input_batches b JOIN public.bot_conversation_runs r "
            "ON r.bot_id=b.bot_id AND r.run_id=b.run_id WHERE b.bot_id=u.bot_id "
            "AND b.input_batch_id=u.input_batch_id AND r.status='RUNNING') OR EXISTS "
            "(SELECT 1 FROM public.bot_agent_steps s JOIN public.bot_conversation_runs r "
            "ON r.bot_id=s.bot_id AND r.run_id=s.run_id WHERE s.bot_id=u.bot_id "
            "AND s.logical_step_id=u.logical_step_id AND r.status='RUNNING') AS pinned "
            "FROM public.bot_context_units u WHERE u.bot_id=%s AND u.conversation_id=%s AND u.visible "
            "ORDER BY u.context_revision,u.unit_index",
            (self.store.bot_id, conversation_id),
        )).fetchall()
        removed = sorted(set(echo_units) | {
            row["unit_id"] for row in rows if not row["pinned"]
        })
        retained = sorted(row["unit_id"] for row in rows if row["pinned"])
        if removed:
            await conn.execute(
                "UPDATE public.bot_context_units SET visible=false "
                "WHERE bot_id=%s AND unit_id=ANY(%s)",
                (self.store.bot_id, removed),
            )
        view_revision = conversation["context_view_revision"] + 1
        manifest = {
            "kind": "RESET",
            "mode": "SAFE_POINT",
            "removed": removed,
            "retained": retained,
        }
        await conn.execute(
            "INSERT INTO public.bot_context_compactions(bot_id,conversation_id,view_revision,context_revision,"
            "policy_revision,token_budget,archive_through_seq,source_manifest_digest,removed_unit_ids,"
            "retained_unit_ids) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
            (
                self.store.bot_id,
                conversation_id,
                view_revision,
                conversation["context_revision"],
                "explicit-safe-point-reset-v1",
                self.context_token_budget,
                conversation["context_applied_seq"],
                digest_json(manifest),
                Jsonb(removed),
                Jsonb(retained),
            ),
        )
        await conn.execute(
            "UPDATE public.bot_conversations SET context_view_revision=%s "
            "WHERE bot_id=%s AND conversation_id=%s",
            (view_revision, self.store.bot_id, conversation_id),
        )

    async def context(self, actor: ActorLease, *, run_id: str | None = None) -> dict:
        """Read canonical visible history with explicit source and author boundaries."""
        async with self.store.pool.connection() as conn, conn.transaction():
            return await self._context_in(conn, actor, run_id=run_id)

    async def _context_in(self, conn, actor: ActorLease, *, run_id: str | None = None) -> dict:
        binding, conversation = await self._actor(conn, actor)
        run = None
        if run_id is not None:
            run = await (await conn.execute(
                "SELECT * FROM public.bot_conversation_runs WHERE bot_id=%s AND run_id=%s "
                "AND conversation_id=%s", (self.store.bot_id, run_id, actor.conversation_id),
            )).fetchone()
            if run is None or run["status"] != "RUNNING":
                raise ForestError("RUN_NOT_ACTIVE", "Run is not active", 409)
        units = await (await conn.execute(
            "SELECT u.unit_id,u.kind,u.context_revision,u.unit_index,u.estimated_tokens,"
            "u.control_command_id,u.logical_step_id,u.business_request_id,u.business_notice_id,"
            "u.message_handler_chain_id,"
            "u.payload_id,"
            "s.run_id AS step_run_id,cr.disposition AS command_disposition,"
            "i.event_id,i.ingress_seq,i.platform_message_id,i.source_event_id,i.author,i.segments,"
            "i.sent_at,i.received_at,i.trigger_decision,i.context_disposition,i.origin,r.reply_id,"
            "t.status AS source_trigger_status "
            "FROM public.bot_context_units u LEFT JOIN public.bot_message_inbox i "
            "ON i.bot_id=u.bot_id AND i.event_id=u.event_id LEFT JOIN public.bot_conversation_replies r "
            "ON r.bot_id=u.bot_id AND r.reply_id=u.reply_id LEFT JOIN public.bot_conversation_triggers t "
            "ON t.bot_id=i.bot_id AND t.event_id=i.event_id LEFT JOIN public.bot_agent_steps s "
            "ON s.bot_id=u.bot_id AND s.logical_step_id=u.logical_step_id "
            "LEFT JOIN public.bot_business_command_results cr ON "
            "cr.bot_id=u.bot_id AND cr.request_id=u.business_request_id "
            "WHERE u.bot_id=%s AND u.conversation_id=%s "
            "AND u.visible ORDER BY u.context_revision,u.unit_index", (self.store.bot_id, actor.conversation_id),
        )).fetchall()
        source = {key: conversation[key] for key in
                  ("conversation_id", "group_source_id", "platform", "channel_kind", "channel_id", "thread_id")}
        from forest.storage.context_payloads import ContextPayloadStore
        from forest.storage.input_payloads import InputContentStore

        content = ContextPayloadStore(self.store)
        inputs = InputContentStore(self.store)
        clock = await content.payloads.lock_in(conn)
        payload_ids = set()
        initial = await self.bindings.initial_in(conn, binding) if any(
            u["kind"] in {
                "CONTROL", "COMMAND_RESULT", "SYSTEM_NOTICE", "MESSAGE_HANDLER_RESULT"
            }
            for u in units
        ) else None
        for unit in units:
            unit["step_parts"] = unit["reply_parts"] = None
            if unit["kind"] == "INPUT":
                hydrated = await inputs.read_input_in(
                    conn, unit["event_id"], expected_deletion_epoch=clock["deletion_epoch"],
                )
                unit.update({key: hydrated[key] for key in ("author", "segments", "payload_id", "payload_state")})
            else:
                hydrated = await content.read_parts_in(
                    conn, unit["payload_id"], expected_deletion_epoch=clock["deletion_epoch"],
                )
                unit["payload_state"] = hydrated["payload_state"]
                parts = hydrated["value"]
                if parts is None:
                    unavailable = {"type": "unavailable", "reason": unit["payload_state"]}
                    parts = ([{"unit_id": unit["unit_id"] + ":unavailable", "role": "assistant",
                               "parts": [unavailable]}] if unit["kind"] == "RUN_STEP" else [[unavailable]])
                    unit["estimated_tokens"] = _tokens(parts)
                unit["step_parts" if unit["kind"] == "RUN_STEP" else "reply_parts"] = parts
            if unit["payload_state"] == "ACTIVE":
                payload_ids.add(unit["payload_id"])
            elif unit["kind"] == "INPUT":
                unit["estimated_tokens"] = _tokens(unit["segments"])
            unit["source"] = source.copy()
            unit["context_role"] = (
                "assistant"
                if unit["kind"] in {
                    "REPLY", "RUN_STEP", "COMMAND_RESULT", "SYSTEM_NOTICE",
                    "MESSAGE_HANDLER_RESULT",
                }
                else
                "control_data" if unit["kind"] == "CONTROL" or unit["trigger_decision"] == "CONTROL_COMMAND" else "user"
            )
            if unit["kind"] == "CONTROL":
                unit["origin"] = "control_output"
                unit["author"] = {"external_user_id": binding["self_id"],
                                  "display_name": initial["display_name"] if initial["display_name_payload_state"] == "ACTIVE" else binding["self_id"],
                                  "identity_origin": "host_output", "is_self": True}
                if initial["display_name_payload_state"] == "ACTIVE":
                    payload_ids.add(initial["initial_payload_id"])
            if unit["kind"] in {"COMMAND_RESULT", "SYSTEM_NOTICE"}:
                unit["origin"] = (
                    "business_command_output"
                    if unit["kind"] == "COMMAND_RESULT"
                    else "business_command_notice"
                )
                unit["author"] = {
                    "external_user_id": binding["self_id"],
                    "display_name": initial["display_name"]
                    if initial["display_name_payload_state"] == "ACTIVE"
                    else binding["self_id"],
                    "identity_origin": "host_command_output",
                    "is_self": True,
                }
                if initial["display_name_payload_state"] == "ACTIVE":
                    payload_ids.add(initial["initial_payload_id"])
            if unit["kind"] == "MESSAGE_HANDLER_RESULT":
                unit["origin"] = "message_handler_output"
                unit["author"] = {
                    "external_user_id": binding["self_id"],
                    "display_name": initial["display_name"]
                    if initial["display_name_payload_state"] == "ACTIVE"
                    else binding["self_id"],
                    "identity_origin": "host_handler_output",
                    "is_self": True,
                }
                if initial["display_name_payload_state"] == "ACTIVE":
                    payload_ids.add(initial["initial_payload_id"])
            if unit["origin"] == "job":
                unit["source"] = {**source, "source_kind": "SYSTEM", "platform": "forest",
                                  "target_platform": source["platform"],
                                  "job_id": unit["author"]["job_id"],
                                  "occurrence_id": unit["author"]["occurrence_id"],
                                  "task_status": unit["source_trigger_status"]}
                unit["context_role"] = "control_data"
                if unit["source_trigger_status"] == "CANCELLED":
                    unit["segments"] = [{"type": "text", "text": "该计划任务已取消；此处仅保留历史记录。"}]
            if unit["context_disposition"] in {"RESTRICTED", "INVALID"}:
                unit["segments"] = [{"type": "unavailable", "reason": unit["context_disposition"]}]
        archive = await (await conn.execute(
            "SELECT min(i.ingress_seq) AS first_seq,max(i.ingress_seq) AS last_seq,count(*) AS count "
            "FROM public.bot_context_units u JOIN public.bot_message_inbox i ON i.bot_id=u.bot_id "
            "AND i.event_id=u.event_id WHERE u.bot_id=%s AND u.conversation_id=%s AND NOT u.visible",
            (self.store.bot_id, actor.conversation_id),
        )).fetchone()
        # Bounds are descriptive only: pinned trigger holes make a range insufficient.
        archive["units"] = await (await conn.execute(
            "SELECT u.unit_id,u.kind,u.event_id,u.reply_id,u.logical_step_id,u.business_request_id,"
            "i.ingress_seq,i.source_event_id "
            "FROM public.bot_context_units u LEFT JOIN public.bot_message_inbox i "
            "ON i.bot_id=u.bot_id AND i.event_id=u.event_id WHERE u.bot_id=%s "
            "AND u.conversation_id=%s AND NOT u.visible ORDER BY u.context_revision,u.unit_index",
            (self.store.bot_id, actor.conversation_id),
        )).fetchall()
        archive["source"] = source.copy()
        triggers = await (await conn.execute(
            "SELECT trigger_id,event_id,ingress_seq,principal_ref FROM public.bot_conversation_triggers "
            "WHERE bot_id=%s AND claimed_run_id=%s AND status='CLAIMED' ORDER BY ingress_seq",
            (self.store.bot_id, run_id),
        )).fetchall() if run else []
        if run and any(u["ingress_seq"] is not None and u["ingress_seq"] > run["selected_input_end_seq"] for u in units):
            raise ForestError("INPUT_CUT_VIOLATION", "Run cannot read queued input", 409)
        await self._actor_final(conn, actor, binding)
        return {"conversation": source, "context_revision": conversation["context_revision"],
                "context_view_revision": conversation["context_view_revision"],
                "context_applied_seq": conversation["context_applied_seq"], "units": units,
                "archive": archive, "run": run, "triggers": triggers,
                "payload_ids": sorted(payload_ids), "deletion_epoch": clock["deletion_epoch"],
                "budget_exceeded": sum(u["estimated_tokens"] for u in units) > self.context_token_budget}

    async def record_business_command_failure(
        self,
        actor: ActorLease,
        operation_id: str,
        *,
        request_id: str,
        error_code: str,
        retryable: bool,
        retry_delay_seconds: float = 0.5,
        max_attempts: int = 3,
    ) -> dict:
        """Persist one failed attempt; terminal failure becomes ordered notice work."""
        _text(operation_id, "operation_id")
        _text(request_id, "request_id")
        _text(error_code, "error_code", 96)
        if (
            not error_code.isascii()
            or not error_code.replace("_", "").isalnum()
            or not error_code[0].isalpha()
            or error_code != error_code.upper()
            or type(retryable) is not bool
            or not isinstance(retry_delay_seconds, (int, float))
            or isinstance(retry_delay_seconds, bool)
            or not 0 <= retry_delay_seconds <= 300
            or type(max_attempts) is not int
            or not 1 <= max_attempts <= 32
        ):
            raise ForestError("INVALID_COMMAND_FAILURE", "Invalid command failure record", 422)
        async with self.store.pool.connection() as conn, conn.transaction():
            binding, conversation = await self._actor(conn, actor)
            existing = await (
                await conn.execute(
                    "SELECT * FROM public.bot_business_command_attempts WHERE bot_id=%s "
                    "AND operation_id=%s FOR UPDATE",
                    (self.store.bot_id, operation_id),
                )
            ).fetchone()
            if existing is not None:
                if existing["request_id"] != request_id or existing["error_code"] != error_code:
                    raise ForestError(
                        "BUSINESS_COMMAND_ATTEMPT_CONFLICT",
                        "Command failure operation was already used",
                        409,
                    )
                request = await (
                    await conn.execute(
                        "SELECT * FROM public.bot_business_command_requests "
                        "WHERE bot_id=%s AND request_id=%s",
                        (self.store.bot_id, request_id),
                    )
                ).fetchone()
                await self._actor_final(conn, actor, binding)
                return {
                    "request_id": request_id,
                    "attempt_no": existing["attempt_no"],
                    "status": request["status"],
                    "retry_at": existing["retry_at"],
                    "error_code": error_code,
                }
            request = await (
                await conn.execute(
                    "SELECT * FROM public.bot_business_command_requests WHERE bot_id=%s "
                    "AND request_id=%s FOR UPDATE",
                    (self.store.bot_id, request_id),
                )
            ).fetchone()
            if (
                request is None
                or request["conversation_id"] != actor.conversation_id
                or request["status"] != "CLAIMED"
                or request["claim_actor_id"] != actor.actor_id
                or request["claim_actor_epoch"] != actor.actor_epoch
                or request["claim_host_incarnation"] != actor.host.incarnation
            ):
                raise ForestError(
                    "BUSINESS_COMMAND_NOT_CLAIMED",
                    "Command failure does not own the current Conversation Actor",
                    409,
                )
            now = (await (await conn.execute("SELECT clock_timestamp() AS now")).fetchone())["now"]
            attempt_no = request["execution_attempt_count"] + 1
            retry_at = now + timedelta(seconds=retry_delay_seconds)
            will_retry = (
                retryable
                and attempt_no < max_attempts
                and retry_at < request["execution_deadline_at"]
            )
            await conn.execute(
                "INSERT INTO public.bot_business_command_attempts("
                "bot_id,request_id,attempt_no,operation_id,actor_id,actor_epoch,"
                "host_incarnation,error_code,retryable,retry_at) "
                "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
                (
                    self.store.bot_id,
                    request_id,
                    attempt_no,
                    operation_id,
                    actor.actor_id,
                    actor.actor_epoch,
                    actor.host.incarnation,
                    error_code,
                    will_retry,
                    retry_at if will_retry else None,
                ),
            )
            if will_retry:
                await conn.execute(
                    "UPDATE public.bot_business_command_requests SET execution_attempt_count=%s,"
                    "next_attempt_at=%s,last_error_code=%s WHERE bot_id=%s AND request_id=%s "
                    "AND status='CLAIMED'",
                    (attempt_no, retry_at, error_code, self.store.bot_id, request_id),
                )
                await conn.execute(
                    "UPDATE public.bot_conversations SET processed_mailbox_revision=mailbox_revision,"
                    "updated_at=clock_timestamp() WHERE bot_id=%s AND conversation_id=%s",
                    (self.store.bot_id, actor.conversation_id),
                )
                status = "RETRY_WAIT"
            else:
                await conn.execute(
                    "UPDATE public.bot_business_command_requests SET status='FAILED',"
                    "terminal_reason=%s,completed_at=clock_timestamp(),execution_attempt_count=%s,"
                    "last_error_code=%s WHERE bot_id=%s AND request_id=%s AND status='CLAIMED'",
                    (error_code, attempt_no, error_code, self.store.bot_id, request_id),
                )
                await self._put_notice_in(
                    conn,
                    event_id=request["event_id"],
                    request_id=request_id,
                    conversation_id=actor.conversation_id,
                    ingress_seq=request["ingress_seq"],
                    input_payload_id=request["input_payload_id"],
                    kind="BUSINESS_COMMAND_FAILED",
                    error_code=error_code,
                )
                await conn.execute(
                    "UPDATE public.bot_conversations SET mailbox_revision=mailbox_revision+1,"
                    "updated_at=clock_timestamp() WHERE bot_id=%s AND conversation_id=%s",
                    (self.store.bot_id, actor.conversation_id),
                )
                status = "FAILED"
            await self.store._event(
                conn,
                "business_command.attempt_failed",
                operation_id,
                {
                    "conversation_id": actor.conversation_id,
                    "request_id": request_id,
                    "attempt_no": attempt_no,
                    "error_code": error_code,
                    "status": status,
                },
            )
            await self._actor_final(conn, actor, binding)
            return {
                "request_id": request_id,
                "attempt_no": attempt_no,
                "status": status,
                "retry_at": retry_at if will_retry else None,
                "error_code": error_code,
            }

    async def finalize_notice(
        self,
        actor: ActorLease,
        operation_id: str,
        *,
        notice_id: str,
        delivery,
    ) -> dict:
        """Atomically publish a safe Actor-ordered notice and CHAT intent."""
        _text(operation_id, "operation_id")
        _text(notice_id, "notice_id")
        async with self.store.pool.connection() as conn, conn.transaction():
            binding, conversation = await self._actor(conn, actor)
            existing = await (
                await conn.execute(
                    "SELECT * FROM public.bot_conversation_notice_results WHERE bot_id=%s "
                    "AND (notice_id=%s OR (conversation_id=%s AND operation_id=%s)) FOR UPDATE",
                    (self.store.bot_id, notice_id, actor.conversation_id, operation_id),
                )
            ).fetchone()
            if existing is not None:
                if existing["notice_id"] != notice_id or existing["operation_id"] != operation_id:
                    raise ForestError(
                        "CONVERSATION_NOTICE_RESULT_CONFLICT",
                        "Notice result operation was already used",
                        409,
                    )
                await self._actor_final(conn, actor, binding)
                return existing
            notice = await (
                await conn.execute(
                    "SELECT * FROM public.bot_conversation_notices WHERE bot_id=%s "
                    "AND notice_id=%s FOR UPDATE",
                    (self.store.bot_id, notice_id),
                )
            ).fetchone()
            if (
                notice is None
                or notice["conversation_id"] != actor.conversation_id
                or notice["status"] != "CLAIMED"
                or notice["claim_actor_id"] != actor.actor_id
                or notice["claim_actor_epoch"] != actor.actor_epoch
                or notice["claim_host_incarnation"] != actor.host.incarnation
            ):
                raise ForestError(
                    "CONVERSATION_NOTICE_NOT_CLAIMED",
                    "Notice does not own the current Conversation Actor",
                    409,
                )
            texts = {
                "BUSINESS_COMMAND_DENIED": "你没有执行此业务命令的权限。",
                "BUSINESS_COMMAND_FAILED": "业务命令执行失败，请稍后重试。",
                "BUSINESS_CLASSIFICATION_FAILED": "业务命令暂时无法处理，请稍后重试。",
                "MESSAGE_HANDLER_REJECTED": "消息处理器当前繁忙，请稍后再试。",
            }
            parts = [[{"type": "text", "text": texts[notice["kind"]]}]]
            from forest.storage.context_payloads import ContextPayloadStore

            content = ContextPayloadStore(self.store)
            clock = await content.payloads.lock_in(conn)
            result_id = "conversation-notice-result:" + digest_json(
                [self.store.bot_id, notice_id]
            )
            body = await content.put_control_in(
                conn,
                result_id,
                parts,
                parents=(notice["input_payload_id"],),
                expected_deletion_epoch=clock["deletion_epoch"],
            )
            response_seq = conversation["next_response_seq"]
            route = {
                "bot_id": self.store.bot_id,
                "binding_id": conversation["binding_id"],
                "platform": conversation["platform"],
                "self_id": binding["self_id"],
                "channel_kind": conversation["channel_kind"],
                "channel_id": conversation["channel_id"],
                "thread_id": conversation["thread_id"],
                "binding_epoch": binding["binding_epoch"],
                "visibility": conversation["channel_kind"],
            }
            delivery_receipt = await delivery.enqueue_in(
                conn,
                actor.host,
                operation_id="conversation-notice-reply:" + notice_id,
                route=route,
                parts=parts,
                conversation_id=actor.conversation_id,
                lane="CHAT",
                response_seq=response_seq,
                root_operation_id=notice_id,
                expected_policy_revision=actor.policy_revision,
                parents=(body.payload_id,),
            )
            semantic_digest = digest_json(
                [notice_id, notice["kind"], notice["error_code"], body.payload_id, parts]
            )
            row = await (
                await conn.execute(
                    "INSERT INTO public.bot_conversation_notice_results("
                    "bot_id,result_id,operation_id,notice_id,conversation_id,context_payload_id,"
                    "context_payload_digest,result_digest,response_seq,delivery) "
                    "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) RETURNING *",
                    (
                        self.store.bot_id,
                        result_id,
                        operation_id,
                        notice_id,
                        actor.conversation_id,
                        body.payload_id,
                        body.digest,
                        semantic_digest,
                        response_seq,
                        Jsonb(delivery_receipt),
                    ),
                )
            ).fetchone()
            revision = conversation["context_revision"] + 1
            await conn.execute(
                "INSERT INTO public.bot_context_units(bot_id,conversation_id,unit_id,"
                "context_revision,unit_index,kind,payload_id,estimated_tokens,business_notice_id) "
                "VALUES (%s,%s,%s,%s,0,'SYSTEM_NOTICE',%s,%s,%s)",
                (
                    self.store.bot_id,
                    actor.conversation_id,
                    result_id,
                    revision,
                    body.payload_id,
                    _tokens(parts),
                    notice_id,
                ),
            )
            await conn.execute(
                "UPDATE public.bot_conversation_notices SET status='COMPLETED',"
                "completed_at=clock_timestamp() WHERE bot_id=%s AND notice_id=%s "
                "AND status='CLAIMED'",
                (self.store.bot_id, notice_id),
            )
            await conn.execute(
                "UPDATE public.bot_conversations SET context_revision=%s,"
                "next_response_seq=next_response_seq+1,mailbox_revision=mailbox_revision+1,"
                "updated_at=clock_timestamp() WHERE bot_id=%s AND conversation_id=%s",
                (revision, self.store.bot_id, actor.conversation_id),
            )
            session = await self._session(conn, conversation)
            current = await self._conversation(conn, actor.conversation_id)
            await self._close_if_drained(conn, current, session)
            await self._compact(conn, actor.conversation_id)
            await self.store._event(
                conn,
                "conversation.notice.completed",
                operation_id,
                {
                    "conversation_id": actor.conversation_id,
                    "notice_id": notice_id,
                    "kind": notice["kind"],
                    "response_seq": response_seq,
                },
            )
            await self._actor_final(conn, actor, binding)
            return row

    async def finalize_message_handler(
        self,
        actor: ActorLease,
        operation_id: str,
        *,
        chain_id: str,
        call_operation_id: str,
        result: dict,
        result_payload_id: str,
        result_payload_digest: str,
        delivery,
    ) -> dict:
        """Commit one frozen route result; one Pass consumes one fair quantum."""
        for value, name in (
            (operation_id, "operation_id"),
            (chain_id, "chain_id"),
            (call_operation_id, "call_operation_id"),
            (result_payload_id, "result_payload_id"),
            (result_payload_digest, "result_payload_digest"),
        ):
            _text(value, name)
        value = _json(result, 262144)
        if not isinstance(value, dict) or value.get("kind") not in {
            "REPLY", "CONSUMED", "PASS"
        }:
            raise ForestError(
                "INVALID_MESSAGE_HANDLER_RESULT",
                "Handler result is not a typed SDK value",
                422,
            )
        if value["kind"] == "REPLY":
            parts = value.get("parts")
            if (
                set(value) != {"kind", "parts"}
                or not isinstance(parts, list)
                or not parts
                or any(not isinstance(part, list) or not part for part in parts)
            ):
                raise ForestError(
                    "INVALID_MESSAGE_HANDLER_RESULT", "Handler reply is malformed", 422
                )
        elif set(value) != {"kind"}:
            raise ForestError(
                "INVALID_MESSAGE_HANDLER_RESULT", "Handler disposition has extra fields", 422
            )
        async with self.store.pool.connection() as conn, conn.transaction():
            binding, conversation = await self._actor(conn, actor)
            existing = await (
                await conn.execute(
                    "SELECT * FROM public.bot_message_handler_results WHERE bot_id=%s "
                    "AND conversation_id=%s AND operation_id=%s "
                    "ORDER BY created_at DESC LIMIT 1 FOR UPDATE",
                    (self.store.bot_id, actor.conversation_id, operation_id),
                )
            ).fetchone()
            semantic_digest = digest_json(
                [chain_id, call_operation_id, result_payload_id, result_payload_digest, value]
            )
            if existing is not None:
                if (
                    existing["chain_id"] != chain_id
                    or existing["operation_id"] != operation_id
                    or existing["result_digest"] != semantic_digest
                ):
                    raise ForestError(
                        "MESSAGE_HANDLER_RESULT_CONFLICT",
                        "Handler result operation was already used",
                        409,
                    )
                await self._actor_final(conn, actor, binding)
                return existing
            chain = await (
                await conn.execute(
                    "SELECT * FROM public.bot_message_handler_chains WHERE bot_id=%s "
                    "AND chain_id=%s FOR UPDATE",
                    (self.store.bot_id, chain_id),
                )
            ).fetchone()
            if (
                chain is None
                or chain["conversation_id"] != actor.conversation_id
                or chain["status"] != "PENDING"
                or (
                    chain["claim_actor_id"],
                    chain["claim_actor_epoch"],
                    chain["claim_host_incarnation"],
                )
                != (actor.actor_id, actor.actor_epoch, actor.host.incarnation)
            ):
                raise ForestError(
                    "MESSAGE_HANDLER_NOT_CLAIMED",
                    "Handler result does not own the current Conversation Actor",
                    409,
                )
            item = await self._message_handler_item_in(
                conn, chain_id, chain["current_index"]
            )
            attempt = await (
                await conn.execute(
                    "SELECT * FROM public.bot_message_handler_attempts WHERE bot_id=%s "
                    "AND chain_id=%s AND attempt_no=%s FOR SHARE",
                    (self.store.bot_id, chain_id, chain["attempt_count"]),
                )
            ).fetchone()
            if attempt is None or attempt["call_operation_id"] != call_operation_id:
                raise ForestError(
                    "MESSAGE_HANDLER_ATTEMPT_INVALID", "Handler call does not match its attempt", 409
                )
            if not await self._message_handler_authorized_in(conn, binding, chain, item):
                raise ForestError(
                    "MESSAGE_HANDLER_POLICY_REVOKED",
                    "Handler reply authorization changed before commit",
                    403,
                )
            call = await (
                await conn.execute(
                    "SELECT * FROM public.bot_calls WHERE bot_id=%s AND operation_id=%s FOR SHARE",
                    (self.store.bot_id, call_operation_id),
                )
            ).fetchone()
            if (
                call is None
                or call["status"] != "COMPLETED"
                or call["component_id"] != item["component_id"]
                or call["generation_id"] != item["generation_id"]
                or call["route_epoch"] != item["route_epoch"]
                or call["result_payload_id"] != result_payload_id
                or call["result_digest"] != result_payload_digest
            ):
                raise ForestError(
                    "MESSAGE_HANDLER_CALL_INVALID",
                    "Handler result has no matching durable Kernel receipt",
                    409,
                )
            from forest.storage.context_payloads import ContextPayloadStore

            content = ContextPayloadStore(self.store)
            clock = await content.payloads.lock_in(conn)
            await content.payloads.require_available_in(
                conn,
                (result_payload_id,),
                expected_deletion_epoch=clock["deletion_epoch"],
            )
            result_id = "message-handler-result:" + digest_json(
                [self.store.bot_id, chain_id, chain["current_index"]]
            )
            body = None
            response_seq = delivery_receipt = None
            if value["kind"] == "REPLY":
                body = await content.put_control_in(
                    conn,
                    result_id,
                    value["parts"],
                    parents=(result_payload_id,),
                    expected_deletion_epoch=clock["deletion_epoch"],
                )
                response_seq = conversation["next_response_seq"]
                route = {
                    "bot_id": self.store.bot_id,
                    "binding_id": conversation["binding_id"],
                    "platform": conversation["platform"],
                    "self_id": binding["self_id"],
                    "channel_kind": conversation["channel_kind"],
                    "channel_id": conversation["channel_id"],
                    "thread_id": conversation["thread_id"],
                    "binding_epoch": binding["binding_epoch"],
                    "visibility": conversation["channel_kind"],
                }
                delivery_receipt = await delivery.enqueue_in(
                    conn,
                    actor.host,
                    operation_id="message-handler-reply:" + chain_id,
                    route=route,
                    parts=value["parts"],
                    conversation_id=actor.conversation_id,
                    lane="CHAT",
                    response_seq=response_seq,
                    root_operation_id=chain_id,
                    expected_policy_revision=actor.policy_revision,
                    parents=(body.payload_id,),
                )
            row = await (
                await conn.execute(
                    "INSERT INTO public.bot_message_handler_results("
                    "bot_id,result_id,operation_id,chain_id,route_index,attempt_no,"
                    "conversation_id,disposition,call_operation_id,call_result_payload_id,"
                    "call_result_digest,context_payload_id,context_payload_digest,estimated_tokens,"
                    "result_digest,response_seq,delivery) VALUES "
                    "(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) RETURNING *",
                    (
                        self.store.bot_id,
                        result_id,
                        operation_id,
                        chain_id,
                        chain["current_index"],
                        chain["attempt_count"],
                        actor.conversation_id,
                        value["kind"],
                        call_operation_id,
                        result_payload_id,
                        result_payload_digest,
                        body.payload_id if body else None,
                        body.digest if body else None,
                        _tokens(value["parts"]) if body else 0,
                        semantic_digest,
                        response_seq,
                        Jsonb(delivery_receipt) if delivery_receipt is not None else None,
                    ),
                )
            ).fetchone()
            next_index = chain["current_index"] + 1
            if value["kind"] == "PASS":
                final = next_index == chain["route_count"]
                chain = await (
                    await conn.execute(
                        "UPDATE public.bot_message_handler_chains SET status=%s,current_index=%s,"
                        "claim_actor_id=NULL,claim_actor_epoch=NULL,claim_host_incarnation=NULL,"
                        "last_error_code=NULL,completed_at=CASE WHEN %s THEN clock_timestamp() END,"
                        "terminal_reason=CASE WHEN %s THEN 'ALL_PASSED' END,revision=revision+1 "
                        "WHERE bot_id=%s AND chain_id=%s RETURNING *",
                        (
                            "PASSED" if final else "PENDING",
                            next_index,
                            final,
                            final,
                            self.store.bot_id,
                            chain_id,
                        ),
                    )
                ).fetchone()
                if final and chain["fallback_decision"] != "PENDING":
                    await self._classify_in(
                        conn,
                        actor.conversation_id,
                        chain["event_id"],
                        chain["fallback_decision"],
                        chain["fallback_rule_revision"],
                        chain["fallback_reason"],
                    )
            else:
                chain = await (
                    await conn.execute(
                        "UPDATE public.bot_message_handler_chains SET status=%s,current_index=%s,"
                        "claim_actor_id=NULL,claim_actor_epoch=NULL,claim_host_incarnation=NULL,"
                        "last_error_code=NULL,terminal_reason=%s,completed_at=clock_timestamp(),"
                        "revision=revision+1 WHERE bot_id=%s AND chain_id=%s RETURNING *",
                        (
                            "REPLIED" if value["kind"] == "REPLY" else "CONSUMED",
                            next_index,
                            value["kind"],
                            self.store.bot_id,
                            chain_id,
                        ),
                    )
                ).fetchone()
                await self._classify_in(
                    conn,
                    actor.conversation_id,
                    chain["event_id"],
                    "NO_TRIGGER",
                    "message-handler-terminal-v1",
                    value["kind"],
                )
            await conn.execute(
                "UPDATE public.bot_conversations SET next_response_seq=next_response_seq+%s,"
                "mailbox_revision=mailbox_revision+1,updated_at=clock_timestamp() "
                "WHERE bot_id=%s AND conversation_id=%s",
                (1 if response_seq is not None else 0, self.store.bot_id, actor.conversation_id),
            )
            await self.store._event(
                conn,
                "message_handler.completed",
                operation_id,
                {
                    "conversation_id": actor.conversation_id,
                    "chain_id": chain_id,
                    "route_index": row["route_index"],
                    "disposition": value["kind"],
                    "response_seq": response_seq,
                },
            )
            await self._actor_final(conn, actor, binding)
            return row

    async def record_message_handler_failure(
        self,
        actor: ActorLease,
        operation_id: str,
        *,
        chain_id: str,
        error_code: str,
        retryable: bool,
        retry_delay_seconds: float,
        max_attempts: int,
        delivery,
    ) -> dict:
        _text(operation_id, "operation_id")
        _text(chain_id, "chain_id")
        _text(error_code, "error_code", 96)
        if (
            not error_code.isascii()
            or not error_code.replace("_", "A").isalnum()
            or not error_code[0].isalpha()
            or error_code.upper() != error_code
            or type(retryable) is not bool
            or isinstance(retry_delay_seconds, bool)
            or not isinstance(retry_delay_seconds, (int, float))
            or not 0 <= retry_delay_seconds <= 300
            or type(max_attempts) is not int
            or not 1 <= max_attempts <= 32
        ):
            raise ForestError(
                "MESSAGE_HANDLER_FAILURE_INVALID", "Handler failure fields are invalid", 422
            )
        async with self.store.pool.connection() as conn, conn.transaction():
            binding, conversation = await self._actor(conn, actor)
            existing = await (
                await conn.execute(
                    "SELECT * FROM public.bot_message_handler_failures WHERE bot_id=%s "
                    "AND operation_id=%s FOR UPDATE",
                    (self.store.bot_id, operation_id),
                )
            ).fetchone()
            if existing is not None:
                await self._actor_final(conn, actor, binding)
                return existing
            chain = await (
                await conn.execute(
                    "SELECT * FROM public.bot_message_handler_chains WHERE bot_id=%s "
                    "AND chain_id=%s FOR UPDATE",
                    (self.store.bot_id, chain_id),
                )
            ).fetchone()
            if (
                chain is None
                or chain["conversation_id"] != actor.conversation_id
                or chain["status"] != "PENDING"
                or (
                    chain["claim_actor_id"],
                    chain["claim_actor_epoch"],
                    chain["claim_host_incarnation"],
                )
                != (actor.actor_id, actor.actor_epoch, actor.host.incarnation)
            ):
                raise ForestError(
                    "MESSAGE_HANDLER_NOT_CLAIMED", "Handler failure has no current claim", 409
                )
            attempt = await (
                await conn.execute(
                    "SELECT * FROM public.bot_message_handler_attempts WHERE bot_id=%s "
                    "AND chain_id=%s AND attempt_no=%s FOR SHARE",
                    (self.store.bot_id, chain_id, chain["attempt_count"]),
                )
            ).fetchone()
            call = await (
                await conn.execute(
                    "SELECT status FROM public.bot_calls WHERE bot_id=%s AND operation_id=%s",
                    (self.store.bot_id, attempt["call_operation_id"]),
                )
            ).fetchone()
            if call is not None and call["status"] in {"ADMITTED", "COMPLETED"}:
                raise ForestError(
                    "MESSAGE_HANDLER_RECOVERY_REQUIRED",
                    "A durable handler call must be replayed before failure can be recorded",
                    503,
                )
            database_now = await self.store._now(conn)
            retry_at = database_now + timedelta(seconds=float(retry_delay_seconds))
            retry = bool(
                retryable
                and chain["attempt_count"] < max_attempts
                and retry_at < chain["deadline_at"]
            )
            from forest.storage.context_payloads import ContextPayloadStore

            failure_id = "message-handler-failure:" + digest_json(
                [self.store.bot_id, chain_id, chain["attempt_count"]]
            )
            body = None
            response_seq = delivery_receipt = None
            if not retry:
                parts = [[{"type": "text", "text": "消息处理器暂时无法完成请求，请稍后重试。"}]]
                content = ContextPayloadStore(self.store)
                clock = await content.payloads.lock_in(conn)
                body = await content.put_control_in(
                    conn,
                    failure_id,
                    parts,
                    parents=(chain["input_payload_id"],),
                    expected_deletion_epoch=clock["deletion_epoch"],
                )
                response_seq = conversation["next_response_seq"]
                route = {
                    "bot_id": self.store.bot_id,
                    "binding_id": conversation["binding_id"],
                    "platform": conversation["platform"],
                    "self_id": binding["self_id"],
                    "channel_kind": conversation["channel_kind"],
                    "channel_id": conversation["channel_id"],
                    "thread_id": conversation["thread_id"],
                    "binding_epoch": binding["binding_epoch"],
                    "visibility": conversation["channel_kind"],
                }
                delivery_receipt = await delivery.enqueue_in(
                    conn,
                    actor.host,
                    operation_id="message-handler-failure-reply:" + chain_id,
                    route=route,
                    parts=parts,
                    conversation_id=actor.conversation_id,
                    lane="CHAT",
                    response_seq=response_seq,
                    root_operation_id=chain_id,
                    expected_policy_revision=actor.policy_revision,
                    parents=(body.payload_id,),
                )
            row = await (
                await conn.execute(
                    "INSERT INTO public.bot_message_handler_failures("
                    "bot_id,failure_id,operation_id,chain_id,route_index,attempt_no,"
                    "conversation_id,error_code,retryable,retry_at,context_payload_id,"
                    "context_payload_digest,estimated_tokens,response_seq,delivery) VALUES "
                    "(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) RETURNING *",
                    (
                        self.store.bot_id,
                        failure_id,
                        operation_id,
                        chain_id,
                        chain["current_index"],
                        chain["attempt_count"],
                        actor.conversation_id,
                        error_code,
                        retry,
                        retry_at if retry else None,
                        body.payload_id if body else None,
                        body.digest if body else None,
                        _tokens(parts) if body else 0,
                        response_seq,
                        Jsonb(delivery_receipt) if delivery_receipt is not None else None,
                    ),
                )
            ).fetchone()
            if retry:
                await conn.execute(
                    "UPDATE public.bot_message_handler_chains SET claim_actor_id=NULL,"
                    "claim_actor_epoch=NULL,claim_host_incarnation=NULL,next_attempt_at=%s,"
                    "last_error_code=%s,revision=revision+1 WHERE bot_id=%s AND chain_id=%s",
                    (retry_at, error_code, self.store.bot_id, chain_id),
                )
            else:
                await conn.execute(
                    "UPDATE public.bot_message_handler_chains SET status='FAILED',"
                    "claim_actor_id=NULL,claim_actor_epoch=NULL,claim_host_incarnation=NULL,"
                    "last_error_code=%s,terminal_reason=%s,completed_at=clock_timestamp(),"
                    "revision=revision+1 WHERE bot_id=%s AND chain_id=%s",
                    (error_code, error_code, self.store.bot_id, chain_id),
                )
                await self._classify_in(
                    conn,
                    actor.conversation_id,
                    chain["event_id"],
                    "FAILED",
                    "message-handler-failure-v1",
                    error_code,
                )
            await conn.execute(
                "UPDATE public.bot_conversations SET next_response_seq=next_response_seq+%s,"
                "mailbox_revision=mailbox_revision+1,updated_at=clock_timestamp() "
                "WHERE bot_id=%s AND conversation_id=%s",
                (1 if response_seq is not None else 0, self.store.bot_id, actor.conversation_id),
            )
            await self.store._event(
                conn,
                "message_handler.failed",
                operation_id,
                {
                    "conversation_id": actor.conversation_id,
                    "chain_id": chain_id,
                    "attempt_no": chain["attempt_count"],
                    "error_code": error_code,
                    "retryable": retry,
                },
            )
            await self._actor_final(conn, actor, binding)
            return row

    async def finalize_business_command(
        self,
        actor: ActorLease,
        operation_id: str,
        *,
        request_id: str,
        call_operation_id: str,
        result: dict,
        result_payload_id: str,
        result_payload_digest: str,
        delivery,
    ) -> dict:
        """Publish one typed command result, Context position and optional CHAT intent."""
        _text(operation_id, "operation_id")
        _text(request_id, "request_id")
        _text(call_operation_id, "call_operation_id")
        _text(result_payload_id, "result_payload_id")
        _text(result_payload_digest, "result_payload_digest")
        value = _json(result, 262144)
        if not isinstance(value, dict) or value.get("kind") not in {
            "REPLY",
            "CONSUMED",
            "PASS",
        }:
            raise ForestError("INVALID_COMMAND_RESULT", "Command result is not a typed SDK value", 422)
        if value["kind"] == "REPLY":
            parts = value.get("parts")
            if (
                set(value) != {"kind", "parts"}
                or not isinstance(parts, list)
                or not parts
                or any(not isinstance(part, list) or not part for part in parts)
            ):
                raise ForestError("INVALID_COMMAND_RESULT", "Command reply is malformed", 422)
        elif set(value) != {"kind"}:
            raise ForestError("INVALID_COMMAND_RESULT", "Command disposition has extra fields", 422)
        async with self.store.pool.connection() as conn, conn.transaction():
            binding, conversation = await self._actor(conn, actor)
            existing = await (
                await conn.execute(
                    "SELECT * FROM public.bot_business_command_results WHERE bot_id=%s "
                    "AND (request_id=%s OR (conversation_id=%s AND operation_id=%s)) FOR UPDATE",
                    (self.store.bot_id, request_id, actor.conversation_id, operation_id),
                )
            ).fetchone()
            semantic_digest = digest_json(
                [request_id, call_operation_id, result_payload_id, result_payload_digest, value]
            )
            if existing is not None:
                if (
                    existing["request_id"] != request_id
                    or existing["operation_id"] != operation_id
                    or existing["result_digest"] != semantic_digest
                ):
                    raise ForestError(
                        "BUSINESS_COMMAND_RESULT_CONFLICT",
                        "Command result operation was already used",
                        409,
                    )
                await self._actor_final(conn, actor, binding)
                return existing
            request = await (
                await conn.execute(
                    "SELECT * FROM public.bot_business_command_requests WHERE bot_id=%s "
                    "AND request_id=%s FOR UPDATE",
                    (self.store.bot_id, request_id),
                )
            ).fetchone()
            if (
                request is None
                or request["conversation_id"] != actor.conversation_id
                or request["status"] != "CLAIMED"
                or request["claim_actor_id"] != actor.actor_id
                or request["claim_actor_epoch"] != actor.actor_epoch
                or request["claim_host_incarnation"] != actor.host.incarnation
            ):
                raise ForestError(
                    "BUSINESS_COMMAND_NOT_CLAIMED",
                    "Command result does not own the current Conversation Actor",
                    409,
                )
            call = await (
                await conn.execute(
                    "SELECT * FROM public.bot_calls WHERE bot_id=%s AND operation_id=%s FOR SHARE",
                    (self.store.bot_id, call_operation_id),
                )
            ).fetchone()
            if (
                call is None
                or call["status"] != "COMPLETED"
                or call["component_id"] != request["component_id"]
                or call["generation_id"] != request["generation_id"]
                or call["route_epoch"] != request["route_epoch"]
                or call["result_payload_id"] != result_payload_id
                or call["result_digest"] != result_payload_digest
            ):
                raise ForestError(
                    "BUSINESS_COMMAND_CALL_INVALID",
                    "Command result has no matching durable Kernel receipt",
                    409,
                )
            from forest.storage.context_payloads import ContextPayloadStore

            content = ContextPayloadStore(self.store)
            clock = await content.payloads.lock_in(conn)
            await content.payloads.require_available_in(
                conn,
                (result_payload_id,),
                expected_deletion_epoch=clock["deletion_epoch"],
            )
            marker = {
                "CONSUMED": "该业务命令已处理，没有产生群聊回复。",
                "PASS": "该业务命令没有处理器接纳，且不会回落到 Agent。",
            }
            context_parts = (
                value["parts"]
                if value["kind"] == "REPLY"
                else [[{"type": "text", "text": marker[value["kind"]]}]]
            )
            result_id = "business-command-result:" + digest_json(
                [self.store.bot_id, request_id]
            )
            body = await content.put_control_in(
                conn,
                result_id,
                context_parts,
                parents=(result_payload_id,),
                expected_deletion_epoch=clock["deletion_epoch"],
            )
            response_seq = delivery_receipt = None
            if value["kind"] == "REPLY":
                response_seq = conversation["next_response_seq"]
                route = {
                    "bot_id": self.store.bot_id,
                    "binding_id": conversation["binding_id"],
                    "platform": conversation["platform"],
                    "self_id": binding["self_id"],
                    "channel_kind": conversation["channel_kind"],
                    "channel_id": conversation["channel_id"],
                    "thread_id": conversation["thread_id"],
                    "binding_epoch": binding["binding_epoch"],
                    "visibility": conversation["channel_kind"],
                }
                delivery_receipt = await delivery.enqueue_in(
                    conn,
                    actor.host,
                    operation_id="business-command-reply:" + request_id,
                    route=route,
                    parts=value["parts"],
                    conversation_id=actor.conversation_id,
                    lane="CHAT",
                    response_seq=response_seq,
                    root_operation_id=request_id,
                    expected_policy_revision=actor.policy_revision,
                    parents=(body.payload_id,),
                )
            terminal_reason = "COMMAND_NOT_HANDLED" if value["kind"] == "PASS" else None
            row = await (
                await conn.execute(
                    "INSERT INTO public.bot_business_command_results(bot_id,result_id,operation_id,"
                    "request_id,conversation_id,disposition,call_operation_id,call_result_payload_id,"
                    "call_result_digest,context_payload_id,context_payload_digest,result_digest,"
                    "response_seq,delivery,terminal_reason) VALUES "
                    "(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) RETURNING *",
                    (
                        self.store.bot_id,
                        result_id,
                        operation_id,
                        request_id,
                        actor.conversation_id,
                        value["kind"],
                        call_operation_id,
                        result_payload_id,
                        result_payload_digest,
                        body.payload_id,
                        body.digest,
                        semantic_digest,
                        response_seq,
                        Jsonb(delivery_receipt) if delivery_receipt is not None else None,
                        terminal_reason,
                    ),
                )
            ).fetchone()
            revision = conversation["context_revision"] + 1
            await conn.execute(
                "INSERT INTO public.bot_context_units(bot_id,conversation_id,unit_id,"
                "context_revision,unit_index,kind,payload_id,estimated_tokens,business_request_id) "
                "VALUES (%s,%s,%s,%s,0,'COMMAND_RESULT',%s,%s,%s)",
                (
                    self.store.bot_id,
                    actor.conversation_id,
                    result_id,
                    revision,
                    body.payload_id,
                    _tokens(context_parts),
                    request_id,
                ),
            )
            await conn.execute(
                "UPDATE public.bot_business_command_requests SET status=%s,terminal_reason=%s,"
                "completed_at=clock_timestamp() WHERE bot_id=%s AND request_id=%s "
                "AND status='CLAIMED'",
                (
                    "FAILED" if value["kind"] == "PASS" else "COMPLETED",
                    terminal_reason,
                    self.store.bot_id,
                    request_id,
                ),
            )
            await conn.execute(
                "UPDATE public.bot_conversations SET context_revision=%s,"
                "next_response_seq=next_response_seq+%s,mailbox_revision=mailbox_revision+1,"
                "updated_at=clock_timestamp() WHERE bot_id=%s AND conversation_id=%s",
                (
                    revision,
                    1 if response_seq is not None else 0,
                    self.store.bot_id,
                    actor.conversation_id,
                ),
            )
            session = await self._session(conn, conversation)
            current = await self._conversation(conn, actor.conversation_id)
            await self._close_if_drained(conn, current, session)
            await self._compact(conn, actor.conversation_id)
            await self.store._event(
                conn,
                "business_command.completed",
                operation_id,
                {
                    "conversation_id": actor.conversation_id,
                    "request_id": request_id,
                    "result_id": result_id,
                    "disposition": value["kind"],
                    "response_seq": response_seq,
                },
            )
            await self._actor_final(conn, actor, binding)
            return row

    async def finalize_reply(
        self, actor: ActorLease, operation_id: str, *, run_id: str,
        expected_input_revision: int, reply_parts: list[list[dict]], delivery, result_parent_ids=(),
    ) -> dict:
        """Commit canonical answer, claims and the actual SendIntent together."""
        _text(operation_id, "operation_id")
        parts = _json(reply_parts)
        if not isinstance(parts, list) or not parts or any(not isinstance(p, list) or not p for p in parts):
            raise ForestError("INVALID_REPLY", "Reply needs nonempty ordered delivery parts", 422)
        async with self.store.pool.connection() as conn, conn.transaction():
            await self._require_legacy_run(conn, actor, run_id=run_id)
            return await self._finalize_reply_in(conn, actor, operation_id, run_id=run_id, expected_input_revision=expected_input_revision, reply_parts=parts, delivery=delivery, result_parent_ids=result_parent_ids)

    async def _finalize_reply_in(
        self, conn, actor: ActorLease, operation_id: str, *, run_id: str,
        expected_input_revision: int, reply_parts: list[list[dict]], delivery, result_parent_ids=(),
    ) -> dict:
        parts = _json(reply_parts)
        value_digest = digest_json([run_id, expected_input_revision, parts])
        if result_parent_ids:
            value_digest = digest_json([run_id, expected_input_revision, parts, sorted(set(result_parent_ids))])
        binding, conversation = await self._actor(conn, actor)
        existing = await (await conn.execute(
            "SELECT * FROM public.bot_conversation_replies WHERE bot_id=%s AND conversation_id=%s AND operation_id=%s",
            (self.store.bot_id, actor.conversation_id, operation_id),
        )).fetchone()
        if existing:
            if existing["input_digest"] != value_digest:
                raise ForestError("REPLY_CONFLICT", "Reply operation was already used", 409)
            from forest.storage.context_payloads import ContextPayloadStore

            content = ContextPayloadStore(self.store)
            clock = await content.payloads.lock_in(conn)
            existing = await content.read_reply_in(
                conn, existing, expected_deletion_epoch=clock["deletion_epoch"],
            )
            if result_parent_ids:
                from forest.storage.payload_scope import guard_payload_scope_in

                original_run = await (await conn.execute(
                    "SELECT selected_input_end_seq FROM public.bot_conversation_runs "
                    "WHERE bot_id=%s AND conversation_id=%s AND run_id=%s",
                    (self.store.bot_id, actor.conversation_id, existing["run_id"]),
                )).fetchone()
                if original_run is None:
                    raise ForestError("PAYLOAD_SCOPE_DENIED", "Reply sources are unavailable", 409)
                await guard_payload_scope_in(
                    conn, self.store, conversation, original_run["selected_input_end_seq"],
                    result_parent_ids,
                )
            if existing["payload_state"] == "ACTIVE":
                await self._memory_sources().register_reply_in(conn, conversation, existing, reconstructed=True)
            await self._actor_final(conn, actor, binding)
            return existing
        session = await self._session(conn, conversation)
        run = await self._run(conn, session)
        if not run or run["run_id"] != run_id or run["status"] != "RUNNING":
            raise ForestError("RUN_NOT_ACTIVE", "Reply does not own the foreground run", 409)
        if run["input_revision"] != expected_input_revision:
            raise ForestError("INPUT_REVISION_CONFLICT", "Reply was generated from another input revision", 409)
        agent = await (await conn.execute(
            "SELECT a.*,s.status AS final_status,s.output_payload_id AS final_output_payload_id FROM public.bot_agent_runs a "
            "LEFT JOIN public.bot_agent_steps s ON s.bot_id=a.bot_id AND s.logical_step_id=a.final_step_id "
            "WHERE a.bot_id=%s AND a.run_id=%s", (self.store.bot_id, run_id),
        )).fetchone()
        from forest.storage.context_payloads import ContextPayloadStore

        content = ContextPayloadStore(self.store)
        clock = await content.payloads.lock_in(conn)
        if agent and agent["phase"] == "FINAL_READY" and agent["final_status"] == "FINAL_READY":
            from forest.storage.agent_payloads import AgentPayloadStore

            agent["final_output"] = await AgentPayloadStore(self.store).read_output_in(conn, agent["final_step_id"])
        if agent and (agent["phase"] != "FINAL_READY" or agent["final_status"] != "FINAL_READY"
                      or agent["final_input_revision"] != expected_input_revision
                      or agent["actor_epoch"] != actor.actor_epoch
                      or agent["actor_host_incarnation"] != actor.host.incarnation
                      or parts != [[{"type": "text", "text": agent["final_output"]["text"]}]]):
            raise ForestError("AGENT_FINAL_NOT_READY", "Only the persisted Agent final candidate can publish", 409)
        logical_step_id = agent["final_step_id"] if agent else None
        reply_id = str(uuid4())
        parents = ([agent["final_output_payload_id"]] if agent else
                   (list(result_parent_ids) or (await self._context_in(conn, actor, run_id=run_id))["payload_ids"]))
        if not agent and result_parent_ids:
            from forest.storage.payload_scope import guard_payload_scope_in

            await guard_payload_scope_in(
                conn, self.store, conversation, run["selected_input_end_seq"], parents,
            )
        body = await content.put_reply_in(
            conn, reply_id, parts, parents=parents, expected_deletion_epoch=clock["deletion_epoch"],
        )
        response_seq = conversation["next_response_seq"]
        route = {"bot_id": self.store.bot_id, "binding_id": conversation["binding_id"],
                 "platform": conversation["platform"], "self_id": binding["self_id"],
                 "channel_kind": conversation["channel_kind"], "channel_id": conversation["channel_id"],
                 "thread_id": conversation["thread_id"], "binding_epoch": binding["binding_epoch"],
                 "visibility": conversation["channel_kind"]}
        delivery_receipt = await delivery.enqueue_in(
            conn, actor.host, operation_id="reply:" + reply_id, route=route, parts=parts,
            conversation_id=actor.conversation_id, lane="CHAT", response_seq=response_seq,
            root_operation_id=run_id, expected_policy_revision=actor.policy_revision,
            parents=(body.payload_id,),
        )
        reply = await (await conn.execute(
            "INSERT INTO public.bot_conversation_replies(bot_id,reply_id,operation_id,conversation_id,run_id,"
            "input_revision,response_seq,payload_id,input_digest,delivery,logical_step_id) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) RETURNING *",
            (self.store.bot_id, reply_id, operation_id, actor.conversation_id, run_id,
             expected_input_revision, response_seq, body.payload_id, value_digest, Jsonb(delivery_receipt), logical_step_id),
        )).fetchone()
        revision = conversation["context_revision"] + 1
        await conn.execute(
            "INSERT INTO public.bot_context_units(bot_id,conversation_id,unit_id,context_revision,unit_index,"
            "kind,reply_id,payload_id,estimated_tokens) VALUES (%s,%s,%s,%s,0,'REPLY',%s,%s,%s)",
            (self.store.bot_id, actor.conversation_id, reply_id, revision, reply_id, body.payload_id, _tokens(parts)),
        )
        reply = {**reply, "parts": parts, "payload_state": "ACTIVE"}
        await self._memory_sources().register_reply_in(
            conn, conversation, reply,
            dependency_payload_ids=parents if agent or result_parent_ids else None,
        )
        await conn.execute(
            "UPDATE public.bot_conversation_triggers SET status='ANSWERED',answered_reply_id=%s "
            "WHERE bot_id=%s AND claimed_run_id=%s AND status='CLAIMED'",
            (reply_id, self.store.bot_id, run_id),
        )
        await conn.execute(
            "UPDATE public.bot_conversation_runs SET status='COMPLETED',completed_at=clock_timestamp() "
            "WHERE bot_id=%s AND run_id=%s", (self.store.bot_id, run_id),
        )
        await self._after_run(conn, conversation, session)
        await conn.execute(
            "UPDATE public.bot_conversations SET context_revision=%s,next_response_seq=next_response_seq+1,"
            "mailbox_revision=mailbox_revision+1,updated_at=clock_timestamp() "
            "WHERE bot_id=%s AND conversation_id=%s",
            (revision, self.store.bot_id, actor.conversation_id),
        )
        await self._compact(conn, actor.conversation_id)
        await self.store._event(conn, "conversation.reply_committed", operation_id,
                                {"conversation_id": actor.conversation_id, "reply_id": reply_id,
                                 "run_id": run_id, "response_seq": response_seq})
        await self._actor_final(conn, actor, binding)
        return reply

    async def _after_run(self, conn, conversation, session):
        pending = await (await conn.execute(
            "SELECT min(ingress_seq) AS first_seq FROM public.bot_conversation_triggers "
            "WHERE bot_id=%s AND conversation_id=%s AND status='PENDING'",
            (self.store.bot_id, conversation["conversation_id"]),
        )).fetchone()
        first = pending["first_seq"]
        state = ("DRAINING" if first is None else "READY_NEXT"
                 if first <= conversation["classified_through_seq"] else "WAIT_INPUT")
        await conn.execute(
            "UPDATE public.bot_conversation_sessions SET status=%s,active_run_id=NULL "
            "WHERE bot_id=%s AND session_id=%s", (state, self.store.bot_id, session["session_id"]),
        )

    async def end_run(self, actor: ActorLease, *, run_id: str, reason: str,
                      status: str = "FAILED", delivery=None, requeue: bool = False) -> dict:
        """Terminate only this run's claims; queued users remain independently pending.

        Requeue is a host recovery operation after the previous execution is
        definitely abandoned. Context ranges remain committed and are not replayed.
        """
        if status not in {"FAILED", "CANCELLED"}:
            raise ValueError("status")
        _text(reason, "reason", 1024)
        async with self.store.pool.connection() as conn, conn.transaction():
            await self._require_legacy_run(conn, actor, run_id=run_id)
            return await self._end_run_in(conn, actor, run_id=run_id, reason=reason, status=status, delivery=delivery, requeue=requeue)

    async def _end_run_in(self, conn, actor: ActorLease, *, run_id: str, reason: str,
                          status: str = "FAILED", delivery=None, requeue: bool = False) -> dict:
        binding, conversation = await self._actor(conn, actor)
        session = await self._session(conn, conversation)
        run = await self._run(conn, session)
        if not run or run["run_id"] != run_id or run["status"] != "RUNNING":
            raise ForestError("RUN_NOT_ACTIVE", "Run is not active", 409)
        if delivery is not None:
            await delivery.cancel_root_in(conn, actor.host, run_id)
        await conn.execute(
            "UPDATE public.bot_conversation_runs SET status=%s,completed_at=clock_timestamp() "
            "WHERE bot_id=%s AND run_id=%s", (status, self.store.bot_id, run_id),
        )
        await conn.execute(
            "UPDATE public.bot_conversation_triggers SET status=%s,terminal_reason=%s,"
            "claimed_run_id=CASE WHEN %s THEN NULL ELSE claimed_run_id END,"
            "claimed_session_id=CASE WHEN %s THEN NULL ELSE claimed_session_id END "
            "WHERE bot_id=%s AND claimed_run_id=%s AND status='CLAIMED'",
            ("PENDING" if requeue else status, reason, requeue, requeue, self.store.bot_id, run_id),
        )
        await self._after_run(conn, conversation, session)
        await conn.execute(
            "UPDATE public.bot_conversations SET mailbox_revision=mailbox_revision+1,updated_at=clock_timestamp() "
            "WHERE bot_id=%s AND conversation_id=%s", (self.store.bot_id, actor.conversation_id),
        )
        await self._compact(conn, actor.conversation_id)
        await self.store._event(conn, "conversation.run_ended", run_id,
                                {"run_id": run_id, "status": status, "requeued": requeue})
        await self._actor_final(conn, actor, binding)
        return {"run_id": run_id, "status": status, "requeued": requeue}
