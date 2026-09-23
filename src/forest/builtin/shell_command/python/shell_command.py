import hashlib
import json
from datetime import datetime, timedelta, timezone

from forest.errors import ForestError
from forest.sdk import Plugin, job, service


def _plain(value):
    if hasattr(value, "items"):
        return {key: _plain(item) for key, item in value.items()}
    if isinstance(value, (tuple, list)):
        return [_plain(item) for item in value]
    return value


def _stable(kind, *parts):
    body = "\0".join(str(part) for part in parts).encode("utf-8")
    return "shell:" + kind + ":" + hashlib.sha256(body).hexdigest()


def _digest(value):
    body = json.dumps(
        value, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False
    ).encode("utf-8")
    return "sha256:" + hashlib.sha256(body).hexdigest()


def _direct_text(normalized):
    segments = normalized.get("segments")
    self_id = normalized.get("self_id")
    if not isinstance(segments, list) or not isinstance(self_id, str):
        return None
    index = 0
    if segments and isinstance(segments[0], dict):
        first = segments[0]
        if first.get("type") == "mention" and first.get("external_user_id") == self_id:
            index = 1
    texts = []
    for segment in segments[index:]:
        if not isinstance(segment, dict) or segment.get("type") != "text":
            return None
        text = segment.get("text")
        if not isinstance(text, str):
            return None
        texts.append(text)
    value = "".join(texts).lstrip(" \t")
    if value == "#forest":
        return ""
    if not value.startswith("#forest ") and not value.startswith("#forest\t"):
        return None
    return value[len("#forest") :].lstrip(" \t")


def _tokenize(value):
    if not isinstance(value, str) or len(value.encode("utf-8")) > 4096:
        raise ValueError
    tokens = []
    current = []
    quote = None
    index = 0
    while index < len(value):
        character = value[index]
        if quote is None:
            if character in " \t":
                if current:
                    tokens.append("".join(current))
                    current.clear()
                index += 1
                continue
            if character in {"'", '"'}:
                quote = character
                index += 1
                continue
            current.append(character)
            index += 1
            continue
        if character == quote:
            quote = None
            index += 1
            continue
        if quote == '"' and character == "\\":
            index += 1
            if index >= len(value) or value[index] not in {'"', "\\"}:
                raise ValueError
            current.append(value[index])
            index += 1
            continue
        current.append(character)
        index += 1
    if quote is not None:
        raise ValueError
    if current:
        tokens.append("".join(current))
    if len(tokens) > 64 or any(not item or len(item.encode("utf-8")) > 512 for item in tokens):
        raise ValueError
    return tokens


def _coerce(raw, schema):
    kind = schema.get("type")
    if kind is None and isinstance(schema.get("enum"), list):
        kind = "string"
    if kind == "boolean":
        if raw is None:
            return True
        if raw in {"true", "false"}:
            return raw == "true"
        raise ValueError
    if raw is None:
        raise ValueError
    if kind == "integer":
        if not raw.isdecimal():
            raise ValueError
        return int(raw)
    if kind == "string":
        return raw
    raise ValueError


def _parse(command_text, directory):
    try:
        tokens = _tokenize(command_text)
    except ValueError:
        return "shell.invalid", 1, {"error_code": "SHELL_SYNTAX_INVALID"}
    candidates = []
    for item in directory["items"]:
        words = item["command_id"].split(".")
        if tokens[: len(words)] == words:
            candidates.append((words, item))
    if not candidates:
        return "shell.invalid", 1, {"error_code": "SHELL_COMMAND_NOT_FOUND"}
    words, selected = max(candidates, key=lambda item: len(item[0]))
    argument_schema = selected["argument_schema"]["properties"]["arguments"]
    properties = argument_schema.get("properties", {})
    aliases = {name.replace("_", "-"): name for name in properties}
    aliases.update(dict(selected.get("option_aliases", [])))
    remaining = list(tokens[len(words) :])
    arguments = {}
    try:
        while remaining:
            option = remaining.pop(0)
            if not option.startswith("--") or option == "--":
                raise ValueError
            key, separator, raw = option[2:].partition("=")
            name = aliases.get(key)
            if name not in properties or name in arguments:
                raise ValueError
            if not separator and properties[name].get("type") != "boolean":
                if not remaining:
                    raise ValueError
                raw = remaining.pop(0)
            arguments[name] = _coerce(raw if separator or raw else None, properties[name])
        required = set(argument_schema.get("required", []))
        if required - set(arguments):
            raise ValueError
    except ValueError:
        return "shell.invalid", 1, {"error_code": "SHELL_ARGUMENTS_INVALID"}
    return selected["command_id"], selected["command_version"], arguments


class ShellCommand(Plugin):
    async def prepare(self, ctx, config, restored_state=None):
        await super().prepare(ctx, config, restored_state)
        value = _plain(config)
        if value.get("protocol") != "forest.shell.command/2":
            raise ForestError(
                "SHELL_CONFIGURATION_INVALID", "Shell configuration is invalid", 422
            )

    @service("shell.ingress.inspect", semantics="read")
    async def inspect(self, payload, ctx):
        value = _plain(payload)
        command_text = _direct_text(value["normalized"])
        if command_text is None:
            return None
        directory = await ctx.shell_commands.list()
        command_id, command_version, arguments = _parse(command_text, directory)
        request_id = _stable("request", value["event_ref"])
        owner_id = _stable("rejection", request_id)
        return {
            "protocol": "forest.shell-ingress/2",
            "shell_generation_id": ctx.identity["generation_id"],
            "request_id": request_id,
            "command_id": command_id,
            "command_version": command_version,
            "arguments": arguments,
            "rejection": {
                "owner_id": owner_id,
                "value": {
                    "text": "Forest 控制命令已拒绝；请检查命令、管理员策略和审计记录。"
                },
            },
        }

    @job("shell.command.execute")
    async def execute(self, task, ctx):
        work = _plain(task["payload"])
        plan = {
            "request_id": work["request_id"],
            "claim_token": work["claim_token"],
            "command_id": work["command_id"],
            "command_version": work["command_version"],
            "target_component_id": work["target_component_id"],
            "target_generation_id": work["target_generation_id"],
        }
        plan_digest = _digest(plan)
        ticket_id = _stable("invoke-ticket", work["request_id"], task["attempt"])
        authorization_operation = _stable(
            "invoke-authorization", work["request_id"], task["attempt"]
        )
        expires_at = (datetime.now(timezone.utc) + timedelta(seconds=20)).isoformat()
        common = {
            "ticket_id": ticket_id,
            "operation_id": authorization_operation,
            "action": "management.invoke",
            "resource_kind": "bot",
            "resource_id": ctx.identity["bot_id"],
            "input_digest": plan_digest,
            "plan_digest": plan_digest,
            "audience_component_id": "shell.command",
            "audience_generation_id": ctx.identity["generation_id"],
        }
        state = "SUCCEEDED"
        error_code = None
        try:
            await ctx.uow.call(
                "shell.authorize-execution",
                {
                    "issue": {
                        "protocol": "forest.authorization-issue/1",
                        "issuance_operation_id": ctx.operations.stable("ticket-issue"),
                        "expires_at": expires_at,
                        **common,
                    },
                    "consume": {
                        "protocol": "forest.authorization-consume/1",
                        **common,
                    },
                    "shell": {
                        "protocol": "forest.shell-authorize-claim/1",
                        "request_id": work["request_id"],
                        "claim_token": work["claim_token"],
                        "ticket_id": ticket_id,
                        "authorization_operation_id": authorization_operation,
                    },
                },
                operation_id=ctx.operations.stable("authorize-execution"),
            )
            result = await ctx.shell_commands.execute(
                command_id=work["command_id"],
                command_version=work["command_version"],
                arguments=work["arguments"],
                request_id=work["request_id"],
                principal_ref=work["principal_ref"],
                conversation_ref=work["conversation_ref"],
                target_component_id=work["target_component_id"],
                target_generation_id=work["target_generation_id"],
                target_service_id=work["target_service_id"],
                target_service_version=work["target_service_version"],
                operation_id=ctx.operations.stable("invoke-command"),
                timeout=work["timeout_seconds"],
            )
            text = result["text"]
        except ForestError as error:
            state = "FAILED"
            error_code = error.code
            text = "Forest 管理命令执行失败；请查看审计记录。"
        except Exception:
            state = "UNKNOWN"
            error_code = "SHELL_EXECUTION_UNKNOWN"
            text = "Forest 管理命令执行状态未知；请查看审计记录。"
        owner_id = _stable("result", work["request_id"])
        saved = await ctx.services.call(
            "kernel.payload.json-write",
            {
                "protocol": "forest.payload-json-write/1",
                "owner_kind": "shell.result",
                "owner_id": owner_id,
                "value": {"text": text},
            },
            operation_id=ctx.operations.stable("result-payload"),
        )
        payload_ref = saved["payload"]
        channel_kind, channel_ref = self._channel(work["conversation_ref"])
        await ctx.uow.call(
            "shell.complete",
            {
                "shell": {
                    "protocol": "forest.shell-complete-claim/1",
                    "request_id": work["request_id"],
                    "claim_token": work["claim_token"],
                    "resulting_state": state,
                    "result": payload_ref,
                    "error_code": error_code,
                },
                "delivery": {
                    "protocol": "forest.delivery-intent/1",
                    "intent_id": _stable("delivery-intent", work["request_id"]),
                    "binding_ref": work["binding_ref"],
                    "lane_kind": "CONTROL",
                    "ordering_key": work["conversation_ref"],
                    "conversation_ref": None,
                    "channel_kind": channel_kind,
                    "channel_ref": channel_ref,
                    "cause_component_id": "shell.command",
                    "cause_kind": "shell.request",
                    "cause_id": work["request_id"],
                    "reply_ref": None,
                    "parts": [{
                        "part_operation_id": _stable("delivery-part", work["request_id"]),
                        "effect_operation_ref": _stable("delivery-effect", work["request_id"]),
                        "payload": payload_ref,
                    }],
                },
                "audit": {
                    "protocol": "forest.audit-append/1",
                    "audit_id": _stable("audit", work["request_id"]),
                    "action": "shell.command." + state.lower(),
                    "origin_kind": "onebot.group-shell",
                    "severity": "INFO" if state == "SUCCEEDED" else "ERROR",
                    "actor_kind": "principal",
                    "actor_id": work["principal_ref"],
                    "resource_kind": "shell.command",
                    "resource_id": work["command_id"],
                    "operation_kind": "shell.request",
                    "operation_id": work["request_id"],
                    "call_id": None,
                    "details_payload_id": None,
                    "details_payload_digest": None,
                },
            },
            operation_id=ctx.operations.stable("complete"),
        )
        return {"disposition": "SUCCEEDED", "request_id": work["request_id"]}

    @staticmethod
    def _channel(conversation_ref):
        if conversation_ref.startswith("onebot:group:"):
            return "GROUP", conversation_ref[len("onebot:group:") :]
        if conversation_ref.startswith("onebot:private:"):
            return "PRIVATE", conversation_ref[len("onebot:private:") :]
        raise ForestError(
            "SHELL_RESULT_ROUTE_INVALID", "Shell conversation route is invalid", 409
        )
