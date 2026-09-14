"""Versioned, bounded control syntax; retained arguments never enter Context."""

from __future__ import annotations

import re
import shlex
from dataclasses import dataclass, field

from forest.shell_registry_v10 import (
    CURRENT_GATE_VERSION,
    DOMAIN_ALIASES_V10,
    SIMPLE_COMMANDS_V10,
    get_command_spec,
)


@dataclass(frozen=True)
class ShellCommand:
    name: str
    payload: dict = field(default_factory=dict)
    priority: int = 1
    gate_version: str = CURRENT_GATE_VERSION


_ID = re.compile(r"[A-Za-z0-9_.:-]{1,128}\Z")


def _command(name, payload=None):
    spec = get_command_spec(name, CURRENT_GATE_VERSION)
    if spec is None:
        raise ValueError("command")
    return ShellCommand(name, {} if payload is None else payload, spec.priority)


def _options(words, required):
    if len(words) != len(required) * 2:
        raise ValueError("options")
    values = {}
    for index in range(0, len(words), 2):
        key, value = words[index:index + 2]
        if key not in required or key in values:
            raise ValueError("option")
        values[key] = value
    return values


def _optional_options(words, allowed):
    if len(words) % 2:
        raise ValueError("options")
    values = {}
    for index in range(0, len(words), 2):
        key, value = words[index:index + 2]
        if key not in allowed or key in values:
            raise ValueError("option")
        values[key] = value
    return values


def _revision(value):
    if not re.fullmatch(r"0|[1-9][0-9]{0,17}", value):
        raise ValueError("revision")
    return int(value)


def _positive_revision(value):
    result = _revision(value)
    if result < 1:
        raise ValueError("positive revision")
    return result


def _identity(value):
    if not _ID.fullmatch(value):
        raise ValueError("identity")
    return value


def _reason(value):
    if not value.strip() or len(value) > 256 or any(ord(c) < 32 for c in value):
        raise ValueError("reason")
    return value


def _parse(words):
    if tuple(words) in SIMPLE_COMMANDS_V10:
        return _command(SIMPLE_COMMANDS_V10[tuple(words)])
    if len(words) < 2:
        raise ValueError("command")
    domain, verb = DOMAIN_ALIASES_V10.get(words[0], words[0]), words[1]
    if (domain, verb) == ("core", "cancel") and len(words) >= 3:
        options = _options(words[3:], {"--expect-checkpoint", "--reason"})
        return _command("run.cancel", {
            "run_id": _identity(words[2]),
            "expected_checkpoint_seq": _revision(options["--expect-checkpoint"]),
            "reason": _reason(options["--reason"]),
        })
    if (domain, verb) == ("core", "disable") and len(words) >= 2:
        options = _options(words[2:], {"--expect-maintenance", "--reason"})
        return _command("bot.pause", {
            "expected_maintenance_epoch": _revision(options["--expect-maintenance"]),
            "data_admission": False,
            "reason": _reason(options["--reason"]),
        })
    if (domain, verb) == ("core", "reload") and len(words) >= 3:
        options = _options(words[3:], {"--expect-revision"})
        return _command("module.reload", {
            "component_id": _identity(words[2]),
            "expected_revision": _positive_revision(options["--expect-revision"]),
        })
    if (domain, verb) == ("module", "list") and len(words) == 2:
        return _command("module.read")
    if (domain, verb) == ("module", "status") and len(words) == 3:
        return _command("module.read", {"component_id": _identity(words[2])})
    if (domain, verb) == ("module", "reload") and len(words) >= 3:
        options = _options(words[3:], {"--expect-revision"})
        expected = _positive_revision(options["--expect-revision"])
        return _command("module.reload", {
            "component_id": _identity(words[2]),
            "expected_revision": expected,
        })
    if (domain, verb) in {("job", "list"), ("run", "list")} and len(words) == 2:
        return _command(domain + ".read")
    if (domain, verb) == ("job", "status") and len(words) == 3:
        return _command("job.read", {"job_id": _identity(words[2])})
    if (domain, verb) == ("job", "run") and len(words) >= 3:
        options = _options(words[3:], {"--expect-revision"})
        return _command("job.run", {
            "job_id": _identity(words[2]),
            "expected_revision": _revision(options["--expect-revision"]),
        })
    if (domain, verb) == ("job", "cancel") and len(words) >= 3:
        options = _options(words[3:], {"--expect-revision", "--scope", "--reason"})
        if options["--scope"] not in {"future", "pending", "all"}:
            raise ValueError("scope")
        return _command("job.cancel", {
            "job_id": _identity(words[2]),
            "expected_revision": _revision(options["--expect-revision"]),
            "scope": options["--scope"], "reason": _reason(options["--reason"]),
        })
    if (domain, verb) == ("job", "replay") and len(words) >= 3:
        options = _options(words[3:], {"--expect-revision"})
        return _command("job.replay", {
            "occurrence_id": _identity(words[2]),
            "expected_revision": _revision(options["--expect-revision"]),
        })
    if (domain, verb) == ("run", "cancel") and len(words) >= 3:
        options = _options(words[3:], {"--expect-checkpoint", "--reason"})
        return _command("run.cancel", {
            "run_id": _identity(words[2]),
            "expected_checkpoint_seq": _revision(options["--expect-checkpoint"]),
            "reason": _reason(options["--reason"]),
        })
    if domain == "run" and verb in {"approve", "reject"} and len(words) >= 3:
        options = _options(words[3:], {"--digest"})
        return _command("run.approval", {
            "approval_id": _identity(words[2]), "arguments_digest": _identity(options["--digest"]),
            "decision": "APPROVE" if verb == "approve" else "REJECT",
        })
    if domain == "bot" and verb in {"pause", "resume"} and len(words) >= 2:
        options = _options(words[2:], {"--expect-maintenance", "--reason"})
        return _command("bot." + verb, {
            "expected_maintenance_epoch": _revision(options["--expect-maintenance"]),
            "data_admission": verb == "resume",
            "reason": _reason(options["--reason"]),
        })
    if (domain, verb) == ("event", "list") and len(words) == 2:
        return _command("event.read")
    if (domain, verb) == ("event", "replay") and len(words) >= 3:
        options = _options(words[3:], {"--expect-revision"})
        return _command("event.replay", {
            "delivery_id": _identity(words[2]),
            "expected_revision": _revision(options["--expect-revision"]),
        })
    if (domain, verb) == ("event", "retire-plan") and len(words) >= 3:
        options = _options(words[3:], {"--contract-major", "--expect-revision"})
        return _command("event.retire.plan", {
            "subscriber_id": _identity(words[2]),
            "contract_major": _positive_revision(options["--contract-major"]),
            "expected_revision": _revision(options["--expect-revision"]),
        })
    if (domain, verb) == ("event", "retire-apply") and len(words) >= 3:
        options = _options(words[3:], {"--digest", "--expect-revision"})
        return _command("event.retire.apply", {
            "plan_id": _identity(words[2]),
            "plan_digest": _identity(options["--digest"]),
            "expected_revision": _revision(options["--expect-revision"]),
        })
    if (domain, verb) == ("component-job", "list") and len(words) == 2:
        return _command("component_job.read")
    if (domain, verb) == ("component-job", "replay") and len(words) >= 3:
        options = _options(words[3:], {"--expect-revision"})
        return _command("component_job.replay", {
            "job_id": _identity(words[2]),
            "expected_revision": _revision(options["--expect-revision"]),
        })
    if (domain, verb) == ("component-job", "retire-plan") and len(words) >= 4:
        options = _options(words[4:], {"--expect-revision"})
        return _command("component_job.retire.plan", {
            "component_id": _identity(words[2]),
            "handler_id": _identity(words[3]),
            "expected_revision": _revision(options["--expect-revision"]),
        })
    if (domain, verb) == ("component-job", "retire-apply") and len(words) >= 3:
        options = _options(words[3:], {"--digest", "--expect-revision"})
        return _command("component_job.retire.apply", {
            "plan_id": _identity(words[2]),
            "plan_digest": _identity(options["--digest"]),
            "expected_revision": _revision(options["--expect-revision"]),
        })
    if (domain, verb) == ("audit", "list"):
        options = _optional_options(words[2:], {"--action", "--before", "--limit"})
        payload = {"limit": 12}
        if "--action" in options:
            payload["action"] = _identity(options["--action"])
        if "--before" in options:
            payload["before"] = _identity(options["--before"])
        if "--limit" in options:
            limit = _positive_revision(options["--limit"])
            if limit > 20:
                raise ValueError("limit")
            payload["limit"] = limit
        return _command("audit.read", payload)
    if (domain, verb) == ("context", "inspect") and len(words) == 2:
        return _command("context.read")
    if (domain, verb) == ("context", "compact"):
        options = _options(words[2:], {"--expect-view"})
        return _command("context.compact", {
            "expected_view_revision": _revision(options["--expect-view"]),
        })
    if (domain, verb) == ("memory", "list") and len(words) == 2:
        return _command("memory.read")
    if (domain, verb) == ("memory", "forget-plan") and len(words) >= 3:
        options = _options(words[3:], {"--expect-deletion-epoch"})
        return _command("memory.forget.plan", {
            "source_unit_ids": [_identity(words[2])],
            "expected_deletion_epoch": _revision(options["--expect-deletion-epoch"]),
        })
    if (domain, verb) == ("memory", "forget-apply") and len(words) >= 3:
        options = _options(words[3:], {"--digest", "--expect-deletion-epoch"})
        return _command("memory.forget.apply", {
            "plan_id": _identity(words[2]),
            "plan_digest": _identity(options["--digest"]),
            "expected_deletion_epoch": _revision(options["--expect-deletion-epoch"]),
        })
    raise ValueError("command")


def parse_command(value: dict) -> ShellCommand | None:
    """Only direct leading text can reserve the namespace, after one self mention.

    Unknown/malformed commands stay reserved, but their raw arguments are discarded.
    This grammar accepts identifiers, revision numbers and a bounded public reason;
    credentials, process commands and configuration values have no grammar entry.
    """
    if value["author"]["is_self"]:
        return None
    parts, index, mentioned = value["segments"], 0, False
    while index < len(parts):
        part = parts[index]
        if part["type"] == "text" and not part["text"].strip():
            index += 1
            continue
        if (not mentioned and part["type"] == "mention"
                and part["external_user_id"] == value["self_id"]):
            mentioned, index = True, index + 1
            continue
        break
    text = ""
    while index < len(parts) and parts[index]["type"] == "text":
        text += parts[index]["text"]
        index += 1
    text = text.lstrip()
    if not re.match(r"#forest(?:\s|$)", text):
        return None
    if index != len(parts) or len(text.encode("utf-8")) > 4096:
        return ShellCommand("unknown")
    try:
        return _parse(shlex.split(text[len("#forest"):], comments=False, posix=True))
    except ValueError:
        return ShellCommand("unknown")
