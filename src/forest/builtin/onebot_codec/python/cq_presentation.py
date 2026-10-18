"""Pure presentation parsing. Media stays as references until host materialization."""
import re

from onebot_codec import _ACTIONS, MAX_SEGMENTS, _cq_segments, _id, _invalid

from forest.errors import ForestError


def _reference(data):
    if (set(data) != {"file", "revision", "source_unit_id", "part_id"}
            or not re.fullmatch(r"media:[a-f0-9]{64}", data["file"])
            or any(not 1 <= len(data[key]) <= 256 for key in ("source_unit_id", "part_id"))):
        raise _invalid("CQ image requires a managed media reference")
    return {"media_ref": data["file"], "media_revision": int(_id(data["revision"])),
            "source_unit_id": data["source_unit_id"], "part_id": data["part_id"]}


def decode_presentation(text):
    result = []
    for segment in _cq_segments(text, keep_source=True):
        kind, data = segment["type"], segment["data"]
        if kind == "text" and "cq_source" not in segment:
            result.append(segment)
            continue
        try:
            if kind == "at" and set(data) == {"qq"}:
                result.append({"type": "at", "data": {"qq": "all" if data["qq"] == "all" else _id(data["qq"])}})
            elif kind == "reply" and set(data) == {"id"}:
                result.append({"type": "reply", "data": {"id": _id(data["id"], message=True)}})
            elif kind == "face" and set(data) == {"id"} and re.fullmatch(r"[0-9]{1,5}", data["id"]):
                result.append({"type": "face", "data": {"id": str(int(data["id"]))}})
            elif kind == "image":
                result.append({"type": "image", "data": {"file": {"media_reference": _reference(data)}}})
            else:
                raise _invalid("Unsupported CQ presentation")
        except ForestError:
            result.append({"type": "text", "data": {"text": segment["cq_source"]}})
    return result


def prepare_template(channel_kind, channel_id, segments):
    if channel_kind not in _ACTIONS or not isinstance(segments, list) or not 1 <= len(segments) <= MAX_SEGMENTS:
        raise _invalid("Invalid outbound message")
    channel_id = _id(channel_id)
    message = []
    for part in segments:
        if not isinstance(part, dict):
            raise _invalid("Invalid outbound part")
        if part.get("type") == "text" and set(part) <= {"type", "text", "format"} and isinstance(part.get("text"), str):
            if part.get("format") == "platform-markup":
                message.extend(decode_presentation(part["text"]))
            elif part.get("format") is None:
                message.append({"type": "text", "data": {"text": part["text"]}})
            else:
                raise _invalid("Unsupported text format")
        elif part.get("type") == "mention" and set(part) == {"type", "external_user_id"}:
            message.append({"type": "at", "data": {"qq": "all" if part["external_user_id"] == "all" else _id(part["external_user_id"])}})
        elif part.get("type") == "reply" and set(part) == {"type", "platform_message_id"}:
            message.append({"type": "reply", "data": {"id": _id(part["platform_message_id"], message=True)}})
        else:
            raise _invalid("Unsupported outbound part")
    if not message or len(message) > MAX_SEGMENTS or (channel_kind != "group" and any(part["type"] == "at" for part in message)):
        raise _invalid("Invalid outbound segments for this conversation")
    return {"action": _ACTIONS[channel_kind], "body": {
        "group_id" if channel_kind == "group" else "user_id": int(channel_id),
        "message": message, "auto_escape": True,
    }}
