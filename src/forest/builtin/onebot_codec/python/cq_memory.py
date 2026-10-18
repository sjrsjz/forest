"""Canonical whole-message CQ text; media bytes never enter this projection."""

import re

from onebot_codec import _IDENTIFIER, MAX_SEND_BYTES, _id, _invalid


def escape(text, *, parameter=False):
    value = text.replace("&", "&amp;").replace("[", "&#91;").replace("]", "&#93;")
    return value.replace(",", "&#44;") if parameter else value


def encode_message(segments):
    result = []
    for part in segments:
        kind = part["type"]
        if kind == "text":
            result.append(escape(part["text"]))
            continue
        if kind == "mention":
            user = part["external_user_id"]
            tag, fields = "at", {"qq": "all" if user == "all" else _id(user)}
        elif kind == "reply":
            tag, fields = "reply", {"id": _id(part["platform_message_id"], message=True)}
        elif kind == "face":
            if not re.fullmatch(r"[0-9]{1,5}", part["face_id"]):
                raise _invalid("Invalid face identity")
            tag, fields = "face", {"id": str(int(part["face_id"]))}
        elif kind == "image":
            if not re.fullmatch(r"media:[a-f0-9]{64}", part["media_ref"]):
                raise _invalid("Memory image requires a managed reference")
            tag, fields = "image", {
                "file": part["media_ref"], "revision": str(part["media_revision"]),
                "source_unit_id": part["source_unit_id"], "part_id": part["part_id"],
            }
        else:
            tag, fields = part["platform_type"], {}
            if not _IDENTIFIER.fullmatch(tag):
                raise _invalid("Invalid unsupported segment type")
        parameters = "".join("," + key + "=" + escape(value, parameter=True)
                             for key, value in fields.items())
        result.append("[CQ:" + tag + parameters + "]")
    text = "".join(result)
    if len(text.encode("utf-8")) > MAX_SEND_BYTES:
        raise _invalid("Canonical CQ message exceeds its bound")
    return text
