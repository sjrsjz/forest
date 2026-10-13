"""Read NapCat QQ login health without printing WebUI credentials or QR codes."""

from __future__ import annotations

import argparse
import hashlib
import json
import sys
import urllib.request
from pathlib import Path
from urllib.parse import urlsplit


def _request(origin: str, path: str, body: dict, credential: str | None = None) -> dict:
    headers = {"Content-Type": "application/json"}
    if credential is not None:
        headers["Authorization"] = "Bearer " + credential
    request = urllib.request.Request(
        origin + "/api" + path,
        data=json.dumps(body, separators=(",", ":")).encode("utf-8"),
        headers=headers,
        method="POST",
    )
    with urllib.request.urlopen(request, timeout=8) as response:
        result = json.load(response)
    if not isinstance(result, dict) or result.get("code") != 0:
        raise ValueError("NapCat API rejected the request")
    return result


def observe(config_path: Path, origin: str) -> dict:
    parsed = urlsplit(origin)
    if (
        parsed.scheme != "http"
        or parsed.hostname != "127.0.0.1"
        or parsed.username is not None
        or parsed.password is not None
        or parsed.path not in ("", "/")
        or parsed.query
        or parsed.fragment
    ):
        raise ValueError("NapCat origin must be local HTTP on 127.0.0.1")
    config = json.loads(config_path.read_text(encoding="utf-8"))
    token = config.get("token")
    if not isinstance(token, str) or not token:
        raise ValueError("NapCat WebUI token is unavailable")
    password_hash = hashlib.sha256((token + ".napcat").encode("utf-8")).hexdigest()
    login = _request(origin.rstrip("/"), "/auth/login", {"hash": password_hash})
    credential = login.get("data", {}).get("Credential")
    if not isinstance(credential, str) or not credential:
        raise ValueError("NapCat WebUI credential is unavailable")
    result = _request(origin.rstrip("/"), "/QQLogin/CheckLoginStatus", {}, credential)
    state = result.get("data")
    if not isinstance(state, dict):
        raise ValueError("NapCat login status is invalid")
    is_login = state.get("isLogin") is True
    is_offline = state.get("isOffline") is True
    core_ready = state.get("coreReady") is True
    phase = state.get("loginPhase")
    if phase is not None and not isinstance(phase, str):
        raise ValueError("NapCat login phase is invalid")
    return {
        "state": "READY" if is_login and core_ready and not is_offline else "NOT_READY",
        "is_login": is_login,
        "is_offline": is_offline,
        "core_ready": core_ready,
        "login_phase": phase,
        "login_error_present": bool(state.get("loginError")),
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, required=True, help="NapCat webui.json path")
    parser.add_argument("--origin", default="http://127.0.0.1:6099")
    args = parser.parse_args()
    try:
        result = observe(args.config, args.origin)
    except Exception as error:
        print(json.dumps({"state": "UNKNOWN", "reason": type(error).__name__}))
        return 1
    print(json.dumps(result, ensure_ascii=False, separators=(",", ":")))
    return 0 if result["state"] == "READY" else 2


if __name__ == "__main__":
    sys.exit(main())
