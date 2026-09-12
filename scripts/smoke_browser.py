"""Exercise a disposable Compose deployment in a real browser; never log secrets.

Run after `docker compose build`. Requires the dev dependencies and a Playwright
Chromium installation, or --browser pointing to a compatible installed browser.
All created containers/volumes have a unique forest-e2e- prefix and are removed.
"""

from __future__ import annotations

import argparse
import json
import os
import secrets
import socket
import subprocess
from pathlib import Path
from urllib.request import ProxyHandler, build_opener
from uuid import uuid4

from playwright.sync_api import expect, sync_playwright


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--port", type=int, default=0)
    parser.add_argument("--browser", type=Path)
    args = parser.parse_args()
    if args.port == 0:
        with socket.socket() as reservation:
            reservation.bind(("127.0.0.1", 0))
            args.port = reservation.getsockname()[1]
    root = Path(__file__).resolve().parents[1]
    project = "forest-e2e-" + uuid4().hex[:12]
    env = dict(os.environ)
    origin = f"http://127.0.0.1:{args.port}"
    env.update(FOREST_HTTP_PORT=str(args.port), FOREST_PUBLIC_ORIGIN=origin)
    command = ["docker", "compose", "-p", project]
    artifacts = root / ".local" / "browser-artifacts"
    artifacts.mkdir(parents=True, exist_ok=True)

    def compose(*arguments: str) -> str:
        result = subprocess.run(
            command + list(arguments),
            cwd=root,
            env=env,
            capture_output=True,
            text=True,
            timeout=180,
        )
        if result.returncode:
            raise RuntimeError(f"Compose operation failed: {arguments[0]}\n{result.stderr[-2000:]}")
        return result.stdout

    try:
        compose("up", "-d", "--no-build", "--wait", "--wait-timeout", "90")
        # Loopback traffic must not inherit a developer machine's outbound proxy.
        direct_http = build_opener(ProxyHandler({}))
        with direct_http.open(origin + "/health/live", timeout=5) as health:
            if json.load(health).get("service") != "forest-management":
                raise RuntimeError("The selected host port does not serve this Forest deployment")
        token = compose("exec", "-T", "forest", "forest", "setup-token").strip()
        password = secrets.token_urlsafe(32)
        with sync_playwright() as playwright:
            browser = playwright.chromium.launch(
                headless=True,
                args=["--no-proxy-server", "--disable-gpu"],
                **({"executable_path": str(args.browser)} if args.browser else {}),
            )
            context = browser.new_context(viewport={"width": 1360, "height": 900})
            page = context.new_page()
            page.goto(origin)
            expect(page.locator("#setup")).to_be_visible()
            page.locator('#setup-form [name="token"]').fill(token)
            page.locator('#setup-form [name="username"]').fill("browser-admin")
            page.locator('#setup-form [name="password"]').fill(password)
            page.locator("#setup-form button").click()
            expect(page.locator("#login")).to_be_visible(timeout=20000)
            page.locator('#login-form [name="username"]').fill("browser-admin")
            page.locator('#login-form [name="password"]').fill(password)
            page.locator("#login-form button").click()
            expect(page.locator("#configuration")).to_be_visible()
            rival = context.new_page()
            rival.goto(origin)
            expect(rival.locator("#configuration")).to_be_visible()
            page.locator('#config-form [name="display_name"]').fill("Forest 浏览器验证")
            page.locator('#config-form [name="session_ttl_minutes"]').fill("90")
            page.locator('#config-form button[type="submit"]').click()
            expect(page.locator("#preview")).to_be_visible()
            page.locator("#apply").click()
            expect(page.locator("#revision")).to_have_text("1")
            rival.locator('#config-form [name="display_name"]').fill("另一个窗口的草稿")
            rival.locator('#config-form button[type="submit"]').click()
            expect(rival.locator("#notice")).to_contain_text("REVISION_CONFLICT")
            expect(rival.locator('#config-form [name="display_name"]')).to_have_value(
                "另一个窗口的草稿"
            )
            page.bring_to_front()
            page.screenshot(path=str(artifacts / "configuration-desktop.png"), full_page=True)
            page.set_viewport_size({"width": 390, "height": 844})
            page.screenshot(path=str(artifacts / "configuration-mobile.png"), full_page=True)
            compose("restart", "forest")
            compose("up", "-d", "--no-build", "--wait", "--wait-timeout", "90")
            page.goto(origin)
            expect(page.locator("#configuration")).to_be_visible(timeout=20000)
            expect(page.locator("#revision")).to_have_text("1")
            expect(page.locator('#config-form [name="display_name"]')).to_have_value(
                "Forest 浏览器验证"
            )
            page.locator("#logout").click()
            expect(page.locator("#login")).to_be_visible()
            rival.reload()
            expect(rival.locator("#login")).to_be_visible()
            browser.close()
        print(
            json.dumps(
                {
                    "status": "passed",
                    "checks": [
                        "setup",
                        "login",
                        "preview",
                        "apply",
                        "two-tab conflict",
                        "restart persistence",
                        "logout revocation",
                    ],
                    "artifacts": str(artifacts),
                },
                ensure_ascii=False,
            )
        )
    finally:
        # This project name was generated here, never supplied by another caller.
        assert project.startswith("forest-e2e-") and len(project) == 23
        compose("down", "--volumes", "--remove-orphans")


if __name__ == "__main__":
    main()
