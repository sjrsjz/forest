"""Exercise the shipped provisioning package and host Bootstrap broker in Edge."""

from __future__ import annotations

import argparse
import hashlib
import json
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

from jsonschema import Draft202012Validator
from playwright.sync_api import expect, sync_playwright

from forest.bootstrap_contributions import BootstrapConfigurationRegistry
from forest.official_bot_provisioning import official_bootstrap_contributions


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--browser", type=Path, help="Chromium or Edge executable")
    args = parser.parse_args()
    root = Path(__file__).resolve().parents[1]
    static = root / "src/forest/management_static"
    ui = (root / "src/forest/builtin/deployment_provisioning/ui/index.html").read_bytes()
    digest = hashlib.sha256(ui).hexdigest()
    catalog = {
        "protocol": "forest.bootstrap-configuration-catalog/1",
        "items": BootstrapConfigurationRegistry(official_bootstrap_contributions()).facts(),
    }
    schema = json.loads((
        root / "src/forest/builtin/deployment_provisioning/schemas/bootstrap-catalog-output.json"
    ).read_text(encoding="utf-8"))
    Draft202012Validator(schema).validate(catalog)
    artifact = f"/management/v1/modules/deployment.provisioning/management-ui/{digest}"
    module = {
        "component_id": "deployment.provisioning",
        "generation_id": "generation/provisioning/1",
        "state": "ACTIVE",
        "artifact": {
            "scope": "deployment", "trust_level": "official",
            "package_digest": "sha256:" + "a" * 64, "package_version": "1.0.0",
        },
        "management_contract": {
            "bootstrap_catalog_query": "provisioning.bootstrap-catalog",
            "queries": [
                {"query_id": "provisioning.bootstrap-catalog"},
                {"query_id": "provisioning.jobs.list"},
            ],
            "actions": [
                {"action_id": "bootstrap.create"},
                {"action_id": "storage.create-local"},
                {"action_id": "provisioning.enqueue"},
                {"action_id": "provisioning.reconcile"},
            ],
        },
        "management_contributions": [{
            "label": "Bot 供应",
            "surface": {"kind": "sandboxed-package", "artifact": artifact,
                        "sha256": digest},
        }],
    }
    runtime = {
        "runtime": {
            "scope_kind": "deployment", "scope_id": "deployment/test",
            "graph_state": "ACTIVE", "graph_id": "graph/test", "graph_revision": 1,
        },
        "modules": [module],
    }
    configuration = {
        "desired": {"value": {"protocol": "forest.deployment-provisioning/1"}, "revision": 1},
        "effective": {"value": {"protocol": "forest.deployment-provisioning/1"}, "revision": 1},
    }
    submissions: list[dict] = []
    storage_submissions: list[dict] = []
    job_submissions: list[dict] = []
    reconciliations: list[dict] = []

    class Handler(BaseHTTPRequestHandler):
        def _respond(self, body: bytes, content_type: str) -> None:
            self.send_response(200)
            self.send_header("Content-Type", content_type)
            self.send_header("Content-Length", str(len(body)))
            if self.path == artifact:
                self.send_header(
                    "Content-Security-Policy",
                    "sandbox allow-scripts allow-forms; default-src 'none'; "
                    "script-src 'unsafe-inline'; style-src 'unsafe-inline'; "
                    "connect-src 'none'; form-action 'none'",
                )
            self.end_headers()
            self.wfile.write(body)

        def _json(self, value: object) -> None:
            self._respond(json.dumps(value, ensure_ascii=False).encode(), "application/json")

        def do_GET(self) -> None:
            if self.path == "/":
                body = (static / "index.html").read_bytes()
                kind = "text/html; charset=utf-8"
            elif self.path.startswith("/static/"):
                path = (static / self.path.removeprefix("/static/")).resolve()
                if not path.is_relative_to(static.resolve()) or not path.is_file():
                    self.send_error(404)
                    return
                body = path.read_bytes()
                kind = ("text/javascript" if path.suffix == ".js" else
                        "text/css" if path.suffix == ".css" else "text/html")
            elif self.path == artifact:
                body, kind = ui, "text/html; charset=utf-8"
            elif self.path == "/management/v1/auth/session":
                return self._json({"subject": {"role": "ADMIN"}})
            elif self.path == "/management/v1/runtime":
                return self._json(runtime)
            elif self.path == "/management/v1/modules/deployment.provisioning/configuration":
                return self._json(configuration)
            else:
                self.send_error(404)
                return
            self._respond(body, kind)

        def do_POST(self) -> None:
            size = int(self.headers.get("Content-Length", "0"))
            value = json.loads(self.rfile.read(size))
            if self.path.endswith("/queries/provisioning.jobs.list"):
                return self._json({"items": [{
                    "job_id": item["job_id"], "bot_id": item["bot_id"],
                    "state": item.get("state", "QUEUED"),
                    "revision": item.get("revision", 1),
                    "attempt_no": item.get("attempt_no", 0),
                    "max_attempts": item["max_attempts"],
                    "last_error_code": item.get("last_error_code"),
                } for item in job_submissions], "next_after": None})
            if self.path.endswith("/queries/provisioning.bootstrap-catalog"):
                return self._json(catalog)
            if self.path.endswith("/actions/bootstrap.create"):
                submissions.append(value)
                return self._json({
                    "profile_ref": value["profile_ref"], "bot_id": value["bot_id"],
                    "manifest_digest": "sha256:" + "b" * 64,
                })
            if self.path.endswith("/actions/storage.create-local"):
                storage_submissions.append(value)
                return self._json(value)
            if self.path.endswith("/actions/provisioning.enqueue"):
                job_submissions.append(value)
                return self._json({
                    "job_id": value["job_id"], "bot_id": value["bot_id"],
                    "state": "QUEUED", "revision": 1, "attempt_no": 0,
                    "max_attempts": value["max_attempts"], "last_error_code": None,
                })
            if self.path.endswith("/actions/provisioning.reconcile"):
                reconciliations.append(value)
                job = next(item for item in job_submissions if item["job_id"] == value["job_id"])
                assert job["state"] == "BLOCKED_UNKNOWN"
                assert value["expected_revision"] == job["revision"]
                job.update(state="SUCCEEDED", revision=job["revision"] + 1,
                           last_error_code=None)
                return self._json({
                    "job_id": job["job_id"], "bot_id": job["bot_id"],
                    "state": job["state"], "revision": job["revision"],
                    "attempt_no": job["attempt_no"],
                    "max_attempts": job["max_attempts"], "last_error_code": None,
                })
            self.send_error(404)

        def log_message(self, *_args):
            pass

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        with sync_playwright() as playwright:
            browser = playwright.chromium.launch(
                headless=True,
                **({"executable_path": str(args.browser)} if args.browser else {}),
            )
            try:
                page = browser.new_page(viewport={"width": 1200, "height": 800})
                errors: list[str] = []
                page.on("pageerror", lambda error: errors.append(str(error)))
                page.goto(f"http://127.0.0.1:{server.server_port}/")
                page.locator("#module-nav button").filter(
                    has_text="deployment.provisioning"
                ).click()
                frame = page.frame_locator(".package-ui-frame")
                frame.locator('[data-tab="bootstrap"]').click()
                frame.locator("#edit-components").click()
                dialog = page.locator("dialog.bootstrap-editor-dialog")
                expect(dialog).to_be_visible()
                expect(dialog.locator(".bootstrap-editor-panel")).to_have_count(7)
                dialog.locator('button[type="submit"]').click()
                expect(frame.locator("#component-status")).to_contain_text("7 个模块")
                form = frame.locator("#bootstrap")
                form.locator('[name="bot_id"]').fill("bot/browser")
                form.locator('[name="profile_ref"]').fill("bootstrap-profile/browser")
                form.locator('[name="storage_profile_ref"]').fill("storage-profile/browser")
                form.locator('[name="storage_generation"]').fill("1")
                form.locator('[type="submit"]').click()
                expect(frame.locator("#status")).to_contain_text("已冻结")
                assert len(submissions) == 1
                assert set(submissions[0]["components"]) == {
                    item["component_id"] for item in catalog["items"]
                }
                frame.locator('[data-tab="storage"]').click()
                storage = frame.locator("#storage")
                storage.locator('[name="bot_id"]').fill("bot/browser")
                storage.locator('[name="profile_ref"]').fill("storage-profile/browser")
                storage.locator('[name="dsn_secret_ref"]').fill("secret://browser/storage")
                storage.locator('[type="submit"]').click()
                expect(frame.locator("#status")).to_contain_text("已创建")
                assert len(storage_submissions) == 1
                frame.locator('[data-tab="enqueue"]').click()
                enqueue = frame.locator("#enqueue")
                enqueue.locator('[name="job_id"]').fill("provisioning/browser")
                enqueue.locator('[name="bot_id"]').fill("bot/browser")
                enqueue.locator('[name="display_name"]').fill("Browser Bot")
                enqueue.locator('[name="storage_profile_ref"]').fill("storage-profile/browser")
                enqueue.locator('[name="bootstrap_profile_ref"]').fill(
                    "bootstrap-profile/browser")
                enqueue.locator('[type="submit"]').click()
                expect(frame.locator("#items .row")).to_have_count(1)
                assert len(job_submissions) == 1
                job_submissions.append({
                    "job_id": "provisioning/blocked", "bot_id": "bot/blocked",
                    "state": "BLOCKED_UNKNOWN", "revision": 4, "attempt_no": 1,
                    "max_attempts": 3,
                    "last_error_code": "PROVISIONING_CLAIM_EXPIRED",
                })
                frame.locator("#refresh").click()
                blocked = frame.locator("#items .row").filter(
                    has_text="provisioning/blocked"
                )
                expect(blocked).to_be_visible()
                blocked.get_by_role("button", name="核对物化").click()
                expect(blocked).to_contain_text("SUCCEEDED")
                assert reconciliations == [{
                    "job_id": "provisioning/blocked", "expected_revision": 4,
                }]
                assert page.url == f"http://127.0.0.1:{server.server_port}/"
                assert not errors, errors
            finally:
                browser.close()
    finally:
        server.shutdown()
        server.server_close()


if __name__ == "__main__":
    main()
