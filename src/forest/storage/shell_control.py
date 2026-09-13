"""Shell-origin authorization for the same typed management repositories as Web."""

from __future__ import annotations

from forest.errors import ForestError
from forest.packages import digest_json
from forest.shell_registry import command_action
from forest.storage.control import BotControl


class ShellControl(BotControl):
    """One host-owned command admission, with authority rechecked in each transaction.

    No signed Web identity is fabricated. Only this instance's opaque admission
    can enter the shared management methods; it never crosses a Worker boundary.
    Operation receipts retain the originating platform command and principal.
    """

    def __init__(self, messages, lease, claim):
        super().__init__(messages.store)
        self.messages, self.lease = messages, lease
        self.command_id, self.owner_id = claim["command_id"], claim["owner_id"]
        self.claim_epoch, self.binding_id = claim["claim_epoch"], claim["binding_id"]
        self.admission = object()
        self.binding = None
        self.author = None

    async def _guard(self, conn, ticket, action, operation_id, payload, plan_digest=""):
        if ticket is not self.admission or operation_id != "shell-action:" + self.command_id:
            raise ForestError("SHELL_AUTHORITY_REJECTED", "Invalid command admission", 403)
        self.binding = await self.messages.bindings._guard(
            conn, self.lease, self.binding_id, data=False,
        )
        request = await (await conn.execute(
            "SELECT * FROM public.bot_shell_requests WHERE bot_id=%s AND command_id=%s FOR UPDATE",
            (self.store.bot_id, self.command_id),
        )).fetchone()
        now = await self.store._now(conn)
        if (request is None or request["status"] != "CLAIMED" or request["owner_id"] != self.owner_id
                or request["claim_epoch"] != self.claim_epoch or request["expires_at"] <= now
                or request["host_incarnation"] != self.lease.incarnation
                or request["binding_id"] != self.binding_id
                or request["binding_epoch"] != self.binding["binding_epoch"]
                or request["policy_revision"] != self.binding["_runtime"]["policy_revision"]
                or command_action(request["command_name"], request["gate_version"]) != action
                or request["arguments"] != payload
                or plan_digest != payload.get("plan_digest", "")):
            raise ForestError("SHELL_AUTHORITY_REJECTED", "Command claim changed or expired", 403)
        source = await (await conn.execute(
            "SELECT author,payload_id FROM public.bot_message_inbox WHERE bot_id=%s AND event_id=%s",
            (self.store.bot_id, request["event_id"]),
        )).fetchone()
        author = source["author"] if source else {}
        await self._scope(conn, request["conversation_id"], action, payload)
        if (author.get("identity_origin") != "adapter_verified" or author.get("anonymous") is not None
                or author.get("is_self") or not isinstance(author.get("principal_ref"), str)):
            raise ForestError("SHELL_FORBIDDEN", "Command author is not a current Bot administrator", 403)
        self.author = author
        # The caller still has business rows to lock. Read only the immutable
        # effective reference here; the content/admin decision is mandatory in
        # _time before this transaction can return or commit any result.
        config_ref = self.binding["config_payload_id"]
        if config_ref is None:
            raise ForestError("CONTROL_CONTENT_REVOKED", "Binding configuration is unavailable", 409)
        return BotControl.admission({
            "issuer_id": "shell:" + self.binding_id, "subject_id": author["principal_ref"],
            "operation_id": operation_id, "action": action, "input_digest": digest_json(payload),
            "plan_digest": plan_digest, "policy_revision": request["policy_revision"],
            "issued_at": now.isoformat(), "expires_at": request["expires_at"].isoformat(),
            "origin_kind": "shell", "source_binding_id": self.binding_id,
            "source_principal": author["principal_ref"], "source_event_id": request["event_id"],
            "source_conversation_id": request["conversation_id"], "command_id": self.command_id,
        }, payload, parents=(source["payload_id"], config_ref))

    async def _scope(self, conn, conversation_id, action, payload):
        target = None
        if action == "agent.cancel":
            target = await (await conn.execute(
                "SELECT conversation_id FROM public.bot_agent_runs WHERE bot_id=%s AND run_id=%s",
                (self.store.bot_id, payload["run_id"]),
            )).fetchone()
        elif action == "agent.approval":
            target = await (await conn.execute(
                "SELECT r.conversation_id FROM public.bot_agent_tool_approvals a "
                "JOIN public.bot_agent_runs r USING(bot_id,run_id) WHERE a.bot_id=%s AND a.approval_id=%s",
                (self.store.bot_id, payload["approval_id"]),
            )).fetchone()
        elif action.startswith("job.") and payload:
            from forest.storage.jobs import JobStore

            await JobStore.authorize_shell_scope_in(conn, self.store.bot_id, conversation_id, payload)
            return
        else:
            return
        if target is None or target["conversation_id"] != conversation_id:
            raise ForestError("SHELL_SCOPE_DENIED", "Command target is outside this conversation", 403)

    async def _time(self, conn, claims):
        await super()._time(conn, claims)
        if self.binding is None:
            raise ForestError("SHELL_AUTHORITY_REJECTED", "No command admission", 403)
        settings = await self.messages.bindings.current_config_in(
            conn, self.store.bot_id, self.binding_id,
            expected_binding_epoch=self.binding["binding_epoch"],
        )
        if self.author is None or self.author.get("external_user_id") not in settings["config"].get("admin_user_ids", []):
            raise ForestError("SHELL_FORBIDDEN", "Command author is not a current Bot administrator", 403)
        await self.messages.bindings._final_guard(conn, self.lease, self.binding, data=False, require_config=True)
