"""Shell presentation of the kernel audit page; no audit storage lives here."""

from forest.errors import ForestError
from forest.sdk import Plugin, service


def _field(value):
    return "-" if value is None else str(value)


class AuditConsole(Plugin):
    async def prepare(self, ctx, config, restored_state=None):
        await super().prepare(ctx, config, restored_state)
        if dict(config) != {"protocol": "forest.audit-console/1"}:
            raise ForestError("AUDIT_CONSOLE_CONFIGURATION_INVALID", "Audit console configuration is invalid", 422)

    @service("shell.audit-console.execute", semantics="read")
    async def tail(self, payload, ctx):
        arguments = payload["arguments"]
        for kind, identity in (
            ("resource_kind", "resource_id"),
            ("operation_kind", "operation_id"),
        ):
            if (kind in arguments) != (identity in arguments):
                raise ForestError(
                    "SHELL_ARGUMENTS_INVALID",
                    f"{kind} and {identity} must be supplied together",
                    422,
                )
        page = await ctx.services.call(
            "audit.page.snapshot",
            {"protocol": "forest.audit-page-request/1", "limit": 10, **arguments},
            operation_id=ctx.operations.stable("audit-tail"),
            timeout=10,
        )
        if not page["records"]:
            return {"text": "Forest audit: no matching records"}
        lines = []
        for item in page["records"]:
            actor = f"{_field(item['actor_kind'])}:{_field(item['actor_id'])}"
            resource = f"{_field(item['resource_kind'])}:{_field(item['resource_id'])}"
            operation = f"{_field(item['operation_kind'])}:{_field(item['operation_id'])}"
            lines.append(
                f"{item['created_at']} {item['severity']} {item['action']} "
                f"producer={item['producer_component_id']} actor={actor} "
                f"resource={resource} operation={operation} id={item['audit_id']}"
            )
        if page["next_before"] is not None:
            lines.append(f"next: --before {page['next_before']}")
        return {"text": "Forest audit:\n" + "\n".join(lines)}
