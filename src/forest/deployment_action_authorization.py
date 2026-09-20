"""Transaction-bound authorization context for deployment management actions."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Awaitable, Callable

from forest.errors import ForestError
from forest.web_authority_ticket import WebAuthorityTicket


@dataclass(frozen=True)
class DeploymentActionAuthorization:
    ticket: WebAuthorityTicket
    guard: Callable[..., Awaitable[dict]]
    component_id: str
    action_key: str
    input_digest: str
    operation_id: str

    async def accept_in(self, conn) -> dict:
        return await self.guard(
            conn,
            ticket=self.ticket,
            component_id=self.component_id,
            operation_id=self.operation_id,
            action_key=self.action_key,
            input_digest=self.input_digest,
            plan_digest=None,
        )


async def authorize_deployment_action_in(conn, authorization, *, operation_id: str) -> None:
    if authorization is None:
        return
    if not isinstance(authorization, DeploymentActionAuthorization):
        raise ForestError(
            "WEB_AUTHORITY_TRANSACTION_REQUIRED",
            "Deployment action lacks its target transaction authorization guard",
            503,
        )
    await authorization.accept_in(conn)
