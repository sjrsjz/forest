"""Scope-neutral configuration application through a durable Graph request."""

from __future__ import annotations

from forest.errors import ForestError
from forest.packages import digest_json


class ControlRuntimeConfigurationPort:
    """A narrow command port shared by deployment and Host package Generations."""

    async def apply_configuration(
        self, configuration, *, operation_id: str, plan_id: str,
        expected_desired_revision: int, authorization, authorization_guard,
        validation_digest: str, authorization_replay_guard=None,
    ) -> dict:
        if self.graph_change_authority is None or authorization_guard is None:
            raise ForestError(
                f"{self.scope_kind.upper()}_CONFIGURATION_CHANGE_UNAVAILABLE",
                "Control-scope configuration change authority is unavailable", 503,
            )
        body = {
            "plan_id": plan_id,
            "expected_desired_revision": expected_desired_revision,
        }

        async def authorize_in(conn):
            return await authorization_guard(
                conn, ticket=authorization, component_id=configuration.component_id,
                operation_id=operation_id, action_key="configuration.apply",
                input_digest=digest_json(body),
                plan_digest=digest_json({"plan_id": plan_id}),
            )

        result = await self.graph_change_authority.enqueue_configuration(
            lease=self.authority_lease(), ticket=authorization,
            configuration=configuration, plan_id=plan_id,
            expected_desired_revision=expected_desired_revision,
            validation_digest=validation_digest,
            authorize_in=authorize_in,
        )
        return {
            "configuration": result["configuration"],
            "rebuild": {
                "request_id": result["request"]["operation_id"],
                "state": result["request"]["state"],
            },
        }
