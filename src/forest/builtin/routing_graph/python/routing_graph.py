from forest.errors import ForestError
from forest.sdk import Plugin, job, service


def _plain(value):
    if hasattr(value, "items"):
        return {key: _plain(item) for key, item in value.items()}
    if isinstance(value, (tuple, list)):
        return [_plain(item) for item in value]
    return value


class RoutingGraph(Plugin):
    async def prepare(self, ctx, config, restored_state=None):
        await super().prepare(ctx, config, restored_state)
        if _plain(config) != {"protocol": "forest.routing.graph/2"}:
            raise ForestError(
                "ROUTING_CONFIGURATION_INVALID",
                "Routing graph configuration is invalid",
                422,
            )

    @service("routing.event.publish", semantics="idempotent")
    async def publish_event(self, payload, ctx):
        return await ctx.commands.call(
            "events.publish",
            _plain(payload),
            operation_id=ctx.operations.stable("event-publish"),
        )

    @service("routing.component-job.schedule", semantics="idempotent")
    async def schedule_component_job(self, payload, ctx):
        return await ctx.commands.call(
            "component-jobs.schedule",
            _plain(payload),
            operation_id=ctx.operations.stable("component-job-schedule"),
        )

    @job("routing.event-delivery.execute")
    async def deliver_event(self, task, ctx):
        work = _plain(task["payload"])
        return await ctx.routing_routes.deliver_event(
            event_id=work["event_id"],
            delivery_id=work["delivery_id"],
            subscriber_id=work["subscriber_id"],
            topic=work["topic"],
            contract=work["contract"],
            publisher_component_id=work["publisher_component_id"],
            publisher_generation_id=work["publisher_generation_id"],
            attempt=task["attempt"],
            payload=work["payload"],
            target_component_id=work["target_component_id"],
            target_generation_id=work["target_generation_id"],
            target_service_id=work["target_service_id"],
            target_service_version=work["target_service_version"],
            operation_id=work["attempt_id"],
            timeout=work["timeout_seconds"],
        )

    @job("routing.component-job.execute")
    async def deliver_component_job(self, task, ctx):
        work = _plain(task["payload"])
        return await ctx.routing_routes.deliver_component_job(
            job_id=work["job_id"],
            handler_id=work["handler_id"],
            scheduled_by_component_id=work["owner_component_id"],
            scheduled_by_generation_id=work["scheduled_by_generation_id"],
            attempt=task["attempt"],
            payload=work["payload"],
            target_component_id=work["target_component_id"],
            target_generation_id=work["target_generation_id"],
            target_service_id=work["target_service_id"],
            target_service_version=work["target_service_version"],
            operation_id=work["attempt_id"],
            timeout=work["timeout_seconds"],
        )
