import time

from forest.errors import ForestError


async def advance(ctx, task):
    generation_id = task["payload"]["generation_id"]
    last_revision = 0
    deadline = time.monotonic() + 140
    for _ in range(32):
        progress = await ctx.queries.call(
            "memory.query-graph-rebuild",
            {"generation_id": generation_id,
             "requested_by": task["payload"]["requested_by"]},
            timeout=10,
        )
        last_revision = progress["replayed_through"]
        if progress["published_revision"] is not None:
            return {
                "generation_id": generation_id,
                "replayed_through": last_revision,
                "published": True,
            }
        if progress["active_generation_id"] != progress["base_generation_id"]:
            raise ForestError(
                "MEMORY_GRAPH_REBUILD_BASE_CHANGED",
                "The candidate's base generation is no longer active",
                409,
            )
        if last_revision < progress["active_revision"]:
            next_revision = last_revision + 1
            replayed = await ctx.commands.call(
                "memory.replay-graph-revision",
                {"generation_id": generation_id, "revision": next_revision},
                operation_id=ctx.operations.stable(
                    f"graph-replay:{generation_id}:{next_revision}"
                ),
                timeout=90,
            )
            last_revision = replayed["replayed_through"]
        else:
            try:
                published = await ctx.uow.call(
                    "memory.publish-graph-rebuild",
                    {
                        "payload_epoch": {
                            "protocol": "forest.payload-epoch-capture/1"
                        },
                        "memory": {
                            "protocol": "forest.memory-graph-publish/1",
                            "deletion_epoch_step_id": "payload_epoch",
                            "generation_id": generation_id,
                            "expected_active_generation_id": progress[
                                "active_generation_id"
                            ],
                            "expected_active_revision": progress[
                                "active_revision"
                            ],
                        },
                    },
                    operation_id=ctx.operations.stable(
                        f"graph-publish:{generation_id}:{last_revision}"
                    ),
                    timeout=30,
                )
            except ForestError as exc:
                if exc.code != "MEMORY_GRAPH_PUBLISH_STALE":
                    raise
            else:
                return {
                    "generation_id": generation_id,
                    "replayed_through": published["graph_revision"],
                    "published": True,
                }
        if time.monotonic() >= deadline:
            break
    return {
        "generation_id": generation_id,
        "replayed_through": last_revision,
        "published": False,
    }
