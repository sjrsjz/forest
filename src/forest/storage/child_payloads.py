"""Private child content inherits the complete parent decision dependencies."""

from forest.errors import ForestError
from forest.packages import digest_json
from forest.storage.agent_payloads import CONTENT_UNAVAILABLE_CODES, AgentPayloadStore

VALUE_METADATA = frozenset(
    {
        "children_enabled",
        "child_concurrency",
        "max_children",
        "child_timeout",
        "child_max_output_tokens",
        "run_token_budget",
        "daily_token_budget",
        "max_output_tokens",
        "model_timeout",
        "stream",
    }
)


def spec_metadata(spec):
    return {
        "config_revision": spec["config_revision"],
        "depth": spec["depth"],
        "delegation_policy_revision": spec["delegation_policy_revision"],
        "values": {
            k: v
            for k, v in spec["values"].items()
            if k in VALUE_METADATA
            and (
                (k in {"children_enabled", "stream"} and type(v) is bool)
                or (
                    k not in {"children_enabled", "stream"}
                    and type(v) is int
                    and 0 < v <= 1000000000
                )
            )
        },
    }


def result_metadata(data, *, discarded_reason=None):
    result = {"status": data["status"]}
    if isinstance(data.get("usage"), dict):
        result["usage"] = {
            k: v
            for k, v in data["usage"].items()
            if k in {"total_tokens", "prompt_tokens", "completion_tokens"}
            and type(v) is int
            and 0 <= v <= 1000000000
        }
    if discarded_reason:
        result["discarded_reason"] = discarded_reason
    return result


class ChildPayloadStore:
    def __init__(self, store):
        self.store = store
        self.content = AgentPayloadStore(store)

    async def spawn_in(self, conn, child_id, step_id, tool, snapshot, spec):
        parents = list(await self.content.step_parents_in(conn, step_id, include_output=True))
        parents.append(tool["arguments_payload_id"])
        snapshot_id = await self.content.put_in(
            conn, "agent.child", "snapshot:" + child_id, snapshot, parents=parents
        )
        spec_id = await self.content.put_in(
            conn, "agent.child", "spec:" + child_id, spec, parents=(snapshot_id,)
        )
        return snapshot_id, spec_id

    async def guard_in(self, conn, row, attempt=None):
        refs = [row["snapshot_payload_id"], row["spec_payload_id"]]
        if row.get("result_payload_id"):
            refs.append(row["result_payload_id"])
        if attempt:
            refs.extend([attempt["request_payload_id"], attempt["manifest_payload_id"]])
        try:
            await self.content.payloads.require_available_in(
                conn, refs, expected_deletion_epoch=await self.content.epoch_in(conn)
            )
        except ForestError as exc:
            if exc.code in CONTENT_UNAVAILABLE_CODES:
                raise ForestError(
                    "AGENT_MEMORY_REVOKED", "Private analysis content is unavailable", 409
                ) from None
            raise

    async def hydrate_in(self, conn, row):
        result = dict(row)
        for field in ("snapshot", "spec", "result"):
            result[field] = await self.content.read_in(
                conn,
                row.get(field + "_payload_id"),
                required=field != "result" or row.get("result_payload_id") is not None,
            )
        return result

    async def hydrate_attempt_in(self, conn, attempt):
        return await self.content.hydrate_attempt_in(conn, attempt)

    async def request_in(self, conn, row, prepared):
        parents = (row["snapshot_payload_id"], row["spec_payload_id"])
        manifest_id = await self.content.put_in(
            conn,
            "model.manifest",
            "child:" + prepared.transport_attempt_id,
            prepared.manifest,
            parents=parents,
        )
        request_id = await self.content.put_in(
            conn,
            "model.request",
            "child:" + prepared.transport_attempt_id,
            prepared.body,
            parents=(manifest_id,),
            binary=True,
        )
        return request_id, manifest_id

    async def observation_in(self, conn, attempt, data):
        return await self.content.put_in(
            conn,
            "model.result",
            "child:" + attempt["attempt_id"],
            data,
            parents=(attempt["request_payload_id"], attempt["manifest_payload_id"]),
        )

    async def candidate_in(self, conn, row, result, observation_id):
        return await self.content.put_in(
            conn,
            "agent.child",
            "result:" + digest_json([row["child_id"], result]),
            result,
            parents=(row["snapshot_payload_id"], row["spec_payload_id"], observation_id),
        )
