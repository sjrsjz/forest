"""Persistent Agent quanta, separate from conversation scheduling capacity."""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from copy import deepcopy
from dataclasses import asdict, dataclass, is_dataclass
from typing import Literal
from uuid import uuid4

from jsonschema import Draft202012Validator

from forest.errors import ForestError


@dataclass(frozen=True)
class RunDisposition:
    """A scheduling hint backed by an already committed Agent checkpoint.

    Waiting and yielding never clear the conversation's active Session. A final
    result is read again by the dedicated fenced finalizer, not carried as an
    unversioned list of reply parts through the scheduler.
    """

    status: Literal[
        "YIELD",
        "WAIT_INPUT",
        "WAIT_TOOL",
        "WAIT_APPROVAL",
        "BLOCKED_UNKNOWN",
        "FINAL_READY",
        "COMPLETED",
        "FAILED",
        "CANCELLED",
    ]
    run_id: str
    checkpoint_seq: int
    error_code: str | None = None


class AgentExecutionError(ForestError):
    """Checkpoint needs recovery; the generic callback path cannot end its Run."""


def _wrapped_arguments_schema(schema: dict) -> dict:
    """Keep local fragment resolution correct beneath the authority wrapper.

    Package admission prohibits nonempty $id/resource changes. Named anchors
    remain in this resource; JSON pointers and root recursion move beneath
    /properties/arguments. Traverse schema locations only, never instances in
    const/default/examples: https://json-schema.org/draft/2020-12/json-schema-core
    """
    result = deepcopy(schema)
    maps = {"$defs", "definitions", "properties", "patternProperties", "dependentSchemas"}
    singles = {
        "additionalProperties",
        "unevaluatedProperties",
        "propertyNames",
        "items",
        "contains",
        "unevaluatedItems",
        "not",
        "if",
        "then",
        "else",
        "contentSchema",
    }
    arrays = {"allOf", "anyOf", "oneOf", "prefixItems"}

    def rewrite(node):
        if not isinstance(node, dict):
            return
        for key in ("$ref", "$dynamicRef"):
            value = node.get(key)
            if isinstance(value, str) and (value == "#" or value.startswith("#/")):
                node[key] = "#/properties/arguments" + value[1:]
        for key in maps:
            if isinstance(node.get(key), dict):
                for child in node[key].values():
                    rewrite(child)
        for key in singles:
            rewrite(node.get(key))
        for key in arrays:
            if isinstance(node.get(key), list):
                for child in node[key]:
                    rewrite(child)

    rewrite(result)
    return result


class AgentRunner:
    """Advance one durable model/tool group, then yield real execution capacity.

    Host callbacks resolve frozen profiles and current authorization. They never
    return credentials in the persisted spec. This object owns no shared client,
    background task, platform route or memory summary job.
    """

    def __init__(
        self,
        messages,
        kernel,
        *,
        resolve_spec: Callable[..., Awaitable[dict]],
        resolve_client: Callable[..., Awaitable[tuple]],
        materialize_context: Callable[..., Awaitable[tuple]],
        authorize_tool: Callable[..., Awaitable[None]],
        state=None,
    ):
        if state is None:
            from forest.storage.agent import AgentStore

            state = AgentStore(messages)
        self.messages, self.kernel, self.state = messages, kernel, state
        self.resolve_spec, self.resolve_client = resolve_spec, resolve_client
        self.materialize_context, self.authorize_tool = materialize_context, authorize_tool

    @staticmethod
    def disposition(head: dict) -> RunDisposition:
        phase = head["phase"]
        status = {"READY_MODEL": "YIELD", "PREPARING_MODEL": "YIELD", "RETRY_MODEL": "YIELD"}.get(
            phase, phase
        )
        if status not in {
            "YIELD",
            "WAIT_INPUT",
            "WAIT_TOOL",
            "WAIT_APPROVAL",
            "BLOCKED_UNKNOWN",
            "FINAL_READY",
            "COMPLETED",
            "FAILED",
            "CANCELLED",
        }:
            raise AgentExecutionError(
                "AGENT_RECOVERY_REQUIRED", "The current model attempt needs reconciliation", 503
            )
        return RunDisposition(
            status, head["run_id"], head["checkpoint_seq"], head.get("error_code")
        )

    async def advance(self, actor, selected: dict, context: dict) -> RunDisposition:
        """The scheduler calls this once per fair quantum under an execution slot."""
        run_id = selected["run_id"]
        head = None
        try:
            head = await self.state.load(actor, run_id)
            spec = head["spec"] if head is not None else await self.resolve_spec(actor, None)
            # Actor handoff renews only execution ownership. The original spec,
            # input, request bytes and tool operation identities remain fixed.
            head = await self.state.ensure_run(actor, run_id, spec)
            if head["phase"] == "BLOCKED_UNKNOWN":
                for tool in head.get("tools", []):
                    if tool["status"] == "UNKNOWN":
                        head = await self.state.recover_tool(actor, run_id, tool["tool_call_id"])
                if head["phase"] == "BLOCKED_UNKNOWN":
                    return self.disposition(head)
            if head["phase"] in {
                "FINAL_READY",
                "WAIT_APPROVAL",
                "COMPLETED",
                "FAILED",
                "CANCELLED",
            }:
                return self.disposition(head)
            if head["phase"] == "WAIT_TOOL":
                return self.disposition(await self._tools(actor, head))
            if head["phase"] in {"READY_MODEL", "WAIT_INPUT", "PREPARING_MODEL", "RETRY_MODEL"}:
                head = await self._model(actor, head)
                if head["phase"] == "WAIT_TOOL":
                    head = await self._tools(actor, head)
                return self.disposition(head)
            raise AgentExecutionError(
                "AGENT_RECOVERY_REQUIRED", "A previous dispatch still requires reconciliation", 503
            )
        except asyncio.CancelledError:
            raise
        except AgentExecutionError:
            raise
        except ForestError as exc:
            if head is None:
                # No Agent attempt exists yet. The legacy scheduler can record
                # an explicit configuration/preparation failure for this Run.
                raise
            if exc.code in {
                "STALE_ACTOR",
                "STALE_HOST",
                "STALE_BINDING",
                "STALE_AGENT_OWNER",
                "MODEL_ATTEMPT_STALE",
                "AGENT_TOOL_STALE",
                "RUN_NOT_ACTIVE",
                "DATA_ADMISSION_CLOSED",
                "STORAGE_SEALED",
                "CONTROL_CLOSED",
            }:
                raise AgentExecutionError(
                    exc.code, "Agent ownership changed; recover from its checkpoint", 503
                ) from None
            try:
                ended = await self.state.fail(actor, run_id, exc.code)
            except Exception:
                raise AgentExecutionError(
                    "AGENT_RECOVERY_REQUIRED", "Agent failure could not be committed", 503
                ) from None
            return self.disposition(ended)
        except Exception:
            # Database/transport uncertainty must not route through the legacy
            # callback end_run path and falsely mark an executed tool abandoned.
            raise AgentExecutionError(
                "AGENT_RECOVERY_REQUIRED", "Agent progress needs durable reconciliation", 503
            ) from None

    async def _model(self, actor, head: dict) -> dict:
        from forest.models import ModelResult, prepare_model_request

        run_id, spec = head["run_id"], head["spec"]
        values = spec.get("values", spec)
        client, capabilities = await self.resolve_client(actor, spec)
        if head["phase"] == "RETRY_MODEL":
            prepared = await self.state.replay_prepared(actor, run_id)
        else:
            if head["phase"] != "PREPARING_MODEL":
                boundary = await self.state.boundary(
                    actor, run_id, f"agent-boundary:{run_id}:{head['next_step_seq']}"
                )
                # PREPARING_MODEL recovery already owns a frozen input snapshot.
                head = boundary.get("head", boundary)
                if head["phase"] in {"WAIT_INPUT", "FINAL_READY", "FAILED", "CANCELLED"}:
                    return head
            snapshot = await self.state.snapshot(actor, run_id)
            frozen_snapshot = deepcopy(snapshot)
            materialized, media = await self.materialize_context(actor, spec, deepcopy(snapshot))
            # Media enrichment cannot smuggle queued input into a frozen model
            # step, even when the host's materializer returns a new dict.
            if materialized != frozen_snapshot:
                raise ForestError(
                    "MODEL_INPUT_CUT_CHANGED", "Media materialization changed canonical input", 409
                )
            prepared = prepare_model_request(
                materialized,
                capabilities,
                request_id="model:" + snapshot["logical_step_id"],
                transport_attempt_id=uuid4().hex,
                system_prompt=values.get("system_prompt", ""),
                tools=self._tool_schemas(spec, snapshot),
                media=media,
                output_tokens=values.get("max_output_tokens", 1024),
                stream=values.get("stream", True),
            )
        grant = await self.state.prepare_model(
            actor, run_id, "model-attempt:" + prepared.transport_attempt_id, prepared
        )
        permit_started = False

        async def check(request):
            nonlocal permit_started
            if request != prepared:
                raise ForestError(
                    "MODEL_REQUEST_TAMPERED", "Transport changed its prepared request", 409
                )
            permit_started = True
            await self.state.check_model_permit(actor, grant, request)

        try:
            result = await client.dispatch(
                prepared, permitcheck=check, timeout=values.get("model_timeout", 30)
            )
        except asyncio.CancelledError:
            # A cancelled local stream cannot establish that the Provider never
            # executed or billed it. Preserve uncertainty before releasing Actor.
            result = ModelResult(
                "UNKNOWN" if permit_started else "NOT_SENT",
                prepared.request_id,
                prepared.transport_attempt_id,
                prepared.payload_digest,
                error_code="MODEL_CANCELLED",
            )
            try:
                await self.state.complete_model(actor, grant, result)
            except Exception:
                pass  # New Actor reconciliation will inspect the durable attempt.
            raise
        except Exception:
            result = ModelResult(
                "UNKNOWN" if permit_started else "NOT_SENT",
                prepared.request_id,
                prepared.transport_attempt_id,
                prepared.payload_digest,
                error_code="MODEL_DISPATCH_FAILED",
            )
        try:
            return await self.state.complete_model(actor, grant, result)
        except Exception:
            # Revocation or a lost DB acknowledgement after dispatch does not
            # prove that the Provider did not execute the frozen request.
            raise AgentExecutionError(
                "AGENT_RECOVERY_REQUIRED", "Model observation needs reconciliation", 503
            ) from None

    async def _observe_tool(self, actor, grant, *, status, result):
        try:
            return await self.state.observe_tool(actor, grant, status=status, result=result)
        except Exception:
            # A current-policy rejection can block result publication after the
            # effect started. Preserve ownership handoff and UNKNOWN recovery;
            # do not let generic failure handling release the foreground Run.
            raise AgentExecutionError(
                "AGENT_TOOL_RECOVERY_REQUIRED", "Tool observation needs reconciliation", 503
            ) from None

    @staticmethod
    def _tool_schemas(spec: dict, snapshot: dict) -> list[dict]:
        triggers = [trigger["trigger_id"] for trigger in snapshot.get("triggers", [])]
        tools = []
        for definition in spec.get("tools", []):
            schema = _wrapped_arguments_schema(definition.get("input_schema", {}))
            tools.append(
                {
                    "name": definition["name"],
                    "description": definition.get(
                        "description", "Call an explicitly authorized Bot service"
                    ),
                    "parameters": {
                        "type": "object",
                        "additionalProperties": False,
                        "properties": {
                            "authority_trigger_id": {"type": "string", "enum": triggers},
                            "arguments": schema,
                        },
                        "required": ["authority_trigger_id", "arguments"],
                    },
                }
            )
        return tools

    async def _tools(self, actor, head: dict) -> dict:
        run_id, spec = head["run_id"], head["spec"]
        definitions = {tool["name"]: tool for tool in spec.get("tools", [])}
        # Rows are created together in the Provider's call_index order. Each
        # complete result is saved before another effect is attempted.
        for proposal in sorted(head.get("tools", []), key=lambda tool: tool["call_index"]):
            if proposal["status"] in {"SUCCEEDED", "FAILED", "CANCELLED"}:
                continue
            if proposal["status"] == "UNKNOWN":
                return await self.state.load(actor, run_id)
            # An accepted Web decision moves the head to WAIT_TOOL while the
            # proposal remains WAIT_APPROVAL. The store consumes that exact
            # decision in permit_tool; its row status is not a reason to wait.
            definition = definitions.get(proposal["tool_name"])
            wrapper = proposal["arguments"]
            if definition is None:
                raise ForestError(
                    "AGENT_TOOL_NOT_ALLOWED", "The model proposed an unconfigured tool", 403
                )
            if not isinstance(wrapper, dict) or set(wrapper) != {
                "authority_trigger_id",
                "arguments",
            }:
                raise ForestError(
                    "AGENT_TOOL_ARGUMENTS_INVALID",
                    "Tool arguments need one explicit trigger authority",
                    422,
                )
            if not isinstance(wrapper["authority_trigger_id"], str):
                raise ForestError(
                    "AGENT_TOOL_AUTHORITY_INVALID",
                    "Tool authority must name a claimed trigger",
                    403,
                )
            snapshot = head["current_step"]["snapshot"]
            trigger = next(
                (
                    item
                    for item in snapshot.get("triggers", [])
                    if item["trigger_id"] == wrapper["authority_trigger_id"]
                ),
                None,
            )
            if trigger is None or not trigger.get("principal_ref"):
                raise ForestError(
                    "AGENT_TOOL_AUTHORITY_INVALID",
                    "Tool authority does not belong to this Run",
                    403,
                )
            if not Draft202012Validator(definition.get("input_schema", {})).is_valid(
                wrapper["arguments"]
            ):
                raise ForestError(
                    "AGENT_TOOL_ARGUMENTS_INVALID",
                    "Tool arguments do not match their frozen contract",
                    422,
                )
            await self.authorize_tool(actor, spec, definition, trigger)
            grant = await self.state.permit_tool(
                actor,
                run_id,
                proposal["tool_call_id"],
                authority_trigger_id=trigger["trigger_id"],
                requires_approval=definition.get("approval", "always") == "always",
            )
            grant_data = (
                {"status": "DISPATCHING", **asdict(grant)} if is_dataclass(grant) else grant
            )
            if grant_data["status"] == "WAIT_APPROVAL":
                return await self.state.load(actor, run_id)
            if grant_data["status"] in {"SUCCEEDED", "FAILED", "CANCELLED"}:
                continue
            if grant_data["status"] != "DISPATCHING":
                raise AgentExecutionError(
                    "AGENT_TOOL_RECOVERY_REQUIRED", "Tool dispatch is not currently permitted", 503
                )
            try:
                result = await self.kernel.invoke(
                    definition["component_id"],
                    definition["service"],
                    wrapper["arguments"],
                    operation_id=grant_data["operation_id"],
                    agent_tool_id=grant_data["operation_id"],
                    contract_major=definition.get("contract_major", 1),
                    timeout=30,
                    expected_policy_revision=actor.policy_revision,
                )
            except asyncio.CancelledError:
                try:
                    await self.state.observe_tool(
                        actor, grant, status="UNKNOWN", result={"error_code": "TOOL_CANCELLED"}
                    )
                except Exception:
                    pass
                raise
            except Exception as exc:
                if isinstance(exc, ForestError) and exc.code == "BACKPRESSURE":
                    call = await self.kernel.store.read_call(grant_data["operation_id"])
                    if call is None:
                        # Kernel rejected before any durable invocation existed.
                        # Keep the same effect operation ID for a bounded retry.
                        return await self._observe_tool(
                            actor, grant, status="NOT_SENT", result={"error_code": exc.code}
                        )
                # A completed Kernel receipt is the only authority for recovering
                # a lost return value. Never re-invoke an uncertain side effect.
                head = await self._observe_tool(
                    actor,
                    grant,
                    status="UNKNOWN",
                    result={
                        "error_code": exc.code if isinstance(exc, ForestError) else "TOOL_FAILED"
                    },
                )
                head = await self.state.recover_tool(actor, run_id, proposal["tool_call_id"])
                if head["phase"] == "BLOCKED_UNKNOWN":
                    return head
            else:
                head = await self._observe_tool(actor, grant, status="SUCCEEDED", result=result)
        return await self.state.close_tools(actor, run_id)

    async def finalize(self, actor, disposition: RunDisposition, delivery) -> dict:
        if disposition.status != "FINAL_READY":
            raise ForestError(
                "AGENT_FINAL_NOT_READY", "Only a persisted final result can be committed", 409
            )
        try:
            return await self.state.commit_final(
                actor, disposition.run_id, "agent-final:" + disposition.run_id, delivery
            )
        except ForestError as exc:
            if exc.code != "AGENT_CONFIGURATION_REVOKED":
                raise
            # FINAL_READY proves the entire model/tool group has closed. An
            # explicit disable/key revocation may terminate this unsent final;
            # the store retains its immutable output for inspection. Transport,
            # storage and ownership uncertainty never use this branch.
            return await self.state.fail(actor, disposition.run_id, exc.code)
