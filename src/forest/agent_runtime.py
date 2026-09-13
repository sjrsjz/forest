"""Bot-owned model clients and frozen Agent configuration resolution."""

from __future__ import annotations

import asyncio
import json
from dataclasses import replace

from forest.agent import AgentRunner
from forest.errors import ForestError
from forest.models import ChatCompletionsClient, ModelCapabilities
from forest.packages import digest_json
from forest.storage.agent_config import AgentConfig


class _ScopedClient:
    """Allocate transport only for dispatch; drafts/config revisions own no pools."""

    def __init__(self, runtime, endpoint, api_key, *, child=False):
        self.runtime, self.endpoint, self.api_key = runtime, endpoint, api_key
        self.child = child

    async def dispatch(self, prepared, *, permitcheck, timeout):
        # Waiting for capacity is bounded by the same call timeout. Children
        # acquire their smaller global pool first, preserving foreground slots.
        async with asyncio.timeout(timeout):
            if self.child:
                async with self.runtime.global_child_slots:
                    return await self._limited(prepared, permitcheck, timeout)
            return await self._limited(prepared, permitcheck, timeout)

    async def _limited(self, prepared, permitcheck, timeout):
        async with self.runtime.global_model_slots, self.runtime.model_slots:
            return await self._dispatch(prepared, permitcheck=permitcheck, timeout=timeout)

    async def _dispatch(self, prepared, *, permitcheck, timeout):
        async with self.runtime.client_lock:
            if self.runtime.closed:
                raise ForestError("AGENT_RUNTIME_CLOSED", "Model runtime is closing", 503)
            client = ChatCompletionsClient(self.endpoint, self.api_key)
            self.runtime.clients.add(client)
        try:
            return await client.dispatch(prepared, permitcheck=permitcheck, timeout=timeout)
        finally:
            await client.aclose()
            self.runtime.clients.discard(client)


class AgentRuntime:
    def __init__(self, platform):
        self.platform = platform
        self.kernel, self.messages = platform.kernel, platform.messages
        self.config = AgentConfig(platform.bot.store, platform.vault)
        self.clients = set()
        self.client_lock = asyncio.Lock()
        # At most two child workers can occupy four HTTP slots; foreground
        # conversation processing always retains capacity.
        self.model_slots = asyncio.Semaphore(4)
        self.global_model_slots = getattr(
            platform, "global_model_slots", None
        ) or asyncio.Semaphore(32)
        self.global_child_slots = getattr(
            platform, "global_child_slots", None
        ) or asyncio.Semaphore(8)
        self.closed = False
        self.runner = AgentRunner(
            self.messages,
            self.kernel,
            resolve_spec=self.resolve_spec,
            resolve_client=self.resolve_client,
            materialize_context=self.materialize_context,
            authorize_tool=self.authorize_tool,
            retrieve_memory=self.retrieve_memory,
        )
        from forest.children import ChildScheduler
        from forest.storage.children import ChildStore

        self.children = ChildScheduler(
            ChildStore(self.runner.state), lambda: self.kernel.lease, self.resolve_child_client
        )

    async def start(self):
        await self.children.start()

    async def resolve_child_client(self, lease, child):
        saved = await self.children.store.load(lease)
        values = saved["spec"]["values"]
        revision = saved["spec"]["config_revision"]
        row = await self.config.runtime(lease.host, revision=revision)
        capabilities = self._capabilities(values, revision)
        return _ScopedClient(
            self,
            values["endpoint"],
            self.platform.vault.decrypt(row["secret_ciphertext"]),
            child=True,
        ), replace(capabilities, supports_tools=False)

    def _capabilities(self, values, revision):
        return ModelCapabilities(
            deployment_id=f"{self.messages.store.bot_id}:agent:{revision}",
            capability_revision=f"configuration:{revision}",
            model=values["model"],
            input_modalities=("text", "image") if values["vision"] else ("text",),
            supports_tools=values["tool_calls"],
            supports_streaming=values["stream"],
            stream_usage=values["stream_usage"],
            context_limit=values["context_tokens"],
            max_output_tokens=values["max_output_tokens"],
            image_token_upper_bound=values["image_token_upper_bound"],
            output_token_parameter=values["output_token_parameter"],
        )

    async def _configuration(self, actor, revision):
        async with self.messages.store.pool.connection() as conn, conn.transaction():
            binding, _ = await self.messages._actor(conn, actor)
            result = await AgentConfig.guard_in(conn, actor, revision)
            await self.messages._actor_final(conn, actor, binding)
            return result

    def _freeze_tool(self, tool):
        component = self.kernel.components.get(tool["component_id"])
        if component is None or tool["service"] not in component.validators:
            raise ForestError(
                "AGENT_TOOL_UNAVAILABLE", "Configured tool service is not loaded", 409
            )
        input_validator, output_validator, major = component.validators[tool["service"]]
        if major != tool["contract_major"]:
            raise ForestError(
                "AGENT_TOOL_CONTRACT_CHANGED", "Configured tool contract is unavailable", 409
            )
        return json.loads(
            json.dumps(
                {
                    **tool,
                    "input_schema": input_validator.schema,
                    "package_digest": component.snapshot.digest,
                    "contract_digest": digest_json(
                        {
                            "input": input_validator.schema,
                            "output": output_validator.schema,
                            "major": major,
                        }
                    ),
                }
            )
        )

    async def resolve_spec(self, actor, saved=None):
        if saved is not None:
            await self._configuration(actor, saved["config_revision"])
            return saved
        row = await self.config.runtime(actor.host)
        await self._configuration(actor, row["revision"])
        values = row["values"]
        tools = [self._freeze_tool(tool) for tool in values["allowed_tools"]]
        return {
            **values,
            "values": values,
            "config_revision": row["revision"],
            "secret_revision": row["secret_revision"],
            "tools": tools,
        }

    async def resolve_client(self, actor, spec):
        await self._configuration(actor, spec["config_revision"])
        values = spec["values"]
        capabilities = ModelCapabilities(
            deployment_id=f"{self.messages.store.bot_id}:agent:{spec['config_revision']}",
            capability_revision=f"configuration:{spec['config_revision']}",
            model=values["model"],
            input_modalities=("text", "image") if values["vision"] else ("text",),
            supports_tools=values["tool_calls"],
            supports_streaming=values["stream"],
            stream_usage=values["stream_usage"],
            context_limit=values["context_tokens"],
            max_output_tokens=values["max_output_tokens"],
            image_token_upper_bound=values["image_token_upper_bound"],
            output_token_parameter=values["output_token_parameter"],
        )
        async with self.client_lock:
            if self.closed:
                raise ForestError("AGENT_RUNTIME_CLOSED", "Model runtime is closing", 503)
            row = await self.config.runtime(actor.host, revision=spec["config_revision"])
            return _ScopedClient(
                self, values["endpoint"], self.platform.vault.decrypt(row["secret_ciphertext"])
            ), capabilities

    async def materialize_context(self, actor, spec, snapshot):
        from forest.storage.media import MediaStore

        # Canonical refs survive text-only projection and remain available to
        # the independent multimodal memory/embedding path.
        media = await MediaStore(self.messages).materialize(
            actor, snapshot, allowed_origins=spec["values"]["media_allowed_origins"]
        )
        return snapshot, media

    async def retrieve_memory(self, actor, spec, snapshot):
        return await self.platform.memory.retrieve_for_step(actor, snapshot, self.runner.state)

    async def authorize_tool(self, actor, spec, tool, trigger):
        current = await self._configuration(actor, spec["config_revision"])
        allowed = next(
            (
                item
                for item in current["current_values"]["allowed_tools"]
                if item["name"] == tool["name"]
            ),
            None,
        )
        if allowed is None or any(tool[key] != value for key, value in allowed.items()):
            raise ForestError("AGENT_TOOL_REVOKED", "Tool authorization has changed", 403)
        actual = self._freeze_tool(allowed)
        if actual != tool:
            raise ForestError(
                "AGENT_TOOL_CONTRACT_CHANGED", "Tool implementation changed during this Run", 409
            )
        async with self.messages.store.pool.connection() as conn, conn.transaction():
            binding, conversation = await self.messages._actor(conn, actor)
            session = await self.messages._session(conn, conversation)
            row = await (
                await conn.execute(
                    "SELECT t.*,i.author FROM public.bot_conversation_triggers t JOIN public.bot_message_inbox i "
                    "ON (i.bot_id,i.event_id)=(t.bot_id,t.event_id) WHERE t.bot_id=%s AND t.trigger_id=%s "
                    "AND t.conversation_id=%s AND t.status='CLAIMED' AND t.claimed_run_id=%s",
                    (
                        self.messages.store.bot_id,
                        trigger["trigger_id"],
                        actor.conversation_id,
                        session["active_run_id"] if session else None,
                    ),
                )
            ).fetchone()
            if not row or row["principal_ref"] != trigger["principal_ref"]:
                raise ForestError(
                    "AGENT_TOOL_AUTHORITY_INVALID", "Tool trigger is no longer claimed", 403
                )
            if row["author"].get("identity_origin") not in {
                "adapter_verified",
                "adapter_anonymous",
            }:
                raise ForestError(
                    "AGENT_TOOL_FORBIDDEN", "System triggers delegate conversation only", 403
                )
            if tool["authority"] == "binding_admin" and (
                row["author"].get("identity_origin") != "adapter_verified"
                or row["author"].get("anonymous")
                or row["author"].get("is_self")
                or row["author"].get("external_user_id") not in binding["config"]["admin_user_ids"]
            ):
                raise ForestError(
                    "AGENT_TOOL_FORBIDDEN", "This trigger author cannot use the tool", 403
                )
            await self.messages._actor_final(conn, actor, binding)

    async def close(self):
        await self.children.close()
        async with self.client_lock:
            self.closed = True
            results = await asyncio.gather(
                *(client.aclose() for client in tuple(self.clients)), return_exceptions=True
            )
            if any(isinstance(result, BaseException) for result in results):
                raise ForestError("MODEL_CLIENT_CLEANUP_FAILED", "Model client cleanup failed", 503)
            self.clients.clear()
