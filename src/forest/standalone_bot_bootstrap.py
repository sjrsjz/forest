"""Adapt the standalone ``forest bot`` environment to package creation inputs."""

from __future__ import annotations

from copy import deepcopy

from forest.errors import ForestError
from forest.model_contract import ModelCapabilities


def standalone_creation_components(settings, snapshots) -> dict[str, dict]:
    """Translate local process options before generic package materialization."""

    components = {
        snapshot.component["id"]: deepcopy(snapshot.bootstrap_contribution()[2])
        for _source, snapshot in snapshots
        if snapshot.bootstrap_contribution() is not None
    }
    required = {
        "provider.chat-completions", "embedding.multimodal", "vector.index",
        "conversation.trigger-policy", "adapter.onebot", "agent.runtime", "shell.command",
    }
    if not required <= components.keys():
        raise ForestError(
            "BOT_BOOTSTRAP_COMPONENT_MISSING",
            "Standalone Bot requires its official creation components", 409,
        )
    capabilities = settings.model_capabilities or ModelCapabilities(
        deployment_id=settings.model_deployment_id,
        model=settings.model,
        capability_revision="operator-declared/1",
        input_modalities=settings.model_input_modalities,
        max_output_tokens=settings.model_max_output_tokens,
    )
    embedding_profile = {
        "deployment_id": settings.embedding_deployment_id,
        "model_revision": settings.embedding_model_revision,
        "model": settings.embedding_model,
        "dimensions": settings.embedding_dimensions,
    }
    components["provider.chat-completions"].update({
        "base_url": settings.model_base_url,
        "credential_ref": settings.model_credential_ref,
        "timeout_seconds": settings.model_timeout_seconds,
        "max_connections": settings.model_max_connections,
        "capabilities": capabilities.as_dict(),
    })
    components["embedding.multimodal"].update({
        "endpoint": settings.embedding_endpoint,
        "credential_ref": settings.embedding_credential_ref,
        "timeout_seconds": settings.embedding_timeout_seconds,
        "max_connections": settings.embedding_max_connections,
        "profile": embedding_profile,
    })
    components["vector.index"].update({
        "endpoint": settings.vector_index_endpoint,
        "credential_ref": settings.vector_index_credential_ref,
        "timeout_seconds": settings.vector_index_timeout_seconds,
    })
    components["conversation.trigger-policy"].update({
        "allowed_group_ids": list(settings.allowed_group_ids),
        "group_trigger_mode": settings.group_trigger_mode,
        "private_enabled": settings.private_enabled,
    })
    components["adapter.onebot"].update({
        "self_id": settings.onebot_self_id,
        "websocket_url": settings.onebot_websocket_url,
        "access_token_ref": settings.onebot_secret_ref,
    })
    preset_system = settings.agent_preset_system or (
        (settings.agent_system_prompt,) if settings.agent_system_prompt else ()
    )
    components["agent.runtime"]["preset_system"] = list(preset_system)
    components["shell.command"]["administrator_principals"] = [
        "qq:" + value for value in settings.shell_administrator_ids
    ]
    return components
