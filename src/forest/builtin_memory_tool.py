"""Agent tools contributed by the Memory graph module."""

from __future__ import annotations

from forest.runtime_modules import RuntimeAgentToolContribution

MEMORY_EXACT_RECALL_TOOL = RuntimeAgentToolContribution(
    name="memory_recall_exact",
    service_id="agent.tool.memory-exact-recall",
    version=1,
    declaration={
        "name": "memory_recall_exact",
        "description": (
            "Read one to eight archived memory units by the exact source_unit_id values "
            "shown in previous memory evidence. The result preserves group and author provenance."
        ),
        "parameters": {
            "type": "object",
            "properties": {
                "source_unit_ids": {
                    "type": "array",
                    "items": {"type": "string", "minLength": 1, "maxLength": 256},
                    "minItems": 1,
                    "maxItems": 8,
                    "uniqueItems": True,
                }
            },
            "required": ["source_unit_ids"],
            "additionalProperties": False,
        },
        "strict": True,
    },
    approval_required=False,
    supports_query=True,
)
