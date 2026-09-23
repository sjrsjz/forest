from forest.sdk import plugin, service


@plugin(
    id="example.auto-echo",
    version="0.1.0",
    requests=[],
    config_schema={
        "type": "object",
        "additionalProperties": False,
        "properties": {"prefix": {"type": "string", "maxLength": 32}},
    },
)
class AutoEcho:
    @service(
        "auto-echo.echo",
        semantics="read",
        contract="1.0",
        input_schema={
            "type": "object",
            "required": ["text"],
            "additionalProperties": False,
            "properties": {"text": {"type": "string", "maxLength": 4096}},
        },
        output_schema={
            "type": "object",
            "required": ["text"],
            "additionalProperties": False,
            "properties": {"text": {"type": "string"}},
        },
    )
    async def echo(self, payload, ctx):
        return {"text": str(ctx.config.get("prefix", "")) + payload["text"]}
