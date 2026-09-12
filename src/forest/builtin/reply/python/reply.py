from forest.sdk import Plugin, service


class Reply(Plugin):
    """A deterministic command-style responder; it is not an LLM or image captioner."""

    @service("reply.respond")
    async def respond(self, payload, ctx):
        text = payload["template"]
        # Substitute only fixed tokens; user text never becomes Python format syntax.
        substitutions = {"{text}": payload["text"], "{sender}": payload["sender"],
                         "{group}": payload["group"]}
        import re
        text = re.sub(r"\{(?:text|sender|group)\}", lambda match: substitutions[match.group()], text)
        return {"parts": [[{"type": "text", "text": text[:4000]}]]}
