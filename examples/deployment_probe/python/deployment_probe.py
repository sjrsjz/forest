from forest.sdk import Plugin, service


class DeploymentProbe(Plugin):
    @service("deployment.probe.echo", semantics="read")
    async def echo(self, payload, ctx):
        return {"echo": payload["value"]}
