from forest.sdk import Plugin, job, subscribe


class RoutingComponent(Plugin):
    @subscribe("tests.capture", "tests.ready", "1.0")
    async def capture(self, event, ctx):
        return None

    @job("tests.echo")
    async def echo(self, task, ctx):
        return {"echo": task["payload"]["value"]}
