import asyncio

from fixtures.host_database import HostDatabase

from durable_actors.runtime import ActorRuntime as Runtime
from durable_actors.sqlite import DatabaseProxy


class ActorRuntime(Runtime):
    def __init__(self, actor, effects, host=None, loop=None):
        self.host = host or HostDatabase()
        loop = loop or asyncio.get_running_loop()
        identity = {"project_id": "local", "actor_name": actor.__name__, "actor_id": "one"}
        super().__init__(actor, identity, effects, DatabaseProxy(self.host.call, loop))
