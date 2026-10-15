"""Define actors whose RPCs and socket hooks run on the event loop."""

from __future__ import annotations

from typing import TYPE_CHECKING, Literal, Self

from .actor_base import ActorBase, Incoming, Metadata, Outgoing, Tag
from .database import AsyncActorAlarm, AsyncActorDatabase
from .json import JsonValue

if TYPE_CHECKING:
    from .async_client import AsyncActorTransport
    from .async_connection import AsyncConnection
    from .socket import ActorSocket


class AsyncActor(ActorBase[Metadata, Incoming, Outgoing, Tag]):
    """Async RPCs and socket hooks, serialized across awaits unless @interleave is used."""

    @classmethod
    def get(cls, actor_id: str, transport: AsyncActorTransport | None = None) -> Self:
        """Return an async reference with the source class's RPC signatures."""
        from .async_reference import actor_reference

        return actor_reference(cls, actor_id, transport)

    async def connect(
        self, metadata: Metadata
    ) -> AsyncConnection[Incoming, Outgoing, JsonValue, JsonValue]:
        """Open a typed WebSocket through a reference returned by get()."""
        raise RuntimeError("connect() requires an actor reference from get()")

    @property
    def db(self) -> AsyncActorDatabase:
        """Await SQLite statements; changes commit with fields after a successful call."""
        return AsyncActorDatabase(self._runtime_context().database)

    @property
    def alarm(self) -> AsyncActorAlarm:
        """The actor's single durable alarm slot."""
        return AsyncActorAlarm(self._runtime_context().alarm)

    async def on_connect(self, socket: ActorSocket[Metadata, Outgoing, Tag]) -> None:
        """Inspect, accept, or reject a connecting socket. The default accepts it."""
        pass

    async def on_message(
        self, socket: ActorSocket[Metadata, Outgoing, Tag], message: Incoming
    ) -> None:
        pass

    async def on_disconnect(
        self, socket: ActorSocket[Metadata, Outgoing, Tag], code: int, reason: str, was_clean: bool
    ) -> None:
        pass

    async def on_alarm(self) -> None:
        """Handle a due alarm. It is cleared afterward unless this hook sets a new one."""
        pass

    async def get_connections(
        self, tag: Tag | None = None
    ) -> list[ActorSocket[Metadata, Outgoing, Tag]]:
        """Fetch socket handles valid for the current invocation."""
        from .socket import current_scope

        scope = current_scope(self)
        return await scope.exchange(lambda: scope.get_connections(tag))

    async def get_connection_count(self) -> int:
        from .socket import current_scope

        scope = current_scope(self)
        return await scope.exchange(scope.get_connection_count)

    async def broadcast(
        self,
        message: Outgoing,
        *,
        except_ids: tuple[str, ...] = (),
        tags: tuple[Tag, ...] = (),
        tag_match: Literal["all", "any"] = "all",
    ) -> None:
        """Queue a typed message for selected connections during this invocation."""
        from .socket import current_scope

        current_scope(self).broadcast(message, except_ids, tags, tag_match)
