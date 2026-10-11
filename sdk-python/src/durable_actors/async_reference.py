"""Typed async references using source actor definitions."""

from __future__ import annotations

from collections.abc import Callable
from functools import wraps
from typing import Any, TypeVar, cast

from pydantic import TypeAdapter

from .async_actor import AsyncActor
from .async_client import AsyncActorTransport, default_async_client
from .async_connection import AsyncConnection
from .client import component
from .contract import Method, decode, describe_actor, encode
from .json import JsonValue
from .reference import remote_arguments, unavailable

A = TypeVar("A", bound=AsyncActor[Any, Any, Any, Any])


def actor_reference(actor: type[A], actor_id: str, transport: AsyncActorTransport | None) -> A:
    component(actor_id, 128)
    definition = describe_actor(actor)
    client = transport if transport is not None else default_async_client()
    metadata_adapter, incoming, outgoing = (
        TypeAdapter(hint) for hint in definition.socket_types[:3]
    )

    async def connect(self: A, metadata: Any) -> AsyncConnection[Any, Any, JsonValue, JsonValue]:
        grant = await client.prepare_websocket(
            actor.__name__, actor_id, encode(metadata_adapter, metadata)
        )
        return await AsyncConnection[Any, Any, JsonValue, JsonValue].open(
            grant, incoming, outgoing, TypeAdapter(JsonValue), TypeAdapter(JsonValue)
        )

    async def broadcast(
        self: A,
        message: Any,
        *,
        except_ids: tuple[str, ...] = (),
        tags: tuple[str, ...] = (),
        tag_match: str = "all",
    ) -> None:
        if except_ids or tags or tag_match != "all":
            raise ValueError(
                "source references broadcast to all connections; filtered broadcasts belong in actor methods"
            )
        await client.broadcast(actor.__name__, actor_id, encode(outgoing, message))

    members: dict[str, Any] = {
        name: remote_method(actor, actor_id, client, name, method)
        for name, method in definition.methods.items()
    }
    members.update({name: property(unavailable) for name in definition.fields})
    members.update(
        {"connect": connect, "broadcast": broadcast, "id": property(lambda self: actor_id)}
    )
    reference = type(f"{actor.__name__}Reference", (actor,), members)
    return cast(A, object.__new__(reference))


def remote_method(
    actor: type[A], actor_id: str, client: AsyncActorTransport, name: str, method: Method
) -> Callable[..., Any]:
    @wraps(getattr(actor, name))
    async def invoke(self: A, *args: Any, **kwargs: Any) -> Any:
        values = remote_arguments(method, args, kwargs)
        result = await client.invoke(actor.__name__, actor_id, name, values)
        return decode(method.result, result)

    return invoke
