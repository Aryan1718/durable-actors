"""Shared field initialization and invocation identity for both actor styles."""

from __future__ import annotations

from copy import deepcopy
from typing import TYPE_CHECKING, Generic

from typing_extensions import TypeVar

from .json import JsonValue

if TYPE_CHECKING:
    from .database import RuntimeActorContext

Metadata = TypeVar("Metadata", default=JsonValue)
Incoming = TypeVar("Incoming", default=JsonValue)
Outgoing = TypeVar("Outgoing", default=Incoming)
Tag = TypeVar("Tag", bound=str, default=str)


class ActorBase(Generic[Metadata, Incoming, Outgoing, Tag]):
    __slots__ = ("__runtime_context",)

    def __init__(self, context: RuntimeActorContext | None = None) -> None:
        """Initialize independent field defaults; the runtime restores persisted values afterward."""
        from .contract import describe_actor

        self.__runtime_context = context
        if context is not None:
            context.bind(self)
        for field in describe_actor(type(self)).fields.values():
            value = (
                field.default_factory()
                if field.default_factory is not None
                else deepcopy(field.default)
            )
            setattr(self, field.name, value)

    def _runtime_context(self) -> RuntimeActorContext:
        if self.__runtime_context is None:
            raise RuntimeError("actor runtime context is unavailable during construction")
        return self.__runtime_context

    @property
    def id(self) -> str:
        """Identity of this actor, available during an active invocation or socket hook."""
        from .socket import current_scope

        return current_scope(self).actor_id

    def set_websocket_auto_response(
        self, request: str | None = None, response: str | None = None
    ) -> None:
        """Match raw text frames without waking the actor. Omit both values to clear."""
        from .socket import current_scope

        current_scope(self).set_websocket_auto_response(request, response)
