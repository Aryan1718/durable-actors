from dataclasses import Field
from typing import Any, TypeGuard

from .actor import Actor
from .actor_base import ActorBase
from .async_actor import AsyncActor


def is_document(value: object) -> TypeGuard[dict[str, Any]]:
    return isinstance(value, dict)


def is_list(value: object) -> TypeGuard[list[Any]]:
    return isinstance(value, list)


def is_actor(value: object) -> TypeGuard[type[ActorBase[Any, Any, Any, Any]]]:
    return (
        isinstance(value, type)
        and value not in (Actor, AsyncActor)
        and issubclass(value, (Actor, AsyncActor))
    )


def is_field(value: object) -> TypeGuard[Field[Any]]:
    return isinstance(value, Field)
