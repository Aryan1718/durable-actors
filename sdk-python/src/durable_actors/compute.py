"""Per-actor compute resources and placement."""

from __future__ import annotations

from collections.abc import Callable
from typing import Any, Literal, TypeVar
from weakref import WeakKeyDictionary

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from .actor_base import ActorBase
from .guards import is_actor

ComputeRegion = Literal[
    "canada",
    "north-america-east",
    "north-america-central",
    "north-america-south",
    "north-america-west",
    "europe-west",
    "asia-southeast",
]
A = TypeVar("A", bound=ActorBase[Any, Any, Any, Any])


class ComputeOptions(BaseModel):
    """Overrides for an actor's deployment defaults; omitted values are inherited.

    cpu and memory_mib are requests in cores and MiB; cpu_limit and memory_limit_mib
    cap them and must be at least the request. An unset limit inherits the server
    default, which may be no CPU cap. idle_timeout_ms is in milliseconds. regions
    limits placement without expressing priority; existing actors retain their
    saved region.
    """

    model_config = ConfigDict(extra="forbid", strict=True, frozen=True, allow_inf_nan=False)
    cpu: float | None = Field(default=None, ge=0.1, le=64, multiple_of=0.001)
    cpu_limit: float | None = Field(
        default=None, ge=0.1, le=64, multiple_of=0.001, serialization_alias="cpuLimit"
    )
    memory_mib: int | None = Field(default=None, ge=128, le=262144, serialization_alias="memoryMiB")
    memory_limit_mib: int | None = Field(
        default=None, ge=128, le=262144, serialization_alias="memoryLimitMiB"
    )
    idle_timeout_ms: int | None = Field(
        default=None, ge=1, le=86400000, serialization_alias="idleTimeoutMs"
    )
    regions: list[ComputeRegion] | None = Field(default=None, min_length=1, max_length=7)

    @field_validator("regions")
    @classmethod
    def unique_regions(cls, value: list[ComputeRegion] | None) -> list[ComputeRegion] | None:
        if value is not None and len(value) != len(set(value)):
            raise ValueError("regions must be unique")
        return value

    @model_validator(mode="after")
    def requests_within_limits(self) -> ComputeOptions:
        if self.cpu is not None and self.cpu_limit is not None and self.cpu > self.cpu_limit:
            raise ValueError("cpu must not exceed cpu_limit")
        if (
            self.memory_mib is not None
            and self.memory_limit_mib is not None
            and self.memory_mib > self.memory_limit_mib
        ):
            raise ValueError("memory_mib must not exceed memory_limit_mib")
        return self


_options: WeakKeyDictionary[type[ActorBase[Any, Any, Any, Any]], ComputeOptions] = (
    WeakKeyDictionary()
)


def compute(
    *,
    cpu: float | None = None,
    cpu_limit: float | None = None,
    memory_mib: int | None = None,
    memory_limit_mib: int | None = None,
    idle_timeout_ms: int | None = None,
    regions: list[ComputeRegion] | None = None,
) -> Callable[[type[A]], type[A]]:
    """Configure an actor class while preserving its type and method signatures.

    cpu and cpu_limit accept 0.1–64 cores in 0.001 increments; memory_mib and
    memory_limit_mib accept 128–262144; the server may allow a narrower range.
    A request must not exceed its limit. idle_timeout_ms accepts 1–86400000.
    regions must be nonempty and unique. Use once per actor class. Unspecified
    values inherit deployment defaults.
    """
    options = ComputeOptions(
        cpu=cpu,
        cpu_limit=cpu_limit,
        memory_mib=memory_mib,
        memory_limit_mib=memory_limit_mib,
        idle_timeout_ms=idle_timeout_ms,
        regions=regions,
    )

    def decorate(actor: type[A]) -> type[A]:
        if not is_actor(actor):
            raise ValueError("compute requires an actor class")
        if actor in _options:
            raise ValueError("compute cannot be repeated")
        _options[actor] = options
        return actor

    return decorate


def compute_contract(actor: type[ActorBase[Any, Any, Any, Any]]) -> dict[str, Any]:
    options = _options.get(actor)
    return (
        {}
        if options is None
        else {"sandbox": options.model_dump(mode="json", by_alias=True, exclude_none=True)}
    )
