"""Invocation-scoped access to an actor's SQLite database."""

from __future__ import annotations

from collections.abc import Generator
from contextlib import contextmanager
from contextvars import ContextVar, Token
from functools import partial
from typing import Protocol
from weakref import ref

from .threads import run_in_thread

SqliteValue = str | int | float | bytes | None


class SqlDatabase(Protocol):
    def exec(self, sql: str, *bindings: SqliteValue) -> list[dict[str, SqliteValue]]: ...

    def get_alarm(self) -> int | None: ...

    def set_alarm(self, timestamp_ms: int) -> None: ...

    def delete_alarm(self) -> None: ...


class _Admission:
    def __init__(self, actor: object) -> None:
        self.actor = actor
        self.active = True


_admission: ContextVar[_Admission | None] = ContextVar("actor_database_admission", default=None)


class ActorDatabase:
    """One actor's SQLite handle. Statements run only during its current invocation."""

    def __init__(self, actor: object, storage: SqlDatabase) -> None:
        self._actor = ref(actor)
        self._storage = storage

    def exec(self, sql: str, *bindings: SqliteValue) -> list[dict[str, SqliteValue]]:
        """Execute one statement with positional bindings and return its rows."""
        _require_active_invocation(self._actor)
        return self._storage.exec(sql, *bindings)


class ActorAlarm:
    """One durable scheduled callback for a synchronous actor."""

    def __init__(self, actor: object, storage: SqlDatabase) -> None:
        self._actor = ref(actor)
        self._storage = storage

    def get(self) -> int | None:
        """Return the scheduled Unix timestamp in milliseconds, or None."""
        _require_active_invocation(self._actor)
        return self._storage.get_alarm()

    def set(self, timestamp_ms: int) -> None:
        """Replace the alarm with a Unix timestamp in milliseconds."""
        _require_active_invocation(self._actor)
        if type(timestamp_ms) is not int or abs(timestamp_ms) > 2**53 - 1:
            raise ValueError("alarm timestamp must be a safe integer")
        self._storage.set_alarm(timestamp_ms)

    def delete(self) -> None:
        """Clear the scheduled alarm."""
        _require_active_invocation(self._actor)
        self._storage.delete_alarm()


class AsyncActorDatabase:
    """Invocation-scoped SQLite statements executed off the event loop."""

    def __init__(self, database: ActorDatabase) -> None:
        self._database = database

    async def exec(self, sql: str, *bindings: SqliteValue) -> list[dict[str, SqliteValue]]:
        """Execute a statement, draining it before cancellation releases the invocation."""
        return await run_in_thread(partial(self._database.exec, sql, *bindings))


class AsyncActorAlarm:
    """One durable scheduled callback for an asynchronous actor."""

    def __init__(self, alarm: ActorAlarm) -> None:
        self._alarm = alarm

    async def get(self) -> int | None:
        """Return the scheduled Unix timestamp in milliseconds, or None."""
        return await run_in_thread(self._alarm.get)

    async def set(self, timestamp_ms: int) -> None:
        """Replace the alarm with a Unix timestamp in milliseconds."""
        await run_in_thread(partial(self._alarm.set, timestamp_ms))

    async def delete(self) -> None:
        """Clear the scheduled alarm."""
        await run_in_thread(self._alarm.delete)


class RuntimeActorContext:
    """Runtime capabilities bound to one actor instance."""

    database: ActorDatabase
    alarm: ActorAlarm

    def __init__(self, storage: SqlDatabase) -> None:
        self._storage = storage
        self._bound = False

    def bind(self, actor: object) -> None:
        if self._bound:
            raise RuntimeError("actor runtime context is already bound")
        self._bound = True
        self.database = ActorDatabase(actor, self._storage)
        self.alarm = ActorAlarm(actor, self._storage)


def _require_active_invocation(actor: ref[object]) -> None:
    admission = _admission.get()
    if admission is None or admission.actor is not actor() or not admission.active:
        raise RuntimeError("actor database is unavailable outside its invocation")


@contextmanager
def actor_database_invocation(actor: object | None) -> Generator[None]:
    if actor is None:
        yield
        return
    admission = _Admission(actor)
    token: Token[_Admission | None] = _admission.set(admission)
    try:
        yield
    finally:
        admission.active = False
        _admission.reset(token)
