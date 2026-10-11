"""Typed async WebSockets and emitted-state events."""

from __future__ import annotations

import asyncio
import json
from types import TracebackType
from typing import Generic, Protocol

from pydantic import TypeAdapter
from websockets.asyncio.client import connect
from websockets.exceptions import ConnectionClosedOK

from .client import SocketGrant
from .connection import Patch, Receive, Send, State, StateSnapshot, StateUpdate, decode_message
from .contract import encode


class AsyncSocketWire(Protocol):
    async def send(self, message: str) -> None: ...
    async def recv(self) -> str | bytes: ...
    async def close(self, code: int = 1000, reason: str = "") -> None: ...


class AsyncConnection(Generic[Send, Receive, State, Patch]):
    """Typed async WebSocket. Use async iteration to receive and async with for cleanup."""

    def __init__(
        self,
        wire: AsyncSocketWire,
        incoming: TypeAdapter[Send],
        outgoing: TypeAdapter[Receive],
        state: TypeAdapter[State],
        patch: TypeAdapter[Patch],
    ) -> None:
        self._wire = wire
        self._incoming = incoming
        self._outgoing = outgoing
        self._state = state
        self._patch = patch

    @classmethod
    async def open(
        cls,
        grant: SocketGrant,
        incoming: TypeAdapter[Send],
        outgoing: TypeAdapter[Receive],
        state: TypeAdapter[State],
        patch: TypeAdapter[Patch],
    ) -> AsyncConnection[Send, Receive, State, Patch]:
        wire = await connect(grant.websocket_url, max_size=None)
        return cls(wire, incoming, outgoing, state, patch)

    async def send(self, message: Send) -> None:
        data = encode(self._incoming, message)
        await self._wire.send(json.dumps(data, separators=(",", ":"), allow_nan=False))

    async def receive(
        self, timeout: float | None = None
    ) -> Receive | StateSnapshot[State] | StateUpdate[Patch]:
        """Await a validated message or state event; raise TimeoutError when the deadline expires."""
        async with asyncio.timeout(timeout):
            data = await self._wire.recv()
        return decode_message(data, self._outgoing, self._state, self._patch)

    def __aiter__(self) -> AsyncConnection[Send, Receive, State, Patch]:
        return self

    async def __anext__(self) -> Receive | StateSnapshot[State] | StateUpdate[Patch]:
        try:
            return await self.receive()
        except ConnectionClosedOK:
            raise StopAsyncIteration from None

    async def aclose(self, code: int = 1000, reason: str = "") -> None:
        await self._wire.close(code, reason)

    async def __aenter__(self) -> AsyncConnection[Send, Receive, State, Patch]:
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        await self.aclose()
