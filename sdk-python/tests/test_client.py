import errno
import os
import subprocess
import sys

import httpx
import pytest

from durable_actors.client import ActorInvocationError, Client


def test_gateway_calls_retry_only_explicit_non_execution():
    calls = []

    def handle(request):
        calls.append(request)
        outcome = (
            {"type": "not_executed", "reason": "host_unavailable"}
            if len(calls) == 2
            else {"type": "completed", "result": 3}
        )
        return httpx.Response(200, json={"outcome": outcome})

    with httpx.Client(transport=httpx.MockTransport(handle)) as http:
        with Client("http://control.test", project_id="test", api_key="key", http=http) as client:
            assert client.invoke("Counter", "one", "increment", []) == 3
            assert client.invoke("Counter", "one", "increment", []) == 3
    assert [request.url.host for request in calls] == ["control.test"] * 3
    assert all(request.headers["authorization"] == "Bearer key" for request in calls)
    assert calls[1].content == calls[2].content


def refused():
    error = httpx.ConnectError("[Errno 61] Connection refused")
    error.__cause__ = ConnectionRefusedError(errno.ECONNREFUSED, "Connection refused")
    return error


NEVER_SENT = pytest.mark.parametrize(
    "failure",
    [
        refused,
        lambda: httpx.ConnectError("[Errno 8] nodename nor servname provided"),
        lambda: httpx.ConnectTimeout("timed out"),
    ],
    ids=["refused", "dns", "connect_timeout"],
)
MAYBE_SENT = pytest.mark.parametrize(
    "failure",
    [
        lambda: httpx.ReadTimeout("timed out"),
        lambda: httpx.ReadError("response lost"),
        lambda: httpx.RemoteProtocolError("server disconnected"),
    ],
    ids=["read_timeout", "read_error", "disconnected"],
)


def invoke_through(failing_host, failure):
    calls = []

    def handle(request):
        calls.append(request.url.host)
        if request.url.host == failing_host:
            raise failure()
        outcome = {"type": "completed", "result": len(calls)}
        return httpx.Response(200, json={"outcome": outcome})

    with httpx.Client(transport=httpx.MockTransport(handle)) as http:
        with Client("http://control.test", project_id="test", http=http) as client:
            try:
                return client.invoke("Counter", "one", "increment", []), calls
            except ActorInvocationError as error:
                return error, calls


@NEVER_SENT
def test_control_plane_request_that_was_never_sent_is_unavailable(failure):
    error, calls = invoke_through("control.test", failure)
    assert isinstance(error, ActorInvocationError)
    assert error.code == "unavailable"
    assert str(error) == f"could not connect to http://control.test: {failure()}"
    assert calls == ["control.test"]


@MAYBE_SENT
def test_control_plane_request_that_may_have_been_sent_is_not_replayed(failure):
    error, calls = invoke_through("control.test", failure)
    assert isinstance(error, ActorInvocationError)
    assert error.code == "outcome_unknown"
    assert calls == ["control.test"]


def test_default_client_shares_its_pool_and_closes_it_at_process_exit(tmp_path):
    closed = tmp_path / "closed"
    result = subprocess.run(
        [
            sys.executable,
            "-c",
            """from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
import sys
import httpx
from durable_actors.client import default_client

class TrackedHttpClient(httpx.Client):
    def close(self):
        super().close()
        assert self.is_closed
        Path(sys.argv[1]).write_text("closed")

httpx.Client = TrackedHttpClient
with ThreadPoolExecutor(max_workers=8) as pool:
    clients = list(pool.map(lambda _: default_client(), range(32)))
assert all(client is clients[0] for client in clients)
""",
            str(closed),
        ],
        env={**os.environ, "DURABLE_ACTORS_CONTROL_PLANE_URL": "http://127.0.0.1:7100"},
        capture_output=True,
        text=True,
        timeout=15,
    )
    assert result.returncode == 0, result.stdout + result.stderr
    assert closed.read_text() == "closed"
