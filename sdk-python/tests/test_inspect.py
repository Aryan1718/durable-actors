"""Inspect CLI lists actor inventory and the latest persisted state without a JS runtime."""

from __future__ import annotations

import json
import os
import subprocess
import sys
import threading
from contextlib import contextmanager
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlparse

from durable_actors.inspect import (
    format_instance,
    format_inventory,
    inspect_actors,
    parse_selector,
)

INVENTORY = {
    "connectionsComplete": True,
    "actors": [
        {
            "actorName": "Counter",
            "live": 1,
            "dormant": 1,
            "unknown": 0,
            "instances": [
                {
                    "actorId": "one",
                    "status": "live",
                    "connections": [{"id": "sock-a", "metadata": {"user": "alice"}}],
                },
                {
                    "actorId": "two",
                    "status": "dormant",
                    "connections": [],
                    "waiting": [{"id": "queued-1", "operation": "increment"}],
                },
            ],
        }
    ],
}

SNAPSHOT = {
    "snapshot": {
        "stateVersion": 3,
        "ownerEpoch": 1,
        "requestId": "req-3",
        "state": {"count": 3},
        "attribution": {
            "operation": "increment",
            "connectionId": None,
            "committedAtMs": 1234,
            "interleaved": False,
        },
    },
    "schema": None,
}


def test_parse_selector_requires_actor_name_and_id():
    assert parse_selector("Counter/one") == ("Counter", "one")
    assert parse_selector("Room.with.dots/state-demo") == ("Room.with.dots", "state-demo")
    for selector in ("Counter", "Counter/", "/one", "Counter/one/two"):
        try:
            parse_selector(selector)
        except ValueError as error:
            assert "Expected ActorName/actorId" in str(error)
        else:
            raise AssertionError(selector)


def test_format_inventory_lists_instances_and_an_empty_project():
    assert format_inventory({"actors": [], "connectionsComplete": True}) == "No actor instances."
    assert format_inventory(INVENTORY) == (
        "ACTOR    INSTANCE ID  STATUS\nCounter  one          live\nCounter  two          dormant"
    )


def test_format_instance_is_status_and_latest_state_json():
    instance = INVENTORY["actors"][0]["instances"][0]
    text = format_instance(("Counter", "one"), instance, {**SNAPSHOT, "schema": {"type": "object"}})
    assert text == ('Actor: Counter\nInstance ID: one\nStatus: live\nState:\n{\n  "count": 3\n}')
    assert "sock-a" not in text
    assert "stateVersion" not in text
    assert "requestId" not in text
    assert "ownerEpoch" not in text
    assert "schema" not in text
    assert format_instance(("Counter", "one"), instance, {"snapshot": None, "schema": None}) == (
        "Actor: Counter\n"
        "Instance ID: one\n"
        "Status: live\n"
        "State:\n"
        "No persisted snapshot for Counter/one."
    )


def test_inspect_actors_json_and_incomplete_connections():
    incomplete = {**INVENTORY, "connectionsComplete": False}
    text, warning = inspect_actors(incomplete, json_output=True)
    assert warning == "Warning: connection inventory is incomplete."
    assert json.loads(text) == incomplete


def test_inspect_actors_requires_an_inventory_instance():
    try:
        inspect_actors(INVENTORY, selector="Counter/missing", state=SNAPSHOT)
    except ValueError as error:
        assert str(error) == "No actor instance Counter/missing."
    else:
        raise AssertionError("missing instance")


def cli(project, environment, *arguments, check=True):
    result = subprocess.run(
        [sys.executable, "-m", "durable_actors", *arguments],
        cwd=project,
        env=environment,
        capture_output=True,
        text=True,
        timeout=30,
    )
    if check:
        assert result.returncode == 0, result.stdout + result.stderr
    return result


@contextmanager
def observe_server(routes, *, secret=None):
    requests = []

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            parsed = urlparse(self.path)
            requests.append(
                {
                    "path": parsed.path,
                    "query": parse_qs(parsed.query),
                    "authorization": self.headers.get("authorization"),
                }
            )
            body = routes.get((parsed.path, parsed.query))
            if body is None:
                body = routes.get(parsed.path)
            if body is None:
                self.send_error(404)
                return
            payload = json.dumps(body).encode()
            self.send_response(200)
            self.send_header("content-type", "application/json")
            self.send_header("content-length", str(len(payload)))
            self.end_headers()
            self.wfile.write(payload)

        def log_message(self, format, *args):
            return

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    origin = f"http://127.0.0.1:{server.server_address[1]}"
    try:
        yield origin, requests
    finally:
        server.shutdown()
        thread.join(timeout=5)


def inspect_env(origin, secret="inspect-key"):
    environment = {
        key: value for key, value in os.environ.items() if not key.startswith("DURABLE_ACTORS_")
    }
    environment["DURABLE_ACTORS_CONTROL_PLANE_URL"] = "http://unreachable.invalid"
    if secret is not None:
        environment["DURABLE_ACTORS_SECRET"] = secret
    return environment, origin


def test_inspect_help_does_not_need_a_server(tmp_path):
    environment = {
        key: value for key, value in os.environ.items() if not key.startswith("DURABLE_ACTORS_")
    }
    result = cli(tmp_path, environment, "inspect", "--help")
    assert "--json" in result.stdout
    assert "ActorName/actorId" in result.stdout
    assert "--control-plane-url" in result.stdout


def test_inspect_cli_lists_inventory_and_state(tmp_path):
    routes = {
        "/v1/projects/local/observe/actors": INVENTORY,
        ("/v1/projects/local/observe/state", "actorName=Counter&actorId=one"): SNAPSHOT,
    }
    with observe_server(routes) as (origin, requests):
        environment, _ = inspect_env(origin)
        listed = cli(tmp_path, environment, "inspect", "--control-plane-url", origin)
        assert "ACTOR    INSTANCE ID  STATUS" in listed.stdout
        assert "Counter  one          live" in listed.stdout
        assert "connections" not in listed.stdout
        payload = cli(tmp_path, environment, "inspect", "--json", "--control-plane-url", origin)
        assert json.loads(payload.stdout) == INVENTORY
        detail = cli(tmp_path, environment, "inspect", "Counter/one", "--control-plane-url", origin)
        assert "Status: live" in detail.stdout
        assert '"count": 3' in detail.stdout
        assert "stateVersion" not in detail.stdout
        assert "sock-a" not in detail.stdout
        assert [item["authorization"] for item in requests] == ["Bearer inspect-key"] * 4
        assert requests[3]["query"] == {"actorName": ["Counter"], "actorId": ["one"]}


def test_inspect_cli_empty_project_and_missing_instance(tmp_path):
    routes = {"/v1/projects/local/observe/actors": {"actors": [], "connectionsComplete": True}}
    with observe_server(routes) as (origin, _requests):
        environment, _ = inspect_env(origin, secret=None)
        environment["DURABLE_ACTORS_CONTROL_PLANE_URL"] = origin
        empty = cli(tmp_path, environment, "inspect")
        assert empty.stdout.strip() == "No actor instances."
        missing = cli(tmp_path, environment, "inspect", "Counter/one", check=False)
        assert missing.returncode == 1
        assert "No actor instance Counter/one." in missing.stderr
        invalid = cli(tmp_path, environment, "inspect", "Counter", check=False)
        assert invalid.returncode != 0
        assert "Expected ActorName/actorId" in invalid.stderr
