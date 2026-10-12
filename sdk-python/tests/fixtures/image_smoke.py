"""Exercise the production executor without installing development tools."""

import json
import os
import socket
import sqlite3
import subprocess
import sys
import tempfile
from pathlib import Path

from durable_actors.build import build_actor


def main() -> None:
    with tempfile.TemporaryDirectory(dir="/tmp") as directory:
        project = Path(directory)
        (project / "actors.py").write_text("""from durable_actors import Actor, persisted
class Counter(Actor):
    count: int = persisted(0)
    def increment(self) -> int:
        self.count += 1
        return self.count
""")
        # Package a fixture artifact; production executors receive compiled artifacts.
        build_actor(project, "actors.py", project / "build")
        database_path = project / "actor.sqlite"
        with sqlite3.connect(database_path) as database:
            database.execute("PRAGMA journal_mode=WAL")
            database.execute(
                "CREATE TABLE __terse_fields(name TEXT PRIMARY KEY, value TEXT NOT NULL CHECK(json_valid(value)))"
            )
        txid = 1
        for expected in (1, 2):
            with socket.socket(socket.AF_UNIX) as listener:
                address = str(project / f"executor-{expected}.sock")
                listener.bind(address)
                listener.listen(1)
                listener.settimeout(10)
                executor = subprocess.Popen(
                    [sys.executable, "-m", "durable_actors.host", "--generic"],
                    env={**os.environ, "DURABLE_ACTORS_EXECUTOR_SOCKET": address},
                )
                try:
                    connection, _ = listener.accept()
                    with connection, connection.makefile("rb") as messages:
                        connection.settimeout(10)

                        def send(message):
                            connection.sendall((json.dumps(message) + "\n").encode())

                        def receive():
                            nonlocal txid
                            while True:
                                message = json.loads(messages.readline())
                                if message["type"] != "persist_sqlite":
                                    return message
                                assert message["mode"] == "atomic_snapshot"
                                txid += 1
                                send(
                                    {
                                        "type": "sqlite_persisted",
                                        "message_id": message["message_id"],
                                        "commit_id": message["commit_id"],
                                        "txid": txid,
                                    }
                                )

                        assert receive() == {"type": "warm", "protocol": 26}
                        send(
                            {
                                "type": "load",
                                "entrypoint": str(project / "build/actors.pyz"),
                                "environment": {},
                            }
                        )
                        assert receive() == {
                            "type": "attach",
                            "protocol": 26,
                            "actor_names": ["Counter"],
                        }
                        send({"type": "attached", "protocol": 26})
                        actor = {"project_id": "local", "actor_name": "Counter", "actor_id": "one"}
                        send(
                            {
                                "type": "command",
                                "message_id": expected,
                                "command": {
                                    "type": "invoke",
                                    "request_id": f"request-{expected}",
                                    "actor": actor,
                                    "method": "increment",
                                    "args": [],
                                    "sqlite": {"txid": txid, "path": str(database_path)},
                                },
                            }
                        )
                        reply = receive()["reply"]
                        assert reply["type"] == "invoked", reply
                        assert reply["result"] == expected
                        send(
                            {
                                "type": "command",
                                "message_id": 10 + expected,
                                "command": {"type": "evict", "actor": actor},
                            }
                        )
                        assert receive()["reply"]["type"] == "evicted"
                finally:
                    executor.terminate()
                    try:
                        executor.wait(timeout=10)
                    except subprocess.TimeoutExpired:
                        executor.kill()
                        executor.wait()
                        raise
    print("Python image: generic warmup, invocation, and SQLite recovery after restart passed")


main()
