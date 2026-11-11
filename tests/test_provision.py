from __future__ import annotations

import base64
import json
import stat
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

import pytest

from mas_loadtest.cli import main
from mas_loadtest.console import ConsoleClient, ConsoleError
from mas_loadtest.engine import capacity
from mas_loadtest.provision import LoadParams, Session, cleanup, provision


class FakeMas(BaseHTTPRequestHandler):
    calls: list[tuple[str, str]] = []
    login_payload: dict[str, object] = {}
    cookie_only = False
    import_status = "completed"

    def log_message(self, *args: object) -> None:
        pass

    def _reply(self, payload: dict[str, object], status: int = 200) -> None:
        body = json.dumps(payload).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _handle(self, method: str) -> None:
        length = int(self.headers.get("Content-Length") or 0)
        raw = self.rfile.read(length) if length else b""
        FakeMas.calls.append((method, self.path))
        if self.path == "/console/api/login":
            FakeMas.login_payload = json.loads(raw)
            if base64.b64decode(FakeMas.login_payload["password"]) != b"secret":
                return self._reply({"code": "authentication_failed"}, 401)
            if FakeMas.cookie_only:
                body = b'{"result":"success"}'
                self.send_response(200)
                self.send_header("Set-Cookie", "access_token=console-token; Path=/")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)
                return
            return self._reply({"result": "success", "data": {"access_token": "console-token"}})
        if self.headers.get("Authorization") != "Bearer console-token" and "/console/" in self.path:
            return self._reply({}, 401)
        routes = {
            ("POST", "/console/api/apps/imports"): {"status": FakeMas.import_status, "app_id": "app-1"},
            ("POST", "/console/api/apps/app-1/api-keys"): {"id": "key-1", "token": "app-token"},
            ("POST", "/console/api/datasets"): {"id": "ds-1"},
            ("POST", "/console/api/datasets/api-keys"): {"id": "dkey-1", "token": "ds-token"},
        }
        if (method, self.path) in routes:
            return self._reply(routes[(method, self.path)])
        if self.path.startswith("/console/api/") and method in {"POST", "DELETE"}:
            return self._reply({"result": "success"})
        self._reply({}, 404)

    def do_POST(self) -> None:
        self._handle("POST")

    def do_DELETE(self) -> None:
        self._handle("DELETE")


@pytest.fixture
def mas():
    FakeMas.calls = []
    FakeMas.cookie_only = False
    FakeMas.import_status = "completed"
    server = ThreadingHTTPServer(("127.0.0.1", 0), FakeMas)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    yield f"http://127.0.0.1:{server.server_port}"
    server.shutdown()


def _params(scenario: str) -> LoadParams:
    return LoadParams(scenario=scenario, min_rate=60, max_rate=60, step=60, stage_seconds=60)


def test_login_sends_base64_password_and_rejects_wrong_one(mas: str) -> None:
    client = ConsoleClient(mas)
    client.login("a@b.c", "secret")
    assert client.access_token == "console-token"
    assert FakeMas.login_payload["password"] != "secret"
    with pytest.raises(ConsoleError) as error:
        ConsoleClient(mas).login("a@b.c", "wrong")
    assert error.value.http_status == 401 and "wrong" not in str(error.value)


def test_workflow_provision_writes_valid_private_config_and_cleanup_deletes_only_recorded(mas: str, tmp_path: Path) -> None:
    client = ConsoleClient(mas)
    client.login("a@b.c", "secret")
    session = Session(tmp_path / "s")
    config_path = provision(client, session, _params("workflow"), base_url=mas, run_id="abc123abc123", ca_file=None)
    config = capacity.load_config(config_path)
    assert config.mode == "workflow" and config.targets[0].fixtures[0].resource_id == "app-1"
    assert stat.S_IMODE((session.setup_dir / "secrets" / "app.token").stat().st_mode) == 0o600
    assert json.loads(session.resources_path.read_text())["apps"] == ["app-1"]
    assert ("POST", "/console/api/apps/app-1/workflows/publish") in FakeMas.calls
    assert cleanup(client, session) == []
    assert ("DELETE", "/console/api/apps/app-1") in FakeMas.calls
    assert ("DELETE", "/console/api/apps/app-1/api-keys/key-1") in FakeMas.calls
    assert json.loads(session.resources_path.read_text())["apps"] == []


def test_rag_write_provision_creates_dataset_and_key(mas: str, tmp_path: Path) -> None:
    client = ConsoleClient(mas)
    client.login("a@b.c", "secret")
    session = Session(tmp_path / "s")
    config = capacity.load_config(provision(client, session, _params("rag_write"), base_url=mas, run_id="abc123abc123", ca_file=None))
    assert config.targets[0].fixtures[0].resource_id == "ds-1"
    assert cleanup(client, session) == []
    assert ("DELETE", "/console/api/datasets/ds-1") in FakeMas.calls
    assert ("DELETE", "/console/api/datasets/api-keys/dkey-1") in FakeMas.calls


def test_cleanup_keeps_record_when_deletion_fails(mas: str, tmp_path: Path) -> None:
    client = ConsoleClient(mas)
    client.login("a@b.c", "secret")
    session = Session(tmp_path / "s")
    session.save_resources({"apps": ["missing"], "datasets": [], "app_keys": [], "dataset_keys": ["dkey-9"]})
    FakeMas.calls = []
    assert cleanup(client, session) == []  # server answers success for unknown ids on console DELETE
    bad = ConsoleClient("http://127.0.0.1:1")
    session.save_resources({"apps": ["x"], "datasets": [], "app_keys": [], "dataset_keys": []})
    assert cleanup(bad, session)
    assert json.loads(session.resources_path.read_text())["apps"] == ["x"]


def test_cli_plan_needs_no_server(capsys: pytest.CaptureFixture[str]) -> None:
    assert main(["plan", "--scenario", "rag_read", "--min-rate", "10", "--max-rate", "30", "--step", "10"]) == 0
    assert json.loads(capsys.readouterr().out)["stages_per_minute"] == [10, 20, 30]


def test_cli_run_requires_password(monkeypatch: pytest.MonkeyPatch, mas: str) -> None:
    monkeypatch.delenv("MAS_PASSWORD", raising=False)
    with pytest.raises(SystemExit):
        main(["run", "--url", mas, "--email", "a@b.c", "--scenario", "workflow", "--min-rate", "6", "--max-rate", "6", "--step", "6"])


def test_login_falls_back_to_access_token_cookie_and_accepts_import_warnings(mas: str, tmp_path: Path) -> None:
    FakeMas.cookie_only = True
    FakeMas.import_status = "completed-with-warnings"
    client = ConsoleClient(mas)
    client.login("a@b.c", "secret")
    assert client.access_token == "console-token"
    assert client.import_app("app: {}", "n") == "app-1"
