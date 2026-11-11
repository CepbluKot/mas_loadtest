"""Create disposable MAS resources for a load-test run and tear them down afterwards."""
from __future__ import annotations

from dataclasses import dataclass, field
import json
import os
from pathlib import Path
import time
from typing import Literal

from mas_loadtest.console import ConsoleClient, ConsoleError
from mas_loadtest.engine import transport as core

Scenario = Literal["workflow", "rag_read", "rag_write"]
FIXTURES = Path(__file__).parent / "fixtures"
SEED_QUERY = "WH-BENCH-204"
PROCESS_RULE = {"mode": "automatic"}


@dataclass
class LoadParams:
    scenario: Scenario
    min_rate: int
    max_rate: int
    step: int
    stage_seconds: int = 120
    max_inflight: int = 16
    max_total_requests: int = 5000
    request_timeout_seconds: float = 120
    indexing_timeout_seconds: float = 600
    max_p95_seconds: float | None = None
    workflow_dsl: Path | None = None
    inputs: dict[str, object] = field(default_factory=lambda: {"question": "ping"})
    query: str = SEED_QUERY
    upload_file: Path | None = None


@dataclass
class Session:
    """On-disk state of one run: resources to clean up, secrets and the generated config."""

    root: Path

    @property
    def setup_dir(self) -> Path:
        return self.root / "setup"

    @property
    def results_root(self) -> Path:
        return self.root / "results"

    @property
    def run_dir(self) -> Path:
        return self.results_root / "run"

    @property
    def config_path(self) -> Path:
        return self.setup_dir / "capacity.json"

    @property
    def resources_path(self) -> Path:
        return self.setup_dir / "resources.json"

    def write_private(self, path: Path, text: str) -> Path:
        path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
        descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
        with os.fdopen(descriptor, "w", encoding="utf-8") as stream:
            stream.write(text)
        return path

    def load_resources(self) -> dict[str, object]:
        if not self.resources_path.is_file():
            return {"apps": [], "datasets": [], "app_keys": [], "dataset_keys": []}
        return json.loads(self.resources_path.read_text(encoding="utf-8"))

    def save_resources(self, resources: dict[str, object]) -> None:
        self.write_private(self.resources_path, json.dumps(resources, indent=1))


def capacity_document(params: LoadParams, *, base_url: str, namespace: str, ca_file: Path | None,
                      results_root: Path, fixtures: list[dict[str, object]]) -> dict[str, object]:
    key = {"workflow": "workflows", "rag_read": "rag_reads", "rag_write": "rag_writes"}[params.scenario]
    target: dict[str, object] = {
        "namespace": namespace, "base_url": base_url, key: fixtures,
    }
    if ca_file is not None:
        target["ca_file"] = str(ca_file)
    document: dict[str, object] = {
        "version": 4, "mode": params.scenario, "results_root": str(results_root),
        "min_rate_per_minute": params.min_rate, "max_rate_per_minute": params.max_rate,
        "rate_step_per_minute": params.step, "max_total_requests": params.max_total_requests,
        "stage_seconds": params.stage_seconds, "max_inflight_per_namespace": params.max_inflight,
        "request_timeout_seconds": params.request_timeout_seconds,
        "indexing_timeout_seconds": params.indexing_timeout_seconds, "targets": [target],
    }
    if params.max_p95_seconds is not None:
        document["max_p95_seconds"] = params.max_p95_seconds
    return document


def _wait_indexed(transport: core.HttpMasTransport, target: core.TargetConfig, token_file: Path,
                  dataset_id: str, upload: dict[str, object], timeout_seconds: float) -> None:
    document = upload.get("document")
    document_id = document.get("id") if isinstance(document, dict) else None
    batch = upload.get("batch")
    if not isinstance(document_id, str) or not isinstance(batch, str):
        raise ConsoleError("seed upload response has no document id")
    deadline = time.monotonic() + timeout_seconds
    while time.monotonic() < deadline:
        status = transport.get_json(target, token_file, f"/v1/datasets/{dataset_id}/documents/{batch}/indexing-status")
        state = core.indexing_state(status, document_id)
        if state == "completed":
            return
        if state == "error":
            raise ConsoleError("seed document indexing failed")
        time.sleep(2)
    raise ConsoleError("seed document indexing timed out")


def provision(client: ConsoleClient, session: Session, params: LoadParams, *, base_url: str,
              run_id: str, ca_file: Path | None) -> Path:
    """Create resources for params.scenario and return the path of the generated capacity config."""
    resources = session.load_resources()
    name = f"mas-loadtest-{run_id}"
    fixtures: list[dict[str, object]] = []
    if params.scenario == "workflow":
        dsl = params.workflow_dsl or FIXTURES / "workflow_echo.yml"
        app_id = client.import_app(dsl.read_text(encoding="utf-8"), name)
        resources["apps"].append(app_id)
        session.save_resources(resources)
        client.publish_workflow(app_id)
        client.enable_api(app_id)
        token, key_id = client.create_app_api_key(app_id)
        resources["app_keys"].append([app_id, key_id])
        session.save_resources(resources)
        token_file = session.write_private(session.setup_dir / "secrets" / "app.token", token + "\n")
        fixtures.append({"label": "workflow", "app_id": app_id, "api_token_file": str(token_file), "inputs": params.inputs})
    else:
        dataset_id = client.create_dataset(name)
        resources["datasets"].append(dataset_id)
        session.save_resources(resources)
        token, key_id = client.create_dataset_api_key()
        resources["dataset_keys"].append(key_id)
        session.save_resources(resources)
        token_file = session.write_private(session.setup_dir / "secrets" / "dataset.token", token + "\n")
        source = params.upload_file or FIXTURES / "synthetic_warehouse_ops.txt"
        if params.scenario == "rag_read":
            transport = core.HttpMasTransport(params.request_timeout_seconds)
            target = core.TargetConfig("setup", base_url, ca_file, None)
            upload = transport.post_multipart(
                target, token_file, f"/v1/datasets/{dataset_id}/document/create-by-file", source.read_bytes(), source.name,
                {"data": json.dumps({"indexing_technique": "economy", "process_rule": PROCESS_RULE})},
            )
            _wait_indexed(transport, target, token_file, dataset_id, upload, params.indexing_timeout_seconds)
            fixtures.append({"label": "knowledge", "dataset_id": dataset_id, "api_token_file": str(token_file),
                             "query": params.query, "top_k": 5, "min_hits": 1})
        else:
            fixtures.append({"label": "knowledge-upload", "dataset_id": dataset_id, "api_token_file": str(token_file),
                             "file_path": str(source), "indexing_technique": "economy", "process_rule": PROCESS_RULE})
    document = capacity_document(
        params, base_url=base_url, namespace=name, ca_file=ca_file, results_root=session.results_root,
        fixtures=fixtures,
    )
    return session.write_private(session.config_path, json.dumps(document, indent=1))


def cleanup(client: ConsoleClient, session: Session) -> list[str]:
    """Delete only resources recorded in resources.json; return failure descriptions."""
    resources = session.load_resources()
    failures: list[str] = []

    def attempt(label: str, action) -> None:
        try:
            action()
        except ConsoleError as exc:
            if exc.http_status != 404:
                failures.append(f"{label}: {exc}")

    for app_id, key_id in resources["app_keys"]:
        attempt(f"app key {key_id}", lambda a=app_id, k=key_id: client.delete_app_api_key(a, k))
    for key_id in resources["dataset_keys"]:
        attempt(f"dataset key {key_id}", lambda k=key_id: client.delete_dataset_api_key(k))
    for app_id in resources["apps"]:
        attempt(f"app {app_id}", lambda a=app_id: client.delete_app(a))
    for dataset_id in resources["datasets"]:
        attempt(f"dataset {dataset_id}", lambda d=dataset_id: client.delete_dataset(d))
    if not failures:
        session.save_resources({"apps": [], "datasets": [], "app_keys": [], "dataset_keys": []})
    return failures
