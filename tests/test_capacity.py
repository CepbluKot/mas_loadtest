from __future__ import annotations

import json
from dataclasses import replace
from pathlib import Path
import stat
from threading import Lock
import time

import pytest

from mas_loadtest.engine import capacity
from mas_loadtest.engine import transport as benchmark


def _private(path: Path, text: str) -> str:
    path.write_text(text, encoding="utf-8")
    path.chmod(0o600)
    return str(path)


def _config(tmp_path: Path, mode: str) -> Path:
    token = _private(tmp_path / "service.token", "service-secret")
    console = _private(tmp_path / "console.token", "console-secret")
    source = _private(tmp_path / "fixture.txt", "fixture text")
    raw = {
        "version": 4,
        "mode": mode,
        "results_root": str(tmp_path / "results"),
        "min_rate_per_minute": 10,
        "max_rate_per_minute": 80,
        "rate_step_per_minute": 10,
        "max_total_requests": 10000,
        "stage_seconds": 60,
        "max_inflight_per_namespace": 2,
        "request_timeout_seconds": 5,
        "indexing_timeout_seconds": 5,
        "max_p95_seconds": 10,
        "targets": [
            {
                "namespace": "mas-a",
                "base_url": "https://mas-a.example.test",
                "console_token_file": console,
                "workflows": [
                    {"label": "job", "app_id": "app-a", "api_token_file": token, "inputs": {"value": 7}}
                ],
                "rag_reads": [
                    {"label": "read", "dataset_id": "dataset-a", "api_token_file": token,
                     "query": "stable query", "min_hits": 1}
                ],
                "rag_writes": [
                    {"label": "write", "dataset_id": "dataset-a", "api_token_file": token,
                     "file_path": source,
                     "indexing_technique": "economy", "process_rule": {"mode": "automatic"}}
                ],
            }
        ],
    }
    return Path(_private(tmp_path / "config.json", json.dumps(raw)))


class FakeTransport:
    def __init__(self) -> None:
        self.posts: list[tuple[str, dict[str, object]]] = []
        self.uploads: list[tuple[str, str]] = []
        self.gets: list[str] = []

    def get_json(self, target, token_file, path):
        self.gets.append(path)
        if path.endswith("/workflows/publish"):
            return {"graph": {"nodes": [{"data": {"type": "start"}}, {"data": {"type": "end"}}]}}
        if path.endswith("/api-keys"):
            return {"data": [{"token": "service-secret"}]}
        if path.endswith("/indexing-status"):
            return {"data": [{"id": "doc-1", "indexing_status": "completed"}]}
        return {"id": "dataset-a", "indexing_technique": "economy"}

    def post_json(self, target, token_file, path, payload):
        self.posts.append((path, payload))
        if path == "/v1/workflows/run":
            return {"data": {"id": "run-1", "status": "succeeded"}}
        return {"records": [{"content": "hit"}]}

    def post_multipart(self, target, token_file, path, file_content, filename, form_data):
        self.uploads.append((path, filename))
        return {"document": {"id": "doc-1"}, "batch": "batch-1"}

    def delete_document(self, target, token_file, path):
        raise AssertionError("capacity runner does not delete data during stages")


@pytest.mark.parametrize("mode", ["workflow", "rag_read", "rag_write"])
def test_api_only_preflight_and_one_completed_operation(tmp_path: Path, mode: str) -> None:
    config = capacity.load_config(_config(tmp_path, mode))
    fake = FakeTransport()

    capacity.preflight(config, fake)
    target = config.targets[0]
    result = capacity.execute_one(target, target.fixtures[0], fake, "runid", 1, 5)

    assert result.success
    assert result.accepted
    assert result.sequence == 1
    assert result.started_at_utc is not None
    assert result.finished_at_utc is not None
    if mode == "workflow":
        assert result.workflow_status == "succeeded"
        assert fake.posts[0][0] == "/v1/workflows/run"
        assert fake.posts[0][1]["inputs"] == {"value": 7}
        assert any(path.endswith("/api-keys") for path in fake.gets)
    elif mode == "rag_read":
        assert result.retrieval_hits == 1
        assert fake.posts[0][0] == "/v1/datasets/dataset-a/retrieve"
        assert fake.posts[0][1]["retrieval_model"]["search_method"] == "keyword_search"
    else:
        assert result.indexing_status == "completed"
        assert result.upload_seconds is not None
        assert result.indexing_seconds is not None
        assert result.indexing_polls == 1
        assert fake.uploads[0][0] == "/v1/datasets/dataset-a/document/create_by_file"
        assert any(path.endswith("/indexing-status") for path in fake.gets)


def test_workflow_preflight_rejects_rag_node(tmp_path: Path) -> None:
    config = capacity.load_config(_config(tmp_path, "workflow"))

    class RagWorkflow(FakeTransport):
        def get_json(self, target, token_file, path):
            if path.endswith("/workflows/publish"):
                return {"graph": {"nodes": [{"data": {"type": "knowledge-retrieval"}}]}}
            return super().get_json(target, token_file, path)

    with pytest.raises(benchmark.PreflightError, match="Knowledge Retrieval"):
        capacity.preflight(config, RagWorkflow())


def test_workflow_preflight_works_with_service_api_only(tmp_path: Path) -> None:
    config_path = _config(tmp_path, "workflow")
    raw = json.loads(config_path.read_text(encoding="utf-8"))
    del raw["targets"][0]["console_token_file"]
    config_path.write_text(json.dumps(raw), encoding="utf-8")
    config_path.chmod(0o600)
    config = capacity.load_config(config_path)

    class ServiceApiOnly(FakeTransport):
        def get_json(self, target, token_file, path):
            self.gets.append(path)
            if path == "/v1/info":
                assert token_file == config.targets[0].fixtures[0].token_file
                return {"mode": "workflow"}
            raise AssertionError(f"unexpected preflight request: {path}")

    transport = ServiceApiOnly()
    capacity.preflight(config, transport)

    assert transport.gets == ["/v1/info"]


def test_chatflow_job_uses_chat_messages_api(tmp_path: Path) -> None:
    config_path = _config(tmp_path, "workflow")
    raw = json.loads(config_path.read_text(encoding="utf-8"))
    target = raw["targets"][0]
    target.pop("console_token_file")
    target["workflows"][0].update({"app_type": "chatflow", "query": "Суммируй заявку"})
    config_path.write_text(json.dumps(raw), encoding="utf-8")
    config_path.chmod(0o600)
    config = capacity.load_config(config_path)

    class ChatflowTransport(FakeTransport):
        def get_json(self, target, token_file, path):
            self.gets.append(path)
            if path == "/v1/info":
                return {"mode": "advanced-chat"}
            raise AssertionError(f"unexpected preflight request: {path}")

        def post_json(self, target, token_file, path, payload):
            self.posts.append((path, payload))
            return {
                "event": "message", "message_id": "message-1", "answer": "Краткое резюме",
                "metadata": {"usage": {"total_tokens": 18, "latency": 0.42}},
            }

    transport = ChatflowTransport()
    capacity.preflight(config, transport)
    result = capacity.execute_one(config.targets[0], config.targets[0].fixtures[0], transport, "runid", 1, 5)

    assert result.success
    assert transport.gets == ["/v1/info"]
    assert transport.posts[0][0] == "/v1/chat-messages"
    assert transport.posts[0][1]["query"] == "Суммируй заявку"
    assert transport.posts[0][1]["response_mode"] == "blocking"
    assert transport.posts[0][1]["auto_generate_name"] is False
    assert result.llm_total_tokens == 18
    assert result.llm_latency_seconds == 0.42


def test_stage_summary_uses_completed_work_not_only_accepted_http(tmp_path: Path) -> None:
    config = capacity.load_config(_config(tmp_path, "rag_write"))
    deadline = 100.0
    results = [
        capacity.Result("mas-a", "write", 90, 95, True, True, None),
        capacity.Result("mas-a", "write", 92, 102, True, True, None),
        capacity.Result("mas-a", "write", 94, 99, False, True, "IndexingError"),
    ]

    stage = capacity.stage_summary(config, 3, 3, 3, deadline, results)

    assert stage["completed"] == 2
    assert stage["completed_in_window"] == 1
    assert stage["verdict"] == "FAIL"
    assert stage["resources"]["mas-a/write"]["submitted"] == 3
    assert stage["p50_seconds"] == 5
    assert stage["p99_seconds"] == 10
    assert stage["resources"]["mas-a/write"]["p99_seconds"] == 10
    assert stage["namespaces"]["mas-a"]["completed_in_window"] == 1


def test_late_but_steady_completions_pass_and_growing_backlog_fails(tmp_path: Path) -> None:
    config_path = _config(tmp_path, "workflow")
    raw = json.loads(config_path.read_text())
    raw.pop("max_p95_seconds")
    _private(config_path, json.dumps(raw))
    config = capacity.load_config(config_path)
    steady = [capacity.Result("mas-a", "job", float(i), float(i + 10), True, True, None) for i in range(1, 7)]
    growing = [capacity.Result("mas-a", "job", float(i), float(2 * i + 9), True, True, None) for i in range(1, 7)]

    passing = capacity.stage_summary(config, 6, 6, 6, 10, steady)
    failing = capacity.stage_summary(config, 6, 6, 6, 10, growing)

    assert passing["completion_ratio"] == 0
    assert passing["throughput_ratio"] == 1
    assert passing["observed_completion_rate_per_minute"] == 60
    assert passing["verdict"] == "PASS"
    assert failing["success_ratio"] == 1
    assert failing["throughput_ratio"] == 0.5
    assert failing["observed_completion_rate_per_minute"] == 30
    assert failing["verdict"] == "FAIL"
    assert "throughput ratio below target" in failing["reasons"]


def test_old_completion_gate_is_rejected(tmp_path: Path) -> None:
    config_path = _config(tmp_path, "workflow")
    raw = json.loads(config_path.read_text())
    raw["min_completion_ratio"] = 0.9
    _private(config_path, json.dumps(raw))

    with pytest.raises(benchmark.ConfigError, match="min_completion_ratio"):
        capacity.load_config(config_path)


def test_global_inflight_limit_is_rejected(tmp_path: Path) -> None:
    config_path = _config(tmp_path, "workflow")
    raw = json.loads(config_path.read_text())
    raw["max_inflight"] = 32
    _private(config_path, json.dumps(raw))

    with pytest.raises(benchmark.ConfigError, match="max_inflight"):
        capacity.load_config(config_path)


def test_rag_write_accepts_file_without_benchmark_size_limit(tmp_path: Path) -> None:
    config_path = _config(tmp_path, "rag_write")
    large_file = tmp_path / "fixture.txt"
    _private(large_file, "x" * 2048)

    config = capacity.load_config(config_path)

    assert config.targets[0].fixtures[0].file_bytes == b"x" * 2048


def test_obsolete_file_size_option_is_rejected(tmp_path: Path) -> None:
    config_path = _config(tmp_path, "rag_write")
    raw = json.loads(config_path.read_text())
    raw["targets"][0]["rag_writes"][0]["max_file_bytes"] = 1024
    _private(config_path, json.dumps(raw))

    with pytest.raises(benchmark.ConfigError, match="max_file_bytes"):
        capacity.load_config(config_path)


def test_single_slow_tail_is_recorded_without_falsely_signaling_backlog(tmp_path: Path) -> None:
    config_path = _config(tmp_path, "workflow")
    raw = json.loads(config_path.read_text())
    raw.pop("max_p95_seconds")
    _private(config_path, json.dumps(raw))
    config = capacity.load_config(config_path)
    results = [
        capacity.Result("mas-a", "job", float(i), float(i + 10), True, True, None)
        for i in range(1, 21)
    ]
    results[-1] = replace(results[-1], finished=130)

    stage = capacity.stage_summary(config, 20, 20, 20, 30, results)

    assert stage["verdict"] == "PASS"
    assert stage["throughput_ratio"] == 1
    assert stage["p99_seconds"] == 110


def test_one_failed_resource_fails_stage_even_when_aggregate_ratio_passes(tmp_path: Path) -> None:
    original = capacity.load_config(_config(tmp_path, "rag_read"))
    first = original.targets[0]
    second_api = replace(first.transport_target, namespace="mas-b", base_url="https://mas-b.example.test")
    second = replace(first, namespace="mas-b", transport_target=second_api)
    config = replace(original, targets=(first, second))
    results = [capacity.Result("mas-a", "read", 1, 2, True, True, None) for _ in range(100)]
    results.append(capacity.Result("mas-b", "read", 1, 2, False, True, "HttpError"))

    stage = capacity.stage_summary(config, 101, 101, 101, 100, results)

    assert stage["success_ratio"] > 0.99
    assert stage["verdict"] == "FAIL"
    assert "mas-b/read" in stage["reasons"][-1]


def test_generator_limit_is_inconclusive_not_mas_failure(tmp_path: Path) -> None:
    config = capacity.load_config(_config(tmp_path, "workflow"))
    stage = capacity.stage_summary(config, 40, 40, 2, time.monotonic() + 60, [])

    assert stage["verdict"] == "INCONCLUSIVE_GENERATOR"


def test_latency_limit_is_optional_and_effective_criteria_are_visible(tmp_path: Path) -> None:
    config_path = _config(tmp_path, "workflow")
    raw = json.loads(config_path.read_text())
    raw.pop("max_p95_seconds")
    _private(config_path, json.dumps(raw))
    config = capacity.load_config(config_path)
    plan = capacity.plan_document(config)
    result = capacity.Result("mas-a", "job", 1, 21, True, True, None)

    assert config.max_p95_seconds is None
    assert plan["load_distribution"] == "equal_per_target"
    assert plan["maximum_concurrent_jobs"] == 2
    assert plan["criteria"] == {
        "max_p95_seconds": None,
        "min_success_ratio": 0.99,
        "min_throughput_ratio": 0.9,
        "min_arrival_fidelity": 0.95,
    }
    assert capacity.stage_summary(config, 1, 1, 1, 30, [result])["verdict"] == "PASS"

    limited = replace(config, max_p95_seconds=10)
    assert capacity.stage_summary(limited, 1, 1, 1, 30, [result])["verdict"] == "FAIL"


def test_uncertain_api_post_does_not_define_capacity_failure(tmp_path: Path) -> None:
    config = capacity.load_config(_config(tmp_path, "workflow"))

    class TimeoutTransport(FakeTransport):
        def post_json(self, target, token_file, path, payload):
            raise benchmark.MasRequestError("RequestTimeout", uncertain=True)

    target = config.targets[0]
    result = capacity.execute_one(target, target.fixtures[0], TimeoutTransport(), "runid", 1, 5)
    stage = capacity.stage_summary(config, 1, 1, 1, result.finished + 1, [result])

    assert result.error == "UncertainPostOutcome"
    assert stage["verdict"] == "INCONCLUSIVE_UNCERTAIN"


def test_unknown_indexing_status_stops_capacity_search(tmp_path: Path) -> None:
    config = capacity.load_config(_config(tmp_path, "rag_write"))

    class MissingStatus(FakeTransport):
        def get_json(self, target, token_file, path):
            if path.endswith("/indexing-status"):
                raise benchmark.PreflightError("temporary status failure")
            return super().get_json(target, token_file, path)

    target = config.targets[0]
    result = capacity.execute_one(target, target.fixtures[0], MissingStatus(), "runid", 1, 5)
    stage = capacity.stage_summary(config, 1, 1, 1, result.finished + 1, [result])

    assert result.error == "UncertainIndexingOutcome"
    assert stage["verdict"] == "INCONCLUSIVE_UNCERTAIN"


def test_stage_distributes_overlapping_calls_across_targets(tmp_path: Path, monkeypatch) -> None:
    original = capacity.load_config(_config(tmp_path, "workflow"))
    first = original.targets[0]
    second_api = replace(first.transport_target, namespace="mas-b", base_url="https://mas-b.example.test")
    second = replace(first, namespace="mas-b", transport_target=second_api)
    config = replace(original, targets=(first, second), stage_seconds=2, max_inflight_per_namespace=1)
    active = 0
    peak = 0
    active_by_namespace = {"mas-a": 0, "mas-b": 0}
    peak_by_namespace = {"mas-a": 0, "mas-b": 0}
    seen: set[str] = set()
    lock = Lock()

    def slow_job(target, fixture, transport, run_id, sequence, indexing_timeout):
        nonlocal active, peak
        started = time.monotonic()
        with lock:
            active += 1
            peak = max(peak, active)
            active_by_namespace[target.namespace] += 1
            peak_by_namespace[target.namespace] = max(
                peak_by_namespace[target.namespace], active_by_namespace[target.namespace]
            )
            seen.add(target.namespace)
        time.sleep(0.7)
        with lock:
            active -= 1
            active_by_namespace[target.namespace] -= 1
        return capacity.Result(target.namespace, fixture.label, started, time.monotonic(), True, True, None)

    monkeypatch.setattr(capacity, "execute_one", slow_job)
    stage, results = capacity.measure_stage(config, FakeTransport(), 120, "runid", 0)

    assert len(results) == 4
    assert seen == {"mas-a", "mas-b"}
    assert peak == 2
    assert peak_by_namespace == {"mas-a": 1, "mas-b": 1}
    assert capacity.plan_document(config)["maximum_concurrent_jobs"] == 2
    assert stage["resources"]["mas-a/job"]["submitted"] == 2
    assert stage["resources"]["mas-b/job"]["submitted"] == 2


def test_stage_splits_global_rate_equally_between_targets_with_unequal_resource_counts(tmp_path: Path) -> None:
    original = capacity.load_config(_config(tmp_path, "workflow"))
    first = original.targets[0]
    second_api = replace(first.transport_target, namespace="mas-b", base_url="https://mas-b.example.test")
    second = replace(first, namespace="mas-b", transport_target=second_api)
    extra = replace(first.fixtures[0], label="job-two", resource_id="app-b")
    first = replace(first, fixtures=(first.fixtures[0], extra))
    config = replace(original, targets=(first, second), stage_seconds=0.6)

    first_stage, _ = capacity.measure_stage(config, FakeTransport(), 600, "runid", 0)
    second_stage, _ = capacity.measure_stage(config, FakeTransport(), 600, "runid", 6)

    for stage in (first_stage, second_stage):
        assert stage["namespaces"]["mas-a"]["submitted"] == 3
        assert stage["namespaces"]["mas-b"]["submitted"] == 3
    assert first_stage["resources"]["mas-a/job"]["submitted"] == 2
    assert first_stage["resources"]["mas-a/job-two"]["submitted"] == 1
    assert second_stage["resources"]["mas-a/job"]["submitted"] == 1
    assert second_stage["resources"]["mas-a/job-two"]["submitted"] == 2


def test_first_rate_covers_largest_target_resource_list(tmp_path: Path) -> None:
    config_path = _config(tmp_path, "workflow")
    raw = json.loads(config_path.read_text())
    first = raw["targets"][0]
    second = dict(first)
    second["namespace"] = "mas-b"
    second["base_url"] = "https://mas-b.example.test"
    second["workflows"] = [
        {**first["workflows"][0], "label": f"job-{index}", "app_id": f"app-{index}"}
        for index in range(3)
    ]
    raw["targets"].append(second)
    raw["min_rate_per_minute"] = 4
    _private(config_path, json.dumps(raw))

    with pytest.raises(benchmark.ConfigError, match="first rate"):
        capacity.load_config(config_path)


def test_full_pool_queues_operations_without_losing_them(tmp_path: Path, monkeypatch) -> None:
    original = capacity.load_config(_config(tmp_path, "workflow"))
    config = replace(original, stage_seconds=0.6, max_inflight_per_namespace=1)
    active = 0
    peak = 0
    lock = Lock()

    def slow_job(target, fixture, transport, run_id, sequence, indexing_timeout):
        nonlocal active, peak
        started = time.monotonic()
        with lock:
            active += 1
            peak = max(peak, active)
        time.sleep(0.35)
        with lock:
            active -= 1
        return capacity.Result(target.namespace, fixture.label, started, time.monotonic(), True, True, None)

    monkeypatch.setattr(capacity, "execute_one", slow_job)
    stage, results = capacity.measure_stage(config, FakeTransport(), 300, "runid", 0)

    assert len(results) == stage["planned"] == stage["submitted"] == 3
    assert peak == 1
    assert stage["peak_pending"] >= 1
    assert stage["started_in_window"] < stage["planned"]
    assert stage["verdict"] == "INCONCLUSIVE_GENERATOR"
    assert any(result.queue_wait_seconds and result.queue_wait_seconds > 0.1 for result in results)


def test_capacity_run_stops_at_first_failed_step(tmp_path: Path, monkeypatch) -> None:
    config = capacity.load_config(_config(tmp_path, "workflow"))
    attempted: list[int] = []

    def fake_stage(config, transport, rate, run_id, sequence_offset, event_sink=None):
        attempted.append(rate)
        return {"verdict": "PASS" if rate <= 27 else "FAIL"}, []

    monkeypatch.setattr(capacity, "measure_stage", fake_stage)
    report = capacity.run(config, config.results_root / "run-one", FakeTransport())

    assert report["capacity_status"] == "BRACKETED"
    assert report["last_passing_rate_per_minute"] == 20
    assert report["first_failing_rate_per_minute"] == 30
    assert attempted == [10, 20, 30]
    assert stat.S_IMODE(Path(report["run_dir"]).stat().st_mode) == 0o700
    for name in ("metadata.json", "context.json", "events.jsonl", "stages.jsonl", "tasks.jsonl", "report.json"):
        assert stat.S_IMODE((Path(report["run_dir"]) / name).stat().st_mode) == 0o600
    stored = (Path(report["run_dir"]) / "report.json").read_text()
    assert "service-secret" not in stored
    assert "fixture text" not in stored
    assert capacity.read_status(Path(report["run_dir"]))["capacity_status"] == "BRACKETED"


def test_configured_minimum_maximum_and_step_control_rates(tmp_path: Path, monkeypatch) -> None:
    config_path = _config(tmp_path, "workflow")
    raw = json.loads(config_path.read_text())
    raw["max_rate_per_minute"] = 45
    raw["rate_step_per_minute"] = 15
    _private(config_path, json.dumps(raw))
    config = capacity.load_config(config_path)
    attempted: list[int] = []

    def fake_stage(config, transport, rate, run_id, sequence_offset, event_sink=None):
        attempted.append(rate)
        return {"verdict": "PASS"}, []

    monkeypatch.setattr(capacity, "measure_stage", fake_stage)
    report = capacity.run(config, config.results_root / "factor", FakeTransport())

    assert attempted == [10, 25, 40, 45]
    assert report["criteria"]["rate_step_per_minute"] == 15
    assert report["capacity_status"] == "LOWER_BOUND"


@pytest.mark.parametrize("step", [0, -1, True, 1.5])
def test_rate_step_must_be_a_positive_integer(tmp_path: Path, step: object) -> None:
    config_path = _config(tmp_path, "workflow")
    raw = json.loads(config_path.read_text())
    raw["rate_step_per_minute"] = step
    _private(config_path, json.dumps(raw))

    with pytest.raises(benchmark.ConfigError, match="rate_step_per_minute"):
        capacity.load_config(config_path)


def test_report_preserves_safe_task_evidence_for_llm(tmp_path: Path, monkeypatch) -> None:
    config = capacity.load_config(_config(tmp_path, "rag_write"))
    task = capacity.Result(
        "mas-a", "write", 10, 13, True, True, None,
        sequence=4,
        scheduled_at_utc="2026-10-08T09:59:59+00:00",
        queue_wait_seconds=1.0,
        started_at_utc="2026-10-08T10:00:00+00:00",
        finished_at_utc="2026-10-08T10:00:03+00:00",
        indexing_status="completed",
        upload_seconds=0.2,
        indexing_seconds=2.8,
        indexing_polls=3,
    )
    monkeypatch.setattr(
        capacity, "measure_stage",
        lambda config, transport, rate, run_id, sequence_offset, event_sink=None: ({"verdict": "PASS"}, [task]),
    )

    report = capacity.run(config, config.results_root / "llm-evidence", FakeTransport())

    run_dir = Path(report["run_dir"])
    record = json.loads((run_dir / "tasks.jsonl").read_text().splitlines()[0])
    assert record["sequence"] == 4
    assert record["scheduled_at_utc"] == "2026-10-08T09:59:59+00:00"
    assert record["queue_wait_seconds"] == 1.0
    assert record["started_at_utc"] == "2026-10-08T10:00:00+00:00"
    assert record["indexing_seconds"] == 2.8
    assert record["indexing_polls"] == 3
    assert report["criteria"]["max_p95_seconds"] == 10
    assert report["targets"] == {"mas-a": ["write"]}
    for path in run_dir.iterdir():
        contents = path.read_text()
        assert "service-secret" not in contents
        assert "fixture text" not in contents
        if path.name in {"metadata.json", "stages.jsonl", "report.json"}:
            assert "dataset-a" not in contents


@pytest.mark.parametrize("mode, resource_id", [
    ("workflow", "app-a"), ("rag_read", "dataset-a"), ("rag_write", "dataset-a"),
])
def test_run_writes_context_and_correlated_events(tmp_path: Path, mode: str, resource_id: str) -> None:
    original = capacity.load_config(_config(tmp_path, mode))
    config = replace(
        original, min_rate_per_minute=200, max_rate_per_minute=200,
        stage_seconds=0.6,
    )

    report = capacity.run(config, config.results_root / "context-run", FakeTransport())

    run_dir = Path(report["run_dir"])
    context = json.loads((run_dir / "context.json").read_text())
    tasks = [json.loads(line) for line in (run_dir / "tasks.jsonl").read_text().splitlines()]
    events = [json.loads(line) for line in (run_dir / "events.jsonl").read_text().splitlines()]
    resource = context["targets"][0]["resources"][0]
    assert context["run_id"] == report["run_id"]
    assert context["targets"][0]["namespace"] == "mas-a"
    assert context["targets"][0]["base_url"] == "https://mas-a.example.test"
    assert resource["resource_id"] == resource_id
    assert resource["kind"] == mode
    assert len(tasks) == 2
    for task in tasks:
        assert task["job_id"] == f'{report["run_id"]}-{task["sequence"]}'
        assert task["namespace"] == "mas-a"
        assert task["resource_id"] == resource_id
        assert task["mode"] == mode
        lifecycle = [event["event"] for event in events if event.get("job_id") == task["job_id"]]
        assert lifecycle == ["scheduled", "dispatched", "completed"]
    for path in run_dir.iterdir():
        assert stat.S_IMODE(path.stat().st_mode) == 0o600
        contents = path.read_text()
        assert "service-secret" not in contents
        assert "fixture text" not in contents
        assert "stable query" not in contents


def test_event_log_distinguishes_namespaces_and_datasets(tmp_path: Path) -> None:
    original = capacity.load_config(_config(tmp_path, "rag_read"))
    first = original.targets[0]
    second = replace(
        first,
        namespace="mas-b",
        transport_target=replace(first.transport_target, namespace="mas-b", base_url="https://mas-b.example.test"),
        fixtures=(replace(first.fixtures[0], label="read-b", resource_id="dataset-b"),),
    )
    config = replace(original, targets=(first, second), stage_seconds=0.6)
    events: list[dict[str, object]] = []

    _, results = capacity.measure_stage(config, FakeTransport(), 200, "runid", 0, event_sink=events.append)

    assert len(results) == 2
    scheduled = [event for event in events if event["event"] == "scheduled"]
    assert [(event["job_id"], event["namespace"], event["resource_id"]) for event in scheduled] == [
        ("runid-0", "mas-a", "dataset-a"),
        ("runid-1", "mas-b", "dataset-b"),
    ]


def test_capacity_status_reports_partial_stages_without_finished_report(tmp_path: Path) -> None:
    run_dir = tmp_path / "in-progress"
    run_dir.mkdir()
    _private(run_dir / "metadata.json", '{"version": 4}')
    _private(run_dir / "stages.jsonl", '{"rate_per_minute": 10, "verdict": "PASS"}\n')

    status = capacity.read_status(run_dir)

    assert status["capacity_status"] == "IN_PROGRESS_OR_INTERRUPTED"
    assert status["stages"][0]["rate_per_minute"] == 10




def test_config_requires_owner_only_file_and_positive_rate_budget(tmp_path: Path) -> None:
    path = _config(tmp_path, "rag_read")
    path.chmod(0o644)
    with pytest.raises(benchmark.ConfigError, match="0600"):
        capacity.load_config(path)
    path.chmod(0o600)
    raw = json.loads(path.read_text())
    raw["max_rate_per_minute"] = 1
    _private(path, json.dumps(raw))
    with pytest.raises(benchmark.ConfigError, match="must cover"):
        capacity.load_config(path)


def test_removed_stage_limit_is_reported_as_configuration_error(tmp_path: Path) -> None:
    path = _config(tmp_path, "workflow")
    raw = json.loads(path.read_text())
    raw["max_stages"] = 12
    _private(path, json.dumps(raw))

    with pytest.raises(benchmark.ConfigError, match="max_stages"):
        capacity.load_config(path)
