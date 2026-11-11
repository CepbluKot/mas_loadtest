"""API-only capacity search for Workflow/Chatflow jobs, knowledge reads, and indexing."""

from __future__ import annotations

import argparse
from collections import Counter, defaultdict, deque
from concurrent.futures import FIRST_COMPLETED, Future, ThreadPoolExecutor, wait
from dataclasses import dataclass, replace
from datetime import UTC, datetime, timedelta
import json
import math
import os
from pathlib import Path
import time
from typing import Callable, Literal
from uuid import uuid4

from mas_loadtest.engine import transport as core

CapacityMode = Literal["workflow", "rag_read", "rag_write"]


@dataclass(frozen=True)
class Fixture:
    label: str
    kind: CapacityMode
    resource_id: str
    token_file: Path
    payload: dict[str, object]
    file_name: str | None = None
    file_bytes: bytes | None = None


@dataclass(frozen=True)
class Target:
    namespace: str
    transport_target: core.TargetConfig
    fixtures: tuple[Fixture, ...]


@dataclass(frozen=True)
class CapacityConfig:
    mode: CapacityMode
    results_root: Path
    min_rate_per_minute: int
    max_rate_per_minute: int
    rate_step_per_minute: int
    max_total_requests: int
    stage_seconds: int
    max_inflight_per_namespace: int
    request_timeout_seconds: float
    indexing_timeout_seconds: float
    max_p95_seconds: float | None
    min_success_ratio: float
    min_throughput_ratio: float
    min_arrival_fidelity: float
    targets: tuple[Target, ...]
    config_sha256: str


@dataclass(frozen=True)
class Result:
    namespace: str
    fixture: str
    started: float
    finished: float
    success: bool
    accepted: bool
    error: str | None
    sequence: int | None = None
    scheduled_at_utc: str | None = None
    queue_wait_seconds: float | None = None
    started_at_utc: str | None = None
    finished_at_utc: str | None = None
    workflow_status: str | None = None
    retrieval_hits: int | None = None
    indexing_status: str | None = None
    upload_seconds: float | None = None
    indexing_seconds: float | None = None
    indexing_polls: int | None = None
    http_error_status: int | None = None
    llm_total_tokens: int | None = None
    llm_latency_seconds: float | None = None


@dataclass(frozen=True)
class ScheduledOperation:
    target: Target
    fixture: Fixture
    sequence: int
    due: float
    scheduled_at_utc: str


def _positive_integer(value: object, name: str) -> int:
    return core._positive_int(value, name)


def _positive_float(value: object, name: str) -> float:
    return core._positive_seconds(value, name)


def _ratio(value: object, name: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)) or not 0 < value <= 1:
        raise core.ConfigError(f"{name} must be in (0, 1]")
    return float(value)


def _fixtures(raw: dict[str, object], mode: CapacityMode, namespace: str) -> tuple[Fixture, ...]:
    key = {"workflow": "workflows", "rag_read": "rag_reads", "rag_write": "rag_writes"}[mode]
    values = raw.get(key)
    if not isinstance(values, list) or not values:
        raise core.ConfigError(f"targets[{namespace}].{key} must be a non-empty list")
    result: list[Fixture] = []
    labels: set[str] = set()
    for value in values:
        item = core._object(value, f"targets[{namespace}].{key}[]")
        label = core._required_string(item, "label", key)
        if label in labels:
            raise core.ConfigError(f"targets[{namespace}].{key} labels must be unique")
        labels.add(label)
        token = core._private_file(item.get("api_token_file"), f"{key}[{label}].api_token_file", secret=True)
        file_name: str | None = None
        file_bytes: bytes | None = None
        if mode == "workflow":
            resource_id = core._required_string(item, "app_id", key)
            app_type = item.get("app_type", "workflow")
            if app_type not in {"workflow", "chatflow"}:
                raise core.ConfigError(f"{key}[{label}].app_type must be workflow or chatflow")
            inputs = core._object(item.get("inputs", {}), f"{key}[{label}].inputs")
            payload = {"inputs": inputs, "app_type": app_type}
            if app_type == "chatflow":
                payload["query"] = core._required_string(item, "query", key)
        elif mode == "rag_read":
            resource_id = core._required_string(item, "dataset_id", key)
            query = core._required_string(item, "query", key)
            top_k = _positive_integer(item.get("top_k", 5), f"{key}[{label}].top_k")
            min_hits = item.get("min_hits", 1)
            if type(min_hits) is not int or min_hits < 0:
                raise core.ConfigError(f"{key}[{label}].min_hits must be non-negative")
            model = item.get("retrieval_model")
            if model is not None:
                model = core._object(model, f"{key}[{label}].retrieval_model")
            payload = {"query": query, "top_k": top_k, "min_hits": min_hits, "retrieval_model": model}
        else:
            resource_id = core._required_string(item, "dataset_id", key)
            if "max_file_bytes" in item:
                raise core.ConfigError(f"{key}[{label}].max_file_bytes is no longer used")
            source = core._private_file(item.get("file_path"), f"{key}[{label}].file_path")
            data = source.read_bytes()
            technique = item.get("indexing_technique", "economy")
            if technique not in {"economy", "high_quality"}:
                raise core.ConfigError(f"{key}[{label}].indexing_technique is invalid")
            process_rule = core._object(item.get("process_rule"), f"{key}[{label}].process_rule")
            payload = {"indexing_technique": technique, "process_rule": process_rule}
            file_name = source.name
            file_bytes = data
        result.append(
            Fixture(
                label=label,
                kind=mode,
                resource_id=resource_id,
                token_file=token,
                payload=payload,
                file_name=file_name,
                file_bytes=file_bytes,
            )
        )
    return tuple(result)


def load_config(path: Path) -> CapacityConfig:
    import hashlib

    config_path = core._private_file(str(path), "configuration", secret=True)
    raw_bytes = config_path.read_bytes()
    try:
        raw = core._object(json.loads(raw_bytes), "configuration")
    except json.JSONDecodeError as exc:
        raise core.ConfigError("configuration is not valid JSON") from exc
    if raw.get("version") != 4:
        raise core.ConfigError("capacity configuration version must be 4; replace rates_per_minute with min_rate_per_minute and rate_step_per_minute")
    mode = raw.get("mode")
    if mode not in {"workflow", "rag_read", "rag_write"}:
        raise core.ConfigError("mode must be workflow, rag_read, or rag_write")
    if "rates_per_minute" in raw:
        raise core.ConfigError("rates_per_minute is replaced by min_rate_per_minute and rate_step_per_minute")
    if "max_stages" in raw:
        raise core.ConfigError("max_stages is no longer used; the rate range and step determine the stage count")
    if "min_completion_ratio" in raw:
        raise core.ConfigError("min_completion_ratio is replaced by min_throughput_ratio")
    if "max_inflight" in raw:
        raise core.ConfigError("max_inflight is no longer used; set max_inflight_per_namespace")
    min_rate = _positive_integer(raw.get("min_rate_per_minute"), "min_rate_per_minute")
    max_rate = _positive_integer(raw.get("max_rate_per_minute"), "max_rate_per_minute")
    step = _positive_integer(raw.get("rate_step_per_minute"), "rate_step_per_minute")
    if max_rate < min_rate:
        raise core.ConfigError("max_rate_per_minute must cover min_rate_per_minute")
    stage_seconds = _positive_integer(raw.get("stage_seconds"), "stage_seconds")
    if stage_seconds < 60:
        raise core.ConfigError("stage_seconds must be at least 60")
    max_total = _positive_integer(raw.get("max_total_requests"), "max_total_requests")
    max_inflight_per_namespace = _positive_integer(
        raw.get("max_inflight_per_namespace"), "max_inflight_per_namespace"
    )
    target_values = raw.get("targets")
    if not isinstance(target_values, list) or not target_values:
        raise core.ConfigError("targets must be a non-empty list")
    targets: list[Target] = []
    namespaces: set[str] = set()
    origins: set[str] = set()
    for value in target_values:
        item = core._object(value, "targets[]")
        namespace = core._required_string(item, "namespace", "targets[]")
        if namespace in namespaces:
            raise core.ConfigError("target namespaces must be unique")
        namespaces.add(namespace)
        origin = core._origin(item.get("base_url"), f"targets[{namespace}].base_url")
        if origin in origins:
            raise core.ConfigError("target base_url values must be unique")
        origins.add(origin)
        ca_file = core._private_file(item["ca_file"], "ca_file") if item.get("ca_file") else None
        fixtures = _fixtures(item, mode, namespace)
        console_token_raw = item.get("console_token_file")
        console_file = (
            core._private_file(console_token_raw, "console_token_file", secret=True)
            if isinstance(console_token_raw, str) and console_token_raw.strip()
            else None
        )
        transport_target = core.TargetConfig(namespace, origin, ca_file, console_file)
        targets.append(Target(namespace, transport_target, fixtures))
    minimum_coverage = len(targets) * max(len(target.fixtures) for target in targets)
    if math.ceil(min_rate * stage_seconds / 60) < minimum_coverage:
        raise core.ConfigError("first rate must schedule at least one request per resource in every target")
    results_root = Path(core._required_string(raw, "results_root", "configuration")).expanduser().resolve()
    return CapacityConfig(
        mode=mode,
        results_root=results_root,
        min_rate_per_minute=min_rate,
        max_rate_per_minute=max_rate,
        rate_step_per_minute=step,
        max_total_requests=max_total,
        stage_seconds=stage_seconds,
        max_inflight_per_namespace=max_inflight_per_namespace,
        request_timeout_seconds=_positive_float(raw.get("request_timeout_seconds"), "request_timeout_seconds"),
        indexing_timeout_seconds=_positive_float(raw.get("indexing_timeout_seconds"), "indexing_timeout_seconds"),
        max_p95_seconds=(
            _positive_float(raw["max_p95_seconds"], "max_p95_seconds")
            if raw.get("max_p95_seconds") is not None else None
        ),
        min_success_ratio=_ratio(raw.get("min_success_ratio", 0.99), "min_success_ratio"),
        min_throughput_ratio=_ratio(raw.get("min_throughput_ratio", 0.9), "min_throughput_ratio"),
        min_arrival_fidelity=_ratio(raw.get("min_arrival_fidelity", 0.95), "min_arrival_fidelity"),
        targets=tuple(targets),
        config_sha256=hashlib.sha256(raw_bytes).hexdigest(),
    )


def plan_document(config: CapacityConfig) -> dict[str, object]:
    stages_to_max = (
        (config.max_rate_per_minute - config.min_rate_per_minute + config.rate_step_per_minute - 1)
        // config.rate_step_per_minute + 1
    )
    return {
        "schema_version": 1,
        "mode": config.mode,
        "load_distribution": "equal_per_target",
        "targets": {
            target.namespace: [fixture.label for fixture in target.fixtures] for target in config.targets
        },
        "min_rate_per_minute": config.min_rate_per_minute,
        "max_rate_per_minute": config.max_rate_per_minute,
        "rate_step_per_minute": config.rate_step_per_minute,
        "stages_to_max_rate": stages_to_max,
        "planned_duration_seconds_to_max_rate": stages_to_max * config.stage_seconds,
        "stage_seconds": config.stage_seconds,
        "max_total_requests": config.max_total_requests,
        "max_inflight_per_namespace": config.max_inflight_per_namespace,
        "maximum_concurrent_jobs": len(config.targets) * config.max_inflight_per_namespace,
        "criteria": {
            "max_p95_seconds": config.max_p95_seconds,
            "min_success_ratio": config.min_success_ratio,
            "min_throughput_ratio": config.min_throughput_ratio,
            "min_arrival_fidelity": config.min_arrival_fidelity,
        },
    }


def preflight(config: CapacityConfig, transport: core.MasTransport) -> None:
    for target in config.targets:
        api_target = target.transport_target
        for fixture in target.fixtures:
            if fixture.kind == "workflow":
                if fixture.payload["app_type"] == "chatflow" or api_target.console_token_file is None:
                    info = transport.get_json(api_target, fixture.token_file, "/v1/info")
                    expected_mode = "advanced-chat" if fixture.payload["app_type"] == "chatflow" else "workflow"
                    if info.get("mode") != expected_mode:
                        raise core.PreflightError(
                            f"workflow API {fixture.label} is inaccessible or not a {fixture.payload['app_type']} app"
                        )
                else:
                    graph_payload = transport.get_json(
                        api_target, api_target.console_token_file,
                        f"/console/api/apps/{fixture.resource_id}/workflows/publish",
                    )
                    graph = graph_payload.get("graph")
                    if not isinstance(graph, dict) or not isinstance(graph.get("nodes"), list):
                        raise core.PreflightError(f"workflow {fixture.label} is not published")
                    if any(
                        isinstance(node, dict)
                        and isinstance(node.get("data"), dict)
                        and node["data"].get("type") == "knowledge-retrieval"
                        for node in graph["nodes"]
                    ):
                        raise core.PreflightError(f"workflow {fixture.label} contains Knowledge Retrieval")
                    keys = transport.get_json(
                        api_target, api_target.console_token_file,
                        f"/console/api/apps/{fixture.resource_id}/api-keys",
                    )
                    core._validate_api_token_binding(keys, fixture.token_file, fixture.label)
            else:
                dataset = transport.get_json(
                    api_target, fixture.token_file, f"/v1/datasets/{fixture.resource_id}"
                )
                if dataset.get("id") != fixture.resource_id:
                    raise core.PreflightError(f"dataset {fixture.label} is inaccessible")
                if fixture.kind == "rag_write" and dataset.get("indexing_technique") != fixture.payload["indexing_technique"]:
                    raise core.PreflightError(f"dataset {fixture.label} indexing technique differs from config")


def _retrieval_payload(fixture: Fixture) -> dict[str, object]:
    if fixture.payload["retrieval_model"] is not None:
        return {"query": fixture.payload["query"], "retrieval_model": fixture.payload["retrieval_model"]}
    return {
        "query": fixture.payload["query"],
        "retrieval_model": {
            "search_method": "keyword_search",
            "reranking_enable": False,
            "reranking_model": {"reranking_provider_name": "", "reranking_model_name": ""},
            "top_k": fixture.payload["top_k"],
            "score_threshold_enabled": False,
        },
    }


def execute_one(
    target: Target,
    fixture: Fixture,
    transport: core.MasTransport,
    run_id: str,
    sequence: int,
    indexing_timeout: float,
) -> Result:
    started = time.monotonic()
    started_at_utc = datetime.now(UTC).isoformat()
    accepted = False
    success = False
    error: str | None = None
    workflow_status: str | None = None
    retrieval_hits: int | None = None
    indexing_status: str | None = None
    upload_seconds: float | None = None
    indexing_started: float | None = None
    indexing_polls: int | None = None
    http_error_status: int | None = None
    llm_total_tokens: int | None = None
    llm_latency_seconds: float | None = None
    api_target = target.transport_target
    try:
        if fixture.kind == "workflow":
            user = f"MASBENCH-{run_id}-{sequence}"
            if fixture.payload["app_type"] == "chatflow":
                response = transport.post_json(
                    api_target, fixture.token_file, "/v1/chat-messages",
                    {
                        "inputs": fixture.payload["inputs"],
                        "query": fixture.payload["query"],
                        "response_mode": "blocking",
                        "user": user,
                        "auto_generate_name": False,
                    },
                )
                accepted = True
                workflow_status = "succeeded" if response.get("event") == "message" else None
                success = (
                    workflow_status == "succeeded"
                    and isinstance(response.get("message_id"), str)
                    and bool(response.get("answer"))
                )
                metadata = response.get("metadata")
                usage = metadata.get("usage") if isinstance(metadata, dict) else None
                if isinstance(usage, dict):
                    total_tokens = usage.get("total_tokens")
                    latency = usage.get("latency")
                    llm_total_tokens = total_tokens if type(total_tokens) is int else None
                    llm_latency_seconds = float(latency) if isinstance(latency, (int, float)) else None
                if not success:
                    error = "ChatflowFailed"
            else:
                response = transport.post_json(
                    api_target, fixture.token_file, "/v1/workflows/run",
                    {"inputs": fixture.payload["inputs"], "response_mode": "blocking", "user": user},
                )
                accepted = True
                data = response.get("data")
                workflow_status = data.get("status") if isinstance(data, dict) and isinstance(data.get("status"), str) else None
                success = isinstance(data, dict) and bool(data.get("id")) and data.get("status") == "succeeded"
                if not success:
                    error = "WorkflowFailed"
        elif fixture.kind == "rag_read":
            response = transport.post_json(
                api_target, fixture.token_file,
                f"/v1/datasets/{fixture.resource_id}/retrieve", _retrieval_payload(fixture),
            )
            accepted = True
            records = response.get("records")
            retrieval_hits = len(records) if isinstance(records, list) else None
            success = isinstance(records, list) and len(records) >= int(fixture.payload["min_hits"])
            if not success:
                error = "RetrievalResultInvalid"
        else:
            assert fixture.file_bytes is not None and fixture.file_name is not None
            suffix = Path(fixture.file_name).suffix
            filename = f"masbench-{run_id}-{sequence}{suffix}"
            upload_started = time.monotonic()
            try:
                response = transport.post_multipart(
                    api_target, fixture.token_file,
                    f"/v1/datasets/{fixture.resource_id}/document/create_by_file",
                    fixture.file_bytes, filename,
                    {"data": json.dumps(fixture.payload, ensure_ascii=False, separators=(",", ":"))},
                )
            finally:
                upload_seconds = time.monotonic() - upload_started
            accepted = True
            document = response.get("document")
            document_id = document.get("id") if isinstance(document, dict) else None
            batch = response.get("batch")
            if not isinstance(document_id, str) or not isinstance(batch, str):
                error = "UploadResponseInvalid"
            else:
                indexing_started = time.monotonic()
                indexing_polls = 0
                deadline = time.monotonic() + indexing_timeout
                while time.monotonic() < deadline:
                    indexing_polls += 1
                    status = transport.get_json(
                        api_target, fixture.token_file,
                        f"/v1/datasets/{fixture.resource_id}/documents/{batch}/indexing-status",
                    )
                    state = core.indexing_state(status, document_id)
                    indexing_status = state
                    if state in {"completed", "error", "paused", "stopped"}:
                        success = state == "completed"
                        if not success:
                            error = "IndexingError"
                        break
                    time.sleep(min(1.0, max(0.0, deadline - time.monotonic())))
                if not success and error is None:
                    error = "UncertainIndexingOutcome"
    except core.MasRequestError as exc:
        error = "UncertainPostOutcome" if exc.uncertain else exc.error_type
        http_error_status = exc.http_status
    except core.PreflightError:
        error = "UncertainIndexingOutcome" if accepted and fixture.kind == "rag_write" else "PreflightError"
    except (ValueError, OSError) as exc:
        error = type(exc).__name__
    except Exception:
        error = "ClientError"
    finished = time.monotonic()
    return Result(
        target.namespace, fixture.label, started, finished, success, accepted, error,
        sequence=sequence,
        started_at_utc=started_at_utc,
        finished_at_utc=datetime.now(UTC).isoformat(),
        workflow_status=workflow_status,
        retrieval_hits=retrieval_hits,
        indexing_status=indexing_status,
        upload_seconds=upload_seconds,
        indexing_seconds=finished - indexing_started if indexing_started is not None else None,
        indexing_polls=indexing_polls,
        http_error_status=http_error_status,
        llm_total_tokens=llm_total_tokens if fixture.payload.get("app_type") == "chatflow" else None,
        llm_latency_seconds=llm_latency_seconds if fixture.payload.get("app_type") == "chatflow" else None,
    )


def _percentile(values: list[float], fraction: float) -> float | None:
    if not values:
        return None
    ordered = sorted(values)
    return ordered[math.ceil(fraction * len(ordered)) - 1]


def _throughput_rates(samples: list[Result]) -> tuple[float | None, float | None, float | None]:
    successful = [sample for sample in samples if sample.success]
    if len(successful) < 3:
        return None, None, None
    lower = math.floor(0.1 * (len(successful) - 1))
    upper = math.ceil(0.9 * (len(successful) - 1))
    starts = sorted(sample.started for sample in successful)
    finishes = sorted(sample.finished for sample in successful)
    start_span = starts[upper] - starts[lower]
    finish_span = finishes[upper] - finishes[lower]
    if start_span <= 0 or finish_span <= 0:
        return None, None, None
    started_rate = (upper - lower) * 60 / start_span
    completed_rate = (upper - lower) * 60 / finish_span
    return started_rate, completed_rate, min(1.0, completed_rate / started_rate)


def _result_stats(samples: list[Result], deadline: float, stage_seconds: int) -> dict[str, object]:
    done = [sample for sample in samples if sample.success]
    in_window = [sample for sample in done if sample.finished <= deadline]
    durations = [sample.finished - sample.started for sample in done]
    queue_waits = [sample.queue_wait_seconds for sample in samples if sample.queue_wait_seconds is not None]
    started_rate, completed_rate, throughput_ratio = _throughput_rates(samples)
    return {
        "submitted": len(samples),
        "started_in_window": sum(sample.started < deadline for sample in samples),
        "accepted": sum(sample.accepted for sample in samples),
        "completed": len(done),
        "completed_in_window": len(in_window),
        "completed_per_second": len(in_window) / stage_seconds,
        "observed_start_rate_per_minute": started_rate,
        "observed_completion_rate_per_minute": completed_rate,
        "throughput_ratio": throughput_ratio,
        "p50_seconds": _percentile(durations, 0.5),
        "p95_seconds": _percentile(durations, 0.95),
        "p99_seconds": _percentile(durations, 0.99),
        "queue_wait_p50_seconds": _percentile(queue_waits, 0.5),
        "queue_wait_p95_seconds": _percentile(queue_waits, 0.95),
        "queue_wait_p99_seconds": _percentile(queue_waits, 0.99),
        "errors": dict(Counter(sample.error for sample in samples if sample.error)),
    }


def stage_summary(
    config: CapacityConfig,
    rate: int,
    planned: int,
    submitted: int,
    deadline: float,
    results: list[Result],
) -> dict[str, object]:
    successful = [result for result in results if result.success]
    completed_in_window = [result for result in successful if result.finished <= deadline]
    durations = [result.finished - result.started for result in successful]
    queue_waits = [result.queue_wait_seconds for result in results if result.queue_wait_seconds is not None]
    p95 = _percentile(durations, 0.95)
    started_in_window = sum(result.started < deadline for result in results)
    fidelity = started_in_window / planned if planned else 0.0
    success_ratio = len(successful) / submitted if submitted else 0.0
    completion_ratio = len(completed_in_window) / submitted if submitted else 0.0
    started_rate, completed_rate, throughput_ratio = _throughput_rates(results)
    reasons: list[str] = []
    if fidelity < config.min_arrival_fidelity:
        verdict = "INCONCLUSIVE_GENERATOR"
        reasons.append("API starts inside the stage window below target")
    else:
        if success_ratio < config.min_success_ratio:
            reasons.append("success ratio below target")
        if throughput_ratio is not None and throughput_ratio < config.min_throughput_ratio:
            reasons.append("throughput ratio below target")
        if config.max_p95_seconds is not None and (p95 is None or p95 > config.max_p95_seconds):
            reasons.append("p95 above target")
        verdict = "FAIL" if reasons else "PASS"
    grouped: dict[str, list[Result]] = defaultdict(list)
    for result in results:
        key = f"{result.namespace}/{result.fixture}"
        grouped[key].append(result)
    resource_stats: dict[str, dict[str, object]] = {}
    namespace_stats: dict[str, dict[str, object]] = {}
    missing_coverage = False
    failed_resources: list[str] = []
    for target in config.targets:
        namespace_samples = [result for result in results if result.namespace == target.namespace]
        namespace_stats[target.namespace] = _result_stats(namespace_samples, deadline, config.stage_seconds)
        for fixture in target.fixtures:
            key = f"{target.namespace}/{fixture.label}"
            samples = grouped.get(key, [])
            done = [sample for sample in samples if sample.success]
            stats = _result_stats(samples, deadline, config.stage_seconds)
            resource_p95 = _percentile([sample.finished - sample.started for sample in done], 0.95)
            _, _, resource_throughput_ratio = _throughput_rates(samples)
            count = len(samples)
            resource_stats[key] = stats
            if not count:
                missing_coverage = True
            elif (
                len(done) / count < config.min_success_ratio
                or (
                    resource_throughput_ratio is not None
                    and resource_throughput_ratio < config.min_throughput_ratio
                )
                or (
                    config.max_p95_seconds is not None
                    and (resource_p95 is None or resource_p95 > config.max_p95_seconds)
                )
            ):
                failed_resources.append(key)
    if any(result.error in {"UncertainPostOutcome", "UncertainIndexingOutcome"} for result in results):
        verdict = "INCONCLUSIVE_UNCERTAIN"
        reasons.append("at least one operation has an unknown server-side outcome")
    elif verdict != "INCONCLUSIVE_GENERATOR":
        if missing_coverage:
            verdict = "INCONCLUSIVE_COVERAGE"
            reasons.append("at least one configured resource received no request")
        elif failed_resources:
            verdict = "FAIL"
            reasons.append("resource SLO failed: " + ", ".join(sorted(failed_resources)))
    return {
        "rate_per_minute": rate,
        "planned": planned,
        "submitted": submitted,
        "started_in_window": started_in_window,
        "accepted": sum(result.accepted for result in results),
        "completed": len(successful),
        "completed_in_window": len(completed_in_window),
        "completed_per_second": len(completed_in_window) / config.stage_seconds,
        "arrival_fidelity": fidelity,
        "success_ratio": success_ratio,
        "completion_ratio": completion_ratio,
        "observed_start_rate_per_minute": started_rate,
        "observed_completion_rate_per_minute": completed_rate,
        "throughput_ratio": throughput_ratio,
        "p50_seconds": _percentile(durations, 0.5),
        "p95_seconds": p95,
        "p99_seconds": _percentile(durations, 0.99),
        "queue_wait_p50_seconds": _percentile(queue_waits, 0.5),
        "queue_wait_p95_seconds": _percentile(queue_waits, 0.95),
        "queue_wait_p99_seconds": _percentile(queue_waits, 0.99),
        "errors": dict(Counter(result.error for result in results if result.error)),
        "namespaces": namespace_stats,
        "resources": resource_stats,
        "verdict": verdict,
        "reasons": reasons,
    }


def measure_stage(
    config: CapacityConfig,
    transport: core.MasTransport,
    rate: int,
    run_id: str,
    sequence_offset: int,
    event_sink: Callable[[dict[str, object]], None] | None = None,
) -> tuple[dict[str, object], list[Result]]:
    target_count = len(config.targets)
    fixture_cursors = [
        (sequence_offset + target_count - 1 - target_index) // target_count
        for target_index in range(target_count)
    ]
    planned = math.ceil(rate * config.stage_seconds / 60)
    interval = 60 / rate
    started = time.monotonic()
    window_started_at = datetime.now(UTC)
    deadline = started + config.stage_seconds
    namespaces = tuple(target.namespace for target in config.targets)
    pending: dict[str, deque[ScheduledOperation]] = {namespace: deque() for namespace in namespaces}
    active: dict[Future[Result], ScheduledOperation] = {}
    active_by_namespace = {namespace: 0 for namespace in namespaces}
    peak_pending_by_namespace = {namespace: 0 for namespace in namespaces}
    peak_pending = 0
    next_index = 0
    dispatch_cursor = 0
    results: list[Result] = []

    def emit(operation: ScheduledOperation, event: str, details: dict[str, object] | None = None) -> None:
        if event_sink is None:
            return
        record: dict[str, object] = {
            "event": event,
            "recorded_at_utc": datetime.now(UTC).isoformat(),
            "scheduled_at_utc": operation.scheduled_at_utc,
            "run_id": run_id,
            "job_id": f"{run_id}-{operation.sequence}",
            "sequence": operation.sequence,
            "namespace": operation.target.namespace,
            "fixture": operation.fixture.label,
            "mode": operation.fixture.kind,
            "resource_id": operation.fixture.resource_id,
        }
        if details:
            record.update(details)
        event_sink(record)

    maximum_concurrent_jobs = target_count * config.max_inflight_per_namespace
    with ThreadPoolExecutor(max_workers=maximum_concurrent_jobs) as pool:
        while next_index < planned or any(pending.values()) or active:
            now = time.monotonic()
            while next_index < planned and started + next_index * interval <= now:
                target_index = (sequence_offset + next_index) % target_count
                target = config.targets[target_index]
                fixture = target.fixtures[fixture_cursors[target_index] % len(target.fixtures)]
                fixture_cursors[target_index] += 1
                due = started + next_index * interval
                operation = ScheduledOperation(
                    target, fixture, sequence_offset + next_index, due,
                    (window_started_at + timedelta(seconds=next_index * interval)).isoformat(),
                )
                pending[target.namespace].append(operation)
                emit(operation, "scheduled")
                next_index += 1

            while len(active) < maximum_concurrent_jobs:
                selected: int | None = None
                for offset in range(len(namespaces)):
                    candidate = (dispatch_cursor + offset) % len(namespaces)
                    namespace = namespaces[candidate]
                    if pending[namespace] and active_by_namespace[namespace] < config.max_inflight_per_namespace:
                        selected = candidate
                        break
                if selected is None:
                    break
                namespace = namespaces[selected]
                operation = pending[namespace].popleft()
                future = pool.submit(
                    execute_one, operation.target, operation.fixture, transport, run_id,
                    operation.sequence, config.indexing_timeout_seconds,
                )
                active[future] = operation
                active_by_namespace[namespace] += 1
                dispatch_cursor = (selected + 1) % len(namespaces)
                emit(operation, "dispatched")

            peak_pending = max(peak_pending, sum(len(queue) for queue in pending.values()))
            for namespace, queue in pending.items():
                peak_pending_by_namespace[namespace] = max(peak_pending_by_namespace[namespace], len(queue))

            completed = [future for future in active if future.done()]
            if completed:
                for future in completed:
                    operation = active.pop(future)
                    active_by_namespace[operation.target.namespace] -= 1
                    result = future.result()
                    completed_result = replace(
                        result,
                        sequence=operation.sequence,
                        scheduled_at_utc=operation.scheduled_at_utc,
                        queue_wait_seconds=max(0.0, result.started - operation.due),
                    )
                    results.append(completed_result)
                    emit(operation, "completed", {
                        "started_at_utc": completed_result.started_at_utc,
                        "finished_at_utc": completed_result.finished_at_utc,
                        "queue_wait_seconds": completed_result.queue_wait_seconds,
                        "duration_seconds": completed_result.finished - completed_result.started,
                        "accepted": completed_result.accepted,
                        "success": completed_result.success,
                        "error": completed_result.error,
                        "http_error_status": completed_result.http_error_status,
                    })
                continue

            next_due = started + next_index * interval if next_index < planned else None
            if active:
                timeout = max(0.0, next_due - time.monotonic()) if next_due is not None else None
                wait(active, timeout=timeout, return_when=FIRST_COMPLETED)
            elif next_due is not None:
                time.sleep(max(0.0, next_due - time.monotonic()))

    delay = deadline - time.monotonic()
    if delay > 0:
        time.sleep(delay)
    results.sort(key=lambda result: result.sequence if result.sequence is not None else -1)
    stage = stage_summary(config, rate, planned, len(results), deadline, results)
    stage.update({
        "window_started_at_utc": window_started_at.isoformat(),
        "window_ended_at_utc": (window_started_at + timedelta(seconds=config.stage_seconds)).isoformat(),
        "all_tasks_finished_at_utc": datetime.now(UTC).isoformat(),
        "drain_seconds": max(0.0, time.monotonic() - deadline),
        "peak_pending": peak_pending,
        "peak_pending_by_namespace": peak_pending_by_namespace,
    })
    return stage, results


def next_rate(current: int, max_rate: int, step: int) -> int | None:
    if current >= max_rate:
        return None
    return min(max_rate, current + step)


def _private_write(path: Path, payload: str) -> None:
    descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    with os.fdopen(descriptor, "w", encoding="utf-8") as stream:
        stream.write(payload)


def _private_append(path: Path, payload: str) -> None:
    descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_APPEND, 0o600)
    os.fchmod(descriptor, 0o600)
    with os.fdopen(descriptor, "w", encoding="utf-8") as stream:
        stream.write(payload)


def run(config: CapacityConfig, run_dir: Path, transport: core.MasTransport | None = None) -> dict[str, object]:
    path = core._safe_run_dir(run_dir, config.results_root)
    if path.exists():
        raise core.ConfigError("run-dir already exists")
    client = transport or core.HttpMasTransport(config.request_timeout_seconds)
    preflight(config, client)
    core._secure_directory(config.results_root)
    core._secure_directory(path)
    run_id = uuid4().hex[:12]
    created_at = datetime.now(UTC).isoformat()
    _private_write(path / "metadata.json", json.dumps({
        "version": 4, "mode": config.mode, "run_id": run_id,
        "config_sha256": config.config_sha256,
        "created_at": created_at,
    }) + "\n")
    _private_write(path / "context.json", json.dumps({
        "schema_version": 1,
        "run_id": run_id,
        "mode": config.mode,
        "config_sha256": config.config_sha256,
        "targets": [
            {
                "namespace": target.namespace,
                "base_url": target.transport_target.base_url,
                "resources": [
                    {"label": fixture.label, "kind": fixture.kind, "resource_id": fixture.resource_id}
                    for fixture in target.fixtures
                ],
            }
            for target in config.targets
        ],
    }, ensure_ascii=False, sort_keys=True) + "\n")
    events_path = path / "events.jsonl"
    _private_write(events_path, "")
    resource_ids = {
        (target.namespace, fixture.label): fixture.resource_id
        for target in config.targets for fixture in target.fixtures
    }

    def record_event(event: dict[str, object]) -> None:
        _private_append(events_path, json.dumps(event, ensure_ascii=False, sort_keys=True) + "\n")

    stages: list[dict[str, object]] = []
    last_pass: int | None = None
    first_fail: int | None = None
    offered_total = 0
    sequence = 0
    rate = config.min_rate_per_minute
    stop_reason = ""
    while True:
        planned = math.ceil(rate * config.stage_seconds / 60)
        if offered_total + planned > config.max_total_requests:
            stop_reason = "request_budget"
            break
        stage_number = len(stages) + 1
        record_event({
            "event": "stage_started", "run_id": run_id, "stage": stage_number,
            "rate_per_minute": rate, "recorded_at_utc": datetime.now(UTC).isoformat(),
        })
        stage, results = measure_stage(
            config, client, rate, run_id, sequence,
            event_sink=lambda event: record_event({"stage": stage_number, **event}),
        )
        stage["stage"] = len(stages) + 1
        stage["finished_at"] = datetime.now(UTC).isoformat()
        stages.append(stage)
        sequence += planned
        offered_total += planned
        _private_append(path / "stages.jsonl", json.dumps(stage, ensure_ascii=False, sort_keys=True) + "\n")
        task_lines = [
            json.dumps({
                "stage": stage["stage"], "namespace": result.namespace,
                "fixture": result.fixture, "success": result.success,
                "mode": config.mode,
                "resource_id": resource_ids[(result.namespace, result.fixture)],
                "job_id": f"{run_id}-{result.sequence}" if result.sequence is not None else None,
                "accepted": result.accepted, "error": result.error,
                "duration_seconds": result.finished - result.started,
                "sequence": result.sequence,
                "scheduled_at_utc": result.scheduled_at_utc,
                "queue_wait_seconds": result.queue_wait_seconds,
                "started_at_utc": result.started_at_utc,
                "finished_at_utc": result.finished_at_utc,
                "workflow_status": result.workflow_status,
                "app_type": next(
                    fixture.payload.get("app_type")
                    for target in config.targets
                    if target.namespace == result.namespace
                    for fixture in target.fixtures
                    if fixture.label == result.fixture
                ),
                "retrieval_hits": result.retrieval_hits,
                "llm_total_tokens": result.llm_total_tokens,
                "llm_latency_seconds": result.llm_latency_seconds,
                "indexing_status": result.indexing_status,
                "upload_seconds": result.upload_seconds,
                "indexing_seconds": result.indexing_seconds,
                "indexing_polls": result.indexing_polls,
                "http_error_status": result.http_error_status,
            }, ensure_ascii=False, sort_keys=True)
            for result in results
        ]
        _private_append(path / "tasks.jsonl", "\n".join(task_lines) + ("\n" if task_lines else ""))
        record_event({
            "event": "stage_finished", "run_id": run_id, "stage": stage_number,
            "rate_per_minute": rate, "verdict": stage["verdict"],
            "recorded_at_utc": datetime.now(UTC).isoformat(),
        })
        if str(stage["verdict"]).startswith("INCONCLUSIVE_"):
            stop_reason = {
                "INCONCLUSIVE_GENERATOR": "generator_limit",
                "INCONCLUSIVE_COVERAGE": "resource_coverage",
                "INCONCLUSIVE_UNCERTAIN": "unknown_server_outcome",
            }[str(stage["verdict"])]
            break
        if stage["verdict"] == "PASS":
            last_pass = rate
            following = next_rate(rate, config.max_rate_per_minute, config.rate_step_per_minute)
            if following is None:
                break
            rate = following
        else:
            first_fail = rate
            break
    if not stop_reason:
        if first_fail is not None:
            stop_reason = "capacity_bracketed" if last_pass is not None else "below_first_rate"
        elif last_pass == config.max_rate_per_minute:
            stop_reason = "max_rate_passed"
    report = {
        "schema_version": 1,
        "mode": config.mode,
        "load_distribution": "equal_per_target",
        "run_dir": str(path),
        "run_id": run_id,
        "started_at_utc": created_at,
        "finished_at_utc": datetime.now(UTC).isoformat(),
        "targets": {
            target.namespace: [fixture.label for fixture in target.fixtures] for target in config.targets
        },
        "context_file": "context.json",
        "events_file": "events.jsonl",
        "tasks_file": "tasks.jsonl",
        "criteria": {
            "stage_seconds": config.stage_seconds,
            "min_rate_per_minute": config.min_rate_per_minute,
            "max_rate_per_minute": config.max_rate_per_minute,
            "rate_step_per_minute": config.rate_step_per_minute,
            "max_total_requests": config.max_total_requests,
            "max_inflight_per_namespace": config.max_inflight_per_namespace,
            "maximum_concurrent_jobs": len(config.targets) * config.max_inflight_per_namespace,
            "request_timeout_seconds": config.request_timeout_seconds,
            "indexing_timeout_seconds": config.indexing_timeout_seconds,
            "max_p95_seconds": config.max_p95_seconds,
            "min_success_ratio": config.min_success_ratio,
            "min_throughput_ratio": config.min_throughput_ratio,
            "min_arrival_fidelity": config.min_arrival_fidelity,
        },
        "last_passing_rate_per_minute": last_pass,
        "first_failing_rate_per_minute": first_fail,
        "stop_reason": stop_reason,
        "capacity_status": "BRACKETED" if first_fail is not None and last_pass is not None else (
            "LOWER_BOUND" if last_pass is not None else "UNDETERMINED"
        ),
        "stages": stages,
    }
    core._atomic_json(path / "report.json", report)
    record_event({
        "event": "run_finished", "run_id": run_id,
        "capacity_status": report["capacity_status"],
        "stop_reason": stop_reason,
        "recorded_at_utc": datetime.now(UTC).isoformat(),
    })
    return report


def read_status(run_dir: Path) -> dict[str, object]:
    report_path = run_dir / "report.json"
    if report_path.is_file():
        return core._object(json.loads(report_path.read_text(encoding="utf-8")), "report")
    metadata_path = run_dir / "metadata.json"
    if not metadata_path.is_file():
        raise FileNotFoundError("capacity run directory does not contain metadata")
    stages_path = run_dir / "stages.jsonl"
    stages: list[dict[str, object]] = []
    if stages_path.is_file():
        contents = stages_path.read_text(encoding="utf-8")
        lines = contents.splitlines()
        for index, line in enumerate(lines):
            try:
                stages.append(core._object(json.loads(line), "stage"))
            except json.JSONDecodeError as exc:
                if index == len(lines) - 1 and not contents.endswith("\n"):
                    break
                raise core.ConfigError("capacity stage log is invalid") from exc
    return {
        "schema_version": 1,
        "run_dir": str(run_dir.resolve()),
        "capacity_status": "IN_PROGRESS_OR_INTERRUPTED",
        "stages": stages,
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    for name in ("plan", "run"):
        command = commands.add_parser(name)
        command.add_argument("--config", type=Path, required=True)
        if name == "run":
            command.add_argument("--run-dir", type=Path, required=True)
    status = commands.add_parser("status")
    status.add_argument("--run-dir", type=Path, required=True)
    args = parser.parse_args(argv)
    try:
        if args.command == "status":
            print(json.dumps(read_status(args.run_dir), ensure_ascii=False, sort_keys=True))
            return 0
        config = load_config(args.config)
        if args.command == "plan":
            print(json.dumps(plan_document(config), ensure_ascii=False, sort_keys=True))
            return 0
        report = run(config, args.run_dir)
        print(json.dumps(report, ensure_ascii=False, sort_keys=True))
        return 0 if report["capacity_status"] != "UNDETERMINED" else 1
    except (core.ConfigError, core.PreflightError, core.MasRequestError, OSError) as exc:
        print(json.dumps({"overall_status": "FAIL", "error_type": type(exc).__name__}, ensure_ascii=False))
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
