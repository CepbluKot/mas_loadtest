#!/usr/bin/env python3
"""Measure streaming LLM completion through a dedicated MAS Completion app API."""

from __future__ import annotations

import argparse
import csv
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
import hashlib
import http.client
import json
import math
import os
from pathlib import Path
import re
import secrets
import socket
import ssl
import threading
import time
from concurrent.futures import Future, ThreadPoolExecutor
from collections.abc import Callable, Iterable
import urllib.parse


@dataclass(frozen=True)
class MetricPoint:
    """One bounded-cardinality OTLP gauge and its stage dimensions."""

    name: str
    value: float
    attributes: dict[str, str]


@dataclass(frozen=True)
class LLMRequestResult:
    """One response's timings and token counts; never contains prompt or answer text."""

    started_at_utc: str
    finished_at_utc: str
    http_status: int
    completed: bool
    duration_seconds: float
    ttft_seconds: float | None
    prompt_tokens: int
    completion_tokens: int
    total_tokens: int
    error_kind: str | None
    finished_in_window: bool


def parse_rates(value: str) -> list[float]:
    """Parse a strictly increasing list of positive requests per second."""
    try:
        rates = [float(item.strip()) for item in value.split(",")]
    except ValueError as error:
        raise ValueError("rates must be numbers") from error
    if not rates or any(not math.isfinite(rate) or rate <= 0 for rate in rates):
        raise ValueError("rates must be positive finite numbers")
    if sorted(set(rates)) != rates:
        raise ValueError("rates must be unique and increasing")
    return rates


def planned_arrivals(rate_rps: float, seconds: int) -> int:
    """Return the number of fixed-rate arrivals scheduled inside the stage."""
    return math.floor(rate_rps * seconds + 1e-9)


def parse_sse_events(
    lines: Iterable[bytes],
    *,
    started_at: float,
    clock: Callable[[], float] = time.monotonic,
) -> dict[str, object]:
    """Extract completion state, TTFT and usage from SSE without retaining text."""
    event_name = ""
    data_lines: list[str] = []
    result: dict[str, object] = {
        "completed": False,
        "ttft_seconds": None,
        "prompt_tokens": 0,
        "completion_tokens": 0,
        "total_tokens": 0,
        "error_kind": None,
    }

    def consume_event() -> None:
        nonlocal event_name, data_lines
        if not data_lines:
            event_name, data_lines = "", []
            return
        try:
            payload = json.loads("\n".join(data_lines))
        except (json.JSONDecodeError, TypeError):
            result["error_kind"] = result["error_kind"] or "invalid_sse_data"
            event_name, data_lines = "", []
            return
        if not isinstance(payload, dict):
            event_name, data_lines = "", []
            return

        current_event = event_name or str(payload.get("event", ""))
        if current_event in {"message", "agent_message"}:
            answer = payload.get("answer")
            if isinstance(answer, str) and answer and result["ttft_seconds"] is None:
                result["ttft_seconds"] = max(0.0, clock() - started_at)
        elif current_event == "message_end":
            metadata = payload.get("metadata")
            usage = metadata.get("usage") if isinstance(metadata, dict) else None
            if isinstance(usage, dict):
                for name in ("prompt_tokens", "completion_tokens", "total_tokens"):
                    value = usage.get(name)
                    if isinstance(value, (int, float)) and math.isfinite(float(value)):
                        result[name] = max(0, int(value))
            result["completed"] = True
        elif current_event == "error":
            result["error_kind"] = "provider_error"
        event_name, data_lines = "", []

    for raw_line in lines:
        line = raw_line.decode("utf-8", errors="replace").rstrip("\r\n")
        if not line:
            consume_event()
        elif line.startswith(":"):
            continue
        elif line.startswith("event:"):
            event_name = line[6:].strip()
        elif line.startswith("data:"):
            data_lines.append(line[5:].lstrip())
    consume_event()
    if not result["completed"] and result["error_kind"] is None:
        result["error_kind"] = "stream_incomplete"
    return result


def percentile(values: list[float], fraction: float) -> float | None:
    if not values:
        return None
    ordered = sorted(values)
    return ordered[math.ceil(fraction * len(ordered)) - 1]


def summarize_stage(
    *,
    rate_rps: float,
    seconds: int,
    planned: int,
    submitted: int,
    results: list[dict[str, object]],
) -> dict[str, object]:
    """Calculate throughput, latency tails, TTFT and token throughput."""
    completed = [item for item in results if bool(item["completed"])]
    successful_http = [
        item for item in results if 200 <= int(item["http_status"]) < 300
    ]
    http_errors = [
        item for item in results if not 200 <= int(item["http_status"]) < 300
    ]
    in_window = [item for item in completed if bool(item["finished_in_window"])]
    latencies = [float(item["duration_seconds"]) for item in results]
    ttfts = [
        float(item["ttft_seconds"])
        for item in completed
        if item["ttft_seconds"] is not None
    ]
    completed_latencies = [float(item["duration_seconds"]) for item in completed]
    output_tokens = sum(int(item["completion_tokens"]) for item in completed)
    input_tokens = sum(int(item["prompt_tokens"]) for item in completed)
    generation_seconds = sum(
        max(0.0, float(item["duration_seconds"]) - float(item["ttft_seconds"]))
        for item in completed
        if item["ttft_seconds"] is not None
    )
    return {
        "target_rps": rate_rps,
        "stage_seconds": seconds,
        "planned": planned,
        "submitted": submitted,
        "successful_http": len(successful_http),
        "http_errors": len(http_errors),
        "completed": len(completed),
        "completed_in_window": len(in_window),
        "arrival_fidelity": submitted / max(1, planned),
        "completed_ratio": len(completed) / max(1, submitted),
        "error_ratio": (submitted - len(completed)) / max(1, submitted),
        "http_rps": len(successful_http) / seconds,
        "completed_rps": len(in_window) / seconds,
        "http_p50_seconds": percentile(latencies, 0.50),
        "http_p95_seconds": percentile(latencies, 0.95),
        "http_p99_seconds": percentile(latencies, 0.99),
        "ttft_p50_seconds": percentile(ttfts, 0.50),
        "ttft_p95_seconds": percentile(ttfts, 0.95),
        "ttft_p99_seconds": percentile(ttfts, 0.99),
        "completion_p50_seconds": percentile(completed_latencies, 0.50),
        "completion_p95_seconds": percentile(completed_latencies, 0.95),
        "completion_p99_seconds": percentile(completed_latencies, 0.99),
        "prompt_tokens": input_tokens,
        "completion_tokens": output_tokens,
        "total_tokens": sum(int(item["total_tokens"]) for item in completed),
        "output_tokens_per_second": output_tokens / generation_seconds
        if generation_seconds
        else 0.0,
    }


def verdict(
    summary: dict[str, object],
    *,
    max_ttft_p95_seconds: float,
    max_latency_p95_seconds: float,
) -> list[str]:
    """Return the first-stage stop reasons for this LLM SLO."""
    reasons: list[str] = []
    if float(summary["arrival_fidelity"]) < 0.95:
        reasons.append("generator missed planned arrivals")
    if float(summary["completed_ratio"]) < 0.99:
        reasons.append("completion ratio below 99%")
    if float(summary["error_ratio"]) > 0.01:
        reasons.append("errors above 1%")
    ttft = summary["ttft_p95_seconds"]
    if ttft is None or float(ttft) > max_ttft_p95_seconds:
        reasons.append("TTFT p95 above target")
    latency = summary["completion_p95_seconds"]
    if latency is None or float(latency) > max_latency_p95_seconds:
        reasons.append("completion p95 above target")
    return reasons


def build_stage_metric_points(
    summary: dict[str, object], *, config: str, model: str, run_id: str
) -> list[MetricPoint]:
    """Map one stage to Prometheus-friendly OTLP gauges without sensitive data."""
    values = {
        "target_rps": "target_rps",
        "planned": "planned",
        "submitted": "submitted",
        "http_errors": "http_errors",
        "completed": "completed",
        "http_rps": "http_rps",
        "completed_rps": "completed_rps",
        "arrival_fidelity": "arrival_fidelity",
        "completed_ratio": "completed_ratio",
        "error_ratio": "error_ratio",
        "http_p50_seconds": "http_p50_seconds",
        "http_p95_seconds": "http_p95_seconds",
        "http_p99_seconds": "http_p99_seconds",
        "ttft_p50_seconds": "ttft_p50_seconds",
        "ttft_p95_seconds": "ttft_p95_seconds",
        "ttft_p99_seconds": "ttft_p99_seconds",
        "completion_p50_seconds": "completion_p50_seconds",
        "completion_p95_seconds": "completion_p95_seconds",
        "completion_p99_seconds": "completion_p99_seconds",
        "prompt_tokens": "prompt_tokens",
        "completion_tokens": "completion_tokens",
        "total_tokens": "total_tokens",
        "output_tokens_per_second": "output_tokens_per_second",
    }
    attributes = {
        "run_id": run_id,
        "config": config,
        "model": model,
        "target_rps": str(summary["target_rps"]),
    }
    points = [
        MetricPoint(
            name=f"llm_benchmark.{metric}",
            value=float(summary[key]),
            attributes=attributes,
        )
        for key, metric in values.items()
        if summary.get(key) is not None
    ]
    points.append(
        MetricPoint(
            name="llm_benchmark.slo_pass",
            value=float(str(summary.get("verdict", "")).upper() == "PASS"),
            attributes=attributes,
        )
    )
    return points


def export_stage_otel(
    summary: dict[str, object],
    *,
    endpoint: str,
    namespace: str,
    release: str,
    config: str,
    model: str,
    run_id: str,
) -> None:
    """Export a completed LLM stage to the configured OTLP gRPC collector."""
    from opentelemetry.exporter.otlp.proto.grpc.metric_exporter import (
        OTLPMetricExporter,
    )
    from opentelemetry.sdk.metrics import MeterProvider
    from opentelemetry.sdk.metrics.export import PeriodicExportingMetricReader
    from opentelemetry.sdk.resources import Resource

    exporter = OTLPMetricExporter(
        endpoint=endpoint, insecure=not endpoint.startswith("https://")
    )
    reader = PeriodicExportingMetricReader(exporter, export_interval_millis=60_000)
    provider = MeterProvider(
        resource=Resource.create(
            {
                "service.name": "mas-llm-benchmark",
                "k8s.namespace.name": namespace,
                "mas.release": release,
            }
        ),
        metric_readers=[reader],
    )
    try:
        meter = provider.get_meter("mas.llm_benchmark")
        for point in build_stage_metric_points(
            summary, config=config, model=model, run_id=run_id
        ):
            meter.create_gauge(point.name).set(point.value, point.attributes)
        if not provider.force_flush(timeout_millis=10_000):
            raise RuntimeError("LLM benchmark metrics did not flush to OTLP")
    finally:
        provider.shutdown()


def sanitize_base_url(value: str) -> str:
    """Reject URL userinfo/query secrets and preserve only the MAS API origin/path."""
    parsed = urllib.parse.urlsplit(value)
    if (
        parsed.scheme not in {"https", "http"}
        or not parsed.hostname
        or parsed.username
        or parsed.password
        or parsed.query
        or parsed.fragment
    ):
        raise ValueError(
            "base URL must be HTTP(S) without credentials, query, or fragment"
        )
    host = parsed.hostname
    if ":" in host and not host.startswith("["):
        host = f"[{host}]"
    netloc = host + (f":{parsed.port}" if parsed.port else "")
    return urllib.parse.urlunsplit(
        (parsed.scheme, netloc, parsed.path.rstrip("/"), "", "")
    )


def classify_transport_error(error: BaseException) -> str:
    if isinstance(error, ssl.SSLError):
        return "tls_error"
    if isinstance(error, TimeoutError):
        return "network_timeout"
    if isinstance(error, ConnectionResetError):
        return "connection_reset"
    if isinstance(error, ConnectionRefusedError):
        return "connection_refused"
    if isinstance(error, socket.gaierror):
        return "name_resolution_error"
    if isinstance(error, OSError):
        return "network_error"
    if isinstance(error, http.client.HTTPException):
        return "http_protocol_error"
    return "request_error"


class CompletionClient:
    """Call one dedicated MAS Completion app while keeping credentials in memory."""

    def __init__(
        self,
        base_url: str,
        token: str,
        prompt: str,
        user: str,
        ca_file: str | None,
        timeout_seconds: float,
    ) -> None:
        parsed = urllib.parse.urlsplit(sanitize_base_url(base_url))
        self.host = parsed.hostname
        self.port = parsed.port
        self.scheme = parsed.scheme
        self.path = f"{parsed.path}/v1/completion-messages"
        self.token = token
        self.prompt = prompt
        self.user = user
        self.context = ssl.create_default_context(cafile=ca_file)
        self.timeout_seconds = timeout_seconds
        self.local = threading.local()
        self.connections: list[http.client.HTTPConnection] = []
        self.lock = threading.Lock()

    def _connection(self) -> http.client.HTTPConnection:
        connection = getattr(self.local, "connection", None)
        if connection is None or connection.sock is None:
            if self.host is None:
                raise ValueError("base URL must include a host")
            if self.scheme == "https":
                connection = http.client.HTTPSConnection(
                    self.host,
                    self.port,
                    timeout=self.timeout_seconds,
                    context=self.context,
                )
            else:
                connection = http.client.HTTPConnection(
                    self.host, self.port, timeout=self.timeout_seconds
                )
            self.local.connection = connection
            with self.lock:
                self.connections.append(connection)
        connection.timeout = self.timeout_seconds
        return connection

    def _discard_connection(self) -> None:
        connection = getattr(self.local, "connection", None)
        if connection is not None:
            connection.close()
            del self.local.connection

    def close_connections(self) -> None:
        with self.lock:
            connections, self.connections = self.connections, []
        for connection in connections:
            connection.close()

    def request(self, *, stage_deadline: float) -> LLMRequestResult:
        started = time.monotonic()
        started_at_utc = datetime.now(UTC).isoformat()
        http_status = 0
        parsed: dict[str, object] = {
            "completed": False,
            "ttft_seconds": None,
            "prompt_tokens": 0,
            "completion_tokens": 0,
            "total_tokens": 0,
            "error_kind": None,
        }
        try:
            body = json.dumps(
                {
                    "inputs": {},
                    "query": self.prompt,
                    "response_mode": "streaming",
                    "user": self.user,
                }
            ).encode("utf-8")
            connection = self._connection()
            connection.request(
                "POST",
                self.path,
                body=body,
                headers={
                    "Authorization": f"Bearer {self.token}",
                    "Content-Type": "application/json",
                    "Accept": "text/event-stream",
                },
            )
            response = connection.getresponse()
            http_status = response.status
            if not 200 <= http_status < 300:
                response.read()
                parsed["error_kind"] = f"http_{http_status // 100}xx"
            else:
                parsed = parse_sse_events(
                    iter(response.readline, b""), started_at=started
                )
        except (OSError, http.client.HTTPException, TimeoutError) as error:
            parsed["error_kind"] = classify_transport_error(error)
            self._discard_connection()

        finished = time.monotonic()
        return LLMRequestResult(
            started_at_utc=started_at_utc,
            finished_at_utc=datetime.now(UTC).isoformat(),
            http_status=http_status,
            completed=bool(parsed["completed"]),
            duration_seconds=max(0.0, finished - started),
            ttft_seconds=(
                float(parsed["ttft_seconds"])
                if parsed["ttft_seconds"] is not None
                else None
            ),
            prompt_tokens=int(parsed["prompt_tokens"]),
            completion_tokens=int(parsed["completion_tokens"]),
            total_tokens=int(parsed["total_tokens"]),
            error_kind=str(parsed["error_kind"]) if parsed["error_kind"] else None,
            finished_in_window=finished <= stage_deadline,
        )


def run_stage(
    client: CompletionClient,
    *,
    rate_rps: float,
    seconds: int,
    max_inflight: int,
) -> tuple[dict[str, object], list[LLMRequestResult]]:
    started = time.monotonic()
    started_at_utc = datetime.now(UTC).isoformat()
    deadline = started + seconds
    planned = planned_arrivals(rate_rps, seconds)
    pending: set[Future[LLMRequestResult]] = set()
    results: list[LLMRequestResult] = []
    submitted = 0
    with ThreadPoolExecutor(max_workers=max_inflight) as executor:
        for sequence in range(planned):
            target = started + sequence / rate_rps
            delay = target - time.monotonic()
            if delay > 0:
                time.sleep(delay)
            finished = {future for future in pending if future.done()}
            for future in finished:
                results.append(future.result())
            pending.difference_update(finished)
            if len(pending) >= max_inflight:
                continue
            pending.add(executor.submit(client.request, stage_deadline=deadline))
            submitted += 1
        for future in pending:
            results.append(future.result())
    raw_results = [asdict(result) for result in results]
    summary = summarize_stage(
        rate_rps=rate_rps,
        seconds=seconds,
        planned=planned,
        submitted=submitted,
        results=raw_results,
    )
    summary["started_at"] = started_at_utc
    summary["ended_at"] = datetime.now(UTC).isoformat()
    return summary, results


def _write_private(path: Path, content: str) -> None:
    path.write_text(content, encoding="utf-8")
    os.chmod(path, 0o600)


def write_report(output: Path) -> None:
    stages = [
        json.loads(line)
        for line in (output / "stages.jsonl").read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]
    if not stages:
        return
    columns = list(stages[0])
    with (output / "results.csv").open("w", encoding="utf-8", newline="") as stream:
        writer = csv.DictWriter(stream, fieldnames=columns)
        writer.writeheader()
        writer.writerows(stages)
    os.chmod(output / "results.csv", 0o600)
    rows = [
        "# MAS LLM completion benchmark",
        "",
        "This load targets one dedicated MAS Completion app. Worker replica/concurrency changes are controlled separately.",
        "Prompts and generated answers are not written to the result files.",
        "",
        "| Config | Target RPS | Offered | Completed | HTTP RPS | TTFT p95 | Completion p95/p99 | Output tok/s | Verdict |",
        "| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- |",
    ]
    for stage in stages:

        def number(key: str) -> str:
            value = stage.get(key)
            return "—" if value is None else f"{float(value):.2f}"

        rows.append(
            f"| {stage['config']} | {number('target_rps')} | {stage['submitted']}/{stage['planned']} | "
            f"{stage['completed_in_window']} | {number('http_rps')} | {number('ttft_p95_seconds')} s | "
            f"{number('completion_p95_seconds')}/{number('completion_p99_seconds')} s | "
            f"{number('output_tokens_per_second')} | {stage['verdict']} |"
        )
    _write_private(output / "report.md", "\n".join(rows) + "\n")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--base-url", required=True, help="MAS origin or reverse-proxy base path"
    )
    parser.add_argument("--api-token-file", type=Path, required=True)
    parser.add_argument(
        "--prompt-file",
        type=Path,
        required=True,
        help="fixed prompt; its contents are not saved",
    )
    parser.add_argument("--ca-file")
    parser.add_argument(
        "--config", required=True, help="operator-supplied API scaling label"
    )
    parser.add_argument("--model", required=True, help="configured MAS model name")
    parser.add_argument(
        "--otlp-endpoint",
        help="optional OTLP gRPC collector endpoint for per-stage Prometheus metrics",
    )
    parser.add_argument("--namespace", default="", help="Kubernetes namespace label for OTLP metrics")
    parser.add_argument("--release", default="multiagentstudio")
    parser.add_argument(
        "--rates",
        required=True,
        help="increasing requests per second, e.g. 0.1,0.2,0.5",
    )
    parser.add_argument("--stage-seconds", type=int, default=180)
    parser.add_argument("--max-inflight", type=int, default=32)
    parser.add_argument("--max-total-requests", type=int, default=500)
    parser.add_argument("--request-timeout-seconds", type=float, default=120)
    parser.add_argument("--max-ttft-p95-seconds", type=float, required=True)
    parser.add_argument("--max-latency-p95-seconds", type=float, required=True)
    parser.add_argument("--run-id", default=secrets.token_hex(6))
    parser.add_argument("--output-dir", type=Path, required=True)
    args = parser.parse_args()

    try:
        base_url = sanitize_base_url(args.base_url)
        rates = parse_rates(args.rates)
    except ValueError as error:
        parser.error(str(error))
    if not re.fullmatch(r"[0-9a-fA-F]{12}", args.run_id):
        parser.error("run id must be 12 hexadecimal characters")
    if (
        args.stage_seconds < 60
        or args.max_inflight < 1
        or args.max_total_requests < 1
        or args.request_timeout_seconds <= 0
        or args.max_ttft_p95_seconds <= 0
        or args.max_latency_p95_seconds <= 0
        or not args.config.strip()
    ):
        parser.error(
            "stage, concurrency, request cap, SLOs, or config label are invalid"
        )
    if args.otlp_endpoint and not args.namespace.strip():
        parser.error("--namespace is required with --otlp-endpoint")
    total_planned = sum(planned_arrivals(rate, args.stage_seconds) for rate in rates)
    if total_planned > args.max_total_requests:
        parser.error(
            f"planned {total_planned} requests exceeds --max-total-requests={args.max_total_requests}"
        )

    token_path = args.api_token_file.resolve()
    prompt_path = args.prompt_file.resolve()
    if not token_path.is_file() or token_path.stat().st_mode & 0o077:
        parser.error("API token file must exist and have mode 0600")
    token = token_path.read_text(encoding="utf-8").strip()
    prompt = prompt_path.read_text(encoding="utf-8").strip()
    if not token or not prompt:
        parser.error("API token and fixed prompt must be non-empty")

    output = args.output_dir.resolve()
    output.mkdir(parents=True, mode=0o700, exist_ok=False)
    os.chmod(output, 0o700)
    _write_private(output / "stages.jsonl", "")
    _write_private(output / "requests.jsonl", "")
    _write_private(
        output / "metadata.json",
        json.dumps(
            {
                "started_at": datetime.now(UTC).isoformat(),
                "api_origin": f"{urllib.parse.urlsplit(base_url).scheme}://{urllib.parse.urlsplit(base_url).netloc}",
                "config": args.config,
                "model": args.model,
                "run_id": args.run_id,
                "prompt_sha256": hashlib.sha256(prompt.encode("utf-8")).hexdigest(),
                "rates_rps": rates,
                "stage_seconds": args.stage_seconds,
                "max_inflight": args.max_inflight,
                "max_total_requests": args.max_total_requests,
                "request_timeout_seconds": args.request_timeout_seconds,
                "max_ttft_p95_seconds": args.max_ttft_p95_seconds,
                "max_latency_p95_seconds": args.max_latency_p95_seconds,
            },
            indent=2,
        )
        + "\n",
    )

    client = CompletionClient(
        base_url,
        token,
        prompt,
        f"MASBENCH-{args.run_id}",
        args.ca_file,
        args.request_timeout_seconds,
    )
    try:
        for rate in rates:
            summary, results = run_stage(
                client,
                rate_rps=rate,
                seconds=args.stage_seconds,
                max_inflight=args.max_inflight,
            )
            reasons = verdict(
                summary,
                max_ttft_p95_seconds=args.max_ttft_p95_seconds,
                max_latency_p95_seconds=args.max_latency_p95_seconds,
            )
            summary.update(
                {
                    "config": args.config,
                    "verdict": "PASS" if not reasons else "FAIL: " + "; ".join(reasons),
                }
            )
            with (output / "requests.jsonl").open("a", encoding="utf-8") as stream:
                for result in results:
                    stream.write(json.dumps(asdict(result), sort_keys=True) + "\n")
            os.chmod(output / "requests.jsonl", 0o600)
            with (output / "stages.jsonl").open("a", encoding="utf-8") as stream:
                stream.write(json.dumps(summary, sort_keys=True) + "\n")
            os.chmod(output / "stages.jsonl", 0o600)
            write_report(output)
            if args.otlp_endpoint:
                export_stage_otel(
                    summary,
                    endpoint=args.otlp_endpoint,
                    namespace=args.namespace,
                    release=args.release,
                    config=args.config,
                    model=args.model,
                    run_id=args.run_id,
                )
            print(
                f"{args.config} {rate:g} RPS: {summary['completed_in_window']}/{summary['submitted']} completed; "
                f"{summary['verdict']}",
                flush=True,
            )
            if reasons:
                break
    finally:
        client.close_connections()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
