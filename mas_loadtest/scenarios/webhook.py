#!/usr/bin/env python3
"""Send fixed-rate requests to a dedicated MAS webhook workflow trigger."""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import math
import ssl
import time
import urllib.error
import urllib.parse
import urllib.request
from concurrent.futures import Future, ThreadPoolExecutor, wait
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path
from uuid import UUID, uuid4


@dataclass(frozen=True)
class RequestResult:
    started_at: float
    finished_at: float
    status_code: int
    error_kind: str | None


def parse_rates(value: str) -> list[int]:
    try:
        rates = [int(item.strip()) for item in value.split(",")]
    except ValueError as error:
        raise ValueError("rates must be integers") from error
    if not rates or rates[0] < 1 or sorted(set(rates)) != rates:
        raise ValueError("rates must be positive, unique and increasing")
    return rates


def percentile(values: list[float], fraction: float) -> float | None:
    if not values:
        return None
    ordered = sorted(values)
    return ordered[math.ceil(fraction * len(ordered)) - 1]


def build_request_body(run_id: str, sequence: int, rate_per_minute: int) -> dict[str, object]:
    return {
        "benchmark_run_id": run_id,
        "sequence": sequence,
        "rate_per_minute": rate_per_minute,
        "source": "mas-capacity-test",
    }


def sanitize_webhook_target(url: str) -> dict[str, str]:
    parsed = urllib.parse.urlsplit(url)
    if parsed.scheme not in {"http", "https"} or not parsed.hostname:
        raise ValueError("webhook URL must use HTTP or HTTPS and include a host")
    if parsed.username or parsed.password or parsed.query or parsed.fragment:
        raise ValueError("webhook URL must not contain user info, query, or fragment")
    if "/triggers/webhook/" not in parsed.path:
        raise ValueError("URL must target a MAS /triggers/webhook/<id> endpoint")
    origin = f"{parsed.scheme}://{parsed.netloc}"
    return {
        "origin": origin,
        "target_sha256": hashlib.sha256(url.encode("utf-8")).hexdigest(),
    }


def summarize_stage(
    *,
    rate_per_minute: int,
    seconds: int,
    planned: int,
    submitted: int,
    results: list[RequestResult],
) -> dict[str, object]:
    accepted = sum(200 <= result.status_code < 300 and result.error_kind is None for result in results)
    errors = len(results) - accepted
    latencies = [result.finished_at - result.started_at for result in results]
    return {
        "target_tasks_per_minute": rate_per_minute,
        "planned": planned,
        "submitted": submitted,
        "accepted": accepted,
        "http_errors": errors,
        "dropped": planned - submitted,
        "arrival_fidelity": submitted / planned if planned else 0.0,
        "acceptance_ratio": accepted / submitted if submitted else 0.0,
        "http_error_ratio": errors / submitted if submitted else None,
        "http_rps": len(results) / seconds,
        "http_p50_seconds": percentile(latencies, 0.50),
        "http_p95_seconds": percentile(latencies, 0.95),
        "http_p99_seconds": percentile(latencies, 0.99),
        "workflow_completions_observed": False,
    }


def verdict(
    summary: dict[str, object], *, min_success_ratio: float, max_p95_seconds: float
) -> list[str]:
    reasons: list[str] = []
    if float(summary["arrival_fidelity"]) < 0.95:
        reasons.append("load generator missed planned arrivals")
    if float(summary["acceptance_ratio"]) < min_success_ratio:
        reasons.append("webhook acceptance ratio below target")
    error_ratio = summary["http_error_ratio"]
    if error_ratio is None or float(error_ratio) > 0.01:
        reasons.append("HTTP errors above 1%")
    p95 = summary["http_p95_seconds"]
    if p95 is None or float(p95) > max_p95_seconds:
        reasons.append("HTTP p95 above target")
    return reasons


def write_report(directory: Path, stages: list[dict[str, object]]) -> None:
    if not stages:
        return
    columns = list(stages[0])
    with (directory / "results.csv").open("w", encoding="utf-8", newline="") as output:
        writer = csv.DictWriter(output, fieldnames=columns)
        writer.writeheader()
        writer.writerows(stages)
    lines = [
        "# Бенчмарк асинхронных workflow через webhook MAS",
        "",
        "HTTP 2xx измеряет только принятие webhook и постановку trigger-задачи. Завершение workflow и время выполнения нужно сверить отдельно по `workflow_trigger_logs` для того же `benchmark_run_id`; пока такой аудит не приложен, максимум нельзя считать подтверждённой пропускной способностью worker-ов.",
        "",
        "| Конфигурация | Цель, задач/мин | Отправлено | Принято | HTTP RPS | p95/p99, с | Итог |",
        "| --- | ---: | ---: | ---: | ---: | ---: | --- |",
    ]
    for stage in stages:
        def number(key: str) -> str:
            value = stage.get(key)
            return "—" if value is None else f"{float(value):.2f}"

        lines.append(
            f"| {stage['config']} | {stage['target_tasks_per_minute']} | "
            f"{stage['submitted']} | {stage['accepted']} | {number('http_rps')} | "
            f"{number('http_p95_seconds')}/{number('http_p99_seconds')} | {stage['verdict']} |"
        )
    lines.extend(["", "Статус `PASS_ACK_ONLY` не подтверждает выполнение workflow в Celery."])
    (directory / "report.md").write_text("\n".join(lines) + "\n", encoding="utf-8")


class WebhookClient:
    def __init__(self, url: str, ca_file: str | None) -> None:
        self.url = url
        self.context = ssl.create_default_context(cafile=ca_file)

    def trigger(self, run_id: str, sequence: int, rate_per_minute: int, timeout: float) -> RequestResult:
        started = time.monotonic()
        status = 0
        error_kind: str | None = None
        request = urllib.request.Request(
            self.url,
            data=json.dumps(build_request_body(run_id, sequence, rate_per_minute)).encode("utf-8"),
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        try:
            with urllib.request.urlopen(request, timeout=timeout, context=self.context) as response:
                status = response.status
                response.read(4096)
        except urllib.error.HTTPError as error:
            status = error.code
            error_kind = "HTTPError"
            error.close()
        except (OSError, TimeoutError) as error:
            error_kind = type(error).__name__
        return RequestResult(started, time.monotonic(), status, error_kind)


def measure_stage(
    client: WebhookClient,
    *,
    run_id: str,
    rate: int,
    seconds: int,
    max_inflight: int,
    request_timeout: float,
) -> tuple[dict[str, object], list[RequestResult]]:
    planned = math.ceil(rate * seconds / 60)
    start = time.monotonic()
    deadline = start + seconds
    futures: list[Future[RequestResult]] = []
    dropped = 0
    with ThreadPoolExecutor(max_workers=max_inflight) as pool:
        for sequence in range(planned):
            delay = start + sequence * 60 / rate - time.monotonic()
            if delay > 0:
                time.sleep(delay)
            if sum(not future.done() for future in futures) >= max_inflight:
                dropped += 1
                continue
            futures.append(pool.submit(client.trigger, run_id, sequence + 1, rate, request_timeout))
        remaining = deadline - time.monotonic()
        if remaining > 0:
            time.sleep(remaining)
        wait(futures, timeout=request_timeout + 5)
    if any(not future.done() for future in futures):
        raise RuntimeError("webhook requests did not settle before the timeout")
    results = [future.result() for future in futures]
    summary = summarize_stage(
        rate_per_minute=rate,
        seconds=seconds,
        planned=planned,
        submitted=len(futures),
        results=results,
    )
    summary["dropped"] = dropped
    return summary, results


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--webhook-url-file", type=Path, required=True)
    parser.add_argument("--ca-file")
    parser.add_argument("--app-id", required=True, help="workflow app UUID for lifecycle audit")
    parser.add_argument("--config", required=True, help="operator-supplied worker scaling label")
    parser.add_argument("--rates", required=True, help="increasing comma-separated tasks per minute")
    parser.add_argument("--stage-seconds", type=int, default=180)
    parser.add_argument("--max-inflight", type=int, default=128)
    parser.add_argument("--request-timeout-seconds", type=float, default=30)
    parser.add_argument("--min-success-ratio", type=float, default=0.99)
    parser.add_argument("--max-p95-seconds", type=float, default=3)
    parser.add_argument("--output-dir", type=Path, required=True)
    args = parser.parse_args()
    try:
        rates = parse_rates(args.rates)
        url_file = args.webhook_url_file.resolve(strict=True)
    except (OSError, ValueError) as error:
        parser.error(str(error))
    if not url_file.is_file() or url_file.stat().st_mode & 0o077:
        parser.error("webhook URL file must have mode 0600")
    try:
        webhook_url = url_file.read_text(encoding="utf-8").strip()
        target = sanitize_webhook_target(webhook_url)
    except (OSError, ValueError) as error:
        parser.error(str(error))
    if args.stage_seconds < 60 or args.max_inflight < 1:
        parser.error("stage must be at least 60s and max-inflight must be positive")
    if not args.config.strip() or not 0 < args.min_success_ratio <= 1:
        parser.error("configuration label or minimum success ratio is invalid")
    try:
        UUID(args.app_id)
    except ValueError:
        parser.error("app-id must be a UUID")

    run_id = f"MASBENCH-{uuid4().hex[:12]}"
    output = args.output_dir.resolve()
    output.mkdir(parents=True, mode=0o700, exist_ok=False)
    (output / "metadata.json").write_text(
        json.dumps(
            {
                "started_at": datetime.now(UTC).isoformat(),
                "target": target,
                "app_id": args.app_id,
                "config": args.config,
                "run_id": run_id,
                "rates_tasks_per_minute": rates,
                "stage_seconds": args.stage_seconds,
                "max_inflight": args.max_inflight,
                "completion_audit_required": "workflow_trigger_logs",
                "request_body_content_persisted": False,
            },
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )
    (output / "requests.jsonl").write_text("", encoding="utf-8")
    client = WebhookClient(webhook_url, args.ca_file)
    stages: list[dict[str, object]] = []
    for rate in rates:
        summary, results = measure_stage(
            client,
            run_id=run_id,
            rate=rate,
            seconds=args.stage_seconds,
            max_inflight=args.max_inflight,
            request_timeout=args.request_timeout_seconds,
        )
        reasons = verdict(summary, min_success_ratio=args.min_success_ratio, max_p95_seconds=args.max_p95_seconds)
        stage = {
            "config": args.config,
            "run_id": run_id,
            **summary,
            "verdict": "PASS_ACK_ONLY" if not reasons else "FAIL: " + "; ".join(reasons),
            "ended_at": datetime.now(UTC).isoformat(),
        }
        stages.append(stage)
        with (output / "requests.jsonl").open("a", encoding="utf-8") as stream:
            for result in results:
                stream.write(json.dumps(asdict(result)) + "\n")
        with (output / "stages.jsonl").open("a", encoding="utf-8") as stream:
            stream.write(json.dumps(stage) + "\n")
        write_report(output, stages)
        print(
            f"{args.config} {rate} tasks/min: {stage['verdict']}; "
            f"{stage['accepted']}/{stage['submitted']} accepted, "
            f"HTTP p95={stage['http_p95_seconds']}",
            flush=True,
        )
        if reasons:
            break
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
