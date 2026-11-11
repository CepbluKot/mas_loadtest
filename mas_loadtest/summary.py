"""Render a human-readable capacity summary from report.json data."""
from __future__ import annotations

from pathlib import Path


def _fmt(value: object) -> str:
    return "-" if value is None else (f"{value:.2f}" if isinstance(value, float) else str(value))


def write_summary(report: dict[str, object], path: Path) -> None:
    lines = [
        f"# MAS capacity summary ({report['mode']})",
        "",
        f"- Status: **{report['capacity_status']}** (stop reason: {report['stop_reason']})",
        f"- Last passing rate: {_fmt(report['last_passing_rate_per_minute'])} req/min",
        f"- First failing rate: {_fmt(report['first_failing_rate_per_minute'])} req/min",
        "",
        "| Rate/min | Submitted | Success | p50 s | p95 s | p99 s | Verdict | Errors |",
        "|---:|---:|---:|---:|---:|---:|---|---|",
    ]
    for stage in report["stages"]:
        errors = ", ".join(f"{name}={count}" for name, count in (stage.get("errors") or {}).items()) or "-"
        lines.append(
            f"| {stage['rate_per_minute']} | {stage['submitted']} | {_fmt(stage['success_ratio'])} | "
            f"{_fmt(stage['p50_seconds'])} | {_fmt(stage['p95_seconds'])} | {_fmt(stage['p99_seconds'])} | "
            f"{stage['verdict']} | {errors} |"
        )
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    path.chmod(0o600)
