"""Command line entry point: provision -> run -> report -> cleanup against one MAS URL."""
from __future__ import annotations

import argparse
from datetime import UTC, datetime
import json
import os
from pathlib import Path
import sys
import tomllib

from mas_loadtest.console import ConsoleClient, ConsoleError, new_run_id
from mas_loadtest.engine import capacity
from mas_loadtest.engine import transport as core
from mas_loadtest.provision import LoadParams, Session, cleanup, provision
from mas_loadtest.summary import write_summary

PARAM_KEYS = {
    "url", "email", "ca_file", "scenario", "min_rate", "max_rate", "step", "stage_seconds", "max_inflight",
    "max_total_requests", "request_timeout", "indexing_timeout", "max_p95", "workflow_dsl", "inputs", "query",
    "upload_file", "out_dir",
}


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="mas_loadtest", description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)

    def common(command: argparse.ArgumentParser) -> None:
        command.add_argument("--params", type=Path, help="TOML file with the options below; flags override it")
        command.add_argument("--url", help="MAS origin, e.g. https://mas.example")
        command.add_argument("--email", help="admin account email; password comes from MAS_PASSWORD or --password-file")
        command.add_argument("--password-file", type=Path)
        command.add_argument("--ca-file", type=Path)
        command.add_argument("--out-dir", type=Path, help="where sessions are written (default ./mas_loadtest_runs)")

    def load(command: argparse.ArgumentParser) -> None:
        command.add_argument("--scenario", choices=["workflow", "rag_read", "rag_write"])
        command.add_argument("--min-rate", type=int, help="first stage, requests per minute")
        command.add_argument("--max-rate", type=int, help="last stage, requests per minute")
        command.add_argument("--step", type=int, help="rate increment per stage, requests per minute")
        command.add_argument("--stage-seconds", type=int, help="at least 60 (default 120)")
        command.add_argument("--max-inflight", type=int, help="concurrent requests cap (default 16)")
        command.add_argument("--max-total-requests", type=int)
        command.add_argument("--request-timeout", type=float)
        command.add_argument("--indexing-timeout", type=float)
        command.add_argument("--max-p95", type=float, help="fail a stage when p95 seconds exceeds this")
        command.add_argument("--workflow-dsl", type=Path, help="custom workflow DSL instead of the built-in echo workflow")
        command.add_argument("--inputs", help="JSON object with workflow inputs")
        command.add_argument("--query", help="rag_read query (default matches the built-in seed document)")
        command.add_argument("--upload-file", type=Path, help="file for rag_read seed / rag_write uploads")

    plan = commands.add_parser("plan", help="show stages and resources without touching MAS")
    common(plan)
    load(plan)
    run = commands.add_parser("run", help="provision, run the rate ramp, write summary, clean up")
    common(run)
    load(run)
    run.add_argument("--keep-resources", action="store_true", help="do not delete created apps/datasets afterwards")
    clean = commands.add_parser("cleanup", help="delete resources recorded in a session")
    common(clean)
    clean.add_argument("--session", type=Path, required=True)
    status = commands.add_parser("status", help="show progress of a session")
    status.add_argument("--session", type=Path, required=True)
    engine = commands.add_parser("engine", help="run the capacity engine on a hand-written v4 config (no provisioning)")
    engine.add_argument("engine_args", nargs=argparse.REMAINDER)
    return parser


def _merged(args: argparse.Namespace) -> dict[str, object]:
    values: dict[str, object] = {}
    if args.params is not None:
        with args.params.open("rb") as stream:
            values.update(tomllib.load(stream))
        unknown = set(values) - PARAM_KEYS
        if unknown:
            raise SystemExit(f"unknown keys in params file: {', '.join(sorted(unknown))}")
    for key in PARAM_KEYS:
        flag = getattr(args, key, None)
        if flag is not None:
            values[key] = flag
    return values


def _params(values: dict[str, object]) -> LoadParams:
    missing = [key for key in ("scenario", "min_rate", "max_rate", "step") if key not in values]
    if missing:
        raise SystemExit(f"missing options: {', '.join(missing)}")
    inputs = values.get("inputs")
    if isinstance(inputs, str):
        inputs = json.loads(inputs)
    extra: dict[str, object] = {}
    for key, field in (("stage_seconds", "stage_seconds"), ("max_inflight", "max_inflight"),
                       ("max_total_requests", "max_total_requests"), ("request_timeout", "request_timeout_seconds"),
                       ("indexing_timeout", "indexing_timeout_seconds"), ("max_p95", "max_p95_seconds"),
                       ("query", "query")):
        if key in values:
            extra[field] = values[key]
    if inputs is not None:
        extra["inputs"] = inputs
    for key in ("workflow_dsl", "upload_file"):
        if key in values:
            extra[key] = Path(str(values[key]))
    return LoadParams(scenario=values["scenario"], min_rate=int(values["min_rate"]), max_rate=int(values["max_rate"]),
                      step=int(values["step"]), **extra)


def _password(args: argparse.Namespace) -> str:
    if args.password_file is not None:
        return args.password_file.read_text(encoding="utf-8").strip()
    password = os.environ.get("MAS_PASSWORD", "")
    if not password:
        raise SystemExit("set MAS_PASSWORD or pass --password-file")
    return password


def _login(args: argparse.Namespace, values: dict[str, object]) -> ConsoleClient:
    if "url" not in values or "email" not in values:
        raise SystemExit("--url and --email are required")
    ca_file = Path(str(values["ca_file"])) if "ca_file" in values else None
    client = ConsoleClient(core._origin(values["url"], "url"), ca_file=ca_file)
    client.login(str(values["email"]), _password(args).strip())
    return client


def _stages(params: LoadParams) -> list[int]:
    return list(range(params.min_rate, params.max_rate + 1, params.step))


def _session(values: dict[str, object], run_id: str) -> Session:
    out = Path(str(values.get("out_dir", "mas_loadtest_runs"))).expanduser().resolve()
    root = out / f"{datetime.now(UTC):%Y%m%d-%H%M%S}-{run_id}"
    root.mkdir(parents=True, mode=0o700)
    return Session(root)


def _cmd_plan(args: argparse.Namespace) -> int:
    params = _params(_merged(args))
    stages = _stages(params)
    print(json.dumps({
        "scenario": params.scenario, "stages_per_minute": stages, "stage_seconds": params.stage_seconds,
        "max_duration_minutes": round(len(stages) * params.stage_seconds / 60, 1),
        "will_create": {"workflow": "1 workflow app + app API key",
                        "rag_read": "1 dataset + dataset API key + seeded document",
                        "rag_write": "1 dataset + dataset API key (each request uploads a new document)"}[params.scenario],
    }, ensure_ascii=False, indent=1))
    return 0


def _cmd_run(args: argparse.Namespace) -> int:
    values = _merged(args)
    params = _params(values)
    client = _login(args, values)
    run_id = new_run_id()
    session = _session(values, run_id)
    ca_file = Path(str(values["ca_file"])) if "ca_file" in values else None
    exit_code = 2
    print(f"session: {session.root}", file=sys.stderr)
    try:
        config_path = provision(client, session, params, base_url=core._origin(values["url"], "url"),
                                run_id=run_id, ca_file=ca_file)
        report = capacity.run(capacity.load_config(config_path), session.run_dir)
        write_summary(report, session.run_dir / "summary.md")
        print((session.run_dir / "summary.md").read_text(encoding="utf-8"))
        exit_code = 0 if report["capacity_status"] != "UNDETERMINED" else 1
    except (ConsoleError, core.ConfigError, core.PreflightError, core.MasRequestError, OSError) as exc:
        print(f"error: {type(exc).__name__}: {exc}", file=sys.stderr)
    finally:
        if not args.keep_resources:
            failures = cleanup(client, session)
            for failure in failures:
                print(f"cleanup failed, retry with `cleanup --session {session.root}`: {failure}", file=sys.stderr)
    return exit_code


def _cmd_cleanup(args: argparse.Namespace) -> int:
    client = _login(args, _merged(args))
    failures = cleanup(client, Session(args.session))
    for failure in failures:
        print(f"cleanup failed: {failure}", file=sys.stderr)
    return 1 if failures else 0


def _cmd_status(args: argparse.Namespace) -> int:
    print(json.dumps(capacity.read_status(Session(args.session).run_dir), ensure_ascii=False, indent=1))
    return 0


def main(argv: list[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    if args.command == "engine":
        return capacity.main(args.engine_args)
    handlers = {"plan": _cmd_plan, "run": _cmd_run, "cleanup": _cmd_cleanup, "status": _cmd_status}
    try:
        return handlers[args.command](args)
    except ConsoleError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
