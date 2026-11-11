# mas_loadtest

Capacity/load testing of MultiAgentStudio through its HTTP API only: no kubectl, Helm, Redis or DB access needed.
Give it a MAS URL, an admin login and load parameters; it creates disposable test resources, ramps the request rate
stage by stage until the stand stops keeping up, writes a report and deletes what it created.

## Install
Python 3.12+, no runtime dependencies. `uv run python -m mas_loadtest ...` or `python3 -m mas_loadtest ...` from this directory.

## Run
```bash
cp .env.example .env && chmod 600 .env     # fill MAS_URL, MAS_EMAIL, MAS_PASSWORD (+ MAS_CA_FILE for internal PKI)
python3 -m mas_loadtest plan --scenario workflow --min-rate 10 --max-rate 100 --step 10     # offline preview
python3 -m mas_loadtest run  --scenario workflow --min-rate 10 --max-rate 100 --step 10 --stage-seconds 120
```
Credentials are read from `./.env` (git-ignored), then real environment variables, then flags (`--url`, `--email`,
`--password-file`, `--ca-file`, `--env-file`); a flag beats the environment, the environment beats `.env`.
All options can live in a TOML file (`--params params.toml`, see `examples/params.example.toml`); flags override it.

| Scenario | What is measured | Created for the run |
|---|---|---|
| `workflow` | `POST /v1/workflows/run`, blocking (default: built-in echo workflow without LLM; or `--workflow-dsl`) | workflow app + API key |
| `rag_read` | `POST /v1/datasets/<id>/retrieve` | dataset + key + seeded document |
| `rag_write` | `document/create-by-file` until indexing completes | dataset + key |

Rates are requests per minute. Each stage lasts `--stage-seconds` (minimum 60). The ramp stops at the first failing stage.

## Output
`mas_loadtest_runs/<timestamp>-<id>/` with `results/run/summary.md` (read this first), `report.json`, `tasks.jsonl`,
`events.jsonl`; `setup/` holds the generated config, `resources.json` and secrets (all 0600/0700).
`capacity_status`: `BRACKETED` = limit found between last passing and first failing rate; `LOWER_BOUND` = every
stage up to `--max-rate` passed, so the real limit is higher; `UNDETERMINED` = even the first stage failed.
Result interpretation rules: `CAPACITY_REPORT_GUIDE_RU.md`; config/engine details: `MULTI_NAMESPACE_CAPACITY_GUIDE_RU.md`.

## Other commands
- `cleanup --session <dir> --url ... --email ...` deletes resources recorded in `resources.json` (use after a failed cleanup or `--keep-resources`).
- `status --session <dir>` shows progress of a run.
- `engine plan|run|status --config capacity.json ...` runs the engine on a hand-written v4 config with existing apps/datasets/tokens (stand must not be modified).

## Tests
`uv run --group dev pytest -q`

`mas_loadtest/scenarios/llm.py` and `webhook.py` are standalone ports of the old LLM-streaming and webhook benchmarks; they are not wired into the CLI yet.

Verified live on a Helm-deployed MAS (cookie-based console auth, CSRF): `workflow`, `rag_read`, `rag_write` smoke at 6 req/min all PASS and cleanup leaves no apps or datasets.
