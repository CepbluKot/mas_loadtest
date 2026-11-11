# Instructions for the model operating mas_loadtest

Goal: find how many requests per minute a MAS stand sustains, using only its HTTP API.

## Ask the human first
1. MAS URL (https origin) and admin email. The password must be in env `MAS_PASSWORD` or a 0600 file: never ask for it in chat, never put it in a flag or echo it.
2. Custom CA (`--ca-file`) if the stand uses internal PKI.
3. Scenario: `workflow` (platform path), `rag_read` (retrieval), `rag_write` (upload + indexing).
4. Rate range. If unknown, propose `--min-rate 10 --max-rate 100 --step 10`.
5. Whether the target is production. Load tests on production require an explicit yes.

## Procedure
1. `python3 -m mas_loadtest plan ...` and show the stage list and duration.
2. Smoke: `run ... --min-rate 6 --max-rate 6 --step 6 --stage-seconds 60`. It must finish with `PASS` verdict and clean up.
3. Full run with the agreed range. Runs take `stages * stage-seconds`; run in the background and read `status --session <dir>`.
4. Report from `results/run/summary.md`. State the capacity status, last passing and first failing rate, p95 at those stages, error types. If `LOWER_BOUND`, say the limit is above the tested max and offer a higher range.

## Do not
- Print or log the password, API tokens or `setup/secrets/*`.
- Delete anything that is not listed in `setup/resources.json` (use `cleanup --session`).
- Re-run `rag_write` against a real knowledge base: every request creates a document. It always uses a fresh disposable dataset.
- Treat one passing stage as the stand's maximum.

## Errors
| Message | Meaning / action |
|---|---|
| `login ... HTTP 401` | wrong email/password, or password login disabled (SSO-only): ask for a local admin account |
| `TLS certificate verification failed` | pass `--ca-file`; never disable verification |
| `PreflightError: ... Service API` | Service API of the app is disabled or the key is not bound; rerun, report if it repeats |
| `app import failed` | DSL rejected (version/plugin dependency); use the built-in echo DSL |
| `cleanup failed` | run `cleanup --session <dir> --url ... --email ...` |
| stage verdict `INCONCLUSIVE_GENERATOR` | load generator could not hold the rate; run from a stronger host or lower `--max-rate` |
| many `HttpError` 429/5xx at low rate | stand is saturated or rate-limited; record it as the finding, do not retry in a loop |

Result interpretation: `CAPACITY_REPORT_GUIDE_RU.md`. Engine config details: `MULTI_NAMESPACE_CAPACITY_GUIDE_RU.md`.
