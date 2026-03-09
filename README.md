# wta_edge_board

## Secrets rotation and safe usage

### 1) Rotate/revoke The Odds API key
1. Sign in to The Odds API provider dashboard.
2. Revoke the currently active key used by this project.
3. Generate a replacement key.
4. Record rotation time and owner in your team runbook.

> Note: key rotation must be performed in the provider dashboard; it cannot be automated from this Apps Script repo.

### 2) Update local/runtime and CI/CD secrets
After rotating, update every location that stores `ODDS_API_KEY`:

- Google Sheet `Config` tab (`ODDS_API_KEY` row).
- Google Apps Script project properties (if you mirror secrets there).
- CI/CD provider secret variables (for example GitHub Actions secrets like `ODDS_API_KEY` / `THE_ODDS_API_KEY`).

Recommended process:
- Update secrets first.
- Run one manual pipeline execution.
- Verify credits and fetch success.
- Delete any temporary plaintext notes.

### 3) Logging safety
This project includes log sanitization helpers that:
- redact query-string secrets such as `apiKey=...`,
- redact common token/header patterns,
- redact known webhook URL formats,
- redact object fields with sensitive names (`apiKey`, `secret`, `token`, `password`, etc.).

### 4) CLI safety guidelines
Avoid verbose CLI options that can print full URLs containing secrets.

- **Do not use:** `curl -v "https://.../odds?apiKey=REAL_KEY&..."`
- **Prefer:**
  - environment variable injection without echoing the value,
  - masked/filtered logs,
  - command history controls where appropriate.

Example safer pattern:

```bash
curl -sS "https://api.the-odds-api.com/v4/sports" \
  --get \
  --data-urlencode "apiKey=${ODDS_API_KEY}" \
  --data-urlencode "all=true"
```

If you must debug requests, sanitize output before sharing logs.

## Logging verbosity controls

Use the `Config` tab to tune logging detail:

- `VERBOSE_LOGGING` keeps compatibility with existing on/off behavior.
- `LOG_VERBOSITY_LEVEL` provides granular control (`0` to `3`):
  - `0`: minimal logging
  - `1`: stage summaries only
  - `2`: stage summaries + window/coverage diagnostics (recommended for optimization)
  - `3`: includes sampled payload-level diagnostics for deeper debugging

All logs continue to pass through secret redaction before being persisted.


### Matcher counters in logs and dashboards

`stageMatchEvents` now reports explicit counters so operators can quickly distinguish successful pairings from persisted rejections:

- `matched_count`: successful odds ↔ schedule pairings.
- `rejected_count`: odds events that were not paired and were written with a rejection code (for example `no_player_match`).
- `diagnostic_records_written`: optional diagnostic records written to `MATCH_MAP` that should not be interpreted as successful matches.

`matched` and `unmatched` summary fields are derived from these explicit counters rather than total `MATCH_MAP` upsert rows, so a run can clearly show patterns like **`0 matched / N rejected`** while still persisting rejection rows for troubleshooting.

## Runtime diagnostics (local)

Use `scripts/scan_runtime_diagnostics.sh` to scan **only explicit runtime artifacts** (exported Run_Log/State CSV or JSON) for common diagnostic keys:

- `provider_returned_null_features`
- `ta_h2h_empty_table`
- `missing_stats`
- `schedule_enrichment_h2h_missing`

The script intentionally fails fast when no supported artifacts are found in the paths you provide, so it does not fall back to scanning repo source files.

### Usage

```bash
scripts/scan_runtime_diagnostics.sh <file-or-directory> [more paths...]
```

Examples:

```bash
scripts/scan_runtime_diagnostics.sh ./exports/Run_Log.csv
scripts/scan_runtime_diagnostics.sh ./exports/run_logs ./exports/state_dump.json
```

Output includes:
- grouped counts per diagnostic key,
- top matching rows (file + row + preview) for quick local triage.
