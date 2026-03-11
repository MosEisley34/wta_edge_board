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

## Runtime diagnostics triage flow

Use this standard bundle flow so each triage cycle always exports `Run_Log`/`State` CSV/JSON into `./exports` (default) before analysis:

```bash
scripts/run_triage_bundle.sh [--out-dir ./exports] <file-or-directory> [more paths...]
```

Examples:

```bash
scripts/run_triage_bundle.sh ./runtime
scripts/run_triage_bundle.sh --out-dir ./exports ./runtime/Run_Log.csv ./runtime/state_dump.json
```

The wrapper runs `scripts/export_runtime_artifacts.sh` and then invokes `scripts/scan_runtime_diagnostics.sh ./exports` (or your custom `--out-dir`) so diagnostics inputs are consistently prepared before scanning.

Manual 3-step flow is still available when needed:

1. **Export artifacts to a known directory** (`./exports` by default).
2. **Run diagnostics scan/triage** against that export directory.
3. **Interpret grouped counts + row previews** to identify the dominant failure mode.

### 1) Export Run_Log/State artifacts

```bash
scripts/export_runtime_artifacts.sh [--out-dir ./exports] <file-or-directory> [more paths...]
```

Examples:

```bash
scripts/export_runtime_artifacts.sh ./runtime
scripts/export_runtime_artifacts.sh --out-dir ./exports ./runtime/Run_Log.csv ./runtime/state_dump.json
```

This command copies matching `Run_Log`/`State` CSV/JSON files into `./exports` and fails with a clear error when no exportable artifacts are found.

### 2) Run scanner/triage

Local triage wrapper (fails fast if exports are missing):

```bash
scripts/triage_runtime_diagnostics_local.sh [./exports]
```

CI triage wrapper (uses `RUNTIME_EXPORT_DIR` or defaults to `./exports`):

```bash
scripts/triage_runtime_diagnostics_ci.sh
```

Direct scanner usage is still available:

```bash
scripts/scan_runtime_diagnostics.sh ./exports
```

### 3) Interpret expected output

The scan reports:
- **Grouped counts** for each diagnostic key:
  - `provider_returned_null_features`
  - `ta_h2h_empty_table`
  - `missing_stats`
  - `schedule_enrichment_h2h_missing`
- **Top matching rows** (file + row + preview) to quickly locate the first representative examples.

Interpretation guide:
- high `provider_returned_null_features`: upstream source returned rows but usable features were null,
- high `ta_h2h_empty_table`: TA H2H source response resolved to an empty table,
- high `missing_stats`: event/player rows were present but stat fields were absent,
- high `schedule_enrichment_h2h_missing`: schedule rows lacked expected H2H enrichment.


## TA parser parity pre-deploy gate

Use `scripts/run_ta_parity.sh` to verify that Tennis Abstract `matchmx` parsing remains aligned with Apps Script normalization before deploys.

```bash
scripts/run_ta_parity.sh
```

This wrapper automatically targets the canonical leadersource artifact at `tmp/source_probe_latest/raw/tennisabstract_leadersource_wta.body` (with a fallback to `tmp/source_probes/raw/tennisabstract_leadersource_wta.body`).

The script prints:
- total parsed rows,
- unique players,
- non-null coverage for ranking/hold/break at both row-level and normalized player-level,
- sample normalized player records.

It exits non-zero with `parser_parity_regression` when row-level (CLI) coverage is healthy while normalized (Apps Script-like) coverage is poor.

## Player feature extraction artifact

Use `scripts/extract_player_features.py` to normalize probe payloads under `OUT_DIR/raw` into model-ready artifacts:

- `OUT_DIR/normalized/player_features.jsonl`
- `OUT_DIR/normalized/player_features.csv`
- `OUT_DIR/normalized/source_diagnostics.csv`

The extractor keeps model features clean by routing source-level parse/status issues (for example pointer/metadata payloads and hard API errors) into `source_diagnostics.csv` instead of `player_features.csv`.
