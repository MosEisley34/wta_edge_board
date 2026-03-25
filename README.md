# wta_edge_board

## Signal stake policy spec (canonical contract)

This contract defines a single deterministic policy for converting model stake units into emitted signal stake amounts.

### Config schema (`stake_policy`)

```yaml
stake_policy:
  base_unit_size: 1.0
  account_currency: MXN
  minimum_stake_per_currency:
    MXN: 20
  policy_mode: strict_suppress_below_min # strict_suppress_below_min | round_up_to_min
  precision:
    decimals: 2
    rounding_mode: round_half_up
```

Schema field requirements:
- `base_unit_size` (number, `> 0`): multiplier used to convert raw stake units into account-currency value.
- `account_currency` (ISO 4217 string): default currency for emitted signal stake values.
- `minimum_stake_per_currency` (map of ISO 4217 → number): per-currency minimum stake floor. Initial required key: `MXN: 20`.
- `policy_mode` (enum):
  - `strict_suppress_below_min`: suppress signal emission when converted stake is below configured minimum.
  - `round_up_to_min`: emit signal and clamp stake to configured minimum when converted stake is below minimum.
- `precision.decimals` (integer): number of decimals used in final emitted stake (`2` required).
- `precision.rounding_mode` (enum): `round_half_up` required for deterministic rounding.

### Conversion, validation, and emission contract

1. **Convert**
   - Compute `raw_stake = signal_stake_units * base_unit_size` in `account_currency`.
2. **Normalize precision**
   - Compute `normalized_stake` by rounding `raw_stake` to `precision.decimals` using `round_half_up`.
3. **Validate minimum policy**
   - Resolve `currency_minimum = minimum_stake_per_currency[account_currency]`.
   - If no currency minimum exists, treat as config error and suppress signal.
   - If `normalized_stake >= currency_minimum`, emit unchanged.
   - If `normalized_stake < currency_minimum`, apply `policy_mode`:
     - `strict_suppress_below_min`: suppress signal.
     - `round_up_to_min`: set emitted stake to `currency_minimum`.
4. **Emit fields** (when signal is emitted)
   - `recommended_stake`: final post-policy stake rounded/represented with 2 decimals.
   - `recommended_stake_currency`: equals `account_currency`.
   - `min_stake_applied`: boolean (`true` only when floor/clamp applied).
   - `stake_policy_decision_reason`: deterministic reason code:
     - `at_or_above_min_emit`
     - `below_min_suppressed_strict`
     - `below_min_rounded_up`
     - `stake_policy_config_error`

### Canonical fixture

Use `scripts/fixtures/stake_policy_mxn20.json` as the canonical test and script fixture for this contract.

## Troubleshooting: menu missing after open

If the custom menu does not appear (for example due to simple-trigger binding/deployment mismatch), use this recovery flow:

* Open **Extensions → Apps Script**
* Run `rebuildMenuNow` once from the editor
* Reload spreadsheet and verify menu presence

### Parser cleanup loop for "already declared" errors

When the Apps Script editor reports parser-level duplicate declaration errors, use this repeatable loop:

1. Save all files in Apps Script.
2. Run `onOpen`.
3. If a new `Identifier 'X' has already been declared` error appears, search that exact identifier with:

   ```regex
   ^(const|let|var)\s+<IDENTIFIER>\b
   ```

4. Remove duplicated declarations and repeat until no syntax errors remain.
5. When parser errors are clear, refresh the spreadsheet and verify the **WTA Edge Board** custom menu appears.

## Apps Script file reconciliation (live project vs repo)

Use `scripts/apps_script_reconcile.py` to compare the live Apps Script project file list against canonical repo modules and optionally remove stale modules from the live project.

Dry-run comparison (no live writes):

```bash
python3 scripts/apps_script_reconcile.py \
  --script-id "${APPS_SCRIPT_ID}" \
  --access-token "${GOOGLE_ACCESS_TOKEN}"
```

Apply cleanup in the live project:

```bash
# delete stale SERVER_JS files from the live project
python3 scripts/apps_script_reconcile.py \
  --script-id "${APPS_SCRIPT_ID}" \
  --access-token "${GOOGLE_ACCESS_TOKEN}" \
  --apply-delete

# or archive stale SERVER_JS files (prefixes with ARCHIVE_YYYYMMDD_)
python3 scripts/apps_script_reconcile.py \
  --script-id "${APPS_SCRIPT_ID}" \
  --access-token "${GOOGLE_ACCESS_TOKEN}" \
  --apply-archive
```

The script reports:
- duplicate/legacy files present only in live Apps Script,
- near-duplicate module names (for example `constant` vs `constants`),
- canonical modules missing in the live Apps Script project,
- duplicate top-level globals in canonical repo modules.

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

- `LOG_PROFILE` is the primary switch (`compact` or `verbose`).
  - `compact` (default): optimized for smaller runtime artifacts; caps effective verbosity at `0-1` and stores a compact `LAST_RUN_VERBOSE_JSON` summary instead of full diagnostics.
  - `verbose`: preserves the previous high-fidelity behavior, including full `LAST_RUN_VERBOSE_JSON` payloads and `LOG_VERBOSITY_LEVEL` up to `3`.
- `VERBOSE_LOGGING` remains for backward compatibility in verbose mode.
- `LOG_VERBOSITY_LEVEL` provides granular control (`0` to `3`) when `LOG_PROFILE=verbose`:
  - `0`: minimal logging
  - `1`: stage summaries only
  - `2`: stage summaries + window/coverage diagnostics (previous default behavior)
  - `3`: includes sampled payload-level diagnostics for deeper debugging

Expected impact:
- `compact`: lower log/state write volume and reduced serialization overhead, with less detail for incident deep-dives.
- `verbose`: higher diagnostics fidelity with larger log payloads and higher write/processing cost.

All logs continue to pass through secret redaction before being persisted.

## Config preset: Free-tier conservation

Use this preset when you want to stay inside lower Odds API daily budgets with graceful degradation as credits run down.

You can apply it from the spreadsheet menu:

- `WTA Edge Board` → `Apply Preset: Free-Tier Conservation`

Preset values written to the `Config` tab:

- `PIPELINE_TRIGGER_EVERY_MIN=30` (optionally raise to `60` for stricter conservation)
- `ODDS_REFRESH_TIER_HIGH_INTERVAL_MIN=15`
- `ODDS_REFRESH_TIER_MED_INTERVAL_MIN=30`
- `ODDS_REFRESH_TIER_LOW_INTERVAL_MIN=60`
- `ODDS_WINDOW_PRE_FIRST_MIN=60`
- `ODDS_WINDOW_POST_LAST_MIN=60`
- `ODDS_BOOTSTRAP_LOOKAHEAD_HOURS=6`
- `ODDS_MIN_CREDITS_SOFT_LIMIT=150`
- `ODDS_MIN_CREDITS_HARD_LIMIT=75`

Expected daily call budget target (typical):

- `PIPELINE_TRIGGER_EVERY_MIN=30`: target roughly **60–120 calls/day**.
- `PIPELINE_TRIGGER_EVERY_MIN=60`: target roughly **30–80 calls/day**.

These ranges assume typical WTA slate density and that runtime caching/window logic suppresses unnecessary refreshes.

Credit fallback behavior:

- **Above soft limit** (`remaining > ODDS_MIN_CREDITS_SOFT_LIMIT`): normal cadence.
- **Soft-limit mode** (`remaining <= ODDS_MIN_CREDITS_SOFT_LIMIT`): degraded polling to conserve credits.
  - doubles odds cache/refresh windows,
  - doubles tier refresh intervals,
  - disables non-critical schedule refresh,
  - disables match-time fallback expansion.
- **Hard-limit mode** (`remaining <= ODDS_MIN_CREDITS_HARD_LIMIT`): odds calls are skipped for the run (`credit_hard_limit_skip_odds`) until credits recover.


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

The wrapper runs `scripts/prepare_runtime_exports.sh` and then invokes `scripts/scan_runtime_diagnostics.sh ./exports` (or your custom `--out-dir`) so diagnostics inputs are consistently prepared before scanning.
`prepare_runtime_exports.sh` now enforces a single-source Run_Log snapshot contract: it re-exports `Run_Log.csv` and `Run_Log.json` together from the same latest source snapshot and applies one export batch timestamp before parity checks.

First-pass diagnosis should always start with the scanner's **Run-health degraded contract (first-pass triage)** section. It standardizes degraded run blocker counts, dominant blocker categories, sampled blocked records, and stage-skipped reason rollups before key-specific deep dives.

Manual 3-step flow is still available when needed:

### Run-ID precheck gate before comparison scripts (standard operator flow)

Before any compare/gate command, always run this single wrapper command first:

```bash
scripts/export_parity_precheck.sh --out-dir ./exports_live <run_id_a> <run_id_b> <live_runtime_dir_or_files>
```

Examples:

```bash
scripts/export_parity_precheck.sh --out-dir ./exports_live 20260320T095837_9ccfe02e 20260319T184422_4bcd8a9f ./live_runtime
scripts/export_parity_precheck.sh --out-dir ./exports_live 20260320T095837_9ccfe02e 20260319T184422_4bcd8a9f ./live_runtime/Run_Log.csv ./live_runtime/Run_Log.json ./live_runtime/State.json
```

Do not run compare/gate scripts against ad-hoc inputs before this wrapper succeeds.

Behavior contract:
- scans `./exports_live/*Run_Log*.json` and `./exports_live/*Run_Log*.csv` (recursive),
- confirms both target run IDs are present,
- when both JSON and CSV sources exist, requires each target run ID in **both** sources (strict mixed-source integrity),
- exits non-zero when either run ID is missing.

Failure contract:
- emits `run_id_source_mismatch` with per-run details (`csv_present=<bool> json_present=<bool>`) when mixed-source integrity fails.

Emergency override (incident-only):
- `--allow-csv-only-triage` allows CSV-present/JSON-missing run IDs to proceed for emergency triage, but now requires `--incident-tag <LETTERS-NNN>` (example: `INC-1234`),
- precheck prints `degraded_confidence_csv_only_triage` so downstream readers know confidence is reduced.

If wrapper parity or precheck fails, **stop triage immediately and re-export before further analysis**.
Only proceed once both run IDs are confirmed present in parity-checked exports.

Downstream contract after wrapper success:
- Use only `./exports_live` outputs for all follow-up commands (`precheck_run_ids.py`, `verify_run_log_parity.py`, `compare_*`, `evaluate_edge_quality.py`, coverage gates).
- Do **not** mix stale `live_runtime/Run_Log.json` with fresh `Run_Log.csv` in operator workflows or examples.

### Player-stats coverage gate (runs after precheck, before verdict publication)

Run the player-stats gate immediately after precheck and before any comparison/verdict script output is published:

```bash
python3 scripts/check_player_stats_coverage.py <run_id_baseline> <run_id_candidate> --export-dir ./exports_live
```

Default gate thresholds:
- `player_stats_resolved_rate` (candidate resolved/requested) must be `>= 0.60`,
- candidate `unresolved_player_count` must be `<= 8`,
- `STATS_MISS_A` and `STATS_MISS_B` may not increase above baseline (`max increase = 0` per side).

Threshold overrides (when explicitly approved for an incident) are available via flags or env vars:
- `--min-resolved-rate` / `PLAYER_STATS_MIN_RESOLVED_RATE`
- `--max-unresolved-players` / `PLAYER_STATS_MAX_UNRESOLVED_PLAYERS`
- `--max-missing-side-increase` / `PLAYER_STATS_MAX_MISSING_SIDE_INCREASE`
- `--override-reason` / `PLAYER_STATS_COVERAGE_GATE_OVERRIDE` (non-empty value records an override and allows exit 0).

Comparison scripts are now wired to enforce this same gate by default before they print reports. To bypass for emergency/manual debugging only, use:
- `--skip-player-stats-coverage-gate`, or
- `--player-stats-gate-override-reason <incident-reference>`.

Comparison scripts also enforce preflight sidecar parity for the current export batch (`run_compare_preflight.json` + `runtime_export_manifest.json`). If sidecar/manifest proves preflight was not run for the current batch, compare scripts exit non-zero.
`run_compare_preflight.json` is required evidence for every comparison report; include it alongside diagnostics/metrics output artifacts in incident or release notes.

Emergency-only preflight bypass requires an explicit incident tag:
- `--emergency-preflight-override-tag <LETTERS-NNN>` (example: `INC-1234`).

Comparison workflows should use the preflight wrappers so command paths always start with preflight:

```bash
scripts/compare_run_diagnostics_preflight.sh --out-dir ./exports_live <run_success> <run_degraded> <live_runtime_dir_or_files>
scripts/compare_run_metrics_preflight.sh --out-dir ./exports_live <run_success> <run_degraded> <live_runtime_dir_or_files>
```

Operator/team usage policy:
- Do not run `scripts/compare_run_diagnostics.py` or `scripts/compare_run_metrics.py` directly in runbook examples.
- Route compare commands through the corresponding `_preflight.sh` wrapper.

Avoid legacy/incorrect flag patterns such as `--run-log` or `--require`; run IDs are positional arguments.


1. **Export artifacts to a known directory** (`./exports` by default).
2. **Run diagnostics scan/triage** against that export directory.
3. **Interpret grouped counts + row previews** to identify the dominant failure mode.

### 1) Repeatable pre-step: export + validate expected files

```bash
scripts/prepare_runtime_exports.sh [--out-dir ./exports] <file-or-directory> [more paths...]
```

Examples:

```bash
scripts/prepare_runtime_exports.sh ./runtime
scripts/prepare_runtime_exports.sh --out-dir ./exports ./runtime/Run_Log.csv ./runtime/state_dump.json
```

This pre-step copies matching `Run_Log`/`State` CSV/JSON files into `./exports` and then validates exports before scanning.
Parity output also writes `./exports/run_log_latest_batch_note.json` so operators can verify the latest batch `run_id` set is identical across CSV and JSON.

Preferred diagnostics artifact source order:
- `Run_Log.csv`
- `Run_Log.json` (fallback when CSV is unavailable)
- `State.csv`
- `State.json` **only** when it is object/record JSON (not list-style key/value dumps)

`State.json` schema expectation for scanner compatibility:
- preferred shape: a JSON object or NDJSON/object records with runtime diagnostic fields (for example `stage`, `message`, `reason_code`),
- unsupported shape: list-style state export (`[{"key":"...","value":"..."}]`), which is now ignored with a warning and scanner falls back to `State.csv` when present.

Expected files (at least one must exist):
- `./exports/*Run_Log*.csv`
- `./exports/*Run_Log*.json`
- `./exports/*State*.csv`
- `./exports/*State*.json`

If these are missing, the script fails early with remediation instructions to rerun export with paths containing Run_Log/State artifacts before invoking the scanner.

### 2) Run scanner/triage

Local triage wrapper (fails fast if exports are missing):

```bash
scripts/triage_runtime_diagnostics_local.sh [./exports]
```

CI triage wrapper (uses `RUNTIME_EXPORT_DIR` or defaults to `./exports`):

```bash
scripts/triage_runtime_diagnostics_ci.sh
```

### CI deterministic profile + parity gate

Run this CI gate after runtime artifact export/diagnostics to enforce compact-profile quality constraints:

```bash
scripts/ci_profile_parity_gate.sh
```

The gate fails when either condition is true:
- compact reduction is below `PROFILE_REDUCTION_TARGET_PCT` (default `60`), or
- any configured critical parity diagnostics (`PROFILE_CRITICAL_PARITY_KEYS`, default `gate_reasons,source_selection,watchdog`) has mismatches.

It also writes a machine-readable summary artifact JSON to `PROFILE_SUMMARY_JSON_OUT` (default `./exports/pipeline_log_profile_ci_summary.json`) for trend tracking.

Run rollups now emit a stage-latency contract (`run_rollup_v2`) with healthy/degraded threshold ranges, anomaly reason codes when stage `avg`/`p95` exceed thresholds, and periodic baseline comparison artifacts persisted to State as `LAST_RUN_BASELINE_COMPARISON_JSON` for trend monitoring.

`triage_runtime_diagnostics_ci.sh` now runs this gate by default after diagnostics scan (`RUN_PROFILE_PARITY_GATE=1`), and it can be disabled with `RUN_PROFILE_PARITY_GATE=0` when needed.

Direct scanner usage is still available:

```bash
scripts/scan_runtime_diagnostics.sh ./exports
```

Compact summary usage (paste-friendly for chat/incident updates):

```bash
scripts/runtime_diagnostics_summary.py ./exports
```

This emits exactly 7 deterministic lines:
- run count + status breakdown,
- daily status snapshot with business-friendly labels (`Runs completed`, `Runs degraded`, `Odds not actionable yet`, `Signals produced`),
- a short `What changed since yesterday` delta block,
- top non-zero reason codes,
- stage duration min/avg/p95,
- watchdog start/end trend delta,
- key operational warnings.

Tuning flags:
- `--top-n` (default `6`),
- `--max-stages` (default `8`),
- `--warning-limit` (default `4`).

For periodic planning/postmortem analysis, generate historical aggregate snapshots (without copying raw runtime logs) into a dedicated rollup directory. These snapshots now include a `daily_status` section plus `what_changed_since_yesterday` deltas for quick day-over-day review:

```bash
scripts/runtime_periodic_aggregates.py ./exports --snapshot-dir ./docs/baselines/runtime_rollups
```

Snapshot artifacts are date-stamped as:
- `docs/baselines/runtime_rollups/runtime_periodic_rollup_YYYY-MM-DD.json`


### 3) Interpret expected output

Start with **Run-health degraded contract (first-pass triage)**:
- confirm degraded records all report the same `run_health_contract_version` (current fixed schema: `v2`),
- confirm **contract field gaps** is `none` so every degraded run includes the same first-pass contract fields (`blocker_counts`, `dominant_blocker_categories`, `sampled_blocked_records`, `stage_skipped_reason_counts`, plus run-level metadata),
- review aggregate blocker counts to identify the largest suppression/failure bucket,
- inspect dominant blocker categories to identify where losses concentrate,
- use sampled blocked records to quickly anchor investigation in real event IDs,
- read stage-skipped reason rollups to verify which skip/blocked reasons dominate.

Then use the key-specific diagnostics section:

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

## Edge quality gate thresholds by run-volume tier

Use `scripts/evaluate_edge_quality.py` with volume-aware thresholds so low-sample runs emit soft warnings instead of false hard failures.

Legacy-schema handling contract:

- Runs emitted with older feature-contract shapes (legacy `schema_id`, non-canonical `stage_summaries`, or missing required feature-contract fields) are classified as `legacy_schema_insufficient_feature_contract`.
- These rows are reported explicitly via warning entries prefixed with `legacy_schema_insufficient_feature_contract` instead of the generic `missing_feature_completeness_metric` hard fail.
- Modern rows continue to enforce `--min-feature-completeness` exactly as before.

Recommended operating tiers:

- **Low volume / sparse windows**
  - `--min-scored-signals-for-volatility=10`
  - `--min-matched-events-for-volatility=5`
  - Expected behavior: if either threshold is not met, status returns `insufficient_sample` and skips volatility hard-fail enforcement.
- **Normal volume (default daily operations)**
  - `--max-edge-volatility=0.03`
  - `--min-scored-signals-for-volatility=20`
  - `--min-matched-events-for-volatility=10`
  - `--volatility-context-min-pairs=4`
  - `--volatility-context-quantile=0.90`
  - `--volatility-context-ceiling-factor=1.25`
- **High volume / major-event windows**
  - `--min-scored-signals-for-volatility=30`
  - `--min-matched-events-for-volatility=15`
  - Keep context-window recalibration enabled and consider tightening `--max-edge-volatility` to `0.02-0.025` when historical variance supports it.

Context-window recalibration contract:

- Volatility ceiling is recalculated from run summaries in the same `(tournament, time_block)` context.
- Effective ceiling = `max(configured_ceiling, quantile(context_volatility) * ceiling_factor)`.
- Keep `--volatility-context-min-pairs` at `4+` so the adaptive ceiling reflects a stable window pair set.

## Daily production edge-quality SLO gate

Run a single daily SLO job against `./exports_live/Run_Log.csv` to evaluate rolling pair quality for the last **3** and **7** days with minimum volume constraints:

```bash
scripts/run_daily_edge_quality_slo.sh
```

Default behavior:
- evaluates rolling windows `3,7` days,
- marks a window as `decisionable` only when `pair_count >= 10`,
- tracks status counts as `pass`, `fail`, `insufficient_sample`,
- computes window fail-rate as `fail / pair_count`,
- fails the daily gate when any decisionable window exceeds `--fail-rate-threshold` (`0.15` by default),
- writes a timestamped report to `reports/edge_quality_daily_slo_<timestamp>.json`,
- appends a trend-baseline summary row to `docs/baselines/edge_quality_slo/edge_quality_daily_slo_summary.jsonl`.

Manual invocation:

```bash
python3 scripts/evaluate_edge_quality.py ./exports_live/Run_Log.csv \
  --daily-slo \
  --daily-slo-windows 3,7 \
  --daily-slo-min-pairs 10 \
  --daily-slo-fail-rate-threshold 0.15 \
  --daily-slo-output-dir reports \
  --daily-slo-archive-dir docs/baselines/edge_quality_slo
```
