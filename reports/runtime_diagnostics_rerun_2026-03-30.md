# Runtime diagnostics rerun — 2026-03-30

Commands rerun:

- `scripts/scan_runtime_diagnostics.sh ./exports_live`
- `python3 scripts/runtime_diagnostics_summary.py ./exports_live`

## Observed summary (rerun)

- Daily status line: `daily status (unknown) Runs completed=0, Runs degraded=0, Odds not actionable yet=0, Signals produced=0`
- Watchdog trend line: `watchdog_trend none`
- Warnings line: `warnings bounded_stage_counter_invariant_exceeded:runs=1;run_health_no_matches_from_odds:runs=1`

## Batch-date sanity check

- `exports_live/runtime_export_manifest.json` still reports `generated_at_utc: 2026-03-20T20:59:09.455868+00:00`.
- Manifest file entries also show `modified_at_utc` on `2026-03-20`.
- Runtime sample data in `Run_Log.json`/`State.json` remains test-style historical content (e.g., run_id `test-run`, event timestamps around `2025-03-01`), not a March 30 production batch.

## Comparison conclusion vs prior March 28 residual concern

This rerun does **not** demonstrate a March 30 batch refresh. The exported inputs currently look older than March 28 and remain anchored to the same residual/test-era dataset. Any triage generated from this rerun should be treated as **not yet based on a March 30 batch** until `exports_live` is refreshed from the intended post-Issue-A source and rerun again.
