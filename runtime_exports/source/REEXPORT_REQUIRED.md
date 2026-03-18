# Runtime export status (2026-03-18)

Checked the working inputs requested for diagnostics:

- `/home/carlos/Apps/wta_edge_model/wta_edge_board/runtime_exports/source/Run_Log.json`
- `/home/carlos/Apps/wta_edge_model/wta_edge_board/runtime_exports/source/State.json`

The absolute path above does not exist in this environment, so validation was run against the repo-local files at:

- `runtime_exports/source/Run_Log.json`
- `runtime_exports/source/State.json`

## Command outputs

```bash
jq -r '.[].run_id' runtime_exports/source/Run_Log.json | sort -u
# => test-run

jq -r '.[].key' runtime_exports/source/State.json | sort -u
# => LAST_RUN_COMPETITION_DIAGNOSTICS_JSON
# => LAST_RUN_VERBOSE_JSON
# => ODDS_OPENING_LAG_GATING_STATE
# => ODDS_REFRESH_MODE_META
# => PRODUCTIVE_OUTPUT_MITIGATION_STATE
# => RUN_CHECKPOINT_1734070671
# => RUN_ROLLUP_STATE
```

## Conclusion

These files currently contain fixture-style data (`test-run`) rather than production-like run IDs (for example `20260318T...`).

A live Google Sheet export is required to overwrite these source files with production-like data before further runtime diagnostics.
