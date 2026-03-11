# Runtime diagnostics runbook

## Usage flow

1. Export runtime artifacts (`Run_Log`/`State` CSV or JSON) into the known export directory (`./exports` by default):

```bash
scripts/export_runtime_artifacts.sh [--out-dir ./exports] <file-or-directory> [more paths...]
```

2. Run diagnostics triage:

```bash
scripts/triage_runtime_diagnostics_local.sh [./exports]
```

(For CI, use `scripts/triage_runtime_diagnostics_ci.sh` and optionally set `RUNTIME_EXPORT_DIR`.)

3. Interpret output:

- `Grouped counts` shows frequency per diagnostic key.
- `Top matching rows` shows concrete file/row examples for quick drill-down.
- Prioritize investigation by highest-count key and confirm with row previews.
