# Runtime diagnostics runbook

## Standard triage bundle (recommended)

Run this wrapper each triage cycle so runtime diagnostics inputs are prepared consistently before analysis:

```bash
scripts/run_triage_bundle.sh [--out-dir ./exports] <file-or-directory> [more paths...]
```

What it does:
1. Exports `Run_Log`/`State` CSV/JSON into `./exports` (default) via `scripts/export_runtime_artifacts.sh`.
2. Immediately invokes `scripts/scan_runtime_diagnostics.sh ./exports` (or your custom `--out-dir`).

## Manual usage flow (optional)

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
