#!/usr/bin/env python3
"""CI guard: ensure compare workflow docs/scripts consistently route through export preflight."""

from __future__ import annotations

from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
DOC_PATHS = [REPO_ROOT / "README.md", REPO_ROOT / "runbook" / "README.md"]
WRAPPER_PATHS = [
    REPO_ROOT / "scripts" / "compare_run_diagnostics_preflight.sh",
    REPO_ROOT / "scripts" / "compare_run_metrics_preflight.sh",
]
PREFLIGHT_CMD = "scripts/export_parity_precheck.sh"
COMPARE_TOKENS = (
    "python3 scripts/compare_run_diagnostics.py",
    "python3 scripts/compare_run_metrics.py",
)


def _check_docs() -> list[str]:
    failures: list[str] = []
    for path in DOC_PATHS:
        text = path.read_text(encoding="utf-8")
        if PREFLIGHT_CMD not in text:
            failures.append(f"{path}: missing {PREFLIGHT_CMD} reference")
            continue

        lines = text.splitlines()
        for idx, line in enumerate(lines):
            stripped = line.strip()
            if not any(token in stripped for token in COMPARE_TOKENS):
                continue
            lookback = "\n".join(lines[max(0, idx - 8) : idx + 1])
            if PREFLIGHT_CMD not in lookback and "_preflight.sh" not in lookback:
                failures.append(
                    f"{path}: compare command on line {idx + 1} not gated by preflight context"
                )
    return failures


def _check_wrappers() -> list[str]:
    failures: list[str] = []
    for path in WRAPPER_PATHS:
        if not path.exists():
            failures.append(f"{path}: missing wrapper script")
            continue
        text = path.read_text(encoding="utf-8")
        if PREFLIGHT_CMD not in text:
            failures.append(f"{path}: wrapper missing {PREFLIGHT_CMD} call")
    return failures


def main() -> int:
    failures = [*_check_docs(), *_check_wrappers()]
    if failures:
        print("preflight_reference_gate: FAIL")
        for failure in failures:
            print(f"- {failure}")
        return 1
    print("preflight_reference_gate: PASS")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
