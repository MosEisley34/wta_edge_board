#!/usr/bin/env python3
"""CI guard: ensure compare workflow docs/scripts consistently route through export preflight."""

from __future__ import annotations

from pathlib import Path
import re

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
DIRECT_COMPARE_PATTERN = re.compile(
    r"(?<![_\w-])(?:python3\s+)?scripts/compare_run_(?:diagnostics|metrics)\.py\b"
)
PRECHECK_CONTEXT_PATTERN = re.compile(
    r"(?:scripts/export_parity_precheck\.sh|scripts/compare_run_(?:diagnostics|metrics)_preflight\.sh)"
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
        failures.extend(_check_doc_code_fences_for_direct_compare(path, text))
    return failures


def _check_doc_code_fences_for_direct_compare(path: Path, text: str) -> list[str]:
    failures: list[str] = []
    lines = text.splitlines()
    in_code_fence = False
    fence_start = 0
    block_lines: list[tuple[int, str]] = []

    for idx, raw_line in enumerate(lines, start=1):
        stripped = raw_line.strip()
        if stripped.startswith("```"):
            if in_code_fence:
                failures.extend(_evaluate_code_block(path, fence_start, block_lines))
                in_code_fence = False
                block_lines = []
                fence_start = 0
            else:
                in_code_fence = True
                fence_start = idx
            continue
        if in_code_fence:
            block_lines.append((idx, raw_line))
    if in_code_fence:
        failures.extend(_evaluate_code_block(path, fence_start, block_lines))
    return failures


def _evaluate_code_block(path: Path, fence_start: int, block_lines: list[tuple[int, str]]) -> list[str]:
    failures: list[str] = []
    if not block_lines:
        return failures
    block_text = "\n".join(line for _, line in block_lines)
    if not DIRECT_COMPARE_PATTERN.search(block_text):
        return failures
    if PRECHECK_CONTEXT_PATTERN.search(block_text):
        return failures
    first_direct_line = next(
        (line_no for line_no, line in block_lines if DIRECT_COMPARE_PATTERN.search(line)),
        fence_start,
    )
    failures.append(
        f"{path}: fenced example starting line {fence_start} has direct compare invocation "
        f"without preflight wrapper/context (first direct compare at line {first_direct_line})"
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
