#!/usr/bin/env python3
"""Read latest daily edge-quality SLO report status across schema variants."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

INCLUDE_GLOB = "edge_quality_daily_slo_*.json"
EXCLUDE_PATTERNS = ("*cli_day*.json", "*summary*.json", "*helper*.json")


def _should_skip(path: Path) -> bool:
    return any(path.match(pattern) for pattern in EXCLUDE_PATTERNS)


def _candidate_reports(reports_dir: Path) -> list[Path]:
    candidates: list[Path] = []
    for path in reports_dir.rglob(INCLUDE_GLOB):
        if _should_skip(path):
            continue
        candidates.append(path)
    return sorted(candidates, key=lambda item: item.stat().st_mtime, reverse=True)


def _normalize_status(value: Any) -> str:
    status = str(value or "").strip().lower()
    if status in {"pass", "ok", "healthy"}:
        return "pass"
    if status in {"fail", "error", "degraded"}:
        return "fail"
    if status in {"insufficient_sample", "insufficient", "unknown", "n/a"}:
        return "insufficient_sample"
    return status


def _derive_from_windows(windows: Any) -> tuple[str | None, dict[str, int]]:
    if not isinstance(windows, list):
        return None, {}

    counts = {"pass": 0, "fail": 0, "insufficient_sample": 0, "unknown": 0}
    for window in windows:
        if not isinstance(window, dict):
            counts["unknown"] += 1
            continue
        verdict = _normalize_status(window.get("verdict"))
        if not verdict:
            verdict = _normalize_status(window.get("status"))
        if verdict in {"pass", "fail", "insufficient_sample"}:
            counts[verdict] += 1
        else:
            counts["unknown"] += 1

    if counts["fail"] > 0:
        derived = "fail"
    elif counts["pass"] > 0:
        derived = "pass"
    elif counts["insufficient_sample"] > 0:
        derived = "insufficient_sample"
    elif counts["unknown"] > 0:
        derived = "unknown"
    else:
        derived = None
    return derived, counts


def _format_counts(counts: dict[str, int]) -> str:
    non_zero = [f"{key}:{value}" for key, value in counts.items() if value > 0]
    return ",".join(non_zero) if non_zero else "none"


def _summarize_payload(path: Path, payload: dict[str, Any]) -> tuple[str, bool]:
    status = _normalize_status(payload.get("status"))
    windows = payload.get("windows")
    schema = "legacy" if isinstance(windows, list) else "unknown"

    if not status:
        gate_verdict = _normalize_status(payload.get("gate_verdict"))
        if gate_verdict:
            status = gate_verdict
            schema = "current"

    if not isinstance(windows, list):
        window_reports = payload.get("window_reports")
        if isinstance(window_reports, list):
            windows = window_reports
            schema = "current"

    derived_status, verdict_counts = _derive_from_windows(windows)
    if not status and derived_status:
        status = derived_status

    if not status:
        return f"{path}: schema warning unable_to_resolve_status_or_windows", True

    window_count = len(windows) if isinstance(windows, list) else "n/a"
    parity_contract_status = _normalize_status(payload.get("parity_contract_status")) or "not_evaluated"
    decisionability_status = _normalize_status(payload.get("decisionability_status"))
    quality_status = _normalize_status(payload.get("quality_status"))
    gate_reason = str(payload.get("gate_reason") or "").strip() or "unknown"

    if not decisionability_status:
        decisionable_window_count = int(payload.get("decisionable_window_count") or 0)
        if gate_reason in {"no_decisionable_windows", "insufficient_sample_floor_not_met_for_all_windows"}:
            decisionability_status = "insufficient_sample"
        else:
            decisionability_status = "pass" if decisionable_window_count > 0 else "insufficient_sample"

    if not quality_status:
        if gate_reason == "decisionable_window_fail_rate_exceeded_threshold":
            quality_status = "fail"
        elif decisionability_status == "insufficient_sample":
            quality_status = "insufficient_sample"
        else:
            quality_status = "pass"

    operator_reason = str(payload.get("operator_composite_reason") or "").strip()
    if not operator_reason:
        if parity_contract_status in {"fail", "insufficient_sample"}:
            operator_reason = f"parity_contract_blocker:{parity_contract_status}"
        elif decisionability_status != "pass":
            operator_reason = f"decisionability_blocker:{gate_reason}"
        elif quality_status != "pass":
            operator_reason = f"quality_blocker:{gate_reason}"
        else:
            operator_reason = "all_components_passing"

    source = "reported" if "status" in payload or "gate_verdict" in payload else "derived"
    return (
        f"{path}: status={status} windows={window_count} schema={schema} source={source} "
        f"verdicts={_format_counts(verdict_counts)} parity_contract_status={parity_contract_status} "
        f"decisionability_status={decisionability_status} quality_status={quality_status} "
        f"operator_reason={operator_reason}",
        False,
    )


def read_status_lines_with_health(reports_dir: Path, limit: int = 1) -> tuple[list[str], bool]:
    lines: list[str] = []
    has_invalid_payload = False

    for path in _candidate_reports(reports_dir)[: max(1, limit)]:
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except Exception as exc:
            lines.append(f"{path}: schema warning invalid_json error={exc}")
            has_invalid_payload = True
            continue

        if not isinstance(payload, dict):
            lines.append(f"{path}: schema warning payload_not_object")
            has_invalid_payload = True
            continue

        summary, is_invalid = _summarize_payload(path, payload)
        lines.append(summary)
        has_invalid_payload = has_invalid_payload or is_invalid

    if not lines:
        lines.append(f"{reports_dir}: no reports found matching {INCLUDE_GLOB}")

    return lines, has_invalid_payload


def read_status_lines(reports_dir: Path, limit: int = 1) -> list[str]:
    lines, _ = read_status_lines_with_health(reports_dir, limit=limit)
    return lines


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Read latest edge-quality daily SLO report status across schema variants.",
    )
    parser.add_argument(
        "--reports-dir",
        default="reports",
        help="Directory containing edge_quality_daily_slo_*.json artifacts (default: reports).",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=1,
        help="Number of latest matching reports to print (default: 1).",
    )
    args = parser.parse_args()

    lines, has_invalid_payload = read_status_lines_with_health(Path(args.reports_dir), limit=args.limit)
    for line in lines:
        print(line)
    return 1 if has_invalid_payload else 0


if __name__ == "__main__":
    raise SystemExit(main())
