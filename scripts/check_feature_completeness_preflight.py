#!/usr/bin/env python3
"""Early completeness gate for compare orchestration workflows."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

from evaluate_edge_quality import EdgeQualityGateConfig, _snapshot, load_run_log_rows

REASON_CODE = "FEATURE_COMPLETENESS_BELOW_FLOOR"


def _build_report(
    *,
    baseline_run_id: str,
    candidate_run_id: str,
    baseline_feature_completeness: float | None,
    candidate_feature_completeness: float | None,
    floor: float,
    candidate_diagnostics: dict[str, Any],
    should_skip_downstream: bool,
    force_full_compare: bool,
) -> dict[str, Any]:
    status = "pass"
    if should_skip_downstream and force_full_compare:
        status = "override"
    elif should_skip_downstream:
        status = "fail"

    report: dict[str, Any] = {
        "schema": "wta_edge_board.feature_completeness_preflight.v1",
        "status": status,
        "reason_code": REASON_CODE if should_skip_downstream else "OK",
        "baseline_run_id": baseline_run_id,
        "candidate_run_id": candidate_run_id,
        "baseline_feature_completeness": baseline_feature_completeness,
        "candidate_feature_completeness": candidate_feature_completeness,
        "min_feature_completeness_floor": floor,
        "candidate_feature_reason_code": str(
            candidate_diagnostics.get("feature_completeness_reason_code") or ""
        ),
        "should_skip_downstream": bool(should_skip_downstream and not force_full_compare),
        "force_full_compare": bool(force_full_compare),
    }
    return report


def run_gate(
    export_dir: str,
    baseline_run_id: str,
    candidate_run_id: str,
    min_feature_completeness: float,
    force_full_compare: bool,
) -> tuple[int, dict[str, Any]]:
    rows = load_run_log_rows(export_dir)
    config = EdgeQualityGateConfig(min_feature_completeness=min_feature_completeness)
    baseline = _snapshot(rows, baseline_run_id, config)
    candidate = _snapshot(rows, candidate_run_id, config)

    candidate_value = candidate.get("feature_completeness")
    below_floor = False
    if candidate_value is None:
        below_floor = True
    else:
        try:
            below_floor = float(candidate_value) < float(min_feature_completeness)
        except (TypeError, ValueError):
            below_floor = True

    report = _build_report(
        baseline_run_id=baseline_run_id,
        candidate_run_id=candidate_run_id,
        baseline_feature_completeness=baseline.get("feature_completeness"),
        candidate_feature_completeness=candidate_value,
        floor=min_feature_completeness,
        candidate_diagnostics=candidate.get("diagnostics") or {},
        should_skip_downstream=below_floor,
        force_full_compare=force_full_compare,
    )

    if below_floor and not force_full_compare:
        return 3, report
    return 0, report


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Gate orchestration early when candidate runEdgeBoard feature_completeness "
            "is missing/non-numeric/below floor."
        )
    )
    parser.add_argument("--export-dir", required=True)
    parser.add_argument("--baseline-run-id", required=True)
    parser.add_argument("--candidate-run-id", required=True)
    parser.add_argument("--min-feature-completeness", type=float, default=0.60)
    parser.add_argument("--force-full-compare", action="store_true")
    parser.add_argument("--report-out", default="")
    args = parser.parse_args(argv)

    exit_code, report = run_gate(
        export_dir=args.export_dir,
        baseline_run_id=args.baseline_run_id,
        candidate_run_id=args.candidate_run_id,
        min_feature_completeness=args.min_feature_completeness,
        force_full_compare=args.force_full_compare,
    )

    payload = json.dumps(report, sort_keys=True)
    print(payload)
    if args.report_out:
        out_path = Path(args.report_out)
        out_path.parent.mkdir(parents=True, exist_ok=True)
        out_path.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")

    return exit_code


if __name__ == "__main__":
    raise SystemExit(main())
