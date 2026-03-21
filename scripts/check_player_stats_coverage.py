#!/usr/bin/env python3
"""Gate pre/post run comparisons on player-stats coverage regressions.

This gate is intended to run after run-id precheck and before pre/post verdict publication.
It evaluates baseline vs candidate run IDs from Run_Log artifacts and fails when:
  * resolved coverage rate is below minimum,
  * unresolved players exceed maximum,
  * STATS_MISS_A/STATS_MISS_B increase beyond threshold.
"""

from __future__ import annotations

import argparse
import csv
import glob
import json
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any


@dataclass(frozen=True)
class GateConfig:
    min_resolved_rate: float = 0.60
    max_unresolved_players: int = 8
    max_missing_side_increase: int = 0
    override_reason: str = ""


def _parse_json_like(value: Any, fallback: Any) -> Any:
    if value in (None, ""):
        return fallback
    if isinstance(value, (dict, list)):
        return value
    if isinstance(value, str):
        text = value.strip()
        if not text:
            return fallback
        try:
            return json.loads(text)
        except Exception:
            return fallback
    return fallback


def _iter_run_log_paths(export_dir_or_file: str) -> list[str]:
    candidate_path = Path(export_dir_or_file)
    if candidate_path.is_file():
        lower = candidate_path.name.lower()
        if lower.endswith("run_log.csv") or lower.endswith("run_log.json"):
            return [str(candidate_path)]
        return []

    found: list[str] = []
    seen: set[str] = set()
    for pattern in ("**/*Run_Log*.json", "**/*Run_Log*.csv"):
        for path in glob.glob(os.path.join(export_dir_or_file, pattern), recursive=True):
            norm = os.path.normpath(path)
            if norm in seen:
                continue
            seen.add(norm)
            found.append(norm)
    return sorted(found)


def load_run_log_rows(export_dir_or_file: str) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for path in _iter_run_log_paths(export_dir_or_file):
        if path.lower().endswith(".json"):
            with open(path, "r", encoding="utf-8") as handle:
                payload = json.load(handle)
            if isinstance(payload, list):
                rows.extend(row for row in payload if isinstance(row, dict))
            continue
        with open(path, "r", encoding="utf-8", newline="") as handle:
            rows.extend(dict(row) for row in csv.DictReader(handle))
    return rows


def _pick_run_summary(rows: list[dict[str, Any]], run_id: str) -> dict[str, Any]:
    latest: dict[str, Any] = {}
    for row in rows:
        if str(row.get("run_id") or "") != run_id:
            continue
        if str(row.get("row_type") or "") == "summary" and str(row.get("stage") or "") == "runEdgeBoard":
            latest = row
    return latest


def _pick_int(metadata: dict[str, Any], keys: tuple[str, ...]) -> int:
    for key in keys:
        value = metadata.get(key)
        try:
            return int(float(value))
        except (TypeError, ValueError):
            continue
    return 0


def _metric_reason_codes(summary_row: dict[str, Any]) -> dict[str, int]:
    parsed = _parse_json_like(summary_row.get("reason_codes"), {})
    if not isinstance(parsed, dict):
        return {}
    out: dict[str, int] = {}
    for key in ("STATS_MISS_A", "STATS_MISS_B"):
        try:
            out[key] = int(float(parsed.get(key, 0) or 0))
        except (TypeError, ValueError):
            out[key] = 0
    return out


def _extract_stage_summaries(summary_row: dict[str, Any]) -> list[dict[str, Any]]:
    stage_summaries = _parse_json_like(summary_row.get("stage_summaries"), [])
    if not isinstance(stage_summaries, list):
        stage_summaries = []
    if stage_summaries:
        return [row for row in stage_summaries if isinstance(row, dict)]

    message = _parse_json_like(summary_row.get("message"), {})
    if isinstance(message, dict):
        message_summaries = message.get("stage_summaries")
        if isinstance(message_summaries, list):
            return [row for row in message_summaries if isinstance(row, dict)]
    return []


def _extract_player_stats_stage_summary(summary_row: dict[str, Any]) -> dict[str, Any]:
    for stage in _extract_stage_summaries(summary_row):
        if str(stage.get("stage") or "") == "stageFetchPlayerStats":
            return stage
    return {}


def _extract_player_stats_coverage(summary_row: dict[str, Any]) -> dict[str, int]:
    stage_summary = _extract_player_stats_stage_summary(summary_row)
    metadata = _parse_json_like(stage_summary.get("reason_metadata"), {})
    if not isinstance(metadata, dict):
        metadata = {}
    coverage = _parse_json_like(metadata.get("coverage"), {})
    if not isinstance(coverage, dict):
        coverage = {}

    resolved = max(0, _pick_int(coverage, ("resolved", "resolved_players", "resolved_player_count", "resolved_players_count")))
    requested = max(0, _pick_int(coverage, ("requested", "requested_players", "requested_player_count", "total_player_count", "total_players_count")))
    unresolved_total = max(0, _pick_int(coverage, ("unresolved", "unresolved_players", "unresolved_player_count", "unresolved_players_count")))

    if resolved == 0:
        resolved = max(0, _pick_int(metadata, ("resolved_player_count", "resolved_players_count", "resolved_name_count")))
    if requested == 0:
        requested = max(0, _pick_int(metadata, ("requested_player_count", "total_player_count", "total_players_count", "players_total")))
    if unresolved_total == 0:
        unresolved_total = max(0, _pick_int(metadata, ("unresolved_player_count", "unresolved_players_count", "players_unresolved")))

    if requested == 0:
        requested = resolved + unresolved_total
    if requested > 0 and unresolved_total == 0:
        unresolved_total = max(0, requested - resolved)

    return {
        "resolved": resolved,
        "requested": requested,
        "unresolved_total": unresolved_total,
    }


def _run_snapshot(rows: list[dict[str, Any]], run_id: str) -> dict[str, Any]:
    summary = _pick_run_summary(rows, run_id)
    if not summary:
        raise ValueError(f"Missing runEdgeBoard summary row for run_id={run_id}")
    coverage = _extract_player_stats_coverage(summary)
    resolved = coverage["resolved"]
    requested = coverage["requested"]
    unresolved_total = coverage["unresolved_total"]

    reason_codes = _metric_reason_codes(summary)
    rate = (resolved / requested) if requested else 0.0

    return {
        "run_id": run_id,
        "resolved": resolved,
        "requested": requested,
        "unresolved_total": unresolved_total,
        "resolved_rate": rate,
        "stats_missing_player_a": int(reason_codes.get("STATS_MISS_A", 0)),
        "stats_missing_player_b": int(reason_codes.get("STATS_MISS_B", 0)),
    }


def evaluate_player_stats_gate(
    rows: list[dict[str, Any]],
    baseline_run_id: str,
    candidate_run_id: str,
    config: GateConfig,
) -> dict[str, Any]:
    baseline = _run_snapshot(rows, baseline_run_id)
    candidate = _run_snapshot(rows, candidate_run_id)

    failures: list[str] = []
    if candidate["resolved_rate"] < config.min_resolved_rate:
        failures.append(
            "player_stats_resolved_rate_below_min "
            f"(candidate={candidate['resolved_rate']:.4f} < min={config.min_resolved_rate:.4f})"
        )

    if candidate["unresolved_total"] > config.max_unresolved_players:
        failures.append(
            "unresolved_players_above_max "
            f"(candidate={candidate['unresolved_total']} > max={config.max_unresolved_players})"
        )

    delta_a = candidate["stats_missing_player_a"] - baseline["stats_missing_player_a"]
    delta_b = candidate["stats_missing_player_b"] - baseline["stats_missing_player_b"]
    if delta_a > config.max_missing_side_increase:
        failures.append(
            "stats_missing_player_a_increase_exceeded "
            f"(delta={delta_a} > max_increase={config.max_missing_side_increase})"
        )
    if delta_b > config.max_missing_side_increase:
        failures.append(
            "stats_missing_player_b_increase_exceeded "
            f"(delta={delta_b} > max_increase={config.max_missing_side_increase})"
        )

    override_used = bool(config.override_reason and failures)
    return {
        "status": "pass" if not failures else ("override" if override_used else "fail"),
        "gate_passed": not failures,
        "override_used": override_used,
        "override_reason": config.override_reason if override_used else "",
        "thresholds": {
            "min_resolved_rate": config.min_resolved_rate,
            "max_unresolved_players": config.max_unresolved_players,
            "max_missing_side_increase": config.max_missing_side_increase,
        },
        "baseline": baseline,
        "candidate": candidate,
        "deltas": {
            "stats_missing_player_a": delta_a,
            "stats_missing_player_b": delta_b,
            "resolved_rate_pp": round((candidate["resolved_rate"] - baseline["resolved_rate"]) * 100.0, 2),
            "unresolved_total": candidate["unresolved_total"] - baseline["unresolved_total"],
        },
        "failures": failures,
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Gate pre/post run comparison on player-stats coverage using exported Run_Log artifacts."
        )
    )
    parser.add_argument("baseline_run_id", help="Baseline/healthy run ID.")
    parser.add_argument("candidate_run_id", help="Candidate/degraded run ID.")
    parser.add_argument(
        "--export-dir",
        default="./exports_live",
        help="Run_Log directory (recursive) or direct Run_Log.csv/Run_Log.json path.",
    )
    parser.add_argument(
        "--min-resolved-rate",
        type=float,
        default=float(os.getenv("PLAYER_STATS_MIN_RESOLVED_RATE", "0.60")),
        help="Minimum required candidate resolved rate (default: 0.60 or PLAYER_STATS_MIN_RESOLVED_RATE).",
    )
    parser.add_argument(
        "--max-unresolved-players",
        type=int,
        default=int(os.getenv("PLAYER_STATS_MAX_UNRESOLVED_PLAYERS", "8")),
        help="Maximum allowed candidate unresolved players (default: 8 or PLAYER_STATS_MAX_UNRESOLVED_PLAYERS).",
    )
    parser.add_argument(
        "--max-missing-side-increase",
        type=int,
        default=int(os.getenv("PLAYER_STATS_MAX_MISSING_SIDE_INCREASE", "0")),
        help=(
            "Maximum allowed increase in STATS_MISS_A or STATS_MISS_B between baseline and candidate "
            "(default: 0 or PLAYER_STATS_MAX_MISSING_SIDE_INCREASE)."
        ),
    )
    parser.add_argument(
        "--override-reason",
        default=os.getenv("PLAYER_STATS_COVERAGE_GATE_OVERRIDE", ""),
        help=(
            "Optional non-empty override reason. If set, threshold failures are reported but process exits 0. "
            "Can also be provided via PLAYER_STATS_COVERAGE_GATE_OVERRIDE."
        ),
    )
    parser.add_argument("--out", default="", help="Optional output file path for JSON gate report.")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    rows = load_run_log_rows(args.export_dir)
    if not rows:
        print(
            json.dumps(
                {
                    "status": "error",
                    "reason_code": "run_log_not_found",
                    "export_dir": args.export_dir,
                    "reason": "No Run_Log CSV/JSON rows found for gate evaluation.",
                },
                indent=2,
                sort_keys=True,
            )
        )
        return 1

    config = GateConfig(
        min_resolved_rate=max(0.0, min(1.0, float(args.min_resolved_rate))),
        max_unresolved_players=max(0, int(args.max_unresolved_players)),
        max_missing_side_increase=max(0, int(args.max_missing_side_increase)),
        override_reason=str(args.override_reason or "").strip(),
    )

    try:
        report = evaluate_player_stats_gate(rows, args.baseline_run_id, args.candidate_run_id, config)
    except ValueError as exc:
        print(json.dumps({"status": "error", "reason_code": "missing_run_summary", "message": str(exc)}, indent=2, sort_keys=True))
        return 1

    text = json.dumps(report, indent=2, sort_keys=True)
    print(text)
    if args.out:
        out_path = Path(args.out)
        out_path.parent.mkdir(parents=True, exist_ok=True)
        out_path.write_text(text + "\n", encoding="utf-8")

    if report["status"] == "fail":
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
