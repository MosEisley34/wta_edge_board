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

from runtime_artifact_codec import normalize_run_log_row


@dataclass(frozen=True)
class GateConfig:
    min_resolved_rate: float = 0.60
    max_unresolved_players: int = 8
    max_missing_side_increase: int = 0
    override_reason: str = ""


def _normalize_schema_missing_details(*detail_lists: list[str]) -> list[str]:
    ordered: list[str] = []
    seen: set[str] = set()
    for details in detail_lists:
        for detail in details:
            code = str(detail or "").strip()
            if not code or code in seen:
                continue
            seen.add(code)
            ordered.append(code)
    return ordered


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


def _normalize_stage_summaries_payload(value: Any) -> list[dict[str, Any]]:
    parsed = _parse_json_like(value, None)
    if isinstance(parsed, list):
        return [row for row in parsed if isinstance(row, dict)]
    if isinstance(parsed, dict):
        nested = parsed.get("stage_summaries")
        if isinstance(nested, list):
            return [row for row in nested if isinstance(row, dict)]
    return []


def _extract_stage_summaries(summary_row: dict[str, Any]) -> list[dict[str, Any]]:
    stage_summaries = _normalize_stage_summaries_payload(summary_row.get("stage_summaries"))
    if stage_summaries:
        return stage_summaries
    return _normalize_stage_summaries_payload(summary_row.get("message"))


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
                rows.extend(normalize_run_log_row(dict(row)) for row in payload if isinstance(row, dict))
            continue
        with open(path, "r", encoding="utf-8", newline="") as handle:
            rows.extend(normalize_run_log_row(dict(row)) for row in csv.DictReader(handle))
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


def _extract_player_stats_stage_summary(summary_row: dict[str, Any]) -> dict[str, Any]:
    direct_stage_summary = summary_row.get("stageFetchPlayerStats")
    if isinstance(direct_stage_summary, dict):
        return direct_stage_summary
    parsed_direct_stage_summary = _parse_json_like(direct_stage_summary, {})
    if isinstance(parsed_direct_stage_summary, dict) and parsed_direct_stage_summary:
        return parsed_direct_stage_summary

    nested_stage_summaries = _parse_json_like(summary_row.get("stage_summaries"), {})
    if isinstance(nested_stage_summaries, dict):
        nested_stage_summary = nested_stage_summaries.get("stageFetchPlayerStats")
        if isinstance(nested_stage_summary, dict):
            return nested_stage_summary
        parsed_nested_stage_summary = _parse_json_like(nested_stage_summary, {})
        if isinstance(parsed_nested_stage_summary, dict) and parsed_nested_stage_summary:
            return parsed_nested_stage_summary

    for stage in _extract_stage_summaries(summary_row):
        if str(stage.get("stage") or "") == "stageFetchPlayerStats":
            return stage
    return {}


def _extract_player_stats_coverage(summary_row: dict[str, Any]) -> dict[str, Any]:
    stage_summary = _extract_player_stats_stage_summary(summary_row)
    schema_failures: list[str] = []
    schema_warnings: list[str] = []
    source_paths: dict[str, str] = {}
    shape_anomalies: list[str] = []
    source_order = (
        "canonical_row",
        "nested_stage_summary",
        "message_fallback",
        "zero_safe_default",
    )

    def _first_present_dict(
        sources: dict[str, Any],
        source_key: str,
        required: bool = True,
    ) -> dict[str, Any]:
        for source_name in source_order:
            candidate = sources.get(source_name)
            if isinstance(candidate, dict) and (candidate or not required):
                source_paths[source_key] = source_name
                return candidate
        source_paths[source_key] = "missing"
        return {}

    def _pick_first_int_from_sources(
        sources: dict[str, dict[str, Any]],
        keys: tuple[str, ...],
        source_key: str,
    ) -> int | None:
        for source_name in source_order:
            values = sources.get(source_name)
            if not isinstance(values, dict):
                continue
            value = _pick_int_optional(values, keys)
            if value is not None:
                source_paths[source_key] = source_name
                return value
        return None

    def _pick_first_float_from_sources(
        sources: dict[str, dict[str, Any]],
        keys: tuple[str, ...],
        source_key: str,
    ) -> float | None:
        for source_name in source_order:
            values = sources.get(source_name)
            if not isinstance(values, dict):
                continue
            value = _pick_float_optional(values, keys)
            if value is not None:
                source_paths[source_key] = source_name
                return value
        return None

    if not stage_summary:
        schema_failures.append("stageFetchPlayerStats summary missing")
        source_paths["stageFetchPlayerStats_summary"] = "missing"
    else:
        if summary_row.get("stageFetchPlayerStats") not in (None, ""):
            source_paths["stageFetchPlayerStats_summary"] = "canonical_row"
        else:
            nested_stage_summaries = _parse_json_like(summary_row.get("stage_summaries"), {})
            if isinstance(nested_stage_summaries, dict) and nested_stage_summaries.get("stageFetchPlayerStats") not in (None, ""):
                source_paths["stageFetchPlayerStats_summary"] = "nested_stage_summary"
            else:
                source_paths["stageFetchPlayerStats_summary"] = "message_fallback"
    if summary_row.get("stage_summaries") == "":
        shape_anomalies.append("empty_string_stage_summaries")
    stage_message = _parse_json_like(stage_summary.get("message"), {})
    if stage_summary.get("message") == "":
        shape_anomalies.append("empty_string_stage_message")
    if not isinstance(stage_message, dict):
        stage_message = {}

    nested_stage_summaries = _parse_json_like(summary_row.get("stage_summaries"), {})
    nested_stage_summary = {}
    if isinstance(nested_stage_summaries, dict):
        nested_stage_summary = _parse_json_like(nested_stage_summaries.get("stageFetchPlayerStats"), {})
    if not isinstance(nested_stage_summary, dict):
        nested_stage_summary = {}
    nested_stage_message = _parse_json_like(nested_stage_summary.get("message"), {})
    if not isinstance(nested_stage_message, dict):
        nested_stage_message = {}

    canonical_reason_metadata = _parse_json_like(stage_summary.get("reason_metadata"), {})
    if stage_summary.get("reason_metadata") == "":
        shape_anomalies.append("empty_string_reason_metadata")
    nested_reason_metadata = _parse_json_like(nested_stage_summary.get("reason_metadata"), {})
    message_reason_metadata = _parse_json_like(stage_message.get("reason_metadata"), {})
    reason_metadata_candidates = {
        "canonical_row": canonical_reason_metadata if isinstance(canonical_reason_metadata, dict) else {},
        "nested_stage_summary": nested_reason_metadata if isinstance(nested_reason_metadata, dict) else {},
        "message_fallback": message_reason_metadata if isinstance(message_reason_metadata, dict) else {},
        "zero_safe_default": {},
    }
    metadata = _first_present_dict(reason_metadata_candidates, "reason_metadata", required=True)
    if not metadata:
        schema_warnings.append("stageFetchPlayerStats reason_metadata missing")
    canonical_coverage = _parse_json_like(metadata.get("coverage"), {})
    nested_coverage = _parse_json_like(nested_reason_metadata.get("coverage"), {})
    message_coverage = _parse_json_like(stage_message.get("coverage"), {})
    if not isinstance(message_coverage, dict):
        message_coverage = _parse_json_like(nested_stage_message.get("coverage"), {})
    coverage_candidates = {
        "canonical_row": canonical_coverage if isinstance(canonical_coverage, dict) else {},
        "nested_stage_summary": nested_coverage if isinstance(nested_coverage, dict) else {},
        "message_fallback": message_coverage if isinstance(message_coverage, dict) else {},
        "zero_safe_default": {},
    }
    coverage = _first_present_dict(coverage_candidates, "coverage", required=False)

    def _pick_int_optional(values: dict[str, Any], keys: tuple[str, ...]) -> int | None:
        for key in keys:
            value = values.get(key)
            try:
                return max(0, int(float(value)))
            except (TypeError, ValueError):
                continue
        return None

    def _pick_float_optional(values: dict[str, Any], keys: tuple[str, ...]) -> float | None:
        for key in keys:
            value = values.get(key)
            try:
                return float(value)
            except (TypeError, ValueError):
                continue
        return None

    coverage_counter_sources = {
        "canonical_row": coverage if isinstance(coverage, dict) else {},
        "nested_stage_summary": nested_coverage if isinstance(nested_coverage, dict) else {},
        "message_fallback": message_coverage if isinstance(message_coverage, dict) else {},
        "zero_safe_default": {},
    }
    metadata_counter_sources = {
        "canonical_row": metadata if isinstance(metadata, dict) else {},
        "nested_stage_summary": nested_reason_metadata if isinstance(nested_reason_metadata, dict) else {},
        "message_fallback": message_reason_metadata if isinstance(message_reason_metadata, dict) else {},
        "zero_safe_default": {},
    }

    requested = _pick_first_int_from_sources(
        coverage_counter_sources,
        ("requested", "requested_players", "total", "total_players"),
        "requested",
    )
    if requested is None:
        requested = _pick_first_int_from_sources(
            metadata_counter_sources,
            (
                "requested_player_count",
                "players_total",
                "total_player_count",
                "total_players_count",
                "requested_players_count",
            ),
            "requested",
        )

    resolved = _pick_first_int_from_sources(
        coverage_counter_sources,
        ("resolved", "resolved_players", "resolved_count"),
        "resolved",
    )
    if resolved is None:
        resolved = _pick_first_int_from_sources(
            metadata_counter_sources,
            (
                "resolved_player_count",
                "resolved_with_usable_stats_count",
                "resolved_players_count",
            ),
            "resolved",
        )

    unresolved_total = _pick_first_int_from_sources(
        coverage_counter_sources,
        ("unresolved", "unresolved_players", "unresolved_count"),
        "unresolved_total",
    )
    if unresolved_total is None:
        unresolved_total = _pick_first_int_from_sources(
            metadata_counter_sources,
            (
                "unresolved_player_count",
                "players_unresolved",
                "unresolved_players_count",
            ),
            "unresolved_total",
        )

    resolved_rate = _pick_first_float_from_sources(
        coverage_counter_sources,
        ("resolved_rate", "coverage_rate", "resolved_ratio"),
        "resolved_rate",
    )
    if resolved_rate is None:
        resolved_rate = _pick_first_float_from_sources(
            metadata_counter_sources,
            ("resolved_rate", "coverage_rate", "resolved_ratio"),
            "resolved_rate",
        )

    evidence_stats_expected = False
    if stage_summary:
        for evidence_key in ("input_count", "output_count", "processed_count", "candidate_count"):
            value = _pick_int_optional(stage_summary, (evidence_key,))
            if value is not None and value > 0:
                evidence_stats_expected = True
                break
        if not evidence_stats_expected:
            reason_codes = _parse_json_like(stage_summary.get("reason_codes"), {})
            if not isinstance(reason_codes, dict) and isinstance(stage_message.get("reason_codes"), dict):
                reason_codes = stage_message.get("reason_codes")
            if isinstance(reason_codes, dict):
                if any(str(key).startswith("STATS_") for key in reason_codes.keys()):
                    evidence_stats_expected = True
                elif any(int(float(reason_codes.get(key) or 0)) > 0 for key in ("stats_loaded", "stats_missing")):
                    evidence_stats_expected = True

    has_any_coverage_counter = any(value is not None for value in (requested, resolved, unresolved_total, resolved_rate))
    if not has_any_coverage_counter and evidence_stats_expected:
        schema_failures.append("player-stats coverage counters missing")
    elif not has_any_coverage_counter:
        schema_warnings.append("player-stats coverage counters missing_without_demand_evidence")

    if requested is None and resolved is not None and unresolved_total is not None:
        requested = resolved + unresolved_total
        source_paths.setdefault("requested", "derived")
    if unresolved_total is None and requested is not None and resolved is not None:
        unresolved_total = max(0, requested - resolved)
        source_paths.setdefault("unresolved_total", "derived")
    if resolved is None and requested is not None and unresolved_total is not None:
        resolved = max(0, requested - unresolved_total)
        source_paths.setdefault("resolved", "derived")
    if requested is None and resolved is not None and resolved_rate and resolved_rate > 0:
        requested = max(resolved, int(round(resolved / resolved_rate)))
        source_paths.setdefault("requested", "derived")

    if requested == 0:
        if resolved is None:
            resolved = 0
            source_paths["resolved"] = "zero_safe_default"
        if unresolved_total is None:
            unresolved_total = 0
            source_paths["unresolved_total"] = "zero_safe_default"
        if resolved_rate is None:
            resolved_rate = 0.0
            source_paths["resolved_rate"] = "zero_safe_default"

    resolved = resolved or 0
    requested = requested or 0
    unresolved_total = unresolved_total or 0
    source_paths.setdefault("requested", "zero_safe_default")
    source_paths.setdefault("resolved", "zero_safe_default")
    source_paths.setdefault("unresolved_total", "zero_safe_default")
    source_paths.setdefault("resolved_rate", "zero_safe_default")
    applicability = "applicable"
    if requested == 0:
        applicability = "no_demand"

    return {
        "resolved": resolved,
        "requested": requested,
        "unresolved_total": unresolved_total,
        "resolved_rate": resolved_rate,
        "schema_failures": schema_failures,
        "schema_warnings": schema_warnings,
        "applicability": applicability,
        "evidence_stats_expected": evidence_stats_expected,
        "source_paths": source_paths,
        "shape_anomalies": shape_anomalies,
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
    inferred_rate = coverage.get("resolved_rate")
    rate = float(inferred_rate) if isinstance(inferred_rate, (int, float)) else ((resolved / requested) if requested else 0.0)

    return {
        "run_id": run_id,
        "resolved": resolved,
        "requested": requested,
        "unresolved_total": unresolved_total,
        "resolved_rate": rate,
        "stats_missing_player_a": int(reason_codes.get("STATS_MISS_A", 0)),
        "stats_missing_player_b": int(reason_codes.get("STATS_MISS_B", 0)),
        "schema_failures": list(coverage.get("schema_failures", [])),
        "schema_warnings": list(coverage.get("schema_warnings", [])),
        "applicability": str(coverage.get("applicability") or "applicable"),
        "source_paths": dict(coverage.get("source_paths", {})),
        "shape_anomalies": list(coverage.get("shape_anomalies", [])),
    }


def evaluate_player_stats_gate(
    rows: list[dict[str, Any]],
    baseline_run_id: str,
    candidate_run_id: str,
    config: GateConfig,
) -> dict[str, Any]:
    baseline = _run_snapshot(rows, baseline_run_id)
    candidate = _run_snapshot(rows, candidate_run_id)

    coverage_failures: list[str] = []
    schema_failures: list[str] = []
    for run_label, snapshot in (("baseline", baseline), ("candidate", candidate)):
        for failure in snapshot.get("schema_failures", []):
            schema_failures.append(f"{run_label}_{failure}")

    schema_missing_detail_codes: list[str] = []
    if any(failure.endswith("stageFetchPlayerStats summary missing") for failure in schema_failures):
        schema_missing_detail_codes.append("missing_summary")
    if any(
        (snapshot.get("source_paths") or {}).get("reason_metadata") == "missing"
        for snapshot in (baseline, candidate)
    ):
        schema_missing_detail_codes.append("missing_reason_metadata")
    if any(failure.endswith("player-stats coverage counters missing") for failure in schema_failures):
        schema_missing_detail_codes.append("missing_coverage_counters")
    if any(
        any("empty_string" in str(marker or "") for marker in (snapshot.get("shape_anomalies") or []))
        for snapshot in (baseline, candidate)
    ):
        schema_missing_detail_codes.append("empty_string_instead_of_array")

    candidate_not_applicable = candidate.get("applicability") in {"no_demand", "not_applicable"}
    baseline_not_applicable = baseline.get("applicability") in {"no_demand", "not_applicable"}
    if candidate_not_applicable:
        coverage_failures.append("player_stats_not_applicable_no_demand")
    elif candidate["resolved_rate"] < config.min_resolved_rate:
        coverage_failures.append(
            "player_stats_resolved_rate_below_min "
            f"(candidate={candidate['resolved_rate']:.4f} < min={config.min_resolved_rate:.4f})"
        )

    if (not candidate_not_applicable) and candidate["unresolved_total"] > config.max_unresolved_players:
        coverage_failures.append(
            "unresolved_players_above_max "
            f"(candidate={candidate['unresolved_total']} > max={config.max_unresolved_players})"
        )

    delta_a = candidate["stats_missing_player_a"] - baseline["stats_missing_player_a"]
    delta_b = candidate["stats_missing_player_b"] - baseline["stats_missing_player_b"]
    if (not candidate_not_applicable) and delta_a > config.max_missing_side_increase:
        coverage_failures.append(
            "stats_missing_player_a_increase_exceeded "
            f"(delta={delta_a} > max_increase={config.max_missing_side_increase})"
        )
    if (not candidate_not_applicable) and delta_b > config.max_missing_side_increase:
        coverage_failures.append(
            "stats_missing_player_b_increase_exceeded "
            f"(delta={delta_b} > max_increase={config.max_missing_side_increase})"
        )

    non_fatal_coverage_notes = []
    if candidate_not_applicable:
        non_fatal_coverage_notes.append("candidate_no_demand_not_applicable")
    if baseline_not_applicable:
        non_fatal_coverage_notes.append("baseline_no_demand_not_applicable")
    coverage_failures = [failure for failure in coverage_failures if failure != "player_stats_not_applicable_no_demand"]

    override_used = bool(config.override_reason and coverage_failures)
    coverage_gate = "pass" if not coverage_failures else ("override" if override_used else "fail")
    schema_integrity = "pass" if not schema_failures else "fail"
    failures = [*schema_failures, *coverage_failures]
    baseline_schema_warnings = list(baseline.get("schema_warnings", []))
    candidate_schema_warnings = list(candidate.get("schema_warnings", []))

    def _is_missing_without_demand_only(snapshot: dict[str, Any], warnings: list[str]) -> bool:
        return (
            bool(warnings)
            and all(warning == "player-stats coverage counters missing_without_demand_evidence" for warning in warnings)
            and int(snapshot.get("requested", 0) or 0) == 0
        )

    non_fatal_schema_warn = _is_missing_without_demand_only(baseline, baseline_schema_warnings) or _is_missing_without_demand_only(
        candidate, candidate_schema_warnings
    )
    if schema_failures:
        status = "schema_missing"
    elif non_fatal_schema_warn:
        status = "warn"
    elif coverage_failures and override_used:
        status = "override"
    elif coverage_failures:
        status = "fail"
    else:
        status = "pass"

    schema_missing_details = _normalize_schema_missing_details(schema_missing_detail_codes) if schema_failures else []

    return {
        "status": status,
        "reason_code": "schema_missing" if schema_failures else "",
        "schema_missing_details": schema_missing_details,
        "gate_passed": not failures,
        "override_used": override_used,
        "override_reason": config.override_reason if override_used else "",
        "coverage_gate": coverage_gate,
        "schema_integrity": schema_integrity,
        "thresholds": {
            "min_resolved_rate": config.min_resolved_rate,
            "max_unresolved_players": config.max_unresolved_players,
            "max_missing_side_increase": config.max_missing_side_increase,
        },
        "baseline": baseline,
        "candidate": candidate,
        "source_path_summary": {
            "baseline": baseline.get("source_paths", {}),
            "candidate": candidate.get("source_paths", {}),
        },
        "deltas": {
            "stats_missing_player_a": delta_a,
            "stats_missing_player_b": delta_b,
            "resolved_rate_pp": round((candidate["resolved_rate"] - baseline["resolved_rate"]) * 100.0, 2),
            "unresolved_total": candidate["unresolved_total"] - baseline["unresolved_total"],
        },
        "schema_failures": schema_failures,
        "schema_warnings": {
            "baseline": baseline_schema_warnings,
            "candidate": candidate_schema_warnings,
        },
        "coverage_failures": coverage_failures,
        "coverage_notes": non_fatal_coverage_notes,
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
