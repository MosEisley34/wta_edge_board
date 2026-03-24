#!/usr/bin/env python3
"""Precheck target run IDs exist in exported Run_Log artifacts before triage comparisons."""

from __future__ import annotations

import argparse
import csv
import glob
import json
import os
from collections import Counter
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Iterable


@dataclass(frozen=True)
class RunLogSource:
    path: str
    kind: str
    mtime: float


DISALLOWED_RUN_REASON_CODES = {"run_debounced_skip", "run_locked_skip"}
REQUIRED_STAGE_CHAIN = (
    "stageFetchOdds",
    "stageFetchSchedule",
    "stageMatchEvents",
    "stageFetchPlayerStats",
    "stageGenerateSignals",
    "stagePersist",
)
REQUIRED_COVERAGE_KEYS = {
    "resolved": (
        "resolved",
        "resolved_players",
        "resolved_player_count",
        "resolved_players_count",
        "resolved_name_count",
    ),
    "requested": (
        "requested",
        "requested_players",
        "requested_player_count",
        "total_player_count",
        "total_players_count",
        "players_total",
    ),
    "unresolved": (
        "unresolved",
        "unresolved_players",
        "unresolved_player_count",
        "unresolved_players_count",
        "players_unresolved",
    ),
}


def _iter_run_log_sources(export_dir: str) -> list[RunLogSource]:
    """Find Run_Log.json / Run_Log.csv artifacts under the export directory or direct file path."""
    if os.path.isfile(export_dir):
        norm_path = os.path.normpath(export_dir)
        lower = norm_path.lower()
        if lower.endswith("run_log.json"):
            try:
                return [RunLogSource(path=norm_path, kind="json", mtime=os.path.getmtime(norm_path))]
            except OSError:
                return []
        if lower.endswith("run_log.csv"):
            try:
                return [RunLogSource(path=norm_path, kind="csv", mtime=os.path.getmtime(norm_path))]
            except OSError:
                return []
        return []

    patterns = (("json", "**/Run_Log.json"), ("csv", "**/Run_Log.csv"))
    sources: list[RunLogSource] = []
    seen: set[str] = set()

    for kind, pattern in patterns:
        for path in glob.glob(os.path.join(export_dir, pattern), recursive=True):
            norm_path = os.path.normpath(path)
            if norm_path in seen:
                continue
            seen.add(norm_path)
            try:
                mtime = os.path.getmtime(norm_path)
            except OSError:
                continue
            sources.append(RunLogSource(path=norm_path, kind=kind, mtime=mtime))

    return sorted(sources, key=lambda source: source.mtime, reverse=True)


def _scan_run_ids(path: str) -> Counter[str]:
    """Return run_id occurrence counts from a Run_Log artifact."""
    run_id_counts: Counter[str] = Counter()
    lower = path.lower()

    if lower.endswith(".json"):
        with open(path, "r", encoding="utf-8") as handle:
            payload = json.load(handle)
        if isinstance(payload, list):
            for row in payload:
                if isinstance(row, dict):
                    run_id = str(row.get("run_id") or "").strip()
                    if run_id:
                        run_id_counts[run_id] += 1
        return run_id_counts

    with open(path, "r", encoding="utf-8", newline="") as handle:
        for row in csv.DictReader(handle):
            run_id = str(row.get("run_id") or "").strip()
            if run_id:
                run_id_counts[run_id] += 1
    return run_id_counts


def _parse_json_like(value: object) -> object:
    if isinstance(value, (dict, list)):
        return value
    if not isinstance(value, str):
        return None
    text = value.strip()
    if not text:
        return None
    try:
        return json.loads(text)
    except Exception:
        return None


def _scan_run_contracts(path: str, target_run_ids: set[str]) -> dict[str, dict[str, object]]:
    contracts: dict[str, dict[str, object]] = {
        run_id: {
            "disallowed_reasons": set(),
            "stages": set(),
            "run_summary_exists": False,
            "stage_fetch_player_stats_summary_exists": False,
            "coverage_prereq": {"resolved": False, "requested": False, "unresolved": False},
            "reason_codes_has_stats_miss_a": False,
            "reason_codes_has_stats_miss_b": False,
        }
        for run_id in target_run_ids
    }
    lower = path.lower()
    if lower.endswith(".json"):
        with open(path, "r", encoding="utf-8") as handle:
            payload = json.load(handle)
        rows = payload if isinstance(payload, list) else []
    else:
        with open(path, "r", encoding="utf-8", newline="") as handle:
            rows = list(csv.DictReader(handle))

    for row in rows:
        if not isinstance(row, dict):
            continue
        run_id = str(row.get("run_id") or "").strip()
        if run_id not in target_run_ids:
            continue
        reason_code = str(row.get("reason_code") or "").strip().lower()
        if reason_code in DISALLOWED_RUN_REASON_CODES:
            contracts[run_id]["disallowed_reasons"].add(reason_code)

        stage = str(row.get("stage") or "").strip()
        if stage and stage != "runEdgeBoard":
            contracts[run_id]["stages"].add(stage)

        if str(row.get("row_type") or "") == "summary" and stage == "runEdgeBoard":
            contracts[run_id]["run_summary_exists"] = True
            reason_codes = _parse_json_like(row.get("reason_codes"))
            if isinstance(reason_codes, dict):
                if "STATS_MISS_A" in reason_codes:
                    contracts[run_id]["reason_codes_has_stats_miss_a"] = True
                if "STATS_MISS_B" in reason_codes:
                    contracts[run_id]["reason_codes_has_stats_miss_b"] = True

            stage_summaries = _parse_json_like(row.get("stage_summaries"))
            if not isinstance(stage_summaries, list):
                message = _parse_json_like(row.get("message"))
                if isinstance(message, dict):
                    stage_summaries = message.get("stage_summaries")
            if isinstance(stage_summaries, list):
                for stage_summary in stage_summaries:
                    if not isinstance(stage_summary, dict):
                        continue
                    stage_name = str(stage_summary.get("stage") or "").strip()
                    if stage_name:
                        contracts[run_id]["stages"].add(stage_name)
                    if stage_name != "stageFetchPlayerStats":
                        continue

                    contracts[run_id]["stage_fetch_player_stats_summary_exists"] = True
                    reason_metadata = _parse_json_like(stage_summary.get("reason_metadata"))
                    if not isinstance(reason_metadata, dict):
                        continue
                    coverage = _parse_json_like(reason_metadata.get("coverage"))
                    coverage_dict = coverage if isinstance(coverage, dict) else {}

                    for prereq_key, aliases in REQUIRED_COVERAGE_KEYS.items():
                        has_alias = any(alias in coverage_dict for alias in aliases)
                        if not has_alias:
                            has_alias = any(alias in reason_metadata for alias in aliases)
                        if has_alias:
                            contracts[run_id]["coverage_prereq"][prereq_key] = True
    return contracts


def _empty_contract() -> dict[str, object]:
    return {
        "disallowed_reasons": set(),
        "stages": set(),
        "run_summary_exists": False,
        "stage_fetch_player_stats_summary_exists": False,
        "coverage_prereq": {"resolved": False, "requested": False, "unresolved": False},
        "reason_codes_has_stats_miss_a": False,
        "reason_codes_has_stats_miss_b": False,
    }


def _format_missing(run_ids: Iterable[str], present_ids: set[str]) -> list[str]:
    return [run_id for run_id in run_ids if run_id not in present_ids]


def _format_mtime(timestamp: float) -> str:
    return datetime.fromtimestamp(timestamp, tz=timezone.utc).isoformat()


def main() -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Verify that both target run IDs exist in exported Run_Log artifacts before triage/comparison."
        )
    )
    parser.add_argument("run_id_a", help="First target run ID.")
    parser.add_argument("run_id_b", help="Second target run ID.")
    parser.add_argument(
        "--export-dir",
        default="./exports_live",
        help=(
            "Directory containing exported runtime artifacts, or a direct Run_Log.csv/Run_Log.json path "
            "(default: ./exports_live)."
        ),
    )
    parser.add_argument(
        "--require-gate-prereqs",
        action="store_true",
        help=(
            "Require runEdgeBoard summary, full stage chain, and player-stats coverage metadata "
            "needed by scripts/check_player_stats_coverage.py."
        ),
    )
    parser.add_argument(
        "--allow-csv-only-triage",
        action="store_true",
        help=(
            "Emergency override: allow precheck to continue when run IDs are present in CSV but missing "
            "from JSON. This degrades confidence and should be used only for incident triage."
        ),
    )
    args = parser.parse_args()

    sources = _iter_run_log_sources(args.export_dir)
    if not sources:
        print(f"Precheck failed: no Run_Log artifacts found under {args.export_dir}.")
        print("Expected a Run_Log.csv/Run_Log.json file or at least one file matching **/Run_Log.{json,csv}.")
        print("Stop triage and re-export from the sheet before further analysis.")
        return 2

    merged_counts: Counter[str] = Counter()
    source_counts: dict[str, Counter[str]] = {}
    merged_contracts: dict[str, dict[str, object]] = {
        args.run_id_a: _empty_contract(),
        args.run_id_b: _empty_contract(),
    }
    kind_counts = Counter(source.kind for source in sources)

    print(f"Precheck source files under {args.export_dir} (newest first):")
    for source in sources:
        print(f"- {source.path} [{source.kind}, mtime={_format_mtime(source.mtime)}]")

    for source in sources:
        try:
            source_counter = _scan_run_ids(source.path)
        except Exception as exc:  # noqa: BLE001 - continue scanning other artifacts
            print(f"Warning: failed to parse {source.path}: {exc}")
            continue

        source_counts[source.path] = source_counter
        merged_counts.update(source_counter)
        try:
            source_contracts = _scan_run_contracts(source.path, {args.run_id_a, args.run_id_b})
        except Exception as exc:  # noqa: BLE001 - continue scanning other artifacts
            print(f"Warning: failed to parse run contract details in {source.path}: {exc}")
            continue
        for run_id, details in source_contracts.items():
            merged_contracts[run_id]["disallowed_reasons"].update(details["disallowed_reasons"])
            merged_contracts[run_id]["stages"].update(details["stages"])
            merged_contracts[run_id]["run_summary_exists"] = (
                merged_contracts[run_id]["run_summary_exists"] or details["run_summary_exists"]
            )
            merged_contracts[run_id]["stage_fetch_player_stats_summary_exists"] = (
                merged_contracts[run_id]["stage_fetch_player_stats_summary_exists"]
                or details["stage_fetch_player_stats_summary_exists"]
            )
            merged_contracts[run_id]["reason_codes_has_stats_miss_a"] = (
                merged_contracts[run_id]["reason_codes_has_stats_miss_a"]
                or details["reason_codes_has_stats_miss_a"]
            )
            merged_contracts[run_id]["reason_codes_has_stats_miss_b"] = (
                merged_contracts[run_id]["reason_codes_has_stats_miss_b"]
                or details["reason_codes_has_stats_miss_b"]
            )
            for prereq_key, present in details["coverage_prereq"].items():
                if present:
                    merged_contracts[run_id]["coverage_prereq"][prereq_key] = True

    targets = (args.run_id_a, args.run_id_b)
    present_ids = set(merged_counts)
    missing = _format_missing(targets, present_ids)
    has_json = kind_counts["json"] > 0
    has_csv = kind_counts["csv"] > 0
    target_source_presence: dict[str, dict[str, bool]] = {
        run_id: {
            "json": any(
                source.kind == "json" and source_counts.get(source.path, Counter()).get(run_id, 0) > 0
                for source in sources
            ),
            "csv": any(
                source.kind == "csv" and source_counts.get(source.path, Counter()).get(run_id, 0) > 0
                for source in sources
            ),
        }
        for run_id in targets
    }

    print(
        "Precheck merged run IDs across "
        f"{len(sources)} source file(s): {kind_counts['json']} JSON, {kind_counts['csv']} CSV."
    )
    for run_id in targets:
        total_matches = merged_counts.get(run_id, 0)
        matched_files = sum(
            1 for counter in source_counts.values() if counter.get(run_id, 0) > 0
        )
        status = "FOUND" if total_matches > 0 else "MISSING"
        print(
            f"- {run_id}: {status} (matches={total_matches}, source_files_with_match={matched_files})"
        )

    if missing:
        print("Precheck failed: one or more target run IDs are missing from merged JSON/CSV sources.")
        print("Stop triage and re-export from the sheet before further analysis.")
        return 2

    source_mismatch_failures: list[str] = []
    degraded_confidence_reasons: list[str] = []
    if has_json and has_csv:
        for run_id in targets:
            source_presence = target_source_presence[run_id]
            if source_presence["json"] and source_presence["csv"]:
                continue
            if args.allow_csv_only_triage and source_presence["csv"] and not source_presence["json"]:
                degraded_confidence_reasons.append(
                    f"{run_id}: csv_present=true json_present=false"
                )
                continue
            source_mismatch_failures.append(
                f"{run_id}: csv_present={str(source_presence['csv']).lower()} "
                f"json_present={str(source_presence['json']).lower()}"
            )

    if source_mismatch_failures:
        print("Precheck failed: run_id_source_mismatch.")
        print("Each target run_id must be present in both Run_Log.csv and Run_Log.json when both sources exist.")
        for failure in source_mismatch_failures:
            print(f"- {failure}")
        print("Stop triage and re-export from the sheet before further analysis.")
        return 2

    if degraded_confidence_reasons:
        print(
            "Precheck warning: degraded_confidence_csv_only_triage "
            "(--allow-csv-only-triage emergency override enabled)."
        )
        for detail in degraded_confidence_reasons:
            print(f"- {detail}")

    contract_failures: list[str] = []
    for run_id in targets:
        disallowed = sorted(merged_contracts[run_id]["disallowed_reasons"])
        if disallowed:
            contract_failures.append(f"{run_id}: disallowed reason_code(s) {', '.join(disallowed)}")
        if args.require_gate_prereqs:
            if not merged_contracts[run_id]["run_summary_exists"]:
                contract_failures.append(
                    f"{run_id}: missing runEdgeBoard summary row (row_type=summary, stage=runEdgeBoard)"
                )
            observed_stages = set(merged_contracts[run_id]["stages"])
            missing_stages = [stage for stage in REQUIRED_STAGE_CHAIN if stage not in observed_stages]
            if missing_stages:
                contract_failures.append(
                    f"{run_id}: missing stage chain entries ({', '.join(missing_stages)})"
                )
            if not merged_contracts[run_id]["stage_fetch_player_stats_summary_exists"]:
                contract_failures.append(
                    f"{run_id}: missing stageFetchPlayerStats summary in stage_summaries"
                )
            missing_coverage = [
                key
                for key, present in merged_contracts[run_id]["coverage_prereq"].items()
                if not present
            ]
            if missing_coverage:
                contract_failures.append(
                    f"{run_id}: missing coverage metadata field group(s) for gate ({', '.join(missing_coverage)})"
                )
            if not merged_contracts[run_id]["reason_codes_has_stats_miss_a"]:
                contract_failures.append(f"{run_id}: reason_codes missing STATS_MISS_A")
            if not merged_contracts[run_id]["reason_codes_has_stats_miss_b"]:
                contract_failures.append(f"{run_id}: reason_codes missing STATS_MISS_B")

    if contract_failures:
        print("Precheck failed: run-pair comparison auto-failed due to invalid run contract.")
        if args.require_gate_prereqs:
            print(
                "Gate prereq mode (--require-gate-prereqs) blocks compare scripts until summary/stage/coverage requirements are met."
            )
        for failure in contract_failures:
            print(f"- {failure}")
        print("Replacement run IDs are required before producing pre/post verdict.")
        return 2

    print("Precheck passed: both target run IDs are present in merged Run_Log JSON/CSV data.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
