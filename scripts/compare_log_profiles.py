#!/usr/bin/env python3
import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SCRIPTS_DIR = ROOT / "scripts"
sys.path.insert(0, str(SCRIPTS_DIR))

from pipeline_log_adapter import adapt_run_log_record_for_legacy  # noqa: E402

WATCHDOG_STAGES = {
    "bootstrap_empty_cycle_watchdog",
    "productive_output_watchdog",
    "schedule_only_watchdog",
    "run_lifecycle",
}
DEFAULT_CRITICAL_PARITY_KEYS = ["gate_reasons", "source_selection", "watchdog"]
SUMMARY_COUNTER_FIELDS = [
    "fetched_odds",
    "fetched_schedule",
    "allowed_tournaments",
    "matched",
    "unmatched",
    "signals_found",
    "rejected",
    "cooldown_suppressed",
    "duplicate_suppressed",
]
BOUNDED_COUNTER_FIELDS = list(SUMMARY_COUNTER_FIELDS)


def derive_run_health_attribution(summary, watchdog, rejection_codes):
    degraded_watchdog = sorted(
        [
            {
                "stage": row.get("stage", ""),
                "status": row.get("status", ""),
                "reason_code": row.get("reason_code", ""),
            }
            for row in watchdog
            if str(row.get("status", "")).lower() not in {"", "success", "ok", "healthy"}
        ],
        key=lambda x: (x["stage"], x["reason_code"], x["status"]),
    )
    degraded_rejection_reasons = sorted(
        [
            str(code)
            for code, count in (rejection_codes or {}).items()
            if float(count or 0) > 0 and (str(code).startswith("run_health_") or str(code).endswith("_detected"))
        ]
    )
    degraded_reason_sources = {
        "watchdog": [row["reason_code"] for row in degraded_watchdog],
        "summary_reason_code": str(summary.get("reason_code", "")),
        "rejection_codes": degraded_rejection_reasons,
    }
    return {
        "status": "degraded" if (degraded_watchdog or degraded_rejection_reasons) else "healthy",
        "degraded_reason_sources": degraded_reason_sources,
        "degraded_attribution_consistent": bool(degraded_watchdog or degraded_rejection_reasons)
        == bool(
            degraded_reason_sources["watchdog"]
            or degraded_reason_sources["rejection_codes"]
            or degraded_reason_sources["summary_reason_code"].startswith("run_health_")
        ),
    }


def canonical_bytes(rows):
    return len(json.dumps(rows, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8"))


def _parse_timestamp_utc(value):
    if not isinstance(value, str) or not value:
        return None
    normalized = value.strip()
    if not normalized:
        return None
    if normalized.endswith("Z"):
        normalized = normalized[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(normalized)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _as_iso_utc(value):
    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def _confidence_badge(last_verified_at, consistency_ratio):
    if last_verified_at is None:
        return "Low"
    age_hours = max(0.0, (datetime.now(timezone.utc) - last_verified_at).total_seconds() / 3600.0)
    if age_hours <= 24 and consistency_ratio >= 0.95:
        return "High"
    if age_hours <= 72 and consistency_ratio >= 0.80:
        return "Medium"
    return "Low"


def _build_verification_matrix(
    latest_verification_utc,
    run_count_compared,
    reduction_pct,
    baseline_reduction,
    target_reduction_pct,
    failed_critical_parity,
    critical_mismatch_counts,
    counter_integrity,
    stage_counter_invariants_verbose,
    stage_counter_invariants_compact,
):
    stable_percentage = abs(float(reduction_pct) - float(baseline_reduction)) <= 1.0
    mismatch_total = sum(int(v) for v in (critical_mismatch_counts or {}).values())
    parity_evidence = max(0, int(run_count_compared))
    parity_consistency = 1.0 if parity_evidence <= 0 else max(0.0, min(1.0, 1.0 - (mismatch_total / parity_evidence)))

    counter_checked = max(0, int((counter_integrity or {}).get("run_count_checked", 0)))
    counter_inflation = sum(int(v) for v in ((counter_integrity or {}).get("inflation_by_counter") or {}).values())
    counter_consistency = 1.0 if counter_checked <= 0 else max(0.0, min(1.0, 1.0 - (counter_inflation / counter_checked)))

    stage_checked_verbose = max(0, int((stage_counter_invariants_verbose or {}).get("checked_run_count", 0)))
    stage_checked_compact = max(0, int((stage_counter_invariants_compact or {}).get("checked_run_count", 0)))
    stage_checked = stage_checked_verbose + stage_checked_compact
    stage_violations = len((stage_counter_invariants_verbose or {}).get("violations") or []) + len(
        (stage_counter_invariants_compact or {}).get("violations") or []
    )
    stage_consistency = 1.0 if stage_checked <= 0 else max(0.0, min(1.0, 1.0 - (stage_violations / stage_checked)))

    rows = [
        {
            "category": "size_reduction_threshold",
            "score": round(float(reduction_pct), 2),
            "target": round(float(target_reduction_pct), 2),
            "status": "pass" if float(reduction_pct) >= float(target_reduction_pct) else "fail",
            "last_verified_utc": latest_verification_utc,
            "evidence_count": max(0, int(run_count_compared)),
            "consistency_ratio": 1.0,
            "confidence_badge": _confidence_badge(_parse_timestamp_utc(latest_verification_utc), 1.0),
            "risk_note": (
                "Gate failed even though reduction percentage is stable versus baseline."
                if stable_percentage and float(reduction_pct) < float(target_reduction_pct)
                else ""
            ),
        },
        {
            "category": "critical_parity",
            "score": round(parity_consistency * 100.0, 2),
            "target": 100.0,
            "status": "pass" if not failed_critical_parity else "fail",
            "last_verified_utc": latest_verification_utc,
            "evidence_count": parity_evidence,
            "consistency_ratio": round(parity_consistency, 4),
            "confidence_badge": _confidence_badge(_parse_timestamp_utc(latest_verification_utc), parity_consistency),
            "risk_note": (
                "Gate failed despite stable percentage reduction; inspect parity mismatches."
                if stable_percentage and bool(failed_critical_parity)
                else ""
            ),
        },
        {
            "category": "counter_integrity",
            "score": round(counter_consistency * 100.0, 2),
            "target": 100.0,
            "status": "pass" if bool((counter_integrity or {}).get("passed")) else "fail",
            "last_verified_utc": latest_verification_utc,
            "evidence_count": counter_checked,
            "consistency_ratio": round(counter_consistency, 4),
            "confidence_badge": _confidence_badge(_parse_timestamp_utc(latest_verification_utc), counter_consistency),
            "risk_note": (
                "Gate failed despite stable percentage reduction; inflated counters detected."
                if stable_percentage and not bool((counter_integrity or {}).get("passed"))
                else ""
            ),
        },
        {
            "category": "stage_counter_invariants",
            "score": round(stage_consistency * 100.0, 2),
            "target": 100.0,
            "status": "pass" if stage_violations == 0 else "fail",
            "last_verified_utc": latest_verification_utc,
            "evidence_count": stage_checked,
            "consistency_ratio": round(stage_consistency, 4),
            "confidence_badge": _confidence_badge(_parse_timestamp_utc(latest_verification_utc), stage_consistency),
            "risk_note": (
                "Gate failed despite stable percentage reduction; stage counter invariant drift detected."
                if stable_percentage and stage_violations > 0
                else ""
            ),
        },
    ]
    return rows


def _render_verification_matrix_markdown(rows):
    lines = [
        "### Verification Matrix",
        "",
        "| Category | Score | Target | Status | Last Verified (UTC) | Evidence Count | Confidence | Risk Note |",
        "| --- | ---: | ---: | --- | --- | ---: | --- | --- |",
    ]
    for row in rows:
        lines.append(
            f"| {row.get('category')} | {row.get('score')} | {row.get('target')} | {row.get('status')} | "
            f"{row.get('last_verified_utc') or 'n/a'} | {row.get('evidence_count', 0)} | "
            f"{row.get('confidence_badge')} | {row.get('risk_note') or '—'} |"
        )
    return "\n".join(lines)


def parse_json_field(value, fallback):
    if value is None or value == "":
        return fallback
    if isinstance(value, (dict, list)):
        return value
    if isinstance(value, str):
        try:
            return json.loads(value)
        except Exception:
            return fallback
    return fallback


def load_raw_rows(path):
    raw = json.loads(Path(path).read_text(encoding="utf-8"))
    return raw if isinstance(raw, list) else [raw]


def load_rows_for_parity(path):
    return [adapt_run_log_record_for_legacy(r) for r in load_raw_rows(path)]


def index_rows(rows):
    by_run = {}
    for row in rows:
        run_id = row.get("run_id", "")
        by_run.setdefault(run_id, []).append(row)

    indexed = {}
    for run_id, run_rows in by_run.items():
        summary = None
        watchdog = []
        for row in run_rows:
            if row.get("row_type") == "summary" and row.get("stage") == "runEdgeBoard":
                summary = row
            if row.get("stage") in WATCHDOG_STAGES:
                watchdog.append(
                    {
                        "stage": row.get("stage", ""),
                        "status": row.get("status", ""),
                        "reason_code": row.get("reason_code", ""),
                    }
                )

        summary = summary or {}
        stage_summaries = parse_json_field(summary.get("stage_summaries"), [])
        if isinstance(stage_summaries, dict) and "stage_summaries" in stage_summaries:
            stage_summaries = stage_summaries.get("stage_summaries") or []

        source_selection = {}
        stage_timing = {}
        for st in stage_summaries if isinstance(stage_summaries, list) else []:
            stage_name = st.get("stage", "")
            source_selection[stage_name] = st.get("provider", "")
            stage_timing[stage_name] = {
                "started_at": st.get("started_at", ""),
                "ended_at": st.get("ended_at", ""),
                "duration_ms": st.get("duration_ms", 0),
            }

        rejection_codes = parse_json_field(summary.get("rejection_codes"), {})
        if isinstance(rejection_codes, dict) and "reason_codes" in rejection_codes:
            rejection_codes = rejection_codes.get("reason_codes") or {}

        summary_counters = {
            field: int(summary.get(field, 0) or 0)
            for field in SUMMARY_COUNTER_FIELDS
        }
        run_health_attribution = derive_run_health_attribution(summary, watchdog, rejection_codes)

        indexed[run_id] = {
            "gate_reasons": {
                "summary_reason_code": summary.get("reason_code", ""),
                "rejection_codes": rejection_codes,
                "stage_summaries": stage_summaries if isinstance(stage_summaries, list) else [],
            },
            "summary_counters": summary_counters,
            "source_selection": source_selection,
            "watchdog": sorted(watchdog, key=lambda x: (x["stage"], x["reason_code"], x["status"])),
            "stage_timing": stage_timing,
            "run_health_attribution": run_health_attribution,
            "summary_ended_at": summary.get("ended_at", ""),
        }

    return indexed


def compare_index(verbose_idx, compact_idx):
    run_ids = sorted(set(verbose_idx.keys()) & set(compact_idx.keys()))
    missing_verbose = sorted(set(compact_idx.keys()) - set(verbose_idx.keys()))
    missing_compact = sorted(set(verbose_idx.keys()) - set(compact_idx.keys()))

    mismatch_counts = {
        "gate_reasons": 0,
        "summary_counters": 0,
        "source_selection": 0,
        "watchdog": 0,
        "stage_timing": 0,
        "run_health_attribution": 0,
    }
    mismatch_samples = []

    for run_id in run_ids:
        sample = {"run_id": run_id, "mismatches": []}
        v = verbose_idx[run_id]
        c = compact_idx[run_id]
        for key in mismatch_counts.keys():
            if v.get(key) != c.get(key):
                mismatch_counts[key] += 1
                sample["mismatches"].append(key)
        if sample["mismatches"]:
            mismatch_samples.append(sample)

    return {
        "run_count_compared": len(run_ids),
        "missing_in_verbose": missing_verbose,
        "missing_in_compact": missing_compact,
        "mismatch_counts": mismatch_counts,
        "mismatch_samples": mismatch_samples[:10],
    }


def evaluate_counter_integrity(verbose_idx, compact_idx):
    run_ids = sorted(set(verbose_idx.keys()) & set(compact_idx.keys()))
    inflation_by_counter = {field: 0 for field in BOUNDED_COUNTER_FIELDS}
    inflation_samples = []

    for run_id in run_ids:
        verbose_counters = verbose_idx[run_id].get("summary_counters", {})
        compact_counters = compact_idx[run_id].get("summary_counters", {})
        inflated = {}
        for field in BOUNDED_COUNTER_FIELDS:
            verbose_value = int(verbose_counters.get(field, 0) or 0)
            compact_value = int(compact_counters.get(field, 0) or 0)
            if compact_value > verbose_value:
                inflation_by_counter[field] += 1
                inflated[field] = {
                    "verbose": verbose_value,
                    "compact": compact_value,
                    "delta": compact_value - verbose_value,
                }
        if inflated:
            inflation_samples.append({"run_id": run_id, "inflated_counters": inflated})

    return {
        "bounded_counter_fields": BOUNDED_COUNTER_FIELDS,
        "run_count_checked": len(run_ids),
        "inflation_by_counter": inflation_by_counter,
        "inflation_samples": inflation_samples[:10],
        "passed": not any(count > 0 for count in inflation_by_counter.values()),
    }


def evaluate_stage_counter_invariants(indexed_runs):
    violations = []
    for run_id, payload in (indexed_runs or {}).items():
        gate_reasons = payload.get("gate_reasons", {}) or {}
        stage_summaries = gate_reasons.get("stage_summaries", [])
        if not isinstance(stage_summaries, list):
            continue
        for summary in stage_summaries:
            if str((summary or {}).get("stage", "")) != "stageMatchEvents":
                continue
            reason_codes = (summary or {}).get("reason_codes", {}) or {}
            output_count = int((summary or {}).get("output_count", 0) or 0)
            matched_count = int(reason_codes.get("matched_count", 0) or 0)
            if matched_count > output_count:
                violations.append(
                    {
                        "run_id": run_id,
                        "stage": "stageMatchEvents",
                        "counter": "matched_count",
                        "value": matched_count,
                        "max_name": "output_count",
                        "max_value": output_count,
                    }
                )
    return {
        "checked_run_count": len(indexed_runs or {}),
        "violations": violations[:20],
        "passed": len(violations) == 0,
    }


def build_summary(verbose_path, compact_path, target_reduction_pct, critical_parity_keys, baseline_summary_path=None):
    verbose_rows_raw = load_raw_rows(verbose_path)
    compact_rows_raw = load_raw_rows(compact_path)

    verbose_rows = [adapt_run_log_record_for_legacy(r) for r in verbose_rows_raw]
    compact_rows = [adapt_run_log_record_for_legacy(r) for r in compact_rows_raw]

    verbose_size = canonical_bytes(verbose_rows_raw)
    compact_size = canonical_bytes(compact_rows_raw)
    reduction_pct = 0.0
    if verbose_size > 0:
        reduction_pct = ((verbose_size - compact_size) / verbose_size) * 100.0

    verbose_idx = index_rows(verbose_rows)
    compact_idx = index_rows(compact_rows)

    parity = compare_index(verbose_idx, compact_idx)
    mismatch_counts = parity.get("mismatch_counts", {})
    critical_mismatch_counts = {
        key: int(mismatch_counts.get(key, 0))
        for key in critical_parity_keys
    }
    failed_critical_parity = sorted([k for k, v in critical_mismatch_counts.items() if v > 0])
    counter_integrity = evaluate_counter_integrity(verbose_idx, compact_idx)
    stage_counter_invariants_verbose = evaluate_stage_counter_invariants(verbose_idx)
    stage_counter_invariants_compact = evaluate_stage_counter_invariants(compact_idx)
    stage_counter_invariants_passed = (
        stage_counter_invariants_verbose["passed"]
        and stage_counter_invariants_compact["passed"]
    )

    quality_gate_failed_reasons = (
        (["reduction_below_target"] if reduction_pct < target_reduction_pct else [])
        + (["critical_parity_failure"] if failed_critical_parity else [])
        + (["counter_integrity_failure"] if not counter_integrity["passed"] else [])
        + (["stage_counter_invariant_failure"] if not stage_counter_invariants_passed else [])
    )

    baseline = {}
    if baseline_summary_path:
        baseline = json.loads(Path(baseline_summary_path).read_text(encoding="utf-8"))

    baseline_compact = int(baseline.get("compact_output_size_bytes", compact_size) or compact_size)
    baseline_reduction = float(baseline.get("percentage_reduction", reduction_pct) or reduction_pct)
    latest_verification_ts = None
    for payload in list(verbose_idx.values()) + list(compact_idx.values()):
        parsed = _parse_timestamp_utc(payload.get("summary_ended_at"))
        if parsed and (latest_verification_ts is None or parsed > latest_verification_ts):
            latest_verification_ts = parsed
    latest_verification_utc = _as_iso_utc(latest_verification_ts) if latest_verification_ts else datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
    verification_matrix = _build_verification_matrix(
        latest_verification_utc=latest_verification_utc,
        run_count_compared=parity.get("run_count_compared", 0),
        reduction_pct=reduction_pct,
        baseline_reduction=baseline_reduction,
        target_reduction_pct=target_reduction_pct,
        failed_critical_parity=failed_critical_parity,
        critical_mismatch_counts=critical_mismatch_counts,
        counter_integrity=counter_integrity,
        stage_counter_invariants_verbose=stage_counter_invariants_verbose,
        stage_counter_invariants_compact=stage_counter_invariants_compact,
    )
    verification_matrix_markdown = _render_verification_matrix_markdown(verification_matrix)

    return {
        "legacy_verbose_output_size_bytes": verbose_size,
        "compact_output_size_bytes": compact_size,
        "percentage_reduction": round(reduction_pct, 2),
        "baseline_compact_output_size_bytes": baseline_compact,
        "incremental_compact_bytes_saved_vs_baseline": baseline_compact - compact_size,
        "incremental_reduction_pct_vs_baseline": round(reduction_pct - baseline_reduction, 2),
        "target_reduction_pct": target_reduction_pct,
        "target_met": reduction_pct >= target_reduction_pct,
        "field_parity": parity,
        "critical_parity_keys": critical_parity_keys,
        "critical_mismatch_counts": critical_mismatch_counts,
        "critical_parity_passed": not failed_critical_parity,
        "counter_integrity": counter_integrity,
        "counter_integrity_passed": counter_integrity["passed"],
        "stage_counter_invariants": {
            "verbose": stage_counter_invariants_verbose,
            "compact": stage_counter_invariants_compact,
        },
        "stage_counter_invariants_passed": stage_counter_invariants_passed,
        "pass_conditions": {
            "size_reduction_threshold_met": reduction_pct >= target_reduction_pct,
            "critical_parity_intact": not failed_critical_parity,
            "counter_integrity_invariants_passed": counter_integrity["passed"],
            "stage_counter_invariants_passed": stage_counter_invariants_passed,
        },
        "quality_gate_failed_reasons": quality_gate_failed_reasons,
        "failed_critical_parity_keys": failed_critical_parity,
        "verification_matrix": verification_matrix,
        "verification_matrix_markdown": verification_matrix_markdown,
    }


def parse_critical_keys(raw):
    if not raw:
        return list(DEFAULT_CRITICAL_PARITY_KEYS)
    keys = [k.strip() for k in raw.split(",") if k.strip()]
    return keys or list(DEFAULT_CRITICAL_PARITY_KEYS)


def main(argv=None):
    parser = argparse.ArgumentParser(
        description="Compare verbose vs compact pipeline logs and enforce deterministic CI quality gates."
    )
    parser.add_argument("verbose_sample", help="Path to verbose profile sample JSON")
    parser.add_argument("compact_sample", help="Path to compact profile sample JSON")
    parser.add_argument(
        "--target-reduction-pct",
        type=float,
        default=60.0,
        help="Minimum required compact size reduction percentage",
    )
    parser.add_argument(
        "--baseline-summary",
        default="",
        help="Optional prior comparison summary JSON for incremental savings deltas.",
    )
    parser.add_argument(
        "--critical-parity-keys",
        default=",".join(DEFAULT_CRITICAL_PARITY_KEYS),
        help="Comma-separated parity groups that must have zero mismatches",
    )
    parser.add_argument(
        "--summary-json-out",
        default="",
        help="Optional output path for machine-readable summary JSON artifact",
    )
    args = parser.parse_args(argv)

    critical_keys = parse_critical_keys(args.critical_parity_keys)
    summary = build_summary(
        args.verbose_sample,
        args.compact_sample,
        args.target_reduction_pct,
        critical_keys,
        args.baseline_summary or None,
    )

    print(json.dumps(summary, indent=2))

    if args.summary_json_out:
        out_path = Path(args.summary_json_out)
        out_path.parent.mkdir(parents=True, exist_ok=True)
        out_path.write_text(json.dumps(summary, indent=2) + "\n", encoding="utf-8")

    return 1 if summary["quality_gate_failed_reasons"] else 0


if __name__ == "__main__":
    raise SystemExit(main())
