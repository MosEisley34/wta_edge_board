#!/usr/bin/env python3
import argparse
import json
import sys
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


def canonical_bytes(rows):
    return len(json.dumps(rows, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8"))


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


def load_rows(path):
    raw = json.loads(Path(path).read_text(encoding="utf-8"))
    return [adapt_run_log_record_for_legacy(r) for r in raw]


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

        indexed[run_id] = {
            "gate_reasons": {
                "summary_reason_code": summary.get("reason_code", ""),
                "rejection_codes": rejection_codes,
            },
            "source_selection": source_selection,
            "watchdog": sorted(watchdog, key=lambda x: (x["stage"], x["reason_code"], x["status"])),
            "stage_timing": stage_timing,
        }

    return indexed


def compare_index(verbose_idx, compact_idx):
    run_ids = sorted(set(verbose_idx.keys()) & set(compact_idx.keys()))
    missing_verbose = sorted(set(compact_idx.keys()) - set(verbose_idx.keys()))
    missing_compact = sorted(set(verbose_idx.keys()) - set(compact_idx.keys()))

    mismatch_counts = {
        "gate_reasons": 0,
        "source_selection": 0,
        "watchdog": 0,
        "stage_timing": 0,
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


def build_summary(verbose_path, compact_path, target_reduction_pct, critical_parity_keys):
    verbose_rows = load_rows(verbose_path)
    compact_rows = load_rows(compact_path)

    verbose_size = canonical_bytes(verbose_rows)
    compact_size = canonical_bytes(compact_rows)
    reduction_pct = 0.0
    if verbose_size > 0:
        reduction_pct = ((verbose_size - compact_size) / verbose_size) * 100.0

    parity = compare_index(index_rows(verbose_rows), index_rows(compact_rows))
    mismatch_counts = parity.get("mismatch_counts", {})
    critical_mismatch_counts = {
        key: int(mismatch_counts.get(key, 0))
        for key in critical_parity_keys
    }
    failed_critical_parity = sorted([k for k, v in critical_mismatch_counts.items() if v > 0])

    return {
        "legacy_verbose_output_size_bytes": verbose_size,
        "compact_output_size_bytes": compact_size,
        "percentage_reduction": round(reduction_pct, 2),
        "target_reduction_pct": target_reduction_pct,
        "target_met": reduction_pct >= target_reduction_pct,
        "field_parity": parity,
        "critical_parity_keys": critical_parity_keys,
        "critical_mismatch_counts": critical_mismatch_counts,
        "critical_parity_passed": not failed_critical_parity,
        "quality_gate_failed_reasons": (
            (["reduction_below_target"] if reduction_pct < target_reduction_pct else [])
            + (["critical_parity_failure"] if failed_critical_parity else [])
        ),
        "failed_critical_parity_keys": failed_critical_parity,
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
    )

    print(json.dumps(summary, indent=2))

    if args.summary_json_out:
        out_path = Path(args.summary_json_out)
        out_path.parent.mkdir(parents=True, exist_ok=True)
        out_path.write_text(json.dumps(summary, indent=2) + "\n", encoding="utf-8")

    return 1 if summary["quality_gate_failed_reasons"] else 0


if __name__ == "__main__":
    raise SystemExit(main())
