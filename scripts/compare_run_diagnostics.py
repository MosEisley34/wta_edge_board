#!/usr/bin/env python3
import argparse, csv, glob, json, os, re
from collections import Counter, defaultdict
from typing import Any, Dict, Iterable, List, Tuple

TARGETS = {
    "stageMatchEvents": ["MATCH_CT", "NO_P_MATCH", "REJ_CT"],
    "stageFetchPlayerStats": ["STATS_ENR", "STATS_MISS_A", "STATS_MISS_B"],
    "stageGenerateSignals": ["missing_match", "missing_stats", "EDGE_LOW"],
}
DISALLOWED_RUN_REASON_CODES = {"run_debounced_skip", "run_locked_skip"}
REQUIRED_STAGE_CHAIN = (
    "stageFetchOdds",
    "stageFetchSchedule",
    "stageMatchEvents",
    "stageFetchPlayerStats",
    "stageGenerateSignals",
    "stagePersist",
)


def _parse_message(value: Any) -> Dict[str, Any]:
    if isinstance(value, dict):
        return value
    if not isinstance(value, str):
        return {}
    text = value.strip()
    if not text:
        return {}
    try:
        parsed = json.loads(text)
        return parsed if isinstance(parsed, dict) else {}
    except Exception:
        return {}


def _read_rows(path: str) -> List[Dict[str, Any]]:
    if path.lower().endswith('.json'):
        payload = json.load(open(path, 'r', encoding='utf-8'))
        if isinstance(payload, list):
            return [row for row in payload if isinstance(row, dict)]
        return []
    rows = []
    with open(path, 'r', encoding='utf-8', newline='') as f:
        for row in csv.DictReader(f):
            rows.append(dict(row))
    return rows


def load_rows(export_dir: str) -> List[Dict[str, Any]]:
    files = sorted(glob.glob(os.path.join(export_dir, '**', '*Run_Log*.json'), recursive=True))
    files += sorted(glob.glob(os.path.join(export_dir, '**', '*Run_Log*.csv'), recursive=True))
    rows: List[Dict[str, Any]] = []
    seen = set()
    for p in files:
        if p in seen:
            continue
        seen.add(p)
        rows.extend(_read_rows(p))
    return rows


def _run_rows(rows: List[Dict[str, Any]], run_id: str) -> List[Dict[str, Any]]:
    return [row for row in rows if str(row.get("run_id") or "") == run_id]


def _run_has_disallowed_skip(rows: List[Dict[str, Any]], run_id: str) -> str | None:
    for row in _run_rows(rows, run_id):
        reason = str(row.get("reason_code") or "").strip().lower()
        if reason in DISALLOWED_RUN_REASON_CODES:
            return reason
    return None


def _run_stage_chain(rows: List[Dict[str, Any]], run_id: str) -> List[str]:
    stages: List[str] = []
    for row in _run_rows(rows, run_id):
        stage = str(row.get("stage") or "").strip()
        if stage and stage != "runEdgeBoard" and stage not in stages:
            stages.append(stage)
    if stages:
        return stages

    for row in _run_rows(rows, run_id):
        if str(row.get("row_type") or "") != "summary" or str(row.get("stage") or "") != "runEdgeBoard":
            continue
        message = _parse_message(row.get("message"))
        stage_summaries = message.get("stage_summaries")
        if not isinstance(stage_summaries, list):
            continue
        for summary in stage_summaries:
            if not isinstance(summary, dict):
                continue
            stage = str(summary.get("stage") or "").strip()
            if stage and stage not in stages:
                stages.append(stage)
    return stages


def _validate_run_pair(rows: List[Dict[str, Any]], run_a: str, run_b: str) -> None:
    failures: List[str] = []
    for run_id in (run_a, run_b):
        disallowed_reason = _run_has_disallowed_skip(rows, run_id)
        if disallowed_reason:
            failures.append(f"{run_id}: disallowed reason_code `{disallowed_reason}`")
        stage_chain = _run_stage_chain(rows, run_id)
        missing_stages = [stage for stage in REQUIRED_STAGE_CHAIN if stage not in stage_chain]
        if missing_stages:
            failures.append(
                f"{run_id}: missing stage chain entries ({', '.join(missing_stages)})"
            )
    if failures:
        raise ValueError(
            "Comparison auto-failed: replacement run IDs required before producing pre/post verdict. "
            f"Details: {'; '.join(failures)}."
        )


def reason_distribution(rows: List[Dict[str, Any]], run_id: str, stage: str) -> Counter:
    counts = Counter()
    for r in rows:
        if str(r.get('run_id') or '') != run_id:
            continue
        stage_name = str(r.get('stage') or '')
        if stage_name != stage and str(r.get('row_type') or '') not in ('summary', 'stage', 'ops', 'diag'):
            continue
        reason = str(r.get('reason_code') or '').strip()
        if reason:
            counts[reason] += 1
        msg = _parse_message(r.get('message'))
        rc = msg.get('reason_codes') if isinstance(msg, dict) else None
        if isinstance(rc, dict):
            for k, v in rc.items():
                try:
                    counts[str(k)] += int(v)
                except Exception:
                    pass
        summaries = msg.get('stage_summaries') if isinstance(msg, dict) else None
        if isinstance(summaries, list):
            for s in summaries:
                if not isinstance(s, dict) or str(s.get('stage') or '') != stage:
                    continue
                for k, v in (s.get('reason_codes') or {}).items():
                    try:
                        counts[str(k)] += int(v)
                    except Exception:
                        pass
    return counts


def collect_pairs(rows: List[Dict[str, Any]], run_id: str) -> Counter:
    pair_counts = Counter()
    for r in rows:
        if str(r.get('run_id') or '') != run_id:
            continue
        reason = str(r.get('reason_code') or '')
        if not reason or reason in ('MATCH_CT', 'STATS_ENR'):
            continue
        msg = _parse_message(r.get('message'))
        odds_event = str(r.get('odds_event_id') or msg.get('odds_event_id') or '').strip()
        sched_event = str(r.get('schedule_event_id') or msg.get('schedule_event_id') or '').strip()
        pa = str(msg.get('player_a') or msg.get('odds_player_a') or msg.get('home_player') or '').strip()
        pb = str(msg.get('player_b') or msg.get('odds_player_b') or msg.get('away_player') or '').strip()
        npa = str(msg.get('normalized_player_a') or msg.get('norm_player_a') or '').strip()
        npb = str(msg.get('normalized_player_b') or msg.get('norm_player_b') or '').strip()
        key = (odds_event or '-', sched_event or '-', pa or '-', pb or '-', npa or '-', npb or '-', reason)
        pair_counts[key] += 1
    return pair_counts


def canonical_counts(counter: Counter, expected: Iterable[str]) -> Dict[str, int]:
    expected = list(expected)
    out = {k: int(counter.get(k, 0)) for k in expected}
    fallback = sum(v for k, v in counter.items() if 'fallback' in k.lower() and k not in out)
    out['fallback_categories'] = int(fallback)
    return out


def _extract_reason_metadata(rows: List[Dict[str, Any]], run_id: str) -> Dict[str, Any]:
    candidates: List[Dict[str, Any]] = []
    for row in _run_rows(rows, run_id):
        if str(row.get("stage") or "") != "stageFetchPlayerStats":
            continue
        message = _parse_message(row.get("message"))
        if isinstance(message.get("reason_metadata"), dict):
            candidates.append(message["reason_metadata"])
    for row in _run_rows(rows, run_id):
        if str(row.get("row_type") or "") != "summary" or str(row.get("stage") or "") != "runEdgeBoard":
            continue
        message = _parse_message(row.get("message"))
        stage_summaries = message.get("stage_summaries")
        if not isinstance(stage_summaries, list):
            continue
        for stage_summary in stage_summaries:
            if not isinstance(stage_summary, dict):
                continue
            if str(stage_summary.get("stage") or "") != "stageFetchPlayerStats":
                continue
            metadata = stage_summary.get("reason_metadata")
            if isinstance(metadata, str):
                metadata = _parse_message(metadata)
            if isinstance(metadata, dict):
                candidates.append(metadata)
    return candidates[-1] if candidates else {}


def _pick_int(meta: Dict[str, Any], keys: Tuple[str, ...]) -> int:
    for key in keys:
        try:
            return int(float(meta.get(key)))
        except Exception:
            continue
    return 0


def _pick_reason_counts(meta: Dict[str, Any]) -> Dict[str, int]:
    for key in (
        "fallback_reason_counts",
        "top_fallback_reason_counts",
        "fallback_reason_breakdown",
        "fallback_reasons",
    ):
        value = meta.get(key)
        if not isinstance(value, dict):
            continue
        out: Dict[str, int] = {}
        for reason in sorted(value, key=lambda v: str(v)):
            try:
                out[str(reason)] = int(float(value[reason]))
            except Exception:
                continue
        if out:
            return out
    return {}


def _player_stats_snapshot(rows: List[Dict[str, Any]], run_id: str) -> Dict[str, Any]:
    meta = _extract_reason_metadata(rows, run_id)
    resolved = max(0, _pick_int(meta, ("resolved_player_count", "resolved_players_count")))
    total = max(0, _pick_int(meta, ("requested_player_count", "total_player_count", "total_players_count")))
    unresolved_total = max(0, _pick_int(meta, ("unresolved_player_count", "unresolved_players_count")))
    if total == 0:
        total = resolved + unresolved_total
    ta = max(0, _pick_int(meta, ("resolved_via_ta_count", "ta_resolved_count")))
    provider_fb = max(0, _pick_int(meta, ("resolved_via_provider_fallback_count", "provider_fallback_resolved_count")))
    model_fb = max(0, _pick_int(meta, ("resolved_via_model_fallback_count", "model_fallback_resolved_count")))
    unresolved_a = max(0, _pick_int(meta, ("unresolved_player_a_count", "unresolved_side_a_count")))
    unresolved_b = max(0, _pick_int(meta, ("unresolved_player_b_count", "unresolved_side_b_count")))
    return {
        "resolved": resolved,
        "total": total,
        "ta": ta,
        "provider_fb": provider_fb,
        "model_fb": model_fb,
        "unresolved_a": unresolved_a,
        "unresolved_b": unresolved_b,
        "reasons": _pick_reason_counts(meta),
    }


def compare_rows(rows: List[Dict[str, Any]], run_a: str, run_b: str) -> str:
    _validate_run_pair(rows, run_a, run_b)
    lines = []
    lines.append(f"# Run diff report: {run_a} vs {run_b}")
    total_a = sum(1 for r in rows if str(r.get('run_id') or '') == run_a)
    total_b = sum(1 for r in rows if str(r.get('run_id') or '') == run_b)
    lines.append(f"\nRows found — {run_a}: {total_a}, {run_b}: {total_b}")
    if total_a == 0 or total_b == 0:
        lines.append("\n## Data availability")
        lines.append("One or both run IDs were not found in available Run_Log artifacts under the chosen export directory.")
        return "\n".join(lines)

    for stage, expected in TARGETS.items():
        ca = reason_distribution(rows, run_a, stage)
        cb = reason_distribution(rows, run_b, stage)
        a = canonical_counts(ca, expected)
        b = canonical_counts(cb, expected)
        lines.append(f"\n## {stage}")
        lines.append("| reason_code | successful | degraded | delta (degraded-successful) |")
        lines.append("|---|---:|---:|---:|")
        for code in list(expected) + ['fallback_categories']:
            da = a.get(code, 0)
            db = b.get(code, 0)
            lines.append(f"| {code} | {da} | {db} | {db-da:+d} |")

    # Canonicalization-focused counts
    lines.append("\n## Canonicalization focus deltas")
    focus_codes = [
        'fallback_match', 'fallback_exhausted', 'schedule_enrichment_h2h_missing',
        'schedule_enrichment_h2h_missing_player_not_found',
        'schedule_enrichment_h2h_missing_source_dataset_unavailable',
        'schedule_enrichment_h2h_missing_matrix_gap',
    ]
    rc_a, rc_b = Counter(), Counter()
    for stage in TARGETS:
        rc_a.update(reason_distribution(rows, run_a, stage))
        rc_b.update(reason_distribution(rows, run_b, stage))
    lines.append("| reason_code | successful | degraded | delta |")
    lines.append("|---|---:|---:|---:|")
    for code in focus_codes:
        da, db = int(rc_a.get(code, 0)), int(rc_b.get(code, 0))
        lines.append(f"| {code} | {da} | {db} | {db-da:+d} |")

    pairs_a = collect_pairs(rows, run_a)
    pairs_b = collect_pairs(rows, run_b)
    delta_pairs = Counter()
    all_keys = set(pairs_a) | set(pairs_b)
    for k in all_keys:
        delta = pairs_b.get(k, 0) - pairs_a.get(k, 0)
        if delta > 0:
            delta_pairs[k] = delta

    lines.append("\n## Top failing player/event pairs (degraded-only deltas)")
    lines.append("| odds_event_id | schedule_event_id | player_a | player_b | norm_a | norm_b | reason_code | delta |")
    lines.append("|---|---|---|---|---|---|---|---:|")
    for (oe, se, pa, pb, npa, npb, rc), delta in delta_pairs.most_common(25):
        lines.append(f"| {oe} | {se} | {pa} | {pb} | {npa} | {npb} | {rc} | {delta} |")

    if not delta_pairs:
        lines.append("| - | - | - | - | - | - | - | 0 |")

    stats_a = _player_stats_snapshot(rows, run_a)
    stats_b = _player_stats_snapshot(rows, run_b)
    cov_a = (100.0 * stats_a["resolved"] / stats_a["total"]) if stats_a["total"] else 0.0
    cov_b = (100.0 * stats_b["resolved"] / stats_b["total"]) if stats_b["total"] else 0.0

    lines.append("\n## stageFetchPlayerStats coverage")
    lines.append("| metric | successful | degraded | delta |")
    lines.append("|---|---:|---:|---:|")
    lines.append(f"| resolved/total | {stats_a['resolved']}/{stats_a['total']} | {stats_b['resolved']}/{stats_b['total']} | - |")
    lines.append(f"| coverage_pct | {cov_a:.2f}% | {cov_b:.2f}% | {cov_b-cov_a:+.2f} pp |")

    lines.append("\n## stageFetchPlayerStats source mix deltas")
    lines.append("| source_bucket | successful | degraded | delta |")
    lines.append("|---|---:|---:|---:|")
    for bucket in ("ta", "provider_fb", "model_fb"):
        lines.append(f"| {bucket} | {stats_a[bucket]} | {stats_b[bucket]} | {stats_b[bucket]-stats_a[bucket]:+d} |")

    lines.append("\n## stageFetchPlayerStats unresolved players by side")
    lines.append("| side | successful | degraded | delta |")
    lines.append("|---|---:|---:|---:|")
    for side_key, label in (("unresolved_a", "player_a"), ("unresolved_b", "player_b")):
        lines.append(
            f"| {label} | {stats_a[side_key]} | {stats_b[side_key]} | {stats_b[side_key]-stats_a[side_key]:+d} |"
        )

    reason_a = Counter(stats_a["reasons"])
    reason_b = Counter(stats_b["reasons"])
    top_reason_codes = sorted(
        set(reason_a) | set(reason_b),
        key=lambda code: (-(reason_a[code] + reason_b[code]), code),
    )[:10]
    lines.append("\n## stageFetchPlayerStats top fallback reason deltas")
    lines.append("| reason | successful | degraded | delta |")
    lines.append("|---|---:|---:|---:|")
    if top_reason_codes:
        for code in top_reason_codes:
            lines.append(f"| {code} | {reason_a[code]} | {reason_b[code]} | {reason_b[code]-reason_a[code]:+d} |")
    else:
        lines.append("| none | 0 | 0 | +0 |")

    return "\n".join(lines)


def main() -> int:
    ap = argparse.ArgumentParser(
        description='Compare diagnostics between two run IDs from exported Run_Log artifacts.',
        epilog=(
            'Usage: python3 scripts/compare_run_diagnostics.py <run_success> <run_degraded> '
            '[--export-dir ./exports_live] [--out ./tmp/run_diff.md]\n'
            'Note: run IDs are positional arguments; do not pass legacy flags like --run-log/--require.'
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    ap.add_argument('run_success', help='Baseline/healthy run ID (positional).')
    ap.add_argument('run_degraded', help='Candidate degraded run ID (positional).')
    ap.add_argument(
        '--export-dir',
        default='runtime_exports',
        help='Directory containing exported runtime artifacts (default: runtime_exports).',
    )
    ap.add_argument('--out', default='', help='Optional markdown output path.')
    args = ap.parse_args()

    rows = load_rows(args.export_dir)
    report = compare_rows(rows, args.run_success, args.run_degraded)
    if args.out:
        os.makedirs(os.path.dirname(args.out), exist_ok=True)
        with open(args.out, 'w', encoding='utf-8') as f:
            f.write(report + '\n')
    print(report)
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
