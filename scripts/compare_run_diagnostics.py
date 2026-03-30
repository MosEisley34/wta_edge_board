#!/usr/bin/env python3
import argparse, csv, glob, json, os, re
from collections import Counter, defaultdict
from typing import Any, Dict, Iterable, List, Tuple
from check_player_stats_coverage import GateConfig, evaluate_player_stats_gate
from preflight_guard import enforce_preflight_guard
from stake_policy import StakePolicyConfig, summarize_run_stake_policy
from runtime_artifact_codec import normalize_run_log_row

TARGETS = {
    "stageMatchEvents": ["MATCH_CT", "NO_P_MATCH", "REJ_CT"],
    "stageFetchPlayerStats": ["STATS_ENR", "STATS_MISS_A", "STATS_MISS_B"],
    "stageGenerateSignals": ["missing_match", "missing_stats", "EDGE_LOW"],
}
NO_HIT_COUNTER_FIELDS = (
    "no_hit_no_events_from_source_count",
    "no_hit_events_outside_time_window_count",
    "no_hit_tournament_filter_excluded_count",
    "no_hit_odds_present_but_match_failed_count",
    "no_hit_schema_invalid_metrics_count",
)
DISALLOWED_RUN_REASON_CODES = {"run_debounced_skip", "run_locked_skip"}
REQUIRED_STAGE_CHAIN = (
    "stageFetchOdds",
    "stageFetchSchedule",
    "stageMatchEvents",
    "stageFetchPlayerStats",
    "stageGenerateSignals",
    "stagePersist",
)
DEBUG_WATERMARK = (
    "### NON-APPROVAL DEBUG OUTPUT ### "
    "gate failed; verdict publication remains blocked."
)


def _with_reason_code_fallback(report: Dict[str, Any], prefix: str = "gate") -> Dict[str, Any]:
    normalized = dict(report) if isinstance(report, dict) else {}
    status = str(normalized.get("status") or "unknown").strip().lower()
    reason_code = str(normalized.get("reason_code") or "").strip()
    if status != "pass" and not reason_code:
        status_token = re.sub(r"[^a-z0-9_]+", "_", status) or "unknown"
        normalized["reason_code"] = f"{prefix}_{status_token}_no_reason_code"
    return normalized


def _emit_error(reason_code: str, message: str, **extra: Any) -> None:
    payload: Dict[str, Any] = {"status": "error", "reason_code": reason_code, "message": message}
    payload.update(extra)
    print(json.dumps(payload, indent=2, sort_keys=True))


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


def _parse_json(value: Any, fallback: Any) -> Any:
    if value is None or value == "":
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


def _normalize_stage_summaries_payload(value: Any) -> List[Dict[str, Any]]:
    parsed = _parse_json(value, None)
    if isinstance(parsed, list):
        return [row for row in parsed if isinstance(row, dict)]
    if isinstance(parsed, dict):
        nested = parsed.get("stage_summaries")
        if isinstance(nested, list):
            return [row for row in nested if isinstance(row, dict)]
    return []


def _extract_stage_summaries(row: Dict[str, Any]) -> List[Dict[str, Any]]:
    stage_summaries = _normalize_stage_summaries_payload(row.get("stage_summaries"))
    if stage_summaries:
        return stage_summaries
    return _normalize_stage_summaries_payload(row.get("message"))


def _read_rows(path: str) -> List[Dict[str, Any]]:
    if path.lower().endswith('.json'):
        payload = json.load(open(path, 'r', encoding='utf-8'))
        if isinstance(payload, list):
            return [normalize_run_log_row(dict(row)) for row in payload if isinstance(row, dict)]
        return []
    rows = []
    with open(path, 'r', encoding='utf-8', newline='') as f:
        for row in csv.DictReader(f):
            rows.append(normalize_run_log_row(dict(row)))
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
        for summary in _extract_stage_summaries(row):
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


def _pick_run_summary(rows: List[Dict[str, Any]], run_id: str) -> Dict[str, Any]:
    last: Dict[str, Any] = {}
    for row in _run_rows(rows, run_id):
        if str(row.get("row_type") or "") == "summary" and str(row.get("stage") or "") == "runEdgeBoard":
            last = row
    return last


def _no_hit_counters(summary: Dict[str, Any]) -> Dict[str, int]:
    counters: Dict[str, int] = {}
    for field in NO_HIT_COUNTER_FIELDS:
        try:
            counters[field] = int(float(summary.get(field, 0) or 0))
        except Exception:
            counters[field] = 0
    return counters


def _no_hit_terminal_reason(summary: Dict[str, Any]) -> str:
    return str(summary.get("no_hit_terminal_reason_code") or "none")


def _summary_stake_policy_enabled(summary: Dict[str, Any]) -> bool | None:
    signal_summary = _parse_json(summary.get("signal_decision_summary"), {})
    if not isinstance(signal_summary, dict):
        return None
    stake_summary = _parse_json(signal_summary.get("stake_policy_summary"), {})
    if not isinstance(stake_summary, dict):
        return None
    enabled = stake_summary.get("enabled")
    if isinstance(enabled, bool):
        return enabled
    if isinstance(enabled, str):
        text = enabled.strip().lower()
        if text in {"true", "1", "yes", "y"}:
            return True
        if text in {"false", "0", "no", "n"}:
            return False
    return None


def _resolve_pair_policy_mode(rows: List[Dict[str, Any]], run_a: str, run_b: str, fallback_enabled: bool) -> bool:
    observed: List[tuple[str, bool]] = []
    for run_id in (run_a, run_b):
        explicit = _summary_stake_policy_enabled(_pick_run_summary(rows, run_id))
        if explicit is not None:
            observed.append((run_id, explicit))
    if observed:
        observed_values = {value for _, value in observed}
        if len(observed_values) > 1:
            details = ", ".join(f"{run_id}={str(value).lower()}" for run_id, value in observed)
            raise ValueError(
                "Mixed stake_policy_enabled states detected within run pair; "
                f"refuse to produce comparison report. pair={details}"
            )
        return bool(observed[0][1])
    return bool(fallback_enabled)


def _debug_gate_failure_report(
    rows: List[Dict[str, Any]],
    run_success: str,
    run_degraded: str,
    gate_report: Dict[str, Any],
) -> str:
    lines: List[str] = [DEBUG_WATERMARK]
    lines.append("# gate_report")
    lines.append(json.dumps(gate_report, indent=2, sort_keys=True))
    for run_id in (run_success, run_degraded):
        lines.append(f"\n## run={run_id} stage_chain_presence")
        stage_chain = _run_stage_chain(rows, run_id)
        if stage_chain:
            lines.append(f"present_stages: {', '.join(stage_chain)}")
        else:
            lines.append("present_stages: none")
        missing_stages = [stage for stage in REQUIRED_STAGE_CHAIN if stage not in stage_chain]
        lines.append(f"missing_required_stages: {', '.join(missing_stages) if missing_stages else 'none'}")

        lines.append(f"\n## run={run_id} reason_code_distributions")
        for stage in TARGETS:
            counts = reason_distribution(rows, run_id, stage)
            lines.append(f"{stage}: {json.dumps(dict(sorted(counts.items())), sort_keys=True)}")

        lines.append(f"\n## run={run_id} discovered_coverage_metadata_payload")
        lines.append(json.dumps(_extract_reason_metadata(rows, run_id), indent=2, sort_keys=True))
    return "\n".join(lines)


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
        for s in _extract_stage_summaries(r):
            if str(s.get('stage') or '') != stage:
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
        for stage_summary in _extract_stage_summaries(row):
            if str(stage_summary.get("stage") or "") != "stageFetchPlayerStats":
                continue
            metadata = stage_summary.get("reason_metadata")
            stage_message = _parse_message(stage_summary.get("message"))
            if isinstance(metadata, str):
                metadata = _parse_message(metadata)
            if not isinstance(metadata, dict) and isinstance(stage_message.get("reason_metadata"), dict):
                metadata = stage_message.get("reason_metadata")
            if isinstance(metadata, dict):
                coverage = _parse_json(metadata.get("coverage"), {})
                if not isinstance(coverage, dict) and isinstance(stage_message.get("coverage"), dict):
                    coverage = stage_message.get("coverage")
                if isinstance(coverage, dict) and coverage:
                    metadata = dict(metadata)
                    metadata.setdefault("requested_player_count", coverage.get("requested"))
                    metadata.setdefault("requested_player_count", coverage.get("requested_players"))
                    metadata.setdefault("resolved_player_count", coverage.get("resolved"))
                    metadata.setdefault("resolved_player_count", coverage.get("resolved_players"))
                    metadata.setdefault("unresolved_player_count", coverage.get("unresolved"))
                    metadata.setdefault("unresolved_player_count", coverage.get("unresolved_players"))
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
    resolved = max(0, _pick_int(meta, ("resolved_player_count", "resolved_players_count", "resolved_count")))
    total = max(
        0,
        _pick_int(meta, ("requested_player_count", "total_player_count", "total_players_count", "requested_players_count")),
    )
    unresolved_total = max(0, _pick_int(meta, ("unresolved_player_count", "unresolved_players_count")))
    if total == 0:
        total = resolved + unresolved_total
    ta = max(0, _pick_int(meta, ("resolved_via_ta_count", "ta_resolved_count", "players_found_ta")))
    provider_fb = max(
        0,
        _pick_int(meta, ("resolved_via_provider_fallback_count", "provider_fallback_resolved_count", "players_fallback_provider")),
    )
    model_fb = max(
        0,
        _pick_int(meta, ("resolved_via_model_fallback_count", "model_fallback_resolved_count", "players_fallback_model")),
    )
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


def compare_rows(
    rows: List[Dict[str, Any]],
    run_a: str,
    run_b: str,
    stake_policy_config: StakePolicyConfig | None = None,
) -> str:
    _validate_run_pair(rows, run_a, run_b)
    stake_config = stake_policy_config or StakePolicyConfig()
    pair_policy_enabled = _resolve_pair_policy_mode(rows, run_a, run_b, fallback_enabled=bool(stake_config.enabled))
    if pair_policy_enabled != bool(stake_config.enabled):
        raise ValueError(
            "stake_policy_enabled mode mismatch between CLI/config and run summaries; "
            f"pair_reports require homogeneous mode. summary_mode={str(pair_policy_enabled).lower()} "
            f"requested_mode={str(bool(stake_config.enabled)).lower()}"
        )
    lines = []
    canonical_policy = stake_config.with_canonicalized_fields().canonical_policy()
    lines.append(f"stake_policy_enabled={str(pair_policy_enabled).lower()}")
    lines.append(f"stake_policy={json.dumps(canonical_policy, sort_keys=True)}")
    lines.append(f"unit_size_mxn={canonical_policy['unit_size_mxn']}")
    lines.append(f"min_bet_mxn={canonical_policy['min_bet_mxn']}")
    lines.append(f"bucket_step_mxn={canonical_policy['bucket_step_mxn']}")
    lines.append(f"rounding_mode={canonical_policy['bucket_rounding']}")
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

    no_hit_a = _no_hit_counters(_pick_run_summary(rows, run_a))
    no_hit_b = _no_hit_counters(_pick_run_summary(rows, run_b))
    lines.append("\n## runEdgeBoard no-hit reason counters")
    lines.append("| counter | successful | degraded | delta |")
    lines.append("|---|---:|---:|---:|")
    for field in NO_HIT_COUNTER_FIELDS:
        va = int(no_hit_a.get(field, 0))
        vb = int(no_hit_b.get(field, 0))
        lines.append(f"| {field} | {va} | {vb} | {vb-va:+d} |")
    lines.append("\n## runEdgeBoard terminal no-hit reason")
    lines.append(f"- successful: `{_no_hit_terminal_reason(_pick_run_summary(rows, run_a))}`")
    lines.append(f"- degraded: `{_no_hit_terminal_reason(_pick_run_summary(rows, run_b))}`")

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

    stake_a = summarize_run_stake_policy(rows, run_a, stake_config)
    stake_b = summarize_run_stake_policy(rows, run_b, stake_config)
    lines.append("\n## stake-policy outcomes")
    lines.append("| metric | successful | degraded | delta |")
    lines.append("|---|---:|---:|---:|")
    for key in ("signal_rows_evaluated", "suppressed_count", "adjusted_count", "passed_count", "missing_stake_count"):
        lines.append(f"| {key} | {stake_a[key]} | {stake_b[key]} | {stake_b[key]-stake_a[key]:+d} |")

    reason_a = Counter(stake_a["reason_counts"])
    reason_b = Counter(stake_b["reason_counts"])
    lines.append("\n## stake-policy reason codes")
    lines.append("| reason_code | successful | degraded | delta |")
    lines.append("|---|---:|---:|---:|")
    reason_codes = sorted(set(reason_a) | set(reason_b))
    if reason_codes:
        for code in reason_codes:
            lines.append(f"| {code} | {reason_a[code]} | {reason_b[code]} | {reason_b[code]-reason_a[code]:+d} |")
    else:
        lines.append("| none | 0 | 0 | +0 |")

    mode_a = Counter(stake_a["stake_mode_counts"])
    mode_b = Counter(stake_b["stake_mode_counts"])
    lines.append("\n## stake-policy stake_mode_used counts")
    lines.append("| stake_mode_used | successful | degraded | delta |")
    lines.append("|---|---:|---:|---:|")
    mode_codes = sorted(set(mode_a) | set(mode_b))
    if mode_codes:
        for code in mode_codes:
            lines.append(f"| {code} | {mode_a[code]} | {mode_b[code]} | {mode_b[code]-mode_a[code]:+d} |")
    else:
        lines.append("| none | 0 | 0 | +0 |")

    adj_a = Counter(stake_a["adjustment_reason_counts"])
    adj_b = Counter(stake_b["adjustment_reason_counts"])
    lines.append("\n## stake-policy adjustment reason codes")
    lines.append("| adjustment_reason_code | successful | degraded | delta |")
    lines.append("|---|---:|---:|---:|")
    adj_codes = sorted(set(adj_a) | set(adj_b))
    if adj_codes:
        for code in adj_codes:
            lines.append(f"| {code} | {adj_a[code]} | {adj_b[code]} | {adj_b[code]-adj_a[code]:+d} |")
    else:
        lines.append("| none | 0 | 0 | +0 |")

    risk_a = stake_a.get("final_risk_mxn_aggregates") or {}
    risk_b = stake_b.get("final_risk_mxn_aggregates") or {}
    lines.append("\n## stake-policy final_risk_mxn aggregates")
    lines.append("| metric | successful | degraded | delta |")
    lines.append("|---|---:|---:|---:|")
    for metric in ("count", "mean", "median"):
        va = float(risk_a.get(metric, 0.0) or 0.0)
        vb = float(risk_b.get(metric, 0.0) or 0.0)
        if metric == "count":
            lines.append(f"| {metric} | {int(va)} | {int(vb)} | {int(vb-va):+d} |")
        else:
            lines.append(f"| {metric} | {va:.4f} | {vb:.4f} | {vb-va:+.4f} |")

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
    ap.add_argument(
        '--skip-player-stats-coverage-gate',
        action='store_true',
        help='Skip player-stats coverage gate (not recommended; intended for emergency/manual debugging only).',
    )
    ap.add_argument(
        '--emergency-preflight-override-tag',
        default='',
        help=(
            'Emergency-only override when preflight sidecar is missing. '
            'Requires incident tag format <LETTERS>-<NNN> (example: INC-1234).'
        ),
    )
    ap.add_argument(
        '--player-stats-gate-override-reason',
        default=os.getenv('PLAYER_STATS_COVERAGE_GATE_OVERRIDE', ''),
        help='Override reason for player-stats gate failures; non-empty value allows report publication.',
    )
    ap.add_argument(
        '--player-stats-min-resolved-rate',
        type=float,
        default=float(os.getenv('PLAYER_STATS_MIN_RESOLVED_RATE', '0.60')),
        help='Minimum candidate resolved rate for player-stats gate (default: 0.60).',
    )
    ap.add_argument(
        '--player-stats-max-unresolved-players',
        type=int,
        default=int(os.getenv('PLAYER_STATS_MAX_UNRESOLVED_PLAYERS', '8')),
        help='Maximum candidate unresolved players for player-stats gate (default: 8).',
    )
    ap.add_argument(
        '--player-stats-max-missing-side-increase',
        type=int,
        default=int(os.getenv('PLAYER_STATS_MAX_MISSING_SIDE_INCREASE', '0')),
        help='Maximum allowed increase in STATS_MISS_A/STATS_MISS_B vs baseline (default: 0).',
    )
    ap.add_argument(
        '--emit-debug-on-gate-fail',
        action='store_true',
        help=(
            'Emit non-verdict diagnostics when player-stats gate fails. '
            'Output is explicitly non-approval and verdict publication remains blocked.'
        ),
    )
    ap.add_argument("--stake-policy-enabled", action="store_true", help="Enable stake-policy evaluation counters.")
    ap.add_argument("--stake-policy-min-stake-mxn", type=float, default=20.0, help="Minimum MXN stake floor (default: 20).")
    ap.add_argument("--stake-policy-round-to-min", action="store_true", help="Adjust below-min stake to floor instead of suppressing.")
    ap.add_argument('--out', default='', help='Optional markdown output path.')
    args = ap.parse_args()
    try:
        preflight_status = enforce_preflight_guard(
            args.export_dir,
            args.run_success,
            args.run_degraded,
            args.emergency_preflight_override_tag,
        )
        if preflight_status.get('status') == 'emergency_override':
            print(
                '# preflight_guard: emergency override active '
                f"(incident_tag={preflight_status.get('incident_tag')})"
            )
            print(
                '# preflight_evidence: MISSING (override mode); '
                'rerun scripts/export_parity_precheck.sh to generate run_compare_preflight.json'
            )
        else:
            print(f"# preflight_evidence: {preflight_status.get('sidecar_path', '')}")

        rows = load_rows(args.export_dir)
        if not args.skip_player_stats_coverage_gate:
            gate_config = GateConfig(
                min_resolved_rate=max(0.0, min(1.0, float(args.player_stats_min_resolved_rate))),
                max_unresolved_players=max(0, int(args.player_stats_max_unresolved_players)),
                max_missing_side_increase=max(0, int(args.player_stats_max_missing_side_increase)),
                override_reason=str(args.player_stats_gate_override_reason or '').strip(),
            )
            gate_report = _with_reason_code_fallback(
                evaluate_player_stats_gate(rows, args.run_success, args.run_degraded, gate_config),
                prefix="gate",
            )
            coverage_gate = str(gate_report.get('coverage_gate') or gate_report.get('status') or 'unknown')
            schema_integrity = str(gate_report.get('schema_integrity') or 'pass')
            print(
                '# operator_summary: '
                f'coverage_gate={coverage_gate} schema_integrity={schema_integrity}'
            )
            if gate_report.get('override_used') and schema_integrity == 'fail':
                print(
                    '# WARNING: player-stats coverage override is active, but schema faults remain; '
                    'result stays schema_missing (override does not bypass schema integrity).'
                )
            if gate_report.get('status') in {'fail', 'schema_missing'}:
                if args.emit_debug_on_gate_fail:
                    print(_debug_gate_failure_report(rows, args.run_success, args.run_degraded, gate_report))
                else:
                    print(json.dumps(gate_report, indent=2, sort_keys=True))
                _emit_error(
                    str(gate_report.get("reason_code") or "gate_fail_no_reason_code"),
                    'Player-stats coverage/schema gate failed; aborting pre/post verdict publication. '
                    'Use --player-stats-gate-override-reason only for approved coverage-threshold exceptions.',
                )
                return 2
            if gate_report.get('status') == 'override':
                print('# player_stats_coverage_gate: override active')
                print(json.dumps(gate_report, indent=2, sort_keys=True))
            if gate_report.get('status') == 'warn':
                print('# player_stats_coverage_gate: pass_with_warning')
                print(json.dumps(gate_report, indent=2, sort_keys=True))

        stake_policy_config = StakePolicyConfig.from_legacy(
            enabled=bool(args.stake_policy_enabled),
            minimum_stake_mxn=max(0.0, float(args.stake_policy_min_stake_mxn)),
            round_to_min=bool(args.stake_policy_round_to_min),
        )
        report = compare_rows(rows, args.run_success, args.run_degraded, stake_policy_config=stake_policy_config)
        if args.out:
            os.makedirs(os.path.dirname(args.out), exist_ok=True)
            with open(args.out, 'w', encoding='utf-8') as f:
                f.write(report + '\n')
        print(report)
        return 0
    except ValueError as exc:
        _emit_error("compare_validation_failed", str(exc))
        return 1
    except FileNotFoundError as exc:
        _emit_error("compare_input_not_found", str(exc))
        return 1
    except Exception as exc:
        _emit_error("compare_unexpected_error", str(exc))
        return 1


if __name__ == '__main__':
    raise SystemExit(main())
