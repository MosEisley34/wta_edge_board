#!/usr/bin/env python3
"""Helpers for evaluating and summarizing stake-policy outcomes from Run_Log rows."""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from statistics import mean, median
from typing import Any


DEFAULT_MIN_STAKE_MXN = 10.0
DEFAULT_UNIT_SIZE_MXN = 100.0
DEFAULT_BUCKET_STEP_MXN = 20.0


@dataclass(frozen=True)
class StakePolicyConfig:
    enabled: bool = False
    minimum_stake_mxn: float = DEFAULT_MIN_STAKE_MXN
    round_to_min: bool = False
    unit_size_mxn: float = DEFAULT_UNIT_SIZE_MXN
    min_bet_mxn: float = 20.0
    bucket_step_mxn: float = DEFAULT_BUCKET_STEP_MXN
    bucket_rounding: str = "down"
    stake_mode_by_odds_sign: dict[str, str] = field(
        default_factory=lambda: {"positive": "to_risk", "negative": "to_win"}
    )
    max_bet_mxn: float | None = None

    @classmethod
    def from_legacy(
        cls,
        *,
        enabled: bool = False,
        minimum_stake_mxn: float = DEFAULT_MIN_STAKE_MXN,
        round_to_min: bool = False,
        max_bet_mxn: float | None = None,
    ) -> "StakePolicyConfig":
        min_bet = max(0.0, float(minimum_stake_mxn))
        return cls(
            enabled=bool(enabled),
            minimum_stake_mxn=min_bet,
            round_to_min=bool(round_to_min),
            unit_size_mxn=DEFAULT_UNIT_SIZE_MXN,
            min_bet_mxn=min_bet,
            bucket_step_mxn=DEFAULT_BUCKET_STEP_MXN,
            bucket_rounding="up_to_min" if bool(round_to_min) else "down",
            stake_mode_by_odds_sign={"positive": "to_risk", "negative": "to_win"},
            max_bet_mxn=(None if max_bet_mxn is None else max(0.0, float(max_bet_mxn))),
        )

    def canonical_policy(self) -> dict[str, Any]:
        return {
            "unit_size_mxn": float(self.unit_size_mxn),
            "min_bet_mxn": float(self.min_bet_mxn),
            "bucket_step_mxn": float(self.bucket_step_mxn),
            "bucket_rounding": str(self.bucket_rounding),
            "stake_mode_by_odds_sign": dict(self.stake_mode_by_odds_sign),
            "max_bet_mxn": (None if self.max_bet_mxn is None else float(self.max_bet_mxn)),
        }

    def with_canonicalized_fields(self) -> "StakePolicyConfig":
        chosen_min = self.min_bet_mxn
        if float(self.minimum_stake_mxn) != DEFAULT_MIN_STAKE_MXN and float(self.minimum_stake_mxn) != float(self.min_bet_mxn):
            chosen_min = self.minimum_stake_mxn
        min_bet = max(0.0, float(chosen_min if chosen_min is not None else self.minimum_stake_mxn))
        return StakePolicyConfig(
            enabled=bool(self.enabled),
            minimum_stake_mxn=min_bet,
            round_to_min=bool(self.round_to_min),
            unit_size_mxn=max(0.0, float(self.unit_size_mxn)),
            min_bet_mxn=min_bet,
            bucket_step_mxn=max(0.0, float(self.bucket_step_mxn)),
            bucket_rounding=str(self.bucket_rounding or "down"),
            stake_mode_by_odds_sign=dict(self.stake_mode_by_odds_sign or {"positive": "to_risk", "negative": "to_win"}),
            max_bet_mxn=(None if self.max_bet_mxn is None else max(0.0, float(self.max_bet_mxn))),
        )


def _parse_json(value: Any, fallback: Any) -> Any:
    if value in (None, ""):
        return fallback
    if isinstance(value, (dict, list)):
        return value
    if isinstance(value, str):
        try:
            return json.loads(value)
        except Exception:
            return fallback
    return fallback


def _as_float(value: Any) -> float | None:
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _row_payload(row: dict[str, Any]) -> dict[str, Any]:
    payload: dict[str, Any] = dict(row)
    message = _parse_json(row.get("message"), {})
    if isinstance(message, dict):
        payload.update(message)
    return payload


def _extract_proposed_stake_mxn(row: dict[str, Any]) -> float | None:
    payload = _row_payload(row)
    for key in (
        "stake_mxn",
        "proposed_stake_mxn",
        "recommended_stake_mxn",
        "suggested_stake_mxn",
        "stake",
    ):
        numeric = _as_float(payload.get(key))
        if numeric is not None:
            return numeric
    return None


def _extract_final_risk_mxn(row: dict[str, Any]) -> float | None:
    payload = _row_payload(row)
    for key in (
        "final_risk_mxn",
        "risk_mxn_final",
        "stake_final_risk_mxn",
    ):
        numeric = _as_float(payload.get(key))
        if numeric is not None:
            return numeric
    return None


def _stake_mode_used(row: dict[str, Any], config: StakePolicyConfig) -> str:
    payload = _row_payload(row)
    explicit = str(payload.get("stake_mode_used") or "").strip()
    if explicit:
        return explicit
    odds = _as_float(payload.get("odds_american") if payload.get("odds_american") is not None else payload.get("odds"))
    if odds is None:
        return "unknown"
    sign_key = "negative" if odds < 0 else "positive"
    return str((config.stake_mode_by_odds_sign or {}).get(sign_key) or "unknown")


def _adjustment_reason_code(row: dict[str, Any]) -> str | None:
    payload = _row_payload(row)
    for key in ("stake_adjustment_reason_code", "adjustment_reason_code", "stake_adjust_reason"):
        reason = str(payload.get(key) or "").strip()
        if reason:
            return reason
    return None


def _is_signal_row(row: dict[str, Any], run_id: str) -> bool:
    if str(row.get("run_id") or "") != run_id:
        return False
    stage = str(row.get("stage") or "").strip()
    if stage and stage != "stageGenerateSignals":
        return False
    if _extract_proposed_stake_mxn(row) is not None:
        return True
    row_type = str(row.get("row_type") or "").strip().lower()
    return row_type in {"signal", "diag", "ops", "stage"}


def evaluate_stake_policy_for_row(row: dict[str, Any], config: StakePolicyConfig) -> dict[str, Any]:
    normalized = config.with_canonicalized_fields()
    proposed = _extract_proposed_stake_mxn(row)
    if not normalized.enabled:
        return {
            "decision": "policy_disabled",
            "reason_code": "stake_policy_disabled",
            "proposed_stake_mxn": proposed,
            "final_stake_mxn": proposed,
            "policy": normalized.canonical_policy(),
        }
    if proposed is None:
        return {
            "decision": "missing_stake",
            "reason_code": "stake_missing_unscored",
            "proposed_stake_mxn": None,
            "final_stake_mxn": None,
            "policy": normalized.canonical_policy(),
        }
    min_bet = float(normalized.min_bet_mxn)
    if proposed < min_bet:
        if normalized.round_to_min:
            return {
                "decision": "adjusted",
                "reason_code": "stake_rounded_to_min",
                "proposed_stake_mxn": proposed,
                "final_stake_mxn": min_bet,
                "policy": normalized.canonical_policy(),
            }
        return {
            "decision": "suppressed",
            "reason_code": "stake_below_min_suppressed",
            "proposed_stake_mxn": proposed,
            "final_stake_mxn": 0.0,
            "policy": normalized.canonical_policy(),
        }
    final_stake = proposed
    bucket_step = float(normalized.bucket_step_mxn)
    if bucket_step > 0 and str(normalized.bucket_rounding).lower() == "down":
        final_stake = max(min_bet, bucket_step * int(final_stake / bucket_step))
    if normalized.max_bet_mxn is not None:
        final_stake = min(final_stake, float(normalized.max_bet_mxn))
    return {
        "decision": "passed",
        "reason_code": "stake_policy_pass",
        "proposed_stake_mxn": proposed,
        "final_stake_mxn": final_stake,
        "policy": normalized.canonical_policy(),
    }


def extract_stake_policy_summary(summary_row: dict[str, Any]) -> dict[str, Any]:
    signal_summary = _parse_json(summary_row.get("signal_decision_summary"), {})
    if not isinstance(signal_summary, dict):
        return {}
    candidate = _parse_json(signal_summary.get("stake_policy_summary"), signal_summary.get("stake_policy_summary"))
    return candidate if isinstance(candidate, dict) else {}


def summarize_run_stake_policy(rows: list[dict[str, Any]], run_id: str, config: StakePolicyConfig) -> dict[str, Any]:
    normalized = config.with_canonicalized_fields()
    signal_rows = [row for row in rows if _is_signal_row(row, run_id)]
    reason_counts: dict[str, int] = {}
    stake_mode_counts: dict[str, int] = {}
    adjustment_reason_counts: dict[str, int] = {}
    final_risk_values: list[float] = []
    suppressed = adjusted = passed = missing = 0
    for row in signal_rows:
        outcome = evaluate_stake_policy_for_row(row, normalized)
        reason = str(outcome.get("reason_code") or "unknown")
        reason_counts[reason] = reason_counts.get(reason, 0) + 1

        mode = _stake_mode_used(row, normalized)
        stake_mode_counts[mode] = stake_mode_counts.get(mode, 0) + 1
        adjustment_reason = _adjustment_reason_code(row)
        if adjustment_reason:
            adjustment_reason_counts[adjustment_reason] = adjustment_reason_counts.get(adjustment_reason, 0) + 1
        final_risk = _extract_final_risk_mxn(row)
        if final_risk is not None:
            final_risk_values.append(float(final_risk))

        decision = str(outcome.get("decision") or "")
        if decision == "suppressed":
            suppressed += 1
        elif decision == "adjusted":
            adjusted += 1
        elif decision == "passed":
            passed += 1
        elif decision == "missing_stake":
            missing += 1

    final_risk_summary: dict[str, Any] = {"count": len(final_risk_values)}
    if final_risk_values:
        final_risk_summary["mean"] = float(mean(final_risk_values))
        final_risk_summary["median"] = float(median(final_risk_values))

    return {
        "enabled": bool(config.enabled),
        "minimum_stake_mxn": float(normalized.minimum_stake_mxn),
        "round_to_min": bool(normalized.round_to_min),
        "policy": normalized.canonical_policy(),
        "signal_rows_evaluated": len(signal_rows),
        "suppressed_count": suppressed,
        "adjusted_count": adjusted,
        "passed_count": passed,
        "missing_stake_count": missing,
        "reason_counts": dict(sorted(reason_counts.items())),
        "stake_mode_counts": dict(sorted(stake_mode_counts.items())),
        "adjustment_reason_counts": dict(sorted(adjustment_reason_counts.items())),
        "final_risk_mxn_aggregates": final_risk_summary,
    }
