import json
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[2]
SCRIPTS_DIR = ROOT / "scripts"
sys.path.insert(0, str(SCRIPTS_DIR))

from stake_policy import StakePolicyConfig, evaluate_stake_policy_for_row, summarize_run_stake_policy


def _load_fixture():
    fixture_path = ROOT / "scripts" / "fixtures" / "stake_policy_mixed_signal_rows.json"
    return json.loads(fixture_path.read_text(encoding="utf-8"))


def test_strict_mode_suppresses_below_min_from_mixed_fixture_rows():
    fixture = _load_fixture()
    run_id = fixture["run_id"]
    rows = fixture["rows"]
    expected = fixture["expected"]["strict"]
    config = StakePolicyConfig(enabled=True, minimum_stake_mxn=20.0, round_to_min=False)

    summary = summarize_run_stake_policy(rows, run_id, config)
    for key, value in expected["counts"].items():
        assert summary[key] == value

    reason_codes = {
        row["case_id"]: evaluate_stake_policy_for_row(row, config)["reason_code"]
        for row in rows
        if row.get("case_id") in expected["decision_by_case_id"]
    }
    assert reason_codes == expected["decision_by_case_id"]


def test_round_up_mode_adjusts_below_min_from_mixed_fixture_rows():
    fixture = _load_fixture()
    run_id = fixture["run_id"]
    rows = fixture["rows"]
    expected = fixture["expected"]["round_up"]
    config = StakePolicyConfig(enabled=True, minimum_stake_mxn=20.0, round_to_min=True)

    summary = summarize_run_stake_policy(rows, run_id, config)
    for key, value in expected["counts"].items():
        assert summary[key] == value

    reason_codes = {
        row["case_id"]: evaluate_stake_policy_for_row(row, config)["reason_code"]
        for row in rows
        if row.get("case_id") in expected["decision_by_case_id"]
    }
    assert reason_codes == expected["decision_by_case_id"]


def test_exact_boundary_at_20_mxn_is_passed_not_adjusted():
    config = StakePolicyConfig(enabled=True, minimum_stake_mxn=20.0, round_to_min=True)
    row = {"row_type": "signal", "stage": "stageGenerateSignals", "run_id": "r", "stake_mxn": 20.0}

    outcome = evaluate_stake_policy_for_row(row, config)

    assert outcome["decision"] == "passed"
    assert outcome["reason_code"] == "stake_policy_pass"
    assert outcome["final_stake_mxn"] == 20.0


def test_decimal_edge_case_19_995_remains_below_min_without_pre_rounding():
    strict = StakePolicyConfig(enabled=True, minimum_stake_mxn=20.0, round_to_min=False)
    round_up = StakePolicyConfig(enabled=True, minimum_stake_mxn=20.0, round_to_min=True)
    row = {"row_type": "signal", "stage": "stageGenerateSignals", "run_id": "r", "stake_mxn": 19.995}

    strict_outcome = evaluate_stake_policy_for_row(row, strict)
    round_up_outcome = evaluate_stake_policy_for_row(row, round_up)

    assert strict_outcome["reason_code"] == "stake_below_min_suppressed"
    assert strict_outcome["final_stake_mxn"] == 0.0
    assert round_up_outcome["reason_code"] == "stake_rounded_to_min"
    assert round_up_outcome["final_stake_mxn"] == 20.0


def test_summarize_run_stake_policy_coerces_structured_stake_mode_payload_to_unknown():
    config = StakePolicyConfig(enabled=True)
    rows = [
        {
            "row_type": "signal",
            "stage": "stageGenerateSignals",
            "run_id": "run-1",
            "stake_mxn": 25.0,
            "stake_mode_used": '{"schema_id":"reason_code_alias_v1","reason_codes":{"X":1}}',
        }
    ]

    summary = summarize_run_stake_policy(rows, "run-1", config)

    assert summary["stake_mode_counts"] == {"unknown": 1}


def test_summarize_run_stake_policy_normalizes_non_enum_stake_mode_to_unknown():
    config = StakePolicyConfig(enabled=True)
    rows = [
        {
            "row_type": "signal",
            "stage": "stageGenerateSignals",
            "run_id": "run-1",
            "stake_mxn": 25.0,
            "stake_mode_used": "legacy_mode",
        }
    ]

    summary = summarize_run_stake_policy(rows, "run-1", config)

    assert summary["stake_mode_counts"] == {"unknown": 1}
