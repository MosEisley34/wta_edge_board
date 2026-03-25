import json
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
SCRIPTS_DIR = ROOT / "scripts"
sys.path.insert(0, str(SCRIPTS_DIR))

from evaluate_edge_quality import (  # noqa: E402
    DailyEdgeQualitySLOConfig,
    EdgeQualityGateConfig,
    evaluate_daily_edge_quality_slo,
    evaluate_edge_quality_gate,
    evaluate_rolling_edge_quality,
    load_run_log_rows,
    write_daily_slo_artifacts,
)


class EvaluateEdgeQualityTests(unittest.TestCase):
    @staticmethod
    def _summary_row(
        run_id: str,
        ended_at: str,
        edge_volatility: float,
        scored_signals: int = 12,
        matched_events: int = 8,
    ) -> dict[str, object]:
        return {
            "row_type": "summary",
            "stage": "runEdgeBoard",
            "run_id": run_id,
            "ended_at": ended_at,
            "feature_completeness": 0.9,
            "edge_volatility": edge_volatility,
            "signal_decision_summary": json.dumps({"suppression_counts": {}}),
            "stage_summaries": json.dumps(
                [
                    {
                        "stage": "stageMatchEvents",
                        "reason_codes": {"matched_count": matched_events},
                    },
                    {
                        "stage": "stageGenerateSignals",
                        "output_count": scored_signals,
                        "input_count": scored_signals,
                    },
                ]
            ),
        }

    def test_gate_passes_within_thresholds(self):
        rows = load_run_log_rows(str(ROOT / "scripts" / "fixtures" / "edge_quality_gate_pass.json"))
        report = evaluate_edge_quality_gate(
            rows,
            baseline_run_id="run-baseline",
            candidate_run_id="run-candidate",
            config=EdgeQualityGateConfig(
                min_feature_completeness=0.6,
                max_edge_volatility=0.03,
                max_suppression_drift=0.6,
                suppression_min_volume=2,
            ),
        )
        self.assertEqual("pass", report["status"])
        self.assertEqual([], report["failures"])
        self.assertTrue(
            any(item.startswith("HIGH_VISIBILITY_STAKE_POLICY_DISABLED") for item in report["high_visibility_warnings"])
        )

    def test_gate_omits_disabled_warning_when_stake_policy_enabled(self):
        rows = load_run_log_rows(str(ROOT / "scripts" / "fixtures" / "edge_quality_gate_pass.json"))
        report = evaluate_edge_quality_gate(
            rows,
            baseline_run_id="run-baseline",
            candidate_run_id="run-candidate",
            config=EdgeQualityGateConfig(stake_policy_enabled=True),
        )
        self.assertFalse(
            any(item.startswith("HIGH_VISIBILITY_STAKE_POLICY_DISABLED") for item in report["high_visibility_warnings"])
        )

    def test_gate_fails_with_threshold_breaches(self):
        rows = load_run_log_rows(str(ROOT / "scripts" / "fixtures" / "edge_quality_gate_fail.json"))
        report = evaluate_edge_quality_gate(
            rows,
            baseline_run_id="run-baseline",
            candidate_run_id="run-candidate",
            config=EdgeQualityGateConfig(
                min_feature_completeness=0.6,
                max_edge_volatility=0.03,
                max_suppression_drift=0.6,
                suppression_min_volume=2,
            ),
        )
        self.assertEqual("fail", report["status"])
        self.assertTrue(any("feature_completeness_below_floor" in item for item in report["failures"]))
        self.assertTrue(any("edge_volatility_above_ceiling" in item for item in report["failures"]))
        self.assertTrue(any("suppression_drift_exceeded" in item for item in report["failures"]))

    def test_cli_returns_non_zero_for_failures(self):
        cmd = [
            "python3",
            str(ROOT / "scripts" / "evaluate_edge_quality.py"),
            str(ROOT / "scripts" / "fixtures" / "edge_quality_gate_fail.json"),
            "--baseline-run-id",
            "run-baseline",
            "--candidate-run-id",
            "run-candidate",
        ]
        proc = subprocess.run(cmd, capture_output=True, text=True)
        self.assertEqual(1, proc.returncode)
        payload = json.loads(proc.stdout)
        self.assertEqual("fail", payload["status"])

    def test_alias_envelope_csv_pass_derives_non_zero_feature_completeness(self):
        rows = load_run_log_rows(str(ROOT / "scripts" / "fixtures" / "edge_quality_gate_alias_pass.csv"))
        report = evaluate_edge_quality_gate(
            rows,
            baseline_run_id="run-baseline",
            candidate_run_id="run-candidate",
            config=EdgeQualityGateConfig(
                min_feature_completeness=0.6,
                max_edge_volatility=0.2,
                max_suppression_drift=0.6,
                suppression_min_volume=2,
            ),
        )
        self.assertEqual("pass", report["status"])
        self.assertGreater(report["candidate"]["feature_completeness"], 0.0)
        self.assertEqual({}, report["candidate"]["diagnostics"])

    def test_alias_envelope_csv_fail_uses_stage_signal_output_derivation(self):
        rows = load_run_log_rows(str(ROOT / "scripts" / "fixtures" / "edge_quality_gate_alias_fail.csv"))
        report = evaluate_edge_quality_gate(
            rows,
            baseline_run_id="run-baseline",
            candidate_run_id="run-candidate",
            config=EdgeQualityGateConfig(
                min_feature_completeness=0.6,
                max_edge_volatility=0.03,
                max_suppression_drift=0.6,
                suppression_min_volume=2,
            ),
        )
        self.assertEqual("fail", report["status"])
        self.assertGreater(report["candidate"]["feature_completeness"], 0.0)
        self.assertTrue(any("feature_completeness_below_floor" in item for item in report["failures"]))
        self.assertTrue(any("edge_volatility_above_ceiling" in item for item in report["failures"]))

    def test_missing_metrics_emit_reason_code_diagnostics(self):
        rows = [
            {
                "row_type": "summary",
                "stage": "runEdgeBoard",
                "run_id": "run-baseline",
                "signal_decision_summary": "{}",
                "stage_summaries": "[]",
            },
            {
                "row_type": "summary",
                "stage": "runEdgeBoard",
                "run_id": "run-candidate",
                "signal_decision_summary": "{}",
                "stage_summaries": "[]",
            },
        ]
        report = evaluate_edge_quality_gate(
            rows,
            baseline_run_id="run-baseline",
            candidate_run_id="run-candidate",
            config=EdgeQualityGateConfig(),
        )
        self.assertEqual("fail", report["status"])
        self.assertIn(
            "missing_feature_completeness_metric reason_code=missing_field_feature_completeness",
            report["failures"],
        )
        self.assertIn("missing_edge_volatility_metric reason_code=missing_field_edge_volatility", report["failures"])

    def test_quality_contract_defaults_are_used_when_upstream_empty(self):
        rows = [
            {
                "row_type": "summary",
                "stage": "runEdgeBoard",
                "run_id": "run-baseline",
                "signal_decision_summary": json.dumps(
                    {
                        "quality_contract": {
                            "feature_completeness": 0.0,
                            "feature_completeness_reason_code": "upstream_stage_empty_player_stats_default",
                            "edge_volatility": 0.0,
                            "edge_volatility_reason_code": "upstream_stage_empty_generate_signals_default",
                        }
                    }
                ),
                "stage_summaries": "[]",
            },
            {
                "row_type": "summary",
                "stage": "runEdgeBoard",
                "run_id": "run-candidate",
                "signal_decision_summary": json.dumps(
                    {
                        "quality_contract": {
                            "feature_completeness": 0.0,
                            "feature_completeness_reason_code": "upstream_stage_empty_player_stats_default",
                            "edge_volatility": 0.0,
                            "edge_volatility_reason_code": "upstream_stage_empty_generate_signals_default",
                        }
                    }
                ),
                "stage_summaries": "[]",
            },
        ]
        report = evaluate_edge_quality_gate(
            rows,
            baseline_run_id="run-baseline",
            candidate_run_id="run-candidate",
            config=EdgeQualityGateConfig(min_feature_completeness=0.0),
        )
        self.assertFalse(any("missing_field_edge_volatility" in item for item in report["failures"]))

    def test_zero_requested_players_does_not_force_zero_completeness(self):
        rows = [
            {
                "row_type": "summary",
                "stage": "runEdgeBoard",
                "run_id": "run-baseline",
                "signal_decision_summary": "{}",
                "stage_summaries": json.dumps(
                    [
                        {
                            "stage": "stageFetchPlayerStats",
                            "reason_metadata": {"requested_player_count": 0, "resolved_player_count": 0},
                        }
                    ]
                ),
            },
            {
                "row_type": "summary",
                "stage": "runEdgeBoard",
                "run_id": "run-candidate",
                "signal_decision_summary": "{}",
                "stage_summaries": json.dumps(
                    [
                        {
                            "stage": "stageFetchPlayerStats",
                            "reason_metadata": {"requested_player_count": 0, "resolved_player_count": 0},
                        }
                    ]
                ),
            },
        ]
        report = evaluate_edge_quality_gate(
            rows,
            baseline_run_id="run-baseline",
            candidate_run_id="run-candidate",
            config=EdgeQualityGateConfig(),
        )
        self.assertTrue(any("missing_feature_completeness_metric" in item for item in report["failures"]))

    def test_legacy_schema_rows_emit_dedicated_status_instead_of_completeness_fail(self):
        rows = load_run_log_rows(str(ROOT / "scripts" / "fixtures" / "edge_quality_gate_legacy_schema_warning.json"))
        report = evaluate_edge_quality_gate(
            rows,
            baseline_run_id="run-baseline",
            candidate_run_id="run-candidate",
            config=EdgeQualityGateConfig(
                min_feature_completeness=0.6,
                max_edge_volatility=0.03,
                max_suppression_drift=0.6,
                suppression_min_volume=2,
            ),
        )
        self.assertEqual("legacy_schema_insufficient_feature_contract", report["status"])
        self.assertEqual([], report["failures"])
        self.assertTrue(
            any(item.startswith("legacy_schema_insufficient_feature_contract") for item in report["warnings"])
        )

    def test_modern_schema_runs_still_enforce_completeness_floor(self):
        rows = load_run_log_rows(str(ROOT / "scripts" / "fixtures" / "edge_quality_gate_fail.json"))
        report = evaluate_edge_quality_gate(
            rows,
            baseline_run_id="run-baseline",
            candidate_run_id="run-candidate",
            config=EdgeQualityGateConfig(
                min_feature_completeness=0.6,
                max_edge_volatility=0.03,
                max_suppression_drift=1.0,
                suppression_min_volume=999,
            ),
        )
        self.assertEqual("fail", report["status"])
        self.assertTrue(any("feature_completeness_below_floor" in item for item in report["failures"]))

    def test_low_volume_comparison_returns_insufficient_sample(self):
        rows = load_run_log_rows(str(ROOT / "scripts" / "fixtures" / "edge_quality_gate_low_volume_insufficient.json"))
        report = evaluate_edge_quality_gate(
            rows,
            baseline_run_id="run-baseline",
            candidate_run_id="run-candidate",
            config=EdgeQualityGateConfig(
                min_feature_completeness=0.6,
                max_edge_volatility=0.03,
                min_scored_signals_for_volatility=10,
                min_matched_events_for_volatility=5,
                max_suppression_drift=0.6,
                suppression_min_volume=2,
            ),
        )
        self.assertEqual("insufficient_sample", report["status"])
        self.assertEqual([], report["failures"])
        self.assertTrue(any("insufficient_sample_for_edge_volatility" in item for item in report["warnings"]))

    def test_normal_volume_still_fails_true_instability(self):
        rows = load_run_log_rows(str(ROOT / "scripts" / "fixtures" / "edge_quality_gate_normal_volume_fail.json"))
        report = evaluate_edge_quality_gate(
            rows,
            baseline_run_id="run-baseline",
            candidate_run_id="run-candidate",
            config=EdgeQualityGateConfig(
                min_feature_completeness=0.6,
                max_edge_volatility=0.03,
                min_scored_signals_for_volatility=10,
                min_matched_events_for_volatility=5,
                volatility_context_min_pairs=4,
                volatility_context_quantile=0.9,
                volatility_context_ceiling_factor=1.2,
                max_suppression_drift=0.6,
                suppression_min_volume=2,
            ),
        )
        self.assertEqual("fail", report["status"])
        self.assertTrue(any("edge_volatility_above_ceiling" in item for item in report["failures"]))
        self.assertEqual("contextual_window_pairs", report["effective_volatility_ceiling"]["source"])

    def test_rolling_report_splits_full_history_and_recent_window(self):
        rows = [
            {
                "row_type": "summary",
                "stage": "runEdgeBoard",
                "run_id": "run-a",
                "ended_at": "2026-03-20T00:00:00Z",
                "feature_completeness": 0.8,
                "edge_volatility": 0.01,
                "signal_decision_summary": json.dumps({"suppression_counts": {}}),
                "stage_summaries": json.dumps([]),
            },
            {
                "row_type": "summary",
                "stage": "runEdgeBoard",
                "run_id": "run-b",
                "ended_at": "2026-03-21T00:00:00Z",
                "feature_completeness": 0.8,
                "edge_volatility": 0.01,
                "signal_decision_summary": json.dumps({"suppression_counts": {}}),
                "stage_summaries": json.dumps([]),
            },
            {
                "row_type": "summary",
                "stage": "runEdgeBoard",
                "run_id": "run-c",
                "ended_at": "2026-03-22T00:00:00Z",
                "feature_completeness": 0.8,
                "edge_volatility": 0.01,
                "signal_decision_summary": json.dumps({"suppression_counts": {}}),
                "stage_summaries": json.dumps([]),
            },
        ]
        report = evaluate_rolling_edge_quality(
            rows=rows,
            config=EdgeQualityGateConfig(),
            min_ended_at="2026-03-21T00:00:00Z",
        )
        self.assertEqual(2, report["full_history_trend"]["pair_count"])
        self.assertEqual(1, report["recent_window_gate"]["pair_count"])
        self.assertEqual("go", report["recent_window_gate"]["go_no_go"])
        self.assertEqual(1, report["recent_window_gate"]["status_counts"]["pass"])

    def test_daily_slo_verdict_fails_when_decisionable_window_fail_rate_exceeds_threshold(self):
        rows = [
            self._summary_row(
                run_id=f"run-{index}",
                ended_at=f"2026-03-{index:02d}T00:00:00Z",
                edge_volatility=0.01 if index != 11 else 0.08,
            )
            for index in range(1, 13)
        ]
        report = evaluate_daily_edge_quality_slo(
            rows=rows,
            gate_config=EdgeQualityGateConfig(max_edge_volatility=0.03),
            slo_config=DailyEdgeQualitySLOConfig(window_days=(12,), min_pairs_per_window=10, fail_rate_threshold=0.05),
            as_of_utc="2026-03-12T12:00:00Z",
        )
        self.assertEqual("fail", report["gate_verdict"])
        self.assertEqual(1, report["decisionable_window_count"])
        self.assertEqual(1, report["window_reports"][0]["decisionable_status_counts"]["fail"])

    def test_daily_slo_excludes_low_activity_pairs_from_fail_rate_denominator(self):
        rows = [
            self._summary_row("run-1", "2026-03-01T00:00:00Z", edge_volatility=0.01, scored_signals=12, matched_events=8),
            self._summary_row("run-2", "2026-03-02T00:00:00Z", edge_volatility=0.01, scored_signals=12, matched_events=8),
            self._summary_row("run-3", "2026-03-03T00:00:00Z", edge_volatility=0.08, scored_signals=12, matched_events=8),
            self._summary_row("run-4", "2026-03-04T00:00:00Z", edge_volatility=0.08, scored_signals=3, matched_events=2),
            self._summary_row("run-5", "2026-03-05T00:00:00Z", edge_volatility=0.08, scored_signals=3, matched_events=2),
        ]
        report = evaluate_daily_edge_quality_slo(
            rows=rows,
            gate_config=EdgeQualityGateConfig(max_edge_volatility=0.03, volatility_context_min_pairs=99),
            slo_config=DailyEdgeQualitySLOConfig(window_days=(7,), min_pairs_per_window=2, fail_rate_threshold=0.40),
            as_of_utc="2026-03-05T12:00:00Z",
        )
        window = report["window_reports"][0]
        self.assertEqual(4, window["pair_count"])
        self.assertEqual(2, window["decisionable_pair_count"])
        self.assertEqual(2, window["excluded_pair_count"])
        self.assertEqual(1, window["decisionable_status_counts"]["fail"])
        self.assertAlmostEqual(0.5, window["fail_rate"])
        self.assertEqual("fail", window["verdict"])
        self.assertTrue(any("low_scored_signals" in pair["reasons"] for pair in window["excluded_pairs"]))
        self.assertTrue(any("low_matched_events" in pair["reasons"] for pair in window["excluded_pairs"]))

    def test_daily_slo_verdict_insufficient_when_window_below_min_pairs(self):
        rows = [
            self._summary_row("run-a", "2026-03-22T00:00:00Z", edge_volatility=0.01, scored_signals=3, matched_events=2),
            self._summary_row("run-b", "2026-03-23T00:00:00Z", edge_volatility=0.01, scored_signals=3, matched_events=2),
        ]
        report = evaluate_daily_edge_quality_slo(
            rows=rows,
            gate_config=EdgeQualityGateConfig(),
            slo_config=DailyEdgeQualitySLOConfig(window_days=(3, 7), min_pairs_per_window=10, fail_rate_threshold=0.15),
            as_of_utc="2026-03-24T12:00:00Z",
        )
        self.assertEqual("insufficient_sample", report["gate_verdict"])
        self.assertTrue(all(not window["decisionable"] for window in report["window_reports"]))
        self.assertTrue(all(window["decisionable_pair_count"] == 0 for window in report["window_reports"]))

    def test_daily_slo_includes_stake_policy_summary_counts(self):
        rows = [
            self._summary_row("run-a", "2026-03-22T00:00:00Z", edge_volatility=0.01, scored_signals=12, matched_events=8),
            {"row_type": "diag", "stage": "stageGenerateSignals", "run_id": "run-a", "stake_mxn": 2},
            self._summary_row("run-b", "2026-03-23T00:00:00Z", edge_volatility=0.01, scored_signals=12, matched_events=8),
            {"row_type": "diag", "stage": "stageGenerateSignals", "run_id": "run-b", "stake_mxn": 20},
        ]
        report = evaluate_daily_edge_quality_slo(
            rows=rows,
            gate_config=EdgeQualityGateConfig(stake_policy_enabled=True),
            slo_config=DailyEdgeQualitySLOConfig(window_days=(3,), min_pairs_per_window=1, fail_rate_threshold=0.5),
            as_of_utc="2026-03-24T00:00:00Z",
        )
        self.assertIn("stake_policy_summary_counts", report)
        self.assertGreaterEqual(report["stake_policy_summary_counts"]["suppressed_count"], 1)

    def test_write_daily_slo_artifacts_writes_timestamped_report_and_summary(self):
        report = {
            "generated_at_utc": "2026-03-24T01:00:00+00:00",
            "as_of_utc": "2026-03-24T00:00:00+00:00",
            "gate_verdict": "pass",
            "gate_reason": "all_decisionable_windows_within_fail_rate_threshold",
            "decisionable_window_count": 2,
            "failing_decisionable_window_count": 0,
            "aggregate_status_counts": {"pass": 4, "fail": 0, "insufficient_sample": 1},
            "window_reports": [{"window_days": 3, "pair_count": 2, "decisionable": True, "fail_rate": 0.0, "verdict": "pass"}],
        }
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_reports_dir = Path(temp_dir) / "reports"
            temp_archive_dir = Path(temp_dir) / "archive"
            daily_path, summary_path = write_daily_slo_artifacts(
                report=report,
                output_dir=str(temp_reports_dir),
                archive_dir=str(temp_archive_dir),
            )
            self.assertTrue(daily_path.exists())
            self.assertTrue(summary_path.exists())
            self.assertIn("edge_quality_daily_slo_", daily_path.name)
            summary_lines = summary_path.read_text(encoding="utf-8").strip().splitlines()
            self.assertGreaterEqual(len(summary_lines), 1)


if __name__ == "__main__":
    unittest.main()
