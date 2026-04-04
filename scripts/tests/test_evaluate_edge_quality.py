import csv
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
    _select_latest_run_ids,
    _snapshot,
    evaluate_edge_quality_compare_report,
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
        suppression_counts: dict[str, int] | None = None,
        suppression_reason_context: dict[str, dict[str, object]] | None = None,
        **extra_fields: object,
    ) -> dict[str, object]:
        suppression_counts = suppression_counts or {}
        row = {
            "row_type": "summary",
            "stage": "runEdgeBoard",
            "run_id": run_id,
            "ended_at": ended_at,
            "feature_completeness": 0.9,
            "edge_volatility": edge_volatility,
            "signal_decision_summary": json.dumps(
                {
                    "suppression_counts": {
                        "all": {
                            "by_reason": suppression_counts,
                        }
                    },
                    "suppression_reason_context": suppression_reason_context or {},
                }
            ),
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
        row.update(extra_fields)
        return row

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

    def test_snapshot_selects_unique_qualifying_row(self):
        summary = _snapshot(
            rows=[self._summary_row("run-unique", "2026-03-10T00:00:00Z", edge_volatility=0.01)],
            run_id="run-unique",
            config=EdgeQualityGateConfig(),
        )
        self.assertEqual("run-unique", summary["run_id"])

    def test_snapshot_raises_when_qualifying_row_missing_with_diagnostics(self):
        rows = [
            {
                "row_type": "trace",
                "stage": "stageGenerateSignals",
                "run_id": "run-missing",
                "_source_file": "Run_Log.csv",
                "_source_kind": "csv",
            }
        ]
        with self.assertRaisesRegex(ValueError, "Expected exactly one runEdgeBoard summary row"):
            _snapshot(rows=rows, run_id="run-missing", config=EdgeQualityGateConfig())
        with self.assertRaises(ValueError) as ctx:
            _snapshot(rows=rows, run_id="run-missing", config=EdgeQualityGateConfig())
        self.assertIn('"qualifying_row_count": 0', str(ctx.exception))
        self.assertIn('"stages_seen": ["stageGenerateSignals"]', str(ctx.exception))
        self.assertIn('"source_kinds": ["csv"]', str(ctx.exception))

    def test_snapshot_raises_when_multiple_qualifying_rows_with_diagnostics(self):
        rows = [
            {
                **self._summary_row("run-dup", "2026-03-10T00:00:00Z", edge_volatility=0.01),
                "_source_file": "Run_Log.csv",
                "_source_kind": "csv",
            },
            {
                **self._summary_row("run-dup", "2026-03-10T00:05:00Z", edge_volatility=0.02),
                "_source_file": "Run_Log.json",
                "_source_kind": "json",
            },
        ]
        with self.assertRaises(ValueError) as ctx:
            _snapshot(rows=rows, run_id="run-dup", config=EdgeQualityGateConfig())
        self.assertIn('"qualifying_row_count": 2', str(ctx.exception))
        self.assertIn('"source_files": ["Run_Log.csv", "Run_Log.json"]', str(ctx.exception))
        self.assertIn('"source_kinds": ["csv", "json"]', str(ctx.exception))

    def test_snapshot_dedupes_identical_csv_json_summary_rows_for_cardinality(self):
        duplicate_summary = self._summary_row("run-dup-same", "2026-03-10T00:00:00Z", edge_volatility=0.01)
        rows = [
            {
                **duplicate_summary,
                "_source_file": "Run_Log.csv",
                "_source_kind": "csv",
            },
            {
                **duplicate_summary,
                "_source_file": "Run_Log.json",
                "_source_kind": "json",
            },
        ]
        summary = _snapshot(rows=rows, run_id="run-dup-same", config=EdgeQualityGateConfig())
        self.assertEqual("run-dup-same", summary["run_id"])
        self.assertEqual(0.9, summary["feature_completeness"])
        self.assertEqual(0.01, summary["edge_volatility"])

    def test_snapshot_diagnostics_include_merged_source_provenance(self):
        duplicate_a = self._summary_row("run-dup-multi", "2026-03-10T00:00:00Z", edge_volatility=0.01)
        duplicate_b = self._summary_row("run-dup-multi", "2026-03-10T00:05:00Z", edge_volatility=0.02)
        rows = [
            {
                **duplicate_a,
                "_source_file": "Run_Log.csv",
                "_source_kind": "csv",
            },
            {
                **duplicate_a,
                "_source_file": "Run_Log.json",
                "_source_kind": "json",
            },
            {
                **duplicate_b,
                "_source_file": "Run_Log.csv",
                "_source_kind": "csv",
            },
            {
                **duplicate_b,
                "_source_file": "Run_Log.json",
                "_source_kind": "json",
            },
        ]
        with self.assertRaises(ValueError) as ctx:
            _snapshot(rows=rows, run_id="run-dup-multi", config=EdgeQualityGateConfig())
        self.assertIn('"qualifying_row_count": 2', str(ctx.exception))
        self.assertIn('"merged_from_sources": ["csv", "json"]', str(ctx.exception))

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
        self.assertIsInstance(report["suppression_drift_details"], dict)
        self.assertIn("suppression_drift", report["failure_diagnostics"])

    def test_gate_pre_gates_on_no_events_from_source_activity(self):
        rows = [
            {
                **self._summary_row("run-baseline", "2026-03-01T00:00:00Z", edge_volatility=0.010),
                "no_hit_no_events_from_source_count": 0,
                "fetched_odds": 3,
                "fetched_schedule": 2,
            },
            {
                **self._summary_row("run-candidate", "2026-03-02T00:00:00Z", edge_volatility=0.090),
                "no_hit_no_events_from_source_count": 4,
                "fetched_odds": 0,
                "fetched_schedule": 0,
                "feature_completeness": 0.2,
            },
        ]
        report = evaluate_edge_quality_gate(
            rows,
            baseline_run_id="run-baseline",
            candidate_run_id="run-candidate",
            config=EdgeQualityGateConfig(max_edge_volatility=0.03),
        )
        self.assertEqual("insufficient_sample", report["status"])
        self.assertEqual("insufficient_source_activity", report["sample_assessment"]["reason_code"])
        self.assertEqual("pre_gate_insufficient_source_activity", report["volatility_diagnostic"]["phase"])
        self.assertFalse(report["volatility_diagnostic"]["strict_gate"]["enforced"])
        self.assertTrue(any(item.startswith("insufficient_source_activity") for item in report["warnings"]))
        self.assertFalse(any("edge_volatility_above_ceiling" in item for item in report["failures"]))
        self.assertFalse(any("feature_completeness_below_floor" in item for item in report["failures"]))

    def test_gate_pre_gates_on_zero_schedule_and_odds_fetch_counts(self):
        rows = [
            self._summary_row("run-baseline", "2026-03-01T00:00:00Z", edge_volatility=0.010),
            {
                **self._summary_row("run-candidate", "2026-03-02T00:00:00Z", edge_volatility=0.080),
                "fetched_odds": 0,
                "fetched_schedule": 0,
            },
        ]
        report = evaluate_edge_quality_gate(
            rows,
            baseline_run_id="run-baseline",
            candidate_run_id="run-candidate",
            config=EdgeQualityGateConfig(max_edge_volatility=0.03),
        )
        self.assertEqual("insufficient_sample", report["status"])
        self.assertEqual("insufficient_source_activity", report["sample_assessment"]["reason_code"])
        self.assertFalse(any("edge_volatility_above_ceiling" in item for item in report["failures"]))

    def test_suppression_drift_failure_emits_pair_level_causal_diagnostics(self):
        rows = [
            self._summary_row(
                "run-1",
                "2026-03-01T00:00:00Z",
                edge_volatility=0.01,
                suppression_counts={"stale_odds_skip": 2},
                suppression_reason_context={
                    "stale_odds_skip": {
                        "raw_count": 2,
                        "event_ids": ["evt-1", "evt-2"],
                        "minutes_to_start_snapshot": {"min": 8, "max": 12},
                        "odds_freshness_metadata": {"max_age_seconds": 420, "stale_threshold_seconds": 300},
                    }
                },
            ),
            self._summary_row(
                "run-2",
                "2026-03-02T00:00:00Z",
                edge_volatility=0.01,
                suppression_counts={"stale_odds_skip": 6},
                suppression_reason_context={
                    "stale_odds_skip": {
                        "raw_count": 6,
                        "event_ids": ["evt-2", "evt-3", "evt-4"],
                        "minutes_to_start_snapshot": {"min": 4, "max": 9},
                        "odds_freshness_metadata": {"max_age_seconds": 840, "stale_threshold_seconds": 300},
                    }
                },
            ),
        ]
        report = evaluate_edge_quality_gate(
            rows,
            baseline_run_id="run-1",
            candidate_run_id="run-2",
            config=EdgeQualityGateConfig(max_suppression_drift=0.5, suppression_min_volume=2),
        )
        self.assertEqual("fail", report["status"])
        reason_diag = report["failure_diagnostics"]["suppression_drift"]["failing_reasons"]["stale_odds_skip"]
        self.assertEqual(4, reason_diag["drift_fraction"]["numerator"])
        self.assertEqual(2, reason_diag["drift_fraction"]["denominator"])
        self.assertEqual(2, reason_diag["raw_counts"]["baseline"])
        self.assertEqual(6, reason_diag["raw_counts"]["candidate"])
        self.assertEqual(["evt-1", "evt-2"], reason_diag["event_ids"]["baseline"])
        self.assertEqual(["evt-2", "evt-3", "evt-4"], reason_diag["event_ids"]["candidate"])
        self.assertEqual(8, reason_diag["minutes_to_start_snapshot"]["baseline"]["min"])
        self.assertEqual(840, reason_diag["odds_freshness_metadata"]["candidate"]["max_age_seconds"])
        self.assertEqual("evt-1", reason_diag["top_contributing_events"][0]["event_id"])

    def test_dynamic_volatility_policy_passes_moderate_volatility_with_adequate_sample(self):
        rows = [
            {
                **self._summary_row("run-baseline", "2026-03-01T00:00:00Z", edge_volatility=0.020, scored_signals=30, matched_events=15),
                "tournament": "wta-miami",
                "time_block": "2026-03-01T12",
            },
            {
                **self._summary_row("run-candidate", "2026-03-01T01:00:00Z", edge_volatility=0.026, scored_signals=30, matched_events=15),
                "tournament": "wta-miami",
                "time_block": "2026-03-01T12",
            },
        ]
        report = evaluate_edge_quality_gate(
            rows,
            baseline_run_id="run-baseline",
            candidate_run_id="run-candidate",
            config=EdgeQualityGateConfig(max_edge_volatility=0.03, volatility_context_min_pairs=99),
        )
        self.assertEqual("pass", report["status"])
        self.assertLess(report["effective_volatility_ceiling"]["ceiling_after_dynamic_scale"], 0.03)

    def test_dynamic_volatility_policy_fails_true_spike(self):
        rows = [
            {
                **self._summary_row("run-baseline", "2026-03-02T00:00:00Z", edge_volatility=0.015, scored_signals=10, matched_events=5),
                "tournament": "wta-miami",
                "time_block": "2026-03-02T12",
            },
            {
                **self._summary_row("run-candidate", "2026-03-02T01:00:00Z", edge_volatility=0.060, scored_signals=10, matched_events=5),
                "tournament": "wta-miami",
                "time_block": "2026-03-02T12",
            },
        ]
        report = evaluate_edge_quality_gate(
            rows,
            baseline_run_id="run-baseline",
            candidate_run_id="run-candidate",
            config=EdgeQualityGateConfig(max_edge_volatility=0.03, volatility_context_min_pairs=99),
        )
        self.assertEqual("fail", report["status"])
        self.assertTrue(any("edge_volatility_above_ceiling" in item for item in report["failures"]))

    def test_dynamic_volatility_policy_is_stable_across_back_to_back_normal_drift(self):
        rows = [
            {**self._summary_row("run-1", "2026-03-03T00:00:00Z", edge_volatility=0.019, scored_signals=20, matched_events=10), "time_block": "2026-03-03T12"},
            {**self._summary_row("run-2", "2026-03-03T01:00:00Z", edge_volatility=0.021, scored_signals=21, matched_events=11), "time_block": "2026-03-03T12"},
            {**self._summary_row("run-3", "2026-03-03T02:00:00Z", edge_volatility=0.022, scored_signals=19, matched_events=10), "time_block": "2026-03-03T12"},
        ]
        first = evaluate_edge_quality_gate(rows, "run-1", "run-2", EdgeQualityGateConfig(max_edge_volatility=0.03))
        second = evaluate_edge_quality_gate(rows, "run-2", "run-3", EdgeQualityGateConfig(max_edge_volatility=0.03))
        self.assertEqual("pass", first["status"])
        self.assertEqual("pass", second["status"])
        self.assertAlmostEqual(
            first["effective_volatility_ceiling"]["ceiling_after_dynamic_scale"],
            second["effective_volatility_ceiling"]["ceiling_after_dynamic_scale"],
            places=2,
        )

    def test_candidate_with_hits_and_none_no_hit_reason_does_not_emit_dominant_no_hit_reason(self):
        rows = [
            {
                **self._summary_row("run-baseline", "2026-03-01T00:00:00Z", edge_volatility=0.01),
                "feature_completeness": 0.9,
                "no_hit_terminal_reason_code": "none",
            },
            {
                **self._summary_row("run-candidate", "2026-03-02T00:00:00Z", edge_volatility=0.01, matched_events=8),
                "feature_completeness": 0.4,
                "no_hit_terminal_reason_code": "none",
            },
        ]
        report = evaluate_edge_quality_gate(
            rows,
            baseline_run_id="run-baseline",
            candidate_run_id="run-candidate",
            config=EdgeQualityGateConfig(min_feature_completeness=0.6, max_edge_volatility=0.03),
        )
        self.assertEqual("fail", report["status"])
        self.assertTrue(any("feature_completeness_below_floor" in item for item in report["failures"]))
        self.assertFalse(any(item.startswith("dominant_no_hit_reason") for item in report["failures"]))

    def test_baseline_only_no_hit_emits_baseline_scoped_failure_code(self):
        rows = [
            {
                **self._summary_row("run-baseline", "2026-03-01T00:00:00Z", edge_volatility=0.01, matched_events=0),
                "feature_completeness": 0.9,
                "no_hit_terminal_reason_code": "events_outside_time_window",
            },
            {
                **self._summary_row("run-candidate", "2026-03-02T00:00:00Z", edge_volatility=0.01, matched_events=8),
                "feature_completeness": 0.4,
                "no_hit_terminal_reason_code": "none",
            },
        ]
        report = evaluate_edge_quality_gate(
            rows,
            baseline_run_id="run-baseline",
            candidate_run_id="run-candidate",
            config=EdgeQualityGateConfig(min_feature_completeness=0.6, max_edge_volatility=0.03),
        )
        self.assertEqual("fail", report["status"])
        self.assertTrue(any("feature_completeness_below_floor" in item for item in report["failures"]))
        self.assertIn(
            "dominant_no_hit_reason_baseline (baseline=events_outside_time_window)",
            report["failures"],
        )

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

    def test_cli_compare_writes_default_artifact_atomically_and_prints_path(self):
        with tempfile.TemporaryDirectory() as tmp:
            cmd = [
                "python3",
                str(ROOT / "scripts" / "evaluate_edge_quality.py"),
                str(ROOT / "scripts" / "fixtures" / "edge_quality_gate_fail.json"),
                "--baseline-run-id",
                "run-baseline",
                "--candidate-run-id",
                "run-candidate",
            ]
            proc = subprocess.run(cmd, capture_output=True, text=True, cwd=tmp)
            self.assertEqual(1, proc.returncode, msg=proc.stdout + proc.stderr)
            artifact = Path(tmp) / "artifacts" / "compare" / "run-baseline_vs_run-candidate.json"
            self.assertTrue(artifact.exists(), msg=f"missing artifact: {artifact}")
            written = json.loads(artifact.read_text(encoding="utf-8"))
            streamed = json.loads(proc.stdout)
            self.assertEqual(streamed["schema"], written["schema"])
            self.assertIn(str(artifact.resolve()), proc.stderr)

    def test_cli_compare_respects_out_json_path(self):
        with tempfile.TemporaryDirectory() as tmp:
            out_path = Path(tmp) / "custom" / "edge_compare.json"
            cmd = [
                "python3",
                str(ROOT / "scripts" / "evaluate_edge_quality.py"),
                str(ROOT / "scripts" / "fixtures" / "edge_quality_gate_fail.json"),
                "--baseline-run-id",
                "run-baseline",
                "--candidate-run-id",
                "run-candidate",
                "--out-json",
                str(out_path),
            ]
            proc = subprocess.run(cmd, capture_output=True, text=True)
            self.assertEqual(1, proc.returncode, msg=proc.stdout + proc.stderr)
            self.assertTrue(out_path.exists(), msg=f"missing artifact: {out_path}")
            self.assertIn(str(out_path.resolve()), proc.stderr)

    def test_cli_compare_fail_out_json_matches_stdout_with_diagnostics(self):
        with tempfile.TemporaryDirectory() as tmp:
            out_path = Path(tmp) / "custom" / "edge_compare_fail.json"
            cmd = [
                "python3",
                str(ROOT / "scripts" / "evaluate_edge_quality.py"),
                str(ROOT / "scripts" / "fixtures" / "edge_quality_gate_fail.json"),
                "--baseline-run-id",
                "run-baseline",
                "--candidate-run-id",
                "run-candidate",
                "--out-json",
                str(out_path),
            ]
            proc = subprocess.run(cmd, capture_output=True, text=True)
            self.assertEqual(1, proc.returncode, msg=proc.stdout + proc.stderr)
            stdout_payload = json.loads(proc.stdout)
            out_json_payload = json.loads(out_path.read_text(encoding="utf-8"))
            self.assertEqual(stdout_payload, out_json_payload)
            pair_level = out_json_payload["pair_level_result"]
            self.assertIn("warnings", pair_level)
            self.assertIn("failures", pair_level)
            self.assertIn("failure_diagnostics", pair_level)
            self.assertIn("baseline", pair_level)
            self.assertIn("candidate", pair_level)
            self.assertIn("windowed_fallback_result", out_json_payload)
            self.assertIn("status_counts", out_json_payload["windowed_fallback_result"])


    def test_cli_compare_duplicate_summary_rows_emits_structured_failure_without_traceback(self):
        with tempfile.TemporaryDirectory() as tmp:
            fixture = Path(tmp) / "Run_Log.json"
            fixture.write_text(
                json.dumps(
                    [
                        self._summary_row("run-baseline", "2026-03-01T00:00:00Z", edge_volatility=0.01),
                        self._summary_row("run-candidate", "2026-03-02T00:00:00Z", edge_volatility=0.02),
                        self._summary_row("run-candidate", "2026-03-02T00:05:00Z", edge_volatility=0.03),
                    ]
                ),
                encoding="utf-8",
            )
            cmd = [
                "python3",
                str(ROOT / "scripts" / "evaluate_edge_quality.py"),
                str(fixture),
                "--baseline-run-id",
                "run-baseline",
                "--candidate-run-id",
                "run-candidate",
            ]
            proc = subprocess.run(cmd, capture_output=True, text=True)
        self.assertEqual(2, proc.returncode, msg=proc.stdout + proc.stderr)
        payload = json.loads(proc.stdout)
        self.assertEqual("run_summary_cardinality_mismatch", payload["error_type"])
        self.assertEqual("duplicate_summary_rows", payload["classification"])
        self.assertIn("duplicate_summary_rows", proc.stderr)
        self.assertIn('"error_type": "run_summary_cardinality_mismatch"', proc.stderr)
        self.assertIn('"qualifying_row_count": 2', proc.stderr)
        self.assertNotIn("Traceback (most recent call last)", proc.stderr)


    def test_cli_preflight_fails_when_run_id_missing(self):
        with tempfile.TemporaryDirectory() as tmp:
            fixture = Path(tmp) / "Run_Log.json"
            fixture.write_text(
                json.dumps(
                    [
                        self._summary_row("run-baseline", "2026-03-01T00:00:00Z", edge_volatility=0.01),
                    ]
                ),
                encoding="utf-8",
            )
            cmd = [
                "python3",
                str(ROOT / "scripts" / "evaluate_edge_quality.py"),
                str(fixture),
                "--baseline-run-id",
                "run-baseline",
                "--candidate-run-id",
                "run-missing",
            ]
            proc = subprocess.run(cmd, capture_output=True, text=True)
        self.assertEqual(2, proc.returncode)
        self.assertIn("Run-ID preflight failed", proc.stderr)
        self.assertIn("run_id `run-missing` not found", proc.stderr)
        self.assertIn("precheck_run_ids.py --require-gate-prereqs", proc.stderr)

    def test_cli_preflight_fails_when_run_summary_row_missing(self):
        with tempfile.TemporaryDirectory() as tmp:
            fixture = Path(tmp) / "Run_Log.json"
            fixture.write_text(
                json.dumps(
                    [
                        self._summary_row("run-baseline", "2026-03-01T00:00:00Z", edge_volatility=0.01),
                        {
                            "row_type": "trace",
                            "stage": "stageGenerateSignals",
                            "run_id": "run-candidate",
                        },
                    ]
                ),
                encoding="utf-8",
            )
            cmd = [
                "python3",
                str(ROOT / "scripts" / "evaluate_edge_quality.py"),
                str(fixture),
                "--baseline-run-id",
                "run-baseline",
                "--candidate-run-id",
                "run-candidate",
            ]
            proc = subprocess.run(cmd, capture_output=True, text=True)
        self.assertEqual(2, proc.returncode)
        self.assertIn("Run-ID preflight failed", proc.stderr)
        self.assertIn(
            "run_id `run-candidate` is present but missing required row_type=summary + stage=runEdgeBoard row",
            proc.stderr,
        )
        self.assertIn("precheck_run_ids.py --require-gate-prereqs", proc.stderr)

    def test_autoselect_run_pair_skips_cancelled_runs_by_default(self):
        rows = load_run_log_rows(
            str(ROOT / "scripts" / "fixtures" / "edge_quality_pair_autoselect_cancelled.json")
        )
        selected, diagnostics = _select_latest_run_ids(
            rows,
            include_cancelled=False,
            diagnostics_limit=6,
        )
        self.assertEqual(["run-prev-valid", "run-post-valid"], selected)
        self.assertTrue(
            any("run_id=run-mid-cancelled rejected" in item for item in diagnostics),
            msg=f"expected rejection diagnostics in {diagnostics}",
        )

    def test_autoselect_run_pair_can_include_cancelled_when_flag_enabled(self):
        rows = load_run_log_rows(
            str(ROOT / "scripts" / "fixtures" / "edge_quality_pair_autoselect_cancelled.json")
        )
        selected, diagnostics = _select_latest_run_ids(
            rows,
            include_cancelled=True,
            diagnostics_limit=6,
        )
        self.assertEqual(["run-mid-cancelled", "run-post-valid"], selected)
        self.assertTrue(
            any("retained due to --include-cancelled" in item for item in diagnostics),
            msg=f"expected include-cancelled diagnostics in {diagnostics}",
        )

    def test_cli_autoselect_emits_selection_diagnostics(self):
        cmd = [
            "python3",
            str(ROOT / "scripts" / "evaluate_edge_quality.py"),
            str(ROOT / "scripts" / "fixtures" / "edge_quality_pair_autoselect_cancelled.json"),
            "--auto-select-run-pair",
        ]
        proc = subprocess.run(cmd, capture_output=True, text=True)
        self.assertEqual(1, proc.returncode, msg=proc.stdout + proc.stderr)
        payload = json.loads(proc.stdout)
        self.assertEqual("run-prev-valid", payload["selected_run_pair"]["baseline_run_id"])
        self.assertEqual("run-post-valid", payload["selected_run_pair"]["candidate_run_id"])
        self.assertTrue(
            any("run_id=run-mid-cancelled rejected" in item for item in payload["selection_diagnostics"]),
            msg=str(payload.get("selection_diagnostics")),
        )
        preconditions = payload["final_operator_summary"]["strict_pair_preconditions"]
        self.assertFalse(preconditions["ok"])
        self.assertIn("invalid_strict_pair_baseline", preconditions["reason_codes"])

    def test_autoselect_rejects_candidate_like_baseline_pair(self):
        rows = [
            self._summary_row("run-baseline-oldest", "2026-03-24T07:00:00Z", edge_volatility=0.01),
            self._summary_row("run-candidate-older", "2026-03-24T08:00:00Z", edge_volatility=0.01),
            self._summary_row("run-candidate-newer", "2026-03-24T09:00:00Z", edge_volatility=0.02),
        ]
        selected, diagnostics = _select_latest_run_ids(rows, include_cancelled=False, diagnostics_limit=10)
        self.assertEqual(["run-baseline-oldest", "run-candidate-newer"], selected)
        self.assertTrue(any("candidate_like_pair_disallowed" in item for item in diagnostics))

    def test_compare_report_precondition_failure_routes_to_fallback_with_reason_code(self):
        rows = [
            self._summary_row("run-1", "2026-03-01T00:00:00Z", edge_volatility=0.01, matched_events=0),
            self._summary_row("run-2", "2026-03-02T00:00:00Z", edge_volatility=0.02, matched_events=8),
            self._summary_row("run-3", "2026-03-03T00:00:00Z", edge_volatility=0.02, matched_events=8),
        ]
        report = evaluate_edge_quality_compare_report(
            rows=rows,
            baseline_run_id="run-1",
            candidate_run_id="run-2",
            config=EdgeQualityGateConfig(max_edge_volatility=0.03),
            ordered_run_ids=["run-1", "run-2", "run-3"],
        )
        self.assertEqual("insufficient_operational_sample", report["pair_level_result"]["status"])
        self.assertEqual("blocked_insufficient_operational_sample", report["gate_verdict"])
        self.assertEqual("EDGE_QUALITY_GATE_BLOCKED_INSUFFICIENT_OPERATIONAL_SAMPLE", report["reason_code"])
        self.assertIsNone(report["windowed_fallback_result"])
        self.assertEqual("strict_pair_gate", report["final_operator_summary"]["decision_authoritative_source"])
        pre_gate = report["final_operator_summary"]["strict_pair_operational_pre_gate"]
        self.assertFalse(pre_gate["ok"])
        self.assertEqual("insufficient_operational_sample", pre_gate["reason_code"])
        self.assertIn("baseline_matched_events_below_minimum", pre_gate["reason_codes"])

    def test_compare_report_marks_operational_sample_pregate_metadata_when_candidate_below_floor(self):
        rows = [
            self._summary_row("run-1", "2026-03-01T00:00:00Z", edge_volatility=0.01, scored_signals=12, matched_events=8),
            self._summary_row("run-2", "2026-03-02T00:00:00Z", edge_volatility=0.02, scored_signals=3, matched_events=2),
            self._summary_row("run-3", "2026-03-03T00:00:00Z", edge_volatility=0.02, scored_signals=12, matched_events=8),
        ]
        report = evaluate_edge_quality_compare_report(
            rows=rows,
            baseline_run_id="run-1",
            candidate_run_id="run-2",
            config=EdgeQualityGateConfig(
                max_edge_volatility=0.03,
                min_scored_signals_for_volatility=10,
                min_matched_events_for_volatility=5,
            ),
            ordered_run_ids=["run-1", "run-2", "run-3"],
        )
        self.assertEqual("insufficient_operational_sample", report["pair_level_result"]["status"])
        self.assertEqual("blocked_insufficient_operational_sample", report["gate_verdict"])
        self.assertEqual("EDGE_QUALITY_GATE_BLOCKED_INSUFFICIENT_OPERATIONAL_SAMPLE", report["reason_code"])
        self.assertEqual("insufficient_operational_sample", report["final_operator_summary"]["strict_pair_status"])
        self.assertEqual(
            "strict_pair_operational_pre_gate_failed",
            report["final_operator_summary"]["strict_pair_status_reason"],
        )
        pre_gate = report["final_operator_summary"]["strict_pair_operational_pre_gate"]
        self.assertFalse(pre_gate["ok"])
        self.assertEqual(
            ["candidate_matched_events_below_minimum", "candidate_scored_signals_below_minimum"],
            sorted(pre_gate["reason_codes"]),
        )
        self.assertEqual(10, pre_gate["minimums"]["min_scored_signals_for_volatility"])
        self.assertEqual(5, pre_gate["minimums"]["min_matched_events_for_volatility"])

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

    def test_reason_mapping_prefers_canonical_codes_over_fallback_alias_duplicates(self):
        rows = [
            {
                "row_type": "summary",
                "stage": "runEdgeBoard",
                "run_id": "run-baseline",
                "ended_at": "2026-03-01T00:00:00Z",
                "edge_volatility": 0.01,
                "reason_codes": json.dumps(
                    {
                        "schema_id": "reason_code_alias_v1",
                        "reason_codes": {"STATS_ENR": 6, "STATS_MISS_A": 1, "STATS_MISS_B": 1},
                    }
                ),
                "stage_summaries": "[]",
            },
            {
                "row_type": "summary",
                "stage": "runEdgeBoard",
                "run_id": "run-candidate",
                "ended_at": "2026-03-02T00:00:00Z",
                "edge_volatility": 0.01,
                "reason_codes": json.dumps(
                    {
                        "schema_id": "reason_code_alias_v1",
                        "reason_codes": {"STATS_ENR": 7, "UNK_STATS_ENR": 1, "STATS_MISS_A": 1, "STATS_MISS_B": 1},
                        "fallback_aliases": {"UNK_STATS_ENR": "stats_enriched"},
                    }
                ),
                "stage_summaries": json.dumps(
                    [
                        {
                            "stage": "stageFetchPlayerStats",
                            "reason_metadata": {"fallback_aliases": {"UNK_STATS_ENR": "stats_enriched"}},
                        }
                    ]
                ),
            },
        ]
        report = evaluate_edge_quality_gate(
            rows,
            baseline_run_id="run-baseline",
            candidate_run_id="run-candidate",
            config=EdgeQualityGateConfig(min_feature_completeness=0.7, max_edge_volatility=0.03),
        )
        self.assertEqual("pass", report["status"])
        self.assertAlmostEqual(0.7777777777, report["candidate"]["feature_completeness"], places=6)
        self.assertIn("stats_enriched", report["candidate"]["diagnostics"]["reason_canonical_precedence_applied"])

    def test_reason_mapping_accepts_fallback_aliases_from_stage_reason_metadata(self):
        rows = [
            {
                "row_type": "summary",
                "stage": "runEdgeBoard",
                "run_id": "run-baseline",
                "ended_at": "2026-03-01T00:00:00Z",
                "edge_volatility": 0.01,
                "reason_codes": json.dumps(
                    {
                        "schema_id": "reason_code_alias_v1",
                        "reason_codes": {"STATS_ENR": 6, "STATS_MISS_A": 1, "STATS_MISS_B": 1},
                    }
                ),
                "stage_summaries": "[]",
            },
            {
                "row_type": "summary",
                "stage": "runEdgeBoard",
                "run_id": "run-candidate",
                "ended_at": "2026-03-02T00:00:00Z",
                "edge_volatility": 0.01,
                "reason_codes": json.dumps(
                    {
                        "schema_id": "reason_code_alias_v1",
                        "reason_codes": {"UNK_A": 4, "UNK_B": 1, "UNK_C": 1},
                    }
                ),
                "stage_summaries": json.dumps(
                    [
                        {
                            "stage": "stageFetchPlayerStats",
                            "reason_metadata": {
                                "fallback_aliases": {
                                    "UNK_A": "stats_enriched",
                                    "UNK_B": "stats_missing_player_a",
                                    "UNK_C": "stats_missing_player_b",
                                }
                            },
                        }
                    ]
                ),
            },
        ]
        report = evaluate_edge_quality_gate(
            rows,
            baseline_run_id="run-baseline",
            candidate_run_id="run-candidate",
            config=EdgeQualityGateConfig(min_feature_completeness=0.6, max_edge_volatility=0.03),
        )
        self.assertEqual("pass", report["status"])
        self.assertAlmostEqual(4 / 6, report["candidate"]["feature_completeness"], places=6)
        self.assertEqual("UNK_A,UNK_B,UNK_C", report["candidate"]["diagnostics"]["reason_fallback_only_aliases_used"])

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
        self.assertEqual("schema_missing", report["status"])
        self.assertIn(
            "missing_feature_completeness_metric reason_code=missing_field_feature_completeness",
            report["failures"],
        )
        self.assertIn("missing_edge_volatility_metric reason_code=missing_field_edge_volatility", report["failures"])

    def test_load_run_log_rows_normalizes_typed_quality_fields(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "Run_Log.json"
            path.write_text(
                json.dumps(
                    [
                        {
                            "row_type": "summary",
                            "stage": "runEdgeBoard",
                            "run_id": "run-typed",
                            "feature_completeness": "0.8",
                            "matched_events": "4",
                            "scored_signals": "",
                        }
                    ]
                ),
                encoding="utf-8",
            )
            rows = load_run_log_rows(str(path))
            self.assertEqual(0.8, rows[0]["feature_completeness"])
            self.assertEqual(4, rows[0]["matched_events"])
            self.assertIsNone(rows[0]["scored_signals"])
            self.assertEqual({}, rows[0]["fallback_aliases"])
            self.assertEqual({}, rows[0]["reason_aliases"])

    def test_load_run_log_rows_preserves_numeric_types_and_json_fields_without_coercion_regression(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "Run_Log.json"
            path.write_text(
                json.dumps(
                    [
                        {
                            "row_type": "summary",
                            "stage": "runEdgeBoard",
                            "run_id": "run-typed-no-regression",
                            "feature_completeness": 0.75,
                            "matched_events": 5,
                            "scored_signals": 2,
                            "fallback_aliases": "{\"UNK_A\":\"stats_enriched\"}",
                            "reason_aliases": {"UNK_A": "stats_enriched"},
                        }
                    ]
                ),
                encoding="utf-8",
            )
            rows = load_run_log_rows(str(path))
            self.assertIsInstance(rows[0]["feature_completeness"], float)
            self.assertIsInstance(rows[0]["matched_events"], int)
            self.assertIsInstance(rows[0]["scored_signals"], int)
            self.assertEqual({"UNK_A": "stats_enriched"}, rows[0]["fallback_aliases"])
            self.assertEqual({"UNK_A": "stats_enriched"}, rows[0]["reason_aliases"])

    def test_load_run_log_rows_marks_feature_completeness_schema_violation(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "Run_Log.json"
            path.write_text(
                json.dumps(
                    [
                        {
                            "row_type": "summary",
                            "stage": "runEdgeBoard",
                            "run_id": "run-bad-shape",
                            "feature_completeness": {"legacy_alias": "payload"},
                            "matched_events": 4,
                            "scored_signals": 1,
                        }
                    ]
                ),
                encoding="utf-8",
            )
            rows = load_run_log_rows(str(path))
            self.assertIsNone(rows[0]["feature_completeness"])
            self.assertEqual("run_log_row_schema_violation", rows[0]["schema_violation"])
            self.assertIn("feature_completeness_expected_numeric_or_null", rows[0]["field_type_error"])

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

    def test_legacy_schema_rows_emit_warning_but_remain_decisionable_when_other_metrics_exist(self):
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
        self.assertEqual("pass", report["status"])
        self.assertEqual([], report["failures"])
        self.assertTrue(
            any(item.startswith("legacy_schema_insufficient_feature_contract") for item in report["warnings"])
        )
        self.assertIn("legacy_missing_reconstruction", report["candidate"]["diagnostics"])

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

    def test_low_volume_can_use_aggregated_window_for_sample_check(self):
        rows = [
            self._summary_row("run-1", "2026-03-01T00:00:00Z", edge_volatility=0.01, scored_signals=5, matched_events=3),
            self._summary_row("run-2", "2026-03-02T00:00:00Z", edge_volatility=0.02, scored_signals=6, matched_events=3),
        ]
        report = evaluate_edge_quality_gate(
            rows,
            baseline_run_id="run-1",
            candidate_run_id="run-2",
            config=EdgeQualityGateConfig(
                max_edge_volatility=0.03,
                min_scored_signals_for_volatility=10,
                min_matched_events_for_volatility=5,
                volatility_sample_window_runs=2,
            ),
            ordered_run_ids=["run-1", "run-2"],
        )
        self.assertEqual("pass", report["status"])
        self.assertEqual("candidate_window_aggregate", report["sample_assessment"]["strategy"])
        self.assertTrue(any("aggregated_sample_used_for_edge_volatility" in item for item in report["warnings"]))

    def test_low_volume_mode_applies_semifinal_profile(self):
        rows = [
            self._summary_row("run-1", "2026-03-01T00:00:00Z", edge_volatility=0.01, scored_signals=9, matched_events=4),
            self._summary_row(
                "run-2",
                "2026-03-02T00:00:00Z",
                edge_volatility=0.02,
                scored_signals=9,
                matched_events=4,
                competition_stage="semifinal",
                upcoming_match_count=3,
            ),
        ]
        report = evaluate_edge_quality_gate(
            rows,
            baseline_run_id="run-1",
            candidate_run_id="run-2",
            config=EdgeQualityGateConfig(
                max_edge_volatility=0.03,
                min_scored_signals_for_volatility=10,
                min_matched_events_for_volatility=5,
                low_volume_min_scored_signals_for_volatility=8,
                low_volume_min_matched_events_for_volatility=4,
            ),
            ordered_run_ids=["run-1", "run-2"],
        )
        self.assertEqual("pass", report["status"])
        self.assertEqual("low_volume_semifinal_final", report["threshold_profile"]["active_profile"])
        self.assertTrue(report["threshold_profile"]["fallback_mode_used"])
        self.assertIn("tournament_phase_semifinal_or_final", report["threshold_profile"]["activation_reasons"])
        self.assertIn("evidence_snapshot", report["threshold_profile"])
        self.assertTrue(report["threshold_profile"]["low_volume_mode"]["active"])
        self.assertEqual("none", report["threshold_profile"]["schedule_context"]["stage_inference_fallback"])
        self.assertFalse(report["sample_assessment"]["strict_default_result"]["enough"])
        self.assertEqual("low_volume_profile_sufficient_sample", report["sample_assessment"]["reason_code"])

    def test_schedule_context_fallback_when_rows_present_without_stage(self):
        rows = [
            self._summary_row("run-1", "2026-03-01T00:00:00Z", edge_volatility=0.01, scored_signals=12, matched_events=8),
            self._summary_row(
                "run-2",
                "2026-03-02T00:00:00Z",
                edge_volatility=0.02,
                scored_signals=12,
                matched_events=8,
                raw_schedule_rows=json.dumps(
                    [
                        {"event_id": "m1", "start_time": "2026-03-02T12:00:00Z", "tournament_tier": "WTA 1000"},
                        {"event_id": "m2", "start_time": "2026-03-02T14:00:00Z", "tournament_tier": "WTA 1000"},
                    ]
                ),
            ),
        ]
        report = evaluate_edge_quality_gate(
            rows,
            baseline_run_id="run-1",
            candidate_run_id="run-2",
            config=EdgeQualityGateConfig(),
            ordered_run_ids=["run-1", "run-2"],
        )
        schedule_context = report["threshold_profile"]["schedule_context"]
        self.assertEqual(2, schedule_context["upcoming_match_count"])
        self.assertFalse(schedule_context["stage_inference_available"])
        self.assertEqual("schedule_rows_present_but_stage_unknown", schedule_context["stage_inference_fallback"])
        self.assertIn("schedule_stage_inference_unavailable", report["threshold_profile"]["activation_reasons"])

    def test_threshold_profile_defaults_to_strict_when_evidence_missing(self):
        rows = [
            self._summary_row("run-1", "2026-03-01T00:00:00Z", edge_volatility=0.01, scored_signals=12, matched_events=8),
            self._summary_row("run-2", "2026-03-02T00:00:00Z", edge_volatility=0.02, scored_signals=12, matched_events=8),
        ]
        report = evaluate_edge_quality_gate(
            rows,
            baseline_run_id="run-1",
            candidate_run_id="run-2",
            config=EdgeQualityGateConfig(),
            ordered_run_ids=["run-1", "run-2"],
        )
        self.assertEqual("strict_default", report["threshold_profile"]["active_profile"])
        self.assertIn("insufficient_low_volume_evidence_keep_strict", report["threshold_profile"]["activation_reasons"])

    def test_threshold_profile_uses_ultra_low_volume_single_match_profile(self):
        rows = [
            self._summary_row("run-1", "2026-03-01T00:00:00Z", edge_volatility=0.01, scored_signals=12, matched_events=8),
            self._summary_row(
                "run-2",
                "2026-03-02T00:00:00Z",
                edge_volatility=0.02,
                scored_signals=9,
                matched_events=4,
                reason_codes=json.dumps({"raw_schedule_upserts": 1}),
            ),
        ]
        report = evaluate_edge_quality_gate(
            rows,
            baseline_run_id="run-1",
            candidate_run_id="run-2",
            config=EdgeQualityGateConfig(
                min_scored_signals_for_volatility=10,
                min_matched_events_for_volatility=5,
                low_volume_min_scored_signals_for_volatility=8,
                low_volume_min_matched_events_for_volatility=4,
            ),
            ordered_run_ids=["run-1", "run-2"],
        )
        self.assertEqual("ultra_low_volume_single_match", report["threshold_profile"]["active_profile"])
        self.assertFalse(report["threshold_profile"]["fallback_mode_used"])
        self.assertEqual("insufficient_sample", report["status"])

    def test_compare_report_marks_low_volume_strict_sample_block(self):
        rows = [
            self._summary_row("run-1", "2026-03-01T00:00:00Z", edge_volatility=0.01, scored_signals=3, matched_events=2),
            self._summary_row(
                "run-2",
                "2026-03-02T00:00:00Z",
                edge_volatility=0.02,
                scored_signals=3,
                matched_events=2,
                competition_stage="final",
                upcoming_match_count=1,
            ),
        ]
        report = evaluate_edge_quality_compare_report(
            rows=rows,
            baseline_run_id="run-1",
            candidate_run_id="run-2",
            config=EdgeQualityGateConfig(
                max_edge_volatility=0.03,
                min_scored_signals_for_volatility=10,
                min_matched_events_for_volatility=5,
                low_volume_min_scored_signals_for_volatility=6,
                low_volume_min_matched_events_for_volatility=3,
            ),
            ordered_run_ids=["run-1", "run-2"],
            fallback_recent_run_window_radius=1,
            fallback_min_neighboring_pairs=1,
        )
        self.assertEqual("blocked_low_volume_strict_sample", report["gate_verdict"])
        self.assertEqual("EDGE_QUALITY_GATE_BLOCKED_LOW_VOLUME_STRICT_SAMPLE", report["reason_code"])
        self.assertTrue(report["final_operator_summary"]["blocked_by_strict_sample_in_low_volume"])
        self.assertIn("schedule_context", report["evidence_bundle"])
        self.assertIn("schedule_context", report["final_operator_summary"])

    def test_candidate_only_marks_zero_scored_signals_as_non_decisive(self):
        rows = [
            self._summary_row(
                "run-1",
                "2026-03-01T00:00:00Z",
                edge_volatility=0.01,
                scored_signals=12,
                matched_events=8,
                suppression_counts={"edge_below_threshold": 1},
            ),
            self._summary_row(
                "run-2",
                "2026-03-02T00:00:00Z",
                edge_volatility=0.02,
                scored_signals=0,
                matched_events=8,
                suppression_counts={"edge_below_threshold": 50},
            ),
        ]
        report = evaluate_edge_quality_gate(
            rows,
            baseline_run_id="run-1",
            candidate_run_id="run-2",
            config=EdgeQualityGateConfig(
                max_edge_volatility=0.03,
                min_scored_signals_for_volatility=0,
                min_matched_events_for_volatility=5,
                max_suppression_drift=0.1,
                suppression_min_volume=1,
                volatility_sample_window_runs=1,
            ),
            ordered_run_ids=["run-1", "run-2"],
        )
        self.assertEqual("insufficient_sample", report["status"])
        self.assertEqual("no_scored_signals", report["sample_assessment"]["reason_code"])
        self.assertEqual({}, report["suppression_drifts"])
        self.assertFalse(any(item.startswith("suppression_drift_exceeded") for item in report["failures"]))
        self.assertTrue(any("reason_code=no_scored_signals" in item for item in report["warnings"]))

    def test_candidate_only_marks_low_matched_events_as_non_decisive(self):
        rows = [
            self._summary_row("run-1", "2026-03-01T00:00:00Z", edge_volatility=0.01, scored_signals=12, matched_events=8),
            self._summary_row("run-2", "2026-03-02T00:00:00Z", edge_volatility=0.02, scored_signals=12, matched_events=4),
        ]
        report = evaluate_edge_quality_gate(
            rows,
            baseline_run_id="run-1",
            candidate_run_id="run-2",
            config=EdgeQualityGateConfig(
                max_edge_volatility=0.03,
                min_scored_signals_for_volatility=10,
                min_matched_events_for_volatility=5,
                volatility_sample_window_runs=1,
            ),
            ordered_run_ids=["run-1", "run-2"],
        )
        self.assertEqual("insufficient_sample", report["status"])
        self.assertEqual("insufficient_matched_events", report["sample_assessment"]["reason_code"])

    def test_candidate_only_accepts_scored_signals_at_threshold(self):
        rows = [
            self._summary_row("run-1", "2026-03-01T00:00:00Z", edge_volatility=0.01, scored_signals=10, matched_events=5),
            self._summary_row("run-2", "2026-03-02T00:00:00Z", edge_volatility=0.02, scored_signals=10, matched_events=5),
        ]
        report = evaluate_edge_quality_gate(
            rows,
            baseline_run_id="run-1",
            candidate_run_id="run-2",
            config=EdgeQualityGateConfig(
                max_edge_volatility=0.03,
                min_scored_signals_for_volatility=10,
                min_matched_events_for_volatility=5,
                volatility_sample_window_runs=1,
            ),
            ordered_run_ids=["run-1", "run-2"],
        )
        self.assertEqual("pass", report["status"])
        self.assertEqual("sufficient_sample", report["sample_assessment"]["reason_code"])

    def test_compare_report_triggers_windowed_fallback_when_pair_is_insufficient_sample(self):
        rows = [
            self._summary_row("run-1", "2026-03-01T00:00:00Z", edge_volatility=0.01, scored_signals=12, matched_events=8),
            self._summary_row("run-2", "2026-03-02T00:00:00Z", edge_volatility=0.02, scored_signals=3, matched_events=2),
            self._summary_row("run-3", "2026-03-03T00:00:00Z", edge_volatility=0.015, scored_signals=12, matched_events=8),
            self._summary_row("run-4", "2026-03-04T00:00:00Z", edge_volatility=0.012, scored_signals=12, matched_events=8),
        ]
        report = evaluate_edge_quality_compare_report(
            rows=rows,
            baseline_run_id="run-1",
            candidate_run_id="run-2",
            config=EdgeQualityGateConfig(
                max_edge_volatility=0.03,
                min_scored_signals_for_volatility=10,
                min_matched_events_for_volatility=5,
                volatility_sample_window_runs=1,
            ),
            ordered_run_ids=["run-1", "run-2", "run-3", "run-4"],
            fallback_recent_run_window_radius=2,
        )
        self.assertEqual("pass", report["status"])
        self.assertEqual("EDGE_QUALITY_GATE_PASSED", report["reason_code"])
        self.assertEqual("insufficient_sample", report["pair_level_result"]["status"])
        self.assertIsNotNone(report["windowed_fallback_result"])
        self.assertEqual("fallback_window_assessment", report["windowed_fallback_result"]["label"])
        self.assertEqual("insufficient_sample", report["windowed_fallback_result"]["triggered_by_status"])
        self.assertEqual("insufficient_scored_signals", report["windowed_fallback_result"]["triggered_by_reason"])
        self.assertEqual(3, report["windowed_fallback_result"]["pair_count"])
        self.assertEqual("pass", report["windowed_fallback_result"]["decision_support_status"])
        self.assertEqual("insufficient_sample", report["final_operator_summary"]["strict_pair_status"])
        self.assertEqual("insufficient_scored_signals", report["final_operator_summary"]["strict_pair_status_reason"])
        self.assertEqual("pass", report["final_operator_summary"]["windowed_decision_status"])
        self.assertEqual("windowed_fallback_result", report["final_operator_summary"]["decision_authoritative_source"])
        self.assertEqual("pass", report["final_operator_summary"]["decision_authoritative_status"])
        self.assertTrue(
            all("failure_diagnostics" in pair for pair in report["windowed_fallback_result"]["pairs"])
        )

    def test_compare_report_keeps_windowed_fallback_null_when_pair_is_decisionable(self):
        rows = [
            self._summary_row("run-1", "2026-03-01T00:00:00Z", edge_volatility=0.01, scored_signals=12, matched_events=8),
            self._summary_row("run-2", "2026-03-02T00:00:00Z", edge_volatility=0.02, scored_signals=12, matched_events=8),
        ]
        report = evaluate_edge_quality_compare_report(
            rows=rows,
            baseline_run_id="run-1",
            candidate_run_id="run-2",
            config=EdgeQualityGateConfig(max_edge_volatility=0.03),
            ordered_run_ids=["run-1", "run-2"],
            fallback_recent_run_window_radius=2,
        )
        self.assertEqual("pass", report["status"])
        self.assertEqual("EDGE_QUALITY_GATE_PASSED", report["reason_code"])
        self.assertEqual("pass", report["pair_level_result"]["status"])
        self.assertIsNone(report["windowed_fallback_result"])
        self.assertEqual("pass", report["final_operator_summary"]["strict_pair_status"])
        self.assertEqual("not_triggered", report["final_operator_summary"]["windowed_decision_status"])
        self.assertEqual("strict_pair_gate", report["final_operator_summary"]["decision_authoritative_source"])
        self.assertEqual("pass", report["final_operator_summary"]["decision_authoritative_status"])

    def test_compare_report_emits_failed_reason_code_when_quality_regresses(self):
        rows = [
            self._summary_row("run-1", "2026-03-01T00:00:00Z", edge_volatility=0.01, scored_signals=12, matched_events=8),
            self._summary_row("run-2", "2026-03-02T00:00:00Z", edge_volatility=0.20, scored_signals=12, matched_events=8),
        ]
        report = evaluate_edge_quality_compare_report(
            rows=rows,
            baseline_run_id="run-1",
            candidate_run_id="run-2",
            config=EdgeQualityGateConfig(max_edge_volatility=0.03),
            ordered_run_ids=["run-1", "run-2"],
        )
        self.assertEqual("failed_quality_regression", report["gate_verdict"])
        self.assertEqual("EDGE_QUALITY_GATE_FAILED", report["reason_code"])

    def test_compare_report_dedupes_logically_identical_csv_json_summaries(self):
        run_1 = self._summary_row("run-1", "2026-03-01T00:00:00Z", edge_volatility=0.01, scored_signals=12, matched_events=8)
        run_2 = self._summary_row("run-2", "2026-03-02T00:00:00Z", edge_volatility=0.02, scored_signals=12, matched_events=8)
        with tempfile.TemporaryDirectory() as tmp:
            csv_path = Path(tmp) / "Run_Log.csv"
            json_path = Path(tmp) / "Run_Log.json"
            csv_fields = sorted(set(run_1.keys()) | {"source_kind", "source_path"})
            with csv_path.open("w", encoding="utf-8", newline="") as handle:
                writer = csv.DictWriter(handle, fieldnames=csv_fields)
                writer.writeheader()
                writer.writerow({**run_1, "source_kind": "csv", "source_path": "exports/Run_Log.csv"})
                writer.writerow(run_2)
            json_path.write_text(
                json.dumps(
                    [
                        {
                            **run_1,
                            "source_kind": "json",
                            "source_path": "exports/Run_Log.json",
                        }
                    ]
                ),
                encoding="utf-8",
            )
            rows = load_run_log_rows(tmp)

        report = evaluate_edge_quality_compare_report(
            rows=rows,
            baseline_run_id="run-1",
            candidate_run_id="run-2",
            config=EdgeQualityGateConfig(max_edge_volatility=0.03),
            ordered_run_ids=["run-1", "run-2"],
            fallback_recent_run_window_radius=2,
        )
        self.assertEqual("pass", report["status"])
        self.assertEqual("pass", report["pair_level_result"]["status"])

    def test_compare_report_expands_fallback_window_to_min_neighboring_pairs(self):
        rows = [
            self._summary_row("run-1", "2026-03-01T00:00:00Z", edge_volatility=0.012, scored_signals=12, matched_events=8),
            self._summary_row("run-2", "2026-03-02T00:00:00Z", edge_volatility=0.020, scored_signals=2, matched_events=1),
            self._summary_row("run-3", "2026-03-03T00:00:00Z", edge_volatility=0.013, scored_signals=12, matched_events=8),
            self._summary_row("run-4", "2026-03-04T00:00:00Z", edge_volatility=0.014, scored_signals=12, matched_events=8),
            self._summary_row("run-5", "2026-03-05T00:00:00Z", edge_volatility=0.015, scored_signals=12, matched_events=8),
        ]
        report = evaluate_edge_quality_compare_report(
            rows=rows,
            baseline_run_id="run-1",
            candidate_run_id="run-2",
            config=EdgeQualityGateConfig(
                max_edge_volatility=0.03,
                min_scored_signals_for_volatility=10,
                min_matched_events_for_volatility=5,
                volatility_sample_window_runs=1,
            ),
            ordered_run_ids=["run-1", "run-2", "run-3", "run-4", "run-5"],
            fallback_recent_run_window_radius=1,
            fallback_min_neighboring_pairs=4,
        )
        self.assertEqual("insufficient_sample", report["pair_level_result"]["status"])
        self.assertEqual("pass", report["status"])
        self.assertEqual(4, report["windowed_fallback_result"]["pair_count"])
        self.assertEqual(4, report["windowed_fallback_result"]["min_neighboring_pairs"])
        self.assertTrue(report["windowed_fallback_result"]["effective_sample_counts"]["enough_sample_for_decision"])
        self.assertEqual("windowed_fallback_result", report["final_operator_summary"]["decision_authoritative_source"])
        self.assertEqual("pass", report["final_operator_summary"]["decision_authoritative_status"])

    def test_compare_report_low_volume_fallback_expands_until_decision_threshold_met(self):
        rows = [
            self._summary_row("run-1", "2026-03-01T00:00:00Z", edge_volatility=0.012, scored_signals=4, matched_events=2),
            self._summary_row("run-2", "2026-03-02T00:00:00Z", edge_volatility=0.015, scored_signals=3, matched_events=2),
            self._summary_row("run-3", "2026-03-03T00:00:00Z", edge_volatility=0.013, scored_signals=4, matched_events=2),
            self._summary_row("run-4", "2026-03-04T00:00:00Z", edge_volatility=0.014, scored_signals=4, matched_events=2),
            self._summary_row("run-5", "2026-03-05T00:00:00Z", edge_volatility=0.016, scored_signals=4, matched_events=2),
        ]
        report = evaluate_edge_quality_compare_report(
            rows=rows,
            baseline_run_id="run-1",
            candidate_run_id="run-2",
            config=EdgeQualityGateConfig(
                max_edge_volatility=0.03,
                min_scored_signals_for_volatility=10,
                min_matched_events_for_volatility=5,
                volatility_sample_window_runs=1,
            ),
            ordered_run_ids=["run-1", "run-2", "run-3", "run-4", "run-5"],
            fallback_recent_run_window_radius=1,
            fallback_min_neighboring_pairs=1,
        )
        self.assertEqual("insufficient_sample", report["pair_level_result"]["status"])
        self.assertEqual("pass", report["status"])
        self.assertEqual(3, report["windowed_fallback_result"]["pair_count"])
        self.assertTrue(report["windowed_fallback_result"]["effective_sample_counts"]["enough_sample_for_decision"])
        self.assertEqual(11, report["windowed_fallback_result"]["effective_sample_counts"]["scored_signals"])
        self.assertEqual(6, report["windowed_fallback_result"]["effective_sample_counts"]["matched_events"])
        self.assertEqual("windowed_fallback_result", report["final_operator_summary"]["decision_authoritative_source"])

    def test_compare_report_low_volume_fallback_does_not_false_pass_when_threshold_unmet(self):
        rows = [
            self._summary_row("run-1", "2026-03-01T00:00:00Z", edge_volatility=0.012, scored_signals=3, matched_events=1),
            self._summary_row("run-2", "2026-03-02T00:00:00Z", edge_volatility=0.015, scored_signals=3, matched_events=1),
            self._summary_row("run-3", "2026-03-03T00:00:00Z", edge_volatility=0.013, scored_signals=3, matched_events=1),
        ]
        report = evaluate_edge_quality_compare_report(
            rows=rows,
            baseline_run_id="run-1",
            candidate_run_id="run-2",
            config=EdgeQualityGateConfig(
                max_edge_volatility=0.03,
                min_scored_signals_for_volatility=10,
                min_matched_events_for_volatility=5,
                volatility_sample_window_runs=1,
            ),
            ordered_run_ids=["run-1", "run-2", "run-3"],
            fallback_recent_run_window_radius=1,
            fallback_min_neighboring_pairs=1,
        )
        self.assertEqual("insufficient_sample", report["pair_level_result"]["status"])
        self.assertEqual("insufficient_sample", report["status"])
        self.assertFalse(report["windowed_fallback_result"]["effective_sample_counts"]["enough_sample_for_decision"])
        self.assertEqual("insufficient_sample", report["final_operator_summary"]["decision_authoritative_status"])

    def test_compare_report_fallback_ignores_partial_legacy_schema_pairs_when_pass_pairs_exist(self):
        rows = load_run_log_rows(
            str(ROOT / "scripts" / "fixtures" / "edge_quality_compare_legacy_partial_metrics.json")
        )
        report = evaluate_edge_quality_compare_report(
            rows=rows,
            baseline_run_id="run-1",
            candidate_run_id="run-2",
            config=EdgeQualityGateConfig(
                max_edge_volatility=0.03,
                min_scored_signals_for_volatility=10,
                min_matched_events_for_volatility=5,
            ),
            ordered_run_ids=["run-1", "run-2", "run-3", "run-4", "run-5"],
            fallback_recent_run_window_radius=3,
        )
        self.assertEqual("insufficient_sample", report["pair_level_result"]["status"])
        self.assertEqual("pass", report["status"])
        self.assertEqual(
            "windowed_fallback_result",
            report["final_operator_summary"]["decision_authoritative_source"],
        )
        self.assertEqual("pass", report["windowed_fallback_result"]["decision_support_status"])
        self.assertGreater(report["windowed_fallback_result"]["status_counts"]["schema_missing"], 0)

    def test_compare_report_stake_policy_enabled_mode_changes_policy_outcome_comparison(self):
        fixture_path = ROOT / "scripts" / "fixtures" / "stake_policy_mixed_signal_rows.json"
        fixture = json.loads(fixture_path.read_text(encoding="utf-8"))

        rows = []
        rows.extend(
            [
                self._summary_row("run-1", "2026-03-01T00:00:00Z", edge_volatility=0.01),
                self._summary_row("run-2", "2026-03-02T00:00:00Z", edge_volatility=0.02),
            ]
        )
        for row in fixture["rows"]:
            case_id = str(row.get("case_id") or "")
            if case_id == "wrong_run_ignored":
                continue
            updated = dict(row)
            updated["run_id"] = "run-1" if case_id in {"boundary_2000", "above_min"} else "run-2"
            rows.append(updated)

        disabled_report = evaluate_edge_quality_compare_report(
            rows=rows,
            baseline_run_id="run-1",
            candidate_run_id="run-2",
            config=EdgeQualityGateConfig(stake_policy_enabled=False),
            ordered_run_ids=["run-1", "run-2"],
        )
        enabled_report = evaluate_edge_quality_compare_report(
            rows=rows,
            baseline_run_id="run-1",
            candidate_run_id="run-2",
            config=EdgeQualityGateConfig(stake_policy_enabled=True, stake_policy_min_stake_mxn=20.0),
            ordered_run_ids=["run-1", "run-2"],
        )

        self.assertEqual("standard_compare_runbook", disabled_report["runbook_branch"])
        self.assertEqual("stake_policy_enabled_compare_runbook", enabled_report["runbook_branch"])
        self.assertEqual(
            "runbook/README.md#phase-2--baseline-vs-policy-on-comparison-on-matched-windows",
            disabled_report["runbook_path"],
        )
        self.assertEqual(
            "runbook/README.md#stake-policy-enabled-compare-lane-policy-on-only",
            enabled_report["runbook_path"],
        )
        self.assertFalse(disabled_report["evidence_bundle"]["stake_policy_enabled"])
        self.assertTrue(enabled_report["evidence_bundle"]["stake_policy_enabled"])
        self.assertFalse(disabled_report["stake_policy_enabled"])
        self.assertTrue(enabled_report["stake_policy_enabled"])
        self.assertEqual(100.0, enabled_report["unit_size_mxn"])
        self.assertEqual(20.0, enabled_report["min_bet_mxn"])
        self.assertEqual(20.0, enabled_report["bucket_step_mxn"])
        self.assertEqual("down", enabled_report["rounding_mode"])
        self.assertEqual(
            3,
            enabled_report["stake_policy_outcome_comparison"]["counts"]["suppressed_count"]["delta"],
        )
        self.assertNotEqual(
            disabled_report["stake_policy_outcome_comparison"]["reason_code_shift"],
            enabled_report["stake_policy_outcome_comparison"]["reason_code_shift"],
        )
        self.assertIn(
            "stake_policy_disabled",
            disabled_report["stake_policy_outcome_comparison"]["reason_code_shift"],
        )
        self.assertIn(
            "stake_below_min_suppressed",
            enabled_report["stake_policy_outcome_comparison"]["reason_code_shift"],
        )
        self.assertIn("stake_mode_used_shift", enabled_report["stake_policy_outcome_comparison"])
        self.assertIn("adjustment_reason_code_shift", enabled_report["stake_policy_outcome_comparison"])
        self.assertIn("final_risk_mxn_aggregate_shift", enabled_report["stake_policy_outcome_comparison"])

    def test_compare_report_fails_when_compare_set_mixes_policy_enabled_tags(self):
        rows = [
            self._summary_row("run-1", "2026-03-01T00:00:00Z", edge_volatility=0.01),
            self._summary_row("run-2", "2026-03-02T00:00:00Z", edge_volatility=0.02),
            self._summary_row("run-3", "2026-03-03T00:00:00Z", edge_volatility=0.02),
        ]
        rows[0]["signal_decision_summary"] = json.dumps({"stake_policy_summary": {"enabled": True}})
        rows[1]["signal_decision_summary"] = json.dumps({"stake_policy_summary": {"enabled": True}})
        rows[2]["signal_decision_summary"] = json.dumps({"stake_policy_summary": {"enabled": False}})

        with self.assertRaisesRegex(ValueError, "Mixed stake_policy_enabled states"):
            evaluate_edge_quality_compare_report(
                rows=rows,
                baseline_run_id="run-1",
                candidate_run_id="run-2",
                config=EdgeQualityGateConfig(stake_policy_enabled=True),
                ordered_run_ids=["run-1", "run-2", "run-3"],
            )

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
        self.assertEqual("pass", report["decisionability_status"])
        self.assertEqual("fail", report["quality_status"])
        self.assertEqual("not_evaluated", report["parity_contract_status"])
        self.assertEqual(
            "quality_blocker:decisionable_window_fail_rate_exceeded_threshold",
            report["operator_composite_reason"],
        )
        self.assertEqual(1, report["decisionable_window_count"])
        self.assertEqual(1, report["window_reports"][0]["decisionable_status_counts"]["true_fail"])

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
        self.assertEqual(1, window["decisionable_status_counts"]["true_fail"])
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
        self.assertEqual("insufficient_sample", report["decisionability_status"])
        self.assertEqual("insufficient_sample", report["quality_status"])
        self.assertEqual("decisionability_blocker:no_decisionable_windows", report["operator_composite_reason"])

    def test_daily_slo_blocks_pass_uplift_when_any_window_is_insufficient(self):
        rows = [
            self._summary_row("run-1", "2026-03-01T00:00:00Z", edge_volatility=0.01, scored_signals=12, matched_events=8),
            self._summary_row("run-2", "2026-03-02T00:00:00Z", edge_volatility=0.01, scored_signals=12, matched_events=8),
            self._summary_row("run-3", "2026-03-03T00:00:00Z", edge_volatility=0.01, scored_signals=12, matched_events=8),
            self._summary_row("run-4", "2026-03-04T00:00:00Z", edge_volatility=0.01, scored_signals=12, matched_events=8),
            self._summary_row("run-5", "2026-03-05T00:00:00Z", edge_volatility=0.01, scored_signals=12, matched_events=8),
        ]
        report = evaluate_daily_edge_quality_slo(
            rows=rows,
            gate_config=EdgeQualityGateConfig(max_edge_volatility=0.03, volatility_context_min_pairs=99),
            slo_config=DailyEdgeQualitySLOConfig(
                window_days=(3, 7),
                min_pairs_per_window=2,
                fail_rate_threshold=0.5,
                min_scored_signals_by_window={7: 20},
                min_matched_events_by_window={7: 10},
            ),
            as_of_utc="2026-03-05T12:00:00Z",
        )
        self.assertEqual("insufficient_sample", report["gate_verdict"])
        self.assertEqual("insufficient_sample_floor_not_met_for_all_windows", report["gate_reason"])
        short_window = next(item for item in report["window_reports"] if item["window_days"] == 3)
        long_window = next(item for item in report["window_reports"] if item["window_days"] == 7)
        self.assertTrue(short_window["decisionable"])
        self.assertFalse(long_window["decisionable"])

    def test_daily_slo_window_specific_sample_thresholds_prevent_low_volume_false_pass(self):
        rows = [
            self._summary_row("run-1", "2026-03-01T00:00:00Z", edge_volatility=0.01, scored_signals=12, matched_events=8),
            self._summary_row("run-2", "2026-03-02T00:00:00Z", edge_volatility=0.01, scored_signals=12, matched_events=8),
            self._summary_row("run-3", "2026-03-03T00:00:00Z", edge_volatility=0.01, scored_signals=12, matched_events=8),
            self._summary_row("run-4", "2026-03-04T00:00:00Z", edge_volatility=0.01, scored_signals=12, matched_events=8),
        ]
        report = evaluate_daily_edge_quality_slo(
            rows=rows,
            gate_config=EdgeQualityGateConfig(max_edge_volatility=0.03, volatility_context_min_pairs=99),
            slo_config=DailyEdgeQualitySLOConfig(
                window_days=(3,),
                min_pairs_per_window=1,
                fail_rate_threshold=0.2,
                min_scored_signals_by_window={3: 20},
                min_matched_events_by_window={3: 10},
            ),
            as_of_utc="2026-03-04T12:00:00Z",
        )
        window = report["window_reports"][0]
        self.assertEqual("insufficient_sample", window["verdict"])
        self.assertEqual(0, window["decisionable_pair_count"])
        self.assertEqual(2, window["excluded_pair_count"])
        self.assertTrue(any("low_scored_signals" in item["reasons"] for item in window["excluded_pairs"]))
        self.assertTrue(any("low_matched_events" in item["reasons"] for item in window["excluded_pairs"]))
        trend = report["window_ratio_trends"][0]
        self.assertEqual(0.0, trend["decisionable_ratio"])
        self.assertEqual(0.0, trend["insufficient_sample_ratio"])

    def test_daily_slo_strict_gating_uses_actionable_window_runs_only(self):
        rows = [
            self._summary_row(
                "run-1",
                "2026-03-01T00:00:00Z",
                edge_volatility=0.01,
                odds_refresh_executed=1,
                opening_lag_exceeded=0,
                opening_lag_within_limit=1,
            ),
            self._summary_row(
                "run-2",
                "2026-03-02T00:00:00Z",
                edge_volatility=0.09,
                odds_refresh_executed=0,
                odds_refresh_skipped_outside_window=1,
                no_hit_terminal_reason_code="events_outside_time_window",
            ),
            self._summary_row(
                "run-3",
                "2026-03-03T00:00:00Z",
                edge_volatility=0.09,
                odds_refresh_executed=1,
                opening_lag_exceeded=1,
                opening_lag_within_limit=0,
            ),
            self._summary_row(
                "run-4",
                "2026-03-04T00:00:00Z",
                edge_volatility=0.02,
                odds_refresh_executed=1,
                opening_lag_exceeded=0,
                opening_lag_within_limit=1,
            ),
        ]
        report = evaluate_daily_edge_quality_slo(
            rows=rows,
            gate_config=EdgeQualityGateConfig(max_edge_volatility=0.03),
            slo_config=DailyEdgeQualitySLOConfig(window_days=(7,), min_pairs_per_window=1, fail_rate_threshold=0.5),
            as_of_utc="2026-03-04T12:00:00Z",
        )
        window = report["window_reports"][0]
        self.assertEqual(4, window["run_count"])
        self.assertEqual(2, window["actionable_window_run_count"])
        self.assertEqual(2, window["non_actionable_run_count"])
        self.assertEqual(1, window["pair_count"])
        self.assertEqual(1, window["decisionable_pair_count"])
        self.assertEqual("pass", window["verdict"])
        self.assertEqual(["run-2", "run-3"], [item["run_id"] for item in window["non_actionable_runs"]])

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
            "aggregate_status_counts": {"pass": 4, "true_fail": 0, "insufficient_sample": 1, "schema_missing": 0},
            "window_reports": [
                {
                    "window_days": 3,
                    "pair_count": 2,
                    "decisionable": True,
                    "fail_rate": 0.0,
                    "verdict": "pass",
                    "pairs": [
                        {
                            "status": "fail",
                            "failures": [
                                "edge_volatility_above_ceiling (candidate=0.0500 > ceiling=0.0300)",
                                "edge_volatility_above_ceiling (candidate=0.0500 > ceiling=0.0300)",
                                "suppression_drift_exceeded (reason=foo)",
                            ],
                            "warnings": [
                                "insufficient_sample_for_edge_volatility (scored_signals=3 required>=10)",
                            ],
                            "candidate": {"ended_at": "2026-03-24T00:10:00+00:00"},
                        },
                        {
                            "status": "insufficient_sample",
                            "failures": [
                                "suppression_drift_exceeded (reason=bar)",
                            ],
                            "warnings": [
                                "insufficient_sample_for_edge_volatility (scored_signals=2 required>=10)",
                                "windowed_fallback_not_triggered (pair_count=0)",
                            ],
                            "candidate": {"ended_at": "2026-03-24T00:20:00+00:00"},
                        },
                    ],
                }
            ],
        }
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_reports_dir = Path(temp_dir) / "reports"
            temp_archive_dir = Path(temp_dir) / "archive"
            daily_path, summary_path, sentinel_path = write_daily_slo_artifacts(
                report=report,
                output_dir=str(temp_reports_dir),
                archive_dir=str(temp_archive_dir),
            )
            self.assertTrue(daily_path.exists())
            self.assertTrue(summary_path.exists())
            self.assertTrue(sentinel_path.exists())
            self.assertIn("edge_quality_daily_slo_", daily_path.name)
            self.assertIn("edge_quality_daily_readiness_sentinel_", sentinel_path.name)
            markdown_paths = list(temp_reports_dir.glob("edge_quality_daily_slo_summary_*.md"))
            self.assertEqual(1, len(markdown_paths))
            markdown = markdown_paths[0].read_text(encoding="utf-8")
            self.assertIn("parity_contract_status", markdown)
            self.assertIn("decisionability_status", markdown)
            self.assertIn("quality_status", markdown)
            self.assertIn("## Quality summary (compact)", markdown)
            self.assertIn("edge_quality_daily_slo_quality_summary_v1", markdown)
            summary_lines = summary_path.read_text(encoding="utf-8").strip().splitlines()
            self.assertGreaterEqual(len(summary_lines), 1)
            summary_payload = json.loads(summary_lines[-1])
            self.assertIn("triage_decision_summary", summary_payload)
            quality_paths = list(temp_reports_dir.glob("edge_quality_daily_slo_quality_summary_*.json"))
            self.assertEqual(1, len(quality_paths))
            quality_summary = json.loads(quality_paths[0].read_text(encoding="utf-8"))
            self.assertEqual({"pass": 0, "fail": 1, "insufficient_sample": 1}, quality_summary["status_counts"])
            self.assertEqual("edge_volatility_above_ceiling", quality_summary["top_failure_codes"][0]["code"])
            self.assertEqual(2, quality_summary["top_failure_codes"][0]["count"])
            self.assertEqual("2026-03-24T00:10:00+00:00", quality_summary["top_failure_codes"][0]["first_seen_utc"])
            self.assertEqual("2026-03-24T00:10:00+00:00", quality_summary["top_failure_codes"][0]["last_seen_utc"])
            self.assertEqual("insufficient_sample_for_edge_volatility", quality_summary["top_warning_codes"][0]["code"])
            self.assertEqual(2, quality_summary["top_warning_codes"][0]["count"])

    def test_daily_slo_builds_readiness_sentinel_and_triage_summary(self):
        rows = [
            self._summary_row("run-a", "2026-03-21T00:00:00Z", edge_volatility=0.01, scored_signals=0, matched_events=0, no_hit_terminal_reason_code="none"),
            self._summary_row("run-b", "2026-03-22T00:00:00Z", edge_volatility=0.01, scored_signals=3, matched_events=1, no_hit_terminal_reason_code="odds_present_but_match_failed"),
            self._summary_row("run-c", "2026-03-22T01:00:00Z", edge_volatility=0.01, scored_signals=2, matched_events=2, no_hit_terminal_reason_code="events_outside_time_window"),
            self._summary_row("run-d", "2026-03-22T02:00:00Z", edge_volatility=0.01, scored_signals=0, matched_events=0, no_hit_terminal_reason_code="events_outside_time_window"),
        ]
        report = evaluate_daily_edge_quality_slo(
            rows=rows,
            gate_config=EdgeQualityGateConfig(max_edge_volatility=0.03),
            slo_config=DailyEdgeQualitySLOConfig(window_days=(3,), min_pairs_per_window=1, fail_rate_threshold=0.5),
            as_of_utc="2026-03-22T12:00:00Z",
        )
        sentinel = report["daily_readiness_sentinel"]
        self.assertEqual("edge_quality_daily_readiness_sentinel_v1", sentinel["schema"])
        self.assertEqual(2, len(sentinel["days"]))
        latest = sentinel["triage_decision_summary"]
        self.assertEqual("2026-03-22", latest["date"])
        self.assertEqual(2, latest["runs_with_matched_events_gt0"])
        self.assertEqual(2, latest["runs_with_scored_signals_gt0"])
        self.assertEqual("events_outside_time_window", latest["dominant_terminal_no_hit_reason"])
        self.assertEqual("ready_for_strict_gate", latest["gate_recommendation"])
        self.assertEqual(latest, report["triage_decision_summary"])


if __name__ == "__main__":
    unittest.main()
