import csv
import json
import subprocess
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
MIRROR_SCRIPT = ROOT / "scripts" / "mirror_runtime_csv_to_json.py"
PRECHECK_SCRIPT = ROOT / "scripts" / "export_parity_precheck.sh"
COMPARE_METRICS = ROOT / "scripts" / "compare_run_metrics.py"
COMPARE_DIAGNOSTICS = ROOT / "scripts" / "compare_run_diagnostics.py"


class GoogleSheetIngestionExportE2ETests(unittest.TestCase):
    @staticmethod
    def _stage_summaries_payload(run_id: str) -> dict:
        latest_run_ids = ["run-a", "run-b"]
        return {
            "schema_id": "reason_alias_schema_v1",
            "stage_summaries": [
                {"stage": "stageFetchOdds", "duration_ms": 10},
                {"stage": "stageFetchSchedule", "duration_ms": 10},
                {"stage": "stageMatchEvents", "duration_ms": 10},
                {
                    "stage": "stageFetchPlayerStats",
                    "duration_ms": 10,
                    "reason_metadata": {
                        "requested_player_count": 6,
                        "resolved_player_count": 6,
                        "resolved_via_ta_count": 4,
                        "resolved_via_provider_fallback_count": 1,
                        "resolved_via_model_fallback_count": 1,
                        "unresolved_player_a_count": 0,
                        "unresolved_player_b_count": 0,
                        "fallback_reason_counts": {},
                    },
                },
                {"stage": "stageGenerateSignals", "duration_ms": 10},
                {"stage": "stagePersist", "duration_ms": 10},
            ],
            "compare_prerequisites": {
                "coverage": {"requested": 0, "resolved": 0, "unresolved": 0},
                "reason_code_placeholders": {"STATS_MISS_A": 0, "STATS_MISS_B": 0},
            },
            "gs_export_parity_contract": {
                "contract_name": "run_log_export_parity_contract_v1",
                "latest_run_ids": latest_run_ids,
                "summary_presence_by_run_id": {"run-a": True, "run-b": True},
                "required_stage_summary_presence_by_run_id": {
                    "run-a": {"stageFetchPlayerStats": True},
                    "run-b": {"stageFetchPlayerStats": True},
                },
                "parity_status": "pass",
                "reason_code": "export_parity_contract_pass",
                "pass": True,
            },
        }

    @staticmethod
    def _signal_summary_payload() -> dict:
        return {
            "stake_policy_summary": {
                "enabled": True,
                "minimum_stake_mxn": 20,
                "unit_size_mxn": 100,
                "min_bet_mxn": 20,
                "bucket_step_mxn": 20,
                "bucket_rounding": "down",
                "stake_mode_by_odds_sign": {"positive": "to_risk", "negative": "to_win"},
                "max_bet_mxn": 100,
            }
        }

    def _write_runtime_csv_batch(self, source: Path) -> None:
        (source / "Config.csv").write_text("key,value\nRUN_ENABLED,true\n", encoding="utf-8")
        (source / "Raw_Odds.csv").write_text("event_id,odds\nmatch-1,-120\n", encoding="utf-8")
        (source / "Raw_Schedule.csv").write_text(
            "event_id,start_time\nmatch-1,2026-03-26T00:00:00Z\n",
            encoding="utf-8",
        )
        (source / "Raw_Player_Stats.csv").write_text(
            "player,metric,value\nPlayer A,serve_pct,0.62\n",
            encoding="utf-8",
        )
        (source / "Match_Map.csv").write_text(
            "source_event_id,canonical_event_id\nmatch-1,match-1\n",
            encoding="utf-8",
        )
        (source / "Signals.csv").write_text(
            "run_id,event_id,signal\nrun-a,match-1,bet\n",
            encoding="utf-8",
        )
        (source / "ProviderHealth.csv").write_text(
            "provider,status\nodds_api,ok\n",
            encoding="utf-8",
        )

        run_log_headers = [
            "row_type",
            "run_id",
            "stage",
            "started_at",
            "stage_summaries",
            "reason_codes",
            "signal_decision_summary",
            "stake_mxn",
            "odds_american",
            "stake_mode_used",
            "final_risk_mxn",
            "stake_adjustment_reason_code",
        ]
        rows = [
            {
                "row_type": "stage",
                "run_id": "run-a",
                "stage": "stageFetchOdds",
                "started_at": "2026-03-26T00:00:00Z",
            },
            {
                "row_type": "stage",
                "run_id": "run-a",
                "stage": "stageFetchSchedule",
                "started_at": "2026-03-26T00:00:00Z",
            },
            {
                "row_type": "stage",
                "run_id": "run-a",
                "stage": "stageMatchEvents",
                "started_at": "2026-03-26T00:00:00Z",
            },
            {
                "row_type": "stage",
                "run_id": "run-a",
                "stage": "stageFetchPlayerStats",
                "started_at": "2026-03-26T00:00:00Z",
            },
            {
                "row_type": "stage",
                "run_id": "run-a",
                "stage": "stagePersist",
                "started_at": "2026-03-26T00:00:00Z",
            },
            {
                "row_type": "summary",
                "run_id": "run-a",
                "stage": "runEdgeBoard",
                "started_at": "2026-03-26T00:00:00Z",
                "stage_summaries": self._stage_summaries_payload("run-a"),
                "reason_codes": {"STATS_MISS_A": 0, "STATS_MISS_B": 0},
                "signal_decision_summary": self._signal_summary_payload(),
            },
            {
                "row_type": "stage",
                "run_id": "run-b",
                "stage": "stageFetchOdds",
                "started_at": "2026-03-26T00:00:00Z",
            },
            {
                "row_type": "stage",
                "run_id": "run-b",
                "stage": "stageFetchSchedule",
                "started_at": "2026-03-26T00:00:00Z",
            },
            {
                "row_type": "stage",
                "run_id": "run-b",
                "stage": "stageMatchEvents",
                "started_at": "2026-03-26T00:00:00Z",
            },
            {
                "row_type": "stage",
                "run_id": "run-b",
                "stage": "stageFetchPlayerStats",
                "started_at": "2026-03-26T00:00:00Z",
            },
            {
                "row_type": "stage",
                "run_id": "run-b",
                "stage": "stagePersist",
                "started_at": "2026-03-26T00:00:00Z",
            },
            {
                "row_type": "summary",
                "run_id": "run-b",
                "stage": "runEdgeBoard",
                "started_at": "2026-03-26T00:00:00Z",
                "stage_summaries": self._stage_summaries_payload("run-b"),
                "reason_codes": {"STATS_MISS_A": 0, "STATS_MISS_B": 0},
                "signal_decision_summary": self._signal_summary_payload(),
            },
            {
                "row_type": "signal",
                "run_id": "run-a",
                "stage": "stageGenerateSignals",
                "started_at": "2026-03-26T00:00:00Z",
                "stake_mxn": 15,
                "odds_american": 125,
                "stake_mode_used": "to_risk",
                "final_risk_mxn": 20,
                "stake_adjustment_reason_code": "stake_rounded_to_min",
            },
            {
                "row_type": "signal",
                "run_id": "run-a",
                "stage": "stageGenerateSignals",
                "started_at": "2026-03-26T00:00:00Z",
                "stake_mxn": 55,
                "odds_american": -140,
                "stake_mode_used": "to_win",
                "final_risk_mxn": 60,
                "stake_adjustment_reason_code": "stake_bucket_rounded_down",
            },
            {
                "row_type": "signal",
                "run_id": "run-a",
                "stage": "stageGenerateSignals",
                "started_at": "2026-03-26T00:00:00Z",
                "stake_mxn": 140,
                "odds_american": 180,
                "stake_mode_used": "to_risk",
                "final_risk_mxn": 100,
                "stake_adjustment_reason_code": "stake_capped_to_max_bet",
            },
            {
                "row_type": "signal",
                "run_id": "run-b",
                "stage": "stageGenerateSignals",
                "started_at": "2026-03-26T00:00:00Z",
                "stake_mxn": 18,
                "odds_american": 110,
                "stake_mode_used": "to_risk",
                "final_risk_mxn": 20,
                "stake_adjustment_reason_code": "stake_rounded_to_min",
            },
            {
                "row_type": "signal",
                "run_id": "run-b",
                "stage": "stageGenerateSignals",
                "started_at": "2026-03-26T00:00:00Z",
                "stake_mxn": 42,
                "odds_american": -120,
                "stake_mode_used": "to_win",
                "final_risk_mxn": 40,
                "stake_adjustment_reason_code": "stake_bucket_rounded_down",
            },
            {
                "row_type": "signal",
                "run_id": "run-b",
                "stage": "stageGenerateSignals",
                "started_at": "2026-03-26T00:00:00Z",
                "stake_mxn": 220,
                "odds_american": -200,
                "stake_mode_used": "to_win",
                "final_risk_mxn": 100,
                "stake_adjustment_reason_code": "stake_capped_to_max_bet",
            },
        ]

        with (source / "Run_Log.csv").open("w", encoding="utf-8", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=run_log_headers)
            writer.writeheader()
            for row in rows:
                normalized = {}
                for key in run_log_headers:
                    value = row.get(key, "")
                    normalized[key] = json.dumps(value) if isinstance(value, (dict, list)) else value
                writer.writerow(normalized)

        state_headers = ["run_id", "state_key", "state_value", "started_at"]
        state_rows = [
            {
                "run_id": "run-a",
                "state_key": "stake_policy_enabled",
                "state_value": "true",
                "started_at": "2026-03-26T00:00:00Z",
            },
            {
                "run_id": "run-b",
                "state_key": "stake_policy_enabled",
                "state_value": "true",
                "started_at": "2026-03-26T00:00:00Z",
            },
        ]
        with (source / "State.csv").open("w", encoding="utf-8", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=state_headers)
            writer.writeheader()
            writer.writerows(state_rows)

    def test_google_sheet_ingestion_export_workflow_end_to_end(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            source_dir = root / "source"
            export_dir = root / "exports"
            source_dir.mkdir(parents=True, exist_ok=True)
            self._write_runtime_csv_batch(source_dir)

            mirror = subprocess.run(
                ["python3", str(MIRROR_SCRIPT), "--input-dir", str(source_dir)],
                cwd=ROOT,
                capture_output=True,
                text=True,
                check=False,
            )
            self.assertEqual(0, mirror.returncode, msg=mirror.stderr)

            with (source_dir / "Run_Log.csv").open("r", encoding="utf-8", newline="") as handle:
                run_log_csv_rows = list(csv.DictReader(handle))
            run_log_json_rows = json.loads((source_dir / "Run_Log.json").read_text(encoding="utf-8"))
            state_json_rows = json.loads((source_dir / "State.json").read_text(encoding="utf-8"))

            # New telemetry columns are present in CSV and mirrored JSON.
            required_cols = {"stake_mode_used", "final_risk_mxn", "odds_american", "stake_mxn"}
            self.assertTrue(required_cols.issubset(set(run_log_csv_rows[0].keys())))
            self.assertTrue(required_cols.issubset(set(run_log_json_rows[0].keys())))
            self.assertGreater(len(state_json_rows), 0)

            # Row-level value parity between CSV and mirrored JSON for telemetry fields.
            csv_signal_rows = [r for r in run_log_csv_rows if r.get("row_type") == "signal"]
            json_signal_rows = [r for r in run_log_json_rows if str(r.get("row_type")) == "signal"]
            self.assertEqual(len(csv_signal_rows), len(json_signal_rows))
            csv_keyed = {
                (r["run_id"], r["started_at"], r["odds_american"]): r
                for r in csv_signal_rows
            }
            for jrow in json_signal_rows:
                key = (str(jrow.get("run_id")), str(jrow.get("started_at")), str(jrow.get("odds_american")))
                self.assertIn(key, csv_keyed)
                crow = csv_keyed[key]
                for field in ("stake_mode_used", "final_risk_mxn", "stake_mxn", "odds_american"):
                    self.assertEqual(str(crow.get(field, "")), str(jrow.get(field, "")))

            # stake_mode_used aligns with odds sign + final_risk_mxn >= 20 and in 20-MXN increments.
            for row in csv_signal_rows:
                odds = float(row["odds_american"])
                mode = row["stake_mode_used"]
                self.assertEqual("to_win" if odds < 0 else "to_risk", mode)
                final_risk = float(row["final_risk_mxn"])
                self.assertGreaterEqual(final_risk, 20.0)
                self.assertAlmostEqual(0.0, final_risk % 20.0)

            precheck = subprocess.run(
                ["bash", str(PRECHECK_SCRIPT), "--out-dir", str(export_dir), "run-a", "run-b", str(source_dir)],
                cwd=ROOT,
                capture_output=True,
                text=True,
                check=False,
            )
            self.assertEqual(0, precheck.returncode, msg=precheck.stderr)

            metrics = subprocess.run(
                [
                    "python3",
                    str(COMPARE_METRICS),
                    "run-a",
                    "run-b",
                    "--input",
                    str(export_dir / "Run_Log.csv"),
                    "--skip-player-stats-coverage-gate",
                    "--stake-policy-enabled",
                ],
                cwd=ROOT,
                capture_output=True,
                text=True,
                check=False,
            )
            self.assertEqual(0, metrics.returncode, msg=metrics.stderr)
            self.assertIn("[stake_policy_stake_mode_used_counts]", metrics.stdout)
            self.assertIn("[stake_policy_final_risk_mxn_aggregates]", metrics.stdout)

            diagnostics = subprocess.run(
                [
                    "python3",
                    str(COMPARE_DIAGNOSTICS),
                    "run-a",
                    "run-b",
                    "--export-dir",
                    str(export_dir),
                    "--skip-player-stats-coverage-gate",
                    "--stake-policy-enabled",
                ],
                cwd=ROOT,
                capture_output=True,
                text=True,
                check=False,
            )
            self.assertEqual(0, diagnostics.returncode, msg=diagnostics.stderr)
            self.assertIn("## stake-policy stake_mode_used counts", diagnostics.stdout)
            self.assertIn("## stake-policy final_risk_mxn aggregates", diagnostics.stdout)

    def test_precheck_auto_repairs_stale_source_json_tabs_before_staging_copy(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            source_dir = root / "source"
            export_dir = root / "exports"
            source_dir.mkdir(parents=True, exist_ok=True)
            self._write_runtime_csv_batch(source_dir)

            # Seed stale JSON tabs that do not match CSV row counts.
            (source_dir / "Run_Log.json").write_text("[]\n", encoding="utf-8")
            (source_dir / "State.json").write_text("[]\n", encoding="utf-8")

            precheck = subprocess.run(
                ["bash", str(PRECHECK_SCRIPT), "--out-dir", str(export_dir), "run-a", "run-b", str(source_dir)],
                cwd=ROOT,
                capture_output=True,
                text=True,
                check=False,
            )
            self.assertEqual(0, precheck.returncode, msg=precheck.stderr)

            with (source_dir / "Run_Log.csv").open("r", encoding="utf-8", newline="") as handle:
                source_run_log_csv_rows = list(csv.DictReader(handle))
            with (source_dir / "State.csv").open("r", encoding="utf-8", newline="") as handle:
                source_state_csv_rows = list(csv.DictReader(handle))
            source_run_log_json_rows = json.loads((source_dir / "Run_Log.json").read_text(encoding="utf-8"))
            source_state_json_rows = json.loads((source_dir / "State.json").read_text(encoding="utf-8"))

            # Auto-mirror in source should repair stale JSON before staging/export.
            self.assertEqual(len(source_run_log_csv_rows), len(source_run_log_json_rows))
            self.assertEqual(len(source_state_csv_rows), len(source_state_json_rows))

            exported_run_log_json_rows = json.loads((export_dir / "Run_Log.json").read_text(encoding="utf-8"))
            exported_state_json_rows = json.loads((export_dir / "State.json").read_text(encoding="utf-8"))
            self.assertEqual(len(source_run_log_json_rows), len(exported_run_log_json_rows))
            self.assertEqual(len(source_state_json_rows), len(exported_state_json_rows))


if __name__ == "__main__":
    unittest.main()
