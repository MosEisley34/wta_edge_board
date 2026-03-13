# Active-window readability QA (2026-03-13)

## Scope
Validated that readability-oriented reason-code changes did **not** hide active-window faults by checking: (1) consecutive active-window runs, (2) lifecycle + actionable progression, (3) watchdog/latency escalation contracts, and (4) before/after reason readability in summary and rollup outputs.

## 1) Active-window run sample (consecutive)
Sample source: `pipeline_log_sample_3h_verbose_2026-03-12_fix.json` (36 consecutive simulated runs).

| Run ID | Summary reason | fetched_odds | matched | signals_found | rejected | Status |
|---|---|---:|---:|---:|---:|---|
| sim-1772366400000 | run_success | 1 | 1 | 1 | 0 | success |
| sim-1772366700000 | run_success | 1 | 1 | 1 | 0 | success |
| sim-1772367000000 | run_success | 1 | 1 | 1 | 0 | success |
| sim-1772367300000 | run_success | 1 | 1 | 1 | 0 | success |
| sim-1772367600000 | run_success | 1 | 1 | 1 | 0 | success |
| sim-1772367900000 | run_success | 1 | 1 | 1 | 0 | success |

Operator read: stable active-window behavior in this sample; no hidden degraded outcome in summary rows.

## 2) Lifecycle completion + actionable progression
- Every sampled run includes the full stage chain ending in `stagePersist` and a final `runEdgeBoard` summary row.
- Progression remains actionable: each sampled run fetched odds and produced a match + signal (`fetched_odds=1`, `matched=1`, `signals_found=1`).
- Run-summary quality stayed explicit (`reason_code=run_success`, `status=success`) with no masking fallback reason observed in this active sample.

## 3) Watchdog + latency escalation behavior (active mode)
From coverage contracts in `08_odds_sport_key_resolver_tests.gs`:
- Active degraded latency is expected to escalate as `warning`, mode `degraded`, evaluation mode `active`, with anomaly reason codes for avg/p95 threshold exceedance.
- Outside-window/idle contexts are explicitly downgraded to `informational` and do **not** emit those active degraded anomaly reason codes.
- This preserves operator signal quality: active-window latency breach remains actionable while idle-window latency does not generate false active alarms.

## 4) Before/after readability check (summary + rollup payloads)
### Summary/readability parity gate evidence
- **Before** (`deterministic_compact_before_report.json`): failed with `critical_parity_failure` and `stage_counter_invariant_failure` due gate-reason mismatch.
- **After** (`deterministic_compact_after_report.json`): parity and invariants pass cleanly; no failed critical parity keys.

### Rollup/readability normalization evidence
- Rollup contracts assert display normalization for legacy aliases (`reason_code_display_normalization.normalization_applied=true`) while retaining raw reason values (`top_reason_codes_raw`) and canonical display alias (`top_reason_codes`).
- This improves readability without losing raw diagnostic provenance.

## 5) Operator-facing QA conclusion
- **Stable:** Active-window sample shows consistent lifecycle completion and actionable odds→match→signal progression.
- **Degraded:** No degraded active-window sample was present in the 3h replay artifacts; degraded escalation behavior is validated via existing test contracts.
- **Actionable signals found:** Yes — readability improvements keep reason codes operator-friendly while preserving parity/invariant checks that catch true active-window issues.
