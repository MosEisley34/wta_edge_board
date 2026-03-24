# Daily Edge Quality SLO Summary — Day 2

- **As-of UTC:** `2026-03-12T23:59:59+00:00`
- **Window:** `7d`
- **Pair count:** `0`

## Status distribution

| status | count |
|---|---:|
| pass | 0 |
| fail | 0 |
| insufficient_sample | 0 |

## Release gate enforcement

| Gate | Threshold | Observed | Result |
|---|---|---|---|
| Fail-rate on decisionable pairs | `<= 15%` | `n/a (0/0)` | FAIL |
| Insufficient-sample rate | `<= 25%` | `n/a (0/0)` | FAIL |
| Parity pass rate | `100%` | `0%` | FAIL |

**Overall release gate:** `FAIL`

## Notes

- Parity evaluation source: `python3 scripts/verify_run_log_parity.py --export-dir runtime_exports/exports`.
- Current parity check outcome:

```text
Run log parity check failed: expected both Run_Log JSON and CSV artifacts in the export batch. Found json=1 csv=0 under runtime_exports/exports.
Remediation: re-export source batch.
```

## Weekly threshold tuning review

- Recompute trailing 7-day median pair volume and edge-volatility p95 each Monday.
- If median pair volume remains below minimum decisionable target for 2 consecutive weeks, reduce minimum-pairs floor in controlled increments and document rationale.
- If edge-volatility p95 rises with stable volume, tighten the volatility ceiling before adjusting fail-rate thresholds.
