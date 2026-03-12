# Deterministic compact reason-code aggregation comparison

- Sample verbose: `deterministic_compact_verbose_sample.json`
- Sample compact (before fix): `deterministic_compact_before_sample.json`
- Sample compact (after fix): `deterministic_compact_after_sample.json`

| Metric | Before | After |
|---|---:|---:|
| stage_counter_invariants_passed | False | True |
| compact violation count | 1 | 0 |
| compact checked run count | 1 | 1 |
| compact violation ratio (violations/runs) | 1.00 | 0.00 |
| quality_gate_failed_reasons | `critical_parity_failure, stage_counter_invariant_failure` | `` |
