# Player Stats Incident Summary (2026-03-18)

## 1) Runtime coverage numbers (from state)

State-derived incident values from the latest player-stats investigation show:

- `stats_enriched=0`
- `stats_missing_player_a=16`
- `stats_missing_player_b=16`
- `missing_stats=16`
- `signals_found=0`
- `PLAYER_STATS_LAST_FETCH_META`: `provider_available=true`, `data_available=false`, `aggregate_reason_code=provider_returned_null_features`

Operationally this is **0/16 model candidates with usable stats**, i.e. effective runtime coverage ratio `0.00`.

## 2) TA parity numbers (from probe/parity)

From `python3 scripts/check_ta_parity.py --input scripts/fixtures/tennisabstract_leadersource_wta.body`:

- `total_rows=8`
- `unique_players=8`
- Row-level non-null coverage:
  - `ranking: 8/8 = 1.00`
  - `hold_pct: 8/8 = 1.00`
  - `break_pct: 8/8 = 1.00`
- Normalized non-null coverage:
  - `ranking: 8/8 = 1.00`
  - `hold_pct: 8/8 = 1.00`
  - `break_pct: 8/8 = 1.00`
- Parser quality signals:
  - `matchmx_unusable_rows=0`
  - `matchmx_expected_non_model_rows=0`
  - `matchmx_unexpected_parse_failure_rows=0`

## 3) Root-cause classification

**Classification: coverage mismatch (not a parser failure in parity fixtures).**

Why: parity probe shows parser+normalization are healthy on known-good TA leadersource payloads, while runtime state indicates all downstream feature slots are null (`stats_enriched=0`, `missing_stats=16`). This points to acquisition/input freshness or source-shape drift at runtime (or empty/partial upstream payload), not the currently tested parser contract.

## 4) Recommended implementation direction

**Recommend: TA-only with expanded acquisition (primary), plus explicit fallback activation criteria.**

Rationale:
- TA parity is currently strong on fixture probes (1.00 coverage for ranking/hold/break), so keeping TA as primary preserves model feature consistency.
- The immediate gap is acquisition robustness (leadersource discovery/fetch variant handling, payload guards, and freshness checks), not baseline parsing logic.
- Keep multi-source fallback as a controlled degraded-mode path rather than primary architecture, to avoid introducing new normalization entropy before TA ingestion is hardened.

## 5) Degraded-mode trigger thresholds

Set and enforce these minimums:

- **Minimum acceptable runtime coverage ratio:** `0.60` (usable stats / candidate players), aligned with TA parity gate defaults.
- Enter degraded mode when either condition is met:
  1. Coverage ratio `< 0.60` for **2 consecutive runs**, or
  2. Coverage ratio `< 0.40` for **any single run**.
- Exit degraded mode only after coverage ratio `>= 0.60` for **2 consecutive runs**.

Suggested degraded behavior:
- Annotate run health reason with `stats_zero_coverage_count`/coverage-specific reason code.
- Continue pipeline with reduced confidence and optional fallback source hydration if enabled.
