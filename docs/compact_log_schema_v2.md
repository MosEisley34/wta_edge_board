# Compact log schema spec (`schema_version: 2`)

## Scope

This document defines the compact-record schema for run-log events currently emitted by the pipeline:

- `run_start_config_audit`
- `stageFetchOdds`
- `stageFetchSchedule`
- `stageMatchEvents`
- `stageFetchPlayerStats`
- `stageGenerateSignals`
- `stagePersist`
- watchdog (`bootstrap_empty_cycle_watchdog`, `productive_output_watchdog`, `schedule_only_watchdog`, `run_lifecycle` with `reason_code=watchdog_recovered`)
- `summary` (`row_type=summary`, `stage=runEdgeBoard`)

## 1) Profiles and omission rules

### 1.1 Log profiles

- `core`: minimum fields to reconstruct run progression and outcomes.
- `standard`: `core` + counters needed for operations dashboards.
- `diagnostic`: `standard` + payloads used for deep incident triage.

### 1.2 Omission rules

1. `schema_version` is always present and always `2`.
2. Fields not listed for the current event/profile are omitted (not serialized as `null`).
3. Numeric counters with value `0` are omitted unless the field is explicitly required for that event type.
4. Empty strings (`""`), empty maps (`{}`), and empty arrays (`[]`) are omitted.
5. `reason_codes` entries with value `0` are omitted. If all reason-code entries are omitted, the `rc` field is omitted.
6. If an omitted counter is read by consumers, it must be interpreted as `0`.
7. If an omitted string field is read by consumers, it must be interpreted as `""`.

## 2) Fixed key order (all compact records)

Serialization must follow this exact key order. Keys not emitted are skipped without reordering.

1. `schema_version`
2. `et`
3. `rid`
4. `st`
5. `sa`
6. `ea`
7. `ss`
8. `rcd`
9. `ic`
10. `oc`
11. `pr`
12. `acu`
13. `rc`
14. `rm`
15. `fo`
16. `fs`
17. `at`
18. `mt`
19. `um`
20. `sg`
21. `rj`
22. `cds`
23. `dds`
24. `lk`
25. `db`
26. `tr`
27. `msg`
28. `ex`
29. `stk`
30. `ssu`

## 3) Field dictionary (canonical mapping)

| Compact key | Source field(s) | Type | Notes |
|---|---|---|---|
| `schema_version` | constant | number | Must be `2`. |
| `et` | derived event type | string | See §4 mapping table. |
| `rid` | `run_id` | string | Required for all event types. |
| `st` | `stage` | string | Current stage name. |
| `sa` | `started_at` | string (ISO) | Stage/run start timestamp. |
| `ea` | `ended_at` | string (ISO) | Stage/run end timestamp. |
| `ss` | `status` | string | `success`, `warning`, `notice`, `skipped`, `failed`, etc. |
| `rcd` | `reason_code` | string | Primary reason code. |
| `ic` | `message.input_count` | number | Stage input count. |
| `oc` | `message.output_count` | number | Stage output count. |
| `pr` | `message.provider` | string | Stage provider id. |
| `acu` | `message.api_credit_usage` (if present), else `0` | number | API credit usage for stage. |
| `rc` | `message.reason_codes` | object<string,number> | Zero-valued entries omitted per §1.2. |
| `rm` | `reason_metadata` (from stage summary payload if available) | object | Diagnostic-only. |
| `fo` | `fetched_odds` | number | Summary-only counter. |
| `fs` | `fetched_schedule` | number | Summary-only counter. |
| `at` | `allowed_tournaments` | number | Summary-only counter. |
| `mt` | `matched` | number | Summary-only counter. |
| `um` | `unmatched` | number | Summary-only counter. |
| `sg` | `signals_found` | number | Summary-only counter. |
| `rj` | `rejection_codes` | object<string,number> | Zero-valued entries omitted per §1.2. |
| `cds` | `cooldown_suppressed` | number | Summary-only counter. |
| `dds` | `duplicate_suppressed` | number | Summary/skip counters. |
| `lk` | `lock_event` | string | Optional lock lifecycle marker. |
| `db` | `debounce_event` | string | Optional debounce lifecycle marker. |
| `tr` | `trigger_event` | string | Optional trigger lifecycle marker. |
| `msg` | `message` | string/object | Keep raw string if not JSON; parsed object if JSON object. |
| `ex` | `exception` | string | Failure-only. |
| `stk` | `stack` | string | Failure-only. |
| `ssu` | `stage_summaries` | array<object> | Summary diagnostic payload. |

## 4) Exact event-type mappings

### 4.1 Event type id mapping

| Current event | Compact `et` |
|---|---|
| `stage=run_start_config_audit` | `run_start_config_audit` |
| `stage=stageFetchOdds` | `stageFetchOdds` |
| `stage=stageFetchSchedule` | `stageFetchSchedule` |
| `stage=stageMatchEvents` | `stageMatchEvents` |
| `stage=stageFetchPlayerStats` | `stageFetchPlayerStats` |
| `stage=stageGenerateSignals` | `stageGenerateSignals` |
| `stage=stagePersist` | `stagePersist` |
| `stage in {bootstrap_empty_cycle_watchdog, productive_output_watchdog, schedule_only_watchdog}` OR (`stage=run_lifecycle` and `rcd=watchdog_recovered`) | `watchdog` |
| `row_type=summary and stage=runEdgeBoard` | `summary` |

### 4.2 Required fields by event type

| `et` | Required fields |
|---|---|
| `run_start_config_audit` | `schema_version`, `et`, `rid`, `st`, `ss`, `rcd` |
| `stageFetchOdds` | `schema_version`, `et`, `rid`, `st`, `sa`, `ea`, `ss`, `rcd`, `ic`, `oc`, `pr` |
| `stageFetchSchedule` | `schema_version`, `et`, `rid`, `st`, `sa`, `ea`, `ss`, `rcd`, `ic`, `oc`, `pr` |
| `stageMatchEvents` | `schema_version`, `et`, `rid`, `st`, `sa`, `ea`, `ss`, `rcd`, `ic`, `oc`, `pr` |
| `stageFetchPlayerStats` | `schema_version`, `et`, `rid`, `st`, `sa`, `ea`, `ss`, `rcd`, `ic`, `oc`, `pr` |
| `stageGenerateSignals` | `schema_version`, `et`, `rid`, `st`, `sa`, `ea`, `ss`, `rcd`, `ic`, `oc`, `pr` |
| `stagePersist` | `schema_version`, `et`, `rid`, `st`, `sa`, `ea`, `ss`, `rcd`, `ic`, `oc`, `pr` |
| `watchdog` | `schema_version`, `et`, `rid`, `st`, `ss`, `rcd` |
| `summary` | `schema_version`, `et`, `rid`, `st`, `sa`, `ea`, `ss`, `rcd` |

### 4.3 Optional fields gated by profile

| `et` | `core` | `standard` | `diagnostic` |
|---|---|---|---|
| `run_start_config_audit` | `msg` | — | — |
| Any stage event (`stageFetchOdds`…`stagePersist`) | `rc` | `acu` | `rm`, `msg` |
| `watchdog` | — | — | `msg` |
| `summary` | `fo`, `fs`, `mt`, `um`, `sg`, `dds` | `at`, `cds`, `lk`, `db`, `tr`, `rj` | `msg`, `ssu`, `ex`, `stk` |

## 5) Event-specific message object expectations

When `msg` is JSON, the expected object shape is:

- `run_start_config_audit`: `{ model_mode, disable_sofascore, require_opening_line_proximity, max_opening_lag_minutes }`
- `stageFetchOdds`…`stagePersist`: `{ input_count, output_count, provider, reason_codes }`
- `watchdog`:
  - `bootstrap_empty_cycle_watchdog`: `{ consecutive_empty_cycles, threshold, diagnostics_counter, last_non_empty_fetch_at, last_non_empty_fetch_at_utc }`
  - `productive_output_watchdog`: `{ reason_code, streak_count, threshold, fetched_odds, signals_found, run_id }`
  - `schedule_only_watchdog`: `{ reason_code, streak_count, threshold, fetched_schedule, fetched_odds, notice_severity, expected_idle, odds_window_context, message, run_id }`
  - `run_lifecycle` watchdog recovery: `{ status, run_key, run_key_hash, recovered_from_run_id }` (subset allowed)
- `summary`: freeform status message string (or failure details in exception flow)

## 6) Backward compatibility contract

1. Existing full-width run-log rows remain source-of-truth for historical records.
2. Compact schema v2 is a projection layer; it does not rename or mutate source rows.
3. Consumers must support mixed-mode history where old records have no `schema_version` and compact records carry `schema_version: 2`.

## 7) Reason-code alias additions (fallback diagnostics)

The `reason_code_alias_v1` dictionary now explicitly aliases these frequently observed fallback diagnostics reason codes so compact logs avoid `UNK_*` fallback aliases:

- `missing_open_timestamp_fallback` → `OPEN_TS_FB`
- `opening_lag_fallback_exemption_denied_source` → `OPEN_FB_DENY_SRC`
- `opening_lag_fallback_exemption_denied_age` → `OPEN_FB_DENY_AGE`
- `opening_lag_fallback_exemption_denied_cap` → `OPEN_FB_DENY_CAP`
- `odds_api_success_sport_key_fallback` → `ODDS_API_SK_FB`
- `schedule_api_success_sport_key_fallback` → `SCH_API_SK_FB`
- `run_health_expected_temporary_no_odds` → `RH_TMP_NO_ODDS`
- `ta_matchmx_stale_fallback` → `TA_MX_STALE_FB`
- `state_stale_payload_write_failed_non_fatal` → `STALE_WRITE_WARN`
- `reason_code_alias_missing_fallback_emitted` → `RC_ALIAS_FALLBACK`

These aliases are deterministic and unique, preserving existing alias keys for backward compatibility with `invertReasonCodeAliasDictionary_` collision checks.
