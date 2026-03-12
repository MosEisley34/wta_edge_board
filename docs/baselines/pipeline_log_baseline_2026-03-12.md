# Pipeline log baseline (edge board runtime)

## Logging entrypoints (stage/ops/summary)

Primary run-log writes are emitted through `appendLogRow_` in shared logging utils, and stage summary records are wrapped by `appendStageLog_` before being routed to `appendLogRow_`. `buildStageSummary_` constructs summary payloads (including `reason_codes`) used by stage rows and by the final run summary. In orchestration, `runEdgeBoard()` emits `ops` and `summary` rows at run lifecycle checkpoints and appends a final `summary` row with `stage_summaries` and aggregated rejection data.

## Sample capture

* Window: **3 hours** (36 runs at 5-minute cadence)
* Capture mode: deterministic local runtime simulation via `scripts/capture_pipeline_log_sample.js`
* Output rows: **432**
* Raw sample artifact: `docs/baselines/pipeline_log_sample_3h.json`

## Size baseline

* **Total bytes:** 284,472

### Bytes by event type

| Event type | Bytes | Share |
|---|---:|---:|
| `summary` | 132,840 | 46.7% |
| `stage` | 90,360 | 31.8% |
| `ops` | 61,272 | 21.5% |

### Bytes by field family

| Family | Bytes | Share |
|---|---:|---:|
| metadata | 214,308 | 75.3% |
| reason_codes | 54,000 | 19.0% |
| timestamps | 31,536 | 11.1% |
| repeated static fields | 25,380 | 8.9% |

> Families are not mutually exclusive in nested payload analysis (e.g., `message.reason_codes` contributes to both `message` and reason-code family totals).

## Top 10 largest fields and reduction targets

| Rank | Field | Bytes | Share | Reduction target |
|---:|---|---:|---:|---|
| 1 | `stage_summaries` | 94,932 | 33.4% | **-60%** by storing only stage ids/status in run-log and full summaries in state sheet |
| 2 | `message` | 75,312 | 26.5% | **-35%** by trimming repeated diagnostic blocks and keeping compact message schema |
| 3 | `rejection_codes` | 22,032 | 7.7% | **-50%** by top-N compression + overflow bucket |
| 4 | `message.reason_codes` | 20,628 | 7.3% | **-40%** by aliasing long keys and omitting zeros |
| 5 | `stage` | 9,072 | 3.2% | **-20%** via short stage code map |
| 6 | `started_at` | 8,784 | 3.1% | **-30%** using epoch millis |
| 7 | `run_id` | 8,208 | 2.9% | **-15%** by shorter run-id format |
| 8 | `ended_at` | 7,848 | 2.8% | **-30%** using epoch millis |
| 9 | `reason_code` | 7,812 | 2.7% | **-25%** by short aliases for frequent reasons |
| 10 | `stage_summaries.[0].reason_codes` | 5,688 | 2.0% | **-50%** with stage-local dedupe and reason-key aliases |

## Repetition rates of long reason-code keys

| Reason-code key | Rows containing key | Row repetition rate |
|---|---:|---:|
| `long_reason_code_key_for_schedule_enrichment_h2h_missing` | 72 | 16.67% |
| `provider_returned_null_features` | 72 | 16.67% |
| `very_long_reason_code_key_player_stats_incomplete_profile` | 72 | 16.67% |
| `signal_edge_above_threshold` | 72 | 16.67% |
| `raw_player_stats_upserts` | 72 | 16.67% |

## Expected first-pass impact

Applying only the top three targets (`stage_summaries`, `message`, `rejection_codes`) projects a reduction of roughly **95–105 KB** over the 3-hour sample (~**33–37%** total byte reduction), before deeper reason-code/key aliasing.
