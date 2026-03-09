# Player stats data source research (WTA edge board)

## 1) What the latest logs say (root-cause analysis)

### Pipeline health summary
- Core pipeline is healthy: odds, schedule, matching, and persistence all complete successfully.
- The bottleneck is **player features availability**: all recent runs show `stats_enriched=0`, `stats_missing_player_a=16`, `stats_missing_player_b=16`, and `missing_stats=16`, yielding `signals_found=0`.
- Productive-output watchdog is firing repeatedly (`consecutive_count` rising to 6), which confirms this is not a one-off run glitch.

### Evidence chain
- Odds/schedule/match are not the issue: 16 odds rows, 8 schedule rows, and 16/16 matched events each run.
- `PLAYER_STATS_LAST_FETCH_META` reports `provider_available=true` but `data_available=false` and `aggregate_reason_code=provider_returned_null_features`.
- `PLAYER_STATS_STALE_PAYLOAD` stores all 16 players with `null` stats.
- H2H cache fetched source date `2026-01-01` with `row_count=0`; diagnostics show `ta_h2h_empty_table`.
- `PLAYER_STATS_TA_LEADERS_STALE_PAYLOAD` was summarized due size guard and indicates `player_count=0` / `stats_count=0`, strongly suggesting upstream format/parse mismatch or payload extraction failure in TA leaders ingestion.

### Immediate conclusion
The current production blocker is not matching or model thresholds; it is **upstream player-feature extraction returning null/empty from Tennis Abstract sources** (leaders + H2H), which prevents scoring.

---

## 2) Best scrapeable tennis stats sources for model features

Below is a prioritized source stack balancing data quality, scrape reliability, and maintenance burden.

## Tier A (primary)

### A1) Tennis Abstract leaders JS payload (current integration target)
- Why: rich per-player serve/return performance stats; historically easiest to normalize into hold/break and form-type features.
- Current method in repo:
  1. GET leaders HTML page (`leaders_wta.cgi?players=top`).
  2. Extract JS URL containing `jsmatches/...leadersource...wta.js`.
  3. GET JS payload.
  4. Parse `matchmx[...]` records.
- Required hardening:
  - support alternative JS variable names and row schema variants,
  - structural fingerprinting + parser-version selection,
  - fail open into partial feature extraction (don’t return all-null map on single-field parse break).

### A2) Tennis Abstract H2H matrix page
- Why: useful as optional H2H prior features.
- Current method in repo:
  1. GET `reports/h2hMatrixWta.html`.
  2. Parse matrix table and/or anchor fallback selectors.
- Required hardening:
  - accept empty table as non-fatal (already done),
  - keep `h2h_source` optional so model can still score with core features.

## Tier B (high-value backups)

### B1) Official WTA player pages / stats pages (if available for structured extraction)
- Why: authoritative player identity and ranking metadata.
- Use: ranking and profile normalization fallback; less ideal for deep match-derived serve/return features unless specific structured endpoints are discoverable.

### B2) Jeff Sackmann tennis datasets (GitHub)
- Why: robust historical match results, open data-friendly workflows.
- Use: derive rolling form and Elo-like features offline, then materialize per-player feature snapshots for runtime lookup.

### B3) Ultimate Tennis Statistics (UTS)
- Why: broad stat depth; good for validation and enrichment.
- Caveat: inspect robots/ToS and anti-bot behavior carefully before production scraping.

## Tier C (optional enrichments)
- Flashscore/Sofascore/TennisExplorer-style score feeds for near-real-time form context.
- Use only if legal/compliance and anti-bot constraints are acceptable.

---

## 3) Exact CLI scraping methods to implement

## Method 1 (preferred): TA leaders two-step fetch

```bash
# 1) fetch leaders page
curl -sS 'https://www.tennisabstract.com/cgi-bin/leaders_wta.cgi?players=top' \
  -H 'User-Agent: Mozilla/5.0 (compatible; WTA-Edge-Board/1.0)' \
  -o /tmp/ta_leaders.html

# 2) extract JS source
python - <<'PY'
import re, pathlib
html = pathlib.Path('/tmp/ta_leaders.html').read_text(errors='ignore')
m = re.search(r'(?:https?:)?//[^"\'\s]*jsmatches/[^"\'\s]*leadersource[^"\'\s]*wta\.js|/?jsmatches/[^"\'\s]*leadersource[^"\'\s]*wta\.js', html, re.I)
print(m.group(0) if m else '')
PY

# 3) fetch JS payload
curl -sS 'https://www.tennisabstract.com/jsmatches/leadersource_wta.js' \
  -H 'User-Agent: Mozilla/5.0 (compatible; WTA-Edge-Board/1.0)' \
  -o /tmp/ta_leaders.js

# 4) parse matchmx rows -> ndjson
python - <<'PY'
import re, json, pathlib
js = pathlib.Path('/tmp/ta_leaders.js').read_text(errors='ignore')
for m in re.finditer(r'matchmx\s*(?:\[\s*\d+\s*\])?\s*=\s*\[([\s\S]*?)\]\s*;', js):
    body = m.group(1)
    toks = re.findall(r'"((?:\\.|[^"\\])*)"|\'((?:\\.|[^\'\\])*)\'|([^,]+)', body)
    vals = []
    for a,b,c in toks:
      v = a or b or c.strip()
      vals.append(v)
    if len(vals) >= 19:
      print(json.dumps({"player": vals[3], "opponent": vals[4], "hold_pct": vals[9], "break_pct": vals[10], "surface": vals[2]}))
PY
```

Implementation note: keep this parser logic aligned with `extractMatchMxRows_` and add schema-version fallback patterns.

## Method 2: TA H2H matrix extraction

```bash
curl -sS 'https://tennisabstract.com/reports/h2hMatrixWta.html' \
  -H 'User-Agent: Mozilla/5.0 (compatible; WTA-Edge-Board/1.0)' \
  -o /tmp/ta_h2h.html

python - <<'PY'
import re, pathlib
html = pathlib.Path('/tmp/ta_h2h.html').read_text(errors='ignore')
anchors = re.findall(r'/cgi-bin/(?:player-classic|h2h)\.cgi\?[^"\']+', html)
print('anchor_count=', len(anchors))
print('sample=', anchors[:5])
PY
```

If matrix table is empty, persist `h2h_source_empty_table` and continue scoring with non-H2H features.

## Method 3: offline fallback from historical datasets

```bash
# Example workflow outline (dataset provider URL depends on chosen source)
# 1) download CSV archives
# 2) compute rolling features by player
# 3) publish small runtime-ready JSON map keyed by canonical player name
```

This method avoids fragile live scraping during runtime and can stabilize feature availability.

---

## 4) Why current run produced zero signals

- The signal builder dropped all 16 candidates for `missing_stats`.
- Odds freshness and matching are fine.
- Therefore, signal recovery requires **restoring non-null player features first** (even minimal ranking/form/hold-break subset).

---

## 5) Recommended rollout plan

1. **Patch TA leaders parser robustness** (highest priority).
2. Add **feature-completeness guardrail**: if parser returns all-null for >N players, mark as parser-failure reason code (distinct from true no-data).
3. Add **secondary source fallback** (official rankings + offline rolling form table).
4. Keep H2H optional; don’t block scoring on empty H2H matrix.
5. Add nightly CLI smoke probe (outside Apps Script) to detect upstream format drift early.

---

## 6) CLI probe result in this execution environment

Outbound HTTPS is blocked in this environment (`CONNECT tunnel failed, response 403`), so live endpoint verification could not be executed here. The methods above are exact command recipes intended for a network-enabled runner.
