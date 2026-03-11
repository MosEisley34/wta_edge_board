#!/usr/bin/env bash
set -euo pipefail

UA='Mozilla/5.0 (compatible; WTA-Edge-Board/1.0)'
OUT_DIR="${1:-./tmp/source_probes}"
CATALOG_PATH="${2:-./config/probe_sources.tsv}"
PROVIDER_ALLOWLIST="${PROVIDER_ALLOWLIST:-tennisabstract_*,ta_*,mcp_report_*,sofascore_*}"
SOFASCORE_PROBE_TENNIS_PLAYER_ID="${SOFASCORE_PROBE_TENNIS_PLAYER_ID:-}"
DEFAULT_EXCLUDED_PROVIDERS="${DEFAULT_EXCLUDED_PROVIDERS:-wta_stats_zone,tennisexplorer,itf}"
INCLUDE_DEFAULT_EXCLUDED_PROVIDERS="${INCLUDE_DEFAULT_EXCLUDED_PROVIDERS:-false}"

RAW_DIR="$OUT_DIR/raw"
LOGS_DIR="$OUT_DIR/logs"
META_DIR="$OUT_DIR/meta"
mkdir -p "$RAW_DIR" "$LOGS_DIR" "$META_DIR"

SUMMARY_TSV="$META_DIR/.summary.tsv"
: > "$SUMMARY_TSV"
SKIPPED_ALLOWLIST_TSV="$META_DIR/.skipped_allowlist.tsv"
: > "$SKIPPED_ALLOWLIST_TSV"

trim() {
  local value="$1"
  value="${value#"${value%%[![:space:]]*}"}"
  value="${value%"${value##*[![:space:]]}"}"
  printf '%s' "$value"
}

is_provider_default_excluded() {
  local source_key="$1"
  local pattern=""

  [[ "${INCLUDE_DEFAULT_EXCLUDED_PROVIDERS,,}" == "true" ]] && return 1

  IFS=',' read -r -a patterns <<< "$DEFAULT_EXCLUDED_PROVIDERS"
  for pattern in "${patterns[@]}"; do
    pattern="$(trim "$pattern")"
    [[ -z "$pattern" ]] && continue
    if [[ "$source_key" == "$pattern" ]]; then
      return 0
    fi
  done
  return 1
}

is_provider_allowed() {
  local source_key="$1"
  local allowlist_raw="$PROVIDER_ALLOWLIST"
  local pattern=""

  if is_provider_default_excluded "$source_key"; then
    return 1
  fi

  IFS=',' read -r -a patterns <<< "$allowlist_raw"
  for pattern in "${patterns[@]}"; do
    pattern="$(trim "$pattern")"
    [[ -z "$pattern" ]] && continue
    if [[ "$source_key" == $pattern ]]; then
      return 0
    fi
  done
  return 1
}



validate_sofascore_tennis_player_id() {
  local candidate_id="$1"
  [[ -z "$candidate_id" || ! "$candidate_id" =~ ^[0-9]+$ ]] && return 1
  local detail_url="https://api.sofascore.com/api/v1/player/$candidate_id"
  local body
  body="$(curl -sS -L --max-time 20 --retry 1 --retry-delay 1 -H "User-Agent: $UA" "$detail_url" 2>/dev/null || true)"
  [[ -z "$body" ]] && return 1
  PLAYER_DETAIL_JSON="$body" python3 - <<'PYT'
import json, os, sys
raw = os.environ.get('PLAYER_DETAIL_JSON', '')
try:
    payload = json.loads(raw)
except Exception:
    sys.exit(1)
player = payload.get('player') if isinstance(payload, dict) else None
sport = player.get('sport') if isinstance(player, dict) else None
slug = str((sport or {}).get('slug', '')).strip().lower() if isinstance(sport, dict) else ''
sys.exit(0 if slug == 'tennis' else 1)
PYT
}

sample_sofascore_tennis_player_id_from_schedule() {
  local date_token
  date_token="$(date -u +%F)"
  local scheduled_url="https://api.sofascore.com/api/v1/sport/tennis/scheduled-events/$date_token"
  local body
  body="$(curl -sS -L --max-time 20 --retry 1 --retry-delay 1 -H "User-Agent: $UA" "$scheduled_url" 2>/dev/null || true)"
  [[ -z "$body" ]] && return 1
  SCHEDULED_JSON="$body" python3 - <<'PYT'
import json, os, sys
raw = os.environ.get('SCHEDULED_JSON', '')
try:
    payload = json.loads(raw)
except Exception:
    sys.exit(1)
if not isinstance(payload, dict):
    sys.exit(1)
for event in payload.get('events', []) or []:
    if not isinstance(event, dict):
        continue
    for side in ('homeTeam', 'awayTeam'):
        team = event.get(side)
        if isinstance(team, dict):
            team_id = team.get('id')
            if isinstance(team_id, int) and team_id > 0:
                print(team_id)
                sys.exit(0)
            if isinstance(team_id, str) and team_id.isdigit():
                print(team_id)
                sys.exit(0)
sys.exit(1)
PYT
}

resolve_sofascore_probe_tennis_player_id() {
  if [[ -n "${RESOLVED_SOFASCORE_PROBE_TENNIS_PLAYER_ID:-}" ]]; then
    printf '%s' "$RESOLVED_SOFASCORE_PROBE_TENNIS_PLAYER_ID"
    return 0
  fi
  local configured_id="$SOFASCORE_PROBE_TENNIS_PLAYER_ID"
  configured_id="$(trim "$configured_id")"

  local validation_outcome="invalid"
  local selected_source="none"
  local sampled_id=""
  sampled_id="$(sample_sofascore_tennis_player_id_from_schedule || true)"
  if [[ -n "$sampled_id" ]] && validate_sofascore_tennis_player_id "$sampled_id"; then
    RESOLVED_SOFASCORE_PROBE_TENNIS_PLAYER_ID="$sampled_id"
    validation_outcome="valid"
    selected_source="resolver"
    echo "INFO: Sofascore probe player selection id=$sampled_id source=$selected_source validation=$validation_outcome" >&2
    printf '%s' "$RESOLVED_SOFASCORE_PROBE_TENNIS_PLAYER_ID"
    return 0
  fi

  if [[ -n "$sampled_id" ]]; then
    RESOLVED_SOFASCORE_PROBE_TENNIS_PLAYER_ID_REASON="resolver_selected_player_failed_validation"
  else
    RESOLVED_SOFASCORE_PROBE_TENNIS_PLAYER_ID_REASON="resolver_no_tennis_player_id_from_live_or_scheduled_events"
  fi

  if [[ -n "$configured_id" ]] && validate_sofascore_tennis_player_id "$configured_id"; then
    RESOLVED_SOFASCORE_PROBE_TENNIS_PLAYER_ID="$configured_id"
    RESOLVED_SOFASCORE_PROBE_TENNIS_PLAYER_ID_REASON="configured_tennis_player_id"
    validation_outcome="valid"
    selected_source="configured_fallback"
    echo "INFO: Sofascore probe player selection id=$configured_id source=$selected_source validation=$validation_outcome" >&2
    printf '%s' "$RESOLVED_SOFASCORE_PROBE_TENNIS_PLAYER_ID"
    return 0
  fi

  if [[ -n "$configured_id" ]]; then
    RESOLVED_SOFASCORE_PROBE_TENNIS_PLAYER_ID_REASON="player_detail_domain_mismatch"
  fi

  echo "INFO: Sofascore probe player selection id=${configured_id:-none} source=none validation=$validation_outcome" >&2
  echo "ERROR: unable to resolve a verified tennis player id from Sofascore schedule and configured fallback did not validate" >&2
  return 1
}
record_allowlist_skip() {
  local source_key="$1"
  local reason="not_in_provider_allowlist"

  if is_provider_default_excluded "$source_key"; then
    reason="excluded_from_default_probe_run"
  fi

  printf '%s\t%s\n' "$source_key" "$reason" >> "$SKIPPED_ALLOWLIST_TSV"
  echo "INFO: skipping provider '$source_key' (status=skipped_by_allowlist; reason=$reason; PROVIDER_ALLOWLIST=$PROVIDER_ALLOWLIST)" >&2
}

emit_entry() {
  local source_key="$1"
  local url="$2"
  local expected_content_type="$3"
  local parser_hint="$4"

  local raw_out="$RAW_DIR/$source_key.body"
  local headers_out="$LOGS_DIR/$source_key.headers"
  local curl_stderr="$LOGS_DIR/$source_key.curl.stderr"
  local meta_out="$META_DIR/$source_key.json"

  echo "==> $source_key"

  local write_out=""
  local curl_exit_code=0
  set +e
  write_out="$(
    curl -sS -L --max-time 30 \
      --retry 2 --retry-delay 1 --retry-all-errors \
      -H "User-Agent: $UA" \
      -D "$headers_out" \
      -o "$raw_out" \
      -w 'http_code=%{http_code}\ncontent_type=%{content_type}\ntime_total=%{time_total}\nfinal_url=%{url_effective}\n' \
      "$url" \
      2>"$curl_stderr"
  )"
  curl_exit_code=$?
  set -e

  local http_code=""
  local write_content_type=""
  local total_time=""
  local final_url=""
  http_code="$(printf '%s\n' "$write_out" | awk -F= '$1=="http_code"{sub(/^http_code=/,""); print; exit}')"
  write_content_type="$(printf '%s\n' "$write_out" | awk -F= '$1=="content_type"{sub(/^content_type=/,""); print; exit}')"
  total_time="$(printf '%s\n' "$write_out" | awk -F= '$1=="time_total"{sub(/^time_total=/,""); print; exit}')"
  final_url="$(printf '%s\n' "$write_out" | awk -F= '$1=="final_url"{sub(/^final_url=/,""); print; exit}')"

  local status="ok"
  local pass=true
  local timeout=false
  local timeout_outcome="no_timeout"

  if [[ "$curl_exit_code" -ne 0 ]]; then
    status="curl_failed"
    pass=false
    if [[ "$curl_exit_code" -eq 28 ]]; then
      timeout=true
      timeout_outcome="request_timed_out"
      status="timeout"
    fi
  elif [[ ! -s "$raw_out" ]]; then
    status="empty_body"
    pass=false
  fi

  local actual_content_type=""
  if [[ -f "$headers_out" ]]; then
    actual_content_type="$(awk -F': ' 'tolower($1)=="content-type" {print $2}' "$headers_out" | tail -n1 | tr -d '\r')"
  fi
  [[ -z "$actual_content_type" ]] && actual_content_type="$write_content_type"

  if [[ "$pass" == true && -n "$expected_content_type" && -n "$actual_content_type" ]]; then
    local expected_lc actual_lc
    expected_lc="$(printf '%s' "$expected_content_type" | tr '[:upper:]' '[:lower:]')"
    actual_lc="$(printf '%s' "$actual_content_type" | tr '[:upper:]' '[:lower:]')"
    if [[ "$actual_lc" != *"$expected_lc"* ]]; then
      status="content_type_mismatch"
      pass=false
    fi
  fi

  local bytes=0
  local sha256=""
  if [[ -f "$raw_out" ]]; then
    bytes="$(wc -c < "$raw_out" | tr -d ' ')"
    sha256="$(sha256sum "$raw_out" | awk '{print $1}')"
  fi

  local retry_attempts=0
  if [[ -f "$curl_stderr" ]]; then
    retry_attempts="$(awk 'BEGIN{c=0} /[Rr]etry/{c++} END{print c}' "$curl_stderr")"
  fi

  SOURCE_KEY="$source_key" \
  URL="$url" \
  EXPECTED_CONTENT_TYPE="$expected_content_type" \
  ACTUAL_CONTENT_TYPE="$actual_content_type" \
  PARSER_HINT="$parser_hint" \
  HTTP_CODE="$http_code" \
  TOTAL_TIME="$total_time" \
  FINAL_URL="$final_url" \
  CURL_EXIT_CODE="$curl_exit_code" \
  RETRY_ATTEMPTS="$retry_attempts" \
  TIMEOUT="$timeout" \
  TIMEOUT_OUTCOME="$timeout_outcome" \
  STATUS="$status" \
  PASS="$pass" \
  BYTES="$bytes" \
  SHA256="$sha256" \
  RAW_PATH="$raw_out" \
  HEADERS_PATH="$headers_out" \
  CURL_STDERR_PATH="$curl_stderr" \
  python3 - "$meta_out" <<'PY'
import json
import os
import sys

meta_out = sys.argv[1]
obj = {
    "source_key": os.environ["SOURCE_KEY"],
    "url": os.environ["URL"],
    "expected_content_type": os.environ["EXPECTED_CONTENT_TYPE"],
    "actual_content_type": os.environ["ACTUAL_CONTENT_TYPE"],
    "parser_hint": os.environ["PARSER_HINT"],
    "http_code": os.environ["HTTP_CODE"],
    "content_type": os.environ["ACTUAL_CONTENT_TYPE"],
    "total_time": float(os.environ["TOTAL_TIME"]) if os.environ["TOTAL_TIME"] else None,
    "final_url": os.environ["FINAL_URL"],
    "curl_exit_code": int(os.environ["CURL_EXIT_CODE"]),
    "retry_attempts": int(os.environ["RETRY_ATTEMPTS"]),
    "timeout": os.environ["TIMEOUT"].lower() == "true",
    "timeout_outcome": os.environ["TIMEOUT_OUTCOME"],
    "status": os.environ["STATUS"],
    "pass": os.environ["PASS"].lower() == "true",
    "bytes": int(os.environ["BYTES"]),
    "sha256": os.environ["SHA256"],
    "raw_path": os.environ["RAW_PATH"],
    "headers_path": os.environ["HEADERS_PATH"],
    "curl_stderr_path": os.environ["CURL_STDERR_PATH"],
}
with open(meta_out, "w", encoding="utf-8") as fh:
    json.dump(obj, fh, indent=2)
    fh.write("\n")
PY

  printf '%s\t%s\t%s\t%s\n' "$source_key" "$status" "$pass" "$http_code" >> "$SUMMARY_TSV"

  echo "status=$status pass=$pass http_code=$http_code bytes=$bytes"
  echo "raw=$raw_out"
  echo "headers=$headers_out"
  echo "curl_stderr=$curl_stderr"
  echo "meta=$meta_out"
  echo
}

iterate_tsv_catalog() {
  while IFS=$'\t' read -r source_key url expected_content_type parser_hint extra || [[ -n "${source_key:-}" ]]; do
    source_key="$(trim "${source_key:-}")"
    [[ -z "$source_key" || "$source_key" == \#* ]] && continue

    url="$(trim "${url:-}")"
    expected_content_type="$(trim "${expected_content_type:-}")"
    parser_hint="$(trim "${parser_hint:-}")"

    if [[ "$url" == *"{date}"* ]]; then
      local date_token
      date_token="$(date -u +%F)"
      url="${url//\{date\}/$date_token}"
    fi

    if [[ "$url" == *"{tennis_player_id}"* ]]; then
      local tennis_player_id
      tennis_player_id="$(resolve_sofascore_probe_tennis_player_id)"
      url="${url//\{tennis_player_id\}/$tennis_player_id}"
    fi

    if [[ -z "$url" || -z "$expected_content_type" || -z "$parser_hint" ]]; then
      echo "WARN: skipping malformed TSV row for source '$source_key'" >&2
      continue
    fi

    if ! is_provider_allowed "$source_key"; then
      record_allowlist_skip "$source_key"
      continue
    fi

    emit_entry "$source_key" "$url" "$expected_content_type" "$parser_hint"
  done < "$CATALOG_PATH"
}

iterate_json_catalog() {
  python3 - "$CATALOG_PATH" <<'PY' | while IFS=$'\t' read -r source_key url expected_content_type parser_hint; do
import json
import sys

catalog_path = sys.argv[1]
with open(catalog_path, 'r', encoding='utf-8') as fh:
    data = json.load(fh)

if isinstance(data, dict):
    data = data.get('sources', [])

for entry in data:
    if not isinstance(entry, dict):
        continue
    values = [
        str(entry.get('source_key', '')).strip(),
        str(entry.get('url', '')).strip(),
        str(entry.get('expected_content_type', '')).strip(),
        str(entry.get('parser_hint', '')).strip(),
    ]
    if values[0].startswith('#'):
        continue
    print("\t".join(values))
PY
    if [[ "$url" == *"{date}"* ]]; then
      date_token="$(date -u +%F)"
      url="${url//\{date\}/$date_token}"
    fi

    if [[ "$url" == *"{tennis_player_id}"* ]]; then
      tennis_player_id="$(resolve_sofascore_probe_tennis_player_id)"
      url="${url//\{tennis_player_id\}/$tennis_player_id}"
    fi

    [[ -z "$source_key" || -z "$url" || -z "$expected_content_type" || -z "$parser_hint" ]] && {
      echo "WARN: skipping malformed JSON entry for source '$source_key'" >&2
      continue
    }

    if ! is_provider_allowed "$source_key"; then
      record_allowlist_skip "$source_key"
      continue
    fi

    emit_entry "$source_key" "$url" "$expected_content_type" "$parser_hint"
  done
}

if [[ ! -f "$CATALOG_PATH" ]]; then
  echo "ERROR: source catalog not found: $CATALOG_PATH" >&2
  exit 1
fi

case "$CATALOG_PATH" in
  *.tsv) iterate_tsv_catalog ;;
  *.json) iterate_json_catalog ;;
  *)
    echo "ERROR: unsupported catalog format for '$CATALOG_PATH' (expected .tsv or .json)" >&2
    exit 1
    ;;
esac

python3 - "$SUMMARY_TSV" "$SKIPPED_ALLOWLIST_TSV" "$META_DIR/summary.json" <<'PY'
import json
import os
import sys

summary_tsv = sys.argv[1]
skipped_tsv = sys.argv[2]
summary_out = sys.argv[3]

sources = []
pass_count = 0
fail_count = 0

with open(summary_tsv, 'r', encoding='utf-8') as fh:
    for line in fh:
        line = line.rstrip('\n')
        if not line:
            continue
        source_key, status, passed, http_code = line.split('\t')
        is_pass = passed.lower() == 'true'
        sources.append({
            'source_key': source_key,
            'status': status,
            'pass': is_pass,
            'http_code': http_code,
        })
        if is_pass:
            pass_count += 1
        else:
            fail_count += 1

skipped_by_allowlist = []
if os.path.exists(skipped_tsv):
    with open(skipped_tsv, 'r', encoding='utf-8') as fh:
        for line in fh:
            line = line.rstrip('\n')
            if not line:
                continue
            source_key, reason = line.split('\t')
            skipped_by_allowlist.append({
                'source_key': source_key,
                'status': 'skipped_by_allowlist',
                'reason': reason,
            })

summary = {
    'total_sources': len(sources),
    'pass_count': pass_count,
    'fail_count': fail_count,
    'skipped_by_allowlist_count': len(skipped_by_allowlist),
    'skipped_by_allowlist': skipped_by_allowlist,
    'sources': sources,
}

with open(summary_out, 'w', encoding='utf-8') as fh:
    json.dump(summary, fh, indent=2)
    fh.write('\n')
PY

rm -f "$SUMMARY_TSV"
rm -f "$SKIPPED_ALLOWLIST_TSV"

LATEST_LINK="./tmp/source_probe_latest"
if [[ "$OUT_DIR" != "$LATEST_LINK" ]]; then
  mkdir -p "$(dirname "$LATEST_LINK")"
  ln -sfn "$(realpath "$OUT_DIR")" "$LATEST_LINK"
  echo "latest_link=$LATEST_LINK -> $(realpath "$OUT_DIR")"
fi

echo "done"
