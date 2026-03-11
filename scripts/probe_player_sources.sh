#!/usr/bin/env bash
set -euo pipefail

UA='Mozilla/5.0 (compatible; WTA-Edge-Board/1.0)'
OUT_DIR="${1:-./tmp/source_probe}"
CATALOG_PATH="${2:-./config/probe_sources.tsv}"
SCHEDULED_PLAYERS_PATH="${3:-}"
PROVIDER_ALLOWLIST="${PROVIDER_ALLOWLIST:-tennisabstract_*,ta_*,mcp_report_*,sofascore_*}"
SOFASCORE_PROBE_TENNIS_PLAYER_ID="${SOFASCORE_PROBE_TENNIS_PLAYER_ID:-}"
DEFAULT_EXCLUDED_PROVIDERS="${DEFAULT_EXCLUDED_PROVIDERS:-wta_stats_zone,tennisexplorer,itf}"
INCLUDE_DEFAULT_EXCLUDED_PROVIDERS="${INCLUDE_DEFAULT_EXCLUDED_PROVIDERS:-false}"

RAW_DIR="$OUT_DIR/raw"
LOGS_DIR="$OUT_DIR/logs"
PARSED_DIR="$OUT_DIR/parsed"
mkdir -p "$RAW_DIR" "$LOGS_DIR" "$PARSED_DIR"

SUMMARY_TSV="$OUT_DIR/.summary.tsv"
: > "$SUMMARY_TSV"
SKIPPED_ALLOWLIST_TSV="$OUT_DIR/.skipped_allowlist.tsv"
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

sample_sofascore_tennis_player_id_from_events() {
  local endpoint="$1"
  local payload_path
  payload_path="$(mktemp "$LOGS_DIR/sofascore_events.XXXXXX.json")"
  if ! curl -sS -L --max-time 20 --retry 1 --retry-delay 1 -H "User-Agent: $UA" -o "$payload_path" "$endpoint" 2>/dev/null; then
    rm -f "$payload_path"
    return 1
  fi
  [[ ! -s "$payload_path" ]] && {
    rm -f "$payload_path"
    return 1
  }

  python3 - "$payload_path" <<'PYT'
import json, sys
path = sys.argv[1]
try:
    with open(path, 'r', encoding='utf-8') as fh:
        payload = json.load(fh)
except Exception:
    sys.exit(1)
if not isinstance(payload, dict):
    sys.exit(1)
for event in payload.get('events', []) or []:
    if not isinstance(event, dict):
        continue
    for side in ('homeTeam', 'awayTeam'):
        team = event.get(side)
        if not isinstance(team, dict):
            continue

        # Prefer explicit player ids when present.
        player = team.get('player')
        player_id = player.get('id') if isinstance(player, dict) else None
        if isinstance(player_id, int) and player_id > 0:
            print(player_id)
            sys.exit(0)
        if isinstance(player_id, str) and player_id.isdigit():
            print(player_id)
            sys.exit(0)

        # Fallback to team id when player object is absent in event payload.
        team_id = team.get('id')
        if isinstance(team_id, int) and team_id > 0:
            print(team_id)
            sys.exit(0)
        if isinstance(team_id, str) and team_id.isdigit():
            print(team_id)
            sys.exit(0)
sys.exit(1)
PYT
  local resolver_exit_code=$?
  rm -f "$payload_path"
  return "$resolver_exit_code"
}


sample_sofascore_tennis_player_id_from_live_or_schedule() {
  local live_url="https://api.sofascore.com/api/v1/sport/tennis/events/live"
  local sampled_id=""
  sampled_id="$(sample_sofascore_tennis_player_id_from_events "$live_url" || true)"
  if [[ -n "$sampled_id" ]]; then
    printf '%s' "$sampled_id"
    return 0
  fi

  local date_token
  date_token="$(date -u +%F)"
  local scheduled_url="https://api.sofascore.com/api/v1/sport/tennis/scheduled-events/$date_token"
  sampled_id="$(sample_sofascore_tennis_player_id_from_events "$scheduled_url" || true)"
  if [[ -n "$sampled_id" ]]; then
    printf '%s' "$sampled_id"
    return 0
  fi

  return 1
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
  sampled_id="$(sample_sofascore_tennis_player_id_from_live_or_schedule || true)"
  if [[ -n "$sampled_id" ]] && validate_sofascore_tennis_player_id "$sampled_id"; then
    RESOLVED_SOFASCORE_PROBE_TENNIS_PLAYER_ID="$sampled_id"
    RESOLVED_SOFASCORE_PROBE_TENNIS_PLAYER_ID_REASON="resolver_selected_tennis_player_id"
    validation_outcome="valid"
    selected_source="resolver"
    echo "INFO: Sofascore probe player selection id=$sampled_id source=$selected_source validation=$validation_outcome" >&2
    printf '%s' "$RESOLVED_SOFASCORE_PROBE_TENNIS_PLAYER_ID"
    return 0
  fi

  # Enforce configured fallback only after dynamic auto-resolution fails.
  if [[ -z "$sampled_id" ]]; then
    RESOLVED_SOFASCORE_PROBE_TENNIS_PLAYER_ID_REASON="resolver_no_tennis_player_id_from_live_or_scheduled_events"
  else
    RESOLVED_SOFASCORE_PROBE_TENNIS_PLAYER_ID_REASON="resolver_selected_player_failed_validation"
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
  else
    RESOLVED_SOFASCORE_PROBE_TENNIS_PLAYER_ID_REASON="resolver_no_tennis_player_id_from_live_or_scheduled_events"
  fi

  echo "INFO: Sofascore probe player selection id=${configured_id:-none} source=none validation=$validation_outcome" >&2
  echo "WARN: unable to resolve a verified tennis player id from Sofascore live/scheduled events" >&2
  return 1
}

source_requires_sofascore_tennis_domain() {
  local source_key="$1"
  case "$source_key" in
    sofascore_player_detail|sofascore_player_recent|sofascore_player_stats_overall|sofascore_player_stats_last52) return 0 ;;
    *) return 1 ;;
  esac
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
  local expected_http_codes="$5"

  local raw_out="$RAW_DIR/$source_key.body"
  local headers_out="$LOGS_DIR/$source_key.headers"
  local curl_stderr="$LOGS_DIR/$source_key.curl.stderr"
  local parsed_out="$PARSED_DIR/$source_key.json"

  echo "==> $source_key"

  local write_out=""
  local curl_exit_code=0

  # Ensure payload artifact exists even when curl fails so downstream
  # validators can report contract failures instead of payload absence.
  : > "$raw_out"
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

  local actual_content_type=""
  if [[ -f "$headers_out" ]]; then
    actual_content_type="$(awk -F': ' 'tolower($1)=="content-type" {print $2}' "$headers_out" | tail -n1 | tr -d '\r')"
  fi
  [[ -z "$actual_content_type" ]] && actual_content_type="$write_content_type"

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
  EXPECTED_HTTP_CODES="$expected_http_codes" \
  ACTUAL_CONTENT_TYPE="$actual_content_type" \
  PARSER_HINT="$parser_hint" \
  HTTP_CODE="$http_code" \
  TOTAL_TIME="$total_time" \
  FINAL_URL="$final_url" \
  CURL_EXIT_CODE="$curl_exit_code" \
  RETRY_ATTEMPTS="$retry_attempts" \
  BYTES="$bytes" \
  SHA256="$sha256" \
  RAW_PATH="$raw_out" \
  HEADERS_PATH="$headers_out" \
  CURL_STDERR_PATH="$curl_stderr" \
  python3 - "$parsed_out" <<'PY'
import json
import os
import re
import sys

parsed_out = sys.argv[1]

header_interest = {
    "content-type": "",
    "content-encoding": "",
    "server": "",
    "cf-ray": "",
}
anti_bot_headers = {}

headers_path = os.environ["HEADERS_PATH"]
if os.path.exists(headers_path):
    with open(headers_path, "r", encoding="utf-8", errors="replace") as fh:
        for line in fh:
            if ":" not in line:
                continue
            name, value = line.split(":", 1)
            k = name.strip().lower()
            v = value.strip()
            if k in header_interest:
                header_interest[k] = v
            if k.startswith("cf-") or any(tag in k for tag in ("captcha", "challenge", "bot", "sucuri", "akamai")):
                anti_bot_headers[k] = v

raw_path = os.environ["RAW_PATH"]
body_bytes = b""
if os.path.exists(raw_path):
    with open(raw_path, "rb") as fh:
        body_bytes = fh.read()

body_text = body_bytes.decode("utf-8", errors="replace")
snippet = ""
if body_text:
    snippet = re.sub(r"\s+", " ", body_text[:2048]).strip()

title = ""
match = re.search(r"<title[^>]*>(.*?)</title>", body_text, flags=re.IGNORECASE | re.DOTALL)
if match:
    title = re.sub(r"\s+", " ", match.group(1)).strip()

content_type = os.environ.get("ACTUAL_CONTENT_TYPE", "")
content_type_lc = content_type.lower()
body_lc = body_text.lower()

expected_content_type = os.environ.get("EXPECTED_CONTENT_TYPE", "")
expected_content_type_lc = expected_content_type.lower()
expected_http_codes = [x.strip() for x in os.environ.get("EXPECTED_HTTP_CODES", "").split(",") if x.strip()]
http_code = (os.environ.get("HTTP_CODE", "") or "").strip()
curl_exit_code = int(os.environ["CURL_EXIT_CODE"])

is_json = ("json" in content_type_lc) or body_lc.lstrip().startswith("{") or body_lc.lstrip().startswith("[")
is_html_shell = ("html" in content_type_lc) or ("<html" in body_lc) or ("<!doctype html" in body_lc)

def is_empty(value):
    if value is None:
        return True
    if isinstance(value, (str, bytes)):
        return len(value.strip()) == 0
    if isinstance(value, (list, dict, tuple, set)):
        return len(value) == 0
    return False

def detect_api_error(value):
    if isinstance(value, dict):
        if "error" in value and not is_empty(value.get("error")) and value.get("error") is not False:
            return True

        has_code = "code" in value
        has_message = "message" in value
        code_val = value.get("code")
        message_val = value.get("message")
        if has_code and has_message:
            if not is_empty(message_val):
                return True
            if str(code_val).strip().lower() not in {"", "0", "200", "ok", "success", "none", "null"}:
                return True
        elif has_code and str(code_val).strip().lower() not in {"", "0", "200", "ok", "success", "none", "null"}:
            return True
        elif has_message and not is_empty(message_val):
            return True

        for nested in value.values():
            if detect_api_error(nested):
                return True
    elif isinstance(value, list):
        return any(detect_api_error(item) for item in value)
    return False

api_error_payload = False
json_parse_error = ""
json_payload = None
if is_json:
    try:
        json_payload = json.loads(body_text)
    except Exception as exc:
        json_parse_error = str(exc)
    else:
        api_error_payload = detect_api_error(json_payload)

def json_path_exists(value, path):
    node = value
    for key in path.split('.'):
        if isinstance(node, dict) and key in node:
            node = node[key]
        else:
            return False
    return True

source_key = os.environ.get("SOURCE_KEY", "")
endpoint_contracts = {
    "itf": {
        "required_json_paths": ["data.rankings"],
        "expected_shape": {"data.rankings": "object_or_array"},
    },
    "sofascore_events_live": {
        "required_json_paths": ["events"],
        "expected_shape": {"events": "array"},
    },
    "sofascore_scheduled_events": {
        "required_json_paths": ["events"],
        "expected_shape": {"events": "array"},
    },
    "sofascore_player_detail": {
        "required_json_paths": ["player"],
        "expected_shape": {"player": "object"},
    },
    "sofascore_player_recent": {
        "required_json_paths": ["events"],
        "expected_shape": {"events": "array"},
    },
    "sofascore_player_stats_overall": {
        "required_json_paths": ["statistics"],
        "expected_shape": {"statistics": "object_or_array"},
    },
    "sofascore_player_stats_last52": {
        "required_json_paths": ["statistics"],
        "expected_shape": {"statistics": "object_or_array"},
    },
}
contract = endpoint_contracts.get(source_key, {"required_json_paths": [], "expected_shape": {}})
required_json_paths = contract.get("required_json_paths", [])

def get_json_path(value, path):
    node = value
    for key in path.split('.'):
        if isinstance(node, dict) and key in node:
            node = node[key]
        else:
            return None
    return node

def matches_shape(value, expected):
    if expected == 'array':
        return isinstance(value, list)
    if expected == 'object':
        return isinstance(value, dict)
    if expected == 'object_or_array':
        return isinstance(value, (dict, list))
    return True

missing_keys = []
contract_check_passed = True
shape_failures = []
if required_json_paths:
    if json_payload is None:
        missing_keys = required_json_paths[:]
        contract_check_passed = False
    else:
        missing_keys = [path for path in required_json_paths if not json_path_exists(json_payload, path)]
        contract_check_passed = len(missing_keys) == 0
        if contract_check_passed:
            expected_shape = contract.get("expected_shape", {})
            for path, expected in expected_shape.items():
                value = get_json_path(json_payload, path)
                if not matches_shape(value, expected):
                    shape_failures.append({"path": path, "expected": expected, "actual": type(value).__name__})
            contract_check_passed = len(shape_failures) == 0

transport_pass = curl_exit_code == 0 and http_code not in ("", "000")
expected_http_ok = (http_code.startswith("2") and len(http_code) == 3 and http_code.isdigit()) if not expected_http_codes else (http_code in expected_http_codes)
content_type_match = True
if expected_content_type_lc and content_type_lc:
    content_type_match = expected_content_type_lc in content_type_lc

status = "ok"
effective_pass = True
if curl_exit_code != 0:
    status = "timeout" if curl_exit_code == 28 else "curl_failed"
    effective_pass = False
elif len(body_bytes) == 0:
    status = "empty_body"
    effective_pass = False
elif not expected_http_ok:
    status = "http_status_unexpected"
    effective_pass = False
elif not content_type_match:
    status = "content_type_mismatch"
    effective_pass = False
elif source_key == "itf" and http_code == "404":
    status = "itf_contract_http_404"
    effective_pass = False
elif source_key == "itf" and not is_json:
    status = "itf_contract_non_json"
    effective_pass = False
elif (("json" in expected_content_type_lc) or os.environ.get("PARSER_HINT", "").strip().lower() == "json_api") and api_error_payload:
    status = "api_error_payload"
    effective_pass = False
elif is_json and isinstance(json_payload, dict) and (json_payload.get('code') == 404 or json_payload.get('status') == 404 or json_payload.get('statusCode') == 404):
    status = "contract_failed_404_json_error"
    effective_pass = False
elif required_json_paths and not contract_check_passed:
    status = "contract_failed"
    effective_pass = False

challenge_patterns = [
    r"attention required",
    r"captcha",
    r"checking your browser",
    r"access denied",
    r"please enable javascript",
    r"challenge",
    r"cloudflare",
]
is_challenge_or_block_page = any(re.search(p, body_lc) for p in challenge_patterns)

likely_spa_bootstrap = any(re.search(p, body_text, flags=re.IGNORECASE) for p in [
    r"__NEXT_DATA__",
    r"window\.__INITIAL_STATE__",
    r"id=[\"']__next[\"']",
    r"data-reactroot",
    r"webpack",
])

counter_patterns = {
    "next_data": r"__NEXT_DATA__",
    "initial_state": r"window\.__INITIAL_STATE__",
    "ld_json": r"application/ld\+json",
    "api_url_hints": r"api/",
    "script_tag": r"<script\b",
    "json_like": r'"[^"\\]{1,80}"\s*:\s*',
}
counters = {k: len(re.findall(p, body_text, flags=re.IGNORECASE)) for k, p in counter_patterns.items()}

obj = {
    "source_key": os.environ["SOURCE_KEY"],
    "url": os.environ["URL"],
    "expected_content_type": os.environ["EXPECTED_CONTENT_TYPE"],
    "expected_http_codes": expected_http_codes,
    "parser_hint": os.environ["PARSER_HINT"],
    "status": status,
    "pass": effective_pass,
    "effective_pass": effective_pass,
    "transport_pass": transport_pass,
    "http_code": http_code,
    "content_type": content_type,
    "total_time": float(os.environ["TOTAL_TIME"]) if os.environ.get("TOTAL_TIME") else None,
    "final_url": os.environ["FINAL_URL"],
    "curl_exit_code": int(os.environ["CURL_EXIT_CODE"]),
    "retry_attempts": int(os.environ["RETRY_ATTEMPTS"]),
    "bytes": int(os.environ["BYTES"]),
    "sha256": os.environ["SHA256"],
    "paths": {
        "raw": os.environ["RAW_PATH"],
        "headers": os.environ["HEADERS_PATH"],
        "curl_stderr": os.environ["CURL_STDERR_PATH"],
    },
    "response_headers": {
        **header_interest,
        "anti_bot": anti_bot_headers,
    },
    "html": {
        "snippet_2kb": snippet,
        "title": title,
    },
    "fingerprint": {
        "is_html_shell": is_html_shell,
        "is_challenge_or_block_page": is_challenge_or_block_page,
        "is_json": is_json,
        "likely_spa_bootstrap": likely_spa_bootstrap,
    },
    "json_analysis": {
        "api_error_payload": api_error_payload,
        "json_parse_error": json_parse_error,
    },
    "contract_check_passed": contract_check_passed,
    "missing_keys": missing_keys,
    "shape_failures": shape_failures,
    "counters": counters,
}

with open(parsed_out, "w", encoding="utf-8") as fh:
    json.dump(obj, fh, indent=2)
    fh.write("\n")
PY

  local status pass effective_pass transport_pass
  status="$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["status"])' "$parsed_out")"
  pass="$(python3 -c 'import json,sys; print(str(json.load(open(sys.argv[1]))["pass"]).lower())' "$parsed_out")"
  effective_pass="$(python3 -c 'import json,sys; print(str(json.load(open(sys.argv[1]))["effective_pass"]).lower())' "$parsed_out")"
  transport_pass="$(python3 -c 'import json,sys; print(str(json.load(open(sys.argv[1]))["transport_pass"]).lower())' "$parsed_out")"

  printf '%s\t%s\t%s\t%s\t%s\t%s\n' "$source_key" "$status" "$pass" "$effective_pass" "$transport_pass" "$http_code" >> "$SUMMARY_TSV"

  echo "status=$status pass=$pass effective_pass=$effective_pass transport_pass=$transport_pass http_code=$http_code bytes=$bytes"
  echo "raw=$raw_out"
  echo "headers=$headers_out"
  echo "curl_stderr=$curl_stderr"
  echo "parsed=$parsed_out"
  echo
}

iterate_tsv_catalog() {
  while IFS=$'\t' read -r source_key url expected_content_type parser_hint extra || [[ -n "${source_key:-}" ]]; do
    source_key="$(trim "${source_key:-}")"
    [[ -z "$source_key" || "$source_key" == \#* ]] && continue

    url="$(trim "${url:-}")"
    expected_content_type="$(trim "${expected_content_type:-}")"
    parser_hint="$(trim "${parser_hint:-}")"
    extra="$(trim "${extra:-}")"

    if [[ "$url" == *"{date}"* ]]; then
      local date_token
      date_token="$(date -u +%F)"
      url="${url//\{date\}/$date_token}"
    fi

    if [[ "$url" == *"{tennis_player_id}"* ]]; then
      local tennis_player_id
      tennis_player_id="$(resolve_sofascore_probe_tennis_player_id || true)"
      if [[ -z "$tennis_player_id" ]]; then
        if source_requires_sofascore_tennis_domain "$source_key"; then
          echo "INFO: skipping '$source_key' due to unmet tennis domain prerequisite (${RESOLVED_SOFASCORE_PROBE_TENNIS_PLAYER_ID_REASON:-player_detail_domain_mismatch})" >&2
        else
          echo "INFO: skipping '$source_key' because no resolver-selected tennis player id is available (${RESOLVED_SOFASCORE_PROBE_TENNIS_PLAYER_ID_REASON:-resolver_no_tennis_player_id_from_live_or_scheduled_events})" >&2
        fi
        continue
      fi
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

    emit_entry "$source_key" "$url" "$expected_content_type" "$parser_hint" "$extra"
  done < "$CATALOG_PATH"
}

iterate_json_catalog() {
  python3 - "$CATALOG_PATH" <<'PY' | while IFS=$'\t' read -r source_key url expected_content_type parser_hint expected_http_codes; do
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
        str(entry.get('expected_http_codes', '')).strip(),
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
      tennis_player_id="$(resolve_sofascore_probe_tennis_player_id || true)"
      if [[ -z "$tennis_player_id" ]]; then
        if source_requires_sofascore_tennis_domain "$source_key"; then
          echo "INFO: skipping '$source_key' due to unmet tennis domain prerequisite (${RESOLVED_SOFASCORE_PROBE_TENNIS_PLAYER_ID_REASON:-player_detail_domain_mismatch})" >&2
        else
          echo "INFO: skipping '$source_key' because no resolver-selected tennis player id is available (${RESOLVED_SOFASCORE_PROBE_TENNIS_PLAYER_ID_REASON:-resolver_no_tennis_player_id_from_live_or_scheduled_events})" >&2
        fi
        continue
      fi
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

    emit_entry "$source_key" "$url" "$expected_content_type" "$parser_hint" "$expected_http_codes"
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

python3 - "$SUMMARY_TSV" "$SKIPPED_ALLOWLIST_TSV" "$PARSED_DIR" "$OUT_DIR/summary.json" <<'PY'
import glob
import json
import os
import sys

summary_tsv = sys.argv[1]
skipped_tsv = sys.argv[2]
parsed_dir = sys.argv[3]
summary_out = sys.argv[4]

sources = []
pass_count = 0
fail_count = 0
partial_run = False
partial_run_reasons = []

if not os.path.exists(summary_tsv):
    partial_run = True
    partial_run_reasons.append('summary_tsv_missing')
else:
    with open(summary_tsv, 'r', encoding='utf-8') as fh:
        for line in fh:
            line = line.rstrip('\n')
            if not line:
                continue
            source_key, status, passed, effective_pass, transport_pass, http_code = line.split('\t')
            is_pass = passed.lower() == 'true'
            is_effective_pass = effective_pass.lower() == 'true'
            is_transport_pass = transport_pass.lower() == 'true'
            sources.append({
                'source_key': source_key,
                'status': status,
                'pass': is_pass,
                'effective_pass': is_effective_pass,
                'transport_pass': is_transport_pass,
                'http_code': http_code,
            })
            if is_effective_pass:
                pass_count += 1
            else:
                fail_count += 1

parsed_entries = []
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

fingerprint_totals = {
    'is_html_shell': 0,
    'is_challenge_or_block_page': 0,
    'is_json': 0,
    'likely_spa_bootstrap': 0,
}
regex_totals = {
    'next_data': 0,
    'initial_state': 0,
    'ld_json': 0,
    'api_url_hints': 0,
}
for path in sorted(glob.glob(os.path.join(parsed_dir, '*.json'))):
    try:
        with open(path, 'r', encoding='utf-8') as fh:
            obj = json.load(fh)
    except Exception:
        partial_run = True
        partial_run_reasons.append(f'parsed_json_unreadable:{os.path.basename(path)}')
        continue
    parsed_entries.append(obj)
    fp = obj.get('fingerprint', {})
    for key in fingerprint_totals:
        if fp.get(key):
            fingerprint_totals[key] += 1
    counters = obj.get('counters', {})
    for key in regex_totals:
        regex_totals[key] += int(counters.get(key, 0) or 0)

summary = {
    'total_sources': len(sources),
    'pass_count': pass_count,
    'fail_count': fail_count,
    'skipped_by_allowlist_count': len(skipped_by_allowlist),
    'skipped_by_allowlist': skipped_by_allowlist,
    'partial_run': partial_run,
    'partial_run_reasons': sorted(set(partial_run_reasons)),
    'sources': sources,
    'fingerprint_totals': fingerprint_totals,
    'regex_totals': regex_totals,
    'parsed_files': [os.path.relpath(p, os.path.dirname(summary_out)) for p in sorted(glob.glob(os.path.join(parsed_dir, '*.json')))],
    'entries': parsed_entries,
}

with open(summary_out, 'w', encoding='utf-8') as fh:
    json.dump(summary, fh, indent=2)
    fh.write('\n')
PY

rm -f "$SUMMARY_TSV"
rm -f "$SKIPPED_ALLOWLIST_TSV"

run_ta_parser_probe() {
  local summary_path="$1"
  local ta_probe_out="$OUT_DIR/ta_parser_probe.json"
  local ta_js_raw="$RAW_DIR/tennisabstract_leadersource_wta.body"
  local ta_js_headers="$LOGS_DIR/tennisabstract_leadersource_wta.headers"
  local ta_js_stderr="$LOGS_DIR/tennisabstract_leadersource_wta.curl.stderr"

  python3 - "$summary_path" "$ta_probe_out" "$SCHEDULED_PLAYERS_PATH" "$ta_js_raw" "$ta_js_headers" "$ta_js_stderr" "$UA" <<'PY'
import json
import os
import re
import sys
import urllib.request

summary_path, out_path, scheduled_path, js_raw_path, js_headers_path, js_stderr_path, ua = sys.argv[1:]

def canonicalize(name: str) -> str:
    val = (name or "").strip().lower()
    if not val:
        return ""
    try:
        import unicodedata
        val = ''.join(ch for ch in unicodedata.normalize('NFD', val) if unicodedata.category(ch) != 'Mn')
    except Exception:
        pass
    val = re.sub(r"[^a-z0-9\s]", " ", val)
    val = re.sub(r"\s+", " ", val).strip()
    return val

def parse_scheduled_players(path: str):
    if not path or not os.path.exists(path):
        return []
    text = open(path, 'r', encoding='utf-8', errors='replace').read()
    players = []
    if path.lower().endswith('.json'):
        try:
            payload = json.loads(text)
            if isinstance(payload, list):
                players = [str(x) for x in payload]
            elif isinstance(payload, dict):
                for key in ('players', 'scheduled_players', 'names'):
                    if isinstance(payload.get(key), list):
                        players = [str(x) for x in payload[key]]
                        break
        except Exception:
            players = []
    if not players:
        players = [line.strip() for line in text.splitlines() if line.strip() and not line.strip().startswith('#')]
    canon = []
    seen = set()
    for p in players:
        c = canonicalize(p)
        if c and c not in seen:
            seen.add(c)
            canon.append(c)
    return canon

def parse_js_array_tokens(body: str):
    pattern = re.compile(r'"((?:\\.|[^"\\])*)"|\'((?:\\.|[^\'\\])*)\'|([^,]+)')
    tokens = []
    for m in pattern.finditer(body):
        raw = m.group(1) if m.group(1) is not None else (m.group(2) if m.group(2) is not None else m.group(3))
        norm = (raw or '').strip()
        if norm in ('null', 'undefined'):
            tokens.append('')
            continue
        tokens.append(norm.replace('\\"', '"').replace("\\'", "'"))
    return tokens

def build_structured(tokens):
    idx = {
        'player_name': 3,
        'score': 5,
        'ranking': 6,
        'recent_form': 7,
        'surface_win_rate': 8,
        'hold_pct': 9,
        'break_pct': 10,
    }
    score = str(tokens[idx['score']] if len(tokens) > idx['score'] else '').strip()
    has_walkover_or_ret = bool(re.search(r'\b(?:ret|wo)\b', score, flags=re.I))
    def take(i):
        try:
            return float(tokens[i])
        except Exception:
            return None
    row = {
        'player_name': str(tokens[idx['player_name']] if len(tokens) > idx['player_name'] else ''),
        'score': score,
        'ranking': take(idx['ranking']),
        'recent_form': None if has_walkover_or_ret else take(idx['recent_form']),
        'surface_win_rate': None if has_walkover_or_ret else take(idx['surface_win_rate']),
        'hold_pct': None if has_walkover_or_ret else take(idx['hold_pct']),
        'break_pct': None if has_walkover_or_ret else take(idx['break_pct']),
    }
    return row

def parse_array_literal_rows(text: str):
    m = re.search(r'\bvar\s+matchmx\s*=\s*', text)
    if not m:
        return []
    start = text.find('[', m.end())
    if start < 0:
        return []
    depth = 0
    quote = ''
    escaped = False
    end = -1
    for i in range(start, len(text)):
        ch = text[i]
        if quote:
            if escaped:
                escaped = False
            elif ch == '\\':
                escaped = True
            elif ch == quote:
                quote = ''
            continue
        if ch in ('"', "'"):
            quote = ch
            continue
        if ch == '[':
            depth += 1
        elif ch == ']':
            depth -= 1
            if depth == 0:
                end = i
                break
    if end < 0:
        return []
    lit = text[start:end + 1]
    rows, depth, quote, escaped, row_start = [], 0, '', False, -1
    for i in range(1, len(lit)):
        ch = lit[i]
        if quote:
            if escaped:
                escaped = False
            elif ch == '\\':
                escaped = True
            elif ch == quote:
                quote = ''
            continue
        if ch in ('"', "'"):
            quote = ch
            continue
        if ch == '[':
            if depth == 0:
                row_start = i
            depth += 1
            continue
        if ch == ']':
            if depth <= 0:
                break
            depth -= 1
            if depth == 0 and row_start >= 0:
                body = lit[row_start + 1:i]
                tokens = parse_js_array_tokens(body)
                if len(tokens) >= 6:
                    r = build_structured(tokens)
                    if r.get('player_name') and r.get('score'):
                        rows.append(r)
                row_start = -1
    return rows

def parse_legacy_rows(text: str):
    rows = []
    row_re = re.compile(r'matchmx\s*(?:\[\s*\d+\s*\])?\s*=\s*\[([\s\S]*?)\]\s*;')
    for m in row_re.finditer(text):
        tokens = parse_js_array_tokens(m.group(1))
        if len(tokens) < 6:
            continue
        r = build_structured(tokens)
        if r.get('player_name') and r.get('score'):
            rows.append(r)
    return rows

def extract_rows(payload: str):
    rows = parse_array_literal_rows(payload)
    return rows if rows else parse_legacy_rows(payload)

summary = json.load(open(summary_path, 'r', encoding='utf-8'))
entries = summary.get('entries') or []
ta = next((e for e in entries if e.get('source_key') == 'tennisabstract_leaders'), None)

result = {
    'available': False,
    'reason': 'tennisabstract_leaders_entry_missing',
    'structured_rows_count': 0,
    'non_null_counts': {
        'ranking': 0,
        'recent_form': 0,
        'surface_win_rate': 0,
        'hold_pct': 0,
        'break_pct': 0,
    },
    'unique_players_parsed': 0,
    'sample_canonical_names': [],
    'scheduled_players_sample_size': 0,
    'scheduled_players_overlap_count': 0,
    'scheduled_players_overlap_sample': [],
    'ta_matchmx_ok': False,
    'ta_matchmx_unusable_payload': False,
    'ta_parse_coverage_mismatch': False,
}

if ta:
    html = ''
    raw_path = (((ta.get('paths') or {}).get('raw')) or '')
    if raw_path and os.path.exists(raw_path):
        html = open(raw_path, 'r', encoding='utf-8', errors='replace').read()
    js_match = re.search(r'(?:https?:)?//[^"\'\s]*jsmatches/[^"\'\s]*leadersource[^"\'\s]*wta\.js|/?jsmatches/[^"\'\s]*leadersource[^"\'\s]*wta\.js', html, flags=re.I)
    js_url = ''
    if js_match:
        token = js_match.group(0)
        if token.startswith('http://') or token.startswith('https://'):
            js_url = token
        elif token.startswith('//'):
            js_url = 'https:' + token
        elif token.startswith('/'):
            js_url = 'https://www.tennisabstract.com' + token
        else:
            js_url = 'https://www.tennisabstract.com/' + token.lstrip('/')
    if js_url:
        req = urllib.request.Request(js_url, headers={'User-Agent': ua})
        try:
            with urllib.request.urlopen(req, timeout=30) as resp:
                data = resp.read()
                payload = data.decode('utf-8', errors='replace')
                open(js_raw_path, 'wb').write(data)
                with open(js_headers_path, 'w', encoding='utf-8') as fh:
                    for k, v in resp.headers.items():
                        fh.write(f"{k}: {v}\n")
        except Exception as e:
            with open(js_stderr_path, 'w', encoding='utf-8') as fh:
                fh.write(str(e) + "\n")
            payload = ''
            result['reason'] = 'ta_js_fetch_failed'
        rows = extract_rows(payload) if payload else []
        counts = {k: 0 for k in ['ranking','recent_form','surface_win_rate','hold_pct','break_pct']}
        player_feature = {}
        canonical_samples = []
        seen_names = set()
        for row in rows:
            c = canonicalize(row.get('player_name', ''))
            if c and c not in seen_names and len(canonical_samples) < 8:
                canonical_samples.append(c)
                seen_names.add(c)
            if c and c not in player_feature:
                player_feature[c] = {k: False for k in counts.keys()}
            for k in counts.keys():
                if row.get(k) is not None:
                    counts[k] += 1
                    if c:
                        player_feature[c][k] = True

        scheduled = parse_scheduled_players(scheduled_path)
        parsed_players = sorted(player_feature.keys())
        overlap = [p for p in parsed_players if p in set(scheduled)]
        normalized_non_null_counts = {
            k: sum(1 for _, f in player_feature.items() if f.get(k))
            for k in counts.keys()
        }
        players_with_non_null_stats = sum(1 for _, f in player_feature.items() if any(f.values()))
        normalized_total = int(normalized_non_null_counts.get('ranking', 0)) + int(normalized_non_null_counts.get('hold_pct', 0)) + int(normalized_non_null_counts.get('break_pct', 0))
        ta_parse_coverage_mismatch = len(rows) > 500 and normalized_total <= 3
        ta_matchmx_unusable_payload = len(rows) >= 500 and players_with_non_null_stats == 0
        ta_matchmx_ok = len(rows) > 0 and not ta_parse_coverage_mismatch and not ta_matchmx_unusable_payload

        result.update({
            'available': True,
            'reason': 'ta_matchmx_ok' if ta_matchmx_ok else ('ta_matchmx_unusable_payload' if ta_matchmx_unusable_payload else ('ta_parse_coverage_mismatch' if ta_parse_coverage_mismatch else ('ta_matchmx_parse_failed' if not rows else 'ta_matchmx_partial'))),
            'ta_js_url': js_url,
            'structured_rows_count': len(rows),
            'non_null_counts': counts,
            'unique_players_parsed': len(parsed_players),
            'sample_canonical_names': canonical_samples,
            'scheduled_players_sample_size': len(scheduled),
            'scheduled_players_overlap_count': len(overlap),
            'scheduled_players_overlap_sample': overlap[:8],
            'normalized_non_null_counts': normalized_non_null_counts,
            'players_with_non_null_stats': players_with_non_null_stats,
            'ta_matchmx_ok': ta_matchmx_ok,
            'ta_matchmx_unusable_payload': ta_matchmx_unusable_payload,
            'ta_parse_coverage_mismatch': ta_parse_coverage_mismatch,
        })
    else:
        result['reason'] = 'ta_js_url_missing'

summary['ta_parser_probe'] = result
with open(summary_path, 'w', encoding='utf-8') as fh:
    json.dump(summary, fh, indent=2)
    fh.write('\n')
with open(out_path, 'w', encoding='utf-8') as fh:
    json.dump(result, fh, indent=2)
    fh.write('\n')
PY
}

run_ta_parser_probe "$OUT_DIR/summary.json"

LATEST_LINK="./tmp/source_probe_latest"
if [[ "$OUT_DIR" != "$LATEST_LINK" ]]; then
  mkdir -p "$(dirname "$LATEST_LINK")"
  ln -sfn "$(realpath "$OUT_DIR")" "$LATEST_LINK"
  echo "latest_link=$LATEST_LINK -> $(realpath "$OUT_DIR")"
fi

echo "done"
