#!/usr/bin/env bash
set -euo pipefail

UA='Mozilla/5.0 (compatible; WTA-Edge-Board/1.0)'
OUT_DIR="${1:-./tmp/source_probe}"
CATALOG_PATH="${2:-./config/probe_sources.tsv}"
SCHEDULED_PLAYERS_PATH="${3:-}"

RAW_DIR="$OUT_DIR/raw"
LOGS_DIR="$OUT_DIR/logs"
PARSED_DIR="$OUT_DIR/parsed"
mkdir -p "$RAW_DIR" "$LOGS_DIR" "$PARSED_DIR"

SUMMARY_TSV="$OUT_DIR/.summary.tsv"
: > "$SUMMARY_TSV"

trim() {
  local value="$1"
  value="${value#"${value%%[![:space:]]*}"}"
  value="${value%"${value##*[![:space:]]}"}"
  printf '%s' "$value"
}

emit_entry() {
  local source_key="$1"
  local url="$2"
  local expected_content_type="$3"
  local parser_hint="$4"

  local raw_out="$RAW_DIR/$source_key.body"
  local headers_out="$LOGS_DIR/$source_key.headers"
  local curl_stderr="$LOGS_DIR/$source_key.curl.stderr"
  local parsed_out="$PARSED_DIR/$source_key.json"

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

  if [[ "$curl_exit_code" -ne 0 ]]; then
    status="curl_failed"
    pass=false
    if [[ "$curl_exit_code" -eq 28 ]]; then
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
  STATUS="$status" \
  PASS="$pass" \
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

is_json = ("json" in content_type_lc) or body_lc.lstrip().startswith("{") or body_lc.lstrip().startswith("[")
is_html_shell = ("html" in content_type_lc) or ("<html" in body_lc) or ("<!doctype html" in body_lc)

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
    "parser_hint": os.environ["PARSER_HINT"],
    "status": os.environ["STATUS"],
    "pass": os.environ["PASS"].lower() == "true",
    "http_code": os.environ["HTTP_CODE"],
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
    "counters": counters,
}

with open(parsed_out, "w", encoding="utf-8") as fh:
    json.dump(obj, fh, indent=2)
    fh.write("\n")
PY

  printf '%s\t%s\t%s\t%s\n' "$source_key" "$status" "$pass" "$http_code" >> "$SUMMARY_TSV"

  echo "status=$status pass=$pass http_code=$http_code bytes=$bytes"
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

    if [[ -z "$url" || -z "$expected_content_type" || -z "$parser_hint" ]]; then
      echo "WARN: skipping malformed TSV row for source '$source_key'" >&2
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
    [[ -z "$source_key" || -z "$url" || -z "$expected_content_type" || -z "$parser_hint" ]] && {
      echo "WARN: skipping malformed JSON entry for source '$source_key'" >&2
      continue
    }

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

python3 - "$SUMMARY_TSV" "$PARSED_DIR" "$OUT_DIR/summary.json" <<'PY'
import glob
import json
import os
import sys

summary_tsv = sys.argv[1]
parsed_dir = sys.argv[2]
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

parsed_entries = []
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
    with open(path, 'r', encoding='utf-8') as fh:
        obj = json.load(fh)
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

echo "done"
