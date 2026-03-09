#!/usr/bin/env bash
set -euo pipefail

UA='Mozilla/5.0 (compatible; WTA-Edge-Board/1.0)'
OUT_DIR="${1:-./tmp/source_probe}"
CATALOG_PATH="${2:-./config/probe_sources.tsv}"

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

echo "done"
