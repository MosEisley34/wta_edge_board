#!/usr/bin/env bash
set -euo pipefail

UA='Mozilla/5.0 (compatible; WTA-Edge-Board/1.0)'
OUT_DIR="${1:-./tmp/source_probes}"
CATALOG_PATH="${2:-./config/probe_sources.tsv}"

RAW_DIR="$OUT_DIR/raw"
LOGS_DIR="$OUT_DIR/logs"
META_DIR="$OUT_DIR/meta"
mkdir -p "$RAW_DIR" "$LOGS_DIR" "$META_DIR"

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
  local curl_log="$LOGS_DIR/$source_key.curl.log"
  local meta_out="$META_DIR/$source_key.meta"

  echo "==> $source_key"
  local http_code
  http_code="$(
    curl -sS -L --max-time 30 \
      -H "User-Agent: $UA" \
      -D "$headers_out" \
      -o "$raw_out" \
      -w '%{http_code}' \
      "$url" \
      >"$curl_log" 2>&1
  )" || true

  local status="ok"
  if [[ ! -s "$raw_out" ]]; then
    status="fetch_failed"
  fi

  local actual_content_type=""
  if [[ -f "$headers_out" ]]; then
    actual_content_type="$(awk -F': ' 'tolower($1)=="content-type" {print $2}' "$headers_out" | tail -n1 | tr -d '\r')"
  fi

  local bytes=0
  local sha256=""
  if [[ -f "$raw_out" ]]; then
    bytes="$(wc -c < "$raw_out" | tr -d ' ')"
    sha256="$(sha256sum "$raw_out" | awk '{print $1}')"
  fi

  cat > "$meta_out" <<META
source_key=$source_key
url=$url
expected_content_type=$expected_content_type
actual_content_type=$actual_content_type
parser_hint=$parser_hint
http_code=$http_code
status=$status
bytes=$bytes
sha256=$sha256
raw_path=$raw_out
headers_path=$headers_out
curl_log_path=$curl_log
META

  echo "status=$status http_code=$http_code bytes=$bytes"
  echo "raw=$raw_out"
  echo "headers=$headers_out"
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

echo "done"
